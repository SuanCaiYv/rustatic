use std::{
    cell::UnsafeCell,
    net::SocketAddr,
    os::fd::{AsRawFd, RawFd},
    sync::Arc,
};

use anyhow::anyhow;
use base64::Engine;
use bytes::BytesMut;
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::mpsc,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    db::{get_metadata_ops, get_user_ops, metadata::Metadata, user::User},
    pool::ThreadPool,
};

use super::{download::Download, upload::Upload, UnsafeFD};

pub(self) const ALPN_RUSTATIC: &[&[u8]] = &[b"rustatic"];

type HmacSha256 = Hmac<Sha256>;

pub(crate) struct ServerConfig {
    pub(crate) data_port: u16,
    pub(crate) ctrl_port: u16,
    pub(crate) cert: rustls::Certificate,
    pub(crate) key: rustls::PrivateKey,
}

pub(crate) struct Server {
    config: Option<ServerConfig>,
}

impl Server {
    pub(crate) fn new(config: ServerConfig) -> Self {
        Self {
            config: Some(config),
        }
    }

    pub(crate) async fn run(&mut self) -> anyhow::Result<()> {
        let ServerConfig {
            data_port,
            ctrl_port,
            cert,
            key,
        } = self.config.take().unwrap();

        let mut rustls_config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?;
        rustls_config.alpn_protocols = ALPN_RUSTATIC.iter().map(|&x| x.into()).collect();

        let ctrl_listener = TcpListener::bind(
            format!("0.0.0.0:{}", ctrl_port)
                .parse::<SocketAddr>()
                .unwrap(),
        )
        .await?;
        let data_listener = TcpListener::bind(
            format!("0.0.0.0:{}", data_port)
                .parse::<SocketAddr>()
                .unwrap(),
        )
        .await?;
        let acceptor = TlsAcceptor::from(Arc::new(rustls_config));

        let data_conn_map = Arc::new(DashMap::new());

        let conn_map = data_conn_map.clone();
        tokio::spawn(async move {
            info!("data listener started");
            let thread_pool = Arc::new(ThreadPool::new(24, 180, 200));
            loop {
                let (data_stream, _) = match data_listener.accept().await {
                    Ok(x) => x,
                    Err(e) => {
                        error!("data listener accept error: {}", e);
                        continue;
                    }
                };

                let conn_map = conn_map.clone();
                let thread_pool = thread_pool.clone();
                tokio::spawn(async move {
                    let (cmd_tx, cmd_rx) = mpsc::channel(32);
                    let mut data_connection = DataConnection::new(data_stream, cmd_rx, thread_pool);
                    let session_id = data_connection.init().await?;
                    conn_map.insert(session_id, cmd_tx);

                    if let Err(e) = data_connection.handle().await {
                        error!("data connection handle error: {}", e);
                    }
                    conn_map.remove(&data_connection.session_id);
                    Ok::<(), anyhow::Error>(())
                });
            }
        });

        info!("ctrl listener started");
        while let Ok((stream, _)) = ctrl_listener.accept().await {
            let stream = acceptor.accept(stream).await?;
            let mut request = Request::new(stream, data_conn_map.clone());
            tokio::spawn(async move {
                if let Err(e) = request.handle().await {
                    error!("request handle error: {}", e);
                }
            });
        }
        Ok(())
    }
}

pub(self) struct Request {
    stream: TlsStream<TcpStream>,
    cmd_map: Arc<DashMap<String, mpsc::Sender<Cmd>>>,
}

impl Request {
    pub(self) fn new(
        stream: TlsStream<TcpStream>,
        cmd_map: Arc<DashMap<String, mpsc::Sender<Cmd>>>,
    ) -> Self {
        Self { stream, cmd_map }
    }

    pub(self) async fn handle(&mut self) -> anyhow::Result<()> {
        let mut buffer = BytesMut::with_capacity(4096);
        unsafe { buffer.set_len(4096) };
        let mut curr_user = None;
        loop {
            let op_code = match self.stream.read_u16().await {
                Ok(code) => code,
                Err(e) => {
                    error!("read request error: {}", e);
                    break;
                }
            };
            let req_len = self.stream.read_u16().await?;
            let content = match self
                .stream
                .read_exact(&mut buffer[0..req_len as usize])
                .await
            {
                Ok(n) => {
                    if n != req_len as usize {
                        error!("read request error: length not match");
                        break;
                    }
                    &buffer[..req_len as usize]
                }
                Err(e) => {
                    error!("read request error: {}", e);
                    break;
                }
            };
            match op_code {
                1 => {
                    let params = Self::parse1(content)?;
                    curr_user = Some(params.0.clone());
                    if let Err(e) = Self::sign(params).await {
                        error!("sign error: {}", e);
                        self.stream.write_all(format!("err {}\n", e.to_string()).as_bytes()).await?;
                        break;
                    }
                    let uuid = Uuid::new_v4().to_string();
                    let engine = base64::engine::GeneralPurpose::new(
                        &base64::alphabet::URL_SAFE,
                        base64::engine::general_purpose::NO_PAD,
                    );
                    let session_id = engine.encode(uuid);
                    self.stream.write_all(format!("ok {}\n", session_id).as_bytes()).await?;
                }
                2 => {
                    let params = Self::parse1(content)?;
                    if let Some(ref curr_user) = curr_user {
                        if curr_user != &params.0 {
                            error!("user not match");
                            break;
                        }
                    } else {
                        curr_user = Some(params.0.clone());
                    }
                    if let Err(e) = Self::login(Self::parse1(content)?).await {
                        error!("login error: {}", e);
                        self.stream.write_all(format!("err {}\n", e.to_string()).as_bytes()).await?;
                        break;
                    }
                    let uuid = Uuid::new_v4().to_string();
                    let engine = base64::engine::GeneralPurpose::new(
                        &base64::alphabet::URL_SAFE,
                        base64::engine::general_purpose::NO_PAD,
                    );
                    let session_id = engine.encode(uuid);
                    self.stream.write_all(format!("ok {}\n", session_id).as_bytes()).await?;
                }
                3 => {
                    let params = match serde_json::from_slice::<serde_json::Value>(content) {
                        Ok(entry) => {
                            let entry = match entry.as_object() {
                                Some(e) => e,
                                None => {
                                    info!("data connection closed");
                                    break;
                                }
                            };
                            (
                                entry
                                    .get("session_id")
                                    .unwrap()
                                    .as_str()
                                    .unwrap()
                                    .to_owned(),
                                entry.get("filename").unwrap().as_str().unwrap().to_owned(),
                                entry.get("size").unwrap().as_i64().unwrap(),
                                entry.get("create_at").unwrap().as_i64().unwrap().to_owned(),
                                entry.get("update_at").unwrap().as_i64().unwrap().to_owned(),
                            )
                        }
                        Err(e) => {
                            error!("parse request error: {}", e);
                            self.stream.write_all(format!("err {}\n", e.to_string()).as_bytes()).await?;
                            continue;
                        }
                    };
                    let uuid = Uuid::new_v4().to_string();
                    let engine = base64::engine::GeneralPurpose::new(
                        &base64::alphabet::URL_SAFE,
                        base64::engine::general_purpose::NO_PAD,
                    );
                    let link = engine.encode(uuid);
                    let (session_id, filename, size, ..) = params;
                    let filepath = format!(
                        "/Users/joker/RustProjects/rustatic/data/{}/{}",
                        curr_user.as_ref().unwrap(),
                        filename
                    );
                    let metadata = Metadata {
                        id: 0,
                        filename: filename.clone(),
                        owner: curr_user.clone().unwrap(),
                        link: link.clone(),
                        size,
                        sha256: "".to_owned(),
                        filepath: filepath.clone(), // set to link.<suffix>
                        encrypt_key: "".to_owned(),
                        permissions: "private".to_owned(),
                        r#type: "".to_owned(),
                        classification: "".to_owned(),
                        create_time: chrono::Local::now().timestamp_millis(),
                        update_time: chrono::Local::now().timestamp_millis(),
                        delete_time: 0,
                    };
                    match get_metadata_ops().await.insert(metadata).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("insert metadata error: {}", e);
                            break;
                        }
                    }
                    match self.cmd_map.get(session_id.as_str()) {
                        Some(cmd_tx) => {
                            cmd_tx.send(Cmd::Upload(filepath, size as usize)).await?;
                            self.stream.write_all(format!("ok {}\n", link).as_bytes()).await?;
                        }
                        None => {
                            error!("session id not found");
                            self.stream.write_all(format!("err {}\n", "bad session_id").as_bytes()).await?;
                        }
                    }
                }
                4 => {
                    let (session_id, link) = Self::parse2(content)?;
                    let metadata = match get_metadata_ops().await.get_by_link(link.clone()).await {
                        Ok(res) => match res {
                            Some(v) => v,
                            None => {
                                error!("metadata not found");
                                break;
                            }
                        },
                        Err(e) => {
                            error!("get metadata error: {}", e);
                            break;
                        }
                    };
                    match self.cmd_map.get(session_id.as_str()) {
                        Some(cmd_tx) => {
                            cmd_tx.send(Cmd::Download(metadata.filepath)).await?;
                        }
                        None => {
                            error!("session id not found");
                            break;
                        }
                    }
                }
                5 => {
                    let (session_id, link) = Self::parse2(content)?;
                    let metadata = match get_metadata_ops().await.get_by_link(link.clone()).await {
                        Ok(res) => match res {
                            Some(v) => v,
                            None => {
                                error!("metadata not found");
                                break;
                            }
                        },
                        Err(e) => {
                            error!("get metadata error: {}", e);
                            break;
                        }
                    };
                    match self.cmd_map.get(session_id.as_str()) {
                        Some(cmd_tx) => {
                            cmd_tx
                                .send(Cmd::DownloadDirectly(metadata.filepath))
                                .await?;
                        }
                        None => {
                            error!("session id not found");
                            break;
                        }
                    }
                }
                _ => {
                    error!("unknown op code: {}", op_code);
                    break;
                }
            }
        }
        Ok(())
    }

    fn parse1(req: &[u8]) -> anyhow::Result<(String, String)> {
        let (username, password) = match serde_json::from_slice::<serde_json::Value>(req) {
            Ok(entry) => {
                let entry = match entry.as_object() {
                    Some(e) => e,
                    None => {
                        error!("parse request error: not a json object");
                        return Err(anyhow!("not a json object"));
                    }
                };
                (
                    entry.get("username").unwrap().as_str().unwrap().to_owned(),
                    entry.get("password").unwrap().as_str().unwrap().to_owned(),
                )
            }
            Err(e) => {
                error!("parse request error: {}", e);
                return Err(anyhow!("parse request error"));
            }
        };
        Ok((username, password))
    }

    fn parse2(req: &[u8]) -> anyhow::Result<(String, String)> {
        Ok(match serde_json::from_slice::<serde_json::Value>(req) {
            Ok(entry) => {
                let entry = match entry.as_object() {
                    Some(e) => e,
                    None => {
                        error!("parse request error: not a json object");
                        return Err(anyhow!("not a json object"));
                    }
                };
                (
                    entry
                        .get("session_id")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    entry.get("link").unwrap().as_str().unwrap().to_owned(),
                )
            }
            Err(e) => {
                error!("parse request error: {}", e);
                return Err(anyhow!("parse request error"));
            }
        })
    }

    pub(self) async fn sign(params: (String, String)) -> anyhow::Result<()> {
        let (username, password) = params;
        if get_user_ops().await.get(username.clone()).await?.is_some() {
            return Err(anyhow!("user already exists"));
        }
        let user_salt = Self::salt(12);
        let mut mac: HmacSha256 = HmacSha256::new_from_slice(user_salt.as_bytes()).unwrap();
        mac.update(password.as_bytes());
        let res = mac.finalize().into_bytes();
        let res_str = format!("{:X}", res);
        let user = User {
            id: 0,
            username,
            password: res_str,
            salt: user_salt,
            create_time: chrono::Local::now().timestamp_millis(),
            update_time: chrono::Local::now().timestamp_millis(),
            delete_time: 0,
        };
        get_user_ops().await.insert(user).await?;
        Ok(())
    }

    pub(self) async fn login(params: (String, String)) -> anyhow::Result<()> {
        let (username, password) = params;
        let user = match get_user_ops().await.get(username.clone()).await? {
            Some(user) => user,
            None => {
                return Err(anyhow!("user not exists"));
            }
        };
        let mut mac: HmacSha256 = HmacSha256::new_from_slice(user.salt.as_bytes()).unwrap();
        mac.update(password.as_bytes());
        let res = mac.finalize().into_bytes();
        let res_str = format!("{:X}", res);
        if res_str != user.password {
            return Err(anyhow!("password not match"));
        }
        Ok(())
    }

    pub(self) fn salt(length: usize) -> String {
        let length = if length > 32 { 32 } else { length };
        let string = Uuid::new_v4().to_string().replace("-", "M");
        String::from_utf8_lossy(&string.as_bytes()[0..length])
            .to_string()
            .to_uppercase()
    }
}

pub(self) struct DataConnection {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    raw_fd: RawFd,
    cmd_rx: mpsc::Receiver<Cmd>,
    thread_pool: Arc<ThreadPool>,
    session_id: String,
}

impl DataConnection {
    pub(self) fn new(
        stream: TcpStream,
        cmd_rx: mpsc::Receiver<Cmd>,
        thread_pool: Arc<ThreadPool>,
    ) -> Self {
        let raw_fd = stream.as_raw_fd();
        let (reader, writer) = stream.into_split();
        Self {
            reader,
            writer,
            raw_fd,
            cmd_rx,
            thread_pool,
            session_id: String::new(),
        }
    }

    pub(self) async fn init(&mut self) -> anyhow::Result<String> {
        let mut buf = [0u8; 256];
        let mut idx = 0;
        loop {
            let n = self.reader.read(&mut buf[idx..]).await?;
            if n == 0 {
                break;
            }
            for i in idx..idx + n {
                if buf[i] == b'\n' {
                    self.session_id = String::from_utf8_lossy(&buf[0..i]).to_string();
                    return Ok(self.session_id.clone());
                }
            }
            idx += n;
        }
        Err(anyhow!("init error"))
    }

    pub(self) async fn handle(&mut self) -> anyhow::Result<()> {
        loop {
            match self.cmd_rx.recv().await {
                Some(cmd) => match cmd {
                    Cmd::Upload(filename, size) => self.upload(filename.as_str(), size).await?,
                    Cmd::Download(filename) => self.download(filename.as_str()).await?,
                    Cmd::DownloadDirectly(filename) => {
                        self.download_directly(filename.as_str()).await?
                    }
                },
                None => break,
            }
        }
        Ok(())
    }

    pub(self) async fn upload(&mut self, filepath: &str, size: usize) -> anyhow::Result<()> {
        Upload::new(
            size,
            filepath.to_owned(),
            self.thread_pool.clone(),
            &mut self.reader,
        )
        .run()
        .await?;
        Ok(())
    }

    pub(self) async fn download(&mut self, filepath: &str) -> anyhow::Result<()> {
        Download::new(
            filepath.to_owned(),
            UnsafeFD { fd: self.raw_fd },
            self.thread_pool.clone(),
            &mut self.writer,
        )
        .run()
        .await?;
        Ok(())
    }

    pub(self) async fn download_directly(&mut self, filepath: &str) -> anyhow::Result<()> {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(filepath)
            .await
            .unwrap();
        let mut buffer = BytesMut::with_capacity(4096 * 4);
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            self.writer.write_all(&buffer[..n]).await?;
        }
        Ok(())
    }
}

pub(self) enum Cmd {
    Upload(String, usize),
    Download(String),
    DownloadDirectly(String),
}

pub(super) struct UnsafePlaceholder<T> {
    value: UnsafeCell<Option<T>>,
}

impl<T> UnsafePlaceholder<T> {
    #[allow(unused)]
    pub fn new() -> Self {
        Self {
            value: UnsafeCell::new(None),
        }
    }

    #[allow(unused)]
    pub fn set(&self, new_value: T) {
        unsafe {
            (&mut (*self.value.get())).replace(new_value);
        }
    }

    #[allow(unused)]
    pub fn get(&self) -> Option<T> {
        unsafe { (&mut (*self.value.get())).take() }
    }
}

unsafe impl<T> Send for UnsafePlaceholder<T> {}

unsafe impl<T> Sync for UnsafePlaceholder<T> {}
