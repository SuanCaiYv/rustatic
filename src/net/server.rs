use std::{
    cell::UnsafeCell,
    net::SocketAddr,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::anyhow;
use base64::Engine;
use byteorder::ByteOrder;
use coordinator::pool::automatic::Submitter;
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{
    db::{get_metadata_ops, get_user_ops, metadata::Metadata, user::User},
    net::rename::Rename,
};

use super::{download::Download, upload::Upload};

pub(self) const ALPN_RUSTATIC: &[&[u8]] = &[b"rustatic"];
pub(self) const SPLIT_FIELD_LENGTH: usize = 2;

type HmacSha256 = Hmac<Sha256>;

pub(crate) struct ServerConfig {
    pub(crate) data_port: u16,
    pub(crate) ctrl_port: u16,
    pub(crate) cert: rustls::Certificate,
    pub(crate) key: rustls::PrivateKey,
    pub(crate) root_folder: String,
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
            root_folder,
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
            let thread_pool = coordinator::pool::Builder::new().build();
            loop {
                let (data_stream, _) = match data_listener.accept().await {
                    Ok(x) => x,
                    Err(e) => {
                        error!("data listener accept error: {}", e);
                        continue;
                    }
                };

                let conn_map = conn_map.clone();
                let submitter = thread_pool.new_submitter();
                tokio::spawn(async move {
                    let (cmd_tx, cmd_rx) = mpsc::channel(32);
                    let mut data_connection = DataConnection::new(data_stream, cmd_rx, submitter);
                    let session_id = data_connection.init().await;
                    if session_id.is_err() {
                        error!("data connection init error: {}", session_id.err().unwrap());
                        return Err(anyhow!("data connection init error"));
                    }
                    let session_id = session_id.unwrap();
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
        let root_folder: &'static str = Box::leak(root_folder.into_boxed_str());
        while let Ok((stream, _)) = ctrl_listener.accept().await {
            let stream = acceptor.accept(stream).await;
            if stream.is_err() {
                error!("tls accept error: {}", stream.err().unwrap());
                continue;
            }
            let stream = stream.unwrap();
            let mut request = Request::new(stream, data_conn_map.clone(), root_folder);
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
    root_folder: &'static str,
}

impl Request {
    pub(self) fn new(
        stream: TlsStream<TcpStream>,
        cmd_map: Arc<DashMap<String, mpsc::Sender<Cmd>>>,
        root_folder: &'static str,
    ) -> Self {
        Self {
            stream,
            cmd_map,
            root_folder,
        }
    }

    pub(self) async fn handle(&mut self) -> anyhow::Result<()> {
        let buffer = UnsafeBuffer::new(4096);
        let mut curr_user: Option<String> = None;
        loop {
            let (op_code, req) = match self.read_req(&buffer).await {
                Ok(res) => res,
                Err(_e) => {
                    break;
                }
            };
            match op_code {
                // sign
                1 => {
                    let username = String::from_utf8_lossy(req[0]).to_string();
                    let password = String::from_utf8_lossy(req[1]).to_string();
                    curr_user = Some(username.clone());
                    if let Err(e) = Self::sign(&username, &password).await {
                        error!("sign error: {}", e);
                        self.write_resp(Some(e.to_string().as_str()), vec![])
                            .await?;
                    }
                    let uuid = Uuid::new_v4().to_string();
                    let engine = base64::engine::GeneralPurpose::new(
                        &base64::alphabet::URL_SAFE,
                        base64::engine::general_purpose::NO_PAD,
                    );
                    let session_id = engine.encode(uuid);
                    self.write_resp(None, vec![session_id.as_bytes()]).await?;
                }
                // login
                2 => {
                    let username = String::from_utf8_lossy(req[0]).to_string();
                    let password = String::from_utf8_lossy(req[1]).to_string();
                    // check for allow multiple login to enable more streams with purpose of concurrent transit.
                    if let Some(ref curr_user) = curr_user {
                        if curr_user != &username {
                            error!("user not match");
                            self.write_resp(Some("user not match"), vec![]).await?;
                            continue;
                        }
                    } else {
                        curr_user = Some(username.clone());
                    }
                    if let Err(e) = Self::login(&username, &password).await {
                        error!("login error: {}", e);
                        self.write_resp(Some(e.to_string().as_str()), vec![])
                            .await?;
                        continue;
                    }
                    let uuid = Uuid::new_v4().to_string();
                    let engine = base64::engine::GeneralPurpose::new(
                        &base64::alphabet::URL_SAFE,
                        base64::engine::general_purpose::NO_PAD,
                    );
                    let session_id = engine.encode(uuid);
                    self.write_resp(None, vec![session_id.as_bytes()]).await?;
                }
                // upload
                3 => {
                    let session_id = String::from_utf8_lossy(req[0]).to_string();
                    let filename = String::from_utf8_lossy(req[1]).to_string();
                    let size = byteorder::BigEndian::read_i64(req[3]);
                    let _create_at = byteorder::BigEndian::read_i64(req[4]);
                    let _update_at = byteorder::BigEndian::read_i64(req[5]);
                    let uuid = Uuid::new_v4().to_string();
                    let engine = base64::engine::GeneralPurpose::new(
                        &base64::alphabet::URL_SAFE,
                        base64::engine::general_purpose::NO_PAD,
                    );
                    let link = engine.encode(uuid);
                    let filepath = format!(
                        "{}/{}/{}",
                        self.root_folder,
                        curr_user.as_ref().unwrap(),
                        filename
                    );
                    let mut tag = 0;
                    if let Ok(same_name_record) = get_metadata_ops()
                        .await
                        .get_by_owner_filename(curr_user.clone().unwrap(), filename.clone())
                        .await
                    {
                        if let Some(record) = same_name_record {
                            tag = record.duplication + 1;
                        }
                    }
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
                        duplication: tag,
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
                    // to handle damn stupid lifetime check.
                    let mut flag = false;
                    match self.cmd_map.get(session_id.as_str()) {
                        Some(cmd_tx) => {
                            cmd_tx.send(Cmd::Upload(filepath, size as usize)).await?;
                            flag = true;
                        }
                        None => {
                            error!("session id not found");
                        }
                    }
                    if flag {
                        self.write_resp(None, vec![link.as_bytes()]).await?;
                    } else {
                        self.write_resp(Some("bad session_id"), vec![]).await?;
                    }
                }
                // download
                4 => {
                    let session_id = String::from_utf8_lossy(req[0]).to_string();
                    let link = String::from_utf8_lossy(req[1]).to_string();
                    let metadata = match get_metadata_ops().await.get_by_link(link.clone()).await {
                        Ok(res) => match res {
                            Some(v) => v,
                            None => {
                                error!("metadata not found");
                                self.write_resp(Some("file not found"), vec![]).await?;
                                continue;
                            }
                        },
                        Err(e) => {
                            error!("get metadata error: {}", e);
                            break;
                        }
                    };
                    let mut flag = false;
                    match self.cmd_map.get(session_id.as_str()) {
                        Some(cmd_tx) => {
                            cmd_tx.send(Cmd::Download(metadata.filepath)).await?;
                            flag = true;
                        }
                        None => {
                            error!("session id not found");
                        }
                    }
                    if flag {
                        let mut data = [0; 8];
                        byteorder::BigEndian::write_i64(&mut data, metadata.size);
                        self.write_resp(None, vec![metadata.filename.as_bytes(), &data[..]])
                            .await?;
                    } else {
                        self.write_resp(Some("bad session_id"), vec![]).await?;
                    }
                }
                // download directly by stream transmit
                5 => {
                    let session_id = String::from_utf8_lossy(req[0]).to_string();
                    let link = String::from_utf8_lossy(req[1]).to_string();
                    let metadata = match get_metadata_ops().await.get_by_link(link.clone()).await {
                        Ok(res) => match res {
                            Some(v) => v,
                            None => {
                                error!("metadata not found");
                                self.write_resp(Some("file not found"), vec![]).await?;
                                continue;
                            }
                        },
                        Err(e) => {
                            error!("get metadata error: {}", e);
                            break;
                        }
                    };
                    let mut flag = false;
                    match self.cmd_map.get(session_id.as_str()) {
                        Some(cmd_tx) => {
                            cmd_tx
                                .send(Cmd::DownloadDirectly(
                                    metadata.filepath,
                                    metadata.size as usize,
                                ))
                                .await?;
                            flag = true;
                        }
                        None => {
                            error!("session id not found");
                        }
                    }
                    if flag {
                        let mut data = [0; 8];
                        byteorder::BigEndian::write_i64(&mut data, metadata.size);
                        self.write_resp(None, vec![metadata.filename.as_bytes(), &data[..]])
                            .await?;
                    } else {
                        self.write_resp(Some("bad session_id"), vec![]).await?;
                    }
                }
                // list files
                6 => {
                    let username = String::from_utf8_lossy(req[0]).to_string();
                    match get_metadata_ops()
                        .await
                        .list_by_owner(username.to_string())
                        .await
                    {
                        Ok(res) => {
                            let buf = UnsafeBuffer::new(8 * 5 * res.len());
                            let mut idx = 0;
                            let mut data = vec![];
                            for m in res.iter() {
                                data.push(m.filename.as_bytes());
                                byteorder::BigEndian::write_i64(
                                    buf.get_mut_slice(idx..idx + 8),
                                    m.size,
                                );
                                data.push(buf.get_slice(idx..idx + 8));
                                idx += 8;
                                byteorder::BigEndian::write_i64(
                                    buf.get_mut_slice(idx..idx + 8),
                                    m.duplication,
                                );
                                data.push(buf.get_slice(idx..idx + 8));
                                idx += 8;
                                byteorder::BigEndian::write_i64(
                                    buf.get_mut_slice(idx..idx + 8),
                                    m.create_time,
                                );
                                data.push(buf.get_slice(idx..idx + 8));
                                idx += 8;
                                byteorder::BigEndian::write_i64(
                                    buf.get_mut_slice(idx..idx + 8),
                                    m.update_time,
                                );
                                data.push(buf.get_slice(idx..idx + 8));
                                idx += 8;
                                byteorder::BigEndian::write_i64(
                                    buf.get_mut_slice(idx..idx + 8),
                                    m.delete_time,
                                );
                                data.push(buf.get_slice(idx..idx + 8));
                                idx += 8;
                                data.push(m.link.as_bytes());
                            }
                            self.write_resp(None, data).await?;
                        }
                        Err(e) => {
                            error!("list metadata error: {}", e);
                            self.write_resp(Some("list files failed"), vec![]).await?;
                            break;
                        }
                    }
                }
                // rename file
                7 => {
                    let session_id = String::from_utf8_lossy(req[0]).to_string();
                    let link = String::from_utf8_lossy(req[1]).to_string();
                    let new_name = String::from_utf8_lossy(req[2]).to_string();
                    let mut metadata =
                        match get_metadata_ops().await.get_by_link(link.clone()).await {
                            Ok(res) => match res {
                                Some(v) => v,
                                None => {
                                    error!("metadata not found");
                                    self.write_resp(Some("file not found"), vec![]).await?;
                                    continue;
                                }
                            },
                            Err(e) => {
                                error!("get metadata error: {}", e);
                                break;
                            }
                        };
                    match get_metadata_ops()
                        .await
                        .get_by_owner_filename(curr_user.clone().unwrap(), new_name.clone())
                        .await
                    {
                        Ok(res) => match res {
                            Some(_v) => {
                                error!("same file name already existed");
                                self.write_resp(
                                    Some("same name for new filename already existed"),
                                    vec![],
                                )
                                .await?;
                                continue;
                            }
                            None => {}
                        },
                        Err(e) => {
                            error!("get metadata error: {}", e);
                            break;
                        }
                    };
                    let path = Path::new(metadata.filepath.as_str());
                    let dir_path = path.parent().unwrap_or_else(|| Path::new(""));
                    let new_filepath = dir_path
                        .join(new_name.clone())
                        .to_str()
                        .unwrap()
                        .to_string();

                    let mut flag = false;
                    match self.cmd_map.get(session_id.as_str()) {
                        Some(cmd_tx) => {
                            cmd_tx
                                .send(Cmd::Rename(metadata.filepath, new_filepath.clone()))
                                .await?;
                            flag = true;
                        }
                        None => {
                            error!("session id not found");
                        }
                    }
                    if flag {
                        metadata.filename = new_name;
                        metadata.filepath = new_filepath;
                        get_metadata_ops().await.update(metadata).await?;
                        self.write_resp(None, vec![]).await?;
                    } else {
                        self.write_resp(Some("bad session_id"), vec![]).await?;
                    }
                }
                // delete file(hide file for 30 days)
                8 => {}
                // restore file
                9 => {}
                // confirm delete file
                10 => {}
                _ => {
                    error!("unknown op code: {}", op_code);
                    break;
                }
            }
        }
        Ok(())
    }

    pub(self) async fn write_resp(
        &mut self,
        e: Option<&str>,
        resp: Vec<&[u8]>,
    ) -> anyhow::Result<()> {
        let mut len = 0;
        for r in resp.iter() {
            len += 2;
            len += r.len();
        }
        if let Some(e) = e {
            len += 2; // err field encode length
            len += 3; // `err` length
            len += 2; // err_str field encode length
            len += e.as_bytes().len(); // `err_str`` length
            self.stream.write_u16(len as u16).await?;
            self.stream.write_u16(3).await?; // length of `err`
            self.stream
                .write_all(['e' as u8, 'r' as u8, 'r' as u8].as_slice())
                .await?;
            self.stream.write_u16(e.as_bytes().len() as u16).await?;
            self.stream.write_all(e.as_bytes()).await?;
        } else {
            len += 2; // ok field encode length
            len += 2; // `ok` length
            self.stream.write_u16(len as u16).await?;
            self.stream.write_u16(2).await?; // length of `ok`
            self.stream
                .write_all(['o' as u8, 'k' as u8].as_slice())
                .await?;
        }
        for r in resp.iter() {
            self.stream.write_u16(r.len() as u16).await?;
            self.stream.write_all(&r[..]).await?;
        }
        return Ok(());
    }

    pub(self) async fn read_req<'a>(
        &mut self,
        buffer: &'a UnsafeBuffer,
    ) -> anyhow::Result<(u16, Vec<&'a [u8]>)> {
        let op_code = match self.stream.read_u16().await {
            Ok(code) => code,
            Err(_e) => {
                debug!("connection closed");
                return Err(anyhow!(""));
            }
        };
        let mut len = self.stream.read_u16().await? as usize;

        let mut res = Vec::new();
        let mut idx = 0;
        while len > 0 {
            let size = self.stream.read_u16().await? as usize;
            let data = buffer.get_mut_slice(idx..idx + size);
            self.stream.read_exact(data).await?;
            res.push(buffer.get_slice(idx..idx + size));
            idx += size;
            len -= SPLIT_FIELD_LENGTH + size;
        }
        return Ok((op_code, res));
    }

    pub(self) async fn sign(username: &str, password: &str) -> anyhow::Result<()> {
        if get_user_ops()
            .await
            .get(username.to_owned())
            .await?
            .is_some()
        {
            return Err(anyhow!("user already exists"));
        }
        let user_salt = Self::salt(12);
        let mut mac: HmacSha256 = HmacSha256::new_from_slice(user_salt.as_bytes()).unwrap();
        mac.update(password.as_bytes());
        let res = mac.finalize().into_bytes();
        let res_str = format!("{:X}", res);
        let user = User {
            id: 0,
            username: username.to_owned(),
            password: res_str,
            salt: user_salt,
            create_time: chrono::Local::now().timestamp_millis(),
            update_time: chrono::Local::now().timestamp_millis(),
            delete_time: 0,
        };
        get_user_ops().await.insert(user).await?;
        Ok(())
    }

    pub(self) async fn login(username: &str, password: &str) -> anyhow::Result<()> {
        let user = match get_user_ops().await.get(username.to_owned()).await? {
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

pub(super) enum ThreadPoolResult {
    Usize(usize),
    None,
    Err(anyhow::Error),
}

pub(self) struct DataConnection {
    stream: TcpStream,
    cmd_rx: mpsc::Receiver<Cmd>,
    submitter: Submitter<ThreadPoolResult>,
    session_id: String,
}

impl DataConnection {
    pub(self) fn new(
        stream: TcpStream,
        cmd_rx: mpsc::Receiver<Cmd>,
        submitter: Submitter<ThreadPoolResult>,
    ) -> Self {
        Self {
            stream,
            cmd_rx,
            submitter,
            session_id: String::new(),
        }
    }

    pub(self) async fn init(&mut self) -> anyhow::Result<String> {
        let mut buf = [0u8; 256];
        let mut idx = 0;
        loop {
            let n = self.stream.read(&mut buf[idx..]).await?;
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
                    Cmd::Upload(filepath, size) => self.upload(filepath, size).await?,
                    Cmd::Download(filepath) => self.download(filepath).await?,
                    Cmd::DownloadDirectly(filepath, size) => {
                        self.download_directly(filepath, size).await?
                    }
                    Cmd::Rename(filepath, new_name) => self.rename(filepath, new_name).await?,
                    Cmd::DeleteFalsely(filepath) => self.delete_falsely(filepath.as_str()).await?,
                    Cmd::DeleteImmediately(filepath) => {
                        self.delete_immediately(filepath.as_str()).await?
                    }
                },
                None => break,
            }
        }
        Ok(())
    }

    pub(self) async fn upload(&mut self, filepath: String, size: usize) -> anyhow::Result<()> {
        Upload::new(
            size,
            filepath.to_owned(),
            self.submitter.clone(),
            &mut self.stream,
        )
        .run()
        .await?;
        Ok(())
    }

    pub(self) async fn download(&mut self, filepath: String) -> anyhow::Result<()> {
        Download::new(
            filepath.to_owned(),
            self.submitter.clone(),
            &mut self.stream,
        )
        .run()
        .await?;
        Ok(())
    }

    pub(self) async fn download_directly(
        &mut self,
        filepath: String,
        size: usize,
    ) -> anyhow::Result<()> {
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .open(filepath)
            .await
            .unwrap();
        let mut buffer = Vec::with_capacity(1024 * 1024 * 8);
        unsafe { buffer.set_len(1024 * 1024 * 8) };
        let buffer = buffer.as_mut_slice();
        let mut total = 0;
        loop {
            let n = file.read(&mut buffer[..]).await?;
            total += n;
            self.stream.write_all(&buffer[..n]).await?;
            if total == size {
                break;
            }
        }
        Ok(())
    }

    pub(self) async fn rename(&mut self, filepath: String, new_name: String) -> anyhow::Result<()> {
        Rename::new(filepath, new_name, self.submitter.clone())
            .run()
            .await
    }

    pub(self) async fn delete_falsely(&mut self, filepath: &str) -> anyhow::Result<()> {
        todo!("")
    }

    pub(self) async fn delete_immediately(&mut self, filepath: &str) -> anyhow::Result<()> {
        todo!("")
    }
}

pub(self) enum Cmd {
    Upload(String, usize),
    Download(String),
    DownloadDirectly(String, usize),
    Rename(String, String),
    DeleteFalsely(String),
    DeleteImmediately(String),
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

pub(self) struct UnsafeBuffer {
    data: Vec<u8>,
}

impl UnsafeBuffer {
    pub(self) fn new(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(capacity);
        unsafe {
            data.set_len(capacity);
        }
        UnsafeBuffer { data }
    }

    pub(self) fn get_mut_slice(&self, index: Range<usize>) -> &mut [u8] {
        let range = index.start..index.end;
        unsafe {
            std::slice::from_raw_parts_mut(
                self.data.as_ptr().add(index.start) as *mut _,
                range.len(),
            )
        }
    }

    pub(self) fn get_slice(&self, index: Range<usize>) -> &[u8] {
        let range = index.start..index.end;
        unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(index.start), range.len()) }
    }
}
