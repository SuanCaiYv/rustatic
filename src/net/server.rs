use std::{
    fs::OpenOptions,
    net::SocketAddr,
    os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    cell::UnsafeCell,
};
use std::io::Write;

use anyhow::anyhow;
use base64::Engine;
use bytes::BytesMut;
use dashmap::DashMap;
use futures::{future::BoxFuture, Future};
use hmac::{Hmac, Mac};
use nix::sys::sendfile;
use sha2::Sha256;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}},
    sync::mpsc,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tracing::error;
use uuid::Uuid;
use crate::db::{get_metadata_ops, get_user_ops};
use crate::db::metadata::Metadata;
use crate::db::user::User;

use crate::pool::ThreadPool;

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
                    let connection_id = data_connection.init().await?;
                    conn_map.insert(connection_id, cmd_tx);

                    if let Err(e) = data_connection.handle().await {
                        error!("data connection handle error: {}", e);
                    }
                    Ok::<(), anyhow::Error>(())
                });
            }
        });

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
    pub(self) fn new(stream: TlsStream<TcpStream>, cmd_map: Arc<DashMap<String, mpsc::Sender<Cmd>>>) -> Self {
        Self { stream, cmd_map }
    }

    pub(self) async fn handle(&mut self) -> anyhow::Result<()> {
        let mut buffer = BytesMut::with_capacity(4096).clone();
        let mut curr_user = None;
        loop {
            let op_code = self.stream.read_u16().await?;
            let req_len = self.stream.read_u16().await?;
            let content = match self
                .stream
                .read_exact(&mut buffer[..req_len as usize])
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
                        self.stream.write_all(e.to_string().as_bytes()).await?;
                        break;
                    }
                    let uuid = Uuid::new_v4().to_string();
                    let engine = base64::engine::GeneralPurpose::new(
                        &base64::alphabet::URL_SAFE,
                        base64::engine::general_purpose::NO_PAD,
                    );
                    let session_id = engine.encode(uuid);
                    self.stream.write_all(session_id.as_bytes()).await?;
                }
                2 => {
                    let params = Self::parse1(content)?;
                    curr_user = Some(params.0.clone());
                    if let Err(e) = Self::login(Self::parse1(content)?).await {
                        error!("login error: {}", e);
                        self.stream.write_all(e.to_string().as_bytes()).await?;
                        break;
                    }
                    let uuid = Uuid::new_v4().to_string();
                    let engine = base64::engine::GeneralPurpose::new(
                        &base64::alphabet::URL_SAFE,
                        base64::engine::general_purpose::NO_PAD,
                    );
                    let session_id = engine.encode(uuid);
                    self.stream.write_all(session_id.as_bytes()).await?;
                }
                3 => {
                    let params = match serde_json::from_slice::<serde_json::Value>(content) {
                        Ok(entry) => {
                            let entry = match entry.as_object() {
                                Some(e) => e,
                                None => {
                                    error!("parse request error: not a json object");
                                    return Err(anyhow!("not a json object"));
                                }
                            };
                            (
                                entry.get("session_id").unwrap().as_str().unwrap().to_owned(),
                                entry.get("filename").unwrap().as_str().unwrap().to_owned(),
                                entry.get("size").unwrap().as_i64().unwrap(),
                                entry.get("create_at").unwrap().as_i64().unwrap().to_owned(),
                                entry.get("update_at").unwrap().as_i64().unwrap().to_owned(),
                            )
                        }
                        Err(e) => {
                            error!("parse request error: {}", e);
                            return Err(anyhow!("parse request error"));
                        }
                    };
                    let uuid = Uuid::new_v4().to_string();
                    let engine = base64::engine::GeneralPurpose::new(
                        &base64::alphabet::URL_SAFE,
                        base64::engine::general_purpose::NO_PAD,
                    );
                    let link = engine.encode(uuid);
                    let (session_id, filename, size, ..) = params;
                    let filepath = format!("/Users/joker/RustProjects/rustatic/data/{}", filename);
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
                        }
                        None => {
                            error!("session id not found");
                            break;
                        }
                    }
                    self.stream.write_all(link.as_bytes()).await?;
                }
                4 => {
                    let params = match serde_json::from_slice::<serde_json::Value>(content) {
                        Ok(entry) => {
                            let entry = match entry.as_object() {
                                Some(e) => e,
                                None => {
                                    error!("parse request error: not a json object");
                                    return Err(anyhow!("not a json object"));
                                }
                            };
                            (
                                entry.get("session_id").unwrap().as_str().unwrap().to_owned(),
                                entry.get("link").unwrap().as_str().unwrap().to_owned(),
                            )
                        }
                        Err(e) => {
                            error!("parse request error: {}", e);
                            return Err(anyhow!("parse request error"));
                        }
                    };
                    let (session_id, link) = params;
                    let metadata = match get_metadata_ops().await.get_by_link(link.clone()).await {
                        Ok(res) => match res {
                            Some(v) => v,
                            None => {
                                error!("metadata not found");
                                break;
                            }
                        }
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
                5 => {}
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
    reader: Option<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    raw_fd: RawFd,
    cmd_rx: mpsc::Receiver<Cmd>,
    thread_pool: Arc<ThreadPool>,
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
            reader: Some(reader),
            writer,
            raw_fd,
            cmd_rx,
            thread_pool,
        }
    }

    pub(self) async fn init(&self) -> anyhow::Result<String> {
        Ok("".to_owned())
    }

    pub(self) async fn handle(&mut self) -> anyhow::Result<()> {
        loop {
            match self.cmd_rx.recv().await {
                Some(cmd) => match cmd {
                    Cmd::Upload(filename, size) => self.upload(filename.as_str(), size).await?,
                    Cmd::Download(filename) => self.download(filename.as_str()).await?,
                },
                None => break,
            }
        }
        Ok(())
    }

    pub(self) async fn upload(&mut self, filepath: &str, mut size: usize) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel(1);
        let mut reader = self.reader.take().unwrap();
        let handle = tokio::spawn(async move {
            // 16KB as `page size` is large enough for most cases.
            let buffer1 = BytesMut::with_capacity(4096 * 4);
            let buffer2 = BytesMut::with_capacity(4096 * 4);
            let mut buffer = buffer1.clone();
            let mut flag = false;
            while size > 0 {
                let n = match reader.read(&mut buffer).await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("read content error: {}", e);
                        break;
                    }
                };
                if n == 0 {
                    break;
                }
                size -= n;
                if let Err(e) = tx.send((buffer.clone(), n)).await {
                    error!("disk operation error: {}", e);
                    break;
                }
                // to handle channel cache.
                buffer = if flag {
                    buffer1.clone()
                } else {
                    buffer2.clone()
                };
                flag = !flag;
            }
            reader
        });
        Upload {
            filepath: Some(filepath.to_owned()),
            content_rx: Some(rx),
            thread_pool: Some(self.thread_pool.clone()),
            send_future: None,
            sent: false,
            task_status: Arc::new(AtomicBool::new(false)),
        }.await?;
        // set back reader.
        self.reader.replace(handle.await.unwrap());
        Ok(())
    }

    pub(self) async fn download(&self, filepath: &str) -> anyhow::Result<()> {
        Download {
            filepath: Some(filepath.to_owned()),
            socket_fd: UnsafeFD {
                fd: self.raw_fd,
            },
            thread_pool: Some(self.thread_pool.clone()),
            send_future: None,
            sent: false,
            task_status: Arc::new(AtomicBool::new(false)),
        }
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

#[derive(Clone, Copy)]
pub(self) struct UnsafeFD {
    fd: RawFd,
}

impl AsFd for UnsafeFD {
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}

pub(self) enum Cmd {
    Upload(String, usize),
    Download(String),
}

pub(super) struct UnsafePlaceholder<T> {
    value: UnsafeCell<Option<T>>,
}

impl<T> UnsafePlaceholder<T> {
    pub fn new() -> Self {
        Self {
            value: UnsafeCell::new(None),
        }
    }

    pub fn set(&self, new_value: T) {
        unsafe {
            (&mut (*self.value.get())).replace(new_value);
        }
    }

    pub fn get(&self) -> Option<T> {
        unsafe { (&mut (*self.value.get())).take() }
    }
}

unsafe impl<T> Send for UnsafePlaceholder<T> {}

unsafe impl<T> Sync for UnsafePlaceholder<T> {}

pub(self) struct Upload {
    filepath: Option<String>,
    content_rx: Option<mpsc::Receiver<(BytesMut, usize)>>,
    thread_pool: Option<Arc<ThreadPool>>,
    send_future: Option<BoxFuture<'static, anyhow::Result<()>>>,
    sent: bool,
    task_status: Arc<AtomicBool>,
}

impl Future for Upload {
    type Output = anyhow::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.send_future.is_none() {
            let waker = cx.waker().clone();
            let status = self.task_status.clone();
            let thread_pool = self.thread_pool.take().unwrap();
            let filepath = self.filepath.take().unwrap();
            let mut content_rx = self.content_rx.take().unwrap();
            let future = async move {
                thread_pool
                    .execute_async(
                        move || {
                            _ = std::fs::create_dir_all(std::path::Path::new(filepath.as_str())
                                .parent()
                                .unwrap()
                                .to_str()
                                .unwrap()
                            );
                            let mut file = OpenOptions::new()
                                .write(true)
                                .append(true)
                                .open(filepath.as_str())
                                .unwrap();
                            loop {
                                match content_rx.blocking_recv() {
                                    Some((content, n)) => {
                                        if let Err(e) = file.write_all(&content[..n]) {
                                            error!("write content error: {}", e);
                                            break;
                                        }
                                    }
                                    None => {
                                        break;
                                    }
                                }
                            }
                        },
                        move || {
                            status.store(true, Ordering::Release);
                            waker.wake();
                        },
                    )
                    .await
            };
            self.send_future = Some(Box::pin(future));
        }
        if !self.sent {
            match self.send_future.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready(Ok(_)) => {
                    self.sent = true;
                }
                Poll::Ready(Err(e)) => {
                    error!("download error: {}", e);
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        if self.task_status.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

pub(self) struct Download {
    filepath: Option<String>,
    socket_fd: UnsafeFD,
    thread_pool: Option<Arc<ThreadPool>>,
    send_future: Option<BoxFuture<'static, anyhow::Result<()>>>,
    sent: bool,
    task_status: Arc<AtomicBool>,
}

impl Unpin for Download {}

impl Future for Download {
    type Output = anyhow::Result<()>;

    fn poll(mut self: Pin<&mut Download>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.send_future.is_none() {
            let waker = cx.waker().clone();
            let status = self.task_status.clone();
            let thread_pool = self.thread_pool.take().unwrap();
            let filepath = self.filepath.take().unwrap();
            let socket_fd = self.socket_fd;
            let future = async move {
                thread_pool
                    .execute_async(
                        move || {
                            let file = OpenOptions::new()
                                .read(true)
                                .open(filepath.as_str())
                                .unwrap();
                            let mut size = file.metadata().unwrap().len() as i64;
                            let file_fd = file.as_fd();
                            let mut offset = 0;
                            loop {
                                let (res, n) = sendfile::sendfile(
                                    file_fd,
                                    socket_fd,
                                    offset,
                                    Some(size),
                                    None,
                                    None,
                                );
                                if res.is_err() {
                                    error!("download error: {}", res.unwrap_err());
                                    break;
                                }
                                if size == 0 {
                                    break;
                                }
                                offset += n;
                                size -= n;
                            }
                        },
                        move || {
                            status.store(true, Ordering::Release);
                            waker.wake();
                        },
                    )
                    .await
            };
            self.send_future = Some(Box::pin(future));
        }
        if !self.sent {
            match self.send_future.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready(Ok(_)) => {
                    self.sent = true;
                }
                Poll::Ready(Err(e)) => {
                    error!("download error: {}", e);
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        if self.task_status.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}