use std::future::Future;
use bytes::BytesMut;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Poll};
use anyhow::anyhow;
use base64::Engine;
use futures::future::LocalBoxFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tracing::error;
use uuid::Uuid;
use crate::pool::ThreadPool;

pub(self) const ALPN_RUSTATIC: &[&[u8]] = &[b"rustatic"];

pub(crate) struct ServerConfig {
    data_port: u16,
    ctrl_port: u16,
    cert: rustls::Certificate,
    key: rustls::PrivateKey,
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
            loop {
                let (data_stream, _) = match data_listener.accept().await {
                    Ok(x) => x,
                    Err(e) => {
                        error!("data listener accept error: {}", e);
                        continue;
                    }
                };

                let conn_map = conn_map.clone();
                tokio::spawn(async move {
                    let (cmd_tx, cmd_rx) = mpsc::channel(32);
                    let mut data_connection = DataConnection::new(data_stream, cmd_rx);
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
            let mut request = Request::new(stream);
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
}

impl Request {
    pub(self) fn new(stream: TlsStream<TcpStream>) -> Self {
        Self { stream }
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
                3 => {}
                4 => {}
                _ => {
                    error!("unknown op code: {}", op_code);
                    break;
                }
            }
        }
        Ok(())
    }

    fn parse1(req: &[u8]) -> anyhow::Result<(String, String)> {
        let (username, password) =
            match serde_json::from_slice::<serde_json::Value>(req) {
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
        Ok(())
    }

    pub(self) async fn login(params: (String, String)) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(self) struct DataConnection<'a> {
    stream: TcpStream,
    cmd_rx: mpsc::Receiver<Cmd<'a>>,
    thread_pool: Arc<ThreadPool>,
}

impl<'a> DataConnection<'a> {
    pub(self) fn new(stream: TcpStream, cmd_rx: mpsc::Receiver<Cmd<'a>>, thread_pool: Arc<ThreadPool>) -> Self {
        Self { stream, cmd_rx, thread_pool }
    }

    pub(self) async fn init(&self) -> anyhow::Result<String> {
        Ok("".to_owned())
    }

    pub(self) async fn handle(&mut self) -> anyhow::Result<()> {
        loop {
            match self.cmd_rx.recv().await {
                Some(cmd) => match cmd {
                    Cmd::Upload(req) => self.upload().await?,
                    Cmd::Download(req) => self.download().await?,
                },
                None => break,
            }
        }
        Ok(())
    }

    pub(self) async fn upload(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub(self) async fn download(&self, filepath: &str) -> anyhow::Result<()> {
        self.thread_pool.execute_async(|| {}, || {}).await?;
        Ok(())
    }
}

pub(self) enum Cmd<'a> {
    Upload(&'a [u8]),
    Download(&'a [u8]),
}

pub(self) struct Download {
    filepath: String,
    socket_fd: RawFd,
    thread_pool: Arc<ThreadPool>,
    send_future: Option<LocalBoxFuture<'static, anyhow::Result<()>>>,
    io_task_status: Arc<AtomicBool>,
}

impl Unpin for Download {}

impl Future for Download {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = cx.waker().clone();
        let mut future = self.thread_pool.execute_async(|| {}, || {});
        match future.as_mut().poll(cx) {}
        Poll::Pending
    }
}
