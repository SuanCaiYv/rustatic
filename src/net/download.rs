use std::{fs::OpenOptions, os::fd::AsRawFd, sync::Arc};

use anyhow::anyhow;
use nix::sys::sendfile;
use tokio::net::TcpStream;
use tracing::error;

use crate::{net::UnsafeFD, pool::ThreadPool};

use super::{server::ThreadPoolResult, DirectStreamWriter};

pub(super) struct Download<'a> {
    filepath: String,
    thread_pool: Arc<ThreadPool<ThreadPoolResult>>,
    write_stream: DirectStreamWriter<'a>,
}

impl<'a> Download<'a> {
    pub(super) fn new(
        filepath: String,
        thread_pool: Arc<ThreadPool<ThreadPoolResult>>,
        write_stream: &'a TcpStream,
    ) -> Self {
        Self {
            filepath,
            thread_pool,
            write_stream: DirectStreamWriter {
                stream: write_stream,
            },
        }
    }

    pub(super) async fn run(&mut self) -> anyhow::Result<()> {
        let file = OpenOptions::new().read(true).open(self.filepath.as_str())?;
        let size: usize = file.metadata()?.len() as usize;
        let file_fd = UnsafeFD {
            fd: file.as_raw_fd(),
        };
        let mut idx = 0;
        while idx < size {
            let socket_fd = self.write_stream.await?;
            match self
                .thread_pool
                .execute_async(move || {
                    #[cfg(target_os = "linux")]
                    {
                        let mut offset0 = idx as i64;
                        loop {
                            let res = sendfile::sendfile(socket_fd, file_fd, Some(&mut offset0), 1024 * 1024 * 4);
                            if res.is_err() {
                                if res.unwrap_err() == nix::errno::Errno::EAGAIN {
                                    continue;
                                } else {
                                    error!("failed to sendfile: {}", res.unwrap_err());
                                    return ThreadPoolResult::Err(anyhow!("failed to sendfile: {}", res.unwrap_err()));
                                }
                            }
                            let n_sent = res.unwrap();
                            return ThreadPoolResult::Usize(n_sent);
                        }
                    }
                    #[cfg(target_os = "macos")]
                    loop {
                        let (res, n) = sendfile::sendfile(
                            file_fd,
                            socket_fd,
                            idx as i64,
                            None,
                            None,
                            None,
                        );
                        if let Err(e) = res {
                            // `EAGAIN` same as `WouldBlock`
                            // resource temporary unavailable, retry
                            if e == nix::errno::Errno::EAGAIN {
                                // same as front
                                if n == 0 {
                                    continue;
                                } else {
                                    println!("sent: {}", n);
                                    // macos return `EAGAIN` doesn't mean 0 bytes sent
                                    return ThreadPoolResult::Usize(n as usize);
                                }
                            } else {
                                error!("failed to sendfile: {}", e);
                                return ThreadPoolResult::Err(anyhow!("failed to sendfile: {}", e));
                            }
                        } else {
                            println!("sent: {}", n);
                            return ThreadPoolResult::Usize(n as usize);
                        }
                    }
                })
                .await?
                .await?
            {
                ThreadPoolResult::Usize(n) => {
                    idx += n;
                }
                ThreadPoolResult::Err(e) => {
                    return Err(e);
                }
                ThreadPoolResult::None => {
                    break;
                }
            }
        }
        Ok(())
    }
}
