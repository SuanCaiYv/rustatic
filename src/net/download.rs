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
                    let n = {
                        let mut offset0 = idx as i64;
                        let res = sendfile::sendfile(socket_fd, file_fd, Some(&mut offset0), 4096 * 4);
                        match res {
                            Ok(n_sent) => n_sent,
                            Err(e) => {
                                if e == nix::errno::Errno::EAGAIN {
                                    return ThreadPoolResult::Usize(0);
                                }
                                error!("failed to sendfile: {}", e);
                                return ThreadPoolResult::Err(anyhow!("failed to sendfile: {}", e));
                            }
                        }
                    };
                    #[cfg(target_os = "macos")]
                    let n = {
                        let (res, n) = sendfile::sendfile(
                            file_fd,
                            socket_fd,
                            idx as i64,
                            Some(4096 * 4),
                            None,
                            None,
                        );
                        if let Err(e) = res {
                            if e == nix::errno::Errno::EAGAIN {
                                // a bug on macos while using with writable()
                                if n == 0 {
                                    return ThreadPoolResult::Usize(0);
                                }
                            } else {
                                error!("failed to sendfile: {}", e);
                                return ThreadPoolResult::Err(anyhow!("failed to sendfile: {}", e));
                            }
                        }
                        n as usize
                    };
                    return ThreadPoolResult::Usize(n);
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
