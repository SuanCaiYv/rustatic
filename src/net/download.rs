use std::{fs::OpenOptions, os::fd::AsRawFd};

use anyhow::anyhow;
use coordinator::pool::automatic::Submitter;
use nix::sys::sendfile;
use tokio::net::TcpStream;
use tracing::error;

use crate::net::UnsafeFD;

use super::{server::ThreadPoolResult, DirectStreamWriter};

pub(super) struct Download<'a> {
    filepath: String,
    submitter: Submitter<ThreadPoolResult>,
    write_stream: DirectStreamWriter<'a>,
}

impl<'a> Download<'a> {
    pub(super) fn new(
        filepath: String,
        submitter: Submitter<ThreadPoolResult>,
        write_stream: &'a TcpStream,
    ) -> Self {
        Self {
            filepath,
            submitter,
            write_stream: DirectStreamWriter {
                stream: write_stream,
            },
        }
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        let file = OpenOptions::new().read(true).open(self.filepath.as_str())?;
        let size: usize = file.metadata()?.len() as usize;
        let file_fd = UnsafeFD {
            fd: file.as_raw_fd(),
        };
        let mut idx = 0;
        while idx < size {
            let socket_fd = self.write_stream.await?;
            match self
                .submitter
                .submit(move || {
                    #[cfg(target_os = "linux")]
                    {
                        let mut offset0 = idx as i64;
                        loop {
                            let res = sendfile::sendfile(socket_fd, file_fd, Some(&mut offset0), 1024 * 1024 * 4);
                            if res.is_err() {
                                // `EAGAIN` same as `WouldBlock`.
                                // Means resource temporary unavailable, retry again.
                                //
                                // The fake positive generated from epoll will squander thread schedule
                                // of thread-pool.
                                //
                                // One possible reason of this scenario is that peer's buffer is full,
                                // but target socket buffer is available. So epoll return `EPOLLOUT` event, but
                                // we can't send any data to peer.
                                if res.unwrap_err() == nix::errno::Errno::EAGAIN {
                                    return ThreadPoolResult::Usize(0);
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
                            // `EAGAIN` same as `WouldBlock`.
                            // Means resource temporary unavailable, retry again.
                            //
                            // The fake positive generated from epoll will squander thread schedule
                            // of thread-pool.
                            //
                            // One possible reason of this scenario is that peer's buffer is full,
                            // but target socket buffer is available. So epoll return `EPOLLOUT` event, but
                            // we can't send any data to peer.
                            if e == nix::errno::Errno::EAGAIN {
                                // same as front
                                if n == 0 {
                                    return ThreadPoolResult::Usize(0);
                                } else {
                                    // println!("sent: {}", n);
                                    // macos return `EAGAIN` doesn't mean 0 bytes sent
                                    return ThreadPoolResult::Usize(n as usize);
                                }
                            } else {
                                error!("failed to sendfile: {}", e);
                                return ThreadPoolResult::Err(anyhow!("failed to sendfile: {}", e));
                            }
                        } else {
                            // println!("sent: {}", n);
                            return ThreadPoolResult::Usize(n as usize);
                        }
                    }
                }).await
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
