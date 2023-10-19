use std::{
    fs::{File, OpenOptions},
    num::NonZeroUsize,
    os::fd::AsRawFd,
    sync::Arc,
};

use anyhow::anyhow;
use nix::{
    libc,
    sys::{
        mman::{
            self, {MapFlags, ProtFlags},
        },
        sendfile,
    },
};
use tokio::{net::TcpStream, sync::mpsc};
use tracing::error;

use crate::{net::UnsafeFD, pool::ThreadPool};

use super::DirectStreamWriter;

pub(super) struct Download<'a> {
    filepath: String,
    thread_pool: Arc<ThreadPool>,
    write_stream: DirectStreamWriter<'a>,
}

impl<'a> Download<'a> {
    pub(super) fn new(
        filepath: String,
        thread_pool: Arc<ThreadPool>,
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
        let (content_tx, mut content_rx) = mpsc::channel(64);
        let mut download_helper = DownloadHelper::new(self.filepath.clone(), 4096 * 64, content_tx);
        self.thread_pool
            .execute_async(
                move || {
                    _ = download_helper.run();
                },
                || {},
            )
            .await?;
        while let Some(entry) = content_rx.recv().await {
            let mut preload = match entry {
                PreloadOrFile::P(preload) => preload,
                PreloadOrFile::F(_file) => {
                    break;
                }
            };
            let mut preload_size = preload.size;
            while preload_size > 0 {
                let socket_fd = self.write_stream.await?;
                #[cfg(target_os = "linux")]
                let n = {
                    let mut offset0 = preload.offset as i64;
                    let res =
                        sendfile::sendfile(socket_fd, preload.fd, Some(&mut offset0), preload_size);
                    match res {
                        Ok(n_sent) => n_sent,
                        Err(e) => {
                            if e == nix::errno::Errno::EAGAIN {
                                continue;
                            }
                            error!("failed to sendfile: {}", e);
                            return Err(anyhow!("failed to sendfile: {}", e));
                        }
                    }
                };
                #[cfg(target_os = "macos")]
                let n = {
                    let (res, n) = sendfile::sendfile(
                        preload.fd,
                        socket_fd,
                        preload.offset as i64,
                        Some(preload_size as i64),
                        None,
                        None,
                    );
                    if let Err(e) = res {
                        if e == nix::errno::Errno::EAGAIN {
                            // a bug on macos while using with writable()
                            if n == 0 {
                                continue;
                            }
                        } else {
                            error!("failed to sendfile: {}", e);
                            return Err(anyhow!("failed to sendfile: {}", e));
                        }
                    }
                    n as usize
                };
                preload_size -= n;
                preload.offset += n;
                if n == 0 || preload_size == 0 {
                    break;
                }
            }
        }
        Ok(())
    }
}

pub(self) struct DownloadHelper {
    filepath: String,
    buffer_size: usize,
    content_tx: mpsc::Sender<PreloadOrFile>,
}

impl DownloadHelper {
    pub(self) fn new(
        filepath: String,
        buffer_size: usize,
        content_tx: mpsc::Sender<PreloadOrFile>,
    ) -> Self {
        Self {
            filepath,
            buffer_size,
            content_tx,
        }
    }

    pub(self) fn run(&mut self) -> anyhow::Result<()> {
        let file = OpenOptions::new().read(true).open(self.filepath.as_str())?;
        let file_len: usize = file.metadata()?.len() as usize;
        let file_fd = UnsafeFD {
            fd: file.as_raw_fd(),
        };
        let mut offset = 0;
        while offset < file_len {
            let len = if file_len - offset > self.buffer_size {
                self.buffer_size
            } else {
                file_len - offset
            };
            let mut preload = match Preload::new(file_fd, len, offset) {
                Err(e) => {
                    error!("failed to preload: {}", e);
                    return Err(anyhow!("failed to preload: {}", e));
                }
                Ok(preload) => preload,
            };
            preload.load_data();
            if self
                .content_tx
                .blocking_send(PreloadOrFile::P(preload))
                .is_err()
            {
                break;
            }
            offset += len;
        }
        self.content_tx.blocking_send(PreloadOrFile::F(file))?;
        Ok(())
    }
}

pub(crate) struct Preload {
    pub(crate) fd: UnsafeFD,
    pub(crate) size: usize,
    pub(crate) offset: usize,
    mmap_ptr: &'static mut u8,
    dummy: u64,
}

impl Preload {
    pub(self) fn new(fd: UnsafeFD, size: usize, offset: usize) -> anyhow::Result<Self> {
        let mmap_ptr = match unsafe {
            mman::mmap(
                None,
                NonZeroUsize::new_unchecked(size),
                ProtFlags::PROT_READ,
                MapFlags::MAP_PRIVATE,
                Some(fd),
                offset as i64,
            )
        } {
            Err(e) => {
                error!("failed to map memory: {}", e);
                return Err(anyhow!("failed to map memory: {}", e));
            }
            Ok(ptr) => unsafe { &mut *(ptr as *mut u8) },
        };

        Ok(Self {
            fd,
            size,
            offset,
            mmap_ptr,
            dummy: 0,
        })
    }

    /// the block method for load data from file to page cache.
    pub(self) fn load_data(&mut self) {
        let arr = unsafe { std::slice::from_raw_parts_mut(self.mmap_ptr as *mut u8, self.size) };
        let page_size = 4096; // assume the page size is 4KB
        let mut dummy: u64 = 0;
        for i in (0..self.size).step_by(page_size) {
            dummy += arr[i] as u64;
        }
        self.dummy = dummy;
    }
}

impl Drop for Preload {
    fn drop(&mut self) {
        unsafe {
            if let Err(e) = mman::munmap(self.mmap_ptr as *mut u8 as *mut libc::c_void, self.size) {
                error!(
                    "failed to unmap memory: {} {} {}",
                    e, self.mmap_ptr, self.size
                )
            }
        }
    }
}

pub(self) enum PreloadOrFile {
    P(Preload),
    F(File),
}
