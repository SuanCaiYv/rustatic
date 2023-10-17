use std::{fs::OpenOptions, num::NonZeroUsize, os::fd::AsRawFd, sync::Arc};

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
use tokio::{net::tcp::OwnedWriteHalf, sync::mpsc};
use tracing::error;

use crate::{net::UnsafeFD, pool::ThreadPool};

pub(super) struct Download<'a> {
    filepath: String,
    socket_fd: UnsafeFD,
    thread_pool: Arc<ThreadPool>,
    write_stream: &'a mut OwnedWriteHalf,
}

impl<'a> Download<'a> {
    pub(super) fn new(
        filepath: String,
        socket_fd: UnsafeFD,
        thread_pool: Arc<ThreadPool>,
        write_stream: &'a mut OwnedWriteHalf,
    ) -> Self {
        Self {
            filepath,
            socket_fd,
            thread_pool,
            write_stream,
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
        while let Some(mut preload) = content_rx.recv().await {
            loop {
                self.write_stream.writable().await?;
                let (res, n) = sendfile::sendfile(
                    preload.fd,
                    self.socket_fd,
                    preload.offset as i64,
                    Some(preload.size as i64),
                    None,
                    None,
                );
                if let Err(e) = res {
                    error!("failed to sendfile: {}", e);
                    return Err(anyhow!("failed to sendfile: {}", e));
                }
                let n = n as usize;
                if n == 0 {
                    break;
                }
                preload.size -= n;
                preload.offset += n;
            }
        }
        Ok(())
    }
}

pub(self) struct DownloadHelper {
    filepath: String,
    buffer_size: usize,
    content_tx: mpsc::Sender<Preload>,
}

impl DownloadHelper {
    pub(self) fn new(
        filepath: String,
        buffer_size: usize,
        content_tx: mpsc::Sender<Preload>,
    ) -> Self {
        Self {
            filepath,
            buffer_size,
            content_tx,
        }
    }

    pub(self) fn run(&mut self) -> anyhow::Result<()> {
        let file = OpenOptions::new().read(true).open(self.filepath.as_str())?;
        let file_fd = UnsafeFD {
            fd: file.as_raw_fd(),
        };
        let mut offset = 0;
        while offset < self.buffer_size {
            let mut preload = match Preload::new(file_fd, self.buffer_size, offset) {
                Err(e) => {
                    error!("failed to preload: {}", e);
                    return Err(anyhow!("failed to preload: {}", e));
                }
                Ok(preload) => preload,
            };
            preload.load_data();
            offset += self.buffer_size;
            if self.content_tx.blocking_send(preload).is_err() {
                break;
            }
        }
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
                error!("failed to unmap memory: {}", e)
            }
        }
    }
}
