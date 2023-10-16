use std::num::NonZeroUsize;
use std::sync::{Arc, atomic::AtomicBool};
use anyhow::anyhow;

use futures::future::BoxFuture;
use nix::{libc, sys::mman::{self, {MapFlags, ProtFlags}}};
use tokio::sync::mpsc;
use tracing::error;
use crate::net::UnsafeFD;
use crate::pool::ThreadPool;

pub(super) struct Download {
    filepath: Option<String>,
    socket_fd: UnsafeFD,
    thread_pool: Option<Arc<ThreadPool>>,
    send_future: Option<BoxFuture<'static, anyhow::Result<()>>>,
    sent: bool,
    task_status: Arc<AtomicBool>,
}

pub(self) struct DownloadHelper {
    fd: UnsafeFD,
    filepath: String,
    buffer_size: usize,
    content_tx: mpsc::Sender<Preload>,
}

impl DownloadHelper {
    pub(self) fn new(fd: UnsafeFD, filepath: String, buffer_size: usize, content_tx: mpsc::Sender<Preload>) -> Self {
        Self {
            fd,
            filepath,
            buffer_size,
            content_tx,
        }
    }

    pub(self) fn run(&mut self) -> anyhow::Result<()> {
        let mut offset = 0;
        while offset < self.buffer_size {
            let mut preload = match Preload::new(self.fd, self.buffer_size, offset) {
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
    pub(crate) size: usize,
    pub(crate) offset: usize,
    mmap_ptr: *mut u8,
    dummy: u64,
}

impl Preload {
    pub(self) fn new(fd: UnsafeFD, size: usize, offset: usize) -> anyhow::Result<Self> {
        let mmap_ptr = match unsafe { mman::mmap(None, NonZeroUsize::new_unchecked(size), ProtFlags::PROT_READ, MapFlags::MAP_PRIVATE, Some(fd), offset as i64) } {
            Err(e) => {
                error!("failed to map memory: {}", e);
                return Err(anyhow!("failed to map memory: {}", e));
            }
            Ok(ptr) => {
                ptr as *mut u8
            }
        };

        Ok(Self {
            size,
            offset,
            mmap_ptr,
            dummy: 0,
        })
    }

    /// the block method for load data from file to page cache.
    pub(self) fn load_data(&mut self) {
        let arr = unsafe { std::slice::from_raw_parts_mut(self.mmap_ptr, self.size) };
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
            if let Err(e) = mman::munmap(self.mmap_ptr as *mut libc::c_void, self.size) {
                error!("failed to unmap memory: {}", e)
            }
        }
    }
}