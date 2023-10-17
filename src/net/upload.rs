use std::{fs::OpenOptions, io::Write, sync::Arc};

use tokio::{io::AsyncReadExt, net::tcp::OwnedReadHalf, sync::mpsc};
use tracing::error;

use crate::pool::ThreadPool;

pub(super) struct Upload<'a> {
    size: usize,
    filepath: String,
    thread_pool: Arc<ThreadPool>,
    read_stream: &'a mut OwnedReadHalf,
}

impl<'a> Upload<'a> {
    pub(super) fn new(
        size: usize,
        filepath: String,
        thread_pool: Arc<ThreadPool>,
        read_stream: &'a mut OwnedReadHalf,
    ) -> Self {
        Self {
            size,
            filepath,
            thread_pool,
            read_stream,
        }
    }

    pub(super) async fn run(&mut self) -> anyhow::Result<()> {
        let filepath = self.filepath.clone();
        let (tx, mut rx) = mpsc::channel::<(Arc<Vec<u8>>, usize)>(1);
        let pool = self.thread_pool.clone();

        tokio::spawn(async move {
            if let Err(e) = pool
                .execute_async(
                    move || {
                        _ = std::fs::create_dir_all(
                            std::path::Path::new(filepath.as_str())
                                .parent()
                                .unwrap()
                                .to_str()
                                .unwrap(),
                        );
                        let mut file = OpenOptions::new()
                            .create_new(true)
                            .write(true)
                            .append(true)
                            .open(filepath.as_str())
                            .unwrap();
                        loop {
                            match rx.blocking_recv() {
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
                    || {},
                )
                .await
            {
                error!("task send error: {}", e);
            }
        });

        let buffer1 = Arc::new(vec![0u8; 1024 * 16]);
        let buffer2 = Arc::new(vec![0u8; 1024 * 16]);
        let buffer3 = Arc::new(vec![0u8; 1024 * 16]);
        let mut buffer_arr = [buffer1, buffer2, buffer3];
        let mut idx: usize = 0;
        while self.size > 0 {
            let buffer = Arc::get_mut(&mut buffer_arr[idx % 3]).unwrap();
            let buffer = buffer.as_mut_slice();
            let n = match self.read_stream.read(buffer).await {
                Ok(n) => n,
                Err(e) => {
                    error!("read content error: {}", e);
                    break;
                }
            };
            if n == 0 {
                break;
            }
            self.size -= n;
            if let Err(e) = tx.send((buffer_arr[idx % 3].clone(), n)).await {
                error!("disk operation error: {}", e);
                break;
            }
            idx += 1;
        }
        Ok(())
    }
}
