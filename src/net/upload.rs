use std::{fs::OpenOptions, io::Write, sync::Arc};

use coordinator::pool::automatic::Submitter;

use tokio::{io::AsyncReadExt, net::TcpStream};
use tracing::error;

use super::server::ThreadPoolResult;

pub(super) struct Upload<'a> {
    size: usize,
    filepath: String,
    submitter: Submitter<ThreadPoolResult>,
    read_stream: &'a mut TcpStream,
}

impl<'a> Upload<'a> {
    pub(super) fn new(
        size: usize,
        filepath: String,
        submitter: Submitter<ThreadPoolResult>,
        read_stream: &'a mut TcpStream,
    ) -> Self {
        Self {
            size,
            filepath,
            submitter,
            read_stream,
        }
    }

    pub(super) async fn run(mut self) -> anyhow::Result<()> {
        let Self {
            filepath,
            ..
        } = self;
        let (tx, rx) = flume::bounded::<(Arc<Vec<u8>>, usize)>(1);
        let submitter = self.submitter.clone();

        tokio::spawn(async move {
            submitter.submit(move || {
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
                    match rx.recv() {
                        Ok((content, n)) => {
                            if let Err(e) = file.write_all(&content[..n]) {
                                error!("write content error: {}", e);
                                break;
                            }
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                file.sync_all().unwrap();
                return ThreadPoolResult::None;
            })
            .await
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
            if let Err(e) = tx.send_async((buffer_arr[idx % 3].clone(), n)).await {
                error!("disk operation error: {}", e);
                break;
            }
            idx += 1;
        }
        Ok(())
    }
}
