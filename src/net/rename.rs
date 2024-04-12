use coordinator::pool::automatic::Submitter;
use tracing::error;

use super::server::ThreadPoolResult;

pub(super) struct Rename {
    filepath: String,
    new_path: String,
    submitter: Submitter<ThreadPoolResult>,
}

impl Rename {
    pub(super) fn new(filepath: String, new_name: String, submitter: Submitter<ThreadPoolResult>) -> Self {
        Self {
            filepath,
            new_path: new_name,
            submitter,
        }
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        let Self {
            filepath,
            new_path,
            submitter,
        } = self;
        match submitter.submit(move || {
            _ = std::fs::create_dir_all(
                    std::path::Path::new(&new_path)
                        .parent()
                        .unwrap()
                        .to_str()
                        .unwrap(),
                );
            match std::fs::rename(&filepath, &new_path) {
                Ok(_) => {
                    ThreadPoolResult::None
                }
                Err(e) => {
                    error!("rename file from {} to {} error: {}", filepath, new_path, e);
                    ThreadPoolResult::Err(anyhow::anyhow!(e.to_string()))
                }
            }
        }).await {
            ThreadPoolResult::Err(e) => {
                Err(e)
            }
            _ => {
                Ok(())
            }
        }
    }
}