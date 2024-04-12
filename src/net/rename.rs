use coordinator::pool::automatic::Submitter;

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
            match std::fs::rename(filepath, new_path) {
                Ok(_) => {
                    ThreadPoolResult::None
                }
                Err(e) => {
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