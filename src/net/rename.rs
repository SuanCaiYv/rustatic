use coordinator::pool::automatic::Submitter;

use super::server::ThreadPoolResult;

pub(super) struct Rename {
    filepath: String,
    new_name: String,
    submitter: Submitter<ThreadPoolResult>,
}

impl Rename {
    pub(super) fn new(filepath: String, new_name: String, submitter: Submitter<ThreadPoolResult>) -> Self {
        Self {
            filepath,
            new_name,
            submitter,
        }
    }

    pub(super) async fn run(&mut self) -> anyhow::Result<()> {
        let old_name = self.filepath.clone();
        let new_name = self.new_name.clone();
        match self.submitter.submit(move || {
            match std::fs::rename(old_name, new_name) {
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