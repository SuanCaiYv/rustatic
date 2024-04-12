use coordinator::pool::automatic::Submitter;

use super::server::ThreadPoolResult;

pub(super) struct Delete {
    filepath: String,
    trash_path: String,
    submitter: Submitter<ThreadPoolResult>,
}

impl Delete {
    pub(super) fn new(filepath: String, trash_path: String, submitter: Submitter<ThreadPoolResult>) -> Self {
        Self {
            filepath,
            trash_path,
            submitter,
        }
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        let Self {
            filepath,
            trash_path,
            submitter,
        } = self;
        match submitter.submit(move || {
            match std::fs::rename(filepath, trash_path) {
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

pub(super) struct Restore {
    filepath: String,
    restore_path: String,
    submitter: Submitter<ThreadPoolResult>,
}

impl Restore {
    pub(super) fn new(filepath: String, restore_path: String, submitter: Submitter<ThreadPoolResult>) -> Self {
        Self {
            filepath,
            restore_path,
            submitter,
        }
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        let Self {
            filepath,
            restore_path,
            submitter,
        } = self;
        match submitter.submit(move || {
            match std::fs::rename(filepath, restore_path) {
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

pub(super) struct Remove {
    filepath: String,
    submitter: Submitter<ThreadPoolResult>,
}

impl Remove {
    pub(super) fn new(filepath: String, submitter: Submitter<ThreadPoolResult>) -> Self {
        Self {
            filepath,
            submitter,
        }
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        let Self {
            filepath,
            submitter,
        } = self;
        match submitter.submit(move || {
            match std::fs::remove_file(filepath) {
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