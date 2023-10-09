use std::collections::LinkedList;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use sysinfo::{System, SystemExt};
use tokio::sync::mpsc;

/// a thread pool for block syscall
pub(crate) struct ThreadPool {
    max_size: usize,
    scale_size: usize,
    default_size: usize,
    workers_handle: JoinHandle<()>,
    inner_tx: mpsc::Sender<Task>,
}

impl ThreadPool {
    pub(crate) fn new(scale_size: usize, max_size: usize, cache_size: usize) -> Self {
        let sys = System::new();
        let default_size = sys.cpus().len();

        let (inner_tx, mut inner_rx) = mpsc::channel(default_size);

        // the backend thread which manage the workers
        let workers_handle = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let mut workers = Vec::with_capacity(default_size);
                let mut task_senders = Vec::with_capacity(default_size);

                let (idle_tx, mut idle_rx) = mpsc::channel(default_size);
                for i in 0..default_size {
                    let (task_tx, task_rx) = mpsc::channel(cache_size);
                    let worker = Worker::new(i, task_rx, idle_tx.clone());
                    workers.push(worker);
                    task_senders.push(task_tx);
                }

                let timer = tokio::time::sleep(Duration::from_secs(321));
                tokio::pin!(timer);
                loop {
                    tokio::select! {
                        _ = &mut timer => {
                            break;
                        }
                        task = inner_rx.recv() => {
                            let task = task.unwrap();
                            let task_sender = task_senders.pop().unwrap();
                            task_sender.send(task).await.unwrap();
                            task_senders.insert(0, task_sender);
                        }
                        idle = idle_rx.recv() => {
                        }
                    }
                }
            });
        });
        Self {
            max_size,
            scale_size,
            default_size,
            workers_handle,
            inner_tx,
        }
    }

    /// if the cache queue for task is full, and number of threads reach the max_size,
    /// then the call of this method will block until the cache queue is not full.
    pub(crate) fn execute<F, Notify>(&self, f: F, notify: Option<Notify>) -> anyhow::Result<()>
        where F: FnOnce() + Send + 'static,
              Notify: FnOnce() + Send + 'static {
        let task = Task::new(f, notify);
        self.inner_tx.blocking_send(task)?;
        Ok(())
    }

    pub(crate) async fn execute_async<F, Notify>(&self, f: F, notify: Option<Notify>) -> anyhow::Result<()>
        where F: FnOnce() + Send + 'static,
              Notify: FnOnce() + Send + 'static {
        let task = Task::new(f, notify);
        self.inner_tx.send(task).await?;
        Ok(())
    }

    pub(crate) fn shutdown(&self) {
        unimplemented!()
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.workers_handle.join().unwrap();
    }
}

pub(self) struct Worker {
    id: usize,
    thread: JoinHandle<()>,
}

impl Worker {
    pub(self) fn new(id: usize,
                     mut task_receiver: mpsc::Receiver<Task>,
                     idle_notify: mpsc::Sender<usize>, status: Arc<AtomicBool>) -> Self {
        let handle = thread::Builder::new()
            .stack_size(1024 * 1024 * 8)
            .name(format!("worker-{}", id))
            .spawn(move || {
                loop {
                    let task = match task_receiver.try_recv() {
                        Ok(task) => task,
                        Err(_) => {
                            status.store(true, Ordering::Release);
                            _ = idle_notify.blocking_send(id);
                            match task_receiver.blocking_recv() {
                                Some(task) => {
                                    status.store(false, Ordering::Release);
                                    _ = idle_notify.blocking_send(id);
                                    task
                                },
                                None => break,
                            }
                        }
                    };
                    task.run();
                }
            })
            .unwrap();
        Self {
            id,
            thread: handle,
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.thread.join().unwrap();
    }
}

pub(self) struct Task {
    pub(crate) f: Box<dyn FnOnce() + Send + 'static>,
    pub(crate) notify: Box<dyn FnOnce() + Send + 'static>,
}

impl Task {
    pub(crate) fn new<F, Notify>(f: F, notify: Option<Notify>) -> Self
        where F: FnOnce() + Send + 'static,
              Notify: FnOnce() + Send + 'static {
        Self {
            f: Box::new(f),
            notify: Box::new(notify.unwrap_or(|| {})),
        }
    }

    pub(crate) fn run(self) {
        self.f();
        self.notify();
    }
}