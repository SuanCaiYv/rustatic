use ahash::AHashMap;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
    workers_handle: Option<JoinHandle<()>>,
    inner_tx: mpsc::Sender<Task>,
}

impl ThreadPool {
    pub(crate) fn new(scale_size: usize, max_size: usize, cache_size: usize) -> Self {
        let mut sys = System::new();
        sys.refresh_all();
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
                    let mut status_map = AHashMap::new();

                    let (idle_tx, mut idle_rx) = mpsc::channel(default_size * 8);
                    for i in 0..default_size {
                        let status = Arc::new(AtomicBool::new(false));
                        let (task_tx, task_rx) = mpsc::channel(cache_size);
                        status_map.insert(i, status.clone());

                        let worker = Worker::new(i, task_rx, idle_tx.clone(), status);
                        workers.push(worker);
                        task_senders.push(task_tx);
                    }

                    let idle_duration = Duration::from_secs(61);
                    let timer = tokio::time::sleep(idle_duration);
                    tokio::pin!(timer);

                    let mut kv_map = AHashMap::new();
                    let mut order_map = BTreeMap::new();
                    let mut next_wake = tokio::time::Instant::now();
                    let mut next_id = 0;
                    loop {
                        tokio::select! {
                        _ = &mut timer => {
                            status_map.get(&next_id).map(|status| {
                                if status.load(Ordering::Acquire) {
                                    // todo delete the worker
                                }
                            });
                            match order_map.first_key_value() {
                                Some(entry) => {
                                    next_wake = *entry.0;
                                    next_id = *entry.1;
                                    timer.as_mut().reset(next_wake);
                                    order_map.remove(&next_wake);
                                }
                                None => {}
                            };
                        }
                        task = inner_rx.recv() => {
                            match task {
                                Some(task) => {
                                    let mut idx = 0;
                                    let mut min = usize::MAX;
                                    let mut count = 0;
                                    task_senders.iter().for_each(|sender| {
                                        if sender.capacity() < min {
                                            min = sender.capacity();
                                            idx = count;
                                        }
                                        count += 1;
                                    });
                                    if min == 0 {
                                        _ = task_senders[idx].send(task).await;
                                    } else {
                                        if task_senders.len() < scale_size {
                                            let status = Arc::new(AtomicBool::new(false));
                                            let (task_tx, task_rx) = mpsc::channel(cache_size);
                                            status_map.insert(workers[workers.len()-1].id + 1, status.clone());

                                            let worker = Worker::new(workers[workers.len()-1].id + 1, task_rx, idle_tx.clone(), status);
                                            workers.push(worker);
                                            _ = task_tx.send(task).await;
                                            task_senders.push(task_tx);
                                        } else {
                                            if task_senders[idx].try_send(task).is_ok() {} else {}
                                        }
                                    }
                                }
                                None => break,
                            }
                        }
                        idle = idle_rx.recv() => {
                            match idle {
                                Some(id) => {
                                    if let Some(old_time) = kv_map.get(&id) {
                                        let old_time = *old_time;
                                        order_map.remove(&old_time);
                                        let trigger_instant = tokio::time::Instant::now() + idle_duration;
                                        order_map.insert(trigger_instant, id);
                                        kv_map.insert(id, trigger_instant);
                                        if old_time == next_wake {
                                            let next = match order_map.first_key_value() {
                                                Some(entry) => entry,
                                                None => (&trigger_instant, &id),
                                            };
                                            next_wake = *next.0;
                                            next_id = *next.1;
                                            timer.as_mut().reset(next_wake);
                                        }
                                    } else {
                                        let trigger_instant = tokio::time::Instant::now() + idle_duration;
                                        order_map.insert(trigger_instant, id);
                                        kv_map.insert(id, trigger_instant);
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                    }
                });
        });
        Self {
            max_size,
            scale_size,
            default_size,
            workers_handle: Some(workers_handle),
            inner_tx,
        }
    }

    /// if the cache queue for task is full, and number of threads reach the max_size,
    /// then the call of this method will block until the cache queue is not full.
    pub(crate) fn execute<F, Notify>(&self, f: F, notify: Option<Notify>) -> anyhow::Result<()>
    where
        F: FnOnce() + Send + Sync + 'static,
        Notify: FnOnce() + Send + Sync + 'static,
    {
        let task = Task::new(f, notify);
        self.inner_tx.blocking_send(task)?;
        Ok(())
    }

    pub(crate) async fn execute_async<F, Notify>(
        &self,
        f: F,
        notify: Option<Notify>,
    ) -> anyhow::Result<()>
    where
        F: FnOnce() + Send + Sync + 'static,
        Notify: FnOnce() + Send + Sync + 'static,
    {
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
        self.workers_handle.take().unwrap().join().unwrap();
    }
}

pub(self) struct Worker {
    id: usize,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    pub(self) fn new(
        id: usize,
        mut task_receiver: mpsc::Receiver<Task>,
        idle_notify: mpsc::Sender<usize>,
        status: Arc<AtomicBool>,
    ) -> Self {
        let handle = thread::Builder::new()
            .stack_size(1024 * 1024 * 8)
            .name(format!("worker-{}", id))
            .spawn(move || loop {
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
                            }
                            None => break,
                        }
                    }
                };
                task.run();
            })
            .unwrap();
        Self {
            id,
            thread: Some(handle),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.thread.take().unwrap().join().unwrap();
    }
}

pub(self) struct Task {
    pub(crate) f: Box<dyn FnOnce() + Send + Sync + 'static>,
    pub(crate) notify: Box<dyn FnOnce() + Send + Sync + 'static>,
}

impl Task {
    pub(crate) fn new<F, Notify>(f: F, notify: Option<Notify>) -> Self
    where
        F: FnOnce() + Send + Sync + 'static,
        Notify: FnOnce() + Send + Sync + 'static,
    {
        match notify {
            Some(notify) => Self {
                f: Box::new(f),
                notify: Box::new(notify),
            },
            None => Self {
                f: Box::new(f),
                notify: Box::new(|| {}),
            },
        }
    }

    pub(crate) fn run(self) {
        (self.f)();
        (self.notify)();
    }
}
