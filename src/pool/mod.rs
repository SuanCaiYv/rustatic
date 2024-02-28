use std::{
    cmp::Reverse,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use ahash::AHashMap;
use priority_queue::PriorityQueue;
use sysinfo::{System, SystemExt};
use tokio::{
    sync::{
        mpsc::{
            self,
            error::{TryRecvError, TrySendError},
        },
        oneshot,
    },
    time::Instant,
};
use tracing::{debug, error, warn};

pub(self) struct Task<T: 'static> {
    pub(self) f: Box<dyn FnOnce() -> T + Send + Sync + 'static>,
    pub(self) sender: oneshot::Sender<T>,
}

impl<T> Task<T> {
    pub(self) fn new<F>(f: F, sender: oneshot::Sender<T>) -> Self
    where
        F: FnOnce() -> T + Send + Sync + 'static,
    {
        Self {
            f: Box::new(f),
            sender,
        }
    }

    pub(self) fn run(self) {
        let res = (self.f)();
        _ = self.sender.send(res);
    }
}

pub(self) struct Worker {
    id: usize,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    pub(self) fn new<T: 'static + Send + Sync>(
        id: usize,
        mut task_receiver: mpsc::Receiver<Task<T>>,
        idle_notify: mpsc::Sender<usize>,
        status: Arc<AtomicBool>,
    ) -> Self {
        let handle = thread::Builder::new()
            .stack_size(1024 * 1024 * 8)
            .name(format!("worker-{}", id))
            .spawn(move || loop {
                let task = match task_receiver.try_recv() {
                    Ok(task) => {
                        debug!("worker: {} try recv task success", id);
                        status.store(false, Ordering::Release);
                        task
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            debug!("worker: {} try recv task failed", id);
                            status.store(true, Ordering::Release);
                            if idle_notify.blocking_send(id).is_err() {
                                break;
                            }
                            match task_receiver.blocking_recv() {
                                Some(task) => {
                                    debug!("worker: {} blocking recv task success", id);
                                    status.store(false, Ordering::Release);
                                    task
                                }
                                None => break,
                            }
                        }
                        TryRecvError::Disconnected => {
                            debug!("worker: {} released.", id);
                            break;
                        }
                    },
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

/// a thread pool for block syscall
pub(crate) struct ThreadPool<T: 'static> {
    workers_handle: Option<JoinHandle<()>>,
    inner_tx: mpsc::Sender<Task<T>>,
}

impl<T: 'static + Sync + Send> ThreadPool<T> {
    /// default size of thread created is equal to number of available cpu cores,
    /// when all workers are busy, new thread will be created until reach the scale size,
    /// when all workers with their sender be fully, create new thread until max size.
    pub(crate) fn new(scale_size: usize, max_size: usize, cache_size: usize) -> Self {
        let mut sys = System::new();
        sys.refresh_cpu();
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
                        let worker = Worker::new(i, task_rx, idle_tx.clone(), status.clone());

                        status_map.insert(i, status);
                        workers.push(worker);
                        task_senders.push(task_tx);
                    }

                    let idle_duration = Duration::from_secs(61 * 60);
                    let sleep_duration = Duration::from_secs(60 * 60 * 24 * 7);
                    let timer = tokio::time::sleep(sleep_duration);
                    tokio::pin!(timer);

                    let mut pq: PriorityQueue<usize, Reverse<Instant>> = PriorityQueue::new();

                    loop {
                        tokio::select! {
                            _ = &mut timer => {
                                let entry = match pq.pop() {
                                    Some(entry) => entry,
                                    None => continue,
                                };
                                match status_map.get(&entry.0) {
                                    Some(status) => {
                                        // idle worker detected.
                                        if status.load(Ordering::Acquire) {
                                            let mut idx = usize::MAX;
                                            for i in 0..workers.len() {
                                                if workers[i].id == entry.0 {
                                                    idx = i;
                                                }
                                            }
                                            if idx == usize::MAX {
                                                error!("worker: {} not found", entry.0);
                                                break;
                                            }
                                            if workers.len() > default_size {
                                                debug!("remove worker: {}", entry.0);
                                                task_senders.remove(idx);
                                                workers.remove(idx);
                                                status_map.remove(&entry.0);
                                            }
                                        } else {
                                            debug!("worker: {} is not idle", entry.0);
                                        }
                                    },
                                    None => continue,
                                }
                                let reset_time = match pq.peek() {
                                    Some(entry) => {
                                        entry.1.0
                                    }
                                    None => {
                                        debug!("no worker idle");
                                        Instant::now() + sleep_duration
                                    }
                                };
                                timer.as_mut().reset(reset_time);
                            }
                            idle = idle_rx.recv() => {
                                match idle {
                                    Some(id) => {
                                        debug!("worker: {} idle", id);
                                        let trigger_instant = Instant::now() + idle_duration;
                                        pq.push(id, Reverse(trigger_instant));
                                        let reset_time = pq.peek().unwrap().1.0;
                                        timer.as_mut().reset(reset_time);
                                    }
                                    None => {
                                        error!("idle_rx recv None");
                                        break;
                                    },
                                }
                            }
                            task = inner_rx.recv() => {
                                match task {
                                    Some(task) => {
                                        let mut index = 0;
                                        let mut idle_idx = usize::MAX;
                                        let mut sender_idx = 0;
                                        let mut remain_size = 0;

                                        workers.iter().for_each(|worker| {
                                            status_map.get(&worker.id)
                                                .map(|status| {
                                                    if status.load(Ordering::Acquire) {
                                                        idle_idx = index;
                                                    }
                                                });
                                            let cap = task_senders[index].capacity();
                                            if cap > remain_size {
                                                remain_size = cap;
                                                sender_idx = index;
                                            }
                                            index += 1;
                                        });

                                        // first, there exists idle worker, select it.
                                        if idle_idx != usize::MAX {
                                            let worker_id = workers[idle_idx].id;
                                            debug!("send task to idle worker: {}", worker_id);

                                            status_map.get(&worker_id).unwrap().store(false, Ordering::Release);
                                            if let Err(e) = task_senders[idle_idx].send(task).await {
                                                error!("send task error: {:?}", e);
                                            }
                                        } else {
                                            // second, create new thread for task when allowed.
                                            if task_senders.len() < scale_size {
                                                let new_worker_id = workers[workers.len()-1].id + 1;
                                                debug!("create new worker: {}", new_worker_id);

                                                let status = Arc::new(AtomicBool::new(false));
                                                let (task_tx, task_rx) = mpsc::channel(cache_size);
                                                let worker = Worker::new(new_worker_id, task_rx, idle_tx.clone(), status.clone());

                                                status_map.insert(new_worker_id, status);
                                                workers.push(worker);
                                                _ = task_tx.send(task).await;
                                                task_senders.push(task_tx);
                                            } else {
                                                // third, send task to the worker which has the most capacity.
                                                match task_senders[sender_idx].try_send(task) {
                                                    Ok(_) => {
                                                        debug!("buffer: {} available", workers[sender_idx].id);
                                                    }
                                                    Err(e) => {
                                                        match e {
                                                            TrySendError::Full(task) => {
                                                                // last, create more threads before reach max size.
                                                                if workers.len() < max_size {
                                                                    let new_worker_id = workers[workers.len()-1].id + 1;
                                                                    debug!("create more worker: {}", new_worker_id);
                                                                    let status = Arc::new(AtomicBool::new(false));
                                                                    let (task_tx, task_rx) = mpsc::channel(cache_size);
                                                                    let worker = Worker::new(new_worker_id, task_rx, idle_tx.clone(), status.clone());

                                                                    status_map.insert(new_worker_id, status);
                                                                    workers.push(worker);
                                                                    _ = task_tx.send(task).await;
                                                                    task_senders.push(task_tx);
                                                                } else {
                                                                    // unfortunately, the system is busy.
                                                                    // consider to increase the max size of thread pool or size of cache queue.
                                                                    warn!("system is busy, queues and threads all full!");
                                                                    let idx = fastrand::usize(0..workers.len());
                                                                    if let Err(e) = task_senders[idx].send(task).await {
                                                                        println!("send task error: {:?}", e);
                                                                    }
                                                                }
                                                            }
                                                            TrySendError::Closed(_task) => {
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
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
            workers_handle: Some(workers_handle),
            inner_tx,
        }
    }

    /// if the cache queue for task is full, and number of threads reach the max_size,
    /// then the call of this method will block until the cache queue is not full.
    #[allow(unused)]
    pub(crate) fn execute<F>(&self, f: F) -> anyhow::Result<oneshot::Receiver<T>>
    where
        F: FnOnce() -> T + Send + Sync + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let task = Task::new(f, sender);
        self.inner_tx.blocking_send(task)?;
        Ok(receiver)
    }

    /// if the cache queue is full and create more threads is not allow, the call will block on async context.
    pub(crate) async fn execute_async<F>(
        &self,
        f: F,
    ) -> anyhow::Result<oneshot::Receiver<T>>
    where
        F: FnOnce() -> T + Send + Sync + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let task = Task::new(f, sender);
        self.inner_tx.send(task).await?;
        Ok(receiver)
    }
    
    pub(crate) fn execute_block<F>(
        &self,
        f: F,
    ) -> anyhow::Result<T>
    where
        F: FnOnce() -> T + Send + Sync + 'static,
    {
        return Ok(f());
    }
}

impl<T> Drop for ThreadPool<T> {
    fn drop(&mut self) {
        self.workers_handle.take().unwrap().join().unwrap();
    }
}
