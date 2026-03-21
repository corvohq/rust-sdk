use corvo_client::conn::{AckJob, AckStatus, Conn, ConnError, FetchedJob as RpcFetchedJob};
use corvo_client::{CorvoClient, CorvoError, HeartbeatEntry};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

/// Configuration for a Corvo worker.
pub struct WorkerConfig {
    pub queues: Vec<String>,
    pub worker_id: String,
    pub hostname: Option<String>,
    pub concurrency: usize,
    pub shutdown_timeout: Duration,
    /// RPC host for the binary protocol (default "127.0.0.1").
    pub rpc_host: String,
    /// RPC port for the binary protocol (default 7438).
    pub rpc_port: u16,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queues: Vec::new(),
            worker_id: String::new(),
            hostname: None,
            concurrency: 10,
            shutdown_timeout: Duration::from_secs(30),
            rpc_host: "127.0.0.1".to_string(),
            rpc_port: 7438,
        }
    }
}

/// A fetched job exposed to handler functions.
///
/// Wraps the binary RPC `FetchedJob` with a parsed JSON payload for
/// ergonomic handler code.
#[derive(Debug, Clone)]
pub struct FetchedJob {
    pub job_id: String,
    pub queue: String,
    pub attempt: u16,
    pub max_retries: u16,
    pub checkpoint: Vec<u8>,
    pub tags: Vec<u8>,
    pub payload: Value,
}

impl From<RpcFetchedJob> for FetchedJob {
    fn from(rpc: RpcFetchedJob) -> Self {
        let payload = if rpc.payload.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&rpc.payload).unwrap_or(Value::Null)
        };
        Self {
            job_id: rpc.id,
            queue: rpc.queue,
            attempt: rpc.attempt,
            max_retries: rpc.max_retries,
            checkpoint: rpc.checkpoint,
            tags: rpc.tags,
            payload,
        }
    }
}

/// Context passed to job handlers for cancellation and progress reporting.
pub struct JobContext {
    job_id: String,
    client: CorvoClient,
    cancelled: Arc<Mutex<bool>>,
}

impl JobContext {
    /// Returns true if this job has been cancelled by the server.
    pub async fn is_cancelled(&self) -> bool {
        *self.cancelled.lock().await
    }

    /// Saves a checkpoint for this job via heartbeat.
    pub async fn checkpoint(&self, data: Value) -> Result<(), CorvoError> {
        let mut jobs = HashMap::new();
        jobs.insert(
            self.job_id.clone(),
            HeartbeatEntry {
                checkpoint: Some(data),
                ..Default::default()
            },
        );
        self.client.heartbeat(jobs).await?;
        Ok(())
    }

    /// Reports progress for this job via heartbeat.
    pub async fn progress(&self, current: u32, _total: u32, _message: &str) -> Result<(), CorvoError> {
        let mut jobs = HashMap::new();
        jobs.insert(
            self.job_id.clone(),
            HeartbeatEntry {
                progress: Some(current),
                ..Default::default()
            },
        );
        self.client.heartbeat(jobs).await?;
        Ok(())
    }
}

type BoxFuture<'a> = Pin<Box<dyn Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send + 'a>>;
type HandlerFn = Arc<dyn Fn(FetchedJob, Arc<JobContext>) -> BoxFuture<'static> + Send + Sync>;

/// A Corvo worker that fetches and processes jobs using the bidi streaming
/// binary RPC protocol.
///
/// Each concurrency slot opens a dedicated `Conn` and uses subscribe +
/// read_pushed_jobs for efficient push-based job delivery.
pub struct CorvoWorker {
    client: CorvoClient,
    config: WorkerConfig,
    handlers: HashMap<String, HandlerFn>,
}

impl CorvoWorker {
    /// Creates a new worker with the given client and configuration.
    ///
    /// The `client` is used for heartbeat RPCs (HTTP). Job fetch and ack
    /// use the binary RPC protocol via `Conn` connections to
    /// `config.rpc_host:config.rpc_port`.
    pub fn new(client: CorvoClient, config: WorkerConfig) -> Self {
        Self {
            client,
            config,
            handlers: HashMap::new(),
        }
    }

    /// Registers a handler for a queue.
    pub fn register<F, Fut>(&mut self, queue: impl Into<String>, handler: F)
    where
        F: Fn(FetchedJob, Arc<JobContext>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send + 'static,
    {
        let handler = Arc::new(move |job, ctx| -> BoxFuture<'static> {
            Box::pin(handler(job, ctx))
        });
        self.handlers.insert(queue.into(), handler);
    }

    /// Starts the worker. Blocks until SIGINT/SIGTERM, then drains gracefully.
    pub async fn start(&self) -> Result<(), CorvoError> {
        let active_jobs: Arc<Mutex<HashMap<String, Arc<Mutex<bool>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let stopping = Arc::new(Mutex::new(false));
        let stop_notify = Arc::new(Notify::new());
        let handlers = Arc::new(self.handlers.clone());

        // Spawn heartbeat task.
        let hb_active = active_jobs.clone();
        let hb_client = self.client.clone();
        let hb_stopping = stopping.clone();
        let hb_stop = stop_notify.clone();
        let hb_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                    _ = hb_stop.notified() => return,
                }
                if *hb_stopping.lock().await {
                    return;
                }
                let active = hb_active.lock().await;
                if active.is_empty() {
                    continue;
                }
                let job_ids: Vec<String> = active.keys().cloned().collect();
                let cancel_flags: Vec<(String, Arc<Mutex<bool>>)> = active
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                drop(active);

                let mut entries = HashMap::new();
                for id in &job_ids {
                    entries.insert(id.clone(), HeartbeatEntry::default());
                }
                if let Ok(result) = hb_client.heartbeat(entries).await {
                    for (id, status) in &result.jobs {
                        if status.status == "cancel" {
                            for (job_id, flag) in &cancel_flags {
                                if job_id == id {
                                    *flag.lock().await = true;
                                }
                            }
                        }
                    }
                }
            }
        });

        // Spawn fetch loop tasks -- one per concurrency slot.
        let mut fetch_handles = Vec::new();
        for _ in 0..self.config.concurrency {
            let client = self.client.clone();
            let rpc_host = self.config.rpc_host.clone();
            let rpc_port = self.config.rpc_port;
            let queues = self.config.queues.clone();
            let worker_id = self.config.worker_id.clone();
            let active = active_jobs.clone();
            let stopping = stopping.clone();
            let stop_notify = stop_notify.clone();
            let handlers = handlers.clone();

            let handle = tokio::spawn(async move {
                let queue_refs: Vec<&str> = queues.iter().map(|s| s.as_str()).collect();
                let credits: u16 = 1;

                let mut conn = Conn::new(&rpc_host, rpc_port).await;

                loop {
                    if *stopping.lock().await {
                        conn.close().await;
                        return;
                    }

                    // Subscribe with credits.
                    if let Err(e) = conn.subscribe(&queue_refs, &worker_id, credits).await {
                        tracing::warn!("subscribe error: {e}, reconnecting");
                        if let Err(re) = conn.reconnect().await {
                            tracing::warn!("reconnect failed: {re}");
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                                _ = stop_notify.notified() => {
                                    conn.close().await;
                                    return;
                                }
                            }
                        }
                        continue;
                    }

                    // Read pushed jobs (blocks until server pushes).
                    let rpc_jobs = match conn.read_pushed_jobs().await {
                        Ok(jobs) => jobs,
                        Err(ConnError::Io(_)) => {
                            tracing::warn!("connection lost, reconnecting");
                            if let Err(re) = conn.reconnect().await {
                                tracing::warn!("reconnect failed: {re}");
                                tokio::select! {
                                    _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                                    _ = stop_notify.notified() => {
                                        conn.close().await;
                                        return;
                                    }
                                }
                            }
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!("read_pushed_jobs error: {e}");
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                                _ = stop_notify.notified() => {
                                    conn.close().await;
                                    return;
                                }
                            }
                        }
                    };

                    if rpc_jobs.is_empty() {
                        continue;
                    }

                    // Process each job and collect acks.
                    let mut acks: Vec<AckJob> = Vec::with_capacity(rpc_jobs.len());

                    for rpc_job in rpc_jobs {
                        let job: FetchedJob = rpc_job.into();
                        let job_queue = job.queue.clone();
                        let job_id = job.job_id.clone();

                        let handler = match handlers.get(&job_queue) {
                            Some(h) => h.clone(),
                            None => {
                                // No handler registered: ack immediately.
                                acks.push(AckJob {
                                    job_id,
                                    queue: job_queue,
                                    ack_status: AckStatus::Done,
                                    result: String::new(),
                                    checkpoint: String::new(),
                                    hold_reason: String::new(),
                                });
                                continue;
                            }
                        };

                        let cancelled = Arc::new(Mutex::new(false));
                        active
                            .lock()
                            .await
                            .insert(job_id.clone(), cancelled.clone());

                        let ctx = Arc::new(JobContext {
                            job_id: job_id.clone(),
                            client: client.clone(),
                            cancelled,
                        });

                        let result = handler(job, ctx).await;
                        active.lock().await.remove(&job_id);

                        match result {
                            Ok(()) => {
                                acks.push(AckJob {
                                    job_id,
                                    queue: job_queue,
                                    ack_status: AckStatus::Done,
                                    result: String::new(),
                                    checkpoint: String::new(),
                                    hold_reason: String::new(),
                                });
                            }
                            Err(e) => {
                                // Use fail via HTTP client for proper retry handling.
                                let _ = client.fail(&job_id, &e.to_string(), "").await;
                            }
                        }
                    }

                    // Send acks via binary RPC.
                    if !acks.is_empty() {
                        if let Err(e) = conn.ack_batch(&acks).await {
                            tracing::warn!("ack_batch error: {e}");
                            // Reconnect on next iteration.
                            let _ = conn.reconnect().await;
                        }
                    }
                }
            });
            fetch_handles.push(handle);
        }

        // Wait for shutdown signal.
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl_c");

        tracing::info!("shutting down worker");

        // Enter drain mode.
        *stopping.lock().await = true;
        stop_notify.notify_waiters();

        // Wait for in-flight handlers.
        let deadline = tokio::time::sleep(self.config.shutdown_timeout);
        tokio::pin!(deadline);

        let join_all = async {
            for handle in fetch_handles {
                let _ = handle.await;
            }
        };

        tokio::select! {
            _ = join_all => {
                tracing::info!("all handlers finished");
            }
            _ = &mut deadline => {
                tracing::warn!("shutdown timeout reached, failing remaining jobs");
                let active = active_jobs.lock().await;
                for job_id in active.keys() {
                    let _ = self.client.fail(job_id, "worker_shutdown", "").await;
                }
            }
        }

        // Stop heartbeat.
        stop_notify.notify_waiters();
        let _ = hb_handle.await;

        tracing::info!("worker stopped");
        Ok(())
    }
}
