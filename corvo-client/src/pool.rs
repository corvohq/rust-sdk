//! Auto-batching pooled client.
//!
//! [`Client`] wraps N internal [`CorvoClient`] instances (lanes) and
//! transparently batches individual `enqueue()` calls into bulk HTTP requests.
//!
//! Callers await their individual result while the background flush task
//! coalesces jobs from many concurrent tasks into a single `enqueue_batch()`
//! call per lane. Flushes happen every `flush_interval` (default 1 ms) or
//! when the buffer reaches `max_batch` entries (default 256), whichever comes
//! first.
//!
//! All non-enqueue operations (`fetch`, `ack`, etc.) are passed through to
//! the underlying clients using round-robin lane selection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::{
    AckBatchItem, AckBatchResult, AckBody, AuthOptions, BatchConfig, BatchJob, BatchResult,
    BulkRequest, BulkResult, BulkTask, CorvoClient, CorvoError, CreateBatchResult, EnqueueOptions,
    FailResult, FetchBatchResult, FetchedJob, HeartbeatEntry, HeartbeatResult, RetryConfig,
    SealBatchResult, SearchFilter, SearchResult, ServerInfo,
};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Configuration for the pooled [`Client`].
#[derive(Clone, Debug)]
pub struct ClientOptions {
    pub base_url: String,
    /// Number of connection lanes (default 8).
    pub lanes: usize,
    /// Maximum entries per batch before an immediate flush (default 256).
    pub max_batch: usize,
    /// Time-based flush interval (default 1 ms).
    pub flush_interval: Duration,
    /// Optional authentication forwarded to every underlying `CorvoClient`.
    pub auth: Option<AuthOptions>,
    /// Optional retry config forwarded to every underlying `CorvoClient`.
    pub retry: Option<RetryConfig>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:7438".to_string(),
            lanes: 8,
            max_batch: 256,
            flush_interval: Duration::from_millis(1),
            auth: None,
            retry: None,
        }
    }
}

/// A pooled, auto-batching Corvo client.
///
/// See the [module-level documentation](self) for details.
pub struct Client {
    lanes: Vec<LaneHandle>,
    next: AtomicU64,
    /// Keep JoinHandles so we can abort on drop.
    _flush_tasks: Vec<tokio::task::JoinHandle<()>>,
}

// ---------------------------------------------------------------------------
// Internal lane types
// ---------------------------------------------------------------------------

/// A pending enqueue request sitting in the lane buffer.
struct PendingEnqueue {
    job: BatchJob,
    tx: oneshot::Sender<Result<String, CorvoError>>,
}

/// The sender half given to callers; the receiver is owned by the flush task.
struct LaneHandle {
    /// Channel to submit enqueue requests into this lane.
    tx: mpsc::UnboundedSender<PendingEnqueue>,
    /// The underlying client, used for passthrough calls.
    client: CorvoClient,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl Client {
    /// Create a new pooled client. Spawns one background flush task per lane.
    ///
    /// Must be called from within a Tokio runtime.
    pub fn new(options: ClientOptions) -> Self {
        let num_lanes = options.lanes.max(1);
        let mut lanes = Vec::with_capacity(num_lanes);
        let mut flush_tasks = Vec::with_capacity(num_lanes);

        for _ in 0..num_lanes {
            let mut client = CorvoClient::new(options.base_url.clone());
            if let Some(ref auth) = options.auth {
                client = client.with_auth(auth.clone());
            }
            if let Some(ref retry) = options.retry {
                client = client.with_retry(retry.clone());
            }

            let (tx, rx) = mpsc::unbounded_channel::<PendingEnqueue>();

            let flush_client = client.clone();
            let max_batch = options.max_batch;
            let flush_interval = options.flush_interval;

            let handle = tokio::spawn(flush_loop(flush_client, rx, max_batch, flush_interval));

            lanes.push(LaneHandle { tx, client });
            flush_tasks.push(handle);
        }

        Self {
            lanes,
            next: AtomicU64::new(0),
            _flush_tasks: flush_tasks,
        }
    }

    /// Returns the number of lanes.
    pub fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    // -- Round-robin helpers -------------------------------------------------

    fn pick_lane(&self) -> usize {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        (idx as usize) % self.lanes.len()
    }

    fn client_for_lane(&self, lane: usize) -> &CorvoClient {
        &self.lanes[lane].client
    }

    fn next_client(&self) -> &CorvoClient {
        self.client_for_lane(self.pick_lane())
    }

    // -- Auto-batched enqueue ------------------------------------------------

    /// Enqueue a single job. The call is transparently batched with other
    /// concurrent callers and flushed as a single HTTP request.
    ///
    /// Returns the job ID assigned by the server.
    pub async fn enqueue(&self, queue: &str, payload: Value) -> Result<String, CorvoError> {
        self.enqueue_with(EnqueueOptions {
            queue: queue.to_string(),
            payload,
            ..Default::default()
        })
        .await
    }

    /// Enqueue a single job with full options. Auto-batched like [`enqueue`](Self::enqueue).
    ///
    /// Note: `priority`, `unique_key`, `unique_period`, `max_retries`,
    /// `scheduled_at`, `tags`, `expire_after`, `chain`, and `batch_id` are
    /// **not** forwarded through the batch endpoint today -- only `queue` and
    /// `payload` are sent via `BatchJob`. If you need the full option set,
    /// use [`enqueue_single`](Self::enqueue_single) which bypasses batching.
    pub async fn enqueue_with(&self, opts: EnqueueOptions) -> Result<String, CorvoError> {
        let lane = self.pick_lane();
        let (tx, rx) = oneshot::channel();

        let pending = PendingEnqueue {
            job: BatchJob {
                queue: opts.queue,
                payload: opts.payload,
            },
            tx,
        };

        self.lanes[lane]
            .tx
            .send(pending)
            .map_err(|_| CorvoError::Api("lane flush task has shut down".to_string()))?;

        // Await until our batch is flushed.
        rx.await
            .map_err(|_| CorvoError::Api("lane dropped our enqueue request".to_string()))?
    }

    /// Enqueue a single job bypassing auto-batching. Useful when you need
    /// the full `EnqueueOptions` (priority, unique key, etc.).
    pub async fn enqueue_single(
        &self,
        opts: EnqueueOptions,
    ) -> Result<crate::EnqueueResult, CorvoError> {
        self.next_client().enqueue_with(opts).await
    }

    // -- Passthrough: batch enqueue ------------------------------------------

    /// Directly send a pre-built batch. Not auto-batched.
    pub async fn enqueue_batch(
        &self,
        jobs: Vec<BatchJob>,
        batch: Option<BatchConfig>,
    ) -> Result<BatchResult, CorvoError> {
        self.next_client().enqueue_batch(jobs, batch).await
    }

    // -- Passthrough: fetch --------------------------------------------------

    pub async fn fetch(
        &self,
        queues: Vec<String>,
        worker_id: &str,
        hostname: &str,
        timeout: u64,
    ) -> Result<Option<FetchedJob>, CorvoError> {
        self.next_client()
            .fetch(queues, worker_id, hostname, timeout)
            .await
    }

    pub async fn fetch_batch(
        &self,
        queues: Vec<String>,
        worker_id: &str,
        hostname: &str,
        timeout: u64,
        count: u32,
    ) -> Result<FetchBatchResult, CorvoError> {
        self.next_client()
            .fetch_batch(queues, worker_id, hostname, timeout, count)
            .await
    }

    // -- Passthrough: ack ----------------------------------------------------

    pub async fn ack(&self, job_id: &str, body: AckBody) -> Result<Value, CorvoError> {
        self.next_client().ack(job_id, body).await
    }

    pub async fn ack_batch(
        &self,
        acks: Vec<AckBatchItem>,
    ) -> Result<AckBatchResult, CorvoError> {
        self.next_client().ack_batch(acks).await
    }

    // -- Passthrough: fail ---------------------------------------------------

    pub async fn fail(
        &self,
        job_id: &str,
        error: &str,
        backtrace: &str,
    ) -> Result<FailResult, CorvoError> {
        self.next_client().fail(job_id, error, backtrace).await
    }

    // -- Passthrough: heartbeat ----------------------------------------------

    pub async fn heartbeat(
        &self,
        jobs: std::collections::HashMap<String, HeartbeatEntry>,
    ) -> Result<HeartbeatResult, CorvoError> {
        self.next_client().heartbeat(jobs).await
    }

    // -- Passthrough: job management -----------------------------------------

    pub async fn get_job(&self, job_id: &str) -> Result<Value, CorvoError> {
        self.next_client().get_job(job_id).await
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<Value, CorvoError> {
        self.next_client().cancel_job(job_id).await
    }

    pub async fn move_job(&self, job_id: &str, target_queue: &str) -> Result<Value, CorvoError> {
        self.next_client().move_job(job_id, target_queue).await
    }

    pub async fn delete_job(&self, job_id: &str) -> Result<Value, CorvoError> {
        self.next_client().delete_job(job_id).await
    }

    // -- Passthrough: search & bulk ------------------------------------------

    pub async fn search(&self, filter: SearchFilter) -> Result<SearchResult, CorvoError> {
        self.next_client().search(filter).await
    }

    pub async fn bulk(&self, req: BulkRequest) -> Result<BulkResult, CorvoError> {
        self.next_client().bulk(req).await
    }

    pub async fn bulk_status(&self, id: &str) -> Result<BulkTask, CorvoError> {
        self.next_client().bulk_status(id).await
    }

    pub async fn bulk_get_jobs(&self, ids: Vec<String>) -> Result<Vec<Value>, CorvoError> {
        self.next_client().bulk_get_jobs(ids).await
    }

    // -- Passthrough: batches (server-side job batches) -----------------------

    pub async fn create_batch(
        &self,
        callback_queue: &str,
        callback_payload: Option<Value>,
    ) -> Result<CreateBatchResult, CorvoError> {
        self.next_client()
            .create_batch(callback_queue, callback_payload)
            .await
    }

    pub async fn seal_batch(&self, batch_id: &str) -> Result<SealBatchResult, CorvoError> {
        self.next_client().seal_batch(batch_id).await
    }

    // -- Passthrough: info ---------------------------------------------------

    pub async fn get_server_info(&self) -> Result<ServerInfo, CorvoError> {
        self.next_client().get_server_info().await
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        for task in &self._flush_tasks {
            task.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Flush loop -- one per lane
// ---------------------------------------------------------------------------

async fn flush_loop(
    client: CorvoClient,
    mut rx: mpsc::UnboundedReceiver<PendingEnqueue>,
    max_batch: usize,
    flush_interval: Duration,
) {
    let mut buffer: Vec<PendingEnqueue> = Vec::with_capacity(max_batch);

    loop {
        // Phase 1: Wait for at least one entry (or channel close).
        let first = match rx.recv().await {
            Some(p) => p,
            None => return, // channel closed, shut down
        };
        buffer.push(first);

        // Phase 2: Drain any already-queued entries up to max_batch.
        while buffer.len() < max_batch {
            match rx.try_recv() {
                Ok(p) => buffer.push(p),
                Err(_) => break,
            }
        }

        // Phase 3: If we haven't hit max_batch, wait up to flush_interval
        // for more entries to arrive.
        if buffer.len() < max_batch {
            let deadline = tokio::time::sleep(flush_interval);
            tokio::pin!(deadline);

            loop {
                tokio::select! {
                    biased;
                    entry = rx.recv() => {
                        match entry {
                            Some(p) => {
                                buffer.push(p);
                                if buffer.len() >= max_batch {
                                    break;
                                }
                            }
                            None => {
                                // Channel closed; flush remaining and exit.
                                flush_batch(&client, &mut buffer).await;
                                return;
                            }
                        }
                    }
                    _ = &mut deadline => {
                        break;
                    }
                }
            }
        }

        // Phase 4: Flush.
        flush_batch(&client, &mut buffer).await;
    }
}

async fn flush_batch(client: &CorvoClient, buffer: &mut Vec<PendingEnqueue>) {
    if buffer.is_empty() {
        return;
    }

    let batch: Vec<PendingEnqueue> = buffer.drain(..).collect();
    let jobs: Vec<BatchJob> = batch
        .iter()
        .map(|p| BatchJob {
            queue: p.job.queue.clone(),
            payload: p.job.payload.clone(),
        })
        .collect();

    match client.enqueue_batch(jobs, None).await {
        Ok(result) => {
            // The server returns one job_id per entry, in order.
            for (i, pending) in batch.into_iter().enumerate() {
                let id = result
                    .job_ids
                    .get(i)
                    .cloned()
                    .unwrap_or_default();
                let _ = pending.tx.send(Ok(id));
            }
        }
        Err(err) => {
            // Propagate the same error to every caller in this batch.
            let msg = err.to_string();
            for pending in batch {
                let _ = pending.tx.send(Err(CorvoError::Api(msg.clone())));
            }
        }
    }
}
