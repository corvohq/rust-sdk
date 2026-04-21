//! Binary RPC connection for Corvo's wire protocol.
//!
//! Low-level TCP connection that speaks the Corvo binary framing protocol.
//! Frame header (9 bytes): `[msg_type:u8][req_id:u32LE][payload_len:u32LE]`
//! followed by `payload_len` bytes of payload.
//!
//! Response frames mirror request frames with the high bit set on `msg_type`
//! (e.g., request `0x01` -> response `0x81`).

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

const FRAME_HEADER_SIZE: usize = 9;

// Request message types.
const MSG_ENQUEUE_BATCH: u8 = 0x01;
const MSG_FETCH_BATCH: u8 = 0x02;
const MSG_ACK_BATCH: u8 = 0x03;
const MSG_PING: u8 = 0x04;
const MSG_HEARTBEAT: u8 = 0x06;
const MSG_FAIL_BATCH: u8 = 0x07;

// Response message types.
const MSG_ENQUEUE_BATCH_RESP: u8 = 0x81;
const MSG_FETCH_BATCH_RESP: u8 = 0x82;
const MSG_ACK_BATCH_RESP: u8 = 0x83;
const MSG_PONG: u8 = 0x84;
const MSG_HEARTBEAT_RESP: u8 = 0x86;
const MSG_FAIL_BATCH_RESP: u8 = 0x87;
const MSG_ERROR: u8 = 0xFF;

const MSG_CANCEL_SIGNAL: u8 = 0x08; // server -> client push

// Bulk action request/response.
const MSG_BULK_ACTION: u8 = 0x14;
const MSG_BULK_ACTION_RESP: u8 = 0x94;

const DEFAULT_LEASE_MS: u32 = 30_000;

// Enqueue job flags bitmask.
const FLAG_PAYLOAD: u16 = 0x0001;
const FLAG_UNIQUE_KEY: u16 = 0x0002;
const FLAG_TAGS: u16 = 0x0004;
const FLAG_BATCH_ID: u16 = 0x0008;
const FLAG_CHAIN_ID: u16 = 0x0010;
const FLAG_CHAIN_CONFIG: u16 = 0x0020;
const FLAG_GROUP: u16 = 0x0040;
const FLAG_PARENT_ID: u16 = 0x0080;

// Ack flags bitmask.
const ACK_FLAG_RESULT: u8 = 0x01;
const ACK_FLAG_CHECKPOINT: u8 = 0x02;
const ACK_FLAG_HOLD_REASON: u8 = 0x04;
const ACK_FLAG_LEASE_TOKEN: u8 = 0x08;

// Heartbeat flags bitmask.
const HB_FLAG_PROGRESS: u8 = 0x01;
const HB_FLAG_CHECKPOINT: u8 = 0x02;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ConnError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("not connected")]
    NotConnected,
    #[error("server error: {0}")]
    Server(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("unexpected response type: 0x{0:02x}")]
    UnexpectedResponse(u8),
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Backoff strategy for retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Backoff {
    #[default]
    None = 0,
    Fixed = 1,
    Linear = 2,
    Exponential = 3,
}

/// Acknowledgement status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum AckStatus {
    #[default]
    Done = 0,
    Hold = 1,
}

/// A job to enqueue via binary RPC.
#[derive(Debug, Clone, Default)]
pub struct EnqueueJob {
    pub queue: String,
    pub job_id: String,
    pub priority: u8,
    pub max_retries: u16,
    pub backoff: Backoff,
    pub base_delay_ms: u32,
    pub max_delay_ms: u32,
    pub unique_period_s: u32,
    pub scheduled_at_ns: u64,
    pub expire_after_ms: u32,
    pub chain_step: u16,
    pub payload: Vec<u8>,
    pub unique_key: String,
    pub tags: String,
    pub batch_id: String,
    pub chain_id: String,
    pub chain_config: String,
    pub group: String,
    pub parent_id: String,
}

/// A job acknowledgement.
#[derive(Debug, Clone)]
pub struct AckJob {
    pub job_id: String,
    pub queue: String,
    pub ack_status: AckStatus,
    pub result: String,
    pub checkpoint: String,
    pub hold_reason: String,
    pub lease_token: u64,
}

/// A job failure report.
#[derive(Debug, Clone)]
pub struct FailJob {
    pub job_id: String,
    pub queue: String,
    pub error: String,
    pub backtrace: String,
    pub lease_token: u64,
}

/// A heartbeat update for a job.
#[derive(Debug, Clone)]
pub struct HeartbeatJob {
    pub job_id: String,
    pub queue: String,
    pub progress: String,
    pub checkpoint: String,
}

/// A job returned from a pushed fetch response (MSG_FETCH_BATCH_RESP).
#[derive(Debug, Clone)]
pub struct FetchedJob {
    pub id: String,
    pub queue: String,
    pub attempt: u16,
    pub max_retries: u16,
    pub checkpoint: Vec<u8>,
    pub tags: Vec<u8>,
    pub payload: Vec<u8>,
    pub lease_token: u64,
}

/// A frame received from the server in the message loop.
/// Use [`Conn::read_frame`] after [`Conn::subscribe`] to receive these.
#[derive(Debug)]
pub enum Frame {
    /// Pushed jobs from subscription.
    FetchResp(Vec<FetchedJob>),
    /// Ack confirmation with affected count.
    AckResp(u16),
    /// Fail confirmation with affected count.
    FailResp(u16),
    /// Server cancelled active jobs — contains cancelled job IDs.
    CancelSignal(Vec<String>),
    /// Pong response.
    Pong,
}

// ---------------------------------------------------------------------------
// Conn
// ---------------------------------------------------------------------------

/// A single TCP connection speaking the Corvo binary RPC protocol.
///
/// Not thread-safe -- use one `Conn` per task. For connection pooling, wrap
/// multiple `Conn` instances behind a higher-level abstraction.
pub struct Conn {
    stream: Option<TcpStream>,
    host: String,
    port: u16,
    req_id: u32,
    send_buf: Vec<u8>,
    recv_buf: Vec<u8>,
}

impl Conn {
    /// Create a new connection and connect to the server.
    ///
    /// Sets `TCP_NODELAY` for low-latency request/response cycles.
    pub async fn new(host: &str, port: u16) -> Self {
        let stream = TcpStream::connect((host, port)).await.ok();
        if let Some(ref s) = stream {
            let _ = s.set_nodelay(true);
        }
        Self {
            stream,
            host: host.to_string(),
            port,
            req_id: 0,
            send_buf: Vec::with_capacity(65536),
            recv_buf: Vec::with_capacity(65536),
        }
    }

    /// Close the connection.
    pub async fn close(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.shutdown().await;
        }
    }

    /// Reconnect to the server (e.g., after a transient failure).
    pub async fn reconnect(&mut self) -> Result<(), ConnError> {
        self.close().await;
        let stream = TcpStream::connect((self.host.as_str(), self.port)).await?;
        let _ = stream.set_nodelay(true);
        self.stream = Some(stream);
        Ok(())
    }

    // -- Public RPC methods --------------------------------------------------

    /// Enqueue a batch of jobs.
    ///
    /// Wire format:
    ///   [count:u16]
    ///   per job:
    ///     [queue:lenPrefixed(u8)][id:lenPrefixed(u8)][priority:u8][max_retries:u16]
    ///     [backoff:u8][base_delay_ms:u32][max_delay_ms:u32]
    ///     [unique_period_s:u32][scheduled_at_ns:u64][expire_after_ms:u32]
    ///     [chain_step:u16][flags:u16]
    ///     if flags & 0x0001: [payload:u16Prefixed]
    ///     if flags & 0x0002: [unique_key:lenPrefixed]
    ///     if flags & 0x0004: [tags:lenPrefixed]
    ///     if flags & 0x0008: [batch_id:lenPrefixed]
    ///     if flags & 0x0010: [chain_id:lenPrefixed]
    ///     if flags & 0x0020: [chain_config:lenPrefixed]
    ///     if flags & 0x0040: [group:lenPrefixed]
    ///     if flags & 0x0080: [parent_id:lenPrefixed]
    ///
    /// Returns the number of jobs the server accepted.
    pub async fn enqueue_batch(
        &mut self,
        jobs: &[EnqueueJob],
    ) -> Result<u32, ConnError> {
        let count = jobs.len() as u16;

        self.send_buf.clear();
        write_u16(&mut self.send_buf, count);

        for job in jobs {
            write_len_prefixed(&mut self.send_buf, job.queue.as_bytes());
            write_len_prefixed(&mut self.send_buf, job.job_id.as_bytes());
            self.send_buf.push(job.priority);
            write_u16(&mut self.send_buf, job.max_retries);
            self.send_buf.push(job.backoff as u8);
            write_u32(&mut self.send_buf, job.base_delay_ms);
            write_u32(&mut self.send_buf, job.max_delay_ms);
            write_u32(&mut self.send_buf, job.unique_period_s);
            write_u64(&mut self.send_buf, job.scheduled_at_ns);
            write_u32(&mut self.send_buf, job.expire_after_ms);
            write_u16(&mut self.send_buf, job.chain_step);

            // Compute flags from non-empty optional fields.
            let mut flags: u16 = 0;
            if !job.payload.is_empty() {
                flags |= FLAG_PAYLOAD;
            }
            if !job.unique_key.is_empty() {
                flags |= FLAG_UNIQUE_KEY;
            }
            if !job.tags.is_empty() {
                flags |= FLAG_TAGS;
            }
            if !job.batch_id.is_empty() {
                flags |= FLAG_BATCH_ID;
            }
            if !job.chain_id.is_empty() {
                flags |= FLAG_CHAIN_ID;
            }
            if !job.chain_config.is_empty() {
                flags |= FLAG_CHAIN_CONFIG;
            }
            if !job.group.is_empty() {
                flags |= FLAG_GROUP;
            }
            if !job.parent_id.is_empty() {
                flags |= FLAG_PARENT_ID;
            }
            write_u16(&mut self.send_buf, flags);

            // Write optional fields in flag order.
            if flags & FLAG_PAYLOAD != 0 {
                write_u16_prefixed(&mut self.send_buf, &job.payload);
            }
            if flags & FLAG_UNIQUE_KEY != 0 {
                write_len_prefixed(&mut self.send_buf, job.unique_key.as_bytes());
            }
            if flags & FLAG_TAGS != 0 {
                write_len_prefixed(&mut self.send_buf, job.tags.as_bytes());
            }
            if flags & FLAG_BATCH_ID != 0 {
                write_len_prefixed(&mut self.send_buf, job.batch_id.as_bytes());
            }
            if flags & FLAG_CHAIN_ID != 0 {
                write_len_prefixed(&mut self.send_buf, job.chain_id.as_bytes());
            }
            if flags & FLAG_CHAIN_CONFIG != 0 {
                write_len_prefixed(&mut self.send_buf, job.chain_config.as_bytes());
            }
            if flags & FLAG_GROUP != 0 {
                write_len_prefixed(&mut self.send_buf, job.group.as_bytes());
            }
            if flags & FLAG_PARENT_ID != 0 {
                write_len_prefixed(&mut self.send_buf, job.parent_id.as_bytes());
            }
        }

        self.send_frame(MSG_ENQUEUE_BATCH).await?;
        let (msg_type, payload) = self.recv_frame().await?;

        if msg_type == MSG_ERROR {
            return Err(ConnError::Server(
                String::from_utf8_lossy(payload).to_string(),
            ));
        }
        if msg_type != MSG_ENQUEUE_BATCH_RESP {
            return Err(ConnError::UnexpectedResponse(msg_type));
        }

        let mut r = BufReader::new(payload);
        let enqueued = r.read_u16()?;
        let err_code = r.read_u8()?;
        if err_code != 0 {
            return Err(ConnError::Server("enqueue batch failed".to_string()));
        }
        Ok(enqueued as u32)
    }

    /// Subscribe to jobs from one or more queues (fire-and-forget).
    ///
    /// Sends a MSG_FETCH_BATCH frame with `credits` indicating how many jobs
    /// the server may push. Does NOT read a response -- the server will push
    /// MSG_FETCH_BATCH_RESP frames asynchronously. Use [`read_pushed_jobs`]
    /// to receive them.
    ///
    /// To replenish credits, call `subscribe` again.
    ///
    /// Wire format:
    ///   [credits:u16][lease_ms:u32][worker_id:lenPrefixed]
    ///   [queue_count:u8][queues:lenPrefixed...]
    pub async fn subscribe(
        &mut self,
        queues: &[&str],
        worker_id: &str,
        credits: u16,
    ) -> Result<(), ConnError> {
        self.send_buf.clear();
        write_u16(&mut self.send_buf, credits);
        write_u32(&mut self.send_buf, DEFAULT_LEASE_MS);
        write_len_prefixed(&mut self.send_buf, worker_id.as_bytes());
        self.send_buf.push(queues.len() as u8);
        for q in queues {
            write_len_prefixed(&mut self.send_buf, q.as_bytes());
        }

        self.send_frame(MSG_FETCH_BATCH).await
    }

    /// Read pushed jobs from the server (blocking).
    ///
    /// Blocks until the server pushes a MSG_FETCH_BATCH_RESP frame containing
    /// one or more jobs. Call [`subscribe`] first to grant credits.
    ///
    /// Returns `Err` on MSG_ERROR or unexpected message types.
    pub async fn read_pushed_jobs(&mut self) -> Result<Vec<FetchedJob>, ConnError> {
        let (msg_type, payload) = self.recv_frame().await?;

        if msg_type == MSG_ERROR {
            return Err(ConnError::Server(
                String::from_utf8_lossy(payload).to_string(),
            ));
        }
        if msg_type != MSG_FETCH_BATCH_RESP {
            return Err(ConnError::UnexpectedResponse(msg_type));
        }

        decode_fetch_response(payload)
    }

    /// Read the next frame from the server, dispatching by message type.
    ///
    /// Use in a message loop after [`subscribe`] to handle interleaved
    /// FETCH_RESP, ACK_RESP, FAIL_RESP, and CANCEL_SIGNAL frames.
    pub async fn read_frame(&mut self) -> Result<Frame, ConnError> {
        let (msg_type, payload) = self.recv_frame().await?;

        match msg_type {
            MSG_FETCH_BATCH_RESP => {
                let jobs = decode_fetch_response(payload)?;
                Ok(Frame::FetchResp(jobs))
            }
            MSG_ACK_BATCH_RESP => {
                let mut r = BufReader::new(payload);
                let affected = r.read_u16().unwrap_or(0);
                Ok(Frame::AckResp(affected))
            }
            MSG_FAIL_BATCH_RESP => {
                let mut r = BufReader::new(payload);
                let affected = r.read_u16().unwrap_or(0);
                Ok(Frame::FailResp(affected))
            }
            MSG_CANCEL_SIGNAL => {
                let mut r = BufReader::new(payload);
                let count = r.read_u16().unwrap_or(0);
                let mut ids = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    match r.read_len_prefixed() {
                        Ok(id_bytes) => ids.push(String::from_utf8_lossy(id_bytes).to_string()),
                        Err(_) => break,
                    }
                }
                Ok(Frame::CancelSignal(ids))
            }
            MSG_PONG => Ok(Frame::Pong),
            MSG_ERROR => {
                Err(ConnError::Server(
                    String::from_utf8_lossy(payload).to_string(),
                ))
            }
            _ => Err(ConnError::UnexpectedResponse(msg_type)),
        }
    }

    /// Send ack batch without waiting for response (fire-and-forget).
    ///
    /// The response will arrive via [`read_frame`] as [`Frame::AckResp`].
    pub async fn send_ack(&mut self, acks: &[AckJob]) -> Result<(), ConnError> {
        self.send_buf.clear();
        encode_ack_payload(&mut self.send_buf, acks);
        self.send_frame(MSG_ACK_BATCH).await
    }

    /// Send fail batch without waiting for response (fire-and-forget).
    ///
    /// The response will arrive via [`read_frame`] as [`Frame::FailResp`].
    pub async fn send_fail(&mut self, jobs: &[FailJob]) -> Result<(), ConnError> {
        self.send_buf.clear();
        encode_fail_payload(&mut self.send_buf, jobs);
        self.send_frame(MSG_FAIL_BATCH).await
    }

    /// Cancel jobs by ID via MSG_BULK_ACTION. Waits for server confirmation.
    ///
    /// Returns the number of jobs cancelled.
    pub async fn cancel(&mut self, job_ids: &[&str]) -> Result<u16, ConnError> {
        self.send_buf.clear();
        // Wire: [action:u8][queue:lenPrefixed][count:u16][{id:lenPrefixed}...][flags:u8][now_ns:u64]
        self.send_buf.push(3); // BulkAction.cancel = 3
        write_len_prefixed(&mut self.send_buf, b""); // queue (empty)
        write_u16(&mut self.send_buf, job_ids.len() as u16);
        for id in job_ids {
            write_len_prefixed(&mut self.send_buf, id.as_bytes());
        }
        self.send_buf.push(0); // flags
        write_u64(&mut self.send_buf, 0); // now_ns (server uses its own clock)

        self.send_frame(MSG_BULK_ACTION).await?;
        let (msg_type, payload) = self.recv_frame().await?;

        if msg_type == MSG_ERROR {
            return Err(ConnError::Server(
                String::from_utf8_lossy(payload).to_string(),
            ));
        }
        if msg_type != MSG_BULK_ACTION_RESP {
            return Err(ConnError::UnexpectedResponse(msg_type));
        }

        let mut r = BufReader::new(payload);
        let affected = r.read_u16().unwrap_or(0);
        Ok(affected)
    }

    /// Acknowledge a batch of jobs.
    ///
    /// Wire format:
    ///   [count:u16]
    ///   per ack:
    ///     [job_id:lenPrefixed][queue:lenPrefixed]
    ///     [ack_status:u8][flags:u8]
    ///     if flags & 0x01: [result:lenPrefixed]
    ///     if flags & 0x02: [checkpoint:lenPrefixed]
    ///     if flags & 0x04: [hold_reason:lenPrefixed]
    ///     if flags & 0x08: [lease_token:u64LE]
    pub async fn ack_batch(
        &mut self,
        acks: &[AckJob],
    ) -> Result<(), ConnError> {
        self.send_buf.clear();
        encode_ack_payload(&mut self.send_buf, acks);

        self.send_frame(MSG_ACK_BATCH).await?;
        let (msg_type, payload) = self.recv_frame().await?;

        if msg_type == MSG_ERROR {
            return Err(ConnError::Server(
                String::from_utf8_lossy(payload).to_string(),
            ));
        }
        if msg_type != MSG_ACK_BATCH_RESP {
            return Err(ConnError::UnexpectedResponse(msg_type));
        }

        let mut r = BufReader::new(payload);
        let _acked = r.read_u16()?;
        let err_code = r.read_u8()?;
        if err_code != 0 {
            return Err(ConnError::Server("ack batch failed".to_string()));
        }
        Ok(())
    }

    /// Fail a batch of jobs.
    ///
    /// Wire format:
    ///   [count:u16]
    ///   per job:
    ///     [job_id:lenPrefixed][queue:lenPrefixed]
    ///     [error_msg:lenPrefixed][backtrace:lenPrefixed]
    ///     [flags:u8][if flags & 0x01: lease_token:u64LE]
    pub async fn fail_batch(
        &mut self,
        jobs: &[FailJob],
    ) -> Result<(), ConnError> {
        self.send_buf.clear();
        encode_fail_payload(&mut self.send_buf, jobs);

        self.send_frame(MSG_FAIL_BATCH).await?;
        let (msg_type, payload) = self.recv_frame().await?;

        if msg_type == MSG_ERROR {
            return Err(ConnError::Server(
                String::from_utf8_lossy(payload).to_string(),
            ));
        }
        if msg_type != MSG_FAIL_BATCH_RESP {
            return Err(ConnError::UnexpectedResponse(msg_type));
        }

        let mut r = BufReader::new(payload);
        let _count = r.read_u16()?;
        let err_code = r.read_u8()?;
        if err_code != 0 {
            return Err(ConnError::Server("fail batch failed".to_string()));
        }
        Ok(())
    }

    /// Send heartbeats for active jobs. Extends leases and optionally updates
    /// progress/checkpoint.
    ///
    /// Wire format:
    ///   [worker_id:lenPrefixed][count:u16]
    ///   per job:
    ///     [job_id:lenPrefixed][queue:lenPrefixed]
    ///     [flags:u8]
    ///     if flags & 0x01: [progress:lenPrefixed]
    ///     if flags & 0x02: [checkpoint:lenPrefixed]
    pub async fn heartbeat(
        &mut self,
        worker_id: &str,
        jobs: &[HeartbeatJob],
    ) -> Result<(), ConnError> {
        self.send_buf.clear();
        write_len_prefixed(&mut self.send_buf, worker_id.as_bytes());
        write_u16(&mut self.send_buf, jobs.len() as u16);

        for job in jobs {
            write_len_prefixed(&mut self.send_buf, job.job_id.as_bytes());
            write_len_prefixed(&mut self.send_buf, job.queue.as_bytes());

            let mut flags: u8 = 0;
            if !job.progress.is_empty() {
                flags |= HB_FLAG_PROGRESS;
            }
            if !job.checkpoint.is_empty() {
                flags |= HB_FLAG_CHECKPOINT;
            }
            self.send_buf.push(flags);

            if flags & HB_FLAG_PROGRESS != 0 {
                write_len_prefixed(&mut self.send_buf, job.progress.as_bytes());
            }
            if flags & HB_FLAG_CHECKPOINT != 0 {
                write_len_prefixed(&mut self.send_buf, job.checkpoint.as_bytes());
            }
        }

        self.send_frame(MSG_HEARTBEAT).await?;
        let (msg_type, payload) = self.recv_frame().await?;

        if msg_type == MSG_ERROR {
            return Err(ConnError::Server(
                String::from_utf8_lossy(payload).to_string(),
            ));
        }
        if msg_type != MSG_HEARTBEAT_RESP {
            return Err(ConnError::UnexpectedResponse(msg_type));
        }

        let mut r = BufReader::new(payload);
        let _affected = r.read_u16()?;
        let err_code = r.read_u8()?;
        if err_code != 0 {
            return Err(ConnError::Server("heartbeat failed".to_string()));
        }
        Ok(())
    }

    /// Ping the server. Returns `Ok(())` on pong.
    pub async fn ping(&mut self) -> Result<(), ConnError> {
        self.send_buf.clear();

        self.send_frame(MSG_PING).await?;
        let (msg_type, payload) = self.recv_frame().await?;

        if msg_type == MSG_ERROR {
            return Err(ConnError::Server(
                String::from_utf8_lossy(payload).to_string(),
            ));
        }
        if msg_type != MSG_PONG {
            return Err(ConnError::UnexpectedResponse(msg_type));
        }
        Ok(())
    }

    // -- Frame I/O -----------------------------------------------------------

    /// Write the 9-byte frame header + payload from `self.send_buf` as a
    /// single contiguous TCP write. With TCP_NODELAY, separate writes would
    /// each become their own TCP segment, causing partial-frame reads on the
    /// server (io_uring completes recv on the 9-byte header alone).
    async fn send_frame(&mut self, msg_type: u8) -> Result<(), ConnError> {
        let stream = self.stream.as_mut().ok_or(ConnError::NotConnected)?;

        self.req_id = self.req_id.wrapping_add(1);
        let payload_len = self.send_buf.len() as u32;

        // Build contiguous frame: header + payload in one allocation.
        let frame_len = FRAME_HEADER_SIZE + self.send_buf.len();
        let mut frame = Vec::with_capacity(frame_len);
        frame.push(msg_type);
        frame.extend_from_slice(&self.req_id.to_le_bytes());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&self.send_buf);

        stream.write_all(&frame).await?;
        stream.flush().await?;
        Ok(())
    }

    /// Read a response frame. Returns `(msg_type, payload_slice)`.
    ///
    /// The returned slice borrows from `self.recv_buf` and is valid until the
    /// next call to `recv_frame`.
    async fn recv_frame(&mut self) -> Result<(u8, &[u8]), ConnError> {
        let stream = self.stream.as_mut().ok_or(ConnError::NotConnected)?;

        let mut header = [0u8; FRAME_HEADER_SIZE];
        stream.read_exact(&mut header).await?;

        let msg_type = header[0];
        let _req_id = u32::from_le_bytes([header[1], header[2], header[3], header[4]]);
        let length = u32::from_le_bytes([header[5], header[6], header[7], header[8]]) as usize;

        self.recv_buf.resize(length, 0);
        if length > 0 {
            stream.read_exact(&mut self.recv_buf[..length]).await?;
        }

        Ok((msg_type, &self.recv_buf[..length]))
    }
}

// ---------------------------------------------------------------------------
// Shared decode helper for fetch responses
// ---------------------------------------------------------------------------

/// Decode a MSG_FETCH_BATCH_RESP payload into a vec of fetched jobs.
fn decode_fetch_response(payload: &[u8]) -> Result<Vec<FetchedJob>, ConnError> {
    let mut r = BufReader::new(payload);
    let count = r.read_u16()?;
    let mut jobs = Vec::with_capacity(count as usize);

    for _ in 0..count {
        let id = r.read_len_prefixed()?;
        let queue = r.read_len_prefixed()?;
        let attempt = r.read_u16()?;
        let max_retries = r.read_u16()?;
        let checkpoint = r.read_len_prefixed()?;
        let tags = r.read_len_prefixed()?;
        let payload_len = r.read_u16()? as usize;
        let payload_bytes = r.read_bytes(payload_len)?;
        let lease_token = r.read_u64()?;

        jobs.push(FetchedJob {
            id: String::from_utf8_lossy(id).to_string(),
            queue: String::from_utf8_lossy(queue).to_string(),
            attempt,
            max_retries,
            checkpoint: checkpoint.to_vec(),
            tags: tags.to_vec(),
            payload: payload_bytes.to_vec(),
            lease_token,
        });
    }

    Ok(jobs)
}

// ---------------------------------------------------------------------------
// Binary encoding helpers
// ---------------------------------------------------------------------------

fn write_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Write a length-prefixed byte string: `[len:u8][bytes...]`.
fn write_len_prefixed(buf: &mut Vec<u8>, data: &[u8]) {
    buf.push(data.len() as u8);
    buf.extend_from_slice(data);
}

/// Write a u16-length-prefixed byte string: `[len:u16LE][bytes...]`.
/// Used for payload fields which can exceed 255 bytes.
fn write_u16_prefixed(buf: &mut Vec<u8>, data: &[u8]) {
    write_u16(buf, data.len() as u16);
    buf.extend_from_slice(data);
}

// ---------------------------------------------------------------------------
// Binary decoding helpers
// ---------------------------------------------------------------------------

struct BufReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BufReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, ConnError> {
        if self.pos >= self.data.len() {
            return Err(ConnError::Protocol("short read: u8".to_string()));
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_u16(&mut self) -> Result<u16, ConnError> {
        if self.pos + 2 > self.data.len() {
            return Err(ConnError::Protocol("short read: u16".to_string()));
        }
        let v = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_u64(&mut self) -> Result<u64, ConnError> {
        if self.pos + 8 > self.data.len() {
            return Err(ConnError::Protocol("short read: u64".to_string()));
        }
        let v = u64::from_le_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(v)
    }

    fn read_len_prefixed(&mut self) -> Result<&'a [u8], ConnError> {
        let len = self.read_u8()? as usize;
        self.read_bytes(len)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], ConnError> {
        if self.pos + len > self.data.len() {
            return Err(ConnError::Protocol(format!(
                "short read: {} bytes at offset {}",
                len, self.pos
            )));
        }
        let data = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(data)
    }
}

// ---------------------------------------------------------------------------
// Extracted encoding helpers (used by Conn methods and tests)
// ---------------------------------------------------------------------------

/// Encode an ack batch payload into `buf`.
///
/// Wire format:
///   [count:u16]
///   per ack:
///     [job_id:lenPrefixed][queue:lenPrefixed]
///     [ack_status:u8][flags:u8]
///     if flags & 0x01: [result:lenPrefixed]
///     if flags & 0x02: [checkpoint:lenPrefixed]
///     if flags & 0x04: [hold_reason:lenPrefixed]
///     if flags & 0x08: [lease_token:u64LE]
fn encode_ack_payload(buf: &mut Vec<u8>, acks: &[AckJob]) {
    write_u16(buf, acks.len() as u16);

    for ack in acks {
        write_len_prefixed(buf, ack.job_id.as_bytes());
        write_len_prefixed(buf, ack.queue.as_bytes());
        buf.push(ack.ack_status as u8);

        let mut flags: u8 = 0;
        if !ack.result.is_empty() {
            flags |= ACK_FLAG_RESULT;
        }
        if !ack.checkpoint.is_empty() {
            flags |= ACK_FLAG_CHECKPOINT;
        }
        if !ack.hold_reason.is_empty() {
            flags |= ACK_FLAG_HOLD_REASON;
        }
        if ack.lease_token != 0 {
            flags |= ACK_FLAG_LEASE_TOKEN;
        }
        buf.push(flags);

        if flags & ACK_FLAG_RESULT != 0 {
            write_len_prefixed(buf, ack.result.as_bytes());
        }
        if flags & ACK_FLAG_CHECKPOINT != 0 {
            write_len_prefixed(buf, ack.checkpoint.as_bytes());
        }
        if flags & ACK_FLAG_HOLD_REASON != 0 {
            write_len_prefixed(buf, ack.hold_reason.as_bytes());
        }
        if flags & ACK_FLAG_LEASE_TOKEN != 0 {
            write_u64(buf, ack.lease_token);
        }
    }
}

/// Encode a fail batch payload into `buf`.
///
/// Wire format:
///   [count:u16]
///   per job:
///     [job_id:lenPrefixed][queue:lenPrefixed]
///     [error_msg:lenPrefixed][backtrace:lenPrefixed]
///     [flags:u8][if flags & 0x01: lease_token:u64LE]
fn encode_fail_payload(buf: &mut Vec<u8>, jobs: &[FailJob]) {
    write_u16(buf, jobs.len() as u16);

    for job in jobs {
        write_len_prefixed(buf, job.job_id.as_bytes());
        write_len_prefixed(buf, job.queue.as_bytes());
        write_len_prefixed(buf, job.error.as_bytes());
        write_len_prefixed(buf, job.backtrace.as_bytes());

        let flags: u8 = if job.lease_token != 0 { 0x01 } else { 0x00 };
        buf.push(flags);
        if flags & 0x01 != 0 {
            write_u64(buf, job.lease_token);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a raw fetch-response payload for a single job with the given fields.
    fn build_fetch_response(
        id: &str,
        queue: &str,
        attempt: u16,
        max_retries: u16,
        checkpoint: &[u8],
        tags: &[u8],
        payload: &[u8],
        lease_token: u64,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        // count = 1
        write_u16(&mut buf, 1);
        write_len_prefixed(&mut buf, id.as_bytes());
        write_len_prefixed(&mut buf, queue.as_bytes());
        write_u16(&mut buf, attempt);
        write_u16(&mut buf, max_retries);
        write_len_prefixed(&mut buf, checkpoint);
        write_len_prefixed(&mut buf, tags);
        // payload is u16-prefixed
        write_u16(&mut buf, payload.len() as u16);
        buf.extend_from_slice(payload);
        write_u64(&mut buf, lease_token);
        buf
    }

    // -- Fetch response parsing -----------------------------------------------

    #[test]
    fn decode_fetch_response_with_lease_token() {
        let token: u64 = 0xDEAD_BEEF_CAFE_BABE;
        let raw = build_fetch_response(
            "job-1",
            "default",
            2,
            5,
            b"chk",
            b"t1",
            b"hello",
            token,
        );

        let jobs = decode_fetch_response(&raw).unwrap();
        assert_eq!(jobs.len(), 1);

        let job = &jobs[0];
        assert_eq!(job.id, "job-1");
        assert_eq!(job.queue, "default");
        assert_eq!(job.attempt, 2);
        assert_eq!(job.max_retries, 5);
        assert_eq!(job.checkpoint, b"chk");
        assert_eq!(job.tags, b"t1");
        assert_eq!(job.payload, b"hello");
        assert_eq!(job.lease_token, token);
    }

    #[test]
    fn decode_fetch_response_lease_token_zero() {
        let raw = build_fetch_response("j2", "q", 0, 3, b"", b"", b"data", 0);
        let jobs = decode_fetch_response(&raw).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].lease_token, 0);
        assert_eq!(jobs[0].payload, b"data");
    }

    #[test]
    fn decode_fetch_response_multiple_jobs() {
        let mut buf = Vec::new();
        write_u16(&mut buf, 2); // count = 2

        // Job 1
        let t1: u64 = 111;
        write_len_prefixed(&mut buf, b"a");
        write_len_prefixed(&mut buf, b"q1");
        write_u16(&mut buf, 1);
        write_u16(&mut buf, 3);
        write_len_prefixed(&mut buf, b"");
        write_len_prefixed(&mut buf, b"");
        write_u16(&mut buf, 0); // empty payload
        write_u64(&mut buf, t1);

        // Job 2
        let t2: u64 = 222;
        write_len_prefixed(&mut buf, b"b");
        write_len_prefixed(&mut buf, b"q2");
        write_u16(&mut buf, 0);
        write_u16(&mut buf, 0);
        write_len_prefixed(&mut buf, b"cp");
        write_len_prefixed(&mut buf, b"tag");
        write_u16(&mut buf, 3);
        buf.extend_from_slice(b"xyz");
        write_u64(&mut buf, t2);

        let jobs = decode_fetch_response(&buf).unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].id, "a");
        assert_eq!(jobs[0].lease_token, 111);
        assert_eq!(jobs[1].id, "b");
        assert_eq!(jobs[1].lease_token, 222);
        assert_eq!(jobs[1].payload, b"xyz");
    }

    #[test]
    fn decode_fetch_response_lease_token_byte_order() {
        // Verify the token is read as little-endian by checking raw bytes.
        let token: u64 = 0x0102_0304_0506_0708;
        let raw = build_fetch_response("j", "q", 0, 0, b"", b"", b"", token);

        // The last 8 bytes should be the LE encoding of the token.
        let last8 = &raw[raw.len() - 8..];
        assert_eq!(last8, &token.to_le_bytes());

        let jobs = decode_fetch_response(&raw).unwrap();
        assert_eq!(jobs[0].lease_token, token);
    }

    // -- Ack encoding ---------------------------------------------------------

    #[test]
    fn encode_ack_with_lease_token() {
        let token: u64 = 0xAABB_CCDD_1122_3344;
        let ack = AckJob {
            job_id: "j1".to_string(),
            queue: "q1".to_string(),
            ack_status: AckStatus::Done,
            result: String::new(),
            checkpoint: String::new(),
            hold_reason: String::new(),
            lease_token: token,
        };

        let mut buf = Vec::new();
        encode_ack_payload(&mut buf, &[ack]);

        // Parse the buffer to verify structure.
        let mut r = BufReader::new(&buf);

        // count
        assert_eq!(r.read_u16().unwrap(), 1);
        // job_id
        let id = r.read_len_prefixed().unwrap();
        assert_eq!(id, b"j1");
        // queue
        let q = r.read_len_prefixed().unwrap();
        assert_eq!(q, b"q1");
        // ack_status
        assert_eq!(r.read_u8().unwrap(), 0); // Done
        // flags -- only lease_token is set
        let flags = r.read_u8().unwrap();
        assert_eq!(flags, ACK_FLAG_LEASE_TOKEN); // 0x08
        assert_eq!(flags & ACK_FLAG_RESULT, 0);
        assert_eq!(flags & ACK_FLAG_CHECKPOINT, 0);
        assert_eq!(flags & ACK_FLAG_HOLD_REASON, 0);
        // lease_token
        assert_eq!(r.read_u64().unwrap(), token);
        // should be at end
        assert_eq!(r.pos, buf.len());
    }

    #[test]
    fn encode_ack_with_all_optional_fields_and_lease_token() {
        let token: u64 = 42;
        let ack = AckJob {
            job_id: "j2".to_string(),
            queue: "q2".to_string(),
            ack_status: AckStatus::Hold,
            result: "ok".to_string(),
            checkpoint: "cp".to_string(),
            hold_reason: "wait".to_string(),
            lease_token: token,
        };

        let mut buf = Vec::new();
        encode_ack_payload(&mut buf, &[ack]);

        let mut r = BufReader::new(&buf);
        assert_eq!(r.read_u16().unwrap(), 1); // count
        let _ = r.read_len_prefixed().unwrap(); // job_id
        let _ = r.read_len_prefixed().unwrap(); // queue
        assert_eq!(r.read_u8().unwrap(), 1); // Hold
        let flags = r.read_u8().unwrap();
        assert_eq!(
            flags,
            ACK_FLAG_RESULT | ACK_FLAG_CHECKPOINT | ACK_FLAG_HOLD_REASON | ACK_FLAG_LEASE_TOKEN
        );
        // result
        assert_eq!(r.read_len_prefixed().unwrap(), b"ok");
        // checkpoint
        assert_eq!(r.read_len_prefixed().unwrap(), b"cp");
        // hold_reason
        assert_eq!(r.read_len_prefixed().unwrap(), b"wait");
        // lease_token
        assert_eq!(r.read_u64().unwrap(), 42);
        assert_eq!(r.pos, buf.len());
    }

    #[test]
    fn encode_ack_lease_token_zero_omits_flag() {
        let ack = AckJob {
            job_id: "j3".to_string(),
            queue: "q3".to_string(),
            ack_status: AckStatus::Done,
            result: String::new(),
            checkpoint: String::new(),
            hold_reason: String::new(),
            lease_token: 0,
        };

        let mut buf = Vec::new();
        encode_ack_payload(&mut buf, &[ack]);

        let mut r = BufReader::new(&buf);
        assert_eq!(r.read_u16().unwrap(), 1);
        let _ = r.read_len_prefixed().unwrap(); // job_id
        let _ = r.read_len_prefixed().unwrap(); // queue
        assert_eq!(r.read_u8().unwrap(), 0); // Done
        let flags = r.read_u8().unwrap();
        // No flags should be set at all.
        assert_eq!(flags, 0x00);
        assert_eq!(flags & ACK_FLAG_LEASE_TOKEN, 0, "flag 0x08 must NOT be set when lease_token is 0");
        // Nothing more to read.
        assert_eq!(r.pos, buf.len());
    }

    // -- Fail encoding --------------------------------------------------------

    #[test]
    fn encode_fail_with_lease_token() {
        let token: u64 = 0x1234_5678_9ABC_DEF0;
        let fail = FailJob {
            job_id: "fj1".to_string(),
            queue: "fq1".to_string(),
            error: "boom".to_string(),
            backtrace: "bt".to_string(),
            lease_token: token,
        };

        let mut buf = Vec::new();
        encode_fail_payload(&mut buf, &[fail]);

        let mut r = BufReader::new(&buf);
        assert_eq!(r.read_u16().unwrap(), 1); // count
        assert_eq!(r.read_len_prefixed().unwrap(), b"fj1");
        assert_eq!(r.read_len_prefixed().unwrap(), b"fq1");
        assert_eq!(r.read_len_prefixed().unwrap(), b"boom");
        assert_eq!(r.read_len_prefixed().unwrap(), b"bt");
        let flags = r.read_u8().unwrap();
        assert_eq!(flags, 0x01);
        assert_eq!(r.read_u64().unwrap(), token);
        assert_eq!(r.pos, buf.len());
    }

    #[test]
    fn encode_fail_lease_token_zero_omits_token() {
        let fail = FailJob {
            job_id: "fj2".to_string(),
            queue: "fq2".to_string(),
            error: "err".to_string(),
            backtrace: String::new(),
            lease_token: 0,
        };

        let mut buf = Vec::new();
        encode_fail_payload(&mut buf, &[fail]);

        let mut r = BufReader::new(&buf);
        assert_eq!(r.read_u16().unwrap(), 1);
        let _ = r.read_len_prefixed().unwrap(); // job_id
        let _ = r.read_len_prefixed().unwrap(); // queue
        let _ = r.read_len_prefixed().unwrap(); // error
        let _ = r.read_len_prefixed().unwrap(); // backtrace
        let flags = r.read_u8().unwrap();
        assert_eq!(flags, 0x00, "flags must be 0x00 when lease_token is 0");
        // No lease_token bytes follow.
        assert_eq!(r.pos, buf.len());
    }

    #[test]
    fn encode_fail_lease_token_byte_order() {
        let token: u64 = 0x0807_0605_0403_0201;
        let fail = FailJob {
            job_id: "x".to_string(),
            queue: "y".to_string(),
            error: "e".to_string(),
            backtrace: String::new(),
            lease_token: token,
        };

        let mut buf = Vec::new();
        encode_fail_payload(&mut buf, &[fail]);

        // The last 8 bytes should be the LE encoding of the token.
        let last8 = &buf[buf.len() - 8..];
        assert_eq!(last8, &token.to_le_bytes());
    }

    // -- Round-trip: encode then decode ack payload via BufReader --------------

    #[test]
    fn ack_round_trip_multiple_jobs() {
        let acks = vec![
            AckJob {
                job_id: "a1".to_string(),
                queue: "qa".to_string(),
                ack_status: AckStatus::Done,
                result: "r1".to_string(),
                checkpoint: String::new(),
                hold_reason: String::new(),
                lease_token: 100,
            },
            AckJob {
                job_id: "a2".to_string(),
                queue: "qb".to_string(),
                ack_status: AckStatus::Hold,
                result: String::new(),
                checkpoint: "chk".to_string(),
                hold_reason: "reason".to_string(),
                lease_token: 0,
            },
        ];

        let mut buf = Vec::new();
        encode_ack_payload(&mut buf, &acks);

        let mut r = BufReader::new(&buf);
        let count = r.read_u16().unwrap();
        assert_eq!(count, 2);

        // Ack 1: has result + lease_token
        let id1 = r.read_len_prefixed().unwrap();
        assert_eq!(id1, b"a1");
        let _ = r.read_len_prefixed().unwrap(); // queue
        assert_eq!(r.read_u8().unwrap(), 0); // Done
        let f1 = r.read_u8().unwrap();
        assert_eq!(f1 & ACK_FLAG_RESULT, ACK_FLAG_RESULT);
        assert_eq!(f1 & ACK_FLAG_LEASE_TOKEN, ACK_FLAG_LEASE_TOKEN);
        assert_eq!(r.read_len_prefixed().unwrap(), b"r1"); // result
        assert_eq!(r.read_u64().unwrap(), 100); // lease_token

        // Ack 2: has checkpoint + hold_reason, no lease_token
        let id2 = r.read_len_prefixed().unwrap();
        assert_eq!(id2, b"a2");
        let _ = r.read_len_prefixed().unwrap(); // queue
        assert_eq!(r.read_u8().unwrap(), 1); // Hold
        let f2 = r.read_u8().unwrap();
        assert_eq!(f2 & ACK_FLAG_CHECKPOINT, ACK_FLAG_CHECKPOINT);
        assert_eq!(f2 & ACK_FLAG_HOLD_REASON, ACK_FLAG_HOLD_REASON);
        assert_eq!(f2 & ACK_FLAG_LEASE_TOKEN, 0);
        assert_eq!(r.read_len_prefixed().unwrap(), b"chk");
        assert_eq!(r.read_len_prefixed().unwrap(), b"reason");

        assert_eq!(r.pos, buf.len());
    }
}

