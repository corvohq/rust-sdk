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
}

/// A job failure report.
#[derive(Debug, Clone)]
pub struct FailJob {
    pub job_id: String,
    pub queue: String,
    pub error: String,
    pub backtrace: String,
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
    pub async fn ack_batch(
        &mut self,
        acks: &[AckJob],
    ) -> Result<(), ConnError> {
        self.send_buf.clear();
        write_u16(&mut self.send_buf, acks.len() as u16);

        for ack in acks {
            write_len_prefixed(&mut self.send_buf, ack.job_id.as_bytes());
            write_len_prefixed(&mut self.send_buf, ack.queue.as_bytes());
            self.send_buf.push(ack.ack_status as u8);

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
            self.send_buf.push(flags);

            if flags & ACK_FLAG_RESULT != 0 {
                write_len_prefixed(&mut self.send_buf, ack.result.as_bytes());
            }
            if flags & ACK_FLAG_CHECKPOINT != 0 {
                write_len_prefixed(&mut self.send_buf, ack.checkpoint.as_bytes());
            }
            if flags & ACK_FLAG_HOLD_REASON != 0 {
                write_len_prefixed(&mut self.send_buf, ack.hold_reason.as_bytes());
            }
        }

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
    pub async fn fail_batch(
        &mut self,
        jobs: &[FailJob],
    ) -> Result<(), ConnError> {
        self.send_buf.clear();
        write_u16(&mut self.send_buf, jobs.len() as u16);

        for job in jobs {
            write_len_prefixed(&mut self.send_buf, job.job_id.as_bytes());
            write_len_prefixed(&mut self.send_buf, job.queue.as_bytes());
            write_len_prefixed(&mut self.send_buf, job.error.as_bytes());
            write_len_prefixed(&mut self.send_buf, job.backtrace.as_bytes());
        }

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

    /// Write the 9-byte frame header + payload from `self.send_buf`.
    async fn send_frame(&mut self, msg_type: u8) -> Result<(), ConnError> {
        let stream = self.stream.as_mut().ok_or(ConnError::NotConnected)?;

        self.req_id = self.req_id.wrapping_add(1);
        let payload_len = self.send_buf.len() as u32;

        let mut header = [0u8; FRAME_HEADER_SIZE];
        header[0] = msg_type;
        header[1..5].copy_from_slice(&self.req_id.to_le_bytes());
        header[5..9].copy_from_slice(&payload_len.to_le_bytes());

        stream.write_all(&header).await?;
        if !self.send_buf.is_empty() {
            stream.write_all(&self.send_buf).await?;
        }
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

        jobs.push(FetchedJob {
            id: String::from_utf8_lossy(id).to_string(),
            queue: String::from_utf8_lossy(queue).to_string(),
            attempt,
            max_retries,
            checkpoint: checkpoint.to_vec(),
            tags: tags.to_vec(),
            payload: payload_bytes.to_vec(),
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

