//! gRPC-based RPC client for the Corvo worker.
//!
//! Mirrors the Go reference implementation at go-sdk/rpc/client.go.
//! Uses tonic for full bidirectional streaming support.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};

use crate::gen::corvo::v1::{
    self as pb, worker_service_client::WorkerServiceClient, AckBatchItem, FailRequest,
    HeartbeatJobUpdate, HeartbeatRequest, LifecycleEnqueueItem, LifecycleStreamRequest,
    LifecycleStreamResponse,
};

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc error: {0}")]
    Status(#[from] tonic::Status),
    #[error("NOT_LEADER: leader is at {leader_addr}")]
    NotLeader { leader_addr: String },
    #[error("stream closed")]
    StreamClosed,
}

// ---------------------------------------------------------------------------
// Auth options
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct RpcAuthOptions {
    pub headers: Vec<(String, String)>,
    pub bearer_token: Option<String>,
    pub api_key: Option<String>,
    pub api_key_header: Option<String>,
}

const SDK_NAME: &str = "corvo-rust";
const SDK_VERSION: &str = "0.3.0";

fn build_metadata(auth: &RpcAuthOptions) -> MetadataMap {
    let mut map = MetadataMap::new();
    map.insert("x-corvo-client-name", SDK_NAME.parse().unwrap());
    map.insert("x-corvo-client-version", SDK_VERSION.parse().unwrap());
    for (k, v) in &auth.headers {
        if let (Ok(name), Ok(val)) = (k.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>(), v.parse()) {
            map.insert(name, val);
        }
    }
    if let Some(key) = &auth.api_key {
        let header = auth
            .api_key_header
            .as_deref()
            .unwrap_or("x-api-key");
        if let (Ok(name), Ok(val)) = (header.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>(), key.parse()) {
            map.insert(name, val);
        }
    }
    if let Some(token) = &auth.bearer_token {
        if let Ok(val) = format!("Bearer {token}").parse() {
            map.insert("authorization", val);
        }
    }
    map
}

// ---------------------------------------------------------------------------
// RPC Client
// ---------------------------------------------------------------------------

pub struct RpcClient {
    client: WorkerServiceClient<Channel>,
    auth: RpcAuthOptions,
}

impl RpcClient {
    pub async fn new(url: &str, auth: RpcAuthOptions) -> Result<Self, RpcError> {
        let endpoint = Endpoint::from_shared(url.to_string())?
            .tcp_keepalive(Some(std::time::Duration::from_secs(30)));
        let channel = endpoint.connect_lazy();
        Ok(Self {
            client: WorkerServiceClient::new(channel),
            auth,
        })
    }

    fn request<T>(&self, msg: T) -> Request<T> {
        let mut req = Request::new(msg);
        let md = build_metadata(&self.auth);
        *req.metadata_mut() = md;
        req
    }

    pub async fn enqueue(
        &mut self,
        queue: &str,
        payload_json: &str,
    ) -> Result<pb::EnqueueResponse, RpcError> {
        let resp = self
            .client
            .enqueue(self.request(pb::EnqueueRequest {
                queue: queue.to_string(),
                payload_json: payload_json.to_string(),
                agent: None,
            }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn fail(
        &mut self,
        job_id: &str,
        error: &str,
        backtrace: &str,
    ) -> Result<pb::FailResponse, RpcError> {
        let resp = self
            .client
            .fail(self.request(FailRequest {
                job_id: job_id.to_string(),
                error: error.to_string(),
                backtrace: backtrace.to_string(),
            }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn heartbeat(
        &mut self,
        jobs: HashMap<String, HeartbeatJobUpdate>,
    ) -> Result<HashMap<String, String>, RpcError> {
        let resp = self
            .client
            .heartbeat(self.request(HeartbeatRequest { jobs }))
            .await?;
        Ok(resp
            .into_inner()
            .jobs
            .into_iter()
            .map(|(k, v)| (k, v.status))
            .collect())
    }

    pub async fn open_lifecycle_stream(
        &mut self,
    ) -> Result<LifecycleStream, RpcError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<LifecycleStreamRequest>(16);
        let rx_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut req = Request::new(rx_stream);
        *req.metadata_mut() = build_metadata(&self.auth);
        let response = self.client.stream_lifecycle(req).await?;
        Ok(LifecycleStream {
            tx,
            rx: response.into_inner(),
        })
    }
}

// ---------------------------------------------------------------------------
// Lifecycle types
// ---------------------------------------------------------------------------

pub struct LifecycleRequest {
    pub request_id: u64,
    pub queues: Vec<String>,
    pub worker_id: String,
    pub hostname: String,
    pub lease_duration: i32,
    pub fetch_count: i32,
    pub acks: Vec<AckBatchItem>,
    pub enqueues: Vec<LifecycleEnqueueItem>,
}

pub struct LifecycleResponse {
    pub request_id: u64,
    pub jobs: Vec<pb::FetchBatchJob>,
    pub acked: i32,
    pub enqueued_job_ids: Vec<String>,
    pub error: String,
    pub leader_addr: String,
}

// ---------------------------------------------------------------------------
// LifecycleStream
// ---------------------------------------------------------------------------

pub struct LifecycleStream {
    tx: tokio::sync::mpsc::Sender<LifecycleStreamRequest>,
    rx: Streaming<LifecycleStreamResponse>,
}

impl LifecycleStream {
    pub async fn exchange(&mut self, req: LifecycleRequest) -> Result<LifecycleResponse, RpcError> {
        let proto_req = LifecycleStreamRequest {
            request_id: req.request_id,
            queues: req.queues,
            worker_id: req.worker_id,
            hostname: req.hostname,
            lease_duration: req.lease_duration,
            fetch_count: req.fetch_count,
            acks: req.acks,
            enqueues: req.enqueues,
        };

        self.tx
            .send(proto_req)
            .await
            .map_err(|_| RpcError::StreamClosed)?;

        let msg = self
            .rx
            .message()
            .await?
            .ok_or(RpcError::StreamClosed)?;

        if msg.error == "NOT_LEADER" {
            return Err(RpcError::NotLeader {
                leader_addr: msg.leader_addr.clone(),
            });
        }

        Ok(LifecycleResponse {
            request_id: msg.request_id,
            jobs: msg.jobs,
            acked: msg.acked,
            enqueued_job_ids: msg.enqueued_job_ids,
            error: msg.error,
            leader_addr: msg.leader_addr,
        })
    }
}

// ---------------------------------------------------------------------------
// ResilientLifecycleStream
// ---------------------------------------------------------------------------

pub struct ResilientLifecycleStream {
    url: String,
    auth: RpcAuthOptions,
    stream: Option<LifecycleStream>,
}

impl ResilientLifecycleStream {
    pub async fn open(url: &str, auth: RpcAuthOptions) -> Result<Self, RpcError> {
        let mut client = RpcClient::new(url, auth.clone()).await?;
        let stream = client.open_lifecycle_stream().await?;
        Ok(Self {
            url: url.to_string(),
            auth,
            stream: Some(stream),
        })
    }

    pub async fn exchange(&mut self, req: LifecycleRequest) -> Result<LifecycleResponse, RpcError> {
        let stream = self.stream.as_mut().ok_or(RpcError::StreamClosed)?;
        match stream.exchange(req).await {
            Ok(resp) => Ok(resp),
            Err(RpcError::NotLeader { leader_addr }) if !leader_addr.is_empty() => {
                self.url = leader_addr;
                let mut client = RpcClient::new(&self.url, self.auth.clone()).await?;
                let new_stream = client.open_lifecycle_stream().await?;
                self.stream = Some(new_stream);
                // Caller should retry the exchange
                Err(RpcError::NotLeader {
                    leader_addr: self.url.clone(),
                })
            }
            Err(e) => Err(e),
        }
    }

    pub fn close(&mut self) {
        self.stream = None;
    }
}
