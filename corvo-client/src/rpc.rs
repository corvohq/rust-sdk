//! Optional gRPC transport for CorvoClient.
//!
//! When `use_rpc` is enabled, enqueue/fail/heartbeat route through gRPC unary
//! calls. Other methods (search, bulk, getJob, queue management) remain HTTP.

use std::collections::HashMap;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::gen::corvo::v1::{self as pb, worker_service_client::WorkerServiceClient};

#[derive(Debug, thiserror::Error)]
pub enum ClientRpcError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc error: {0}")]
    Status(#[from] tonic::Status),
}

#[derive(Clone, Debug, Default)]
pub struct RpcAuthOptions {
    pub headers: Vec<(String, String)>,
    pub bearer_token: Option<String>,
    pub api_key: Option<String>,
    pub api_key_header: Option<String>,
}

const SDK_NAME: &str = "corvo-rust";
const SDK_VERSION: &str = "0.2.0";

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
        let header = auth.api_key_header.as_deref().unwrap_or("x-api-key");
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

pub struct ClientRpc {
    client: WorkerServiceClient<Channel>,
    auth: RpcAuthOptions,
}

impl ClientRpc {
    pub async fn new(url: &str, auth: RpcAuthOptions) -> Result<Self, ClientRpcError> {
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
        *req.metadata_mut() = build_metadata(&self.auth);
        req
    }

    pub async fn enqueue(
        &mut self,
        queue: &str,
        payload_json: &str,
    ) -> Result<pb::EnqueueResponse, ClientRpcError> {
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
    ) -> Result<pb::FailResponse, ClientRpcError> {
        let resp = self
            .client
            .fail(self.request(pb::FailRequest {
                job_id: job_id.to_string(),
                error: error.to_string(),
                backtrace: backtrace.to_string(),
            }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn heartbeat(
        &mut self,
        jobs: HashMap<String, pb::HeartbeatJobUpdate>,
    ) -> Result<HashMap<String, String>, ClientRpcError> {
        let resp = self
            .client
            .heartbeat(self.request(pb::HeartbeatRequest { jobs }))
            .await?;
        Ok(resp
            .into_inner()
            .jobs
            .into_iter()
            .map(|(k, v)| (k, v.status))
            .collect())
    }
}
