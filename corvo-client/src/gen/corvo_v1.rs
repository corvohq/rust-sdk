// Hand-maintained proto types equivalent to tonic-build output.
// Regenerate with `protoc` when available.

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnqueueRequest {
    #[prost(string, tag = "1")]
    pub queue: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub payload_json: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub agent: ::core::option::Option<AgentConfig>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnqueueResponse {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub status: ::prost::alloc::string::String,
    #[prost(bool, tag = "3")]
    pub unique_existing: bool,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchBatchJob {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub queue: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub payload_json: ::prost::alloc::string::String,
    #[prost(int32, tag = "4")]
    pub attempt: i32,
    #[prost(int32, tag = "5")]
    pub max_retries: i32,
    #[prost(int32, tag = "6")]
    pub lease_duration: i32,
    #[prost(string, tag = "7")]
    pub checkpoint_json: ::prost::alloc::string::String,
    #[prost(string, tag = "8")]
    pub tags_json: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "9")]
    pub agent: ::core::option::Option<AgentState>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UsageReport {
    #[prost(int64, tag = "1")]
    pub input_tokens: i64,
    #[prost(int64, tag = "2")]
    pub output_tokens: i64,
    #[prost(int64, tag = "3")]
    pub cache_creation_tokens: i64,
    #[prost(int64, tag = "4")]
    pub cache_read_tokens: i64,
    #[prost(string, tag = "5")]
    pub model: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub provider: ::prost::alloc::string::String,
    #[prost(double, tag = "7")]
    pub cost_usd: f64,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AckBatchItem {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub result_json: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub usage: ::core::option::Option<UsageReport>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LifecycleEnqueueItem {
    #[prost(string, tag = "1")]
    pub queue: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub payload_json: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub agent: ::core::option::Option<AgentConfig>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AgentConfig {
    #[prost(int32, tag = "1")]
    pub max_iterations: i32,
    #[prost(double, tag = "2")]
    pub max_cost_usd: f64,
    #[prost(string, tag = "3")]
    pub iteration_timeout: ::prost::alloc::string::String,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AgentState {
    #[prost(int32, tag = "1")]
    pub max_iterations: i32,
    #[prost(double, tag = "2")]
    pub max_cost_usd: f64,
    #[prost(string, tag = "3")]
    pub iteration_timeout: ::prost::alloc::string::String,
    #[prost(int32, tag = "4")]
    pub iteration: i32,
    #[prost(double, tag = "5")]
    pub total_cost_usd: f64,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LifecycleStreamRequest {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(string, repeated, tag = "2")]
    pub queues: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag = "3")]
    pub worker_id: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub hostname: ::prost::alloc::string::String,
    #[prost(int32, tag = "5")]
    pub lease_duration: i32,
    #[prost(int32, tag = "6")]
    pub fetch_count: i32,
    #[prost(message, repeated, tag = "7")]
    pub acks: ::prost::alloc::vec::Vec<AckBatchItem>,
    #[prost(message, repeated, tag = "8")]
    pub enqueues: ::prost::alloc::vec::Vec<LifecycleEnqueueItem>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LifecycleStreamResponse {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(message, repeated, tag = "2")]
    pub jobs: ::prost::alloc::vec::Vec<FetchBatchJob>,
    #[prost(int32, tag = "3")]
    pub acked: i32,
    #[prost(string, tag = "4")]
    pub error: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "5")]
    pub enqueued_job_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag = "6")]
    pub leader_addr: ::prost::alloc::string::String,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FailRequest {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub error: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub backtrace: ::prost::alloc::string::String,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FailResponse {
    #[prost(string, tag = "1")]
    pub status: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub next_attempt_at: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(int32, tag = "3")]
    pub attempts_remaining: i32,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatJobUpdate {
    #[prost(string, tag = "1")]
    pub progress_json: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub checkpoint_json: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub usage: ::core::option::Option<UsageReport>,
    #[prost(string, tag = "4")]
    pub stream_delta: ::prost::alloc::string::String,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatRequest {
    #[prost(map = "string, message", tag = "1")]
    pub jobs: ::std::collections::HashMap<::prost::alloc::string::String, HeartbeatJobUpdate>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatJobResponse {
    #[prost(string, tag = "1")]
    pub status: ::prost::alloc::string::String,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatResponse {
    #[prost(map = "string, message", tag = "1")]
    pub jobs: ::std::collections::HashMap<::prost::alloc::string::String, HeartbeatJobResponse>,
}

// -- gRPC service client --

pub mod worker_service_client {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct WorkerServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }

    impl WorkerServiceClient<tonic::transport::Channel> {
        pub fn new(channel: tonic::transport::Channel) -> Self {
            let inner = tonic::client::Grpc::new(channel);
            Self { inner }
        }
    }

    impl<T> WorkerServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as tonic::codegen::Body>::Error: Into<Box<dyn std::error::Error + Send + Sync>> + std::marker::Send,
    {
        pub async fn enqueue(
            &mut self,
            request: impl tonic::IntoRequest<EnqueueRequest>,
        ) -> std::result::Result<tonic::Response<EnqueueResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| tonic::Status::new(tonic::Code::Unknown, format!("{}", e.into())))?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/corvo.v1.WorkerService/Enqueue");
            let mut req = request.into_request();
            req.extensions_mut().insert(tonic::GrpcMethod::new("corvo.v1.WorkerService", "Enqueue"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn fail(
            &mut self,
            request: impl tonic::IntoRequest<FailRequest>,
        ) -> std::result::Result<tonic::Response<FailResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| tonic::Status::new(tonic::Code::Unknown, format!("{}", e.into())))?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/corvo.v1.WorkerService/Fail");
            let mut req = request.into_request();
            req.extensions_mut().insert(tonic::GrpcMethod::new("corvo.v1.WorkerService", "Fail"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn heartbeat(
            &mut self,
            request: impl tonic::IntoRequest<HeartbeatRequest>,
        ) -> std::result::Result<tonic::Response<HeartbeatResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| tonic::Status::new(tonic::Code::Unknown, format!("{}", e.into())))?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/corvo.v1.WorkerService/Heartbeat");
            let mut req = request.into_request();
            req.extensions_mut().insert(tonic::GrpcMethod::new("corvo.v1.WorkerService", "Heartbeat"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn stream_lifecycle(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = LifecycleStreamRequest>,
        ) -> std::result::Result<tonic::Response<tonic::Streaming<LifecycleStreamResponse>>, tonic::Status> {
            self.inner.ready().await.map_err(|e| tonic::Status::new(tonic::Code::Unknown, format!("{}", e.into())))?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/corvo.v1.WorkerService/StreamLifecycle");
            let mut req = request.into_streaming_request();
            req.extensions_mut().insert(tonic::GrpcMethod::new("corvo.v1.WorkerService", "StreamLifecycle"));
            self.inner.streaming(req, path, codec).await
        }
    }
}
