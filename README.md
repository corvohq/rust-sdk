# Corvo Rust SDK

Rust client and worker runtime for [Corvo](https://corvo.dev), the fast job queue.

## Crates

- `corvo-client` --- Async HTTP client for the Corvo API
- `corvo-worker` --- Async worker runtime for processing jobs

## Installation

```toml
[dependencies]
corvo-client = "0.1"
# For worker:
corvo-worker = "0.1"
```

## Quick Start

### Client

```rust
use corvo_client::CorvoClient;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = CorvoClient::new("http://localhost:7080");
    let result = client.enqueue("emails", json!({"to": "user@example.com"})).await?;
    println!("Enqueued: {}", result.job_id);
    Ok(())
}
```

### Worker

```rust
use corvo_client::CorvoClient;
use corvo_worker::{CorvoWorker, WorkerConfig};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = CorvoClient::new("http://localhost:7080");
    let mut worker = CorvoWorker::new(client, WorkerConfig {
        queues: vec!["emails".into()],
        worker_id: "worker-1".into(),
        concurrency: 5,
        ..Default::default()
    });

    worker.register("emails", |job, ctx| async move {
        println!("Processing: {}", job.job_id);
        Ok(())
    });

    worker.start().await?;
    Ok(())
}
```

## Authentication

```rust
use corvo_client::{CorvoClient, AuthOptions};

let client = CorvoClient::new("http://localhost:7080")
    .with_auth(AuthOptions {
        api_key: Some("your-key".into()),
        ..Default::default()
    });
```

## License

MIT
