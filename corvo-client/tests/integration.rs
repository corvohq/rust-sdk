//! Integration tests for the Corvo Rust SDK.
//!
//! Run with: cargo test --test integration
//! Requires a running Corvo server (default: http://localhost:8080).

use corvo_client::{AckBody, CorvoClient, EnqueueOptions, SearchFilter};
use serde_json::json;
use std::time::Duration;

fn corvo_url() -> String {
    std::env::var("CORVO_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
}

#[tokio::test]
async fn test_job_lifecycle() {
    let client = CorvoClient::new(&corvo_url());

    let result = client
        .enqueue("rust-test", json!({"hello": "world"}))
        .await
        .expect("enqueue failed");
    assert!(!result.job_id.is_empty(), "expected non-empty job_id");

    let job = client
        .fetch(vec!["rust-test".into()], "rust-integration-worker", "test-host", 5)
        .await
        .expect("fetch failed")
        .expect("expected a job");
    assert_eq!(job.job_id, result.job_id);
    assert_eq!(job.payload["hello"], "world");

    client
        .ack(&job.job_id, AckBody::default())
        .await
        .expect("ack failed");

    let got = client.get_job(&result.job_id).await.expect("get_job failed");
    assert_eq!(got["state"], "completed", "expected completed, got {}", got["state"]);
}

#[tokio::test]
async fn test_enqueue_options() {
    let client = CorvoClient::new(&corvo_url());

    let result = client
        .enqueue_with(EnqueueOptions {
            queue: "rust-opts-test".into(),
            payload: json!({"key": "value"}),
            priority: Some("high".into()),
            max_retries: Some(5),
            ..Default::default()
        })
        .await
        .expect("enqueue failed");

    let got = client.get_job(&result.job_id).await.expect("get_job failed");
    assert_eq!(got["priority"], 1, "expected priority=1 (high)");
    assert_eq!(got["max_retries"], 5, "expected max_retries=5");

    // Clean up
    if let Ok(Some(job)) = client
        .fetch(vec!["rust-opts-test".into()], "w1", "host", 3)
        .await
    {
        let _ = client.ack(&job.job_id, AckBody::default()).await;
    }
}

#[tokio::test]
async fn test_search() {
    let client = CorvoClient::new(&corvo_url());

    client.enqueue("rust-search-test", json!({"n": 1})).await.unwrap();
    client.enqueue("rust-search-test", json!({"n": 2})).await.unwrap();

    let result = client
        .search(SearchFilter {
            queue: Some("rust-search-test".into()),
            state: Some(vec!["pending".into()]),
            ..Default::default()
        })
        .await
        .expect("search failed");
    assert!(result.total >= 2, "expected at least 2, got {}", result.total);

    // Clean up
    for _ in 0..2 {
        if let Ok(Some(job)) = client
            .fetch(vec!["rust-search-test".into()], "w1", "host", 1)
            .await
        {
            let _ = client.ack(&job.job_id, AckBody::default()).await;
        }
    }
}

#[tokio::test]
async fn test_fail_and_retry() {
    let client = CorvoClient::new(&corvo_url());

    let result = client
        .enqueue_with(EnqueueOptions {
            queue: "rust-fail-test".into(),
            payload: json!({"x": 1}),
            max_retries: Some(2),
            ..Default::default()
        })
        .await
        .expect("enqueue failed");

    let job = client
        .fetch(vec!["rust-fail-test".into()], "w1", "host", 5)
        .await
        .expect("fetch failed")
        .expect("expected a job");
    client.fail(&job.job_id, "test error", "").await.expect("fail failed");

    tokio::time::sleep(Duration::from_secs(6)).await;

    let job2 = client
        .fetch(vec!["rust-fail-test".into()], "w1", "host", 5)
        .await
        .expect("fetch failed")
        .expect("expected retry job");
    assert_eq!(job2.attempt, 2, "expected attempt=2");

    client.ack(&job2.job_id, AckBody::default()).await.expect("ack failed");

    let got = client.get_job(&result.job_id).await.expect("get_job failed");
    assert_eq!(got["state"], "completed", "expected completed, got {}", got["state"]);
}

#[tokio::test]
async fn test_batch_enqueue() {
    use corvo_client::BatchJob;

    let client = CorvoClient::new(&corvo_url());

    let result = client
        .enqueue_batch(
            vec![
                BatchJob { queue: "rust-batch-test".into(), payload: json!({"n": 1}) },
                BatchJob { queue: "rust-batch-test".into(), payload: json!({"n": 2}) },
                BatchJob { queue: "rust-batch-test".into(), payload: json!({"n": 3}) },
            ],
            None,
        )
        .await
        .expect("batch enqueue failed");
    assert_eq!(result.job_ids.len(), 3, "expected 3 jobs");

    // Clean up
    for _ in 0..3 {
        if let Ok(Some(job)) = client
            .fetch(vec!["rust-batch-test".into()], "w1", "host", 1)
            .await
        {
            let _ = client.ack(&job.job_id, AckBody::default()).await;
        }
    }
}
