use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThrottledRequest {
    pub rule_name: String,
    pub ip_address: String,
    pub method: String,
    pub path: String,
    pub queued_at_unix_seconds: u64,
    pub retry_after_seconds: u64,
}

impl ThrottledRequest {
    pub fn new(
        rule_name: impl Into<String>,
        ip_address: impl Into<String>,
        method: impl Into<String>,
        path: impl Into<String>,
        retry_after_seconds: u64,
    ) -> Self {
        Self {
            rule_name: rule_name.into(),
            ip_address: ip_address.into(),
            method: method.into(),
            path: path.into(),
            queued_at_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            retry_after_seconds,
        }
    }
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("redis queue error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("failed to serialize queued request: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("in-memory queue lock poisoned")]
    LockPoisoned,
}

#[async_trait]
pub trait ThrottledRequestQueue: Send + Sync {
    async fn enqueue(&self, queue_key: &str, request: ThrottledRequest) -> Result<(), QueueError>;
}

#[derive(Clone)]
pub struct RedisThrottledRequestQueue {
    client: redis::Client,
}

impl RedisThrottledRequestQueue {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ThrottledRequestQueue for RedisThrottledRequestQueue {
    async fn enqueue(&self, queue_key: &str, request: ThrottledRequest) -> Result<(), QueueError> {
        let payload = serde_json::to_string(&request)?;
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let _: usize = redis::cmd("RPUSH")
            .arg(queue_key)
            .arg(payload)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct InMemoryThrottledRequestQueue {
    entries: Arc<Mutex<Vec<ThrottledRequest>>>,
}

impl InMemoryThrottledRequestQueue {
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .map(|entries| entries.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.entries
            .lock()
            .map(|entries| entries.is_empty())
            .unwrap_or(true)
    }

    pub fn snapshot(&self) -> Vec<ThrottledRequest> {
        self.entries
            .lock()
            .map(|entries| entries.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl ThrottledRequestQueue for InMemoryThrottledRequestQueue {
    async fn enqueue(&self, _queue_key: &str, request: ThrottledRequest) -> Result<(), QueueError> {
        let mut entries = self.entries.lock().map_err(|_| QueueError::LockPoisoned)?;
        entries.push(request);
        Ok(())
    }
}
