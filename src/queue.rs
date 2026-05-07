use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
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
}

#[derive(Clone)]
pub struct ThrottledRequestQueue {
    client: redis::Client,
}

impl ThrottledRequestQueue {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }

    pub async fn enqueue(
        &self,
        queue_key: &str,
        request: ThrottledRequest,
    ) -> Result<(), QueueError> {
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
