use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

const FIXED_WINDOW_LUA: &str = r#"
local current = redis.call("INCR", KEYS[1])
if current == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
local ttl = redis.call("PTTL", KEYS[1])
return {current, ttl}
"#;

const TOKEN_BUCKET_LUA: &str = r#"
local capacity = tonumber(ARGV[1])
local refill_tokens = tonumber(ARGV[2])
local refill_interval_ms = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

local data = redis.call("HMGET", KEYS[1], "tokens", "last_refill_ms")
local tokens = tonumber(data[1])
local last_refill_ms = tonumber(data[2])

if tokens == nil then
  tokens = capacity
end

if last_refill_ms == nil then
  last_refill_ms = now_ms
end

local elapsed_ms = math.max(0, now_ms - last_refill_ms)
local refill_periods = math.floor(elapsed_ms / refill_interval_ms)

if refill_periods > 0 then
  tokens = math.min(capacity, tokens + refill_periods * refill_tokens)
  last_refill_ms = last_refill_ms + refill_periods * refill_interval_ms
end

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

local elapsed_since_last_refill = math.max(0, now_ms - last_refill_ms)
local retry_after_ms = 0
if allowed == 0 then
  retry_after_ms = refill_interval_ms - elapsed_since_last_refill
  if retry_after_ms <= 0 then
    retry_after_ms = refill_interval_ms
  end
end

local reset_after_ms = 0
if tokens < capacity then
  local missing_tokens = capacity - tokens
  local refill_steps = math.ceil(missing_tokens / refill_tokens)
  reset_after_ms = refill_steps * refill_interval_ms - elapsed_since_last_refill
  if reset_after_ms < 0 then
    reset_after_ms = 0
  end
end

redis.call("HSET", KEYS[1], "tokens", tokens, "last_refill_ms", last_refill_ms)
redis.call("PEXPIRE", KEYS[1], math.max(reset_after_ms, refill_interval_ms))

return {allowed, tokens, retry_after_ms, reset_after_ms}
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CounterState {
    pub current: u64,
    pub ttl: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenBucketState {
    pub allowed: bool,
    pub tokens_remaining: u64,
    pub retry_after: Duration,
    pub reset_after: Duration,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("redis store error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("in-memory store lock poisoned")]
    LockPoisoned,
}

#[async_trait]
pub trait RateLimitStore: Send + Sync {
    async fn increment_fixed_window(
        &self,
        key: &str,
        window: Duration,
    ) -> Result<CounterState, StoreError>;

    async fn take_token_from_bucket(
        &self,
        key: &str,
        capacity: u64,
        refill_tokens: u64,
        refill_interval: Duration,
    ) -> Result<TokenBucketState, StoreError>;
}

#[derive(Clone)]
pub struct RedisCounterStore {
    client: redis::Client,
}

impl RedisCounterStore {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RateLimitStore for RedisCounterStore {
    async fn increment_fixed_window(
        &self,
        key: &str,
        window: Duration,
    ) -> Result<CounterState, StoreError> {
        let script = redis::Script::new(FIXED_WINDOW_LUA);
        let mut connection = self.client.get_multiplexed_tokio_connection().await?;
        let (current, ttl_ms): (u64, i64) = script
            .key(key)
            .arg(window.as_millis() as u64)
            .invoke_async(&mut connection)
            .await?;

        Ok(CounterState {
            current,
            ttl: duration_from_millis(ttl_ms),
        })
    }

    async fn take_token_from_bucket(
        &self,
        key: &str,
        capacity: u64,
        refill_tokens: u64,
        refill_interval: Duration,
    ) -> Result<TokenBucketState, StoreError> {
        let script = redis::Script::new(TOKEN_BUCKET_LUA);
        let mut connection = self.client.get_multiplexed_tokio_connection().await?;
        let now_ms = unix_timestamp_millis();
        let (allowed, tokens_remaining, retry_after_ms, reset_after_ms): (u64, u64, i64, i64) =
            script
                .key(key)
                .arg(capacity)
                .arg(refill_tokens)
                .arg(refill_interval.as_millis() as u64)
                .arg(now_ms)
                .invoke_async(&mut connection)
                .await?;

        Ok(TokenBucketState {
            allowed: allowed == 1,
            tokens_remaining,
            retry_after: duration_from_millis(retry_after_ms),
            reset_after: duration_from_millis(reset_after_ms),
        })
    }
}

#[derive(Clone, Default)]
pub struct InMemoryCounterStore {
    fixed_windows: Arc<Mutex<HashMap<String, InMemoryFixedWindowEntry>>>,
    buckets: Arc<Mutex<HashMap<String, InMemoryBucketEntry>>>,
}

#[derive(Debug, Clone)]
struct InMemoryFixedWindowEntry {
    count: u64,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct InMemoryBucketEntry {
    tokens: u64,
    last_refill: Instant,
}

#[async_trait]
impl RateLimitStore for InMemoryCounterStore {
    async fn increment_fixed_window(
        &self,
        key: &str,
        window: Duration,
    ) -> Result<CounterState, StoreError> {
        let mut entries = self
            .fixed_windows
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let now = Instant::now();

        entries.retain(|_, entry| entry.expires_at > now);

        let entry = entries
            .entry(key.to_string())
            .or_insert_with(|| InMemoryFixedWindowEntry {
                count: 0,
                expires_at: now + window,
            });

        if entry.expires_at <= now {
            entry.count = 0;
            entry.expires_at = now + window;
        }

        entry.count += 1;

        Ok(CounterState {
            current: entry.count,
            ttl: entry.expires_at.saturating_duration_since(now),
        })
    }

    async fn take_token_from_bucket(
        &self,
        key: &str,
        capacity: u64,
        refill_tokens: u64,
        refill_interval: Duration,
    ) -> Result<TokenBucketState, StoreError> {
        let mut entries = self.buckets.lock().map_err(|_| StoreError::LockPoisoned)?;
        let now = Instant::now();

        let entry = entries
            .entry(key.to_string())
            .or_insert_with(|| InMemoryBucketEntry {
                tokens: capacity,
                last_refill: now,
            });

        let elapsed = now.saturating_duration_since(entry.last_refill);
        let refill_periods = elapsed.as_millis() / refill_interval.as_millis().max(1);

        if refill_periods > 0 {
            let replenished = refill_periods as u64 * refill_tokens;
            entry.tokens = entry.tokens.saturating_add(replenished).min(capacity);
            entry.last_refill += refill_interval.mul_f64(refill_periods as f64);
        }

        let allowed = entry.tokens >= 1;
        if allowed {
            entry.tokens -= 1;
        }

        let elapsed_since_last_refill = now.saturating_duration_since(entry.last_refill);
        let retry_after = if allowed {
            Duration::ZERO
        } else {
            refill_interval.saturating_sub(elapsed_since_last_refill)
        };

        let reset_after = if entry.tokens >= capacity {
            Duration::ZERO
        } else {
            let missing_tokens = capacity - entry.tokens;
            let refill_steps = missing_tokens.div_ceil(refill_tokens);
            let total_reset = refill_interval.mul_f64(refill_steps as f64);
            total_reset.saturating_sub(elapsed_since_last_refill)
        };

        Ok(TokenBucketState {
            allowed,
            tokens_remaining: entry.tokens,
            retry_after,
            reset_after,
        })
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn duration_from_millis(milliseconds: i64) -> Duration {
    Duration::from_millis(milliseconds.max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::{InMemoryCounterStore, RateLimitStore};
    use std::time::Duration;

    #[actix_web::test]
    async fn in_memory_store_increments_within_window() {
        let store = InMemoryCounterStore::default();
        let first = store
            .increment_fixed_window("key", Duration::from_secs(60))
            .await
            .unwrap();
        let second = store
            .increment_fixed_window("key", Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(first.current, 1);
        assert_eq!(second.current, 2);
    }

    #[actix_web::test]
    async fn in_memory_token_bucket_refuses_request_after_capacity_is_spent() {
        let store = InMemoryCounterStore::default();

        let first = store
            .take_token_from_bucket("bucket", 2, 1, Duration::from_secs(60))
            .await
            .unwrap();
        let second = store
            .take_token_from_bucket("bucket", 2, 1, Duration::from_secs(60))
            .await
            .unwrap();
        let third = store
            .take_token_from_bucket("bucket", 2, 1, Duration::from_secs(60))
            .await
            .unwrap();

        assert!(first.allowed);
        assert!(second.allowed);
        assert!(!third.allowed);
        assert_eq!(third.tokens_remaining, 0);
        assert!(third.retry_after > Duration::ZERO);
    }
}
