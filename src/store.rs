use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
}

#[derive(Clone)]
pub struct RateLimitStore {
    client: redis::Client,
}

impl RateLimitStore {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }

    pub async fn increment_fixed_window(
        &self,
        key: &str,
        window: Duration,
    ) -> Result<CounterState, StoreError> {
        let script = redis::Script::new(FIXED_WINDOW_LUA);
        let mut connection = self.client.get_multiplexed_async_connection().await?;
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

    pub async fn take_token_from_bucket(
        &self,
        key: &str,
        capacity: u64,
        refill_tokens: u64,
        refill_interval: Duration,
    ) -> Result<TokenBucketState, StoreError> {
        let script = redis::Script::new(TOKEN_BUCKET_LUA);
        let mut connection = self.client.get_multiplexed_async_connection().await?;
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
    use super::RateLimitStore;
    use redis::AsyncCommands;
    use std::{
        env,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_redis_url() -> String {
        env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
    }

    async fn build_store() -> RateLimitStore {
        let client = redis::Client::open(test_redis_url()).expect("failed to build redis client");
        RateLimitStore::new(client)
    }

    async fn delete_key(key: &str) {
        let client = redis::Client::open(test_redis_url()).expect("failed to build redis client");
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .expect("failed to connect to redis");
        let _: usize = connection
            .del(key)
            .await
            .expect("failed to delete redis key");
    }

    fn unique_key(prefix: &str) -> String {
        let nonce = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("test:{prefix}:{timestamp}:{nonce}")
    }

    #[actix_web::test]
    async fn redis_store_increments_within_window() {
        let store = build_store().await;
        let key = unique_key("fixed-window");

        let first = store
            .increment_fixed_window(&key, Duration::from_secs(60))
            .await
            .unwrap();
        let second = store
            .increment_fixed_window(&key, Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(first.current, 1);
        assert_eq!(second.current, 2);

        delete_key(&key).await;
    }

    #[actix_web::test]
    async fn redis_token_bucket_refuses_request_after_capacity_is_spent() {
        let store = build_store().await;
        let key = unique_key("token-bucket");

        let first = store
            .take_token_from_bucket(&key, 2, 1, Duration::from_secs(60))
            .await
            .unwrap();
        let second = store
            .take_token_from_bucket(&key, 2, 1, Duration::from_secs(60))
            .await
            .unwrap();
        let third = store
            .take_token_from_bucket(&key, 2, 1, Duration::from_secs(60))
            .await
            .unwrap();

        assert!(first.allowed);
        assert!(second.allowed);
        assert!(!third.allowed);
        assert_eq!(third.tokens_remaining, 0);
        assert!(third.retry_after > Duration::ZERO);

        delete_key(&key).await;
    }
}
