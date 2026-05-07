use crate::{
    config::{RateLimitAlgorithm, RateLimitBehavior, RateLimitConfig, RateLimitRule},
    queue::{QueueError, ThrottledRequest, ThrottledRequestQueue},
    store::{RateLimitStore, StoreError},
};
use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitHeaders {
    pub limit: u64,
    pub remaining: u64,
    pub retry_after_seconds: u64,
    pub reset_unix_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowedResponse {
    pub headers: RateLimitHeaders,
    pub rule_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueDisposition {
    NotQueued,
    Queued,
    QueueFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThrottledResponse {
    pub headers: RateLimitHeaders,
    pub rule_name: String,
    pub queue_disposition: QueueDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnforcementResult {
    NotConfigured,
    Allowed(AllowedResponse),
    Throttled(ThrottledResponse),
}

#[derive(Debug, Error)]
pub enum LimiterError {
    #[error("counter store error: {0}")]
    Store(#[from] StoreError),
    #[error("queue error: {0}")]
    Queue(#[from] QueueError),
}

#[derive(Clone)]
pub struct RateLimiter {
    config: Arc<RateLimitConfig>,
    store: Arc<RateLimitStore>,
    queue: Arc<dyn ThrottledRequestQueue>,
}

impl RateLimiter {
    pub fn new(
        config: RateLimitConfig,
        store: Arc<RateLimitStore>,
        queue: Arc<dyn ThrottledRequestQueue>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            store,
            queue,
        }
    }

    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }

    pub async fn check_ip(
        &self,
        ip: IpAddr,
        method: &str,
        path: &str,
    ) -> Result<EnforcementResult, LimiterError> {
        let Some(rule) = self.config.find_rule(method, path) else {
            return Ok(EnforcementResult::NotConfigured);
        };

        let decision = match &rule.algorithm {
            RateLimitAlgorithm::FixedWindow {
                requests_per_window,
                window,
            } => {
                let key = build_fixed_window_key(rule, ip, *window);
                let counter = self.store.increment_fixed_window(&key, *window).await?;
                let headers = build_headers(
                    *requests_per_window,
                    requests_per_window.saturating_sub(counter.current.min(*requests_per_window)),
                    if counter.current > *requests_per_window {
                        counter.ttl
                    } else {
                        Duration::ZERO
                    },
                    counter.ttl,
                );

                if counter.current <= *requests_per_window {
                    EnforcementResult::Allowed(AllowedResponse {
                        headers,
                        rule_name: rule.name.clone(),
                    })
                } else {
                    EnforcementResult::Throttled(ThrottledResponse {
                        headers,
                        rule_name: rule.name.clone(),
                        queue_disposition: QueueDisposition::NotQueued,
                    })
                }
            }
            RateLimitAlgorithm::TokenBucket {
                capacity,
                refill_tokens,
                refill_interval,
            } => {
                let key = build_token_bucket_key(rule, ip);
                let bucket = self
                    .store
                    .take_token_from_bucket(&key, *capacity, *refill_tokens, *refill_interval)
                    .await?;
                let headers = build_headers(
                    *capacity,
                    bucket.tokens_remaining,
                    bucket.retry_after,
                    bucket.reset_after,
                );

                if bucket.allowed {
                    EnforcementResult::Allowed(AllowedResponse {
                        headers,
                        rule_name: rule.name.clone(),
                    })
                } else {
                    EnforcementResult::Throttled(ThrottledResponse {
                        headers,
                        rule_name: rule.name.clone(),
                        queue_disposition: QueueDisposition::NotQueued,
                    })
                }
            }
        };

        if let EnforcementResult::Throttled(mut throttled) = decision {
            throttled.queue_disposition = self
                .handle_throttled_request(
                    rule,
                    ip,
                    method,
                    path,
                    throttled.headers.retry_after_seconds,
                )
                .await;
            return Ok(EnforcementResult::Throttled(throttled));
        }

        Ok(decision)
    }

    async fn handle_throttled_request(
        &self,
        rule: &RateLimitRule,
        ip: IpAddr,
        method: &str,
        path: &str,
        retry_after_seconds: u64,
    ) -> QueueDisposition {
        match rule.behavior {
            RateLimitBehavior::Block => QueueDisposition::NotQueued,
            RateLimitBehavior::Queue => {
                let request = ThrottledRequest::new(
                    rule.name.clone(),
                    ip.to_string(),
                    method.to_string(),
                    path.to_string(),
                    retry_after_seconds,
                );

                match self.queue.enqueue(&self.config.queue_key, request).await {
                    Ok(()) => QueueDisposition::Queued,
                    Err(_) => QueueDisposition::QueueFailed,
                }
            }
        }
    }
}

fn build_fixed_window_key(rule: &RateLimitRule, ip: IpAddr, window: Duration) -> String {
    let window_seconds = window.as_secs().max(1);
    let now_seconds = unix_timestamp_seconds();
    let window_start = now_seconds / window_seconds * window_seconds;

    format!("rl:{}:{}:{}", rule.name, ip, window_start)
}

fn build_token_bucket_key(rule: &RateLimitRule, ip: IpAddr) -> String {
    format!("rl:{}:{}", rule.name, ip)
}

fn build_headers(
    limit: u64,
    remaining: u64,
    retry_after: Duration,
    reset_after: Duration,
) -> RateLimitHeaders {
    let now = unix_timestamp_seconds();

    RateLimitHeaders {
        limit,
        remaining,
        retry_after_seconds: retry_after.as_secs(),
        reset_unix_seconds: now + reset_after.as_secs(),
    }
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::{EnforcementResult, QueueDisposition, RateLimiter};
    use crate::{
        config::{RateLimitAlgorithm, RateLimitBehavior, RateLimitConfig, RateLimitRule},
        queue::InMemoryThrottledRequestQueue,
        store::RateLimitStore,
    };
    use redis::AsyncCommands;
    use std::{
        env,
        net::IpAddr,
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
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

    fn unique_name(prefix: &str) -> String {
        let nonce = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{prefix}-{timestamp}-{nonce}")
    }

    async fn delete_keys(pattern: &str) {
        let client = redis::Client::open(test_redis_url()).expect("failed to build redis client");
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .expect("failed to connect to redis");
        let keys: Vec<String> = connection
            .keys(pattern)
            .await
            .expect("failed to scan redis keys");

        if !keys.is_empty() {
            let _: usize = connection
                .del(keys)
                .await
                .expect("failed to delete redis keys");
        }
    }

    async fn build_fixed_window_limiter() -> (RateLimiter, InMemoryThrottledRequestQueue, String) {
        let queue = InMemoryThrottledRequestQueue::default();
        let rule_name = unique_name("login");
        let limiter = RateLimiter::new(
            RateLimitConfig {
                rules: vec![RateLimitRule {
                    name: rule_name.clone(),
                    path: Some("/login".to_string()),
                    methods: vec!["POST".to_string()],
                    algorithm: RateLimitAlgorithm::FixedWindow {
                        requests_per_window: 1,
                        window: Duration::from_secs(60),
                    },
                    behavior: RateLimitBehavior::Queue,
                }],
                queue_key: "queue".to_string(),
            },
            Arc::new(build_store().await),
            Arc::new(queue.clone()),
        );

        (limiter, queue, rule_name)
    }

    async fn build_token_bucket_limiter() -> (RateLimiter, InMemoryThrottledRequestQueue, String) {
        let queue = InMemoryThrottledRequestQueue::default();
        let rule_name = unique_name("status");
        let limiter = RateLimiter::new(
            RateLimitConfig {
                rules: vec![RateLimitRule {
                    name: rule_name.clone(),
                    path: Some("/status".to_string()),
                    methods: vec!["GET".to_string()],
                    algorithm: RateLimitAlgorithm::TokenBucket {
                        capacity: 1,
                        refill_tokens: 1,
                        refill_interval: Duration::from_secs(60),
                    },
                    behavior: RateLimitBehavior::Queue,
                }],
                queue_key: "queue".to_string(),
            },
            Arc::new(build_store().await),
            Arc::new(queue.clone()),
        );

        (limiter, queue, rule_name)
    }

    #[actix_web::test]
    async fn fixed_window_limiter_allows_first_request_and_queues_second() {
        let (limiter, queue, rule_name) = build_fixed_window_limiter().await;
        let ip = IpAddr::from_str("127.0.0.1").unwrap();

        let first = limiter.check_ip(ip, "POST", "/login").await.unwrap();
        let second = limiter.check_ip(ip, "POST", "/login").await.unwrap();

        assert!(matches!(first, EnforcementResult::Allowed(_)));

        match second {
            EnforcementResult::Throttled(result) => {
                assert_eq!(result.queue_disposition, QueueDisposition::Queued);
            }
            other => panic!("unexpected result: {other:?}"),
        }

        assert_eq!(queue.len(), 1);
        delete_keys(&format!("rl:{rule_name}:*")).await;
    }

    #[actix_web::test]
    async fn token_bucket_limiter_throttles_after_capacity_is_spent() {
        let (limiter, queue, rule_name) = build_token_bucket_limiter().await;
        let ip = IpAddr::from_str("127.0.0.1").unwrap();

        let first = limiter.check_ip(ip, "GET", "/status").await.unwrap();
        let second = limiter.check_ip(ip, "GET", "/status").await.unwrap();

        assert!(matches!(first, EnforcementResult::Allowed(_)));

        match second {
            EnforcementResult::Throttled(result) => {
                assert_eq!(result.queue_disposition, QueueDisposition::Queued);
                assert_eq!(result.headers.limit, 1);
                assert_eq!(result.headers.remaining, 0);
            }
            other => panic!("unexpected result: {other:?}"),
        }

        assert_eq!(queue.len(), 1);
        delete_keys(&format!("rl:{rule_name}:*")).await;
    }
}
