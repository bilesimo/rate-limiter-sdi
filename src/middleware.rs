use crate::limiter::{EnforcementResult, QueueDisposition, RateLimitHeaders, RateLimiter};
use actix_web::{
    Error, HttpResponse,
    body::{EitherBody, MessageBody},
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    error::ErrorInternalServerError,
    http::header::{HeaderName, HeaderValue},
};
use serde::Serialize;
use std::{
    future::{Future, Ready, ready},
    net::IpAddr,
    pin::Pin,
    rc::Rc,
    sync::Arc,
};
use tracing::warn;

const HEADER_LIMIT: &str = "x-ratelimit-limit";
const HEADER_REMAINING: &str = "x-ratelimit-remaining";
const HEADER_RETRY_AFTER: &str = "x-ratelimit-retry-after";
const HEADER_RESET: &str = "x-ratelimit-reset";

// Based on https://actix.rs/docs/middleware/
#[derive(Clone)]
pub struct RateLimitMiddleware {
    limiter: Arc<RateLimiter>,
}

impl RateLimitMiddleware {
    pub fn new(limiter: RateLimiter) -> Self {
        Self {
            limiter: Arc::new(limiter),
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for RateLimitMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type InitError = ();
    type Transform = RateLimitMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RateLimitMiddlewareService {
            service: Rc::new(service),
            limiter: self.limiter.clone(),
        }))
    }
}

pub struct RateLimitMiddlewareService<S> {
    service: Rc<S>,
    limiter: Arc<RateLimiter>,
}

impl<S, B> Service<ServiceRequest> for RateLimitMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(
        &self,
        ctx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let service = self.service.clone();
        let limiter = self.limiter.clone();

        Box::pin(async move {
            let maybe_ip = extract_client_ip(&req);
            let method = req.method().as_str().to_string();
            let path = req.path().to_string();

            if let Some(ip) = maybe_ip {
                match limiter.check_ip(ip, &method, &path).await {
                    Ok(EnforcementResult::Allowed(allowed)) => {
                        let mut response = service.call(req).await?.map_into_left_body();
                        insert_headers(response.response_mut(), &allowed.headers)?;
                        return Ok(response);
                    }
                    Ok(EnforcementResult::Throttled(throttled)) => {
                        let body = ThrottleBody {
                            message: "request was rate limited".to_string(),
                            rule_name: throttled.rule_name,
                            queued: matches!(throttled.queue_disposition, QueueDisposition::Queued),
                            queue_failed: matches!(
                                throttled.queue_disposition,
                                QueueDisposition::QueueFailed
                            ),
                            retry_after_seconds: throttled.headers.retry_after_seconds,
                        };
                        let mut response = HttpResponse::TooManyRequests().json(body);
                        insert_headers(&mut response, &throttled.headers)?;
                        return Ok(req.into_response(response.map_into_right_body()));
                    }
                    Ok(EnforcementResult::NotConfigured) => {}
                    Err(error) => {
                        warn!("rate limiter degraded to fail-open mode: {error}");
                    }
                }
            }

            Ok(service.call(req).await?.map_into_left_body())
        })
    }
}

#[derive(Debug, Serialize)]
struct ThrottleBody {
    message: String,
    rule_name: String,
    queued: bool,
    queue_failed: bool,
    retry_after_seconds: u64,
}

fn insert_headers<B>(
    response: &mut HttpResponse<B>,
    headers: &RateLimitHeaders,
) -> Result<(), Error>
where
    B: MessageBody,
{
    insert_header(response, HEADER_LIMIT, &headers.limit.to_string())?;
    insert_header(response, HEADER_REMAINING, &headers.remaining.to_string())?;
    insert_header(
        response,
        HEADER_RETRY_AFTER,
        &headers.retry_after_seconds.to_string(),
    )?;
    insert_header(
        response,
        HEADER_RESET,
        &headers.reset_unix_seconds.to_string(),
    )?;
    Ok(())
}

fn insert_header<B>(response: &mut HttpResponse<B>, name: &str, value: &str) -> Result<(), Error>
where
    B: MessageBody,
{
    let name = HeaderName::from_lowercase(name.as_bytes()).map_err(ErrorInternalServerError)?;
    let value = HeaderValue::from_str(value).map_err(ErrorInternalServerError)?;
    response.headers_mut().insert(name, value);
    Ok(())
}

fn extract_client_ip(req: &ServiceRequest) -> Option<IpAddr> {
    req.peer_addr().map(|addr| addr.ip())
}

#[cfg(test)]
mod tests {
    use super::RateLimitMiddleware;
    use crate::{
        config::{RateLimitAlgorithm, RateLimitBehavior, RateLimitConfig, RateLimitRule},
        limiter::RateLimiter,
        queue::ThrottledRequestQueue,
        store::RateLimitStore,
    };
    use actix_web::{
        App, HttpRequest, HttpResponse,
        http::StatusCode,
        test,
        web::{self, Data},
    };
    use redis::AsyncCommands;
    use std::{
        env,
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    async fn ok_handler(_request: HttpRequest) -> HttpResponse {
        HttpResponse::Ok().finish()
    }

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

    async fn queue_len(queue_key: &str) -> usize {
        let client = redis::Client::open(test_redis_url()).expect("failed to build redis client");
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .expect("failed to connect to redis");
        connection
            .llen(queue_key)
            .await
            .expect("failed to read redis queue length")
    }

    async fn build_middleware() -> (RateLimitMiddleware, String, String) {
        let rule_name = unique_name("status");
        let queue_key = unique_name("queue");
        let queue_client =
            redis::Client::open(test_redis_url()).expect("failed to build redis client");
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
                queue_key: queue_key.clone(),
            },
            Arc::new(build_store().await),
            Arc::new(ThrottledRequestQueue::new(queue_client)),
        );

        (RateLimitMiddleware::new(limiter), rule_name, queue_key)
    }

    #[actix_web::test]
    async fn middleware_throttles_second_request_and_enqueues_it() {
        let (middleware, rule_name, queue_key) = build_middleware().await;
        let app = test::init_service(
            App::new()
                .app_data(Data::new(()))
                .wrap(middleware)
                .route("/status", web::get().to(ok_handler)),
        )
        .await;

        let request_one = test::TestRequest::get()
            .uri("/status")
            .peer_addr(SocketAddr::from(([203, 0, 113, 10], 40000)))
            .to_request();
        let response_one = test::call_service(&app, request_one).await;
        assert_eq!(response_one.status(), StatusCode::OK);

        let request_two = test::TestRequest::get()
            .uri("/status")
            .peer_addr(SocketAddr::from(([203, 0, 113, 10], 40001)))
            .to_request();
        let response_two = test::call_service(&app, request_two).await;
        assert_eq!(response_two.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(queue_len(&queue_key).await, 1);
        delete_keys(&format!("rl:{rule_name}:*")).await;
        delete_keys(&queue_key).await;
    }

    #[actix_web::test]
    async fn middleware_ignores_forwarded_headers_for_rate_limit_identity() {
        let (middleware, rule_name, queue_key) = build_middleware().await;
        let app = test::init_service(
            App::new()
                .app_data(Data::new(()))
                .wrap(middleware)
                .route("/status", web::get().to(ok_handler)),
        )
        .await;

        let request_one = test::TestRequest::get()
            .uri("/status")
            .peer_addr(SocketAddr::from(([203, 0, 113, 10], 40000)))
            .insert_header(("x-forwarded-for", "198.51.100.1"))
            .to_request();
        let response_one = test::call_service(&app, request_one).await;
        assert_eq!(response_one.status(), StatusCode::OK);

        let request_two = test::TestRequest::get()
            .uri("/status")
            .peer_addr(SocketAddr::from(([203, 0, 113, 10], 40001)))
            .insert_header(("x-forwarded-for", "198.51.100.2"))
            .to_request();
        let response_two = test::call_service(&app, request_two).await;
        assert_eq!(response_two.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(queue_len(&queue_key).await, 1);
        delete_keys(&format!("rl:{rule_name}:*")).await;
        delete_keys(&queue_key).await;
    }
}
