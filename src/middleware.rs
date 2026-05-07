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
    net::{IpAddr, SocketAddr},
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
    req.connection_info()
        .realip_remote_addr()
        .and_then(parse_ip)
        .or_else(|| req.peer_addr().map(|addr| addr.ip()))
}

fn parse_ip(raw: &str) -> Option<IpAddr> {
    let candidate = raw.split(',').next()?.trim();

    if let Ok(ip) = candidate.parse::<IpAddr>() {
        return Some(ip);
    }

    if let Ok(addr) = candidate.parse::<SocketAddr>() {
        return Some(addr.ip());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::RateLimitMiddleware;
    use crate::{
        config::{RateLimitAlgorithm, RateLimitBehavior, RateLimitConfig, RateLimitRule},
        limiter::RateLimiter,
        queue::InMemoryThrottledRequestQueue,
        store::InMemoryCounterStore,
    };
    use actix_web::{
        App, HttpRequest, HttpResponse,
        http::StatusCode,
        test,
        web::{self, Data},
    };
    use std::{sync::Arc, time::Duration};

    async fn ok_handler(_request: HttpRequest) -> HttpResponse {
        HttpResponse::Ok().finish()
    }

    fn build_middleware() -> (RateLimitMiddleware, InMemoryThrottledRequestQueue) {
        let queue = InMemoryThrottledRequestQueue::default();
        let limiter = RateLimiter::new(
            RateLimitConfig {
                rules: vec![RateLimitRule {
                    name: "status".to_string(),
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
            Arc::new(InMemoryCounterStore::default()),
            Arc::new(queue.clone()),
        );

        (RateLimitMiddleware::new(limiter), queue)
    }

    #[actix_web::test]
    async fn middleware_throttles_second_request_and_enqueues_it() {
        let (middleware, queue) = build_middleware();
        let app = test::init_service(
            App::new()
                .app_data(Data::new(()))
                .wrap(middleware)
                .route("/status", web::get().to(ok_handler)),
        )
        .await;

        let request_one = test::TestRequest::get()
            .uri("/status")
            .insert_header(("x-forwarded-for", "203.0.113.10"))
            .to_request();
        let response_one = test::call_service(&app, request_one).await;
        assert_eq!(response_one.status(), StatusCode::OK);

        let request_two = test::TestRequest::get()
            .uri("/status")
            .insert_header(("x-forwarded-for", "203.0.113.10"))
            .to_request();
        let response_two = test::call_service(&app, request_two).await;
        assert_eq!(response_two.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(queue.len(), 1);
    }
}
