use actix_web::{App, HttpResponse, HttpServer, web};
use rate_limiter_sdi::{
    RateLimitMiddleware, RateLimiter, config::RateLimitConfig, queue::RedisThrottledRequestQueue,
    store::RateLimitStore,
};
use std::{env, sync::Arc};

async fn return_ok() -> HttpResponse {
    HttpResponse::Ok().body("ok")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let redis_client = redis::Client::open(redis_url)
        .map_err(|error| std::io::Error::other(format!("redis client error: {error}")))?;

    let config = RateLimitConfig::from_yaml_file("config/rate-limits.yaml").map_err(|error| {
        std::io::Error::other(format!("failed to load rate-limit config: {error}"))
    })?;
    let limiter = RateLimiter::new(
        config,
        Arc::new(RateLimitStore::new(redis_client.clone())),
        Arc::new(RedisThrottledRequestQueue::new(redis_client)),
    );

    HttpServer::new(move || {
        App::new()
            .wrap(RateLimitMiddleware::new(limiter.clone()))
            .route("/status", web::get().to(return_ok))
            .route("/login", web::get().to(return_ok))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
