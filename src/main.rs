use actix_web::{App, HttpResponse, HttpServer, web};
use rate_limiter_sdi::{
    RateLimitMiddleware, RateLimiter, ThrottledRequestQueue, config::RateLimitConfig,
    store::RateLimitStore,
};
use std::{env, path::PathBuf, sync::Arc};

async fn status() -> HttpResponse {
    HttpResponse::Ok().body("ok")
}

async fn login() -> HttpResponse {
    HttpResponse::Ok().body("login accepted")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let redis_client = redis::Client::open(redis_url)
        .map_err(|error| std::io::Error::other(format!("redis client error: {error}")))?;

    let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/rate-limits.yaml");
    let config = RateLimitConfig::from_yaml_file(&config_path).map_err(|error| {
        std::io::Error::other(format!(
            "failed to load rate-limit config from {}: {error}",
            config_path.display()
        ))
    })?;

    let limiter = RateLimiter::new(
        config,
        Arc::new(RateLimitStore::new(redis_client.clone())),
        Arc::new(ThrottledRequestQueue::new(redis_client)),
    );

    HttpServer::new(move || {
        App::new()
            .wrap(RateLimitMiddleware::new(limiter.clone()))
            .route("/status", web::get().to(status))
            .route("/login", web::post().to(login))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
