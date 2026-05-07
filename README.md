# rate-limiter-sdi

A Rust rate limiter based on chapter 4 of *System Design Interview* by Alex Xu.

This project implements a server-side API rate limiter with:

- `actix-web` middleware
- per-IP enforcement
- Redis-backed shared state
- two algorithms: `fixed_window` and `token_bucket`
- optional queueing for throttled requests
- fail-open behavior when the limiter backend is unavailable

The implementation is designed to show the system design ideas clearly, not to be a fully production-hardened gateway.

## What It Does

For each incoming HTTP request, the middleware:

1. extracts the client IP address;
2. matches the request against a configured rule;
3. evaluates the selected rate-limiting algorithm in Redis;
4. allows the request or returns `429 Too Many Requests`;
5. adds rate-limit headers to the response;
6. optionally enqueues throttled requests as lightweight job descriptors.

If Redis fails, the middleware logs the issue and lets the request through.

## Project Structure

- [src/lib.rs](/Users/bilesimo/Development/rate-limiter-sdi/src/lib.rs): crate exports
- [src/config.rs](/Users/bilesimo/Development/rate-limiter-sdi/src/config.rs): YAML config model and validation
- [src/limiter.rs](/Users/bilesimo/Development/rate-limiter-sdi/src/limiter.rs): algorithm dispatch and enforcement logic
- [src/store.rs](/Users/bilesimo/Development/rate-limiter-sdi/src/store.rs): Redis and in-memory rate-limit state backends
- [src/middleware.rs](/Users/bilesimo/Development/rate-limiter-sdi/src/middleware.rs): `actix-web` middleware
- [src/queue.rs](/Users/bilesimo/Development/rate-limiter-sdi/src/queue.rs): throttled-request queue abstraction
- [examples/actix_demo.rs](/Users/bilesimo/Development/rate-limiter-sdi/examples/actix_demo.rs): runnable demo server
- [compose.yaml](/Users/bilesimo/Development/rate-limiter-sdi/compose.yaml): local Redis service for development
- [config/rate-limits.yaml](/Users/bilesimo/Development/rate-limiter-sdi/config/rate-limits.yaml): sample rules
- [docs/chapter-4-rate-limiter-plan.md](/Users/bilesimo/Development/rate-limiter-sdi/docs/chapter-4-rate-limiter-plan.md): implementation plan derived from the book chapter

## Supported Algorithms

### Fixed Window

`fixed_window` counts requests in a discrete time window.

Pros:

- simple
- memory efficient
- easy to explain in an interview

Tradeoff:

- bursts at the edge of adjacent windows can allow more traffic than a strict rolling limit

### Token Bucket

`token_bucket` allows short bursts while still enforcing an average rate over time.

Pros:

- handles bursty traffic better
- common real-world choice
- still efficient

Tradeoff:

- requires more state and slightly more logic than fixed window

## Configuration

Rules are loaded from YAML. A rule chooses a path, one or more HTTP methods, an algorithm, and what to do when the request is throttled.

Current behavior options:

- `block`: return `429` and do not queue
- `queue`: return `429` and enqueue a lightweight throttled-request descriptor

Example:

```yaml
queue_key: "ratelimiter:throttled"
rules:
  - name: "login-per-ip"
    path: "/login"
    methods:
      - "POST"
    algorithm:
      type: "fixed_window"
      requests_per_window: 5
      window: 60
    behavior: "queue"

  - name: "status-per-ip"
    path: "/status"
    methods:
      - "GET"
    algorithm:
      type: "token_bucket"
      capacity: 30
      refill_tokens: 5
      refill_interval: 10
    behavior: "block"
```

### Rule Fields

- `name`: unique logical identifier for the rule
- `path`: exact request path to match
- `methods`: list of HTTP methods; empty means any method
- `algorithm`: algorithm configuration
- `behavior`: `block` or `queue`

### Fixed Window Fields

- `type: fixed_window`
- `requests_per_window`: max requests allowed in the window
- `window`: window size in seconds

### Token Bucket Fields

- `type: token_bucket`
- `capacity`: max tokens in the bucket
- `refill_tokens`: how many tokens are added each refill interval
- `refill_interval`: refill interval in seconds

## Response Contract

Allowed and throttled responses include these headers:

- `x-ratelimit-limit`
- `x-ratelimit-remaining`
- `x-ratelimit-retry-after`
- `x-ratelimit-reset`

Throttled requests return `429 Too Many Requests` with a JSON body indicating:

- the rule that triggered the throttle
- whether the request was queued
- whether queueing failed
- how long the client should wait before retrying

## Running The Demo

Start Redis locally with Docker Compose:

```bash
docker compose up -d redis
```

This uses [compose.yaml](/Users/bilesimo/Development/rate-limiter-sdi/compose.yaml) and starts a Redis container on `127.0.0.1:6379`.

Then run:

```bash
cargo run --example actix_demo
```

Optional environment variable:

```bash
REDIS_URL=redis://127.0.0.1:6379 cargo run --example actix_demo
```

The demo server binds to `127.0.0.1:8080` and exposes:

- `GET /status`

When you are done, stop Redis with:

```bash
docker compose down
```

## Using It In An Actix App

Minimal integration shape:

```rust
use actix_web::{web, App, HttpResponse, HttpServer};
use rate_limiter_sdi::{
    config::RateLimitConfig,
    queue::RedisThrottledRequestQueue,
    store::RedisCounterStore,
    RateLimitMiddleware,
    RateLimiter,
};
use std::sync::Arc;

async fn status() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let redis_client = redis::Client::open("redis://127.0.0.1:6379")
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let config = RateLimitConfig::from_yaml_file("config/rate-limits.yaml")
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let limiter = RateLimiter::new(
        config,
        Arc::new(RedisCounterStore::new(redis_client.clone())),
        Arc::new(RedisThrottledRequestQueue::new(redis_client)),
    );

    HttpServer::new(move || {
        App::new()
            .wrap(RateLimitMiddleware::new(limiter.clone()))
            .route("/status", web::get().to(status))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
```

See [examples/actix_demo.rs](/Users/bilesimo/Development/rate-limiter-sdi/examples/actix_demo.rs) for the working example.

## Queueing Model

This project does not try to serialize and replay arbitrary raw HTTP requests.

Instead, throttled requests can be stored as lightweight descriptors containing:

- rule name
- source IP
- method
- path
- queue time
- retry-after value

That keeps the queueing side interview-friendly and avoids pretending that generic HTTP replay is trivial.

## Failure Behavior

The limiter is intentionally configured to fail open:

- if Redis is unavailable or rate-limit evaluation fails, the request proceeds;
- the middleware logs the degradation with `tracing::warn`.

This avoids turning the limiter into a hard availability dependency.

## Development

Useful commands:

```bash
cargo fmt
cargo check
cargo test
```

Current test coverage includes:

- config rule matching
- fixed-window limiter behavior
- token-bucket limiter behavior
- in-memory store behavior
- middleware throttling behavior

## Current Limitations

This project is intentionally scoped. It does not yet include:

- a rolling-window algorithm
- a replay worker for queued requests
- dynamic config reload
- distributed metrics/export pipelines
- multi-dimensional keys such as `user_id` or `api_key`
- wildcard or pattern-based route matching

## Design Notes

This implementation follows the chapter’s major recommendations:

- server-side enforcement
- centralized shared state
- clear throttling responses
- low-latency in-memory backing store
- explicit handling of distributed concerns

The detailed planning notes are in [docs/chapter-4-rate-limiter-plan.md](/Users/bilesimo/Development/rate-limiter-sdi/docs/chapter-4-rate-limiter-plan.md).
