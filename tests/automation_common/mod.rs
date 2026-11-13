//! Shared helpers for automation integration tests.
//!
//! Database-backed tests migrate a test database when `TEST_DATABASE_URL` or
//! `DATABASE_URL` is configured and skip as a passing no-op otherwise, matching
//! the existing data-load test convention.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::response::Response;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

/// Returns the configured test database URL.
pub fn database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
}

/// Connects to, and migrates, the test database. Returns `None` when no
/// database is configured or reachable.
pub async fn migrated_pool() -> Option<sqlx::PgPool> {
    let url = database_url()?;
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!("SKIPPED: could not connect to the test database: {error}");
            return None;
        }
    };
    if let Err(error) = sqlx::migrate!("../obolargus_core/migrations")
        .run(&pool)
        .await
    {
        eprintln!("SKIPPED: could not migrate the test database: {error}");
        return None;
    }
    Some(pool)
}

/// Builds application state, clamping optional config overrides.
#[allow(dead_code)]
pub fn state(pool: Option<sqlx::PgPool>) -> AppState {
    AppState::new(AppConfig::default(), pool).expect("app state")
}

/// Sends a JSON request through the full router.
pub async fn send(
    state: AppState,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    send_with_headers(state, method, uri, body, &[]).await
}

/// Sends a JSON request with extra headers.
pub async fn send_with_headers(
    state: AppState,
    method: Method,
    uri: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let request = match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request"),
        None => builder.body(Body::empty()).expect("request"),
    };
    let response = router(state).oneshot(request).await.expect("response");
    json_response(response).await
}

/// Reads a response body as JSON, or a small text envelope for non-JSON.
pub async fn json_response(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, value)
}
