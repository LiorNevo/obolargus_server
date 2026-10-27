//! HTTP contract tests for the temporal (bitemporal) fact endpoints.
//!
//! Drives `routes::temporal` through the real router, so the path segments,
//! query parsing, status mapping and JSON envelope are all exercised.
//!
//! The database-backed cases are gated on `TEST_DATABASE_URL` /
//! `DATABASE_URL` and skip (as a passing no-op) when no database is
//! configured, per the `obolargus_server/AGENTS.md` convention. They share
//! the database with the core and CLI suites, so the workspace runs one
//! crate's tests at a time (`RUST_TEST_THREADS=1`).

use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

/// Connects to the configured test database and applies the migrations.
async fn migrated_pool() -> Option<PgPool> {
    let url = std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())?;
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
    if let Err(error) = obolargus_core::bin_helpers::run_migrations(&pool).await {
        eprintln!("SKIPPED: could not apply the migrations: {error}");
        return None;
    }
    Some(pool)
}

/// Inserts an asset and returns its id, so the fact tables' foreign keys are
/// satisfied.
async fn seed_asset(pool: &PgPool) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO assets (id, asset_type, display_name, currency) \
         VALUES (gen_random_uuid(), 'EQUITY', $1, 'USD') RETURNING id",
    )
    .bind(format!("temporal-api-{}", Uuid::new_v4()))
    .fetch_one(pool)
    .await
    .expect("seed an asset")
}

async fn drop_asset(pool: &PgPool, id: Uuid) {
    for statement in [
        "DELETE FROM asset_reference_data WHERE asset_id = $1",
        "DELETE FROM assets WHERE id = $1",
    ] {
        sqlx::query(statement)
            .bind(id)
            .execute(pool)
            .await
            .expect("clean up the seeded asset");
    }
}

fn app(pool: Option<PgPool>) -> axum::Router {
    let state = AppState::new(AppConfig::default(), pool).expect("application state");
    router(state)
}

async fn send(app: &axum::Router, request: Request<Body>) -> (StatusCode, Bytes) {
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("read the response body")
        .to_bytes();
    (status, body)
}

fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
        .expect("request")
}

async fn error_code(body: &Bytes) -> String {
    let value: Value = serde_json::from_slice(body).expect("the error is JSON");
    value["error"]["code"]
        .as_str()
        .expect("the envelope carries a code")
        .to_string()
}

/// A full append → read → history → retract round trip.
#[tokio::test]
async fn fact_endpoints_round_trip() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let asset_id = seed_asset(&pool).await;
    let app = app(Some(pool.clone()));
    let base = format!("/api/domain/assets/{asset_id}/facts/market_price");
    let source = format!("api-coverage-{}", Uuid::new_v4());

    // Append.
    let (status, body) = send(
        &app,
        json_request(
            "POST",
            &base,
            json!({
                "valid_at": "2026-02-01",
                "known_at": "2026-02-02T00:00:00Z",
                "value": "{\"close\":\"100\"}",
                "source": source,
            }),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let point: Value = serde_json::from_slice(&body).expect("point JSON");
    assert_eq!(point["subject"], "asset");
    assert_eq!(point["subject_id"], asset_id.to_string());
    assert_eq!(point["fact_type"], "market_price");
    assert_eq!(point["source"], source);
    assert_eq!(point["retracted"], false);
    assert!(point["valid_at"].as_str().is_some());
    assert!(point["known_at"].as_str().is_some());

    // Latest-knowledge read.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!("{base}?valid_at=2026-02-01"))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let read: Value = serde_json::from_slice(&body).expect("read JSON");
    assert_eq!(read["value"], "{\"close\":\"100\"}");
    assert_eq!(read["source"], source);

    // As-known read (the RFC 3339 branch of the instant parser).
    let (status, _) = send(
        &app,
        Request::builder()
            .uri(format!(
                "{base}?valid_at=2026-02-01&known_at=2026-02-02T00:00:00Z"
            ))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // An as-known read before the belief existed is a 404 `no_value`.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!(
                "{base}?valid_at=2026-02-01&known_at=2026-02-01T00:00:00Z"
            ))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body).await, "no_value");

    // History lists the revision.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!("{base}/history?valid_at=2026-02-01"))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let history: Value = serde_json::from_slice(&body).expect("history JSON");
    assert_eq!(history.as_array().expect("array").len(), 1);

    // A duplicate append at the same known-at conflicts (409).
    let (status, body) = send(
        &app,
        json_request(
            "POST",
            &base,
            json!({
                "valid_at": "2026-02-01",
                "known_at": "2026-02-02T00:00:00Z",
                "value": "{\"close\":\"101\"}",
                "source": source,
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error_code(&body).await, "conflicting_append");

    // Retract with a later known-at.
    let (status, body) = send(
        &app,
        json_request(
            "POST",
            &format!("{base}/retract"),
            json!({
                "valid_at": "2026-02-01",
                "known_at": "2026-02-03T00:00:00Z",
                "reason": "superseded",
                "source": source,
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let retraction: Value = serde_json::from_slice(&body).expect("retraction JSON");
    assert_eq!(retraction["retracted"], true);

    // The current read now reports no value.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!("{base}?valid_at=2026-02-01"))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body).await, "no_value");

    // History keeps both revisions, retraction first.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!("{base}/history?valid_at=2026-02-01"))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let history: Value = serde_json::from_slice(&body).expect("history JSON");
    let rows = history.as_array().expect("array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["retracted"], true);
    assert_eq!(rows[1]["retracted"], false);

    // An empty history is also a 404 `no_value`.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!("{base}/history?valid_at=2024-01-01"))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body).await, "no_value");

    // A missing fact is a 404 `no_value`.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!("{base}?valid_at=2024-01-01"))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body).await, "no_value");

    drop_asset(&pool, asset_id).await;
}

/// Malformed paths and instants map onto the contract's 400/404 codes.
#[tokio::test]
async fn temporal_validation_maps_to_contract_errors() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let asset_id = seed_asset(&pool).await;
    let app = app(Some(pool.clone()));

    // Unknown subject kind.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!(
                "/api/domain/houses/{asset_id}/facts/market_price?valid_at=2026-02-01"
            ))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body).await, "unknown_subject");

    // Invalid instant on a read.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!(
                "/api/domain/assets/{asset_id}/facts/market_price?valid_at=nope"
            ))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body).await, "invalid_time");

    // Invalid instant on an append.
    let (status, body) = send(
        &app,
        json_request(
            "POST",
            &format!("/api/domain/assets/{asset_id}/facts/market_price"),
            json!({
                "valid_at": "2026-02-01",
                "known_at": "not-an-instant",
                "value": "{}",
                "source": "api",
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body).await, "invalid_time");

    // Invalid subject kind on an append.
    let (status, body) = send(
        &app,
        json_request(
            "POST",
            &format!("/api/domain/houses/{asset_id}/facts/market_price"),
            json!({
                "valid_at": "2026-02-01",
                "known_at": "2026-02-02T00:00:00Z",
                "value": "{}",
                "source": "api",
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body).await, "unknown_subject");

    // Invalid instant on a history read.
    let (status, body) = send(
        &app,
        Request::builder()
            .uri(format!(
                "/api/domain/assets/{asset_id}/facts/market_price/history?valid_at=nope"
            ))
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body).await, "invalid_time");

    // Invalid instant on a retraction.
    let (status, body) = send(
        &app,
        json_request(
            "POST",
            &format!("/api/domain/assets/{asset_id}/facts/market_price/retract"),
            json!({
                "valid_at": "nope",
                "known_at": "2026-02-03T00:00:00Z",
                "reason": "bad",
                "source": "api",
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body).await, "invalid_time");

    drop_asset(&pool, asset_id).await;
}

/// Every handler degrades to `infrastructure` when no pool is configured.
#[tokio::test]
async fn temporal_handlers_need_a_database_pool() {
    let app = app(None);
    let asset_id = Uuid::from_u128(42);
    let base = format!("/api/domain/assets/{asset_id}/facts/market_price");

    let requests = [
        Request::builder()
            .uri(format!("{base}?valid_at=2026-02-01"))
            .body(Body::empty())
            .expect("request"),
        json_request(
            "POST",
            &base,
            json!({
                "valid_at": "2026-02-01",
                "known_at": "2026-02-02T00:00:00Z",
                "value": "{}",
                "source": "api",
            }),
        ),
        json_request(
            "POST",
            &format!("{base}/retract"),
            json!({
                "valid_at": "2026-02-01",
                "known_at": "2026-02-02T00:00:00Z",
                "reason": "reason",
                "source": "api",
            }),
        ),
        Request::builder()
            .uri(format!("{base}/history?valid_at=2026-02-01"))
            .body(Body::empty())
            .expect("request"),
    ];

    for request in requests {
        let (status, body) = send(&app, request).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error_code(&body).await, "infrastructure");
    }
}
