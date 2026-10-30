//! HTTP contract tests for `GET /api/assets`.
//!
//! Database-gated: skips (as a passing no-op) when no test database is
//! configured, matching the server's integration-test convention.

use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

async fn migrated_pool() -> Option<PgPool> {
    let url = std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .ok()?;
    obolargus_core::bin_helpers::run_migrations(&pool)
        .await
        .ok()?;
    Some(pool)
}

fn app(pool: PgPool) -> axum::Router {
    router(AppState::new(AppConfig::default(), Some(pool)).expect("state"))
}

async fn send(app: &axum::Router, uri: &str) -> (StatusCode, Bytes) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router answers");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    (status, body)
}

#[tokio::test]
async fn search_matches_name_and_identifier() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let asset: Uuid = sqlx::query_scalar(
        "INSERT INTO assets (id, asset_type, display_name, currency) \
         VALUES (gen_random_uuid(), 'EQUITY', $1, 'USD') RETURNING id",
    )
    .bind(format!("Search Asset {}", Uuid::new_v4()))
    .fetch_one(&pool)
    .await
    .expect("seed asset");
    sqlx::query(
        "INSERT INTO asset_identifiers (id, asset_id, scheme, value) \
         VALUES (gen_random_uuid(), $1, 'TICKER', $2)",
    )
    .bind(asset)
    .bind(format!("SRCH{}", &Uuid::new_v4().to_string()[..6]))
    .execute(&pool)
    .await
    .expect("seed identifier");

    let app = app(pool.clone());
    let (status, body) = send(&app, "/api/assets?q=Search&limit=10").await;
    assert_eq!(status, StatusCode::OK);
    let json: Value = serde_json::from_slice(&body).expect("json");
    assert!(
        json["items"]
            .as_array()
            .expect("items")
            .iter()
            .any(|item| item["id"] == asset.to_string())
    );

    let (status, body) = send(&app, "/api/assets?q=no-such-asset-xyz").await;
    assert_eq!(status, StatusCode::OK);
    let json: Value = serde_json::from_slice(&body).expect("json");
    assert!(json["items"].as_array().expect("items").is_empty());

    for (uri, code) in [
        ("/api/assets?limit=0", "invalid_limit"),
        ("/api/assets?limit=51", "invalid_limit"),
    ] {
        let (status, body) = send(&app, uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let json: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(json["error"]["code"], code);
    }

    sqlx::query("DELETE FROM asset_identifiers WHERE asset_id = $1")
        .bind(asset)
        .execute(&pool)
        .await
        .expect("cleanup identifiers");
    sqlx::query("DELETE FROM assets WHERE id = $1")
        .bind(asset)
        .execute(&pool)
        .await
        .expect("cleanup asset");
}

fn app_without_database() -> axum::Router {
    router(AppState::new(AppConfig::default(), None).expect("state"))
}

#[tokio::test]
async fn search_validates_query_and_limit_without_a_database() {
    let app = app_without_database();
    let long_query = "a".repeat(201);
    for (uri, code) in [
        ("/api/assets?limit=0", "invalid_limit"),
        ("/api/assets?limit=51", "invalid_limit"),
        (
            &format!("/api/assets?q={long_query}") as &str,
            "invalid_query",
        ),
    ] {
        let (status, body) = send(&app, uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let json: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(json["error"]["code"], code);
    }
}

#[tokio::test]
async fn search_without_a_database_returns_infrastructure() {
    let (status, body) = send(&app_without_database(), "/api/assets?q=AAPL").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let json: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["error"]["code"], "infrastructure");
}
