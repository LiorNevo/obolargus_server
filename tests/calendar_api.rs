//! HTTP contract tests for calendar coverage and calendar management.
//!
//! Database-gated: skips as a passing no-op when no test database is
//! configured.

use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

use obolargus_core::domain::asset::create::create_asset_with_identifier;
use obolargus_core::domain::asset::entity::NewAsset;
use obolargus_core::market_data::calendar::{
    export_document, get_calendar, seed_bundled_calendars,
};
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

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Bytes) {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(body) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&body).expect("json"))
        }
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("router answers");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    (status, bytes)
}

fn body_json(body: &Bytes) -> Value {
    serde_json::from_slice(body).expect("json body")
}

async fn seed_asset(pool: &PgPool, symbol: &str) -> uuid::Uuid {
    create_asset_with_identifier(
        pool,
        NewAsset {
            asset_type: "stock".into(),
            display_name: format!("Test {symbol}"),
            currency: "USD".into(),
            backed_by_portfolio_id: None,
            issuer_id: None,
        },
        "TICKER:XNAS",
        symbol,
    )
    .await
    .expect("asset")
    .id()
    .0
}

#[tokio::test]
async fn coverage_endpoint_returns_calendar_truth() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    seed_bundled_calendars(&pool).await.expect("seed");
    let asset_id = seed_asset(&pool, "COVER").await;
    let app = app(pool);

    let (status, body) = send(
        &app,
        "GET",
        &format!("/api/market-data/assets/{asset_id}/coverage?from=2025-11-26&to=2025-11-28"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = body_json(&body);
    assert_eq!(body["expected_session_count"], 2);
    assert_eq!(
        body["missing_sessions"],
        json!(["2025-11-26", "2025-11-28"])
    );
    assert_eq!(body["segments"][0]["mic"], "XNAS");

    let (status, _) = send(
        &app,
        "GET",
        "/api/market-data/assets/00000000-0000-4000-8000-000000000000/coverage?from=2025-11-26&to=2025-11-28",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(
        &app,
        "GET",
        &format!("/api/market-data/assets/{asset_id}/coverage?from=2025-11-29&to=2025-11-28"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = send(
        &app,
        "GET",
        &format!("/api/market-data/assets/{asset_id}/coverage?from=not-a-date&to=2025-11-28"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn management_surface_lists_imports_assigns_and_retires() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    seed_bundled_calendars(&pool).await.expect("seed");
    let asset_id = seed_asset(&pool, "MGMT").await;
    let app = app(pool.clone());

    let (status, body) = send(&app, "GET", "/api/market-data/calendars", None).await;
    assert_eq!(status, StatusCode::OK);
    let body = body_json(&body);
    assert_eq!(body["calendars"].as_array().expect("calendars").len(), 6);
    assert_eq!(body["calendars"][0]["mic"], "ARCX");

    let (status, body) = send(&app, "GET", "/api/market-data/calendars/XNAS/export", None).await;
    assert_eq!(status, StatusCode::OK);
    let mut document = body_json(&body);
    assert_eq!(document["mic"], "XNAS");

    document["name"] = json!("Nasdaq Test Name");
    let (status, body) = send(
        &app,
        "PUT",
        "/api/market-data/calendars/XNAS?expected_version=1",
        Some(document.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    let body = body_json(&body);
    assert_eq!(body["changed"], true);
    assert_eq!(body["calendar"]["version"], 2);

    let (status, body) = send(
        &app,
        "PUT",
        "/api/market-data/calendars/XNAS?expected_version=1",
        Some(document),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body_json(&body)["error"]["code"], "version_conflict");

    let mut invalid = export_document(
        &get_calendar(&pool, "XNAS")
            .await
            .expect("read")
            .expect("calendar"),
    );
    invalid.exceptions[0].date = chrono::NaiveDate::from_ymd_opt(1999, 1, 1).expect("date");
    let (status, body) = send(
        &app,
        "PUT",
        "/api/market-data/calendars/XNAS?expected_version=2",
        Some(serde_json::to_value(invalid).expect("document")),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        body_json(&body)["error"]["code"],
        "calendar_validation_failed"
    );

    let (status, body) = send(
        &app,
        "POST",
        "/api/market-data/calendar-assignments",
        Some(json!({
            "asset_id": asset_id,
            "mic": "XNAS",
            "valid_from": "2020-01-01",
            "valid_to": null
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    let assignment = body_json(&body);
    assert_eq!(assignment["mic"], "XNAS");

    let (status, body) = send(
        &app,
        "POST",
        "/api/market-data/calendar-assignments",
        Some(json!({
            "asset_id": asset_id,
            "mic": "XNYS",
            "valid_from": "2021-01-01",
            "valid_to": null
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body_json(&body)["error"]["code"], "assignment_overlap");

    let (status, body) = send(
        &app,
        "GET",
        &format!("/api/market-data/calendar-assignments?asset_id={asset_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body_json(&body)["total"], 1);

    let (status, body) = send(
        &app,
        "POST",
        "/api/market-data/calendars/XNAS/retire",
        Some(json!({"confirm": false})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let body = body_json(&body);
    assert_eq!(body["error"]["code"], "calendar_in_use");
    assert!(body["affected_count"].as_i64().unwrap_or(0) >= 1);

    let (status, body) = send(
        &app,
        "POST",
        "/api/market-data/calendars/XNAS/retire",
        Some(json!({"confirm": true})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body_json(&body)["calendar"]["active"], false);

    let assignment_id = assignment["id"].as_str().expect("assignment id");
    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/api/market-data/calendar-assignments/{assignment_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/api/market-data/calendar-assignments/{assignment_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // Calendar writes never touch price observations.
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_observations")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn management_error_branches_are_explicit() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    seed_bundled_calendars(&pool).await.expect("seed");
    let app = app(pool);

    let (status, _) = send(&app, "GET", "/api/market-data/calendars/ZZZZ/export", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let mut document = body_json(
        &send(&app, "GET", "/api/market-data/calendars/XNAS/export", None)
            .await
            .1,
    );
    document["active"] = json!(false);
    let (status, body) = send(
        &app,
        "PUT",
        "/api/market-data/calendars/XNAS?expected_version=1",
        Some(document),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(&body)["error"]["code"], "use_retire_endpoint");

    let (status, _) = send(
        &app,
        "PUT",
        "/api/market-data/calendars/XNYS",
        Some(json!({"mic": "XNAS"})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, _) = send(
        &app,
        "POST",
        "/api/market-data/calendars/ZZZZ/retire",
        Some(json!({"confirm": true})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        &app,
        "POST",
        "/api/market-data/calendar-assignments",
        Some(json!({
            "asset_id": "00000000-0000-4000-8000-000000000000",
            "mic": "ZZZZ",
            "valid_from": "2026-01-01",
            "valid_to": null
        })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        &app,
        "POST",
        "/api/market-data/calendar-assignments",
        Some(json!({
            "asset_id": "00000000-0000-4000-8000-000000000000",
            "mic": "XNAS",
            "valid_from": "2026-01-02",
            "valid_to": "2026-01-01"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, _) = send(
        &app,
        "GET",
        "/api/market-data/calendar-assignments?mic=XNAS&limit=10",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}
