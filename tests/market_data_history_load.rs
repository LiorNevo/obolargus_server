//! Contract surface for the additive chart history endpoint.
//!
//! Database-backed behavior is covered by the shared market-data integration
//! suite; this file locks the HTTP/OpenAPI surface that the chart consumes.

use serde_json::Value;

#[test]
fn history_load_operation_is_in_the_openapi_document() {
    let document: Value =
        serde_json::to_value(obolargus_server::openapi_document()).expect("OpenAPI document");
    let operation = &document["paths"]["/api/market-data/history-load"]["post"];
    assert!(
        operation.is_object(),
        "missing POST /api/market-data/history-load"
    );
    assert_eq!(operation["tags"][0], "market-data");
    assert_eq!(
        operation["summary"],
        "Load a bounded history window for an asset"
    );
}

#[test]
fn history_load_documents_the_request_and_report_schemas() {
    let document: Value =
        serde_json::to_value(obolargus_server::openapi_document()).expect("OpenAPI document");
    let operation = &document["paths"]["/api/market-data/history-load"]["post"];
    let request = &operation["requestBody"]["content"]["application/json"]["schema"];
    assert!(request.is_object());
    let response = &operation["responses"]["200"]["content"]["application/json"]["schema"];
    assert!(response.is_object());
}

use axum::Json;
use axum::extract::State;
use obolargus_server::AppState;
use obolargus_server::config::AppConfig;
use obolargus_server::routes::market_data::{HistoryLoadBody, history_load};

fn state_without_pool() -> AppState {
    AppState::new(AppConfig::default(), None).expect("default app state")
}

fn body(from: &str, to: &str) -> HistoryLoadBody {
    HistoryLoadBody {
        asset_id: uuid::Uuid::from_u128(1),
        from: from.to_string(),
        to: to.to_string(),
        source: None,
    }
}

#[tokio::test]
async fn rejects_invalid_dates_before_consulting_the_pool() {
    let state = state_without_pool();
    let error = history_load(State(state), Json(body("not-a-date", "2026-01-01")))
        .await
        .expect_err("invalid date");
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_reversed_and_future_ranges() {
    let state = state_without_pool();
    let reversed = history_load(State(state.clone()), Json(body("2026-02-01", "2026-01-01")))
        .await
        .expect_err("reversed");
    assert_eq!(reversed.0, axum::http::StatusCode::BAD_REQUEST);

    let future = history_load(State(state), Json(body("2027-01-01", "2027-02-01")))
        .await
        .expect_err("future");
    assert_eq!(future.0, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_ranges_longer_than_the_bounded_window() {
    let state = state_without_pool();
    let today = chrono::Utc::now().date_naive();
    let from = today - chrono::Duration::days(500);
    let error = history_load(
        State(state),
        Json(body(
            &from.format("%Y-%m-%d").to_string(),
            &today.format("%Y-%m-%d").to_string(),
        )),
    )
    .await
    .expect_err("over-long");
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn valid_requests_reach_the_pool_guard() {
    let state = state_without_pool();
    let today = chrono::Utc::now().date_naive();
    let error = history_load(
        State(state),
        Json(body(
            &(today - chrono::Duration::days(2))
                .format("%Y-%m-%d")
                .to_string(),
            &today.format("%Y-%m-%d").to_string(),
        )),
    )
    .await
    .expect_err("no pool");
    assert_eq!(error.0, axum::http::StatusCode::SERVICE_UNAVAILABLE);
}
