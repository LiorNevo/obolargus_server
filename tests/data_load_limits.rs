//! Pre-flight limit and input-shape tests for the range-load endpoints.
//!
//! Invalid requests must be rejected before any provider or database work.

mod data_load_common;

use axum::http::StatusCode;
use chrono::NaiveDate;
use serde_json::Value;
use uuid::Uuid;

use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::trait_::MarketDataProvider;
use obolargus_core::market_data::types::{
    HistoricalRange, HistoricalRangeRequest, LatestBar, LatestBarRequest, TickerLookupRequest,
    TickerLookupResult,
};

use data_load_common::{identifier, migrated_pool, range_body, send, state_with};

/// A provider that fails the test if the pre-flight rejects reach it.
struct NeverCalledProvider {
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl MarketDataProvider for NeverCalledProvider {
    fn source_id(&self) -> &str {
        "never_called"
    }
    fn display_name(&self) -> &str {
        "Never Called"
    }
    fn supported_schemes(&self) -> &[String] {
        static SCHEMES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
        SCHEMES.get_or_init(|| vec!["TICKER".to_string()])
    }
    fn enabled(&self) -> bool {
        true
    }
    fn priority(&self) -> i32 {
        10
    }
    async fn get_latest_bar(
        &self,
        _request: LatestBarRequest,
    ) -> Result<LatestBar, MarketDataProviderError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(MarketDataProviderError::Transient("unexpected".into()))
    }
    async fn get_historical_range(
        &self,
        _request: HistoricalRangeRequest,
    ) -> Result<HistoricalRange, MarketDataProviderError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(MarketDataProviderError::Transient("unexpected".into()))
    }
    async fn lookup_ticker(
        &self,
        _request: TickerLookupRequest,
    ) -> Result<TickerLookupResult, MarketDataProviderError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(MarketDataProviderError::Transient("unexpected".into()))
    }
}

fn date(value: &str) -> NaiveDate {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("date")
}

fn state() -> obolargus_server::AppState {
    state_with(None, None)
}

async fn post_range(body: Value) -> data_load_common::TestResponse {
    send(
        state(),
        "POST",
        "/api/market-data/load-range/async",
        Some(body),
    )
    .await
}

fn assert_field(response: &data_load_common::TestResponse, field: &str) {
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .error_details()
            .and_then(|details| details.get("field")),
        Some(&Value::String(field.to_string())),
        "body: {}",
        response.body
    );
}

#[tokio::test]
async fn too_many_identifiers_is_a_limit_error() {
    let identifiers = (0..501)
        .map(|index| identifier(&format!("T{index}")))
        .collect();
    let response = post_range(range_body(
        identifiers,
        "2026-01-01",
        "2026-01-02",
        4,
        Uuid::new_v4(),
    ))
    .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.error_code(), Some("limit_exceeded"));
    assert_field(&response, "identifiers");
    assert_eq!(
        response
            .error_details()
            .and_then(|details| details.get("limit")),
        Some(&Value::from(500))
    );
}

#[tokio::test]
async fn too_many_days_is_a_limit_error() {
    let from = date("2020-01-01");
    let to = from + chrono::Duration::days(1_826);
    let response = post_range(range_body(
        vec![identifier("AAPL")],
        &from.to_string(),
        &to.to_string(),
        4,
        Uuid::new_v4(),
    ))
    .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.error_code(), Some("limit_exceeded"));
    assert_field(&response, "range_days");
}

#[tokio::test]
async fn too_many_identifier_days_is_a_limit_error() {
    let identifiers = (0..500)
        .map(|index| identifier(&format!("T{index}")))
        .collect();
    let from = date("2020-01-01");
    let to = from + chrono::Duration::days(200);
    let response = post_range(range_body(
        identifiers,
        &from.to_string(),
        &to.to_string(),
        4,
        Uuid::new_v4(),
    ))
    .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.error_code(), Some("limit_exceeded"));
    assert_field(&response, "identifier_days");
}

#[tokio::test]
async fn a_request_at_the_limits_passes_preflight() {
    let identifiers = (0..500)
        .map(|index| identifier(&format!("T{index}")))
        .collect();
    let from = date("2020-01-01");
    let to = from + chrono::Duration::days(199);
    let response = post_range(range_body(
        identifiers,
        &from.to_string(),
        &to.to_string(),
        32,
        Uuid::new_v4(),
    ))
    .await;
    // No database pool is configured: validation passed and the request
    // reached the pool check.
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn zero_concurrency_and_over_max_concurrency_are_rejected() {
    for concurrency in [0, 33] {
        let response = post_range(range_body(
            vec![identifier("AAPL")],
            "2026-01-01",
            "2026-01-02",
            concurrency,
            Uuid::new_v4(),
        ))
        .await;
        assert_field(&response, "concurrency");
    }
}

#[tokio::test]
async fn an_empty_identifier_list_is_rejected() {
    let response = post_range(range_body(
        Vec::new(),
        "2026-01-01",
        "2026-01-02",
        4,
        Uuid::new_v4(),
    ))
    .await;
    assert_field(&response, "identifiers");
}

#[tokio::test]
async fn a_reversed_range_is_rejected() {
    let response = post_range(range_body(
        vec![identifier("AAPL")],
        "2026-02-01",
        "2026-01-01",
        4,
        Uuid::new_v4(),
    ))
    .await;
    assert_field(&response, "range");
}

#[tokio::test]
async fn an_end_date_more_than_one_day_ahead_is_rejected() {
    let today = chrono::Utc::now().date_naive();
    let to = today + chrono::Duration::days(2);
    let response = post_range(range_body(
        vec![identifier("AAPL")],
        &today.to_string(),
        &to.to_string(),
        4,
        Uuid::new_v4(),
    ))
    .await;
    assert_field(&response, "to");
}

#[tokio::test]
async fn duplicates_are_removed_before_counting() {
    let identifiers = (0..501).map(|_| identifier("aapl")).collect();
    let response = post_range(range_body(
        identifiers,
        "2026-01-01",
        "2026-01-02",
        4,
        Uuid::new_v4(),
    ))
    .await;
    // 501 raw entries with one normalized key pass the 500 limit.
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn a_malformed_date_names_the_field() {
    let response = post_range(range_body(
        vec![identifier("AAPL")],
        "not-a-date",
        "2026-01-02",
        4,
        Uuid::new_v4(),
    ))
    .await;
    assert_field(&response, "from");
}

#[tokio::test]
async fn invalid_requests_never_call_a_provider() {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let provider: std::sync::Arc<dyn MarketDataProvider + Send + Sync> =
        std::sync::Arc::new(NeverCalledProvider {
            calls: std::sync::Arc::clone(&calls),
        });
    let state = state_with(None, Some(provider));

    let bodies = [
        range_body(vec![], "2026-01-01", "2026-01-02", 4, Uuid::new_v4()),
        range_body(
            vec![identifier("AAPL")],
            "2026-02-01",
            "2026-01-01",
            4,
            Uuid::new_v4(),
        ),
        range_body(
            vec![identifier("AAPL")],
            "2026-01-01",
            "2026-01-02",
            0,
            Uuid::new_v4(),
        ),
        range_body(
            (0..501)
                .map(|index| identifier(&format!("T{index}")))
                .collect(),
            "2026-01-01",
            "2026-01-02",
            4,
            Uuid::new_v4(),
        ),
    ];
    for body in bodies {
        let response = send(
            state.clone(),
            "POST",
            "/api/market-data/load-range/async",
            Some(body),
        )
        .await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "rejected requests never reach a provider"
    );
}

#[tokio::test]
async fn invalid_requests_write_no_audit_row() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM reference_data_load_runs")
        .fetch_one(&pool)
        .await
        .expect("count runs");
    let response = send(
        state_with(Some(pool.clone()), None),
        "POST",
        "/api/market-data/load-range",
        Some(range_body(
            Vec::new(),
            "2026-01-01",
            "2026-01-02",
            4,
            Uuid::new_v4(),
        )),
    )
    .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM reference_data_load_runs")
        .fetch_one(&pool)
        .await
        .expect("count runs");
    assert_eq!(before, after, "a rejected request writes no audit row");
}
