//! No-database HTTP contract tests for the Data Load surface.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::registry::DataSourceRegistry;
use obolargus_core::market_data::trait_::MarketDataProvider;
use obolargus_core::market_data::types::{
    HistoricalRange, HistoricalRangeRequest, LatestBar, LatestBarRequest, TickerLookupRequest,
    TickerLookupResult,
};
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};
use tower::ServiceExt;

struct Source;

#[async_trait::async_trait]
impl MarketDataProvider for Source {
    fn source_id(&self) -> &str {
        "yahoo_finance"
    }
    fn display_name(&self) -> &str {
        "Yahoo Finance"
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
        Err(MarketDataProviderError::Unsupported(
            "not implemented".into(),
        ))
    }
    async fn get_historical_range(
        &self,
        _request: HistoricalRangeRequest,
    ) -> Result<HistoricalRange, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported(
            "not implemented".into(),
        ))
    }
    async fn lookup_ticker(
        &self,
        _request: TickerLookupRequest,
    ) -> Result<TickerLookupResult, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported(
            "not implemented".into(),
        ))
    }
}

fn state() -> AppState {
    let provider: std::sync::Arc<dyn MarketDataProvider + Send + Sync> =
        std::sync::Arc::new(Source);
    AppState {
        config: AppConfig::default(),
        pool: None,
        market_data: std::sync::Arc::new(
            DataSourceRegistry::new(vec![provider]).expect("registry"),
        ),
    }
}

async fn get(uri: &str) -> (StatusCode, String) {
    let response = router(state())
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("response");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

async fn send(method: &str, uri: &str, body: &str) -> (StatusCode, String) {
    let response = router(state())
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("response");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn domains_are_served_without_a_database() {
    let (status, body) = get("/api/data-load/domains").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("market_data"));
    assert!(body.contains("holdings"));
    assert!(body.contains("\"loadable\":false"));
}

#[tokio::test]
async fn source_catalog_lists_disabled_and_domain_flags() {
    let (status, body) = get("/api/data-load/sources?domain=market_data").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("yahoo_finance"));
    assert!(body.contains("\"selectable_for_domain\":true"));
    assert!(body.contains("\"supported_data_domains\":[\"market_data\"]"));
}

#[tokio::test]
async fn unknown_catalog_domain_is_a_400() {
    let (status, body) = get("/api/data-load/sources?domain=unknown").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("validation_error"));
}

#[tokio::test]
async fn toggle_requires_a_database() {
    let (status, body) = send(
        "PATCH",
        "/api/data-load/sources/yahoo_finance",
        r#"{"enabled":false}"#,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("database_unavailable"));
}

#[tokio::test]
async fn history_requires_a_database() {
    let (status, body) = get("/api/data-load/history").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("database_unavailable"));
}

#[tokio::test]
async fn progress_requires_a_database() {
    let (status, body) =
        get("/api/data-load/history/groups/00000000-0000-4000-8000-000000000001/progress").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("database_unavailable"));
}

#[tokio::test]
async fn non_loadable_domain_is_rejected_before_the_pool() {
    let (status, body) = send(
        "POST",
        "/api/market-data/load",
        r#"{"identifiers":["TICKER:XNAS:AAPL"],"date":"2026-10-01","data_domain":"holdings"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("not loadable"));
}

#[tokio::test]
async fn an_invalid_group_id_is_a_400() {
    let (status, body) = send(
        "POST",
        "/api/market-data/load-range",
        r#"{"identifiers":["TICKER:XNAS:AAPL"],"from":"2026-10-01","to":"2026-10-02","client_group_id":"not-a-uuid"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("not a valid group id"));
}
