//! No-database HTTP contract tests for the Data Load surface.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::registry::DataSourceRegistry;
use obolargus_core::market_data::trait_::MarketDataProvider;
use obolargus_core::market_data::types::{
    HistoricalRange, HistoricalRangeRequest, LatestBar, LatestBarRequest, TickerLookupHit,
    TickerLookupRequest, TickerLookupResult,
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
        automation: std::sync::Arc::new(obolargus_server::automation::state::AutomationState::new()),
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
async fn identifier_search_validation_rejects_bad_queries() {
    for uri in [
        "/api/data-load/identifier-search",
        "/api/data-load/identifier-search?q=",
        "/api/data-load/identifier-search?q=%20%20",
        "/api/data-load/identifier-search?q=a",
        "/api/data-load/identifier-search?q=ab&domain=unknown",
        "/api/data-load/identifier-search?q=ab&domain=holdings",
        "/api/data-load/identifier-search?q=ab&limit=0",
        "/api/data-load/identifier-search?q=ab&limit=101",
        "/api/data-load/identifier-search?q=ab&offset=-1",
    ] {
        let (status, body) = get(uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "uri: {uri} body: {body}");
        assert!(body.contains("validation_error"), "uri: {uri} body: {body}");
    }
    let too_long = format!("/api/data-load/identifier-search?q={}", "a".repeat(121));
    let (status, body) = get(&too_long).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("validation_error"));
}

#[tokio::test]
async fn identifier_search_requires_a_database_after_validation() {
    let (status, body) = get("/api/data-load/identifier-search?q=apple").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("database_unavailable"));
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
async fn async_range_validates_before_the_pool() {
    let (status, body) = send(
        "POST",
        "/api/market-data/load-range/async",
        r#"{"identifiers":["TICKER:XNAS:AAPL"],"from":"2026-10-01","to":"2026-10-02","client_group_id":"not-a-uuid"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("not a valid group id"));
}

#[tokio::test]
async fn async_range_requires_a_pool_after_validation() {
    let (status, body) = send(
        "POST",
        "/api/market-data/load-range/async",
        r#"{"identifiers":["TICKER:XNAS:AAPL"],"from":"2026-10-01","to":"2026-10-02"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("no database pool configured"));
}

#[tokio::test]
async fn stopping_an_inactive_group_reports_false() {
    let (status, body) = send(
        "POST",
        "/api/data-load/history/groups/00000000-0000-4000-8000-000000000001/stop",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"stopped\":false"));
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

// ---------------------------------------------------------------------------
// Database-gated identifier-search outcome contract.
//
// Upstream failures are data in the 200 body, never a 4xx/5xx. These tests
// skip as a passing no-op when no test database is configured.
// ---------------------------------------------------------------------------

fn search_test_database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
}

async fn search_migrated_pool() -> Option<sqlx::PgPool> {
    let url = search_test_database_url()?;
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
    if let Err(error) = obolargus_core::temporal::catalog::sync_fact_catalog(&pool).await {
        eprintln!("SKIPPED: could not sync the fact catalog: {error}");
        return None;
    }
    Some(pool)
}

struct ScriptedSearchSource {
    source_id: String,
    display_name: String,
    priority: i32,
    answer: Result<TickerLookupResult, MarketDataProviderError>,
}

#[async_trait::async_trait]
impl MarketDataProvider for ScriptedSearchSource {
    fn source_id(&self) -> &str {
        &self.source_id
    }
    fn display_name(&self) -> &str {
        &self.display_name
    }
    fn supported_schemes(&self) -> &[String] {
        static SCHEMES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
        SCHEMES.get_or_init(|| vec!["TICKER".to_string()])
    }
    fn enabled(&self) -> bool {
        true
    }
    fn priority(&self) -> i32 {
        self.priority
    }
    async fn get_latest_bar(
        &self,
        _request: LatestBarRequest,
    ) -> Result<LatestBar, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported(
            "search contract test".into(),
        ))
    }
    async fn get_historical_range(
        &self,
        _request: HistoricalRangeRequest,
    ) -> Result<HistoricalRange, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported(
            "search contract test".into(),
        ))
    }
    async fn lookup_ticker(
        &self,
        _request: TickerLookupRequest,
    ) -> Result<TickerLookupResult, MarketDataProviderError> {
        self.answer.clone()
    }
}

fn search_hit(symbol: &str) -> TickerLookupHit {
    TickerLookupHit {
        symbol: symbol.to_string(),
        short_name: Some(format!("{symbol} Inc.")),
        long_name: None,
        quote_type: Some("EQUITY".to_string()),
        exchange: Some("NMS".to_string()),
        currency: Some("USD".to_string()),
        schemes: Vec::new(),
        relevance: Some(0.9),
    }
}

fn answered(source_id: &str, priority: i32, symbols: &[&str]) -> ScriptedSearchSource {
    ScriptedSearchSource {
        source_id: source_id.to_string(),
        display_name: format!("{source_id} display"),
        priority,
        answer: Ok(TickerLookupResult {
            hits: symbols.iter().map(|symbol| search_hit(symbol)).collect(),
            source_id: source_id.to_string(),
        }),
    }
}

fn failed(source_id: &str, priority: i32, error: MarketDataProviderError) -> ScriptedSearchSource {
    ScriptedSearchSource {
        source_id: source_id.to_string(),
        display_name: format!("{source_id} display"),
        priority,
        answer: Err(error),
    }
}

fn search_state(pool: sqlx::PgPool, sources: Vec<ScriptedSearchSource>) -> AppState {
    let providers: Vec<std::sync::Arc<dyn MarketDataProvider + Send + Sync>> = sources
        .into_iter()
        .map(|source| {
            std::sync::Arc::new(source) as std::sync::Arc<dyn MarketDataProvider + Send + Sync>
        })
        .collect();
    AppState {
        config: AppConfig::default(),
        pool: Some(pool),
        market_data: std::sync::Arc::new(DataSourceRegistry::new(providers).expect("registry")),
        automation: std::sync::Arc::new(obolargus_server::automation::state::AutomationState::new()),
    }
}

async fn search(state: AppState, query: &str) -> (StatusCode, serde_json::Value) {
    let response = router(state)
        .oneshot(
            Request::builder()
                .uri(format!("/api/data-load/identifier-search?q={query}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).expect("json body");
    (status, json)
}

#[tokio::test]
async fn identifier_search_returns_200_with_a_complete_outcome() {
    let Some(pool) = search_migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let state = search_state(pool, vec![answered("one", 1, &["AAPL"])]);
    let (status, body) = search(state, "apple").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "complete");
    assert_eq!(body["total"], 1);
    assert_eq!(body["sources"][0]["status"], "answered");
    assert_eq!(body["results"][0]["symbol"], "AAPL");
}

#[tokio::test]
async fn identifier_search_reports_a_partial_outcome_in_the_200_body() {
    let Some(pool) = search_migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let state = search_state(
        pool,
        vec![
            answered("one", 1, &["AAPL"]),
            failed(
                "two",
                2,
                MarketDataProviderError::Transient("upstream down".into()),
            ),
        ],
    );
    let (status, body) = search(state, "apple").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "partial");
    assert_eq!(body["total"], 1);
    let statuses = body["sources"]
        .as_array()
        .expect("sources")
        .iter()
        .map(|source| source["status"].as_str().unwrap_or(""))
        .collect::<Vec<_>>();
    assert!(statuses.contains(&"answered"));
    assert!(statuses.contains(&"unavailable"));
}

#[tokio::test]
async fn identifier_search_reports_a_total_failure_in_the_200_body() {
    let Some(pool) = search_migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let state = search_state(
        pool,
        vec![failed(
            "one",
            1,
            MarketDataProviderError::Infrastructure("boom".into()),
        )],
    );
    let (status, body) = search(state, "apple").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "failed");
    assert_eq!(body["total"], 0);
    assert_eq!(body["sources"][0]["status"], "unavailable");
    assert!(body["message"].as_str().is_some());
}

#[tokio::test]
async fn identifier_search_reports_a_rate_limit_with_its_cooldown() {
    let Some(pool) = search_migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let state = search_state(
        pool,
        vec![failed(
            "one",
            1,
            MarketDataProviderError::RateLimited {
                msg: "budget".into(),
                retry_after: Some(std::time::Duration::from_secs(60)),
            },
        )],
    );
    let (status, body) = search(state, "apple").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "failed");
    assert_eq!(body["sources"][0]["status"], "rate_limited");
    assert_eq!(body["sources"][0]["retry_after_seconds"], 60);
}

#[tokio::test]
async fn identifier_search_reports_no_hits_as_a_complete_no_match() {
    let Some(pool) = search_migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let state = search_state(pool, vec![answered("one", 1, &[])]);
    let (status, body) = search(state, "nothing").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "complete");
    assert_eq!(body["total"], 0);
    assert_eq!(body["sources"][0]["status"], "no_match");
}
