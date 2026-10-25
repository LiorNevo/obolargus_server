//! HTTP contract tests for the market-data API.
//!
//! Covers the handler surface that does not need a database: source
//! enumeration, the lookup path, the validation guards, and the
//! "no database pool" degradation. The database-backed paths are covered
//! by `tests/market_data_e2e.rs` (which is gated on `DATABASE_URL`).
//!
//! Implements T113 / T067 / T077 / T088 of
//! `specs/017-yahoo-data-source/tasks.md`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use obolargus_core::market_data::MarketDataProviderError;
use tower::ServiceExt;

use obolargus_core::market_data::registry::DataSourceRegistry;
use obolargus_core::market_data::trait_::MarketDataProvider;
use obolargus_core::market_data::types::{
    HistoricalRange, HistoricalRangeRequest, LatestBar, LatestBarRequest, PriceBar,
    TickerLookupHit, TickerLookupRequest, TickerLookupResult,
};
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

use async_trait::async_trait;
use chrono::NaiveDate;
use rust_decimal::Decimal;

/// A provider that answers the lookup with a fixed hit list.
struct LookupProvider {
    source_id: String,
    hits: Vec<TickerLookupHit>,
}

#[async_trait]
impl MarketDataProvider for LookupProvider {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn display_name(&self) -> &str {
        "Lookup Provider"
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
        Ok(TickerLookupResult {
            hits: self.hits.clone(),
            source_id: self.source_id.clone(),
        })
    }
}

/// Builds a state whose registry holds the given providers.
fn state_with(providers: Vec<LookupProvider>) -> AppState {
    let providers: Vec<std::sync::Arc<dyn MarketDataProvider + Send + Sync>> = providers
        .into_iter()
        .map(|p| std::sync::Arc::new(p) as _)
        .collect();
    let registry = DataSourceRegistry::new(providers).expect("registry");
    AppState {
        config: AppConfig::default(),
        pool: None,
        market_data: std::sync::Arc::new(registry),
    }
}

/// Builds a lookup hit with the given symbol and relevance.
fn hit(symbol: &str, relevance: f64) -> TickerLookupHit {
    TickerLookupHit {
        symbol: symbol.to_string(),
        short_name: Some(format!("{symbol} Inc.")),
        long_name: None,
        quote_type: Some("EQUITY".to_string()),
        exchange: Some("NMS".to_string()),
        currency: None,
        schemes: vec![],
        relevance: Some(relevance),
    }
}

/// Reads the response body as a string.
async fn body_text(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// `GET /api/market-data/sources` lists the configured sources.
#[tokio::test]
async fn sources_endpoint_lists_every_configured_source() {
    let state = state_with(vec![
        LookupProvider {
            source_id: "alpha".to_string(),
            hits: vec![],
        },
        LookupProvider {
            source_id: "beta".to_string(),
            hits: vec![],
        },
    ]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .uri("/api/market-data/sources")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let text = body_text(response).await;
    assert!(text.contains("\"source_id\":\"alpha\""), "body: {text}");
    assert!(text.contains("\"source_id\":\"beta\""), "body: {text}");
}

/// `GET /api/market-data/lookup?q=…` returns the hits, sorted by
/// relevance descending then symbol ascending.
#[tokio::test]
async fn lookup_endpoint_returns_sorted_hits() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![hit("ZZZ", 0.1), hit("AAA", 0.9), hit("MMM", 0.5)],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .uri("/api/market-data/lookup?q=MSFT")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let text = body_text(response).await;
    let aaa = text.find("AAA").expect("AAA should be present");
    let mmm = text.find("MMM").expect("MMM should be present");
    let zzz = text.find("ZZZ").expect("ZZZ should be present");
    assert!(
        aaa < mmm && mmm < zzz,
        "hits must be sorted by relevance (body: {text})"
    );
}

/// An empty `q` is a 400.
#[tokio::test]
async fn lookup_endpoint_rejects_an_empty_query() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .uri("/api/market-data/lookup?q=%20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("q must be non-empty"), "body: {text}");
}

/// A lookup with no matching hits returns an empty list, not an error.
#[tokio::test]
async fn lookup_endpoint_returns_empty_hits_for_an_unknown_query() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .uri("/api/market-data/lookup?q=ZZZZZNOTREAL")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let text = body_text(response).await;
    assert!(text.contains("\"hits\":[]"), "body: {text}");
}

/// `POST /api/market-data/refresh` without a database pool degrades to a
/// 503 rather than a 500.
#[tokio::test]
async fn refresh_endpoint_without_a_pool_returns_503() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/refresh")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"asset_id":"00000000-0000-0000-0000-000000000001"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let text = body_text(response).await;
    assert!(text.contains("no database pool configured"), "body: {text}");
}

/// `POST /api/market-data/reconcile` without a database pool degrades to
/// a 503.
#[tokio::test]
async fn reconcile_endpoint_without_a_pool_returns_503() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/reconcile")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"asset_id":"00000000-0000-0000-0000-000000000001","fact_type":"market_price","as_of_date":"2026-01-02","policy":"primary_wins"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// The deferred `"majority"` policy is rejected with a 400 — it is never
/// silently substituted with a shipped policy.
#[tokio::test]
async fn reconcile_endpoint_rejects_the_deferred_majority_policy() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/reconcile")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"asset_id":"00000000-0000-0000-0000-000000000001","fact_type":"market_price","as_of_date":"2026-01-02","policy":"majority"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("Majority is deferred"), "body: {text}");
}

/// An unknown policy name is a 400.
#[tokio::test]
async fn reconcile_endpoint_rejects_an_unknown_policy() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/reconcile")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"asset_id":"00000000-0000-0000-0000-000000000001","fact_type":"market_price","as_of_date":"2026-01-02","policy":"nonsense"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("unknown policy"), "body: {text}");
}

/// The typed-error → HTTP-status mapping is total.
#[test]
fn typed_errors_map_to_http_statuses() {
    use obolargus_server::routes::market_data::typed_error_to_http;

    let cases: Vec<(MarketDataProviderError, StatusCode)> = vec![
        (
            MarketDataProviderError::NotFound("x".into()),
            StatusCode::BAD_REQUEST,
        ),
        (
            MarketDataProviderError::InvalidInput("x".into()),
            StatusCode::BAD_REQUEST,
        ),
        (
            MarketDataProviderError::Unsupported("x".into()),
            StatusCode::BAD_REQUEST,
        ),
        (
            MarketDataProviderError::Validation("x".into()),
            StatusCode::BAD_REQUEST,
        ),
        (
            MarketDataProviderError::Unauthorized("x".into()),
            StatusCode::UNAUTHORIZED,
        ),
        (
            MarketDataProviderError::Transient("x".into()),
            StatusCode::BAD_GATEWAY,
        ),
        (
            MarketDataProviderError::RateLimited {
                msg: "x".into(),
                retry_after: None,
            },
            StatusCode::BAD_GATEWAY,
        ),
        (
            MarketDataProviderError::Infrastructure("x".into()),
            StatusCode::BAD_GATEWAY,
        ),
    ];

    for (error, expected) in cases {
        let (status, message) = typed_error_to_http(&error);
        assert_eq!(status, expected, "wrong status for {error:?}");
        assert!(!message.is_empty(), "the message should not be empty");
    }
}

/// The price-bar payload shape is stable (six fields, decimal prices).
#[test]
fn price_bar_payload_shape_is_stable() {
    let bar = PriceBar {
        as_of_date: NaiveDate::from_ymd_opt(2026, 1, 2).expect("date"),
        open: Decimal::new(100, 0),
        high: Decimal::new(101, 0),
        low: Decimal::new(99, 0),
        close: Decimal::new(100, 0),
        adjclose: Decimal::new(100, 0),
        volume: 1_234_567,
    };
    let payload = obolargus_core::market_data::refresh::serialize_bar(&bar);
    for field in ["open", "high", "low", "close", "adjclose", "volume"] {
        assert!(
            payload.get(field).is_some(),
            "the payload should carry a '{field}' field (got {payload})"
        );
    }
    assert!(payload["close"].is_string(), "prices are decimal strings");
}

// ---------------------------------------------------------------------------
// `POST /api/market-data/load` — the batch reference-data load (spec 018)
// ---------------------------------------------------------------------------

/// A valid load body reaches the operation; without a pool it degrades to
/// a 503 like its sibling routes.
#[tokio::test]
async fn load_without_a_database_pool_returns_503() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/load")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"identifiers": ["TICKER:AAPL"], "date": "2026-10-24"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let text = body_text(response).await;
    assert!(text.contains("no database pool configured"), "body: {text}");
}

/// An empty identifier list is a caller-input validation failure.
#[tokio::test]
async fn load_rejects_an_empty_identifier_list() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/load")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"identifiers": [], "date": "2026-10-24"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("identifier list is empty"), "body: {text}");
}

/// A malformed identifier string is a caller-input validation failure.
#[tokio::test]
async fn load_rejects_a_malformed_identifier() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/load")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"identifiers": ["AAPL"], "date": "2026-10-24"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("validation error"), "body: {text}");
}

/// A future date is a caller-input validation failure.
#[tokio::test]
async fn load_rejects_a_future_date() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/load")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"identifiers": ["TICKER:AAPL"], "date": "2099-01-01"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// An unparseable date is a caller-input validation failure.
#[tokio::test]
async fn load_rejects_an_unparseable_date() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/load")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"identifiers": ["TICKER:AAPL"], "date": "not-a-date"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// An identifier object without a type is a caller-input validation failure.
#[tokio::test]
async fn load_rejects_an_identifier_without_a_type() {
    let state = state_with(vec![LookupProvider {
        source_id: "mock".to_string(),
        hits: vec![],
    }]);

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/load")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"identifiers": [{"type": "", "value": "AAPL"}], "date": "2026-10-24"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("validation error"), "body: {text}");
}

/// The identifier input accepts both the string form and the explicit
/// `{type, value}` form.
#[tokio::test]
async fn load_accepts_both_identifier_forms() {
    use obolargus_server::routes::market_data::IdentifierInput;

    let text_form = IdentifierInput::Text("TICKER:XNAS:AAPL".to_string())
        .into_identifier()
        .expect("text form");
    assert_eq!(text_form.scheme, "TICKER:XNAS");
    assert_eq!(text_form.value, "AAPL");

    let pair_form = IdentifierInput::Pair {
        identifier_type: "ticker:xnas".to_string(),
        value: "aapl".to_string(),
    }
    .into_identifier()
    .expect("pair form");
    assert_eq!(pair_form.scheme, "TICKER:XNAS");
    assert_eq!(pair_form.value, "aapl");
}
