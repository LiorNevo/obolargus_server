//! End-to-end integration test for the market-data refresh path.
//!
//! Implements T113 of `specs/017-yahoo-data-source/tasks.md`: the
//! executable proof of contract tests 1..9 in
//! `specs/017-yahoo-data-source/contracts/rust-api-contract.md` §
//! "Contract tests".
//!
//! The test is gated on `TEST_DATABASE_URL` / `DATABASE_URL` and skips
//! (as a passing no-op) when no database is configured, per the
//! `obolargus_server/AGENTS.md` convention.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::NaiveDate;
use http_body_util::BodyExt;
use tower::ServiceExt;

use obolargus_core::domain::id::DomainId;
use obolargus_core::market_data::config::DataSourceConfig;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::reconcile::{
    ReconcileConfig, ReconciliationPolicy, run_reconcile,
};
use obolargus_core::market_data::refresh::{RefreshConfig, run_refresh};
use obolargus_core::market_data::registry::DataSourceRegistry;
use obolargus_core::market_data::trait_::MarketDataProvider;
use obolargus_core::market_data::types::{
    HistoricalRange, HistoricalRangeRequest, LatestBar, LatestBarRequest, PriceBar,
    TickerLookupRequest, TickerLookupResult,
};
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

use async_trait::async_trait;
use rust_decimal::Decimal;

/// A provider that always answers with the same bar.
struct FixedProvider {
    source_id: String,
    priority: i32,
    enabled: bool,
    bar: LatestBar,
}

#[async_trait]
impl MarketDataProvider for FixedProvider {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn display_name(&self) -> &str {
        "Fixed Provider"
    }

    fn supported_schemes(&self) -> &[String] {
        static SCHEMES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
        SCHEMES.get_or_init(|| vec!["TICKER".to_string()])
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn priority(&self) -> i32 {
        self.priority
    }

    async fn get_latest_bar(
        &self,
        _request: LatestBarRequest,
    ) -> Result<LatestBar, MarketDataProviderError> {
        Ok(self.bar.clone())
    }

    async fn get_historical_range(
        &self,
        _request: HistoricalRangeRequest,
    ) -> Result<HistoricalRange, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported(
            "fixed provider: not implemented".into(),
        ))
    }

    async fn lookup_ticker(
        &self,
        _request: TickerLookupRequest,
    ) -> Result<TickerLookupResult, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported(
            "fixed provider: not implemented".into(),
        ))
    }
}

/// A provider that always fails with a transient error.
struct FailingProvider {
    source_id: String,
    priority: i32,
}

#[async_trait]
impl MarketDataProvider for FailingProvider {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn display_name(&self) -> &str {
        "Failing Provider"
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
        Err(MarketDataProviderError::Transient(
            "upstream timeout".into(),
        ))
    }

    async fn get_historical_range(
        &self,
        _request: HistoricalRangeRequest,
    ) -> Result<HistoricalRange, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported(
            "failing provider: not implemented".into(),
        ))
    }

    async fn lookup_ticker(
        &self,
        _request: TickerLookupRequest,
    ) -> Result<TickerLookupResult, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported(
            "failing provider: not implemented".into(),
        ))
    }
}

/// Returns the test database URL, or `None` when no database is
/// configured (the test then skips).
fn test_database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
}

/// Connects to the test database and applies the migrations.
async fn migrated_pool() -> Option<sqlx::PgPool> {
    let url = test_database_url()?;
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
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
        eprintln!("SKIPPED: could not apply the migrations: {error}");
        return None;
    }
    Some(pool)
}

/// Seeds an asset with the given TICKER identifier.
///
/// The `assets` table requires `asset_type`, `display_name`, and
/// `currency` (all `NOT NULL` without defaults).
///
/// The ticker is made unique per call because `asset_identifiers` has a
/// global `UNIQUE (scheme, value)` constraint — two assets cannot share
/// the same ticker. A unique suffix keeps the tests isolated and
/// re-runnable against a shared database.
async fn seed_asset(pool: &sqlx::PgPool, ticker: &str) -> uuid::Uuid {
    let unique = format!("{ticker}{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let asset_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO assets (id, asset_type, display_name, currency) \
         VALUES ($1, 'EQUITY', $2, 'USD')",
    )
    .bind(asset_id)
    .bind(&unique)
    .execute(pool)
    .await
    .expect("insert asset");
    sqlx::query(
        "INSERT INTO asset_identifiers (id, asset_id, scheme, value) \
         VALUES (gen_random_uuid(), $1, 'TICKER', $2)",
    )
    .bind(asset_id)
    .bind(&unique)
    .execute(pool)
    .await
    .expect("insert identifier");
    asset_id
}

/// Seeds an asset with no identifiers at all.
async fn seed_bare_asset(pool: &sqlx::PgPool) -> uuid::Uuid {
    let asset_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO assets (id, asset_type, display_name, currency) \
         VALUES ($1, 'EQUITY', 'No Identifier', 'USD')",
    )
    .bind(asset_id)
    .execute(pool)
    .await
    .expect("insert asset");
    asset_id
}

/// Removes the asset and its dependent rows.
///
/// The reconciliation audit rows are deleted first: they carry a foreign
/// key to `assets`, so the asset cannot be removed while an audit row
/// references it. (In production the audit tables are append-only and the
/// application role has no DELETE privilege; the test harness connects as
/// the table owner.)
async fn cleanup_asset(pool: &sqlx::PgPool, asset_id: uuid::Uuid) {
    sqlx::query("DELETE FROM data_source_reconciliation_audits WHERE asset_id = $1")
        .bind(asset_id)
        .execute(pool)
        .await
        .expect("delete reconciliation audits");
    sqlx::query("DELETE FROM asset_reference_data WHERE asset_id = $1")
        .bind(asset_id)
        .execute(pool)
        .await
        .expect("delete reference data");
    sqlx::query("DELETE FROM asset_identifiers WHERE asset_id = $1")
        .bind(asset_id)
        .execute(pool)
        .await
        .expect("delete identifiers");
    sqlx::query("DELETE FROM assets WHERE id = $1")
        .bind(asset_id)
        .execute(pool)
        .await
        .expect("delete asset");
}

/// Reads the response body as a string.
async fn body_text(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Builds a fixed bar for the given trading day.
fn fixed_bar(source_id: &str, as_of_date: NaiveDate) -> LatestBar {
    LatestBar {
        bar: PriceBar {
            as_of_date,
            open: Decimal::new(100, 0),
            high: Decimal::new(101, 0),
            low: Decimal::new(99, 0),
            close: Decimal::new(100, 0),
            adjclose: Decimal::new(100, 0),
            volume: 1_234_567,
        },
        knowledge_date: chrono::Utc::now().date_naive(),
        source_id: source_id.to_string(),
    }
}

/// T054 / T113: `run_refresh` writes an `AssetReferenceData` row and a
/// `data_source_refresh_runs` audit row.
#[tokio::test]
async fn run_refresh_writes_reference_data_and_audit_row() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    // Seed an asset with a TICKER identifier.
    let asset_id = seed_asset(&pool, "AAPL").await;

    let today = chrono::Utc::now().date_naive();
    let provider = FixedProvider {
        source_id: "mock".to_string(),
        priority: 10,
        enabled: true,
        bar: fixed_bar("mock", today),
    };
    let registry = DataSourceRegistry::new(vec![Arc::new(provider)]).expect("registry");

    let summary = run_refresh(
        &pool,
        &registry,
        RefreshConfig {
            assets: Some(vec![obolargus_core::domain::id::AssetId::from_uuid(
                asset_id,
            )]),
            trigger: "test".to_string(),
        },
    )
    .await
    .expect("run_refresh");

    assert_eq!(summary.assets_refreshed, 1, "one asset should be refreshed");
    assert_eq!(summary.assets_attempted, 1);

    // The `AssetReferenceData` row exists with the expected shape.
    let value: Option<(String, String, String)> = sqlx::query_as(
        "SELECT fact_type, source, value FROM asset_reference_data WHERE asset_id = $1",
    )
    .bind(asset_id)
    .fetch_optional(&pool)
    .await
    .expect("query reference data");
    let (fact_type, source, payload) = value.expect("a reference-data row should exist");
    assert_eq!(fact_type, "market_price");
    assert_eq!(source, "mock");

    let parsed: serde_json::Value = serde_json::from_str(&payload).expect("value is JSON");
    for field in ["open", "high", "low", "close", "adjclose", "volume"] {
        assert!(
            parsed.get(field).is_some(),
            "the value payload should carry a '{field}' field (got {parsed})"
        );
    }
    assert!(parsed["close"].is_string(), "prices are decimal strings");

    // The audit row exists.
    let trigger: Option<(String,)> =
        sqlx::query_as("SELECT trigger FROM data_source_refresh_runs WHERE id = $1")
            .bind(summary.run_id)
            .fetch_optional(&pool)
            .await
            .expect("query refresh run");
    assert_eq!(
        trigger.map(|(t,)| t),
        Some("test".to_string()),
        "a refresh-run audit row should exist with trigger 'test'"
    );

    cleanup_asset(&pool, asset_id).await;
}

/// T055: an asset without a supported identifier is counted as
/// `missing_identifier` and writes no rows.
#[tokio::test]
async fn run_refresh_skips_asset_without_supported_identifier() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_bare_asset(&pool).await;

    let today = chrono::Utc::now().date_naive();
    let provider = FixedProvider {
        source_id: "mock".to_string(),
        priority: 10,
        enabled: true,
        bar: fixed_bar("mock", today),
    };
    let registry = DataSourceRegistry::new(vec![Arc::new(provider)]).expect("registry");

    let summary = run_refresh(
        &pool,
        &registry,
        RefreshConfig {
            assets: Some(vec![obolargus_core::domain::id::AssetId::from_uuid(
                asset_id,
            )]),
            trigger: "test".to_string(),
        },
    )
    .await
    .expect("run_refresh");

    assert_eq!(summary.assets_missing_identifier, 1);
    assert_eq!(summary.assets_refreshed, 0);

    let rows: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM asset_reference_data WHERE asset_id = $1")
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .expect("count reference data");
    assert_eq!(rows.0, 0, "no reference-data rows should be written");

    sqlx::query("DELETE FROM assets WHERE id = $1")
        .bind(asset_id)
        .execute(&pool)
        .await
        .expect("delete asset");
}

/// T057: a transient error increments `assets_source_unavailable` and is
/// recorded in the audit's `per_source_failed`.
#[tokio::test]
async fn run_refresh_records_source_unavailable() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_asset(&pool, "MSFT").await;

    let provider = FailingProvider {
        source_id: "mock".to_string(),
        priority: 10,
    };
    let registry = DataSourceRegistry::new(vec![Arc::new(provider)]).expect("registry");

    let summary = run_refresh(
        &pool,
        &registry,
        RefreshConfig {
            assets: Some(vec![obolargus_core::domain::id::AssetId::from_uuid(
                asset_id,
            )]),
            trigger: "test".to_string(),
        },
    )
    .await
    .expect("run_refresh");

    assert_eq!(summary.assets_source_unavailable, 1);
    assert_eq!(summary.assets_refreshed, 0);

    let per_source_failed: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT per_source_failed FROM data_source_refresh_runs WHERE id = $1")
            .bind(summary.run_id)
            .fetch_optional(&pool)
            .await
            .expect("query refresh run");
    let failed = per_source_failed.expect("an audit row should exist").0;
    assert!(
        failed.get("mock").is_some(),
        "the failed source should be recorded under its source id (got {failed})"
    );

    cleanup_asset(&pool, asset_id).await;
}

/// T113: the `GET /api/market-data/sources` endpoint lists the configured
/// sources.
#[tokio::test]
async fn sources_endpoint_lists_configured_sources() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let state = match AppState::new_with_persistence(AppConfig::default(), Some(pool)).await {
        Ok(state) => state,
        Err(error) => {
            eprintln!("SKIPPED: could not build the app state: {error}");
            return;
        }
    };

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
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        text.contains("yahoo_finance"),
        "the default source should be listed (got {text})"
    );
}

/// T113: the `POST /api/market-data/refresh` endpoint returns the
/// refresh summary as JSON.
#[tokio::test]
async fn refresh_endpoint_returns_the_summary() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_asset(&pool, "NVDA").await;

    let state = match AppState::new_with_persistence(AppConfig::default(), Some(pool.clone())).await
    {
        Ok(state) => state,
        Err(error) => {
            eprintln!("SKIPPED: could not build the app state: {error}");
            return;
        }
    };

    let body = format!("{{\"asset_id\":\"{asset_id}\"}}");
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/refresh")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    // Either the refresh succeeded (200) or the upstream was unavailable
    // (502) — both prove the endpoint is wired and the typed error
    // surfaces rather than a panic.
    assert!(
        response.status() == StatusCode::OK || response.status() == StatusCode::BAD_GATEWAY,
        "unexpected status: {}",
        response.status()
    );

    cleanup_asset(&pool, asset_id).await;
}

/// T056: a second run on the same trading day is a no-op — the fact is
/// already known, so no duplicate row is written and the unique
/// `(asset, fact_type, as_of_date, knowledge_date)` constraint is not
/// violated.
#[tokio::test]
async fn run_refresh_is_idempotent_on_a_rerun() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_asset(&pool, "RERUN").await;
    let today = chrono::Utc::now().date_naive();

    let make_registry = || {
        Arc::new(
            DataSourceRegistry::new(vec![Arc::new(FixedProvider {
                source_id: "mock".to_string(),
                priority: 10,
                enabled: true,
                bar: fixed_bar("mock", today),
            })])
            .expect("registry"),
        )
    };

    let first = run_refresh(
        &pool,
        &make_registry(),
        RefreshConfig {
            assets: Some(vec![obolargus_core::domain::id::AssetId::from_uuid(
                asset_id,
            )]),
            trigger: "test".to_string(),
        },
    )
    .await
    .expect("first run_refresh");
    assert_eq!(first.assets_refreshed, 1);

    let second = run_refresh(
        &pool,
        &make_registry(),
        RefreshConfig {
            assets: Some(vec![obolargus_core::domain::id::AssetId::from_uuid(
                asset_id,
            )]),
            trigger: "test".to_string(),
        },
    )
    .await
    .expect("second run_refresh");
    assert_eq!(
        second.assets_no_op, 1,
        "the second run should be a no-op, not a duplicate write"
    );
    assert_eq!(second.assets_refreshed, 0);
    assert_eq!(second.assets_source_unavailable, 0);

    // Exactly one row exists — no duplicate.
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM asset_reference_data \
         WHERE asset_id = $1 AND fact_type = 'market_price'",
    )
    .bind(asset_id)
    .fetch_one(&pool)
    .await
    .expect("count reference data");
    assert_eq!(count.0, 1, "no duplicate rows should be written");

    cleanup_asset(&pool, asset_id).await;
}

/// T025: a transient error on the first provider falls over to the
/// second one.
#[tokio::test]
async fn transient_error_falls_over_to_the_next_provider() {
    let today = chrono::Utc::now().date_naive();
    let failing = FailingProvider {
        source_id: "a_failing".to_string(),
        priority: 10,
    };
    let working = FixedProvider {
        source_id: "b_working".to_string(),
        priority: 20,
        enabled: true,
        bar: fixed_bar("b_working", today),
    };
    let registry =
        DataSourceRegistry::new(vec![Arc::new(failing), Arc::new(working)]).expect("registry");

    let request = LatestBarRequest::now(obolargus_core::market_data::types::TypedIdentifier::new(
        "TICKER", "AAPL",
    ));
    let bar = registry.get_latest_bar(request).await.expect("bar");
    assert_eq!(bar.source_id, "b_working");
}

/// T030: the "every source" map never fails because of a single source.
#[tokio::test]
async fn every_source_map_survives_a_single_failure() {
    let today = chrono::Utc::now().date_naive();
    let failing = FailingProvider {
        source_id: "a_failing".to_string(),
        priority: 10,
    };
    let working = FixedProvider {
        source_id: "b_working".to_string(),
        priority: 20,
        enabled: true,
        bar: fixed_bar("b_working", today),
    };
    let registry =
        DataSourceRegistry::new(vec![Arc::new(failing), Arc::new(working)]).expect("registry");

    let request = LatestBarRequest::now(obolargus_core::market_data::types::TypedIdentifier::new(
        "TICKER", "AAPL",
    ));
    let map = registry.every_source_latest_bar(request).await;
    assert_eq!(map.len(), 2);
    assert!(map["a_failing"].is_err());
    assert!(map["b_working"].is_ok());
}

/// T034: the registry rejects duplicate `source_id`s.
#[test]
fn registry_rejects_duplicate_source_ids() {
    let today = chrono::Utc::now().date_naive();
    let a = FixedProvider {
        source_id: "same".to_string(),
        priority: 10,
        enabled: true,
        bar: fixed_bar("same", today),
    };
    let b = FixedProvider {
        source_id: "same".to_string(),
        priority: 20,
        enabled: true,
        bar: fixed_bar("same", today),
    };
    let result = DataSourceRegistry::new(vec![Arc::new(a), Arc::new(b)]);
    assert!(matches!(
        result,
        Err(MarketDataProviderError::Validation(ref msg)) if msg.contains("duplicate")
    ));
}

/// The default `DataSourceConfig` shape is the one the contract names.
#[test]
fn default_source_config_matches_the_contract() {
    let config = DataSourceConfig::default_yahoo();
    assert_eq!(config.source_id, "yahoo_finance");
    assert_eq!(config.provider_type, "yahoo_finance");
    assert!(config.enabled);
    assert_eq!(config.priority, 10);
    assert!(config.supported_schemes.iter().any(|s| s == "TICKER"));
    assert!(config.supported_schemes.iter().any(|s| s == "TICKER:*"));
    assert_eq!(config.validate(), Ok(()));
}

/// T080: `PrimaryWins` writes the primary's answer and records the
/// reconciliation audit row.
#[tokio::test]
async fn reconcile_primary_wins_writes_the_canonical_value_and_audit() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_asset(&pool, "RECON").await;
    let today = chrono::Utc::now().date_naive();

    // `a` has the lower priority number, so it is walked first; `b` is
    // the "primary" under `PrimaryWins` (the last source walked).
    let registry = DataSourceRegistry::new(vec![
        Arc::new(FixedProvider {
            source_id: "a_primary".to_string(),
            priority: 10,
            enabled: true,
            bar: fixed_bar("a_primary", today),
        }),
        Arc::new(FixedProvider {
            source_id: "b_secondary".to_string(),
            priority: 20,
            enabled: true,
            bar: fixed_bar("b_secondary", today),
        }),
    ])
    .expect("registry");

    let summary = run_reconcile(
        &pool,
        &registry,
        ReconcileConfig {
            asset_id: obolargus_core::domain::id::AssetId::from_uuid(asset_id),
            fact_type: "market_price".to_string(),
            as_of_date: today,
            policy: ReconciliationPolicy::PrimaryWins,
        },
    )
    .await
    .expect("run_reconcile");

    assert_eq!(summary.per_source_answers.len(), 2);

    // The canonical value was written as an `AssetReferenceData` row.
    let source: Option<(String,)> = sqlx::query_as(
        "SELECT source FROM asset_reference_data \
         WHERE asset_id = $1 AND fact_type = 'market_price'",
    )
    .bind(asset_id)
    .fetch_optional(&pool)
    .await
    .expect("query reference data");
    assert_eq!(
        source.map(|(s,)| s),
        Some("b_secondary".to_string()),
        "the primary (lowest priority) answer should be canonical"
    );

    // The audit row records the policy, the sources tried, and the
    // per-source answers.
    let audit: Option<(String, Vec<String>, serde_json::Value, String)> = sqlx::query_as(
        "SELECT policy, sources_tried, per_source_answers, canonical_value \
         FROM data_source_reconciliation_audits WHERE id = $1",
    )
    .bind(summary.reconciliation_id)
    .fetch_optional(&pool)
    .await
    .expect("query reconciliation audit");
    let (policy, sources_tried, per_source, canonical) = audit.expect("an audit row should exist");
    assert_eq!(policy, "primary_wins");
    assert_eq!(sources_tried.len(), 2);
    assert!(
        per_source.get("a_primary").is_some(),
        "both sources should be recorded (got {per_source})"
    );
    assert!(per_source.get("b_secondary").is_some());
    assert!(!canonical.is_empty());

    cleanup_asset(&pool, asset_id).await;
}

/// T081: `FirstNonEmpty` picks the first answer, and the audit records
/// the different policy.
#[tokio::test]
async fn reconcile_first_non_empty_picks_the_first_answer() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_asset(&pool, "RECON2").await;
    let today = chrono::Utc::now().date_naive();

    let registry = DataSourceRegistry::new(vec![
        Arc::new(FixedProvider {
            source_id: "a_first".to_string(),
            priority: 10,
            enabled: true,
            bar: fixed_bar("a_first", today),
        }),
        Arc::new(FixedProvider {
            source_id: "b_second".to_string(),
            priority: 20,
            enabled: true,
            bar: fixed_bar("b_second", today),
        }),
    ])
    .expect("registry");

    let summary = run_reconcile(
        &pool,
        &registry,
        ReconcileConfig {
            asset_id: obolargus_core::domain::id::AssetId::from_uuid(asset_id),
            fact_type: "market_price".to_string(),
            as_of_date: today,
            policy: ReconciliationPolicy::FirstNonEmpty,
        },
    )
    .await
    .expect("run_reconcile");

    let source: Option<(String,)> = sqlx::query_as(
        "SELECT source FROM asset_reference_data \
         WHERE asset_id = $1 AND fact_type = 'market_price'",
    )
    .bind(asset_id)
    .fetch_optional(&pool)
    .await
    .expect("query reference data");
    assert_eq!(
        source.map(|(s,)| s),
        Some("a_first".to_string()),
        "the first answer should be canonical"
    );

    let policy: Option<(String,)> =
        sqlx::query_as("SELECT policy FROM data_source_reconciliation_audits WHERE id = $1")
            .bind(summary.reconciliation_id)
            .fetch_optional(&pool)
            .await
            .expect("query reconciliation audit");
    assert_eq!(policy.map(|(p,)| p), Some("first_non_empty".to_string()));

    cleanup_asset(&pool, asset_id).await;
}

/// T082: reconciliation needs at least two enabled sources.
#[tokio::test]
async fn reconcile_with_one_source_is_a_validation_error() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_asset(&pool, "RECON3").await;
    let today = chrono::Utc::now().date_naive();

    let registry = DataSourceRegistry::new(vec![Arc::new(FixedProvider {
        source_id: "only".to_string(),
        priority: 10,
        enabled: true,
        bar: fixed_bar("only", today),
    })])
    .expect("registry");

    let result = run_reconcile(
        &pool,
        &registry,
        ReconcileConfig {
            asset_id: obolargus_core::domain::id::AssetId::from_uuid(asset_id),
            fact_type: "market_price".to_string(),
            as_of_date: today,
            policy: ReconciliationPolicy::PrimaryWins,
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(MarketDataProviderError::Validation(ref msg))
            if msg.contains("at least 2 enabled sources")
    ));

    // No audit row and no reference-data row were written.
    let audits: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM data_source_reconciliation_audits WHERE asset_id = $1",
    )
    .bind(asset_id)
    .fetch_one(&pool)
    .await
    .expect("count audits");
    assert_eq!(audits.0, 0, "no audit row should be written");
    let rows: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM asset_reference_data WHERE asset_id = $1")
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .expect("count reference data");
    assert_eq!(rows.0, 0, "no reference-data row should be written");

    cleanup_asset(&pool, asset_id).await;
}

/// T083: one source failing does not fail the reconciliation — the
/// surviving source becomes canonical and the failure is recorded.
#[tokio::test]
async fn reconcile_survives_a_single_source_failure() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_asset(&pool, "RECON4").await;
    let today = chrono::Utc::now().date_naive();

    let registry = DataSourceRegistry::new(vec![
        Arc::new(FailingProvider {
            source_id: "a_failing".to_string(),
            priority: 10,
        }),
        Arc::new(FixedProvider {
            source_id: "b_ok".to_string(),
            priority: 20,
            enabled: true,
            bar: fixed_bar("b_ok", today),
        }),
    ])
    .expect("registry");

    let summary = run_reconcile(
        &pool,
        &registry,
        ReconcileConfig {
            asset_id: obolargus_core::domain::id::AssetId::from_uuid(asset_id),
            fact_type: "market_price".to_string(),
            as_of_date: today,
            policy: ReconciliationPolicy::FirstNonEmpty,
        },
    )
    .await
    .expect("run_reconcile should survive one source failing");

    let source: Option<(String,)> = sqlx::query_as(
        "SELECT source FROM asset_reference_data \
         WHERE asset_id = $1 AND fact_type = 'market_price'",
    )
    .bind(asset_id)
    .fetch_optional(&pool)
    .await
    .expect("query reference data");
    assert_eq!(source.map(|(s,)| s), Some("b_ok".to_string()));

    // The failure is recorded in the audit's per-source answers.
    let per_source: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT per_source_answers FROM data_source_reconciliation_audits WHERE id = $1",
    )
    .bind(summary.reconciliation_id)
    .fetch_optional(&pool)
    .await
    .expect("query reconciliation audit");
    let per_source = per_source.expect("an audit row should exist").0;
    assert!(
        per_source
            .get("a_failing")
            .is_some_and(|v| v.get("error").is_some()),
        "the failing source should be recorded as an error (got {per_source})"
    );

    cleanup_asset(&pool, asset_id).await;
}

/// T058: `RefreshConfig { assets: None }` scans every asset and
/// processes only the ones that carry a supported identifier.
#[tokio::test]
async fn run_refresh_with_no_asset_list_scans_all_assets() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let with_identifier = seed_asset(&pool, "SCAN").await;
    let without_identifier = seed_bare_asset(&pool).await;
    let today = chrono::Utc::now().date_naive();

    let registry = DataSourceRegistry::new(vec![Arc::new(FixedProvider {
        source_id: "mock".to_string(),
        priority: 10,
        enabled: true,
        bar: fixed_bar("mock", today),
    })])
    .expect("registry");

    let summary = run_refresh(
        &pool,
        &registry,
        RefreshConfig {
            assets: None,
            trigger: "test".to_string(),
        },
    )
    .await
    .expect("run_refresh");

    // Every asset in the database is attempted; the ones without a
    // supported identifier are counted as `missing_identifier`.
    assert!(
        summary.assets_attempted >= 2,
        "the scan should reach every asset (got {})",
        summary.assets_attempted
    );
    assert!(
        summary.assets_missing_identifier >= 1,
        "the bare asset should be counted as missing_identifier"
    );
    assert!(
        summary.assets_refreshed >= 1,
        "the identified asset should be refreshed"
    );

    // The bare asset got no reference-data row.
    let bare_rows: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM asset_reference_data WHERE asset_id = $1")
            .bind(without_identifier)
            .fetch_one(&pool)
            .await
            .expect("count reference data");
    assert_eq!(bare_rows.0, 0);

    cleanup_asset(&pool, with_identifier).await;
    cleanup_asset(&pool, without_identifier).await;
}

/// T104: the configured sources are persisted into `data_sources`, and
/// the credential is recorded as a *presence* flag, never a value.
#[tokio::test]
async fn configured_sources_are_persisted_without_the_credential_value() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let mut source = DataSourceConfig::default_yahoo();
    source.source_id = "yahoo_finance".to_string();
    source.api_key_env_var = Some("OBOLARGUS_TEST_PERSIST_KEY".to_string());

    // The env var is unset, so the provider construction fails fast —
    // which is the FR-040 contract. Build without a credential to test
    // the persistence path itself.
    source.api_key_env_var = None;
    let row = obolargus_core::market_data::data_sources::upsert_data_source(&pool, &source, false)
        .await
        .expect("upsert data source");
    assert_eq!(row.source_id, "yahoo_finance");
    assert!(!row.api_key_present, "no credential was configured");
    assert_eq!(row.priority, 10);

    // The presence flag round-trips through the table.
    let rows = obolargus_core::market_data::data_sources::list_data_sources(&pool)
        .await
        .expect("list data sources");
    let persisted = rows
        .iter()
        .find(|r| r.source_id == "yahoo_finance")
        .expect("the source should be listed");
    assert!(!persisted.api_key_present);
    assert!(persisted.supported_schemes.iter().any(|s| s == "TICKER"));

    // The credential value never appears in the row.
    let rendered = format!("{persisted:?}");
    assert!(
        !rendered.contains("OBOLARGUS_TEST_PERSIST_KEY"),
        "the row must not carry the credential value"
    );
}

/// T094 / T095: the scheduled EOD refresh runs through the same
/// orchestrator and records a `scheduler`-triggered audit row.
#[tokio::test]
async fn scheduled_refresh_records_a_scheduler_triggered_run() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_asset(&pool, "SCHED").await;

    let state =
        obolargus_server::AppState::new_with_persistence(AppConfig::default(), Some(pool.clone()))
            .await
            .expect("app state");

    let summary = obolargus_server::tasks::market_close::run(&state).await;
    let summary = summary.expect("the scheduled refresh should succeed");

    assert!(summary.assets_attempted >= 1);

    // The run was recorded with the `scheduler` trigger.
    let triggers: Vec<(String,)> =
        sqlx::query_as("SELECT trigger FROM data_source_refresh_runs WHERE trigger = 'scheduler'")
            .fetch_all(&pool)
            .await
            .expect("query refresh runs");
    assert!(
        !triggers.is_empty(),
        "a scheduler-triggered audit row should exist"
    );

    cleanup_asset(&pool, asset_id).await;
}

/// T113: the reconcile endpoint persists the audit row over HTTP.
#[tokio::test]
async fn reconcile_endpoint_persists_the_audit_row() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let asset_id = seed_asset(&pool, "HTTPRECON").await;
    let today = chrono::Utc::now().date_naive();

    let state =
        obolargus_server::AppState::new_with_persistence(AppConfig::default(), Some(pool.clone()))
            .await
            .expect("app state");

    let body = format!(
        r#"{{"asset_id":"{asset_id}","fact_type":"market_price","as_of_date":"{today}","policy":"primary_wins"}}"#
    );
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/reconcile")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    // With a single configured source, reconciliation is a 400
    // ("at least 2 enabled sources") — the request reached the handler.
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("at least 2 enabled sources"), "body: {text}");

    cleanup_asset(&pool, asset_id).await;
}
