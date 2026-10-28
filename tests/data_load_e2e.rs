//! Database-backed history tests for the Data Load page.
//!
//! Skips as a passing no-op when no test database is configured, matching the
//! repository's integration-test convention.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::NaiveDate;
use http_body_util::BodyExt;
use tower::ServiceExt;
use uuid::Uuid;

use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

use obolargus_core::market_data::audit::{
    LoadRunContext, finish_load_run, start_load_run_with_context,
};
use obolargus_core::market_data::config::DataSourceConfig;
use obolargus_core::market_data::data_sources::upsert_data_source;
use obolargus_core::market_data::history::{
    HistoryFilter, get_history_detail, list_history, range_progress,
};
use obolargus_core::market_data::registry::DataSourceRegistry;
use obolargus_core::market_data::source_state::set_source_enabled;

fn test_database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
}

async fn migrated_pool() -> Option<sqlx::PgPool> {
    let url = test_database_url()?;
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

#[tokio::test]
async fn history_lists_runs_and_source_changes() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let date = NaiveDate::from_ymd_opt(2026, 10, 1).expect("date");
    let context = LoadRunContext::market_data(Some(date));
    let run = start_load_run_with_context(&pool, "test", None, &context)
        .await
        .expect("start run");
    let empty = serde_json::json!([]);
    finish_load_run(
        &pool,
        run.id,
        0,
        0,
        &Default::default(),
        &serde_json::json!({}),
        &serde_json::json!({}),
        &empty,
    )
    .await
    .expect("finish run");

    let change_id: Uuid = sqlx::query_scalar(
        "INSERT INTO data_source_state_changes
            (source_id, before_enabled, after_enabled, actor, reason)
         VALUES ('history_test_source', true, false, 'test', 'validation')
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .expect("insert change");

    let page = list_history(
        &pool,
        &HistoryFilter {
            limit: 50,
            ..Default::default()
        },
        7200,
    )
    .await
    .expect("list history");
    assert!(page.total >= 2);
    assert!(page.entries.iter().any(|entry| entry.entry_id == run.id));
    assert!(page.entries.iter().any(|entry| entry.entry_id == change_id));

    let run_detail = get_history_detail(&pool, run.id, 7200)
        .await
        .expect("run detail");
    assert!(run_detail.load.is_some());
    assert_eq!(run_detail.load.expect("load").report.as_of_date, Some(date));

    let change_detail = get_history_detail(&pool, change_id, 7200)
        .await
        .expect("change detail");
    assert_eq!(
        change_detail
            .source_change
            .expect("source change")
            .reason
            .as_deref(),
        Some("validation")
    );

    let progress = range_progress(&pool, Uuid::new_v4())
        .await
        .expect("progress");
    assert_eq!(progress.status, "starting");

    let _ = sqlx::query("DELETE FROM reference_data_load_runs WHERE id = $1")
        .bind(run.id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM data_source_state_changes WHERE id = $1")
        .bind(change_id)
        .execute(&pool)
        .await;
}

#[tokio::test]
async fn source_toggle_persists_and_audits() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let mut source = DataSourceConfig::default_yahoo();
    source.source_id = "e2e_toggle".to_string();
    source.display_name = "E2E Toggle".to_string();
    let _inserted = upsert_data_source(&pool, &source, false)
        .await
        .expect("upsert source");
    sqlx::query("UPDATE data_sources SET enabled = TRUE WHERE source_id = $1")
        .bind(&source.source_id)
        .execute(&pool)
        .await
        .expect("enable source");
    let registry = DataSourceRegistry::new(vec![std::sync::Arc::new(RangeSource {
        source_id: "e2e_toggle".to_string(),
    })])
    .expect("registry");
    // Seed the runtime projection from the persisted row.
    registry.set_enabled(&source.source_id, true).expect("seed");

    let disabled = set_source_enabled(
        &pool,
        &registry,
        &source.source_id,
        false,
        Some("e2e"),
        Some("validation"),
    )
    .await
    .expect("disable");
    assert!(disabled.changed);
    assert!(!disabled.source.enabled);
    assert_eq!(
        disabled.change.expect("change").actor.as_deref(),
        Some("e2e")
    );

    let no_op = set_source_enabled(&pool, &registry, &source.source_id, false, None, None)
        .await
        .expect("no-op");
    assert!(!no_op.changed);
    assert!(no_op.change.is_none());

    let enabled = set_source_enabled(&pool, &registry, &source.source_id, true, None, None)
        .await
        .expect("enable");
    assert!(enabled.changed);
    assert!(enabled.source.enabled);

    let _ = sqlx::query("DELETE FROM data_source_state_changes WHERE source_id = $1")
        .bind(&source.source_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM data_sources WHERE source_id = $1")
        .bind(&source.source_id)
        .execute(&pool)
        .await;
}

struct RangeSource {
    source_id: String,
}

#[async_trait::async_trait]
impl obolargus_core::market_data::trait_::MarketDataProvider for RangeSource {
    fn source_id(&self) -> &str {
        &self.source_id
    }
    fn display_name(&self) -> &str {
        "Range Source"
    }
    fn supported_schemes(&self) -> &[String] {
        static SCHEMES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
        SCHEMES.get_or_init(|| vec!["TICKER".to_string(), "TICKER:*".to_string()])
    }
    fn enabled(&self) -> bool {
        true
    }
    fn priority(&self) -> i32 {
        10
    }
    async fn get_latest_bar(
        &self,
        request: obolargus_core::market_data::types::LatestBarRequest,
    ) -> Result<
        obolargus_core::market_data::types::LatestBar,
        obolargus_core::market_data::error::MarketDataProviderError,
    > {
        Ok(obolargus_core::market_data::types::LatestBar {
            bar: obolargus_core::market_data::types::PriceBar {
                as_of_date: request.as_of_now.date_naive(),
                open: rust_decimal::Decimal::new(100, 0),
                high: rust_decimal::Decimal::new(101, 0),
                low: rust_decimal::Decimal::new(99, 0),
                close: rust_decimal::Decimal::new(100, 0),
                adjclose: rust_decimal::Decimal::new(100, 0),
                volume: 1_000,
            },
            knowledge_date: request.as_of_now.date_naive(),
            source_id: self.source_id.clone(),
        })
    }
    async fn get_historical_range(
        &self,
        request: obolargus_core::market_data::types::HistoricalRangeRequest,
    ) -> Result<
        obolargus_core::market_data::types::HistoricalRange,
        obolargus_core::market_data::error::MarketDataProviderError,
    > {
        Ok(obolargus_core::market_data::types::HistoricalRange {
            bars: vec![obolargus_core::market_data::types::PriceBar {
                as_of_date: request.from,
                open: rust_decimal::Decimal::new(100, 0),
                high: rust_decimal::Decimal::new(101, 0),
                low: rust_decimal::Decimal::new(99, 0),
                close: rust_decimal::Decimal::new(100, 0),
                adjclose: rust_decimal::Decimal::new(100, 0),
                volume: 1_000,
            }],
            knowledge_date: request.from,
            source_id: self.source_id.clone(),
        })
    }
    async fn lookup_ticker(
        &self,
        _request: obolargus_core::market_data::types::TickerLookupRequest,
    ) -> Result<
        obolargus_core::market_data::types::TickerLookupResult,
        obolargus_core::market_data::error::MarketDataProviderError,
    > {
        Err(obolargus_core::market_data::error::MarketDataProviderError::Unsupported("stub".into()))
    }
}

#[tokio::test]
async fn http_range_load_groups_rows_and_reports_progress() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let ticker = format!("M{}", &Uuid::new_v4().simple().to_string()[..8]);
    let asset = obolargus_core::domain::asset::create::create_asset(
        &pool,
        obolargus_core::domain::asset::entity::NewAsset {
            asset_type: "Equity".to_string(),
            display_name: format!("Range {ticker}"),
            currency: "USD".to_string(),
            backed_by_portfolio_id: None,
            issuer_id: None,
        },
    )
    .await
    .expect("asset");
    obolargus_core::domain::asset_identifier::create::create_asset_identifier(
        &pool,
        obolargus_core::domain::asset_identifier::entity::NewAssetIdentifier {
            asset_id: obolargus_core::domain::id::AssetId(asset.id),
            scheme: "TICKER:XNAS".to_string(),
            value: ticker.clone(),
        },
    )
    .await
    .expect("identifier");

    let registry = std::sync::Arc::new(
        obolargus_core::market_data::registry::DataSourceRegistry::new(vec![std::sync::Arc::new(
            RangeSource {
                source_id: "yahoo_finance".to_string(),
            },
        )])
        .expect("registry"),
    );
    let state = AppState {
        config: AppConfig::default(),
        pool: Some(pool.clone()),
        market_data: registry,
    };
    let group_id = Uuid::new_v4();
    let body = format!(
        r#"{{"identifiers":["TICKER:XNAS:{ticker}"],"from":"2026-10-01","to":"2026-10-02","concurrency":2,"client_group_id":"{group_id}","data_domain":"market_data"}}"#
    );
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/load-range")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(json["range_group_id"], group_id.to_string());
    assert_eq!(json["dates"].as_array().expect("dates").len(), 2);

    let rows: Vec<(String, i32)> = sqlx::query_as(
        "SELECT data_domain, range_concurrency FROM reference_data_load_runs WHERE range_group_id = $1",
    )
    .bind(group_id)
    .fetch_all(&pool)
    .await
    .expect("rows");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.0 == "market_data" && row.1 == 2));

    let progress = obolargus_core::market_data::history::range_progress(&pool, group_id)
        .await
        .expect("progress");
    assert_eq!(progress.status, "complete");
    assert_eq!(progress.dates_completed, 2);
}

#[tokio::test]
async fn http_single_date_load_reports_stored_provenance() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let ticker = format!("N{}", &Uuid::new_v4().simple().to_string()[..8]);
    let asset = obolargus_core::domain::asset::create::create_asset(
        &pool,
        obolargus_core::domain::asset::entity::NewAsset {
            asset_type: "Equity".to_string(),
            display_name: format!("Single {ticker}"),
            currency: "USD".to_string(),
            backed_by_portfolio_id: None,
            issuer_id: None,
        },
    )
    .await
    .expect("asset");
    obolargus_core::domain::asset_identifier::create::create_asset_identifier(
        &pool,
        obolargus_core::domain::asset_identifier::entity::NewAssetIdentifier {
            asset_id: obolargus_core::domain::id::AssetId(asset.id),
            scheme: "TICKER:XNAS".to_string(),
            value: ticker.clone(),
        },
    )
    .await
    .expect("identifier");

    let registry = std::sync::Arc::new(
        obolargus_core::market_data::registry::DataSourceRegistry::new(vec![std::sync::Arc::new(
            RangeSource {
                source_id: "yahoo_finance".to_string(),
            },
        )])
        .expect("registry"),
    );
    let state = AppState {
        config: AppConfig::default(),
        pool: Some(pool),
        market_data: registry,
    };
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/market-data/load")
                .header("content-type", "application/json")
                .body(Body::from(
                    format!(r#"{{"identifiers":["TICKER:XNAS:{ticker}"],"date":"2026-10-01","data_domain":"market_data"}}"#, ticker = ticker),
                ))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(json["data_domain"], "market_data");
    assert_eq!(json["as_of_date"], "2026-10-01");
    assert_eq!(json["identifiers"][0]["outcome"], "loaded");
    assert_eq!(
        json["identifiers"][0]["stored"]["source_id"],
        "yahoo_finance"
    );
}

fn state_with_pool(pool: sqlx::PgPool, source_id: &str) -> AppState {
    let registry = std::sync::Arc::new(
        obolargus_core::market_data::registry::DataSourceRegistry::new(vec![std::sync::Arc::new(
            RangeSource {
                source_id: source_id.to_string(),
            },
        )])
        .expect("registry"),
    );
    AppState {
        config: AppConfig::default(),
        pool: Some(pool),
        market_data: registry,
    }
}

async fn call(
    state: AppState,
    method: &str,
    uri: &str,
    body: Option<&str>,
) -> (StatusCode, String) {
    let mut builder = Request::builder().method(method).uri(uri);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let response = router(state)
        .oneshot(
            builder
                .body(Body::from(body.unwrap_or_default().to_string()))
                .unwrap(),
        )
        .await
        .expect("response");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn http_data_load_read_and_control_surface() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let mut source = DataSourceConfig::default_yahoo();
    source.source_id = "e2e_read".to_string();
    source.display_name = "E2E Read".to_string();
    upsert_data_source(&pool, &source, false)
        .await
        .expect("source");
    sqlx::query("UPDATE data_sources SET enabled = TRUE WHERE source_id = $1")
        .bind(&source.source_id)
        .execute(&pool)
        .await
        .expect("enable source");
    let date = NaiveDate::from_ymd_opt(2026, 10, 1).expect("date");
    let run = start_load_run_with_context(
        &pool,
        "test",
        None,
        &LoadRunContext::market_data(Some(date)),
    )
    .await
    .expect("run");
    finish_load_run(
        &pool,
        run.id,
        0,
        0,
        &Default::default(),
        &serde_json::json!({}),
        &serde_json::json!({}),
        &serde_json::json!([]),
    )
    .await
    .expect("finish");
    let change_id: Uuid = sqlx::query_scalar(
        "INSERT INTO data_source_state_changes
            (source_id, before_enabled, after_enabled, reason)
         VALUES ('e2e_read', true, false, 'e2e')
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .expect("change");

    let (status, body) = call(
        state_with_pool(pool.clone(), "e2e_read"),
        "GET",
        "/api/data-load/domains",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("holdings"));

    let (status, body) = call(
        state_with_pool(pool.clone(), "e2e_read"),
        "GET",
        "/api/data-load/sources?domain=market_data",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("e2e_read"));

    let (status, body) = call(
        state_with_pool(pool.clone(), "e2e_read"),
        "PATCH",
        "/api/data-load/sources/e2e_read",
        Some(r#"{"enabled":false,"reason":"e2e"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"changed\":true"));
    assert!(body.contains("\"enabled\":false"));

    let (status, body) = call(
        state_with_pool(pool.clone(), "e2e_read"),
        "GET",
        "/api/data-load/history?limit=10",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let page: serde_json::Value = serde_json::from_str(&body).expect("page");
    assert!(page["total"].as_i64().unwrap_or(0) >= 2);

    let (status, _) = call(
        state_with_pool(pool.clone(), "e2e_read"),
        "GET",
        &format!("/api/data-load/history/{}", run.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = call(
        state_with_pool(pool.clone(), "e2e_read"),
        "GET",
        &format!("/api/data-load/history/{change_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("e2e"));

    let (status, body) = call(
        state_with_pool(pool.clone(), "e2e_read"),
        "GET",
        "/api/data-load/history/groups/00000000-0000-4000-8000-000000000001/progress",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("starting"));

    for uri in [
        "/api/data-load/history?from=not-a-date",
        "/api/data-load/history?status=bogus",
        "/api/data-load/history?entry_type=bogus",
        "/api/data-load/history?domain=bogus",
        "/api/data-load/history?limit=0",
        "/api/data-load/history?from=2026-10-02&to=2026-10-01",
    ] {
        let (status, body) =
            call(state_with_pool(pool.clone(), "e2e_read"), "GET", uri, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert!(body.contains("validation_error"));
    }
}
