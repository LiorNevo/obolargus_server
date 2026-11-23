//! Shared fixtures for the spec 034 data-load integration tests.
//!
//! Database-backed tests self-skip when no test database is configured,
//! matching the repository convention.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::registry::DataSourceRegistry;
use obolargus_core::market_data::trait_::MarketDataProvider;
use obolargus_core::market_data::types::{
    HistoricalRange, HistoricalRangeRequest, LatestBar, LatestBarRequest, PriceBar,
    TickerLookupRequest, TickerLookupResult,
};
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

/// The owner id used by the dev auth state.
pub const DEV_OWNER: &str = "dev-user";

/// Returns the configured test database URL, if any.
pub fn test_database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
}

/// Connects to the test database and applies pending migrations.
pub async fn migrated_pool() -> Option<sqlx::PgPool> {
    let url = test_database_url()?;
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
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

/// A test configuration with authentication disabled on loopback.
pub fn test_config() -> AppConfig {
    AppConfig {
        auth_enabled: false,
        run_scheduled_tasks: false,
        host: "127.0.0.1".to_string(),
        ..AppConfig::default()
    }
}

/// Builds an application state for tests.
pub fn state_with(
    pool: Option<sqlx::PgPool>,
    provider: Option<Arc<dyn MarketDataProvider + Send + Sync>>,
) -> AppState {
    let config = test_config();
    let market_data = match provider {
        Some(provider) => Arc::new(DataSourceRegistry::new(vec![provider]).expect("registry")),
        None => obolargus_server::build_market_data_registry(&config).expect("registry"),
    };
    AppState {
        config: config.clone(),
        pool,
        market_data,
        automation: Arc::new(obolargus_server::automation::state::AutomationState::new()),
        morpheus: Arc::new(
            obolargus_server::morpheus::state::MorpheusState::new(&config).expect("morpheus"),
        ),
        auth: Arc::new(obolargus_server::auth::AuthState::dev()),
    }
}

/// A response captured from the router.
pub struct TestResponse {
    /// HTTP status.
    pub status: StatusCode,
    /// Response headers.
    pub headers: HeaderMap,
    /// Parsed JSON body (or `Null` for an empty/non-JSON body).
    pub body: Value,
}

impl TestResponse {
    /// The structured error code, if the body carries one.
    pub fn error_code(&self) -> Option<&str> {
        self.body
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(Value::as_str)
    }

    /// The structured error details, if any.
    pub fn error_details(&self) -> Option<&Value> {
        self.body
            .get("error")
            .and_then(|error| error.get("details"))
    }

    /// The `Retry-After` header parsed as seconds.
    pub fn retry_after(&self) -> Option<u64> {
        self.headers
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
    }
}

/// Sends one request through the full router.
pub async fn send(state: AppState, method: &str, uri: &str, body: Option<Value>) -> TestResponse {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(body) => {
            builder = builder.header("content-type", "application/json");
            Body::from(body.to_string())
        }
        None => Body::empty(),
    };
    let response = router(state)
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    TestResponse {
        status,
        headers,
        body,
    }
}

/// Builds a range request body.
pub fn range_body(
    identifiers: Vec<Value>,
    from: &str,
    to: &str,
    concurrency: usize,
    client_group_id: Uuid,
) -> Value {
    serde_json::json!({
        "identifiers": identifiers,
        "from": from,
        "to": to,
        "concurrency": concurrency,
        "create_missing_assets": false,
        "data_domain": "market_data",
        "client_group_id": client_group_id.to_string(),
    })
}

/// One identifier in the object form.
pub fn identifier(value: &str) -> Value {
    serde_json::json!({ "type": "TICKER", "value": value })
}

/// Inserts one range group row.
pub struct GroupFixture {
    /// Group id.
    pub id: Uuid,
    /// Owner id.
    pub owner_id: String,
    /// Client idempotency key.
    pub client_group_id: Uuid,
    /// Inclusive first date.
    pub from: chrono::NaiveDate,
    /// Inclusive last date.
    pub to: chrono::NaiveDate,
    /// Status.
    pub status: String,
    /// Requested inclusive day count.
    pub dates_requested: i32,
    /// Optional stop marker.
    pub cancel_requested_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Start time.
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// Optional terminal time.
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl GroupFixture {
    /// A running group owned by the dev actor.
    pub fn running(dates_requested: i32) -> Self {
        let from = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).expect("date");
        let to = from + chrono::Duration::days(i64::from(dates_requested) - 1);
        Self {
            id: Uuid::new_v4(),
            owner_id: DEV_OWNER.to_string(),
            client_group_id: Uuid::new_v4(),
            from,
            to,
            status: "running".to_string(),
            dates_requested,
            cancel_requested_at: None,
            started_at: chrono::Utc::now(),
            ended_at: None,
        }
    }

    /// Sets the owner.
    pub fn owner(mut self, owner: &str) -> Self {
        self.owner_id = owner.to_string();
        self
    }

    /// Sets the status.
    pub fn status(mut self, status: &str) -> Self {
        self.status = status.to_string();
        if status != "running" {
            self.ended_at = Some(chrono::Utc::now());
        }
        self
    }

    /// Sets the start time.
    pub fn started_at(mut self, started_at: chrono::DateTime<chrono::Utc>) -> Self {
        self.started_at = started_at;
        self
    }

    /// Persists the group row.
    pub async fn insert(&self, pool: &sqlx::PgPool) {
        sqlx::query(
            "INSERT INTO load_range_groups (
                id, owner_id, client_group_id, from_date, to_date, concurrency,
                status, dates_requested, cancel_requested_at, started_at, ended_at
             ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9, $10)",
        )
        .bind(self.id)
        .bind(&self.owner_id)
        .bind(self.client_group_id)
        .bind(self.from)
        .bind(self.to)
        .bind(&self.status)
        .bind(self.dates_requested)
        .bind(self.cancel_requested_at)
        .bind(self.started_at)
        .bind(self.ended_at)
        .execute(pool)
        .await
        .expect("insert load_range_group");
    }
}

/// Inserts one completed or open child run for a group.
pub async fn insert_child(
    pool: &sqlx::PgPool,
    group: &GroupFixture,
    date: chrono::NaiveDate,
    completed: bool,
    failed: bool,
) -> Uuid {
    let context = obolargus_core::market_data::audit::LoadRunContext::range(
        obolargus_core::market_data::data_domain::DataDomain::MarketData,
        date,
        group.id,
        group.from,
        group.to,
        1,
    )
    .with_owner(&group.owner_id);
    let run = obolargus_core::market_data::audit::start_load_run_with_context(
        pool, "test", None, &context,
    )
    .await
    .expect("start child run");
    if completed {
        let mut counters = obolargus_core::market_data::load::DataSourceLoadCounters::default();
        if failed {
            counters.rejected = 1;
        } else {
            counters.loaded = 1;
        }
        obolargus_core::market_data::audit::finish_load_run(
            pool,
            run.id,
            1,
            0,
            &counters,
            &serde_json::json!({}),
            &serde_json::json!({}),
            &serde_json::json!([]),
        )
        .await
        .expect("finish child run");
    }
    run.id
}

/// Removes a group and its children.
pub async fn cleanup_group(pool: &sqlx::PgPool, group_id: Uuid) {
    let _ = sqlx::query("DELETE FROM reference_data_load_runs WHERE range_group_id = $1")
        .bind(group_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM load_range_groups WHERE id = $1")
        .bind(group_id)
        .execute(pool)
        .await;
}

/// Reads one group row.
pub async fn read_group(
    pool: &sqlx::PgPool,
    group_id: Uuid,
) -> Option<(
    String,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<chrono::DateTime<chrono::Utc>>,
)> {
    sqlx::query_as(
        "SELECT status, cancel_requested_at, ended_at FROM load_range_groups WHERE id = $1",
    )
    .bind(group_id)
    .fetch_optional(pool)
    .await
    .expect("read group")
}

/// Seeds one asset and its identifier.
pub async fn seed_asset(pool: &sqlx::PgPool, scheme: &str, value: &str) -> Uuid {
    let asset_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO assets (id, asset_type, display_name, currency)
         VALUES ($1, 'equity', $2, 'USD')",
    )
    .bind(asset_id)
    .bind(format!("Test {value}"))
    .execute(pool)
    .await
    .expect("insert asset");
    sqlx::query(
        "INSERT INTO asset_identifiers (id, asset_id, scheme, value)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(Uuid::new_v4())
    .bind(asset_id)
    .bind(scheme)
    .bind(value)
    .execute(pool)
    .await
    .expect("insert asset identifier");
    asset_id
}

/// Removes a seeded asset and its facts.
pub async fn cleanup_asset(pool: &sqlx::PgPool, asset_id: Uuid) {
    let _ = sqlx::query("DELETE FROM market_observations WHERE asset_id = $1")
        .bind(asset_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM temporal_facts WHERE asset_id = $1")
        .bind(asset_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM asset_identifiers WHERE asset_id = $1")
        .bind(asset_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM assets WHERE id = $1")
        .bind(asset_id)
        .execute(pool)
        .await;
}

/// A provider that sleeps and then answers one bar for the requested date.
pub struct SlowProvider {
    /// Delay before answering.
    pub delay: Duration,
}

#[async_trait::async_trait]
impl MarketDataProvider for SlowProvider {
    fn source_id(&self) -> &str {
        "slow_source"
    }
    fn display_name(&self) -> &str {
        "Slow Source"
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
        Err(MarketDataProviderError::Unsupported("unused".into()))
    }
    async fn get_historical_range(
        &self,
        request: HistoricalRangeRequest,
    ) -> Result<HistoricalRange, MarketDataProviderError> {
        tokio::time::sleep(self.delay).await;
        Ok(HistoricalRange {
            bars: vec![PriceBar {
                as_of_date: request.from,
                open: rust_decimal::Decimal::from(100),
                high: rust_decimal::Decimal::from(101),
                low: rust_decimal::Decimal::from(99),
                close: rust_decimal::Decimal::from(100),
                adjclose: rust_decimal::Decimal::from(100),
                volume: 1_000,
            }],
            knowledge_date: request.from,
            source_id: "slow_source".into(),
        })
    }
    async fn lookup_ticker(
        &self,
        _request: TickerLookupRequest,
    ) -> Result<TickerLookupResult, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported("unused".into()))
    }
}

/// Polls a group's progress until it is terminal or the deadline expires.
pub async fn wait_terminal(
    pool: &sqlx::PgPool,
    group_id: Uuid,
    timeout: Duration,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM load_range_groups WHERE id = $1")
                .bind(group_id)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();
        if let Some(ref status) = status
            && status != "running"
        {
            return Some(status.clone());
        }
        if tokio::time::Instant::now() >= deadline {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
