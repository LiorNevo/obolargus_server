//! HTTP handlers for the source-agnostic market data API.
//!
//! Implements the wire surface named in `specs/017-yahoo-data-source/spec.md`:
//! - `GET  /api/market-data/sources`    — enumerate configured sources.
//! - `POST /api/market-data/refresh`    — on-demand refresh for one asset.
//! - `GET  /api/market-data/lookup`     — ticker / company-name lookup.
//! - `POST /api/market-data/reconcile`  — cross-source reconciliation.
//!
//! Plus the batch load surface of `specs/018-load-reference-data/spec.md`:
//! - `POST /api/market-data/load`       — load reference data by identifier
//!   and date, returning the execution report.

#![forbid(unsafe_code)]
// The structured error carries a details JSON value and a message; boxing it
// would only move an error-path allocation into every handler signature.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use axum::Json;
use axum::extract::{Extension, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use obolargus_core::domain::asset::read::get_asset;
use obolargus_core::domain::error::DomainError;
use obolargus_core::domain::id::{AssetId, DomainId};
use obolargus_core::market_data::audit::{
    GroupRegistration, LoadRunContext, insert_load_range_group,
};
use obolargus_core::market_data::data_domain::DataDomain;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::history::finalize_group_from_dates;
use obolargus_core::market_data::history_load::{
    HistoryLoadConfig, HistoryLoadReport, run_asset_history_load,
};
use obolargus_core::market_data::load::{
    IdentifierLoadConfig, IdentifierLoadReport, run_identifier_load_with_snapshot,
};
use obolargus_core::market_data::load_range::{
    IdentifierRangeLoadConfig, IdentifierRangeLoadReport,
    run_identifier_range_load_with_group_cancellable_owned,
};
use obolargus_core::market_data::reconcile::{
    ReconcileConfig, ReconciliationPolicy, run_reconcile,
};
use obolargus_core::market_data::refresh::{RefreshConfig, run_refresh};
use obolargus_core::market_data::registry::SourceDescriptor;
use obolargus_core::market_data::types::{TickerLookupRequest, TypedIdentifier};

use crate::AppState;
use crate::auth::AuthUser;

/// Structured error envelope shared by the load endpoints.
///
/// Implements FR-011 / FR-012 / FR-015 of
/// `specs/034-production-hardening/spec.md`.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct LoadErrorEnvelope {
    /// Error details.
    pub error: LoadErrorBody,
}

/// One structured load error.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct LoadErrorBody {
    /// Stable machine-readable code.
    pub code: String,
    /// Operator-facing message naming the field and values.
    pub message: String,
    /// Machine-readable details (`field`, `limit`, `actual`, `group_id`).
    pub details: Option<serde_json::Value>,
}

/// A load-request rejection.
#[derive(Debug, Clone)]
pub struct LoadApiError {
    /// HTTP status.
    pub status: StatusCode,
    /// Stable error code.
    pub code: &'static str,
    /// Operator-facing message.
    pub message: String,
    /// Optional machine-readable details.
    pub details: Option<serde_json::Value>,
    /// Seconds until the caller may retry (429 only).
    pub retry_after: Option<u64>,
}

impl LoadApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            details: None,
            retry_after: None,
        }
    }

    fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    fn validation(field: &str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "validation_error", message)
            .with_details(serde_json::json!({ "field": field }))
    }

    fn limit(field: &str, limit: i64, actual: i64, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "limit_exceeded", message)
            .with_details(serde_json::json!({ "field": field, "limit": limit, "actual": actual }))
    }

    fn conflict(group_id: Uuid) -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "group_id_conflict",
            "the client group id is already in use",
        )
        .with_details(serde_json::json!({ "group_id": group_id.to_string() }))
    }

    fn rate_limited(retry_after: u64) -> Self {
        let mut error = Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "too many load starts; wait before starting another run",
        );
        error.retry_after = Some(retry_after.max(1));
        error
    }
}

impl IntoResponse for LoadApiError {
    fn into_response(self) -> Response {
        let envelope = LoadErrorEnvelope {
            error: LoadErrorBody {
                code: self.code.to_string(),
                message: self.message,
                details: self.details,
            },
        };
        let mut response = (self.status, Json(envelope)).into_response();
        if let Some(retry_after) = self.retry_after
            && let Ok(value) = HeaderValue::from_str(&retry_after.to_string())
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        response
    }
}

type LoadApiResult<T> = Result<Json<T>, LoadApiError>;

/// One fixed-window load-start bucket.
struct StartWindow {
    started: Instant,
    count: u32,
}

/// Process-local fixed-window load-start limiter keyed by owner.
///
/// Implements FR-012 of `specs/034-production-hardening/spec.md`.
fn start_limiter() -> &'static Mutex<HashMap<String, StartWindow>> {
    static LIMITER: OnceLock<Mutex<HashMap<String, StartWindow>>> = OnceLock::new();
    LIMITER.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Consumes one start token, or returns the `Retry-After` seconds.
fn check_start_limit(owner_id: &str, limit: u32) -> Result<(), u64> {
    if limit == 0 {
        return Ok(());
    }
    let mut buckets = start_limiter()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let now = Instant::now();
    buckets.retain(|_, window| now.duration_since(window.started).as_secs() < 60);
    let window = buckets.entry(owner_id.to_string()).or_insert(StartWindow {
        started: now,
        count: 0,
    });
    let elapsed = now.duration_since(window.started).as_secs();
    if elapsed >= 60 {
        *window = StartWindow {
            started: now,
            count: 0,
        };
    }
    if window.count >= limit {
        return Err(60u64.saturating_sub(elapsed).max(1));
    }
    window.count += 1;
    Ok(())
}

/// `GET /api/market-data/sources` — return every configured source.
#[utoipa::path(
    get,
    path = "/api/market-data/sources",
    tag = "market-data",
    summary = "List configured market-data sources",
    description = "Returns every configured source with its enabled flag, priority and supported identifier schemes.",
    responses(
        (status = 200, description = "The configured market-data sources", body = Vec<SourceDescriptor>)
    )
)]
pub async fn list_sources(State(state): State<AppState>) -> Json<Vec<SourceDescriptor>> {
    Json(state.market_data.enumerate_sources())
}

/// `POST /api/market-data/refresh` — on-demand refresh for one asset.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct RefreshBody {
    /// Asset id (UUID) to refresh.
    pub asset_id: Uuid,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct RefreshResponse {
    /// Run id of the persisted audit row.
    pub run_id: Uuid,
    /// Total assets attempted.
    pub assets_attempted: i64,
    /// Assets refreshed.
    pub assets_refreshed: i64,
    /// No-op assets.
    pub assets_no_op: i64,
    /// Missing-identifier assets.
    pub assets_missing_identifier: i64,
    /// Source-unavailable assets.
    pub assets_source_unavailable: i64,
    /// Latest completed session resolved for the requested asset.
    pub latest_completed_session: Option<chrono::NaiveDate>,
}

#[utoipa::path(
    post,
    path = "/api/market-data/refresh",
    tag = "market-data",
    summary = "Refresh one asset on demand",
    description = "Fetches and stores the latest reference data for one asset, returning the run summary.",
    request_body = RefreshBody,
    responses(
        (status = 200, description = "The refresh run summary", body = RefreshResponse),
        (status = 400, description = "Invalid request", body = String, content_type = "text/plain"),
        (status = 401, description = "The provider rejected the credentials", body = String, content_type = "text/plain"),
        (status = 415, description = "Missing or unsupported JSON content type", body = String, content_type = "text/plain"),
        (status = 422, description = "The JSON body does not match the required shape", body = String, content_type = "text/plain"),
        (status = 500, description = "Unexpected server error", body = String, content_type = "text/plain"),
        (status = 502, description = "Provider unavailable, rate-limited or infrastructure failure", body = String, content_type = "text/plain"),
        (status = 503, description = "No database pool is configured", body = String, content_type = "text/plain")
    )
)]
pub async fn refresh(
    State(state): State<AppState>,
    Json(body): Json<RefreshBody>,
) -> Result<Json<RefreshResponse>, (StatusCode, String)> {
    let pool = state.pool.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "no database pool configured".to_string(),
        )
    })?;
    let asset_id = obolargus_core::domain::id::AssetId::from_uuid(body.asset_id);
    let summary = run_refresh(
        pool,
        &state.market_data,
        RefreshConfig {
            assets: Some(vec![asset_id]),
            trigger: "api".to_string(),
        },
    )
    .await
    .map_err(|e| typed_error_to_http(&e))?;
    Ok(Json(RefreshResponse {
        run_id: summary.run_id,
        assets_attempted: summary.assets_attempted,
        assets_refreshed: summary.assets_refreshed,
        assets_no_op: summary.assets_no_op,
        assets_missing_identifier: summary.assets_missing_identifier,
        assets_source_unavailable: summary.assets_source_unavailable,
        latest_completed_session: summary.latest_completed_session,
    }))
}

/// The `q` query parameter for `GET /api/market-data/lookup`.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct LookupQuery {
    /// The free-text query (ticker or company-name fragment).
    pub q: String,
}

/// One source's lookup result.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct LookupResponse {
    /// The hits this source returned.
    pub hits: Vec<obolargus_core::market_data::types::TickerLookupHit>,
    /// The source id that produced the hits.
    pub source_id: String,
}

/// `GET /api/market-data/lookup?q=…` — ticker / company-name lookup.
///
/// Aggregates the hits from every enabled source into one list, sorted by
/// relevance descending then symbol ascending (the tie-breaker). No asset
/// or identifier row is created — the look-up is read-only (FR-012).
#[utoipa::path(
    get,
    path = "/api/market-data/lookup",
    tag = "market-data",
    summary = "Look up tickers or company names",
    description = "Aggregates lookup hits from every enabled source, sorted by relevance then symbol. Read-only; creates no asset or identifier rows.",
    params(
        ("q" = String, Query, description = "Free-text ticker or company-name query; must be non-empty", example = "AAPL")
    ),
    responses(
        (status = 200, description = "Lookup hits grouped by answering source", body = Vec<LookupResponse>),
        (status = 400, description = "The query is missing or empty", body = String, content_type = "text/plain")
    )
)]
pub async fn lookup(
    State(state): State<AppState>,
    Query(query): Query<LookupQuery>,
) -> Result<Json<Vec<LookupResponse>>, (StatusCode, String)> {
    if query.q.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "q must be non-empty".to_string()));
    }
    let map = state
        .market_data
        .every_source_lookup_ticker(TickerLookupRequest {
            query: query.q.trim().to_string(),
        })
        .await;
    let mut out = Vec::with_capacity(map.len());
    for (source_id, outcome) in map {
        match outcome {
            Ok(result) => {
                let mut hits = result.hits;
                hits.sort_by(|a, b| {
                    b.relevance
                        .partial_cmp(&a.relevance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| a.symbol.cmp(&b.symbol))
                });
                out.push(LookupResponse {
                    hits,
                    source_id: result.source_id.clone(),
                });
            }
            Err(e) => {
                tracing::info!(source_id = %source_id, error = %e, "every-source: lookup error");
            }
        }
    }
    Ok(Json(out))
}

/// `POST /api/market-data/reconcile` — cross-source reconciliation (opt-in per FR-021).
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ReconcileBody {
    /// Asset id (UUID) to reconcile.
    pub asset_id: Uuid,
    /// Reference-data fact type to reconcile.
    pub fact_type: String,
    /// The as-of date, as an ISO date (`YYYY-MM-DD`).
    pub as_of_date: chrono::NaiveDate,
    /// Reconciliation policy: `primary_wins` or `first_non_empty`.
    pub policy: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ReconcileResponse {
    /// The persisted reconciliation id.
    pub reconciliation_id: Uuid,
    /// The canonical value selected by the policy.
    #[schema(value_type = Object)]
    pub canonical_value: serde_json::Value,
    /// Number of sources consulted.
    pub sources_tried: usize,
}

#[utoipa::path(
    post,
    path = "/api/market-data/reconcile",
    tag = "market-data",
    summary = "Reconcile reference data across sources",
    description = "Selects a canonical value for one asset/fact/date using the named policy. `majority` is deferred and returns a client error.",
    request_body = ReconcileBody,
    responses(
        (status = 200, description = "The reconciliation result", body = ReconcileResponse),
        (status = 400, description = "Invalid policy or request", body = String, content_type = "text/plain"),
        (status = 401, description = "The provider rejected the credentials", body = String, content_type = "text/plain"),
        (status = 415, description = "Missing or unsupported JSON content type", body = String, content_type = "text/plain"),
        (status = 422, description = "The JSON body does not match the required shape", body = String, content_type = "text/plain"),
        (status = 500, description = "Unexpected server error", body = String, content_type = "text/plain"),
        (status = 502, description = "Provider unavailable, rate-limited or infrastructure failure", body = String, content_type = "text/plain"),
        (status = 503, description = "No database pool is configured", body = String, content_type = "text/plain")
    )
)]
pub async fn reconcile(
    State(state): State<AppState>,
    Json(body): Json<ReconcileBody>,
) -> Result<Json<ReconcileResponse>, (StatusCode, String)> {
    // The policy is validated first: a malformed request is a client
    // error regardless of whether a database is reachable.
    let policy = match body.policy.as_str() {
        "primary_wins" => ReconciliationPolicy::PrimaryWins,
        "first_non_empty" => ReconciliationPolicy::FirstNonEmpty,
        "majority" => {
            return Err((
                StatusCode::BAD_REQUEST,
                "ReconciliationPolicy::Majority is deferred; use primary_wins or first_non_empty"
                    .to_string(),
            ));
        }
        other => {
            return Err((StatusCode::BAD_REQUEST, format!("unknown policy '{other}'")));
        }
    };
    let pool = state.pool.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "no database pool configured".to_string(),
        )
    })?;
    let asset_id = obolargus_core::domain::id::AssetId::from_uuid(body.asset_id);
    let summary = run_reconcile(
        pool,
        &state.market_data,
        ReconcileConfig {
            asset_id,
            fact_type: body.fact_type,
            as_of_date: body.as_of_date,
            policy,
        },
    )
    .await
    .map_err(|e| typed_error_to_http(&e))?;
    Ok(Json(ReconcileResponse {
        reconciliation_id: summary.reconciliation_id,
        canonical_value: summary.canonical_value,
        sources_tried: summary.per_source_answers.len(),
    }))
}

/// One identifier in a load request.
///
/// Accepts either form the contract allows: the explicit object carrying
/// the identifier's **type** and value, or the `TYPE:VALUE` string.
///
/// Implements FR-012 of `specs/018-load-reference-data/spec.md`.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum IdentifierInput {
    /// The `TYPE:VALUE` string form.
    Text(String),
    /// The explicit `{ "type": …, "value": … }` form.
    Pair {
        /// The identifier's type (the stored `scheme`).
        #[serde(rename = "type")]
        identifier_type: String,
        /// The identifier's value.
        value: String,
    },
}

impl IdentifierInput {
    /// Converts the input into a typed identifier.
    ///
    /// The string form is parsed at the **last** colon; the object form's
    /// `type` becomes the stored scheme.
    ///
    /// Implements FR-002 / FR-012 of `specs/018-load-reference-data/spec.md`.
    pub fn into_identifier(self) -> Result<TypedIdentifier, String> {
        match self {
            IdentifierInput::Text(raw) => {
                let trimmed = raw.trim();
                let Some((scheme, value)) = trimmed.rsplit_once(':') else {
                    return Err(format!(
                        "identifier '{trimmed}' must be '<type>:<value>' or {{type, value}}"
                    ));
                };
                let scheme = scheme.trim().to_uppercase();
                let value = value.trim();
                if scheme.is_empty() || value.is_empty() {
                    return Err(format!(
                        "identifier '{trimmed}' must carry a non-empty type and value"
                    ));
                }
                Ok(TypedIdentifier::new(scheme, value))
            }
            IdentifierInput::Pair {
                identifier_type,
                value,
            } => {
                let scheme = identifier_type.trim().to_uppercase();
                let value = value.trim();
                if scheme.is_empty() || value.is_empty() {
                    return Err("an identifier must carry a non-empty type and value".to_string());
                }
                Ok(TypedIdentifier::new(scheme, value))
            }
        }
    }
}

/// The request body of `POST /api/market-data/load`.
///
/// Implements FR-012 of `specs/018-load-reference-data/spec.md`.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct LoadBody {
    /// The identifiers to load.
    pub identifiers: Vec<IdentifierInput>,
    /// The single as-of date, as `YYYY-MM-DD`.
    ///
    /// Kept as a string so an unparseable date is reported as a
    /// caller-input validation error (HTTP 400) rather than as the
    /// extractor's own rejection.
    pub date: String,
    /// The preferred upstream source id (optional). It is consulted first
    /// for every identifier; the normal fail-over continues when it cannot
    /// answer.
    pub source: Option<String>,
    /// Create an asset when an identifier resolves to none (FR-014).
    #[serde(default)]
    pub create_missing_assets: bool,
    /// Data domain (defaults to `market_data`).
    #[serde(default)]
    pub data_domain: Option<String>,
}

/// Body of `POST /api/market-data/load-range`.
///
/// Implements FR-014 of `specs/018-load-reference-data/spec.md`.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct LoadRangeBody {
    /// The identifiers to load.
    pub identifiers: Vec<IdentifierInput>,
    /// First date to load, as `YYYY-MM-DD` (inclusive).
    pub from: String,
    /// Last date to load, as `YYYY-MM-DD` (inclusive).
    pub to: String,
    /// The size of the concurrency window (defaults to `4`).
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// The preferred upstream source id (optional).
    pub source: Option<String>,
    /// Create an asset when an identifier resolves to none.
    #[serde(default)]
    pub create_missing_assets: bool,
    /// Data domain (defaults to `market_data`).
    #[serde(default)]
    pub data_domain: Option<String>,
    /// Client-generated group id for progress polling.
    #[serde(default)]
    pub client_group_id: Option<String>,
}

/// The default range window when the caller does not name one.
fn default_concurrency() -> usize {
    4
}

/// A validated range-load request ready to hand to the background runner.
struct PreparedRangeLoad {
    identifiers: Vec<TypedIdentifier>,
    from: chrono::NaiveDate,
    to: chrono::NaiveDate,
    concurrency: usize,
    source: Option<String>,
    create_missing_assets: bool,
    data_domain: DataDomain,
    client_group_id: Uuid,
    owner_id: String,
    dates_requested: i32,
}

/// Normalizes and de-duplicates identifiers before counting.
///
/// Trimming and case-folding are deterministic, so a batch that differs only
/// by whitespace or case is counted once.
///
/// Implements FR-011 of `specs/034-production-hardening/spec.md`.
fn normalize_identifiers(
    inputs: Vec<IdentifierInput>,
    max_identifiers: usize,
) -> Result<Vec<TypedIdentifier>, LoadApiError> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::with_capacity(inputs.len());
    for input in inputs {
        let identifier = input
            .into_identifier()
            .map_err(|error| LoadApiError::validation("identifiers", error))?;
        let key = (
            identifier.scheme.to_uppercase(),
            identifier.value.to_uppercase(),
        );
        if seen.insert(key) {
            unique.push(identifier);
        }
    }
    if unique.is_empty() {
        return Err(LoadApiError::validation(
            "identifiers",
            "the identifier list is empty after trimming and de-duplication",
        ));
    }
    if unique.len() > max_identifiers {
        return Err(LoadApiError::limit(
            "identifiers",
            max_identifiers as i64,
            unique.len() as i64,
            format!(
                "{} unique identifiers exceed the configured limit of {max_identifiers}",
                unique.len()
            ),
        ));
    }
    Ok(unique)
}

/// Validates a range request before any database or provider work.
///
/// Shared by the synchronous and fire-and-forget range endpoints so both
/// surfaces reject the same inputs with the same messages.
///
/// Implements FR-011 / FR-012 of `specs/034-production-hardening/spec.md`.
fn validate_range_load(
    body: LoadRangeBody,
    config: &crate::config::AppConfig,
    owner_id: &str,
) -> Result<PreparedRangeLoad, LoadApiError> {
    let identifiers = normalize_identifiers(body.identifiers, config.data_load_max_identifiers)?;

    let from = chrono::NaiveDate::parse_from_str(body.from.trim(), "%Y-%m-%d").map_err(|_| {
        LoadApiError::validation(
            "from",
            format!("'{}' is not a valid YYYY-MM-DD date", body.from),
        )
    })?;
    let to = chrono::NaiveDate::parse_from_str(body.to.trim(), "%Y-%m-%d").map_err(|_| {
        LoadApiError::validation(
            "to",
            format!("'{}' is not a valid YYYY-MM-DD date", body.to),
        )
    })?;
    if from > to {
        return Err(LoadApiError::validation(
            "range",
            format!("range start {from} is later than range end {to}"),
        ));
    }
    let days = (to - from).num_days() + 1;
    if days > config.data_load_max_range_days {
        return Err(LoadApiError::limit(
            "range_days",
            config.data_load_max_range_days,
            days,
            format!(
                "range length {days} days exceeds the configured limit of {}",
                config.data_load_max_range_days
            ),
        ));
    }
    let today = chrono::Utc::now().date_naive();
    let latest = today + chrono::Duration::days(1);
    if to > latest {
        return Err(LoadApiError::validation(
            "to",
            format!("end date {to} is more than one day in the future (latest {latest})"),
        ));
    }
    if body.concurrency == 0 || body.concurrency > config.data_load_max_concurrency {
        return Err(LoadApiError::validation(
            "concurrency",
            format!(
                "concurrency must be between 1 and {}, got {}",
                config.data_load_max_concurrency, body.concurrency
            ),
        ));
    }
    let identifier_days = (identifiers.len() as i64).saturating_mul(days);
    if identifier_days > config.data_load_max_identifier_days {
        return Err(LoadApiError::limit(
            "identifier_days",
            config.data_load_max_identifier_days,
            identifier_days,
            format!(
                "identifier-days {identifier_days} exceeds the configured limit of {}",
                config.data_load_max_identifier_days
            ),
        ));
    }
    let data_domain = parse_data_domain(&body.data_domain)?;
    let client_group_id = match body.client_group_id.as_deref() {
        Some(raw) => Uuid::parse_str(raw.trim()).map_err(|_| {
            LoadApiError::validation(
                "client_group_id",
                format!("'{raw}' is not a valid group id"),
            )
        })?,
        None => Uuid::new_v4(),
    };

    Ok(PreparedRangeLoad {
        identifiers,
        from,
        to,
        concurrency: body.concurrency,
        source: body.source,
        create_missing_assets: body.create_missing_assets,
        data_domain,
        client_group_id,
        owner_id: owner_id.to_string(),
        dates_requested: days as i32,
    })
}

/// Registers the durable range group and maps a duplicate to a 409.
///
/// Implements FR-015 of `specs/034-production-hardening/spec.md`.
async fn register_range_group(
    pool: &sqlx::PgPool,
    prepared: &PreparedRangeLoad,
) -> Result<Uuid, LoadApiError> {
    match insert_load_range_group(
        pool,
        &prepared.owner_id,
        prepared.client_group_id,
        prepared.from,
        prepared.to,
        prepared.concurrency as i32,
        prepared.dates_requested,
    )
    .await
    .map_err(internal_error)?
    {
        GroupRegistration::Inserted(group) => Ok(group.id),
        GroupRegistration::Conflict(existing) => Err(LoadApiError::conflict(existing)),
    }
}

fn internal_error(error: MarketDataProviderError) -> LoadApiError {
    tracing::error!(%error, "load request failed");
    let status = match &error {
        MarketDataProviderError::NotFound(_) => StatusCode::NOT_FOUND,
        other => typed_error_to_http(other).0,
    };
    LoadApiError::new(status, "storage_failure", error.to_string())
}

/// Finalizes a registered group from its persisted dates.
///
/// Called on every exit path of the sync and background runners.
///
/// Implements FR-015 / FR-019 of `specs/034-production-hardening/spec.md`.
async fn finalize_registered_group(pool: &sqlx::PgPool, group_id: Uuid) {
    if let Err(error) = finalize_group_from_dates(pool, group_id).await {
        tracing::error!(%group_id, %error, "could not finalize range group");
    }
}

/// Response of the fire-and-forget range endpoint.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct RangeLoadAccepted {
    /// Client group id used by the progress endpoint and history detail.
    pub group_id: Uuid,
    /// Always `"accepted"` for a 202 response.
    pub status: String,
}

/// Parses and validates the optional data domain.
fn parse_data_domain(value: &Option<String>) -> Result<DataDomain, LoadApiError> {
    let raw = value.as_deref().unwrap_or("market_data");
    let domain = raw
        .parse::<DataDomain>()
        .map_err(|error| LoadApiError::validation("data_domain", error))?;
    if !domain.is_loadable() {
        return Err(LoadApiError::validation(
            "data_domain",
            format!(
                "domain '{raw}' is not loadable: {}",
                domain.unavailable_reason().unwrap_or("not available")
            ),
        ));
    }
    Ok(domain)
}

/// `POST /api/market-data/load` — load reference data for a batch of
/// identifiers as of one date.
///
/// Per-identifier failures never produce a non-200 status: the report body
/// is the outcome. Only caller-input validation (HTTP 400) and a missing
/// pool (HTTP 503) produce an error status.
///
/// Implements FR-011 / FR-012 / FR-013 of
/// `specs/018-load-reference-data/spec.md`.
#[utoipa::path(
    post,
    path = "/api/market-data/load",
    tag = "market-data",
    summary = "Load reference data for a batch of identifiers as of one date",
    description = "Loads reference data for unique identifiers and stores each value dual-temporally. Per-identifier failures are reported in the 200 execution report; only caller-input validation and infrastructure failures produce non-200 statuses.",
    request_body = LoadBody,
    responses(
        (status = 200, description = "The batch execution report (per-identifier failures are reported here)", body = IdentifierLoadReport),
        (status = 400, description = "Invalid identifier, date or empty identifier list", body = LoadErrorEnvelope),
        (status = 401, description = "The provider rejected the credentials", body = String, content_type = "text/plain"),
        (status = 415, description = "Missing or unsupported JSON content type", body = String, content_type = "text/plain"),
        (status = 422, description = "The JSON body does not match the required shape", body = String, content_type = "text/plain"),
        (status = 500, description = "Unexpected server error", body = String, content_type = "text/plain"),
        (status = 502, description = "Provider unavailable, rate-limited or infrastructure failure", body = String, content_type = "text/plain"),
        (status = 503, description = "No database pool is configured", body = String, content_type = "text/plain")
    )
)]
pub async fn load(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Json(body): Json<LoadBody>,
) -> LoadApiResult<IdentifierLoadReport> {
    let identifiers =
        normalize_identifiers(body.identifiers, state.config.data_load_max_identifiers)?;

    let as_of_date =
        chrono::NaiveDate::parse_from_str(body.date.trim(), "%Y-%m-%d").map_err(|_| {
            LoadApiError::validation(
                "date",
                format!("'{}' is not a valid YYYY-MM-DD date", body.date),
            )
        })?;
    let data_domain = parse_data_domain(&body.data_domain)?;

    // A date in the future is caller-input validation, checked before the
    // pool so the answer does not depend on whether a database is configured.
    let today = chrono::Utc::now().date_naive();
    if as_of_date > today {
        return Err(LoadApiError::validation(
            "date",
            format!("as-of date {as_of_date} is later than today ({today})"),
        ));
    }

    let pool = state.pool.as_ref().ok_or_else(|| {
        LoadApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "database_unavailable",
            "no database pool configured",
        )
    })?;

    let snapshot = state.market_data.snapshot();
    let report = run_identifier_load_with_snapshot(
        Some(pool),
        &snapshot,
        IdentifierLoadConfig {
            identifiers,
            as_of_date,
            preferred_source: body.source,
            trigger: "api".to_string(),
            dry_run: false,
            create_missing_assets: body.create_missing_assets,
            default_currency: obolargus_core::market_data::load::DEFAULT_ASSET_CURRENCY.to_string(),
        },
        &LoadRunContext {
            data_domain,
            as_of_date: Some(as_of_date),
            range_group_id: None,
            range_from: None,
            range_to: None,
            range_concurrency: None,
            owner_id: auth.owner_id,
        },
    )
    .await
    .map_err(internal_error)?;

    Ok(Json(report))
}

/// `POST /api/market-data/load-range` — load reference data for a batch of
/// identifiers over every calendar day in `from..=to`.
///
/// Implements FR-014 of `specs/018-load-reference-data/spec.md`.
#[utoipa::path(
    post,
    path = "/api/market-data/load-range",
    tag = "market-data",
    summary = "Load reference data for a date range",
    description = "Fans the single-date load out over every calendar day in from..=to with a bounded concurrency window. Per-identifier and per-date failures are reported in the 200 range report; only caller-input validation and infrastructure failures produce non-200 statuses.",
    request_body = LoadRangeBody,
    responses(
        (status = 200, description = "The range execution report (per-date summaries and aggregate counters)", body = IdentifierRangeLoadReport),
        (status = 400, description = "Invalid identifier, date, range, concurrency, or limit", body = LoadErrorEnvelope),
        (status = 409, description = "The client group id is already in use", body = LoadErrorEnvelope),
        (status = 429, description = "Too many load starts", body = LoadErrorEnvelope),
        (status = 401, description = "The provider rejected the credentials", body = String, content_type = "text/plain"),
        (status = 415, description = "Missing or unsupported JSON content type", body = String, content_type = "text/plain"),
        (status = 422, description = "The JSON body does not match the required shape", body = String, content_type = "text/plain"),
        (status = 500, description = "Unexpected server error", body = String, content_type = "text/plain"),
        (status = 502, description = "Provider unavailable, rate-limited or infrastructure failure", body = String, content_type = "text/plain"),
        (status = 503, description = "No database pool is configured", body = String, content_type = "text/plain")
    )
)]
pub async fn load_range(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Json(body): Json<LoadRangeBody>,
) -> LoadApiResult<IdentifierRangeLoadReport> {
    let prepared = validate_range_load(body, &state.config, &auth.owner_id)?;
    check_start_limit(&auth.owner_id, state.config.data_load_starts_per_minute)
        .map_err(LoadApiError::rate_limited)?;
    let pool = state.pool.as_ref().ok_or_else(|| {
        LoadApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "database_unavailable",
            "no database pool configured",
        )
    })?;
    let group_id = register_range_group(pool, &prepared).await?;
    let cancel = match crate::routes::load_cancellation::register(group_id) {
        Ok(cancel) => cancel,
        Err(()) => {
            finalize_registered_group(pool, group_id).await;
            return Err(LoadApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_failure",
                "cancellation registry unavailable",
            ));
        }
    };

    let report = run_identifier_range_load_with_group_cancellable_owned(
        Some(pool),
        &state.market_data,
        IdentifierRangeLoadConfig {
            identifiers: prepared.identifiers,
            from: prepared.from,
            to: prepared.to,
            preferred_source: prepared.source,
            trigger: "api".to_string(),
            dry_run: false,
            concurrency: prepared.concurrency,
            create_missing_assets: prepared.create_missing_assets,
            default_currency: obolargus_core::market_data::load::DEFAULT_ASSET_CURRENCY.to_string(),
        },
        group_id,
        prepared.data_domain,
        Some(cancel),
        &prepared.owner_id,
    )
    .await;

    // Every exit path finalizes the registered group and clears the
    // in-process cancellation handle.
    crate::routes::load_cancellation::finish(group_id);
    finalize_registered_group(pool, group_id).await;

    let report = report.map_err(internal_error)?;
    Ok(Json(report))
}

/// `POST /api/market-data/load-range/async` — validate a range, start it in
/// the background, and return immediately with the group id to poll.
///
/// The progress endpoint already reads completed child rows, so the UI can
/// poll `GET /api/data-load/history/groups/{group_id}/progress` and fetch the
/// final `GET /api/data-load/history/{group_id}` detail when it settles.
#[utoipa::path(
    post,
    path = "/api/market-data/load-range/async",
    tag = "market-data",
    summary = "Start a date-range load in the background",
    description = "Validates the range synchronously, starts the load in the background, and returns 202 with the client group id. Poll the data-load progress endpoint for completion, then read the history detail for the report.",
    request_body = LoadRangeBody,
    responses(
        (status = 202, description = "The range load was accepted", body = RangeLoadAccepted),
        (status = 400, description = "Invalid identifier, date, range, concurrency, or limit", body = LoadErrorEnvelope),
        (status = 409, description = "The client group id is already in use", body = LoadErrorEnvelope),
        (status = 429, description = "Too many load starts", body = LoadErrorEnvelope),
        (status = 503, description = "No database pool is configured", body = LoadErrorEnvelope)
    )
)]
pub async fn start_load_range(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Json(body): Json<LoadRangeBody>,
) -> Result<(StatusCode, Json<RangeLoadAccepted>), LoadApiError> {
    let prepared = validate_range_load(body, &state.config, &auth.owner_id)?;
    check_start_limit(&auth.owner_id, state.config.data_load_starts_per_minute)
        .map_err(LoadApiError::rate_limited)?;
    let pool = state.pool.as_ref().ok_or_else(|| {
        LoadApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "database_unavailable",
            "no database pool configured",
        )
    })?;
    let group_id = register_range_group(pool, &prepared).await?;
    let cancel = match crate::routes::load_cancellation::register(group_id) {
        Ok(cancel) => cancel,
        Err(()) => {
            finalize_registered_group(pool, group_id).await;
            return Err(LoadApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_failure",
                "cancellation registry unavailable",
            ));
        }
    };
    let background_pool = pool.clone();
    let registry = state.market_data.clone();

    tokio::spawn(async move {
        let result = run_identifier_range_load_with_group_cancellable_owned(
            Some(&background_pool),
            &registry,
            IdentifierRangeLoadConfig {
                identifiers: prepared.identifiers,
                from: prepared.from,
                to: prepared.to,
                preferred_source: prepared.source,
                trigger: "api".to_string(),
                dry_run: false,
                concurrency: prepared.concurrency,
                create_missing_assets: prepared.create_missing_assets,
                default_currency: obolargus_core::market_data::load::DEFAULT_ASSET_CURRENCY
                    .to_string(),
            },
            group_id,
            prepared.data_domain,
            Some(cancel),
            &prepared.owner_id,
        )
        .await;

        // Every exit path, including an early infrastructure error, leaves a
        // terminal group and clears the in-process stop handle.
        crate::routes::load_cancellation::finish(group_id);
        finalize_registered_group(&background_pool, group_id).await;
        if let Err(error) = result {
            tracing::error!(%group_id, %error, "background range load failed");
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(RangeLoadAccepted {
            group_id,
            status: "accepted".to_string(),
        }),
    ))
}

/// Body of `POST /api/market-data/history-load`.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct HistoryLoadBody {
    /// Existing asset id.
    pub asset_id: Uuid,
    /// Inclusive first date (`YYYY-MM-DD`).
    pub from: String,
    /// Inclusive last date (`YYYY-MM-DD`), not later than today.
    pub to: String,
    /// Optional preferred source id.
    #[serde(default)]
    pub source: Option<String>,
}

/// `POST /api/market-data/history-load` — load one bounded window for an asset.
///
/// The window is at most 366 calendar days. The operation resolves the asset's
/// preferred identifier, walks the configured sources once, and stores every
/// returned bar. Provider failures are report outcomes; only validation,
/// unknown assets, a missing pool, or audit/storage errors produce non-200
/// statuses.
#[utoipa::path(
    post,
    path = "/api/market-data/history-load",
    tag = "market-data",
    summary = "Load a bounded history window for an asset",
    description = "Loads stored end-of-day bars for one existing asset over a window of at most 366 days. Per-source and absence outcomes are reported in the 200 body.",
    request_body = HistoryLoadBody,
    responses(
        (status = 200, description = "The history-load report", body = HistoryLoadReport),
        (status = 400, description = "Invalid date, reversed/future/over-long range, or asset without identifiers", body = String, content_type = "text/plain"),
        (status = 404, description = "Unknown asset", body = String, content_type = "text/plain"),
        (status = 502, description = "Provider unavailable or rate-limited before a report could be produced", body = String, content_type = "text/plain"),
        (status = 503, description = "No database pool configured", body = String, content_type = "text/plain"),
        (status = 500, description = "Unexpected server, audit, or storage failure", body = String, content_type = "text/plain")
    )
)]
pub async fn history_load(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Json(body): Json<HistoryLoadBody>,
) -> Result<Json<HistoryLoadReport>, (StatusCode, String)> {
    let from = chrono::NaiveDate::parse_from_str(body.from.trim(), "%Y-%m-%d").map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            format!(
                "validation error: '{}' is not a valid YYYY-MM-DD date",
                body.from
            ),
        )
    })?;
    let to = chrono::NaiveDate::parse_from_str(body.to.trim(), "%Y-%m-%d").map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            format!(
                "validation error: '{}' is not a valid YYYY-MM-DD date",
                body.to
            ),
        )
    })?;
    if from > to {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("validation error: range start {from} is later than range end {to}"),
        ));
    }
    let today = chrono::Utc::now().date_naive();
    if to > today {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("validation error: as-of date {to} is later than today ({today})"),
        ));
    }
    let days = (to - from).num_days() + 1;
    if days > obolargus_core::market_data::history_load::MAX_HISTORY_WINDOW_DAYS {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "validation error: the history range is {days} days; the maximum is {}",
                obolargus_core::market_data::history_load::MAX_HISTORY_WINDOW_DAYS
            ),
        ));
    }
    let pool = state.pool.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "no database pool configured".to_string(),
        )
    })?;
    let asset_id = AssetId::from_uuid(body.asset_id);
    get_asset(pool, asset_id)
        .await
        .map_err(|error| match error {
            DomainError::NotFound { .. } => (
                StatusCode::NOT_FOUND,
                format!("unknown asset '{}'", body.asset_id),
            ),
            other => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("asset lookup failed: {other}"),
            ),
        })?;
    let report = run_asset_history_load(
        pool,
        &state.market_data,
        HistoryLoadConfig {
            asset_id,
            from,
            to,
            preferred_source: body.source,
            trigger: "chart".to_string(),
            owner_id: auth.owner_id,
        },
    )
    .await
    .map_err(|error| {
        tracing::error!(%error, "history load failed");
        typed_error_to_http(&error)
    })?;
    Ok(Json(report))
}

/// Maps a typed provider error onto an HTTP status code and message.
///
/// Implements the error surface of FR-005 for the HTTP layer.
pub fn typed_error_to_http(e: &MarketDataProviderError) -> (StatusCode, String) {
    use MarketDataProviderError::*;
    let status = match e {
        NotFound(_) | InvalidInput(_) | Unsupported(_) => StatusCode::BAD_REQUEST,
        Unauthorized(_) => StatusCode::UNAUTHORIZED,
        Validation(_) => StatusCode::BAD_REQUEST,
        Transient(_) | RateLimited { .. } | Infrastructure(_) => StatusCode::BAD_GATEWAY,
        // The error enum is `#[non_exhaustive]`; unknown future variants
        // map to 500 rather than silently becoming a 4xx.
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, format!("{e}"))
}
