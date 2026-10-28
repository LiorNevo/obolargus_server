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

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use obolargus_core::domain::id::DomainId;
use obolargus_core::market_data::audit::LoadRunContext;
use obolargus_core::market_data::data_domain::DataDomain;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::load::{
    IdentifierLoadConfig, IdentifierLoadReport, run_identifier_load_with_snapshot,
};
use obolargus_core::market_data::load_range::{
    IdentifierRangeLoadConfig, IdentifierRangeLoadReport, run_identifier_range_load_with_group,
};
use obolargus_core::market_data::reconcile::{
    ReconcileConfig, ReconciliationPolicy, run_reconcile,
};
use obolargus_core::market_data::refresh::{RefreshConfig, run_refresh};
use obolargus_core::market_data::registry::SourceDescriptor;
use obolargus_core::market_data::types::{TickerLookupRequest, TypedIdentifier};

use crate::AppState;

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
    pub canonical_value: String,
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

/// Parses and validates the optional data domain.
fn parse_data_domain(value: &Option<String>) -> Result<DataDomain, (StatusCode, String)> {
    let raw = value.as_deref().unwrap_or("market_data");
    let domain = raw.parse::<DataDomain>().map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("validation error: {error}"),
        )
    })?;
    if !domain.is_loadable() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "validation error: domain '{raw}' is not loadable: {}",
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
        (status = 400, description = "Invalid identifier, date or empty identifier list", body = String, content_type = "text/plain"),
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
    Json(body): Json<LoadBody>,
) -> Result<Json<IdentifierLoadReport>, (StatusCode, String)> {
    let mut identifiers = Vec::with_capacity(body.identifiers.len());
    for input in body.identifiers {
        identifiers.push(
            input
                .into_identifier()
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("validation error: {e}")))?,
        );
    }
    if identifiers.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "validation error: the identifier list is empty".to_string(),
        ));
    }

    let as_of_date =
        chrono::NaiveDate::parse_from_str(body.date.trim(), "%Y-%m-%d").map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                format!(
                    "validation error: '{}' is not a valid YYYY-MM-DD date",
                    body.date
                ),
            )
        })?;
    let data_domain = parse_data_domain(&body.data_domain)?;

    // A date in the future is caller-input validation, checked before the
    // pool so the answer does not depend on whether a database is
    // configured.
    let today = chrono::Utc::now().date_naive();
    if as_of_date > today {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("validation error: as-of date {as_of_date} is later than today ({today})"),
        ));
    }

    let pool = state.pool.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "no database pool configured".to_string(),
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
        },
    )
    .await
    .map_err(|e| typed_error_to_http(&e))?;

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
        (status = 200, description = "The range execution report (per-identifier failures are reported here)", body = IdentifierRangeLoadReport),
        (status = 400, description = "Invalid identifier, date, range or concurrency", body = String, content_type = "text/plain"),
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
    Json(body): Json<LoadRangeBody>,
) -> Result<Json<IdentifierRangeLoadReport>, (StatusCode, String)> {
    let mut identifiers = Vec::with_capacity(body.identifiers.len());
    for input in body.identifiers {
        identifiers.push(
            input
                .into_identifier()
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("validation error: {e}")))?,
        );
    }
    if identifiers.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "validation error: the identifier list is empty".to_string(),
        ));
    }

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
    let data_domain = parse_data_domain(&body.data_domain)?;
    let group_id = match body.client_group_id.as_deref() {
        Some(raw) => Uuid::parse_str(raw.trim()).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                format!("validation error: '{raw}' is not a valid group id"),
            )
        })?,
        None => Uuid::new_v4(),
    };

    let today = chrono::Utc::now().date_naive();
    if from > to {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("validation error: range start {from} is later than range end {to}"),
        ));
    }
    if to > today {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("validation error: as-of date {to} is later than today ({today})"),
        ));
    }
    if body.concurrency == 0
        || body.concurrency > obolargus_core::market_data::load_range::MAX_CONCURRENCY
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "validation error: concurrency must be between 1 and {}",
                obolargus_core::market_data::load_range::MAX_CONCURRENCY
            ),
        ));
    }

    let pool = state.pool.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "no database pool configured".to_string(),
        )
    })?;

    let conflict: bool = sqlx::query_scalar(
        "SELECT EXISTS(
            SELECT 1 FROM reference_data_load_runs
            WHERE id = $1 OR range_group_id = $1
         )",
    )
    .bind(group_id)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("group id check failed: {e}"),
        )
    })?;
    if conflict {
        return Err((
            StatusCode::CONFLICT,
            "group_id_conflict: the client group id is already in use".to_string(),
        ));
    }

    let report = run_identifier_range_load_with_group(
        Some(pool),
        &state.market_data,
        IdentifierRangeLoadConfig {
            identifiers,
            from,
            to,
            preferred_source: body.source,
            trigger: "api".to_string(),
            dry_run: false,
            concurrency: body.concurrency,
            create_missing_assets: body.create_missing_assets,
            default_currency: obolargus_core::market_data::load::DEFAULT_ASSET_CURRENCY.to_string(),
        },
        group_id,
        data_domain,
    )
    .await
    .map_err(|e| typed_error_to_http(&e))?;

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
