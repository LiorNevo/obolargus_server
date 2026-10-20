//! HTTP handlers for the source-agnostic market data API.
//!
//! Implements the wire surface named in `specs/017-yahoo-data-source/spec.md`:
//! - `GET  /api/market-data/sources`    — enumerate configured sources.
//! - `POST /api/market-data/refresh`    — on-demand refresh for one asset.
//! - `GET  /api/market-data/lookup`     — ticker / company-name lookup.
//! - `POST /api/market-data/reconcile`  — cross-source reconciliation.

#![forbid(unsafe_code)]

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use obolargus_core::domain::id::DomainId;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::reconcile::{
    ReconcileConfig, ReconciliationPolicy, run_reconcile,
};
use obolargus_core::market_data::refresh::{RefreshConfig, run_refresh};
use obolargus_core::market_data::types::TickerLookupRequest;

use crate::AppState;

/// `GET /api/market-data/sources` — return every configured source.
pub async fn list_sources(
    State(state): State<AppState>,
) -> Json<Vec<obolargus_core::market_data::registry::SourceDescriptor>> {
    Json(state.market_data.enumerate_sources())
}

/// `POST /api/market-data/refresh` — on-demand refresh for one asset.
#[derive(Debug, Deserialize)]
pub struct RefreshBody {
    /// Asset id (UUID) to refresh.
    pub asset_id: Uuid,
}

#[derive(Debug, Serialize)]
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
#[derive(Debug, Deserialize)]
pub struct LookupQuery {
    /// The free-text query (ticker or company-name fragment).
    pub q: String,
}

/// One source's lookup result.
#[derive(Debug, Serialize)]
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
#[derive(Debug, Deserialize)]
pub struct ReconcileBody {
    pub asset_id: Uuid,
    pub fact_type: String,
    pub as_of_date: chrono::NaiveDate,
    pub policy: String,
}

#[derive(Debug, Serialize)]
pub struct ReconcileResponse {
    pub reconciliation_id: Uuid,
    pub canonical_value: String,
    pub sources_tried: usize,
}

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
