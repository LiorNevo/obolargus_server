//! HTTP handlers for the Data Load page.
//!
//! Implements the read/control surface of
//! `specs/022-market-data-load-ui/contracts/data-load-api.md`.

#![forbid(unsafe_code)]

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use obolargus_core::market_data::data_domain::DataDomain;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::history::{
    HistoryFilter, LoadHistoryDetail, LoadHistoryPage, RangeProgress, get_history_detail,
    list_history, range_progress,
};
use obolargus_core::market_data::registry::SourceCatalogEntry;
use obolargus_core::market_data::source_state::{SourceStateChangeResult, set_source_enabled};

use crate::AppState;

/// JSON error envelope for the `/api/data-load/*` surface.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ApiErrorBody {
    /// Error details.
    pub error: ApiErrorDetail,
}

/// One error code/message pair.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ApiErrorDetail {
    /// Stable machine-readable code.
    pub code: String,
    /// Operator-facing message.
    pub message: String,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiErrorBody>)>;

fn api_error(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
) -> (StatusCode, Json<ApiErrorBody>) {
    (
        status,
        Json(ApiErrorBody {
            error: ApiErrorDetail {
                code: code.to_string(),
                message: message.into(),
            },
        }),
    )
}

fn map_error(error: MarketDataProviderError) -> (StatusCode, Json<ApiErrorBody>) {
    match error {
        MarketDataProviderError::NotFound(message) => {
            api_error(StatusCode::NOT_FOUND, "not_found", message)
        }
        MarketDataProviderError::Validation(message)
        | MarketDataProviderError::InvalidInput(message) => {
            api_error(StatusCode::BAD_REQUEST, "validation_error", message)
        }
        MarketDataProviderError::Unauthorized(message) => {
            api_error(StatusCode::UNAUTHORIZED, "unauthorized", message)
        }
        MarketDataProviderError::Transient(message)
        | MarketDataProviderError::Infrastructure(message) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage_failure",
            message,
        ),
        MarketDataProviderError::RateLimited { msg, .. } => {
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_failure", msg)
        }
        MarketDataProviderError::Unsupported(message) => {
            api_error(StatusCode::BAD_REQUEST, "validation_error", message)
        }
        _ => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage_failure",
            "unexpected server error",
        ),
    }
}

fn require_pool(state: &AppState) -> Result<&sqlx::PgPool, (StatusCode, Json<ApiErrorBody>)> {
    state.pool.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "database_unavailable",
            "no database pool configured",
        )
    })
}

/// One domain in the catalog.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DomainEntry {
    /// Wire id.
    pub id: String,
    /// Human-readable label.
    pub label: String,
    /// Whether it can be loaded in this release.
    pub loadable: bool,
    /// Why it cannot be loaded, when it cannot.
    pub unavailable_reason: Option<String>,
}

/// Response of `GET /api/data-load/domains`.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DomainCatalog {
    /// Declared domains.
    pub domains: Vec<DomainEntry>,
}

/// `GET /api/data-load/domains`
#[utoipa::path(
    get,
    path = "/api/data-load/domains",
    tag = "data-load",
    summary = "List data domains",
    description = "Returns every declared data domain and whether it is loadable in this release.",
    responses((status = 200, description = "The domain catalog", body = DomainCatalog))
)]
pub async fn list_domains() -> Json<DomainCatalog> {
    let domains = [DataDomain::MarketData, DataDomain::Holdings]
        .into_iter()
        .map(|domain| DomainEntry {
            id: domain.as_str().to_string(),
            label: domain.label().to_string(),
            loadable: domain.is_loadable(),
            unavailable_reason: domain.unavailable_reason().map(str::to_string),
        })
        .collect();
    Json(DomainCatalog { domains })
}

/// Query for the source catalog.
#[derive(Debug, Deserialize)]
pub struct SourceQuery {
    /// Optional domain filter.
    pub domain: Option<String>,
}

/// Response of `GET /api/data-load/sources`.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct SourceCatalogResponse {
    /// Domain the selectability was computed for.
    pub domain: Option<String>,
    /// Configured sources.
    pub sources: Vec<SourceCatalogEntry>,
}

/// `GET /api/data-load/sources`
#[utoipa::path(
    get,
    path = "/api/data-load/sources",
    tag = "data-load",
    summary = "List configured sources",
    description = "Returns every configured source, including disabled ones, with runtime state and domain metadata.",
    params(("domain" = Option<String>, Query, description = "Optional data domain")),
    responses(
        (status = 200, description = "The source catalog", body = SourceCatalogResponse),
        (status = 400, description = "Unknown domain", body = ApiErrorBody)
    )
)]
pub async fn list_sources(
    State(state): State<AppState>,
    Query(query): Query<SourceQuery>,
) -> ApiResult<SourceCatalogResponse> {
    let domain = match query.domain.as_deref() {
        Some(raw) => Some(
            raw.parse::<DataDomain>()
                .map_err(|error| api_error(StatusCode::BAD_REQUEST, "validation_error", error))?,
        ),
        None => None,
    };
    Ok(Json(SourceCatalogResponse {
        domain: domain.map(|value| value.as_str().to_string()),
        sources: state.market_data.catalog(domain),
    }))
}

/// Body of `PATCH /api/data-load/sources/{source_id}`.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct SourceToggleBody {
    /// Desired state.
    pub enabled: bool,
    /// Optional operator note.
    #[serde(default)]
    pub reason: Option<String>,
}

/// `PATCH /api/data-load/sources/{source_id}`
#[utoipa::path(
    patch,
    path = "/api/data-load/sources/{source_id}",
    tag = "data-load",
    summary = "Enable or disable a configured source",
    description = "Persists the runtime enable state and appends an audit entry. A no-op request returns changed=false and writes no audit row.",
    request_body = SourceToggleBody,
    responses(
        (status = 200, description = "The resulting source state", body = SourceStateChangeResult),
        (status = 400, description = "Invalid body", body = ApiErrorBody),
        (status = 404, description = "Unknown source", body = ApiErrorBody),
        (status = 409, description = "Source configuration cannot be enabled", body = ApiErrorBody),
        (status = 500, description = "Storage failure", body = ApiErrorBody),
        (status = 503, description = "No database pool", body = ApiErrorBody)
    )
)]
pub async fn toggle_source(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Json(body): Json<SourceToggleBody>,
) -> ApiResult<SourceStateChangeResult> {
    let pool = require_pool(&state)?;
    let result = set_source_enabled(
        pool,
        &state.market_data,
        &source_id,
        body.enabled,
        None,
        body.reason.as_deref(),
    )
    .await
    .map_err(map_error)?;
    Ok(Json(result))
}

/// Query for the history list.
#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    /// Inclusive lower date.
    pub from: Option<String>,
    /// Inclusive upper date.
    pub to: Option<String>,
    /// Data domain.
    pub domain: Option<String>,
    /// Requested/answering/source-change source.
    pub source: Option<String>,
    /// Derived status.
    pub status: Option<String>,
    /// Entry type.
    pub entry_type: Option<String>,
    /// Page size.
    pub limit: Option<u32>,
    /// Offset.
    pub offset: Option<u32>,
}

/// `GET /api/data-load/history`
#[utoipa::path(
    get,
    path = "/api/data-load/history",
    tag = "data-load",
    summary = "List load history",
    description = "Returns paginated load runs, range groups, and source configuration changes, newest first.",
    responses(
        (status = 200, description = "A page of history", body = LoadHistoryPage),
        (status = 400, description = "Invalid filter", body = ApiErrorBody),
        (status = 503, description = "No database pool", body = ApiErrorBody)
    )
)]
pub async fn list_history_handler(
    State(state): State<AppState>,
    Query(query): Query<HistoryQuery>,
) -> ApiResult<LoadHistoryPage> {
    let pool = require_pool(&state)?;
    let filter = build_filter(query)?;
    let page = list_history(pool, &filter, state.config.data_load_stale_after_seconds)
        .await
        .map_err(map_error)?;
    Ok(Json(page))
}

fn build_filter(query: HistoryQuery) -> Result<HistoryFilter, (StatusCode, Json<ApiErrorBody>)> {
    let parse_date = |value: Option<String>,
                      name: &str|
     -> Result<Option<chrono::NaiveDate>, (StatusCode, Json<ApiErrorBody>)> {
        value
            .map(|raw| {
                chrono::NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d").map_err(|_| {
                    api_error(
                        StatusCode::BAD_REQUEST,
                        "validation_error",
                        format!("invalid {name} date '{raw}'"),
                    )
                })
            })
            .transpose()
    };
    let from = parse_date(query.from, "from")?;
    let to = parse_date(query.to, "to")?;
    if let (Some(from), Some(to)) = (from, to)
        && from > to
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "from is later than to",
        ));
    }
    if let Some(domain) = query.domain.as_deref()
        && domain.parse::<DataDomain>().is_err()
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("unknown data domain '{domain}'"),
        ));
    }
    if let Some(status) = query.status.as_deref()
        && !matches!(
            status,
            "complete"
                | "partial"
                | "failed"
                | "running"
                | "incomplete"
                | "source_enabled"
                | "source_disabled"
        )
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("unknown status '{status}'"),
        ));
    }
    if let Some(entry_type) = query.entry_type.as_deref()
        && !matches!(
            entry_type,
            "load_run" | "range_run" | "source_change" | "run"
        )
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("unknown entry type '{entry_type}'"),
        ));
    }
    let limit = query.limit.unwrap_or(50);
    if !(1..=200).contains(&limit) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "limit must be between 1 and 200",
        ));
    }
    Ok(HistoryFilter {
        from,
        to,
        domain: query.domain,
        source: query.source,
        status: query.status,
        entry_type: query.entry_type,
        limit: i64::from(limit),
        offset: i64::from(query.offset.unwrap_or(0)),
    })
}

/// `GET /api/data-load/history/{entry_id}`
#[utoipa::path(
    get,
    path = "/api/data-load/history/{entry_id}",
    tag = "data-load",
    summary = "Read one history entry",
    description = "Returns the summary and the complete read-only detail for a run, range, or source change.",
    responses(
        (status = 200, description = "The entry detail", body = LoadHistoryDetail),
        (status = 400, description = "Invalid entry id", body = ApiErrorBody),
        (status = 404, description = "Unknown entry", body = ApiErrorBody),
        (status = 503, description = "No database pool", body = ApiErrorBody)
    )
)]
pub async fn get_history_handler(
    State(state): State<AppState>,
    Path(entry_id): Path<Uuid>,
) -> ApiResult<LoadHistoryDetail> {
    let pool = require_pool(&state)?;
    let detail = get_history_detail(pool, entry_id, state.config.data_load_stale_after_seconds)
        .await
        .map_err(map_error)?;
    Ok(Json(detail))
}

/// `GET /api/data-load/history/groups/{group_id}/progress`
#[utoipa::path(
    get,
    path = "/api/data-load/history/groups/{group_id}/progress",
    tag = "data-load",
    summary = "Read range progress",
    description = "Counts a range group's completed child runs. Returns starting before the first child exists.",
    responses(
        (status = 200, description = "The progress snapshot", body = RangeProgress),
        (status = 400, description = "Invalid group id", body = ApiErrorBody),
        (status = 503, description = "No database pool", body = ApiErrorBody)
    )
)]
pub async fn get_progress_handler(
    State(state): State<AppState>,
    Path(group_id): Path<Uuid>,
) -> ApiResult<RangeProgress> {
    let pool = require_pool(&state)?;
    let progress = range_progress(pool, group_id).await.map_err(map_error)?;
    Ok(Json(progress))
}
