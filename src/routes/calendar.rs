//! Calendar-aware chart coverage reads.
//!
//! The chart asks one question: for this asset and range, which dates were
//! expected, which are missing, and which calendar basis applied? All holiday
//! policy stays in `obolargus_core::market_data::calendar`.

#![forbid(unsafe_code)]

use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use obolargus_core::domain::asset::get_asset;
use obolargus_core::domain::error::DomainError;
use obolargus_core::domain::id::{AssetId, DomainId};
use obolargus_core::market_data::calendar::{
    AssetCoverage, CalendarAssignmentPage, CalendarDetail, CalendarDocument, CalendarSummary,
    ExchangeCalendar, affected_assets, asset_coverage, create_assignment, export_document,
    get_calendar, get_calendar_detail, list_assignment_views, list_calendar_summaries,
    remove_assignment, replace_calendar, retire_calendar,
};

use crate::AppState;
use crate::auth::AuthUser;
use crate::routes::ErrorEnvelope;

/// Query for the asset coverage read.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct CoverageQuery {
    /// Inclusive first date (`YYYY-MM-DD`).
    pub from: String,
    /// Inclusive last date (`YYYY-MM-DD`).
    pub to: String,
}

fn parse_date(raw: &str, field: &str) -> Result<NaiveDate, CoverageError> {
    NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d").map_err(|_| CoverageError {
        status: StatusCode::BAD_REQUEST,
        code: "validation_error",
        message: format!("validation error: '{raw}' is not a valid YYYY-MM-DD {field}"),
    })
}

fn map_domain_error(error: DomainError) -> CoverageError {
    match error {
        DomainError::NotFound { .. } => CoverageError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: error.to_string(),
        },
        DomainError::Validation(message) => CoverageError {
            status: StatusCode::BAD_REQUEST,
            code: "validation_error",
            message,
        },
        other => CoverageError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "infrastructure",
            message: other.to_string(),
        },
    }
}

/// Coverage read failure rendered as the standard error envelope.
pub struct CoverageError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl IntoResponse for CoverageError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorEnvelope::new(self.code, self.message)),
        )
            .into_response()
    }
}

/// `GET /api/market-data/assets/{asset_id}/coverage?from=YYYY-MM-DD&to=YYYY-MM-DD`
#[utoipa::path(
    get,
    path = "/api/market-data/assets/{asset_id}/coverage",
    tag = "market-data",
    summary = "Read calendar-aware coverage for one asset",
    description = "Returns calendar resolution segments, the completed expected session count, missing session dates, and the latest completed session. No price values are returned.",
    params(
        ("asset_id" = Uuid, Path, description = "Asset id"),
        ("from" = String, Query, description = "Inclusive first date"),
        ("to" = String, Query, description = "Inclusive last date")
    ),
    responses(
        (status = 200, description = "Calendar-aware coverage", body = AssetCoverage),
        (status = 400, description = "Invalid range", body = ErrorEnvelope),
        (status = 404, description = "Unknown asset", body = ErrorEnvelope),
        (status = 401, description = "Unauthenticated", body = ErrorEnvelope),
        (status = 500, description = "Infrastructure failure", body = ErrorEnvelope),
        (status = 503, description = "No database", body = ErrorEnvelope)
    )
)]
pub async fn coverage_handler(
    State(state): State<AppState>,
    Path(asset_id): Path<Uuid>,
    Query(query): Query<CoverageQuery>,
) -> Result<Json<AssetCoverage>, CoverageError> {
    let from = parse_date(&query.from, "start date")?;
    let to = parse_date(&query.to, "end date")?;
    if from > to {
        return Err(CoverageError {
            status: StatusCode::BAD_REQUEST,
            code: "validation_error",
            message: "validation error: coverage start is later than its end".to_string(),
        });
    }
    let pool = state.pool.as_ref().ok_or_else(|| CoverageError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "database_unavailable",
        message: "no database pool configured".to_string(),
    })?;
    let asset = AssetId::from_uuid(asset_id);
    get_asset(pool, asset).await.map_err(map_domain_error)?;
    asset_coverage(pool, asset, from, to, chrono::Utc::now())
        .await
        .map(Json)
        .map_err(map_domain_error)
}

/// Calendar list response.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct CalendarListResponse {
    /// Calendars ordered by venue code.
    pub calendars: Vec<CalendarSummary>,
}

/// Import response.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct CalendarImportResponse {
    /// Stored calendar.
    pub calendar: ExchangeCalendar,
    /// Whether the document changed the stored version.
    pub changed: bool,
    /// Human summary.
    pub summary: String,
}

/// Retire request body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct RetireBody {
    /// Confirms the fallback for affected instruments.
    #[serde(default)]
    pub confirm: bool,
}

/// Retire response.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct CalendarRetireResponse {
    /// Stored calendar after the state change.
    pub calendar: ExchangeCalendar,
    /// Instruments that fall back to the weekday estimate.
    pub affected_count: i64,
}

/// Optional version guard on import.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct ImportQuery {
    /// Expected stored version (`0` or absent when creating).
    pub expected_version: Option<i32>,
}

/// Assignment list filters.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct AssignmentQuery {
    /// Optional instrument id.
    pub asset_id: Option<Uuid>,
    /// Optional venue code.
    pub mic: Option<String>,
    /// Page size (default 50, max 200).
    pub limit: Option<i64>,
    /// Offset (default 0).
    pub offset: Option<i64>,
}

/// Assignment create body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct AssignmentBody {
    /// Instrument id.
    pub asset_id: Uuid,
    /// Venue code.
    pub mic: String,
    /// Inclusive first effective date.
    pub valid_from: NaiveDate,
    /// Exclusive last effective date.
    pub valid_to: Option<NaiveDate>,
}

/// Calendar management failure rendered as the standard error envelope.
pub struct CalendarApiError {
    status: StatusCode,
    body: Json<Value>,
}

impl IntoResponse for CalendarApiError {
    fn into_response(self) -> Response {
        (self.status, self.body).into_response()
    }
}

fn api_error(status: StatusCode, code: &str, message: &str) -> CalendarApiError {
    CalendarApiError {
        status,
        body: Json(json!({"error": {"code": code, "message": message}})),
    }
}

fn validation_error(message: String) -> CalendarApiError {
    let mut error = api_error(
        StatusCode::UNPROCESSABLE_ENTITY,
        "calendar_validation_failed",
        "The calendar document is invalid.",
    );
    error.body = Json(json!({
        "error": {
            "code": "calendar_validation_failed",
            "message": "The calendar document is invalid.",
        },
        "issues": [{"field": "document", "message": message}],
    }));
    error
}

fn map_calendar_error(error: DomainError) -> CalendarApiError {
    match error {
        DomainError::NotFound { .. } => {
            api_error(StatusCode::NOT_FOUND, "not_found", &error.to_string())
        }
        DomainError::Validation(message) if message.contains("retire action") => api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "use_retire_endpoint",
            &message,
        ),
        DomainError::Validation(message) => validation_error(message),
        DomainError::InvariantViolation { invariant, detail } => match invariant {
            "calendar_version_conflict" => {
                api_error(StatusCode::CONFLICT, "version_conflict", &detail)
            }
            "calendar_assignment_overlap" => {
                let mut parsed = detail.split(';');
                let id = parsed
                    .next()
                    .and_then(|part| part.strip_prefix("id="))
                    .and_then(|value| Uuid::parse_str(value).ok());
                let from = parsed
                    .next()
                    .and_then(|part| part.strip_prefix("from="))
                    .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok());
                let to = parsed
                    .next()
                    .and_then(|part| part.strip_prefix("to="))
                    .filter(|value| !value.is_empty())
                    .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok());
                CalendarApiError {
                    status: StatusCode::CONFLICT,
                    body: Json(json!({
                        "error": {
                            "code": "assignment_overlap",
                            "message": "This instrument already has a calendar assignment in that period.",
                        },
                        "conflict": {"id": id, "valid_from": from, "valid_to": to},
                    })),
                }
            }
            "calendar_in_use" => api_error(
                StatusCode::CONFLICT,
                "calendar_in_use",
                "The calendar is in use; confirm the fallback.",
            ),
            _ => api_error(StatusCode::INTERNAL_SERVER_ERROR, "infrastructure", &detail),
        },
        other => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "infrastructure",
            &other.to_string(),
        ),
    }
}

fn require_pool(state: &AppState) -> Result<&sqlx::PgPool, CalendarApiError> {
    state.pool.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "database_unavailable",
            "no database pool configured",
        )
    })
}

fn require_primary(state: &AppState, auth: &AuthUser) -> Result<(), CalendarApiError> {
    if state.auth.is_primary_owner(auth) {
        Ok(())
    } else {
        Err(api_error(
            StatusCode::FORBIDDEN,
            "primary_owner_required",
            "Only the primary owner may change calendars.",
        ))
    }
}

/// `GET /api/market-data/calendars`
#[utoipa::path(
    get,
    path = "/api/market-data/calendars",
    tag = "market-data",
    summary = "List exchange calendars",
    responses(
        (status = 200, description = "Calendars", body = CalendarListResponse),
        (status = 401, description = "Unauthenticated", body = ErrorEnvelope),
        (status = 500, description = "Infrastructure failure", body = ErrorEnvelope),
        (status = 503, description = "No database", body = ErrorEnvelope)
    )
)]
pub async fn list_calendars_handler(
    State(state): State<AppState>,
) -> Result<Json<CalendarListResponse>, CalendarApiError> {
    let pool = require_pool(&state)?;
    let calendars = list_calendar_summaries(pool, chrono::Utc::now().date_naive())
        .await
        .map_err(map_calendar_error)?;
    Ok(Json(CalendarListResponse { calendars }))
}

/// `GET /api/market-data/calendars/{mic}`
#[utoipa::path(
    get,
    path = "/api/market-data/calendars/{mic}",
    tag = "market-data",
    summary = "Read one exchange calendar",
    params(("mic" = String, Path, description = "Venue code")),
    responses(
        (status = 200, description = "Calendar detail", body = CalendarDetail),
        (status = 401, description = "Unauthenticated", body = ErrorEnvelope),
        (status = 404, description = "Unknown venue code", body = ErrorEnvelope),
        (status = 500, description = "Infrastructure failure", body = ErrorEnvelope),
        (status = 503, description = "No database", body = ErrorEnvelope)
    )
)]
pub async fn get_calendar_handler(
    State(state): State<AppState>,
    Path(mic): Path<String>,
) -> Result<Json<CalendarDetail>, CalendarApiError> {
    let pool = require_pool(&state)?;
    get_calendar_detail(pool, &mic, chrono::Utc::now().date_naive())
        .await
        .map(Json)
        .map_err(map_calendar_error)
}

/// `GET /api/market-data/calendars/{mic}/export`
#[utoipa::path(
    get,
    path = "/api/market-data/calendars/{mic}/export",
    tag = "market-data",
    summary = "Export one exchange calendar",
    params(("mic" = String, Path, description = "Venue code")),
    responses(
        (status = 200, description = "Importable calendar document", body = CalendarDocument),
        (status = 401, description = "Unauthenticated", body = ErrorEnvelope),
        (status = 404, description = "Unknown venue code", body = ErrorEnvelope),
        (status = 500, description = "Infrastructure failure", body = ErrorEnvelope),
        (status = 503, description = "No database", body = ErrorEnvelope)
    )
)]
pub async fn export_calendar_handler(
    State(state): State<AppState>,
    Path(mic): Path<String>,
) -> Result<Json<CalendarDocument>, CalendarApiError> {
    let pool = require_pool(&state)?;
    let calendar = get_calendar(pool, &mic)
        .await
        .map_err(map_calendar_error)?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "not_found", "unknown venue code"))?;
    Ok(Json(export_document(&calendar)))
}

/// `PUT /api/market-data/calendars/{mic}`
#[utoipa::path(
    put,
    path = "/api/market-data/calendars/{mic}",
    tag = "market-data",
    summary = "Import or replace an exchange calendar",
    params(
        ("mic" = String, Path, description = "Venue code"),
        ("expected_version" = Option<i32>, Query, description = "Optimistic concurrency guard")
    ),
    request_body = CalendarDocument,
    responses(
        (status = 200, description = "Stored calendar", body = CalendarImportResponse),
        (status = 401, description = "Unauthenticated", body = ErrorEnvelope),
        (status = 403, description = "Primary owner required", body = ErrorEnvelope),
        (status = 409, description = "Version conflict", body = ErrorEnvelope),
        (status = 422, description = "Document validation failed", body = ErrorEnvelope),
        (status = 500, description = "Infrastructure failure", body = ErrorEnvelope),
        (status = 503, description = "No database", body = ErrorEnvelope)
    )
)]
pub async fn put_calendar_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(mic): Path<String>,
    Query(query): Query<ImportQuery>,
    Json(document): Json<CalendarDocument>,
) -> Result<Json<CalendarImportResponse>, CalendarApiError> {
    require_primary(&state, &auth)?;
    let pool = require_pool(&state)?;
    if !mic.trim().eq_ignore_ascii_case(document.mic.trim()) {
        return Err(validation_error(
            "the path venue code and the document venue code must match".to_string(),
        ));
    }
    let existing = get_calendar(pool, &mic).await.map_err(map_calendar_error)?;
    let next = replace_calendar(pool, document, query.expected_version, &auth.owner_id)
        .await
        .map_err(map_calendar_error)?;
    let changed = existing
        .as_ref()
        .map(|previous| previous.version != next.version)
        .unwrap_or(true);
    Ok(Json(CalendarImportResponse {
        summary: format!(
            "Imported {} version {} ({} weekdays, {} exceptions)",
            next.mic,
            next.version,
            next.weekdays.len(),
            next.exceptions.len()
        ),
        calendar: next,
        changed,
    }))
}

/// `POST /api/market-data/calendars/{mic}/retire`
#[utoipa::path(
    post,
    path = "/api/market-data/calendars/{mic}/retire",
    tag = "market-data",
    summary = "Retire an exchange calendar",
    params(("mic" = String, Path, description = "Venue code")),
    request_body = RetireBody,
    responses(
        (status = 200, description = "Retired calendar", body = CalendarRetireResponse),
        (status = 401, description = "Unauthenticated", body = ErrorEnvelope),
        (status = 403, description = "Primary owner required", body = ErrorEnvelope),
        (status = 404, description = "Unknown venue code", body = ErrorEnvelope),
        (status = 409, description = "Confirmation required", body = ErrorEnvelope),
        (status = 500, description = "Infrastructure failure", body = ErrorEnvelope),
        (status = 503, description = "No database", body = ErrorEnvelope)
    )
)]
pub async fn retire_calendar_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(mic): Path<String>,
    Json(body): Json<RetireBody>,
) -> Result<Json<CalendarRetireResponse>, CalendarApiError> {
    require_primary(&state, &auth)?;
    let pool = require_pool(&state)?;
    let in_use = affected_assets(pool, &mic, chrono::Utc::now().date_naive())
        .await
        .map_err(map_calendar_error)?;
    if in_use.affected_count > 0 && !body.confirm {
        return Err(CalendarApiError {
            status: StatusCode::CONFLICT,
            body: Json(json!({
                "error": {
                    "code": "calendar_in_use",
                    "message": format!(
                        "This calendar is assigned to {} instruments. Confirm to fall back.",
                        in_use.affected_count
                    ),
                },
                "affected_count": in_use.affected_count,
                "affected_assets": in_use.affected_assets,
            })),
        });
    }
    let calendar = retire_calendar(pool, &mic, &auth.owner_id, body.confirm)
        .await
        .map_err(map_calendar_error)?;
    Ok(Json(CalendarRetireResponse {
        calendar,
        affected_count: in_use.affected_count,
    }))
}

/// `GET /api/market-data/calendar-assignments`
#[utoipa::path(
    get,
    path = "/api/market-data/calendar-assignments",
    tag = "market-data",
    summary = "List calendar assignments",
    params(
        ("asset_id" = Option<Uuid>, Query, description = "Instrument id"),
        ("mic" = Option<String>, Query, description = "Venue code"),
        ("limit" = Option<i64>, Query, description = "Page size"),
        ("offset" = Option<i64>, Query, description = "Offset")
    ),
    responses(
        (status = 200, description = "Assignments", body = CalendarAssignmentPage),
        (status = 401, description = "Unauthenticated", body = ErrorEnvelope),
        (status = 500, description = "Infrastructure failure", body = ErrorEnvelope),
        (status = 503, description = "No database", body = ErrorEnvelope)
    )
)]
pub async fn list_assignments_handler(
    State(state): State<AppState>,
    Query(query): Query<AssignmentQuery>,
) -> Result<Json<CalendarAssignmentPage>, CalendarApiError> {
    let pool = require_pool(&state)?;
    list_assignment_views(
        pool,
        query.asset_id,
        query.mic.as_deref(),
        query.limit.unwrap_or(50),
        query.offset.unwrap_or(0),
    )
    .await
    .map(Json)
    .map_err(map_calendar_error)
}

/// `POST /api/market-data/calendar-assignments`
#[utoipa::path(
    post,
    path = "/api/market-data/calendar-assignments",
    tag = "market-data",
    summary = "Create a calendar assignment",
    request_body = AssignmentBody,
    responses(
        (status = 200, description = "Created assignment", body = obolargus_core::market_data::calendar::CalendarAssignment),
        (status = 401, description = "Unauthenticated", body = ErrorEnvelope),
        (status = 403, description = "Primary owner required", body = ErrorEnvelope),
        (status = 404, description = "Unknown venue code", body = ErrorEnvelope),
        (status = 409, description = "Overlapping assignment", body = ErrorEnvelope),
        (status = 422, description = "Invalid assignment", body = ErrorEnvelope),
        (status = 500, description = "Infrastructure failure", body = ErrorEnvelope),
        (status = 503, description = "No database", body = ErrorEnvelope)
    )
)]
pub async fn create_assignment_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Json(body): Json<AssignmentBody>,
) -> Result<Json<obolargus_core::market_data::calendar::CalendarAssignment>, CalendarApiError> {
    require_primary(&state, &auth)?;
    let pool = require_pool(&state)?;
    create_assignment(
        pool,
        body.asset_id,
        &body.mic,
        body.valid_from,
        body.valid_to,
        &auth.owner_id,
    )
    .await
    .map(Json)
    .map_err(map_calendar_error)
}

/// `DELETE /api/market-data/calendar-assignments/{id}`
#[utoipa::path(
    delete,
    path = "/api/market-data/calendar-assignments/{id}",
    tag = "market-data",
    summary = "Remove a calendar assignment",
    params(("id" = Uuid, Path, description = "Assignment id")),
    responses(
        (status = 200, description = "Removed", body = Value),
        (status = 401, description = "Unauthenticated", body = ErrorEnvelope),
        (status = 403, description = "Primary owner required", body = ErrorEnvelope),
        (status = 404, description = "Unknown assignment", body = ErrorEnvelope),
        (status = 500, description = "Infrastructure failure", body = ErrorEnvelope),
        (status = 503, description = "No database", body = ErrorEnvelope)
    )
)]
pub async fn delete_assignment_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, CalendarApiError> {
    require_primary(&state, &auth)?;
    let pool = require_pool(&state)?;
    remove_assignment(pool, id, &auth.owner_id)
        .await
        .map_err(map_calendar_error)?;
    Ok(Json(json!({"removed": true})))
}
