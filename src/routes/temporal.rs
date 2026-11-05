//! HTTP handlers for the temporal (bitemporal) surface.
//!
//! Implements the fact endpoints of
//! `specs/021-bitemporal-market-data/contracts/http-api-contract.md` §3.
//! Every handler delegates to `obolargus_core::temporal`; no behavior lives
//! here.
//!
//! Delivered: single read, append, retract, history (US1). Coverage, series,
//! and profile arrive with US2 and are intentionally absent rather than
//! stubbed, so the OpenAPI document never advertises behaviour that does not
//! exist.

#![forbid(unsafe_code)]

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::SecondsFormat;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use obolargus_core::temporal::facts::read::{
    fact_as_known, fact_at, fact_history, fact_series_page,
};
use obolargus_core::temporal::{
    FactInput, FactPoint, FactSubject, FactValue, KnownAt, RetractionInput, TemporalError,
    TemporalWindow, ValidAt, retract_fact, write_fact,
};

use crate::AppState;

/// The JSON view of a [`FactPoint`]. Timestamps are RFC 3339 UTC strings and
/// decimals/values are passed through verbatim.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FactPointBody {
    /// The fact subject kind (`asset`, `issuer`, `portfolio`).
    pub subject: String,
    /// The subject id.
    pub subject_id: Uuid,
    /// The row id.
    pub id: Uuid,
    /// The fact type.
    pub fact_type: String,
    /// The catalog schema version.
    pub schema_version: i32,
    /// Valid time, RFC 3339 UTC.
    pub valid_at: String,
    /// Transaction time, RFC 3339 UTC.
    pub known_at: String,
    /// The typed domain payload, or `null` for a retraction.
    #[schema(value_type = Option<Object>)]
    pub value: Option<serde_json::Value>,
    /// The reporting source.
    pub source: String,
    /// Whether this belief retracts the value.
    pub retracted: bool,
    /// Why the value was retracted.
    pub retraction_reason: Option<String>,
}

impl FactPointBody {
    fn from_point(point: &FactPoint) -> Self {
        Self {
            subject: point.subject.subject_name().to_string(),
            subject_id: point.subject.id(),
            id: point.id,
            fact_type: point.fact_type.clone(),
            schema_version: point.schema_version,
            valid_at: point
                .valid_at
                .as_datetime()
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
            known_at: point
                .known_at
                .as_datetime()
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
            value: point
                .value
                .as_ref()
                .and_then(|value| serde_json::to_value(value).ok()),
            source: point.source.clone(),
            retracted: point.retracted,
            retraction_reason: point.retraction_reason.clone(),
        }
    }
}

/// One field in the data dictionary.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct FactFieldBody {
    /// Field name.
    pub name: String,
    /// Logical data type.
    pub data_type: String,
    /// Whether the field may be absent.
    pub nullable: bool,
    /// Unit of measure.
    pub unit: Option<String>,
    /// Decimal precision.
    pub precision: Option<i32>,
    /// Decimal scale.
    pub scale: Option<i32>,
    /// Closed value vocabulary, when any.
    pub allowed_values: Vec<String>,
    /// Aggregates that apply to the field.
    pub aggregates: Vec<String>,
    /// Human-readable description.
    pub description: String,
}

/// One fact type in the data dictionary.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct FactTypeBody {
    /// Stable fact type key.
    pub fact_type: String,
    /// Human-readable label.
    pub display_name: String,
    /// One-sentence description.
    pub description: String,
    /// Physical storage shape.
    pub storage_kind: String,
    /// Aggregate that applies to the fact as a whole.
    pub aggregation: String,
    /// Unit of the fact as a whole.
    pub unit: Option<String>,
    /// Observation kind for observation facts.
    pub observation_kind: Option<String>,
    /// Observation interval for observation facts.
    pub interval_seconds: Option<i32>,
    /// Catalog schema version.
    pub schema_version: i32,
    /// Subject kinds this fact applies to.
    pub subjects: Vec<String>,
    /// Typed fields of the value.
    pub fields: Vec<FactFieldBody>,
    /// Sources that declare support for this fact type.
    pub supported_by_sources: Vec<String>,
}

/// The data-dictionary response.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct FactTypesResponse {
    /// Every fact type in the catalog.
    pub fact_types: Vec<FactTypeBody>,
}

/// `GET /api/domain/fact-types` — the machine-readable data dictionary.
#[utoipa::path(
    get,
    path = "/api/domain/fact-types",
    tag = "temporal",
    summary = "Read the fact catalog",
    responses(
        (status = 200, description = "Every fact type and its fields", body = FactTypesResponse),
        (status = 503, description = "No database pool is configured", body = ErrorEnvelope)
    )
)]
pub async fn list_fact_types(State(state): State<AppState>) -> Response {
    let Some(pool) = state.pool.as_ref() else {
        return series_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "infrastructure",
            "The fact catalog is unavailable without a database.",
        );
    };

    let mut fact_types = Vec::new();
    for def in obolargus_core::temporal::catalog::fact_catalog() {
        let supported_by_sources: Vec<String> = match sqlx::query_scalar(
            "SELECT source_id FROM data_source_fact_types WHERE fact_type = $1 ORDER BY source_id",
        )
        .bind(def.fact_type)
        .fetch_all(pool)
        .await
        {
            Ok(rows) => rows,
            Err(error) => {
                return error_response(&TemporalError::Infrastructure(error));
            }
        };

        fact_types.push(FactTypeBody {
            fact_type: def.fact_type.to_string(),
            display_name: def.display_name.to_string(),
            description: def.description.to_string(),
            storage_kind: def.storage_kind.as_str().to_string(),
            aggregation: def.aggregation.to_string(),
            unit: def.unit.map(str::to_string),
            observation_kind: def.observation_kind.map(str::to_string),
            interval_seconds: def.interval_seconds,
            schema_version: def.schema_version,
            subjects: def
                .subjects
                .iter()
                .map(|subject| subject.as_str().to_string())
                .collect(),
            fields: def
                .fields
                .iter()
                .map(|field| FactFieldBody {
                    name: field.name.to_string(),
                    data_type: field.data_type.to_string(),
                    nullable: field.nullable,
                    unit: field.unit.map(str::to_string),
                    precision: field.precision,
                    scale: field.scale,
                    allowed_values: field
                        .allowed_values
                        .iter()
                        .map(|value| (*value).to_string())
                        .collect(),
                    aggregates: field
                        .aggregates
                        .iter()
                        .map(|value| (*value).to_string())
                        .collect(),
                    description: field.description.to_string(),
                })
                .collect(),
            supported_by_sources,
        });
    }

    Json(FactTypesResponse { fact_types }).into_response()
}

/// The single error envelope from the contract.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorEnvelope {
    /// The wrapped error.
    pub error: ErrorBody,
}

/// The error code and human message.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorBody {
    /// Stable machine code (e.g. `no_value`).
    pub code: String,
    /// Human-readable message.
    pub message: String,
}

/// Maps a [`TemporalError`] onto the contract's status and code.
pub fn error_response(error: &TemporalError) -> Response {
    let (status, code) = match error {
        TemporalError::InvalidTime { .. } => (StatusCode::BAD_REQUEST, "invalid_time"),
        TemporalError::UnknownFactType { .. } => (StatusCode::BAD_REQUEST, "unknown_fact_type"),
        TemporalError::InvalidSubject { .. } => (StatusCode::BAD_REQUEST, "invalid_subject"),
        TemporalError::InvalidFactValue { .. } => (StatusCode::BAD_REQUEST, "invalid_fact_value"),
        TemporalError::MissingPayload { .. } => {
            (StatusCode::INTERNAL_SERVER_ERROR, "missing_payload")
        }
        TemporalError::UnsupportedInterval { .. } => {
            (StatusCode::BAD_REQUEST, "unsupported_interval")
        }
        TemporalError::UnknownSubject { .. } => (StatusCode::NOT_FOUND, "unknown_subject"),
        TemporalError::NoValue { .. } => (StatusCode::NOT_FOUND, "no_value"),
        TemporalError::OutsideRetention { .. } => (StatusCode::GONE, "outside_retention"),
        TemporalError::ConflictingAppend { .. } => (StatusCode::CONFLICT, "conflicting_append"),
        TemporalError::IngestUnavailable { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "ingest_unavailable")
        }
        TemporalError::Retention { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "retention_failure"),
        TemporalError::Infrastructure(_) => (StatusCode::INTERNAL_SERVER_ERROR, "infrastructure"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "infrastructure"),
    };
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorBody {
                code: code.to_string(),
                message: error.to_string(),
            },
        }),
    )
        .into_response()
}

fn no_value(subject: FactSubject, fact_type: &str, at: &str) -> TemporalError {
    TemporalError::NoValue {
        subject: format!("{} {fact_type}", subject.subject_name()),
        at: at.to_string(),
    }
}

fn subject_from_path(segment: &str, id: Uuid) -> Result<FactSubject, TemporalError> {
    let kind = match segment {
        "assets" => "asset",
        "issuers" => "issuer",
        "portfolios" => "portfolio",
        other => {
            return Err(TemporalError::UnknownSubject {
                subject: other.to_string(),
                id: id.to_string(),
            });
        }
    };
    FactSubject::from_parts(kind, id)
}

fn parse_valid(raw: &str) -> Result<ValidAt, TemporalError> {
    ValidAt::new(parse_instant(raw)?)
}

fn parse_known(raw: &str) -> Result<KnownAt, TemporalError> {
    KnownAt::new(parse_instant(raw)?)
}

fn parse_instant(raw: &str) -> Result<chrono::DateTime<chrono::Utc>, TemporalError> {
    if let Ok(parsed) = raw.parse::<chrono::DateTime<chrono::Utc>>() {
        return Ok(parsed);
    }
    let date = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .map_err(|error| TemporalError::invalid_time(format!("{raw:?}: {error}")))?;
    Ok(ValidAt::midnight_utc(date).as_datetime())
}

fn pool(state: &AppState) -> Result<&sqlx::PgPool, TemporalError> {
    state
        .pool
        .as_ref()
        .ok_or_else(|| TemporalError::Infrastructure(sqlx::Error::PoolClosed))
}

/// Query for the single-fact read.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct FactQuery {
    /// Instant the fact is about (RFC 3339 or `YYYY-MM-DD`).
    pub valid_at: String,
    /// Transaction-time cutoff; omit for latest knowledge.
    pub known_at: Option<String>,
}

/// `GET /api/domain/{subject}/{subject_id}/facts/{fact_type}` — one value.
#[utoipa::path(
    get,
    path = "/api/domain/{subject}/{subject_id}/facts/{fact_type}",
    tag = "temporal",
    summary = "Read one bitemporal fact",
    params(("subject" = String, Path, description = "assets | issuers | portfolios"), ("subject_id" = Uuid, Path), ("fact_type" = String, Path), FactQuery),
    responses(
        (status = 200, description = "The resolved fact", body = FactPointBody),
        (status = 400, description = "invalid_time", body = ErrorEnvelope),
        (status = 404, description = "no_value or unknown_subject", body = ErrorEnvelope),
        (status = 410, description = "outside_retention", body = ErrorEnvelope)
    )
)]
pub async fn get_fact(
    State(state): State<AppState>,
    Path((subject, subject_id, fact_type)): Path<(String, Uuid, String)>,
    Query(query): Query<FactQuery>,
) -> Response {
    let result = async {
        let subject = subject_from_path(&subject, subject_id)?;
        let valid_at = parse_valid(&query.valid_at)?;
        match &query.known_at {
            Some(raw) => {
                fact_as_known(
                    pool(&state)?,
                    subject,
                    &fact_type,
                    valid_at,
                    parse_known(raw)?,
                )
                .await
            }
            None => fact_at(pool(&state)?, subject, &fact_type, valid_at).await,
        }
    }
    .await;

    match result {
        Ok(Some(point)) => Json(FactPointBody::from_point(&point)).into_response(),
        Ok(None) => {
            let subject = subject_from_path(&subject, subject_id).unwrap_or(FactSubject::Asset(
                obolargus_core::domain::id::AssetId(subject_id),
            ));
            error_response(&no_value(subject, &fact_type, &query.valid_at))
        }
        Err(error) => error_response(&error),
    }
}

/// Body for an append.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct AppendFactBody {
    /// Valid time (RFC 3339 or `YYYY-MM-DD`).
    pub valid_at: String,
    /// Transaction time (RFC 3339).
    pub known_at: String,
    /// The typed payload, validated against the catalog.
    #[schema(value_type = Object)]
    pub value: FactValue,
    /// The reporting source.
    pub source: String,
}

/// `POST /api/domain/{subject}/{subject_id}/facts/{fact_type}` — append.
#[utoipa::path(
    post,
    path = "/api/domain/{subject}/{subject_id}/facts/{fact_type}",
    tag = "temporal",
    summary = "Append a bitemporal fact revision",
    params(("subject" = String, Path), ("subject_id" = Uuid, Path), ("fact_type" = String, Path)),
    request_body = AppendFactBody,
    responses(
        (status = 201, description = "The appended belief", body = FactPointBody),
        (status = 400, description = "invalid_time", body = ErrorEnvelope),
        (status = 409, description = "conflicting_append", body = ErrorEnvelope)
    )
)]
pub async fn append_fact_handler(
    State(state): State<AppState>,
    Path((subject, subject_id, fact_type)): Path<(String, Uuid, String)>,
    Json(body): Json<AppendFactBody>,
) -> Response {
    let result = async {
        let subject = subject_from_path(&subject, subject_id)?;
        write_fact(
            pool(&state)?,
            FactInput {
                subject,
                fact_type,
                valid_at: parse_valid(&body.valid_at)?,
                known_at: parse_known(&body.known_at)?,
                source: body.source,
                value: body.value,
            },
        )
        .await
    }
    .await;

    match result {
        Ok(point) => (StatusCode::CREATED, Json(FactPointBody::from_point(&point))).into_response(),
        Err(error) => error_response(&error),
    }
}

/// Body for a retraction.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct RetractFactBody {
    /// Valid time.
    pub valid_at: String,
    /// Transaction time.
    pub known_at: String,
    /// Why the value is retracted.
    pub reason: String,
    /// The reporting source.
    pub source: String,
}

/// `POST /api/domain/{subject}/{subject_id}/facts/{fact_type}/retract`.
#[utoipa::path(
    post,
    path = "/api/domain/{subject}/{subject_id}/facts/{fact_type}/retract",
    tag = "temporal",
    summary = "Retract a bitemporal fact",
    params(("subject" = String, Path), ("subject_id" = Uuid, Path), ("fact_type" = String, Path)),
    request_body = RetractFactBody,
    responses(
        (status = 201, description = "The retraction belief", body = FactPointBody),
        (status = 409, description = "conflicting_append", body = ErrorEnvelope)
    )
)]
pub async fn retract_fact_handler(
    State(state): State<AppState>,
    Path((subject, subject_id, fact_type)): Path<(String, Uuid, String)>,
    Json(body): Json<RetractFactBody>,
) -> Response {
    let result = async {
        let subject = subject_from_path(&subject, subject_id)?;
        retract_fact(
            pool(&state)?,
            RetractionInput {
                subject,
                fact_type,
                valid_at: parse_valid(&body.valid_at)?,
                known_at: parse_known(&body.known_at)?,
                source: body.source,
                reason: body.reason,
            },
        )
        .await
    }
    .await;

    match result {
        Ok(point) => (StatusCode::CREATED, Json(FactPointBody::from_point(&point))).into_response(),
        Err(error) => error_response(&error),
    }
}

/// Query for history.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct HistoryQuery {
    /// Valid time of the fact.
    pub valid_at: String,
}

/// `GET /api/domain/{subject}/{subject_id}/facts/{fact_type}/history`.
#[utoipa::path(
    get,
    path = "/api/domain/{subject}/{subject_id}/facts/{fact_type}/history",
    tag = "temporal",
    summary = "Every revision of one bitemporal fact",
    params(("subject" = String, Path), ("subject_id" = Uuid, Path), ("fact_type" = String, Path), HistoryQuery),
    responses(
        (status = 200, description = "Newest knowledge first", body = Vec<FactPointBody>),
        (status = 404, description = "no_value", body = ErrorEnvelope)
    )
)]
pub async fn fact_history_handler(
    State(state): State<AppState>,
    Path((subject, subject_id, fact_type)): Path<(String, Uuid, String)>,
    Query(query): Query<HistoryQuery>,
) -> Response {
    let result = async {
        let subject = subject_from_path(&subject, subject_id)?;
        fact_history(
            pool(&state)?,
            subject,
            &fact_type,
            parse_valid(&query.valid_at)?,
        )
        .await
    }
    .await;

    match result {
        Ok(points) if points.is_empty() => {
            let subject = subject_from_path(&subject, subject_id).unwrap_or(FactSubject::Asset(
                obolargus_core::domain::id::AssetId(subject_id),
            ));
            error_response(&no_value(subject, &fact_type, &query.valid_at))
        }
        Ok(points) => Json(
            points
                .iter()
                .map(FactPointBody::from_point)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => error_response(&error),
    }
}

/// Query for the resolved series.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct SeriesQuery {
    /// Inclusive lower valid-time bound (RFC 3339 or `YYYY-MM-DD`).
    pub from: String,
    /// Exclusive upper valid-time bound (RFC 3339 or `YYYY-MM-DD`).
    pub to: String,
    /// Transaction-time cutoff; omit for latest knowledge.
    pub known_at: Option<String>,
    /// Page size, 1..=20000; defaults to 5000.
    pub limit: Option<u32>,
    /// Opaque continuation token from `next_cursor`.
    pub cursor: Option<String>,
}

/// Page coverage metadata.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SeriesCoverage {
    /// Number of points in this page.
    pub points: usize,
    /// First point's valid time, or `null`.
    pub first: Option<String>,
    /// Last point's valid time, or `null`.
    pub last: Option<String>,
}

/// The resolved-series response.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SeriesResponse {
    /// Subject kind (`asset`, `issuer`, `portfolio`).
    pub subject: String,
    /// Subject id.
    pub subject_id: Uuid,
    /// Fact type.
    pub fact_type: String,
    /// Echoed as-known cutoff, or `null`.
    pub known_at: Option<String>,
    /// Resolved points for this page.
    pub points: Vec<FactPointBody>,
    /// Page coverage.
    pub coverage: SeriesCoverage,
    /// Cursor for the next page, or `null` when complete.
    pub next_cursor: Option<String>,
}

fn series_error(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorBody {
                code: code.to_string(),
                message: message.into(),
            },
        }),
    )
        .into_response()
}

/// `GET /api/domain/{subject}/{subject_id}/facts/{fact_type}/series`.
#[utoipa::path(
    get,
    path = "/api/domain/{subject}/{subject_id}/facts/{fact_type}/series",
    tag = "temporal",
    summary = "Read a resolved bitemporal series",
    params(
        ("subject" = String, Path, description = "assets | issuers | portfolios"),
        ("subject_id" = Uuid, Path),
        ("fact_type" = String, Path),
        SeriesQuery
    ),
    responses(
        (status = 200, description = "Resolved points and coverage", body = SeriesResponse),
        (status = 400, description = "invalid_time, invalid_limit, or invalid_cursor", body = ErrorEnvelope),
        (status = 404, description = "unknown_subject", body = ErrorEnvelope),
        (status = 500, description = "infrastructure", body = ErrorEnvelope)
    )
)]
pub async fn fact_series_handler(
    State(state): State<AppState>,
    Path((subject, subject_id, fact_type)): Path<(String, Uuid, String)>,
    Query(query): Query<SeriesQuery>,
) -> Response {
    let subject = match subject_from_path(&subject, subject_id) {
        Ok(subject) => subject,
        Err(error) => return error_response(&error),
    };
    let limit = query.limit.unwrap_or(5_000);
    if limit == 0 || limit > 20_000 {
        return series_error(
            StatusCode::BAD_REQUEST,
            "invalid_limit",
            "limit must be between 1 and 20000.",
        );
    }
    let after = match query.cursor.as_deref() {
        Some(raw) => match parse_valid(raw) {
            Ok(instant) => Some(instant),
            Err(_) => {
                return series_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_cursor",
                    "The cursor is not a valid RFC 3339 instant.",
                );
            }
        },
        None => None,
    };
    let window = match (parse_valid(&query.from), parse_valid(&query.to)) {
        (Ok(from), Ok(to)) => match TemporalWindow::new(from, to) {
            Ok(window) => window,
            Err(error) => return error_response(&error),
        },
        (Err(error), _) | (_, Err(error)) => return error_response(&error),
    };
    let known_at = match query.known_at.as_deref() {
        Some(raw) => match parse_known(raw) {
            Ok(instant) => Some(instant),
            Err(error) => return error_response(&error),
        },
        None => None,
    };
    let Some(pool) = state.pool.as_ref() else {
        return series_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "infrastructure",
            "The series read is unavailable without a database.",
        );
    };
    match fact_series_page(pool, subject, &fact_type, window, known_at, after, limit).await {
        Ok(page) => {
            let first = page.points.first().map(|point| {
                point
                    .valid_at
                    .as_datetime()
                    .to_rfc3339_opts(SecondsFormat::AutoSi, true)
            });
            let last = page.points.last().map(|point| {
                point
                    .valid_at
                    .as_datetime()
                    .to_rfc3339_opts(SecondsFormat::AutoSi, true)
            });
            let next_cursor = if page.has_more {
                page.points.last().map(|point| {
                    point
                        .valid_at
                        .as_datetime()
                        .to_rfc3339_opts(SecondsFormat::AutoSi, true)
                })
            } else {
                None
            };
            Json(SeriesResponse {
                subject: subject.subject_name().to_string(),
                subject_id,
                fact_type,
                known_at: query.known_at,
                coverage: SeriesCoverage {
                    points: page.points.len(),
                    first,
                    last,
                },
                points: page.points.iter().map(FactPointBody::from_point).collect(),
                next_cursor,
            })
            .into_response()
        }
        Err(error) => error_response(&error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_segments_map_to_the_three_fact_tables() {
        let id = Uuid::from_u128(5);
        assert_eq!(
            subject_from_path("assets", id)
                .expect("asset")
                .subject_name(),
            "asset"
        );
        assert_eq!(
            subject_from_path("issuers", id)
                .expect("issuer")
                .subject_name(),
            "issuer"
        );
        assert_eq!(
            subject_from_path("portfolios", id)
                .expect("portfolio")
                .subject_name(),
            "portfolio"
        );
        assert!(subject_from_path("houses", id).is_err());
    }

    #[tokio::test]
    async fn error_mapping_is_the_contract_mapping() {
        let cases = [
            (
                TemporalError::invalid_time("bad"),
                StatusCode::BAD_REQUEST,
                "invalid_time",
            ),
            (
                TemporalError::NoValue {
                    subject: "asset".to_string(),
                    at: "now".to_string(),
                },
                StatusCode::NOT_FOUND,
                "no_value",
            ),
            (
                TemporalError::conflict("identity"),
                StatusCode::CONFLICT,
                "conflicting_append",
            ),
            (
                TemporalError::OutsideRetention {
                    window: "w".to_string(),
                },
                StatusCode::GONE,
                "outside_retention",
            ),
            (
                TemporalError::UnsupportedInterval {
                    requested: 1,
                    stored: vec![60],
                },
                StatusCode::BAD_REQUEST,
                "unsupported_interval",
            ),
            (
                TemporalError::UnknownSubject {
                    subject: "houses".to_string(),
                    id: "id".to_string(),
                },
                StatusCode::NOT_FOUND,
                "unknown_subject",
            ),
            (
                TemporalError::IngestUnavailable {
                    source_id: "yahoo".to_string(),
                    detail: "down".to_string(),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "ingest_unavailable",
            ),
            (
                TemporalError::Retention {
                    detail: "detach failed".to_string(),
                },
                StatusCode::INTERNAL_SERVER_ERROR,
                "retention_failure",
            ),
            (
                TemporalError::Infrastructure(sqlx::Error::RowNotFound),
                StatusCode::INTERNAL_SERVER_ERROR,
                "infrastructure",
            ),
        ];
        for (error, status, code) in cases {
            let response = error_response(&error);
            assert_eq!(response.status(), status);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("read the error body");
            let body: serde_json::Value =
                serde_json::from_slice(&bytes).expect("the envelope is JSON");
            assert_eq!(body["error"]["code"], code);
            assert_eq!(body["error"]["message"], error.to_string());
        }
    }
}
