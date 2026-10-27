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
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use obolargus_core::temporal::facts::read::{fact_as_known, fact_at, fact_history};
use obolargus_core::temporal::facts::write::{append_fact, revoke_fact};
use obolargus_core::temporal::{FactPoint, FactSubject, KnownAt, TemporalError, ValidAt};

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
    /// Valid time, RFC 3339 UTC.
    pub valid_at: String,
    /// Transaction time, RFC 3339 UTC.
    pub known_at: String,
    /// The domain payload.
    pub value: String,
    /// The reporting source.
    pub source: String,
    /// Whether this belief retracts the value.
    pub retracted: bool,
}

impl FactPointBody {
    fn from_point(point: &FactPoint) -> Self {
        Self {
            subject: point.subject.subject_name().to_string(),
            subject_id: point.subject.id(),
            id: point.id,
            fact_type: point.fact_type.clone(),
            valid_at: point.valid_at.to_string(),
            known_at: point.known_at.to_string(),
            value: point.value.clone(),
            source: point.source.clone(),
            retracted: point.retracted,
        }
    }
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
    /// The payload, stored verbatim.
    pub value: String,
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
        append_fact(
            pool(&state)?,
            subject,
            &fact_type,
            parse_valid(&body.valid_at)?,
            parse_known(&body.known_at)?,
            &body.value,
            &body.source,
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
        revoke_fact(
            pool(&state)?,
            subject,
            &fact_type,
            parse_valid(&body.valid_at)?,
            parse_known(&body.known_at)?,
            &body.reason,
            &body.source,
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
