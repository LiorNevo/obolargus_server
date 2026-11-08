//! Run message and log routes.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use obolargus_core::automation::model::{LogRecord, MessageRecord};
use obolargus_core::automation::runtime::persistence;

use super::{ApiResult, api_error, map_error, require_enabled, require_pool};
use crate::AppState;

/// Message list query.
#[derive(Debug, Clone, Deserialize, utoipa::IntoParams)]
pub struct MessageQuery {
    /// Node filter.
    #[serde(default)]
    pub node_id: Option<String>,
    /// Direction filter (`in`/`out`).
    #[serde(default)]
    pub direction: Option<String>,
    /// Cursor.
    #[serde(default)]
    pub after_id: Option<i64>,
    /// Page size (default 200, max 1000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Message view.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct MessageView {
    /// Row id.
    pub id: i64,
    /// Node id.
    pub node_id: Option<String>,
    /// Direction.
    pub direction: String,
    /// Output port.
    pub output_port: Option<String>,
    /// Runtime message id.
    pub msg_id: String,
    /// Timestamp.
    pub ts: DateTime<Utc>,
    /// Size in bytes.
    pub size_bytes: i32,
    /// Truncation flag.
    pub truncated: bool,
    /// Payload (omitted when truncated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

impl From<MessageRecord> for MessageView {
    fn from(value: MessageRecord) -> Self {
        Self {
            id: value.id,
            node_id: value.node_id,
            direction: value.direction,
            output_port: value.output_port,
            msg_id: value.msg_id,
            ts: value.ts,
            size_bytes: value.size_bytes,
            truncated: value.truncated,
            payload: if value.truncated { None } else { value.payload },
        }
    }
}

/// Message list response.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct MessageList {
    /// Messages.
    pub items: Vec<MessageView>,
}

/// `GET /api/automation/runs/{run_id}/messages`
#[utoipa::path(
    get,
    path = "/api/automation/runs/{run_id}/messages",
    tag = "automation",
    params(("run_id" = Uuid, Path, description = "Run id"), MessageQuery),
    responses((status = 200, body = MessageList))
)]
pub async fn list_messages(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    Query(query): Query<MessageQuery>,
) -> ApiResult<MessageList> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::get_run(pool, run_id)
        .await
        .map_err(map_error)?;
    let limit = query.limit.unwrap_or(200).clamp(1, 1_000);
    let items = persistence::list_messages(
        pool,
        run_id,
        query.node_id.as_deref(),
        query.direction.as_deref(),
        query.after_id,
        limit,
    )
    .await
    .map_err(map_error)?;
    Ok(Json(MessageList {
        items: items.into_iter().map(MessageView::from).collect(),
    }))
}

/// Log list query.
#[derive(Debug, Clone, Deserialize, utoipa::IntoParams)]
pub struct LogQuery {
    /// Node filter.
    #[serde(default)]
    pub node_id: Option<String>,
    /// Cursor.
    #[serde(default)]
    pub after_id: Option<i64>,
    /// Page size (default 200, max 1000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Log view.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct LogView {
    /// Row id.
    pub id: i64,
    /// Node id.
    pub node_id: Option<String>,
    /// Timestamp.
    pub ts: DateTime<Utc>,
    /// Level.
    pub level: String,
    /// Message.
    pub message: String,
    /// Structured fields.
    pub fields: Option<Value>,
}

impl From<LogRecord> for LogView {
    fn from(value: LogRecord) -> Self {
        Self {
            id: value.id,
            node_id: value.node_id,
            ts: value.ts,
            level: value.level,
            message: value.message,
            fields: value.fields,
        }
    }
}

/// Log list response.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct LogList {
    /// Logs.
    pub items: Vec<LogView>,
}

/// `GET /api/automation/runs/{run_id}/logs`
#[utoipa::path(
    get,
    path = "/api/automation/runs/{run_id}/logs",
    tag = "automation",
    params(("run_id" = Uuid, Path, description = "Run id"), LogQuery),
    responses((status = 200, body = LogList))
)]
pub async fn list_logs(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    Query(query): Query<LogQuery>,
) -> ApiResult<LogList> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::get_run(pool, run_id)
        .await
        .map_err(map_error)?;
    let limit = query.limit.unwrap_or(200).clamp(1, 1_000);
    let items = persistence::list_logs(
        pool,
        run_id,
        query.node_id.as_deref(),
        query.after_id,
        limit,
    )
    .await
    .map_err(map_error)?;
    Ok(Json(LogList {
        items: items.into_iter().map(LogView::from).collect(),
    }))
}

/// Ensures a run exists, returning the contract 404 otherwise.
pub async fn ensure_run(state: &AppState, run_id: Uuid) -> Result<(), super::ApiError> {
    let pool = require_pool(state)?;
    persistence::get_run(pool, run_id)
        .await
        .map(|_| ())
        .map_err(|_| api_error(StatusCode::NOT_FOUND, "not_found", format!("run {run_id}")))
}
