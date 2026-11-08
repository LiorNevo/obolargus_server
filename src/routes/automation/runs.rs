//! Run creation, detail, cancel, and retry routes.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use obolargus_core::automation::error::AutomationError;
use obolargus_core::automation::flow::document::FlowDocument;
use obolargus_core::automation::model::{NodeRunRecord, RunRecord};
use obolargus_core::automation::runtime::persistence;

use super::{ApiResult, Page, Pagination, api_error, map_error, require_enabled, require_pool};
use crate::AppState;
use crate::automation::{audit, executor};

/// Body of a manual or API run request.
#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
pub struct RunRequest {
    /// Runtime inputs.
    #[serde(default)]
    pub inputs: Value,
    /// Requested published version number.
    #[serde(default)]
    pub version_number: Option<i32>,
    /// Inject node that starts the run.
    #[serde(default)]
    pub inject_node_id: Option<String>,
    /// Required secret inputs on retry.
    #[serde(default)]
    pub secret_inputs: Option<Value>,
}

/// Run creation acknowledgement.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct RunAccepted {
    /// New run id.
    pub run_id: Uuid,
    /// Retry parent, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<Uuid>,
    /// Initial status.
    pub status: String,
    /// Flow version number, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_version_number: Option<i32>,
}

/// Run list query.
#[derive(Debug, Clone, Deserialize, utoipa::IntoParams)]
pub struct RunListQuery {
    /// Flow id filter.
    #[serde(default)]
    pub flow_id: Option<Uuid>,
    /// Status filter.
    #[serde(default)]
    pub status: Option<String>,
    /// Trigger kind filter.
    #[serde(default)]
    pub trigger_kind: Option<String>,
    /// Page size.
    #[serde(default)]
    pub limit: Option<i64>,
    /// Offset.
    #[serde(default)]
    pub offset: Option<i64>,
}

/// Run list item.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct RunSummaryView {
    /// Run id.
    pub id: Uuid,
    /// Flow id.
    pub flow_id: Uuid,
    /// Flow name.
    pub flow_name: String,
    /// Version number.
    pub flow_version_number: Option<i32>,
    /// Trigger kind.
    pub trigger_kind: String,
    /// Inject node.
    pub trigger_node_id: Option<String>,
    /// Actor.
    pub actor_id: Option<String>,
    /// Status.
    pub status: String,
    /// Queue instant.
    pub queued_at: DateTime<Utc>,
    /// Start instant.
    pub started_at: Option<DateTime<Utc>>,
    /// End instant.
    pub ended_at: Option<DateTime<Utc>>,
    /// Duration.
    pub duration_ms: Option<i64>,
    /// Message count.
    pub message_count: i64,
    /// Summary.
    pub summary: Value,
}

/// Node run projection.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct NodeRunView {
    /// Node id.
    pub node_id: String,
    /// Node type id.
    pub node_type_id: String,
    /// Node type version.
    pub node_type_version: i32,
    /// Attempt.
    pub attempt: i32,
    /// Status.
    pub status: String,
    /// Start instant.
    pub started_at: Option<DateTime<Utc>>,
    /// End instant.
    pub ended_at: Option<DateTime<Utc>>,
    /// Duration.
    pub duration_ms: Option<i64>,
    /// Messages in.
    pub messages_in: i64,
    /// Messages out.
    pub messages_out: i64,
    /// Output truncation flag.
    pub output_truncated: bool,
    /// Error.
    pub error: Option<Value>,
}

impl From<NodeRunRecord> for NodeRunView {
    fn from(value: NodeRunRecord) -> Self {
        Self {
            node_id: value.node_id,
            node_type_id: value.node_type_id,
            node_type_version: value.node_type_version,
            attempt: value.attempt,
            status: value.status,
            started_at: value.started_at,
            ended_at: value.ended_at,
            duration_ms: value.duration_ms,
            messages_in: value.messages_in,
            messages_out: value.messages_out,
            output_truncated: value.output_truncated,
            error: value.error,
        }
    }
}

/// Run detail.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct RunDetailView {
    /// Run id.
    pub id: Uuid,
    /// Flow id.
    pub flow_id: Uuid,
    /// Version id.
    pub flow_version_id: Uuid,
    /// Version number.
    pub flow_version_number: Option<i32>,
    /// Trigger kind.
    pub trigger_kind: String,
    /// Inject node.
    pub trigger_node_id: Option<String>,
    /// Actor.
    pub actor_id: Option<String>,
    /// Status.
    pub status: String,
    /// Inputs (secrets redacted).
    pub inputs: Value,
    /// Outputs.
    pub outputs: Value,
    /// Summary.
    pub summary: Value,
    /// Terminal error.
    pub error: Option<Value>,
    /// Retry parent.
    pub parent_run_id: Option<Uuid>,
    /// Queue instant.
    pub queued_at: DateTime<Utc>,
    /// Start instant.
    pub started_at: Option<DateTime<Utc>>,
    /// End instant.
    pub ended_at: Option<DateTime<Utc>>,
    /// Cancellation request instant.
    pub cancel_requested_at: Option<DateTime<Utc>>,
    /// Message count.
    pub message_count: i64,
    /// Node runs.
    pub nodes: Vec<NodeRunView>,
}

/// Node detail.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct NodeDetailView {
    /// Node id.
    pub node_id: String,
    /// Attempt.
    pub attempt: i32,
    /// Status.
    pub status: String,
    /// Input message.
    pub input_message: Option<Value>,
    /// Output messages.
    pub output_messages: Option<Value>,
    /// Output truncation flag.
    pub output_truncated: bool,
    /// Messages in.
    pub messages_in: i64,
    /// Messages out.
    pub messages_out: i64,
    /// Error.
    pub error: Option<Value>,
}

/// Node detail query.
#[derive(Debug, Clone, Deserialize, utoipa::IntoParams)]
pub struct NodeQuery {
    /// Attempt number (default latest).
    #[serde(default)]
    pub attempt: Option<i32>,
}

async fn version_number(
    pool: &sqlx::PgPool,
    version_id: Uuid,
) -> Result<Option<i32>, AutomationError> {
    Ok(persistence::get_version_by_id(pool, version_id)
        .await?
        .and_then(|version| version.version_number))
}

async fn run_detail(pool: &sqlx::PgPool, run: RunRecord) -> Result<RunDetailView, super::ApiError> {
    let nodes = persistence::list_node_runs(pool, run.id)
        .await
        .map_err(map_error)?;
    let number = version_number(pool, run.flow_version_id)
        .await
        .map_err(map_error)?;
    Ok(RunDetailView {
        id: run.id,
        flow_id: run.flow_id,
        flow_version_id: run.flow_version_id,
        flow_version_number: number,
        trigger_kind: run.trigger_kind,
        trigger_node_id: run.trigger_node_id,
        actor_id: run.actor_id,
        status: run.status,
        inputs: executor::redact(&run.inputs),
        outputs: executor::redact(&run.outputs),
        summary: run.summary,
        error: run.error,
        parent_run_id: run.parent_run_id,
        queued_at: run.queued_at,
        started_at: run.started_at,
        ended_at: run.ended_at,
        cancel_requested_at: run.cancel_requested_at,
        message_count: run.message_count,
        nodes: nodes.into_iter().map(NodeRunView::from).collect(),
    })
}

/// Creates a run against a published version.
pub async fn create_run(
    state: &AppState,
    flow_id: Uuid,
    body: &RunRequest,
    trigger_id: Option<Uuid>,
    trigger_kind: &str,
    idempotency_key: Option<&str>,
) -> Result<RunRecord, super::ApiError> {
    let pool = require_pool(state)?;
    let (flow, _) = persistence::get_flow(pool, flow_id)
        .await
        .map_err(map_error)?;
    if flow.status != "active" {
        return Err(api_error(StatusCode::CONFLICT, "conflict", "flow.archived"));
    }
    let version = match body.version_number {
        Some(number) => persistence::get_version(pool, flow_id, number)
            .await
            .map_err(map_error)?,
        None => match flow.current_published_version_id {
            Some(id) => persistence::get_version_by_id(pool, id)
                .await
                .map_err(map_error)?
                .ok_or_else(|| api_error(StatusCode::CONFLICT, "conflict", "flow.not_published"))?,
            None => {
                return Err(api_error(
                    StatusCode::CONFLICT,
                    "conflict",
                    "flow.not_published",
                ));
            }
        },
    };
    let explicit_version = body.version_number.is_some();
    if version.status != "published" && !(explicit_version && version.status == "archived") {
        return Err(api_error(
            StatusCode::CONFLICT,
            "conflict",
            "version.not_published",
        ));
    }
    if let Some(key) = idempotency_key
        && let Some(existing) = persistence::find_run_by_idempotency(pool, flow_id, trigger_id, key)
            .await
            .map_err(map_error)?
    {
        if existing.inputs == body.inputs {
            return Ok(existing);
        }
        return Err(api_error(
            StatusCode::CONFLICT,
            "conflict",
            "idempotency.payload_mismatch",
        ));
    }
    let active = persistence::count_active_runs(pool, None)
        .await
        .map_err(map_error)?;
    if active >= i64::from(state.config.automation_max_concurrent_runs) {
        return Err(api_error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "run.concurrency_limit",
        ));
    }
    let flow_active = persistence::count_active_runs(pool, Some(flow_id))
        .await
        .map_err(map_error)?;
    if flow_active >= i64::from(state.config.automation_max_concurrent_runs_per_flow) {
        return Err(api_error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "run.flow_concurrency_limit",
        ));
    }
    if trigger_kind == "api"
        && let Some(trigger_id) = trigger_id
    {
        let since = Utc::now() - chrono::Duration::minutes(1);
        let recent = persistence::count_trigger_runs_since(pool, trigger_id, since)
            .await
            .map_err(map_error)?;
        if recent >= i64::from(state.config.automation_api_trigger_rate_limit_per_minute) {
            return Err(api_error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "trigger.rate_limited",
            ));
        }
    }
    let document: FlowDocument = serde_json::from_value(version.flow_json).map_err(|error| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            error.to_string(),
        )
    })?;
    let mut inputs = if body.inputs.is_null() {
        json!({})
    } else {
        body.inputs.clone()
    };
    if let Some(secret_inputs) = body.secret_inputs.clone()
        && let (Some(target), Some(source)) = (inputs.as_object_mut(), secret_inputs.as_object())
    {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
    let inputs_size = serde_json::to_vec(&inputs)
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    if inputs_size > state.config.automation_max_input_bytes {
        return Err(api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "run.input_too_large",
        ));
    }
    for input in &document.inputs {
        if input.required && inputs.get(&input.name).is_none() && !input.secret {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                "validation_error",
                format!("run.input_missing: {}", input.name),
            ));
        }
    }
    let run = persistence::insert_run(
        pool,
        flow_id,
        version.id,
        trigger_id,
        trigger_kind,
        body.inject_node_id.as_deref(),
        Some(&state.config.automation_actor_id),
        &inputs,
        idempotency_key,
        None,
    )
    .await
    .map_err(map_error)?;
    Ok(run)
}

/// `POST /api/automation/flows/{flow_id}/runs`
#[utoipa::path(
    post,
    path = "/api/automation/flows/{flow_id}/runs",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    request_body = RunRequest,
    responses(
        (status = 202, body = RunAccepted),
        (status = 400, body = super::ApiErrorBody),
        (status = 409, body = super::ApiErrorBody)
    )
)]
pub async fn start_run(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<RunRequest>,
) -> Result<(StatusCode, Json<RunAccepted>), super::ApiError> {
    require_enabled(&state)?;
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok());
    let run = create_run(&state, flow_id, &body, None, "manual", key).await?;
    let number = {
        let pool = require_pool(&state)?;
        version_number(pool, run.flow_version_id)
            .await
            .map_err(map_error)?
    };
    audit::record_best_effort(
        &state,
        "run.start",
        "run",
        run.id,
        Some(flow_id),
        Some(run.id),
        json!({ "trigger_kind": "manual" }),
    )
    .await;
    executor::spawn(state.clone(), run.id);
    Ok((
        StatusCode::ACCEPTED,
        Json(RunAccepted {
            run_id: run.id,
            parent_run_id: run.parent_run_id,
            status: run.status,
            flow_version_number: number,
        }),
    ))
}

/// `GET /api/automation/runs`
#[utoipa::path(
    get,
    path = "/api/automation/runs",
    tag = "automation",
    params(RunListQuery),
    responses((status = 200, body = Page<RunSummaryView>))
)]
pub async fn list_runs(
    State(state): State<AppState>,
    Query(query): Query<RunListQuery>,
) -> ApiResult<Page<RunSummaryView>> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let (limit, offset) = Pagination {
        limit: query.limit,
        offset: query.offset,
    }
    .bounds();
    let runs = persistence::list_runs(
        pool,
        query.flow_id,
        query.status.as_deref(),
        query.trigger_kind.as_deref(),
        limit,
        offset,
    )
    .await
    .map_err(map_error)?;
    let total = persistence::count_runs(
        pool,
        query.flow_id,
        query.status.as_deref(),
        query.trigger_kind.as_deref(),
    )
    .await
    .map_err(map_error)?;
    let mut items = Vec::with_capacity(runs.len());
    for run in runs {
        let flow_name = persistence::get_flow(pool, run.flow_id)
            .await
            .map(|(flow, _)| flow.name)
            .unwrap_or_default();
        let duration_ms = match (run.started_at, run.ended_at) {
            (Some(start), Some(end)) => Some((end - start).num_milliseconds()),
            _ => None,
        };
        items.push(RunSummaryView {
            id: run.id,
            flow_id: run.flow_id,
            flow_name,
            flow_version_number: version_number(pool, run.flow_version_id)
                .await
                .map_err(map_error)?,
            trigger_kind: run.trigger_kind,
            trigger_node_id: run.trigger_node_id,
            actor_id: run.actor_id,
            status: run.status,
            queued_at: run.queued_at,
            started_at: run.started_at,
            ended_at: run.ended_at,
            duration_ms,
            message_count: run.message_count,
            summary: run.summary,
        });
    }
    Ok(Json(Page::new(items, total, limit, offset)))
}

/// `GET /api/automation/runs/{run_id}`
#[utoipa::path(
    get,
    path = "/api/automation/runs/{run_id}",
    tag = "automation",
    params(("run_id" = Uuid, Path, description = "Run id")),
    responses((status = 200, body = RunDetailView), (status = 404, body = super::ApiErrorBody))
)]
pub async fn get_run(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
) -> ApiResult<RunDetailView> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let run = persistence::get_run(pool, run_id)
        .await
        .map_err(map_error)?;
    Ok(Json(run_detail(pool, run).await?))
}

/// `GET /api/automation/runs/{run_id}/nodes/{node_id}`
#[utoipa::path(
    get,
    path = "/api/automation/runs/{run_id}/nodes/{node_id}",
    tag = "automation",
    params(
        ("run_id" = Uuid, Path, description = "Run id"),
        ("node_id" = String, Path, description = "Node id"),
        NodeQuery
    ),
    responses((status = 200, body = NodeDetailView), (status = 404, body = super::ApiErrorBody))
)]
pub async fn get_node(
    State(state): State<AppState>,
    Path((run_id, node_id)): Path<(Uuid, String)>,
    Query(query): Query<NodeQuery>,
) -> ApiResult<NodeDetailView> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let rows = persistence::list_node_runs(pool, run_id)
        .await
        .map_err(map_error)?;
    let row = rows
        .into_iter()
        .filter(|row| row.node_id == node_id)
        .filter(|row| {
            query
                .attempt
                .map(|attempt| row.attempt == attempt)
                .unwrap_or(true)
        })
        .max_by_key(|row| row.attempt)
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("node {node_id}"),
            )
        })?;
    Ok(Json(NodeDetailView {
        node_id: row.node_id,
        attempt: row.attempt,
        status: row.status,
        input_message: row.input_message,
        output_messages: row.output_messages,
        output_truncated: row.output_truncated,
        messages_in: row.messages_in,
        messages_out: row.messages_out,
        error: row.error,
    }))
}

/// Cancellation acknowledgement.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct CancelResponse {
    /// Run id.
    pub id: Uuid,
    /// Current status.
    pub status: String,
    /// Cancellation request instant.
    pub cancel_requested_at: Option<DateTime<Utc>>,
}

/// `POST /api/automation/runs/{run_id}/cancel`
#[utoipa::path(
    post,
    path = "/api/automation/runs/{run_id}/cancel",
    tag = "automation",
    params(("run_id" = Uuid, Path, description = "Run id")),
    responses((status = 200, body = CancelResponse), (status = 409, body = super::ApiErrorBody))
)]
pub async fn cancel_run(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
) -> ApiResult<CancelResponse> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let run = persistence::request_cancel(pool, run_id)
        .await
        .map_err(map_error)?;
    state.automation.cancel(run_id);
    audit::record_best_effort(
        &state,
        "run.cancel",
        "run",
        run_id,
        Some(run.flow_id),
        Some(run_id),
        json!({}),
    )
    .await;
    Ok(Json(CancelResponse {
        id: run.id,
        status: run.status,
        cancel_requested_at: run.cancel_requested_at,
    }))
}

/// `POST /api/automation/runs/{run_id}/retry`
#[utoipa::path(
    post,
    path = "/api/automation/runs/{run_id}/retry",
    tag = "automation",
    params(("run_id" = Uuid, Path, description = "Run id")),
    request_body = RunRequest,
    responses((status = 202, body = RunAccepted), (status = 409, body = super::ApiErrorBody))
)]
pub async fn retry_run(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    Json(body): Json<RunRequest>,
) -> Result<(StatusCode, Json<RunAccepted>), super::ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let original = persistence::get_run(pool, run_id)
        .await
        .map_err(map_error)?;
    if !matches!(
        original.status.as_str(),
        "succeeded" | "partial" | "failed" | "cancelled" | "interrupted"
    ) {
        return Err(api_error(
            StatusCode::CONFLICT,
            "conflict",
            "run.not_terminal",
        ));
    }
    let mut inputs = original.inputs.clone();
    if let Some(secret_inputs) = body.secret_inputs.clone()
        && let (Some(target), Some(source)) = (inputs.as_object_mut(), secret_inputs.as_object())
    {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
    let retry_version_number = persistence::get_version_by_id(pool, original.flow_version_id)
        .await
        .map_err(map_error)?
        .and_then(|version| version.version_number);
    let retry = RunRequest {
        inputs,
        version_number: retry_version_number,
        inject_node_id: original.trigger_node_id.clone(),
        secret_inputs: None,
    };
    let run = create_run(
        &state,
        original.flow_id,
        &retry,
        original.trigger_id,
        &original.trigger_kind,
        None,
    )
    .await?;
    let parent = persistence::get_run(pool, run.id)
        .await
        .map_err(map_error)?;
    let _ = persistence::set_run_parent(pool, run.id, Some(run_id)).await;
    let number = version_number(pool, parent.flow_version_id)
        .await
        .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "run.retry",
        "run",
        run.id,
        Some(original.flow_id),
        Some(run.id),
        json!({ "parent_run_id": run_id }),
    )
    .await;
    executor::spawn(state.clone(), run.id);
    Ok((
        StatusCode::ACCEPTED,
        Json(RunAccepted {
            run_id: run.id,
            parent_run_id: Some(run_id),
            status: "queued".to_string(),
            flow_version_number: number,
        }),
    ))
}
