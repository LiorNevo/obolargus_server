//! Trigger CRUD and API-trigger invocation routes.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use obolargus_core::automation::model::TriggerRecord;
use obolargus_core::automation::runtime::persistence;

use super::runs::{RunAccepted, RunRequest, create_run};
use super::{ApiResult, api_error, map_error, require_enabled, require_pool};
use crate::AppState;
use crate::automation::{audit, executor};

/// Trigger projection.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct TriggerView {
    /// Trigger id.
    pub id: Uuid,
    /// Trigger kind.
    pub kind: String,
    /// Inject node id.
    pub node_id: Option<String>,
    /// Enabled state.
    pub enabled: bool,
    /// Repeat kind.
    pub repeat_kind: Option<String>,
    /// Cron expression.
    pub cron_expression: Option<String>,
    /// Interval seconds.
    pub interval_seconds: Option<i32>,
    /// Timezone.
    pub timezone: String,
    /// Overlap policy.
    pub overlap_policy: String,
    /// Missed policy.
    pub missed_policy: String,
    /// Next occurrence.
    pub next_occurrence_at: Option<DateTime<Utc>>,
    /// Last occurrence.
    pub last_occurrence_at: Option<DateTime<Utc>>,
}

impl From<TriggerRecord> for TriggerView {
    fn from(value: TriggerRecord) -> Self {
        Self {
            id: value.id,
            kind: value.kind,
            node_id: value.node_id,
            enabled: value.enabled,
            repeat_kind: value.repeat_kind,
            cron_expression: value.cron_expression,
            interval_seconds: value.interval_seconds,
            timezone: value.timezone,
            overlap_policy: value.overlap_policy,
            missed_policy: value.missed_policy,
            next_occurrence_at: value.next_occurrence_at,
            last_occurrence_at: value.last_occurrence_at,
        }
    }
}

/// Trigger list response.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct TriggerList {
    /// Triggers.
    pub items: Vec<TriggerView>,
}

/// Body of `POST /flows/{flow_id}/triggers`.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct CreateTriggerRequest {
    /// Trigger kind; only `api` may be created manually.
    pub kind: String,
    /// Enabled state.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Overlap policy.
    #[serde(default = "default_skip")]
    pub overlap_policy: String,
    /// Missed policy.
    #[serde(default = "default_run_once")]
    pub missed_policy: String,
}

fn default_true() -> bool {
    true
}
fn default_skip() -> String {
    "skip".to_string()
}
fn default_run_once() -> String {
    "run_once".to_string()
}

/// Body of `PATCH /triggers/{trigger_id}`.
#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
pub struct UpdateTriggerRequest {
    /// Enabled state.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Overlap policy.
    #[serde(default)]
    pub overlap_policy: Option<String>,
    /// Missed policy.
    #[serde(default)]
    pub missed_policy: Option<String>,
}

fn valid_policy(value: &str) -> bool {
    matches!(value, "skip" | "queue" | "cancel_previous")
}

fn valid_missed(value: &str) -> bool {
    matches!(value, "run_once" | "skip")
}

/// `GET /api/automation/flows/{flow_id}/triggers`
#[utoipa::path(
    get,
    path = "/api/automation/flows/{flow_id}/triggers",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    responses((status = 200, body = TriggerList))
)]
pub async fn list_triggers(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
) -> ApiResult<TriggerList> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let items = persistence::list_triggers(pool, flow_id)
        .await
        .map_err(map_error)?;
    Ok(Json(TriggerList {
        items: items.into_iter().map(TriggerView::from).collect(),
    }))
}

/// `POST /api/automation/flows/{flow_id}/triggers`
#[utoipa::path(
    post,
    path = "/api/automation/flows/{flow_id}/triggers",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    request_body = CreateTriggerRequest,
    responses(
        (status = 201, body = TriggerView),
        (status = 400, body = super::ApiErrorBody)
    )
)]
pub async fn create_trigger(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    Json(body): Json<CreateTriggerRequest>,
) -> Result<(StatusCode, Json<TriggerView>), super::ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::get_flow(pool, flow_id)
        .await
        .map_err(map_error)?;
    if body.kind != "api" {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "trigger.inject_managed_by_publish",
        ));
    }
    if !valid_policy(&body.overlap_policy) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "trigger.policy_invalid",
        ));
    }
    if !valid_missed(&body.missed_policy) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "trigger.missed_policy_invalid",
        ));
    }
    let trigger = persistence::upsert_api_trigger(
        pool,
        flow_id,
        body.enabled,
        &body.overlap_policy,
        &body.missed_policy,
        &state.config.automation_actor_id,
    )
    .await
    .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "trigger.create",
        "trigger",
        trigger.id,
        Some(flow_id),
        None,
        json!({ "kind": "api" }),
    )
    .await;
    Ok((StatusCode::CREATED, Json(trigger.into())))
}

/// `PATCH /api/automation/triggers/{trigger_id}`
#[utoipa::path(
    patch,
    path = "/api/automation/triggers/{trigger_id}",
    tag = "automation",
    params(("trigger_id" = Uuid, Path, description = "Trigger id")),
    request_body = UpdateTriggerRequest,
    responses((status = 200, body = TriggerView), (status = 400, body = super::ApiErrorBody))
)]
pub async fn update_trigger(
    State(state): State<AppState>,
    Path(trigger_id): Path<Uuid>,
    Json(body): Json<UpdateTriggerRequest>,
) -> ApiResult<TriggerView> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    if let Some(policy) = body.overlap_policy.as_deref()
        && !valid_policy(policy)
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "trigger.policy_invalid",
        ));
    }
    if let Some(policy) = body.missed_policy.as_deref()
        && !valid_missed(policy)
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "trigger.missed_policy_invalid",
        ));
    }
    let trigger = persistence::update_trigger(
        pool,
        trigger_id,
        body.enabled,
        body.overlap_policy.as_deref(),
        body.missed_policy.as_deref(),
    )
    .await
    .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "trigger.update",
        "trigger",
        trigger.id,
        Some(trigger.flow_id),
        None,
        json!({}),
    )
    .await;
    Ok(Json(trigger.into()))
}

/// `DELETE /api/automation/triggers/{trigger_id}`
#[utoipa::path(
    delete,
    path = "/api/automation/triggers/{trigger_id}",
    tag = "automation",
    params(("trigger_id" = Uuid, Path, description = "Trigger id")),
    responses((status = 204), (status = 404, body = super::ApiErrorBody))
)]
pub async fn delete_trigger(
    State(state): State<AppState>,
    Path(trigger_id): Path<Uuid>,
) -> Result<StatusCode, super::ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let trigger = persistence::get_trigger(pool, trigger_id)
        .await
        .map_err(map_error)?;
    persistence::delete_trigger(pool, trigger_id)
        .await
        .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "trigger.delete",
        "trigger",
        trigger_id,
        Some(trigger.flow_id),
        None,
        json!({}),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/automation/triggers/{trigger_id}/invoke`
#[utoipa::path(
    post,
    path = "/api/automation/triggers/{trigger_id}/invoke",
    tag = "automation",
    params(("trigger_id" = Uuid, Path, description = "Trigger id")),
    request_body = RunRequest,
    responses(
        (status = 202, body = RunAccepted),
        (status = 409, body = super::ApiErrorBody),
        (status = 429, body = super::ApiErrorBody)
    )
)]
pub async fn invoke_trigger(
    State(state): State<AppState>,
    Path(trigger_id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<RunRequest>,
) -> Result<(StatusCode, Json<RunAccepted>), super::ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let trigger = persistence::get_trigger(pool, trigger_id)
        .await
        .map_err(map_error)?;
    if trigger.kind != "api" {
        return Err(api_error(
            StatusCode::CONFLICT,
            "conflict",
            "trigger.not_api",
        ));
    }
    if !trigger.enabled {
        return Err(api_error(
            StatusCode::CONFLICT,
            "conflict",
            "trigger.disabled",
        ));
    }
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok());
    let run = create_run(&state, trigger.flow_id, &body, Some(trigger.id), "api", key).await?;
    let number = persistence::get_version_by_id(pool, run.flow_version_id)
        .await
        .map_err(map_error)?
        .and_then(|version| version.version_number);
    audit::record_best_effort(
        &state,
        "trigger.invoke",
        "trigger",
        trigger.id,
        Some(trigger.flow_id),
        Some(run.id),
        json!({}),
    )
    .await;
    executor::spawn(state.clone(), run.id);
    Ok((
        StatusCode::ACCEPTED,
        Json(RunAccepted {
            run_id: run.id,
            parent_run_id: None,
            status: "queued".to_string(),
            flow_version_number: number,
        }),
    ))
}

/// Placeholder for future trigger metadata; keeps the JSON shape explicit.
pub fn _trigger_metadata(trigger: &TriggerRecord) -> Value {
    json!({ "id": trigger.id, "kind": trigger.kind })
}
