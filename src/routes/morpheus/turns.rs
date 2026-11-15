//! Turn detail and stop routes.

use axum::Json;
use axum::extract::{Path, State};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use obolargus_core::morpheus::model::{MessageStatus, TurnStatus};
use obolargus_core::morpheus::persistence;

use super::{
    ApiError, MessageView, StepView, ToolCallView, TurnSummaryView, UiActionView, actor,
    require_enabled, require_pool,
};
use crate::AppState;

/// Full turn projection.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TurnDetail {
    /// Turn summary.
    pub turn: TurnSummaryView,
    /// Agent message.
    pub agent_message: MessageView,
    /// Steps.
    pub steps: Vec<StepView>,
    /// Tool calls.
    pub tool_calls: Vec<ToolCallView>,
    /// UI actions.
    pub ui_actions: Vec<UiActionView>,
}

/// Reads a turn.
#[utoipa::path(
    get,
    path = "/api/morpheus/turns/{turn_id}",
    params(("turn_id" = Uuid, Path, description = "Turn id")),
    responses((status = 200, body = TurnDetail), (status = 404, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn get_turn(
    State(state): State<AppState>,
    Path(turn_id): Path<Uuid>,
) -> Result<Json<TurnDetail>, ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let turn = persistence::get_owned_turn(&pool, actor(&state), turn_id).await?;
    let agent = persistence::get_turn_agent_message(&pool, turn_id).await?;
    let steps = persistence::list_steps(&pool, turn_id).await?;
    let tool_calls = persistence::list_tool_calls(&pool, turn_id).await?;
    let ui_actions = persistence::list_ui_actions(&pool, turn_id).await?;
    Ok(Json(TurnDetail {
        turn: turn.into(),
        agent_message: agent.into(),
        steps: steps.into_iter().map(Into::into).collect(),
        tool_calls: tool_calls.into_iter().map(Into::into).collect(),
        ui_actions: ui_actions.into_iter().map(Into::into).collect(),
    }))
}

/// Stop response.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StopResponse {
    /// Turn id.
    pub turn_id: Uuid,
    /// Current status.
    pub status: String,
}

/// Stops a turn idempotently.
#[utoipa::path(
    post,
    path = "/api/morpheus/turns/{turn_id}/stop",
    params(("turn_id" = Uuid, Path, description = "Turn id")),
    responses((status = 200, body = StopResponse), (status = 404, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn stop_turn(
    State(state): State<AppState>,
    Path(turn_id): Path<Uuid>,
) -> Result<Json<StopResponse>, ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let turn = persistence::get_owned_turn(&pool, actor(&state), turn_id).await?;
    if matches!(turn.status.as_str(), "completed" | "stopped" | "failed") {
        return Ok(Json(StopResponse {
            turn_id,
            status: turn.status,
        }));
    }
    state.morpheus.cancel(turn_id);
    persistence::set_message_status(
        &pool,
        turn.agent_message_id,
        MessageStatus::Stopped.as_str(),
        None,
        None,
    )
    .await?;
    persistence::set_turn_status(
        &pool,
        turn_id,
        TurnStatus::Stopped.as_str(),
        None,
        None,
        None,
    )
    .await?;
    Ok(Json(StopResponse {
        turn_id,
        status: TurnStatus::Stopped.as_str().to_string(),
    }))
}

/// Retry response.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RetryResponse {
    /// New turn.
    pub turn: TurnSummaryView,
    /// New agent message.
    pub agent_message: MessageView,
}

/// Creates a retry attempt for a failed or stopped turn.
#[utoipa::path(
    post,
    path = "/api/morpheus/turns/{turn_id}/retry",
    params(("turn_id" = Uuid, Path, description = "Turn id")),
    responses((status = 202, body = RetryResponse), (status = 409, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn retry_turn(
    State(state): State<AppState>,
    Path(turn_id): Path<Uuid>,
) -> Result<(axum::http::StatusCode, Json<RetryResponse>), ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let (turn, agent) = persistence::retry_turn(&pool, actor(&state), turn_id).await?;
    crate::morpheus::scheduler::kick(state.clone(), turn.conversation_id);
    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(RetryResponse {
            turn: turn.into(),
            agent_message: agent.into(),
        }),
    ))
}
