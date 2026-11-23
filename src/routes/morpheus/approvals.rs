//! Approval decisions and standing grants.

use axum::Json;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use obolargus_core::morpheus::approvals::{ApprovalDecision, tool_grants};
use obolargus_core::morpheus::error::MorpheusError;
use obolargus_core::morpheus::model::{ToolCallStatus, TurnStatus};
use obolargus_core::morpheus::persistence;

use super::{ApiError, ApiResult, ApprovalView, require_enabled, require_pool};
use crate::AppState;
use crate::auth::AuthUser;
use crate::morpheus::engine;

/// Decision body.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct DecisionRequest {
    /// `approve`, `deny`, `approve_all_turn`, or `approve_all_remember`.
    pub decision: String,
}

/// Standing approval projection.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StandingApprovalView {
    /// Id.
    pub id: Uuid,
    /// Granted tool ids.
    pub tool_ids: Vec<String>,
    /// Covered interface action kinds.
    pub ui_action_kinds: Vec<String>,
    /// Grant instant.
    pub granted_at: DateTime<Utc>,
    /// Revocation instant.
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Standing response.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StandingResponse {
    /// Active grant.
    pub active: Option<StandingApprovalView>,
    /// Revoked history.
    pub history: Vec<StandingApprovalView>,
}

/// Decision response.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DecisionResponse {
    /// Updated approval.
    pub approval: ApprovalView,
    /// Created or replaced standing grant.
    pub standing_approval: Option<StandingApprovalView>,
}

/// Applies a decision and resumes the turn when no decisions remain.
#[utoipa::path(
    post,
    path = "/api/morpheus/approvals/{approval_id}",
    request_body = DecisionRequest,
    params(("approval_id" = Uuid, Path, description = "Approval id")),
    responses(
        (status = 200, body = DecisionResponse),
        (status = 400, body = super::ErrorEnvelope),
        (status = 409, body = super::ErrorEnvelope)
    ),
    tag = "morpheus"
)]
pub async fn decide(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(approval_id): Path<Uuid>,
    Json(body): Json<DecisionRequest>,
) -> ApiResult<DecisionResponse> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let owner = &auth.owner_id;
    let decision = ApprovalDecision::parse(&body.decision)
        .ok_or_else(|| MorpheusError::validation("Unknown decision."))?;
    if matches!(
        decision,
        ApprovalDecision::ApproveAllTurn | ApprovalDecision::ApproveAllRemember
    ) && !state.config.morpheus_standing_approvals_enabled
    {
        return Err(MorpheusError::validation(
            "Standing approvals are disabled for this deployment.",
        )
        .into());
    }
    let approval = persistence::get_owned_approval(&pool, owner, approval_id).await?;
    if approval.status != "pending" {
        return Ok(Json(DecisionResponse {
            approval: approval.into(),
            standing_approval: None,
        }));
    }
    if chrono::Utc::now() >= approval.expires_at {
        persistence::decide_approval(&pool, approval_id, "expired", "individual").await?;
        persistence::set_tool_call_status(
            &pool,
            approval.action_id,
            ToolCallStatus::Expired.as_str(),
        )
        .await?;
        resume_if_ready(&state, approval.turn_id).await?;
        return Err(MorpheusError::Approval("The approval request expired.".to_string()).into());
    }

    let (status, mode) = match decision {
        ApprovalDecision::Deny => ("denied", "individual"),
        ApprovalDecision::Approve => ("approved", "individual"),
        ApprovalDecision::ApproveAllTurn => ("approved", "turn"),
        ApprovalDecision::ApproveAllRemember => ("approved", "standing"),
    };
    persistence::decide_approval(&pool, approval_id, status, mode).await?;
    persistence::set_tool_call_status(
        &pool,
        approval.action_id,
        if decision.approves() {
            ToolCallStatus::Approved.as_str()
        } else {
            ToolCallStatus::Denied.as_str()
        },
    )
    .await?;

    let mut standing_view = None;
    match decision {
        ApprovalDecision::ApproveAllTurn => {
            persistence::set_approve_all_turn(&pool, approval.turn_id, true).await?;
            persistence::approve_pending_turn_approvals(&pool, approval.turn_id, "turn").await?;
            persistence::approve_pending_turn_tools(&pool, approval.turn_id).await?;
        }
        ApprovalDecision::ApproveAllRemember => {
            let grants: Vec<(String, String)> = state
                .morpheus
                .catalog
                .specs()
                .iter()
                .map(|spec| (spec.id.to_string(), spec.spec_hash()))
                .collect();
            let kinds: Vec<String> = [
                "navigate", "click", "type", "select", "press", "scroll", "submit",
            ]
            .into_iter()
            .map(str::to_string)
            .collect();
            let standing =
                persistence::upsert_standing(&pool, owner, tool_grants(grants), &kinds).await?;
            persistence::approve_pending_turn_approvals(&pool, approval.turn_id, "standing")
                .await?;
            persistence::approve_pending_turn_tools(&pool, approval.turn_id).await?;
            standing_view = Some(standing_view_from(&standing));
        }
        _ => {}
    }

    resume_if_ready(&state, approval.turn_id).await?;
    let updated = persistence::get_owned_approval(&pool, owner, approval_id).await?;
    Ok(Json(DecisionResponse {
        approval: updated.into(),
        standing_approval: standing_view,
    }))
}

async fn resume_if_ready(state: &AppState, turn_id: Uuid) -> Result<(), MorpheusError> {
    let Some(pool) = state.pool.as_ref() else {
        return Ok(());
    };
    let pending = persistence::list_pending_approvals(pool, turn_id).await?;
    if pending.is_empty() {
        persistence::set_turn_status(
            pool,
            turn_id,
            TurnStatus::Running.as_str(),
            None,
            None,
            None,
        )
        .await?;
        engine::resume(state.clone(), turn_id);
    }
    Ok(())
}

/// Lists the active and revoked standing grants.
#[utoipa::path(
    get,
    path = "/api/morpheus/approvals/standing",
    responses((status = 200, body = StandingResponse), (status = 503, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn list_standing(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
) -> ApiResult<StandingResponse> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let owner = &auth.owner_id;
    let active = persistence::get_active_standing(&pool, owner).await?;
    let history = persistence::list_standing_history(&pool, owner).await?;
    Ok(Json(StandingResponse {
        active: active.map(|value| standing_view_from(&value)),
        history: history.iter().map(standing_view_from).collect(),
    }))
}

/// Revokes a standing grant.
#[utoipa::path(
    delete,
    path = "/api/morpheus/approvals/standing/{approval_id}",
    params(("approval_id" = Uuid, Path, description = "Standing approval id")),
    responses((status = 204), (status = 404, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn revoke_standing(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(approval_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::revoke_standing(&pool, &auth.owner_id, approval_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn standing_view_from(
    record: &obolargus_core::morpheus::model::StandingApprovalRecord,
) -> StandingApprovalView {
    let mut tool_ids: Vec<String> = record
        .tool_grants
        .as_object()
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default();
    tool_ids.sort();
    StandingApprovalView {
        id: record.id,
        tool_ids,
        ui_action_kinds: record.ui_action_kinds.clone(),
        granted_at: record.granted_at,
        revoked_at: record.revoked_at,
    }
}
