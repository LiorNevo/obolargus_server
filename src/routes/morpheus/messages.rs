//! Message enqueue route.

use axum::Json;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use obolargus_core::morpheus::error::MorpheusError;
use obolargus_core::morpheus::persistence;

use super::{ApiError, MessageView, TurnSummaryView, require_enabled, require_pool};
use crate::AppState;
use crate::auth::AuthUser;
use crate::morpheus::scheduler;

/// Send-message body.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct SendMessageRequest {
    /// User content.
    pub content: String,
    /// Client-generated idempotency id.
    pub client_message_id: String,
}

/// Accepted send response.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SendMessageResponse {
    /// Persisted user message.
    pub message: MessageView,
    /// Created turn.
    pub turn: TurnSummaryView,
    /// Position when queued, otherwise 0.
    pub queue_position: i64,
}

/// Enqueues a user message and starts the turn when possible.
#[utoipa::path(
    post,
    path = "/api/morpheus/conversations/{conversation_id}/messages",
    request_body = SendMessageRequest,
    params(("conversation_id" = Uuid, Path, description = "Conversation id")),
    responses(
        (status = 202, body = SendMessageResponse),
        (status = 400, body = super::ErrorEnvelope),
        (status = 409, body = super::ErrorEnvelope),
        (status = 413, body = super::ErrorEnvelope)
    ),
    tag = "morpheus"
)]
pub async fn send_message(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(conversation_id): Path<Uuid>,
    Json(body): Json<SendMessageRequest>,
) -> Result<(StatusCode, Json<SendMessageResponse>), ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let owner = &auth.owner_id;

    let content = body.content.trim();
    if content.is_empty() {
        return Err(MorpheusError::validation("A message is required.").into());
    }
    if content.chars().count() > state.config.morpheus_max_message_chars
        || content.len() > state.config.morpheus_max_message_bytes
    {
        return Err(MorpheusError::Validation {
            message: "That message is too long.".to_string(),
        }
        .into());
    }
    if body.client_message_id.trim().is_empty() {
        return Err(MorpheusError::validation("client_message_id is required.").into());
    }

    persistence::get_conversation(&pool, owner, conversation_id).await?;
    let queued = persistence::queued_turn_count(&pool, conversation_id).await?;
    if queued >= i64::from(state.config.morpheus_queue_max_per_conversation) {
        return Err(
            MorpheusError::Conflict("There are too many queued messages.".to_string()).into(),
        );
    }

    let (user, _agent, turn) = persistence::create_turn(
        &pool,
        owner,
        conversation_id,
        content,
        body.client_message_id.trim(),
    )
    .await?;

    scheduler::kick(state.clone(), conversation_id);

    let queue_position = if turn.status == "queued" {
        queued + 1
    } else {
        0
    };
    Ok((
        StatusCode::ACCEPTED,
        Json(SendMessageResponse {
            message: user.into(),
            turn: turn.into(),
            queue_position,
        }),
    ))
}
