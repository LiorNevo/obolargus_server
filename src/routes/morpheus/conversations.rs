//! Conversation create, list, detail, rename, delete, and pagination routes.

use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use obolargus_core::morpheus::error::MorpheusError;
use obolargus_core::morpheus::persistence;

use super::{
    ApiError, ApiResult, ConversationSummaryView, MessageView, TurnSummaryView,
    conversation_summary, require_enabled, require_pool,
};
use crate::AppState;
use crate::auth::AuthUser;

/// Create-conversation body.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateConversationRequest {
    /// Client-generated idempotency id.
    pub client_conversation_id: Uuid,
}

/// Conversation page.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ConversationPage {
    /// Items.
    pub items: Vec<ConversationSummaryView>,
    /// Total.
    pub total: i64,
    /// Limit.
    pub limit: i64,
    /// Offset.
    pub offset: i64,
}

/// List query.
#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct ListQuery {
    /// Page size (default 20, 1–100).
    #[serde(default)]
    pub limit: Option<i64>,
    /// Offset (default 0).
    #[serde(default)]
    pub offset: Option<i64>,
}

/// Detail query.
#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct DetailQuery {
    /// Message count (default 100, 1–200).
    #[serde(default)]
    pub message_limit: Option<i64>,
}

/// Conversation detail.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ConversationDetail {
    /// Summary.
    pub conversation: ConversationSummaryView,
    /// Newest messages.
    pub messages: Vec<MessageView>,
    /// Turn summaries.
    pub turns: Vec<TurnSummaryView>,
    /// Whether older messages exist.
    pub has_more: bool,
}

/// Older-message query.
#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct BeforeQuery {
    /// Message id cursor.
    pub before: Uuid,
    /// Page size.
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Message page.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MessagePage {
    /// Items.
    pub items: Vec<MessageView>,
    /// Whether more exist.
    pub has_more: bool,
}

/// Rename body.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct RenameRequest {
    /// New title (1–120 chars after trim).
    pub title: String,
}

/// Creates a conversation idempotently.
#[utoipa::path(
    post,
    path = "/api/morpheus/conversations",
    request_body = CreateConversationRequest,
    responses(
        (status = 200, body = ConversationSummaryView),
        (status = 201, body = ConversationSummaryView),
        (status = 400, body = super::ErrorEnvelope)
    ),
    tag = "morpheus"
)]
pub async fn create_conversation(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Json(body): Json<CreateConversationRequest>,
) -> Result<(StatusCode, Json<ConversationSummaryView>), ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let owner = &auth.owner_id;
    match persistence::get_conversation_by_client(&pool, owner, body.client_conversation_id).await {
        Ok(existing) => Ok((StatusCode::OK, Json(conversation_summary(&existing)))),
        Err(MorpheusError::NotFound(_)) => {
            let record =
                persistence::create_conversation(&pool, owner, body.client_conversation_id).await?;
            Ok((StatusCode::CREATED, Json(conversation_summary(&record))))
        }
        Err(error) => Err(error.into()),
    }
}

/// Lists conversations.
#[utoipa::path(
    get,
    path = "/api/morpheus/conversations",
    params(ListQuery),
    responses((status = 200, body = ConversationPage), (status = 503, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn list_conversations(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Query(query): Query<ListQuery>,
) -> ApiResult<ConversationPage> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let offset = query.offset.unwrap_or(0).max(0);
    let (items, total) =
        persistence::list_conversations(&pool, &auth.owner_id, limit, offset).await?;
    Ok(Json(ConversationPage {
        items: items.into_iter().map(Into::into).collect(),
        total,
        limit,
        offset,
    }))
}

/// Reads one conversation with its newest messages and turn summaries.
#[utoipa::path(
    get,
    path = "/api/morpheus/conversations/{conversation_id}",
    params(("conversation_id" = Uuid, Path, description = "Conversation id"), DetailQuery),
    responses((status = 200, body = ConversationDetail), (status = 404, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn get_conversation(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(conversation_id): Path<Uuid>,
    Query(query): Query<DetailQuery>,
) -> ApiResult<ConversationDetail> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let conversation =
        persistence::get_conversation(&pool, &auth.owner_id, conversation_id).await?;
    let limit = query.message_limit.unwrap_or(100).clamp(1, 200);
    let (messages, has_more) = persistence::list_messages(&pool, conversation_id, limit).await?;
    let turns = persistence::list_turns(&pool, conversation_id).await?;
    let message_count = persistence::count_messages(&pool, conversation_id).await?;
    let summary = ConversationSummaryView {
        id: conversation.id,
        title: conversation.title,
        preview: messages
            .last()
            .map(|message| message.content.chars().take(120).collect())
            .unwrap_or_default(),
        message_count,
        created_at: conversation.created_at,
        updated_at: conversation.updated_at,
    };
    Ok(Json(ConversationDetail {
        conversation: summary,
        messages: messages.into_iter().map(Into::into).collect(),
        turns: turns.into_iter().map(Into::into).collect(),
        has_more,
    }))
}

/// Reads messages older than a cursor.
#[utoipa::path(
    get,
    path = "/api/morpheus/conversations/{conversation_id}/messages",
    params(("conversation_id" = Uuid, Path, description = "Conversation id"), BeforeQuery),
    responses((status = 200, body = MessagePage), (status = 404, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn list_messages(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(conversation_id): Path<Uuid>,
    Query(query): Query<BeforeQuery>,
) -> ApiResult<MessagePage> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::get_conversation(&pool, &auth.owner_id, conversation_id).await?;
    let before = persistence::get_message(&pool, conversation_id, query.before)
        .await?
        .seq;
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let (items, has_more) =
        persistence::list_messages_before(&pool, conversation_id, before, limit).await?;
    Ok(Json(MessagePage {
        items: items.into_iter().map(Into::into).collect(),
        has_more,
    }))
}

/// Renames a conversation.
#[utoipa::path(
    patch,
    path = "/api/morpheus/conversations/{conversation_id}",
    request_body = RenameRequest,
    params(("conversation_id" = Uuid, Path, description = "Conversation id")),
    responses((status = 200, body = ConversationSummaryView), (status = 400, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn rename_conversation(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(conversation_id): Path<Uuid>,
    Json(body): Json<RenameRequest>,
) -> ApiResult<ConversationSummaryView> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let title = body.title.trim();
    if title.is_empty() || title.chars().count() > 120 {
        return Err(MorpheusError::validation("Title must be 1–120 characters.").into());
    }
    let record =
        persistence::rename_conversation(&pool, &auth.owner_id, conversation_id, title).await?;
    Ok(Json(conversation_summary(&record)))
}

/// Deletes a conversation.
#[utoipa::path(
    delete,
    path = "/api/morpheus/conversations/{conversation_id}",
    params(("conversation_id" = Uuid, Path, description = "Conversation id")),
    responses((status = 204), (status = 404, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn delete_conversation(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(conversation_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::delete_conversation(&pool, &auth.owner_id, conversation_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
