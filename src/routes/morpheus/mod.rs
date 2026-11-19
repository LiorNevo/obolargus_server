//! Morpheus HTTP routes.

pub mod approvals;
pub mod catalog;
pub mod conversations;
pub mod events;
pub mod messages;
pub mod status;
pub mod turns;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use utoipa::ToSchema;
use uuid::Uuid;

use obolargus_core::morpheus::error::{ErrorCode, MorpheusError};
use obolargus_core::morpheus::model::{
    ApprovalRecord, ConversationRecord, ConversationSummaryRecord, MessageRecord,
    ObservationRecord, StepRecord, ToolCallRecord, TurnRecord, UiActionRecord,
};

use crate::AppState;

/// API result alias.
pub type ApiResult<T> = Result<Json<T>, ApiError>;

/// Error body.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorBody {
    /// Stable code.
    pub code: String,
    /// User-safe message.
    pub message: String,
    /// Optional details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Error envelope.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorEnvelope {
    /// Error body.
    pub error: ErrorBody,
}

/// API failure.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
}

impl ApiError {
    /// Builds an API failure.
    pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }

    /// Feature disabled.
    pub fn disabled() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "morpheus_disabled",
            "Morpheus is not enabled for this deployment.",
        )
    }

    /// Missing database.
    pub fn no_pool() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "internal_error",
            "The workspace database is not available.",
        )
    }
}

impl From<MorpheusError> for ApiError {
    fn from(error: MorpheusError) -> Self {
        let status = match error.code() {
            ErrorCode::Validation => StatusCode::BAD_REQUEST,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            ErrorCode::Conflict => StatusCode::CONFLICT,
            ErrorCode::Limit | ErrorCode::QueueFull => StatusCode::CONFLICT,
            ErrorCode::ProviderRateLimited => StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::ProviderUnavailable
            | ErrorCode::ProviderAuth
            | ErrorCode::ProviderCapability => StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::ProviderTimeout => StatusCode::GATEWAY_TIMEOUT,
            ErrorCode::ApprovalExpired | ErrorCode::ApprovalDenied => StatusCode::CONFLICT,
            ErrorCode::Infrastructure => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        };
        Self::new(status, error.code().as_str(), error.user_message())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                    details: None,
                },
            }),
        )
            .into_response()
    }
}

/// The acting user identifier.
pub fn actor(state: &AppState) -> &str {
    &state.config.morpheus_actor_id
}

/// Rejects requests when the feature is disabled.
pub fn require_enabled(state: &AppState) -> Result<(), ApiError> {
    if state.config.morpheus_enabled {
        Ok(())
    } else {
        Err(ApiError::disabled())
    }
}

/// Returns the database pool or a 503.
pub fn require_pool(state: &AppState) -> Result<sqlx::PgPool, ApiError> {
    state.pool.clone().ok_or_else(ApiError::no_pool)
}

/// Conversation summary view.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ConversationSummaryView {
    /// Id.
    pub id: Uuid,
    /// Title.
    pub title: String,
    /// Preview.
    pub preview: String,
    /// Message count.
    pub message_count: i64,
    /// Created.
    pub created_at: DateTime<Utc>,
    /// Last activity.
    pub updated_at: DateTime<Utc>,
}

impl From<ConversationSummaryRecord> for ConversationSummaryView {
    fn from(record: ConversationSummaryRecord) -> Self {
        Self {
            id: record.id,
            title: record.title,
            preview: record.preview,
            message_count: record.message_count,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

/// Builds a summary from a fresh conversation.
pub fn conversation_summary(record: &ConversationRecord) -> ConversationSummaryView {
    ConversationSummaryView {
        id: record.id,
        title: record.title.clone(),
        preview: String::new(),
        message_count: 0,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

/// Message view.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MessageView {
    /// Id.
    pub id: Uuid,
    /// Conversation id.
    pub conversation_id: Uuid,
    /// Role.
    pub role: String,
    /// Status.
    pub status: String,
    /// Content.
    pub content: String,
    /// Whether context was trimmed.
    pub context_trimmed: bool,
    /// Error.
    pub error: Option<ErrorBody>,
    /// Created.
    pub created_at: DateTime<Utc>,
    /// Updated.
    pub updated_at: DateTime<Utc>,
    /// Completed.
    pub completed_at: Option<DateTime<Utc>>,
}

impl From<MessageRecord> for MessageView {
    fn from(record: MessageRecord) -> Self {
        let error = record.error_code.clone().map(|code| ErrorBody {
            code,
            message: record
                .error_message
                .clone()
                .unwrap_or_else(|| "Something went wrong.".to_string()),
            details: None,
        });
        Self {
            id: record.id,
            conversation_id: record.conversation_id,
            role: record.role,
            status: record.status,
            content: record.content,
            context_trimmed: record.context_trimmed,
            error,
            created_at: record.created_at,
            updated_at: record.updated_at,
            completed_at: record.completed_at,
        }
    }
}

/// Turn summary view.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TurnSummaryView {
    /// Id.
    pub id: Uuid,
    /// Conversation id.
    pub conversation_id: Uuid,
    /// Status.
    pub status: String,
    /// Attempt.
    pub attempt: i32,
    /// Retry parent.
    pub retry_of: Option<Uuid>,
    /// User message.
    pub user_message_id: Uuid,
    /// Agent message.
    pub agent_message_id: Uuid,
    /// Steps used.
    pub step_count: i32,
    /// Limit reached.
    pub limit_reached: Option<String>,
    /// Error.
    pub error: Option<ErrorBody>,
    /// Created.
    pub created_at: DateTime<Utc>,
    /// Started.
    pub started_at: Option<DateTime<Utc>>,
    /// Ended.
    pub ended_at: Option<DateTime<Utc>>,
}

impl From<TurnRecord> for TurnSummaryView {
    fn from(record: TurnRecord) -> Self {
        let error = record.error_code.clone().map(|code| ErrorBody {
            code,
            message: record
                .error_message
                .clone()
                .unwrap_or_else(|| "Something went wrong.".to_string()),
            details: None,
        });
        Self {
            id: record.id,
            conversation_id: record.conversation_id,
            status: record.status,
            attempt: record.attempt,
            retry_of: record.retry_of,
            user_message_id: record.user_message_id,
            agent_message_id: record.agent_message_id,
            step_count: record.step_count,
            limit_reached: record.limit_reached,
            error,
            created_at: record.created_at,
            started_at: record.started_at,
            ended_at: record.ended_at,
        }
    }
}

/// Step view.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StepView {
    /// Id.
    pub id: Uuid,
    /// Sequence.
    pub seq: i32,
    /// Kind.
    pub kind: String,
    /// Status.
    pub status: String,
    /// Summary.
    pub summary: String,
    /// Related id.
    pub ref_id: Option<Uuid>,
    /// Created.
    pub created_at: DateTime<Utc>,
    /// Completed.
    pub completed_at: Option<DateTime<Utc>>,
}

impl From<StepRecord> for StepView {
    fn from(record: StepRecord) -> Self {
        Self {
            id: record.id,
            seq: record.seq,
            kind: record.kind,
            status: record.status,
            summary: record.summary,
            ref_id: record.ref_id,
            created_at: record.created_at,
            completed_at: record.completed_at,
        }
    }
}

/// Tool call view.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ToolCallView {
    /// Id.
    pub id: Uuid,
    /// Tool id.
    pub tool_id: String,
    /// Risk.
    pub risk: String,
    /// Redacted arguments summary.
    pub arguments_summary: String,
    /// Status.
    pub status: String,
    /// Result summary.
    pub result_summary: Option<String>,
    /// Truncated.
    pub result_truncated: bool,
    /// Error.
    pub error: Option<ErrorBody>,
    /// Approval id.
    pub approval_id: Option<Uuid>,
    /// Started.
    pub started_at: Option<DateTime<Utc>>,
    /// Completed.
    pub completed_at: Option<DateTime<Utc>>,
}

impl From<ToolCallRecord> for ToolCallView {
    fn from(record: ToolCallRecord) -> Self {
        let error = record.error_code.clone().map(|code| ErrorBody {
            code,
            message: record.error_message.clone().unwrap_or_default(),
            details: None,
        });
        Self {
            id: record.id,
            tool_id: record.tool_id,
            risk: record.risk,
            arguments_summary: record.arguments_summary,
            status: record.status,
            result_summary: record.result_summary,
            result_truncated: record.result_truncated,
            error,
            approval_id: record.approval_id,
            started_at: record.started_at,
            completed_at: record.completed_at,
        }
    }
}

/// UI action view.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UiActionView {
    /// Id.
    pub id: Uuid,
    /// Kind.
    pub kind: String,
    /// Target ref.
    pub target_ref: Option<String>,
    /// Target label.
    pub target_label: String,
    /// Value summary.
    pub value_summary: Option<String>,
    /// Status.
    pub status: String,
    /// Result summary.
    pub result_summary: Option<String>,
    /// Error.
    pub error: Option<ErrorBody>,
    /// Approval id.
    pub approval_id: Option<Uuid>,
    /// Started.
    pub started_at: Option<DateTime<Utc>>,
    /// Completed.
    pub completed_at: Option<DateTime<Utc>>,
}

impl From<UiActionRecord> for UiActionView {
    fn from(record: UiActionRecord) -> Self {
        let error = record.error_code.clone().map(|code| ErrorBody {
            code,
            message: String::new(),
            details: None,
        });
        Self {
            id: record.id,
            kind: record.kind,
            target_ref: record.target_ref,
            target_label: record.target_label,
            value_summary: record.value_summary,
            status: record.status,
            result_summary: record.result_summary,
            error,
            approval_id: record.approval_id,
            started_at: record.started_at,
            completed_at: record.completed_at,
        }
    }
}

/// Observation metadata (never image bytes).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ObservationView {
    /// Id.
    pub id: Uuid,
    /// Captured at.
    pub captured_at: Option<DateTime<Utc>>,
    /// Width.
    pub width: Option<i32>,
    /// Height.
    pub height: Option<i32>,
    /// Elements.
    pub element_count: i32,
    /// Masks.
    pub mask_count: i32,
    /// Whether the image was discarded.
    pub deleted: bool,
}

impl From<ObservationRecord> for ObservationView {
    fn from(record: ObservationRecord) -> Self {
        Self {
            id: record.id,
            captured_at: record.captured_at,
            width: record.width,
            height: record.height,
            element_count: record.element_count,
            mask_count: record.mask_count,
            deleted: record.image.is_none(),
        }
    }
}

/// Approval view.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ApprovalView {
    /// Id.
    pub id: Uuid,
    /// Action kind.
    pub action_kind: String,
    /// Action id.
    pub action_id: Uuid,
    /// Status.
    pub status: String,
    /// Decision mode.
    pub decision_mode: Option<String>,
    /// Expiry.
    pub expires_at: DateTime<Utc>,
    /// Created.
    pub created_at: DateTime<Utc>,
    /// Decided.
    pub decided_at: Option<DateTime<Utc>>,
}

impl From<ApprovalRecord> for ApprovalView {
    fn from(record: ApprovalRecord) -> Self {
        Self {
            id: record.id,
            action_kind: record.action_kind,
            action_id: record.action_id,
            status: record.status,
            decision_mode: record.decision_mode,
            expires_at: record.expires_at,
            created_at: record.created_at,
            decided_at: record.decided_at,
        }
    }
}

/// JSON error used by SSE preflight failures.
pub fn error_json(code: &str, message: &str) -> Value {
    json!({ "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_errors_map_every_morpheus_code() {
        let cases = [
            (ErrorCode::Validation, StatusCode::BAD_REQUEST),
            (ErrorCode::NotFound, StatusCode::NOT_FOUND),
            (ErrorCode::Conflict, StatusCode::CONFLICT),
            (ErrorCode::Limit, StatusCode::CONFLICT),
            (ErrorCode::QueueFull, StatusCode::CONFLICT),
            (
                ErrorCode::ProviderRateLimited,
                StatusCode::TOO_MANY_REQUESTS,
            ),
            (
                ErrorCode::ProviderUnavailable,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (ErrorCode::ProviderAuth, StatusCode::SERVICE_UNAVAILABLE),
            (
                ErrorCode::ProviderCapability,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (ErrorCode::ProviderTimeout, StatusCode::GATEWAY_TIMEOUT),
            (ErrorCode::ApprovalExpired, StatusCode::CONFLICT),
            (ErrorCode::ApprovalDenied, StatusCode::CONFLICT),
            (ErrorCode::Infrastructure, StatusCode::INTERNAL_SERVER_ERROR),
            (ErrorCode::ToolFailed, StatusCode::BAD_REQUEST),
        ];
        for (code, status) in cases {
            let error = ApiError::from(MorpheusError::provider(code, "detail"));
            assert_eq!(error.status, status, "{}", code.as_str());
            assert_eq!(error.code, code.as_str());
        }
    }

    #[test]
    fn disabled_and_no_pool_errors_have_stable_wire_codes() {
        let disabled = ApiError::disabled();
        assert_eq!(disabled.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(disabled.code, "morpheus_disabled");

        let no_pool = ApiError::no_pool();
        assert_eq!(no_pool.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(no_pool.code, "internal_error");
    }
}
