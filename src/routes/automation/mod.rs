//! HTTP surface for the automation flow engine.
//!
//! Implements `specs/030-automation-workflow-engine/contracts/http-api.md`.

pub mod audit;
pub mod catalog;
pub mod events;
pub mod flows;
pub mod messages;
pub mod runs;
pub mod triggers;
pub mod versions;

use axum::Json;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use obolargus_core::automation::error::AutomationError;

use crate::AppState;

/// JSON error envelope shared with the rest of the API.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ApiErrorBody {
    /// Error details.
    pub error: ApiErrorDetail,
}

/// One error code/message pair.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ApiErrorDetail {
    /// Stable machine-readable code.
    pub code: String,
    /// Operator-facing message.
    pub message: String,
    /// Optional structured details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Error response alias used by every automation handler.
pub type ApiError = (StatusCode, Json<Box<ApiErrorBody>>);

/// Successful JSON response alias.
pub type ApiResult<T> = Result<Json<T>, ApiError>;

/// Builds an error response.
pub fn api_error(status: StatusCode, code: &str, message: impl Into<String>) -> ApiError {
    (
        status,
        Json(Box::new(ApiErrorBody {
            error: ApiErrorDetail {
                code: code.to_string(),
                message: message.into(),
                details: None,
            },
        })),
    )
}

/// Maps a domain error to the contract envelope.
pub fn map_error(error: AutomationError) -> ApiError {
    match error {
        AutomationError::Validation(message) => {
            api_error(StatusCode::BAD_REQUEST, "validation_error", message)
        }
        AutomationError::NotFound(message) => {
            api_error(StatusCode::NOT_FOUND, "not_found", message)
        }
        AutomationError::Conflict(message) => api_error(StatusCode::CONFLICT, "conflict", message),
        AutomationError::Unsupported(message) => api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_flow",
            message,
        ),
        AutomationError::PayloadTooLarge(message) => {
            api_error(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large", message)
        }
        AutomationError::Infrastructure(message) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            message,
        ),
        AutomationError::Internal(message) => {
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
        }
    }
}

/// Requires a configured database pool.
pub fn require_pool(state: &AppState) -> Result<&sqlx::PgPool, ApiError> {
    state.pool.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "no database pool configured",
        )
    })
}

/// Requires the automation feature flag.
pub fn require_enabled(state: &AppState) -> Result<(), ApiError> {
    if state.config.automation_enabled {
        Ok(())
    } else {
        Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "automation is disabled",
        ))
    }
}

/// Common pagination query.
#[derive(Debug, Clone, Copy, Deserialize, utoipa::IntoParams)]
pub struct Pagination {
    /// Page size (default 50, max 200).
    #[serde(default)]
    pub limit: Option<i64>,
    /// Offset (default 0).
    #[serde(default)]
    pub offset: Option<i64>,
}

impl Pagination {
    /// Returns `(limit, offset)` clamped to contract bounds.
    pub fn bounds(self) -> (i64, i64) {
        let limit = self.limit.unwrap_or(50).clamp(1, 200);
        let offset = self.offset.unwrap_or(0).max(0);
        (limit, offset)
    }
}

/// Paginated response envelope.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct Page<T> {
    /// Page items.
    pub items: Vec<T>,
    /// Total matching rows.
    pub total: i64,
    /// Page size.
    pub limit: i64,
    /// Offset.
    pub offset: i64,
}

impl<T> Page<T> {
    /// Builds a page.
    pub fn new(items: Vec<T>, total: i64, limit: i64, offset: i64) -> Self {
        Self {
            items,
            total,
            limit,
            offset,
        }
    }
}
