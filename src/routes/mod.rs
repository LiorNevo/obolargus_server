//! HTTP route modules for the API server.

pub mod assets;
pub mod auth;
pub mod automation;
pub mod calendar;
pub mod data_load;
pub mod domain;
pub mod health;
pub(crate) mod load_cancellation;
pub mod market_data;
pub mod morpheus;
pub mod temporal;

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::{Value, json};

/// Standard error envelope used by auth and data-load responses.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ErrorEnvelope {
    /// Error payload.
    pub error: ApiError,
}

/// One API error.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ApiError {
    /// Stable machine-readable code.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Optional structured context (field/limit/actual, existing id).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub details: Option<Value>,
}

impl ErrorEnvelope {
    /// Builds an envelope without details.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: ApiError {
                code: code.into(),
                message: message.into(),
                details: None,
            },
        }
    }

    /// Adds structured details.
    pub fn with_details(mut self, details: Value) -> Self {
        self.error.details = Some(details);
        self
    }

    /// Builds a JSON response, optionally with `Retry-After`.
    pub fn response(self, status: StatusCode, retry_after: Option<u64>) -> Response {
        let mut response = (status, Json(self)).into_response();
        if let Some(seconds) = retry_after
            && let Ok(value) = HeaderValue::from_str(&seconds.to_string())
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        response
    }

    /// Convenience for validation errors.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new("validation_error", message)
    }

    /// Convenience for limit errors with field/limit/actual.
    pub fn limit(field: &str, limit: i64, actual: i64, message: impl Into<String>) -> Self {
        Self::new("limit_exceeded", message).with_details(json!({
            "field": field,
            "limit": limit,
            "actual": actual,
        }))
    }
}
