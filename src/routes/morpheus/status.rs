//! Status and catalog routes.

use axum::Json;
use axum::extract::State;
use serde::Serialize;
use utoipa::ToSchema;

use super::{ApiError, ApiResult, require_enabled};
use crate::AppState;

/// Status response.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StatusResponse {
    /// Feature enabled.
    pub enabled: bool,
    /// Provider label.
    pub provider: String,
    /// Text model.
    pub model: String,
    /// Vision model.
    pub vision_model: String,
    /// Provider reachable.
    pub available: bool,
    /// Vision configured.
    pub vision_available: bool,
    /// Probe instant.
    pub checked_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Last user-safe failure.
    pub last_error: Option<super::ErrorBody>,
    /// Catalog summary.
    pub catalog: CatalogSummary,
    /// Limits.
    pub limits: StatusLimits,
}

/// Catalog summary.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CatalogSummary {
    /// Fingerprint.
    pub fingerprint: String,
    /// Tool count.
    pub tool_count: usize,
}

/// User-relevant limits.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StatusLimits {
    /// Message characters.
    pub max_message_chars: usize,
    /// Context messages.
    pub max_context_messages: usize,
    /// ReACT steps.
    pub max_react_steps: u32,
    /// Observations.
    pub max_observations_per_turn: u32,
    /// Queue.
    pub max_queued_messages: u32,
    /// Approval timeout.
    pub approval_timeout_seconds: u64,
}

/// Reads status and probes the provider when the cache is stale.
#[utoipa::path(
    get,
    path = "/api/morpheus/status",
    responses((status = 200, body = StatusResponse), (status = 503, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn get_status(State(state): State<AppState>) -> ApiResult<StatusResponse> {
    let config = &state.config;
    let mut checked_at = None;
    let mut available = false;
    let mut last_error = None;
    if config.morpheus_enabled {
        let stale = match state.morpheus.health.lock() {
            Ok(health) => health.as_ref().is_none_or(|snapshot| {
                chrono::Utc::now()
                    .signed_duration_since(snapshot.checked_at)
                    .num_seconds()
                    >= config.morpheus_status_probe_seconds as i64
            }),
            Err(_) => true,
        };
        if stale {
            match state.morpheus.provider.list_models().await {
                Ok(_) => state.morpheus.set_health(true, None),
                Err(error) => state.morpheus.set_health(
                    false,
                    Some((error.code().as_str().to_string(), error.user_message())),
                ),
            }
        }
        if let Ok(health) = state.morpheus.health.lock()
            && let Some(snapshot) = health.as_ref()
        {
            available = snapshot.available;
            checked_at = Some(snapshot.checked_at);
            last_error = snapshot
                .last_error
                .as_ref()
                .map(|(code, message)| super::ErrorBody {
                    code: code.clone(),
                    message: message.clone(),
                    details: None,
                });
        }
    }
    Ok(Json(StatusResponse {
        enabled: config.morpheus_enabled,
        provider: config.morpheus_provider_label.clone(),
        model: config.morpheus_model.clone(),
        vision_model: if config.morpheus_vision_model.trim().is_empty() {
            config.morpheus_model.clone()
        } else {
            config.morpheus_vision_model.clone()
        },
        available,
        vision_available: config.morpheus_enabled && config.morpheus_vision_enabled,
        checked_at,
        last_error,
        catalog: CatalogSummary {
            fingerprint: state.morpheus.catalog.fingerprint(),
            tool_count: state.morpheus.catalog.specs().len(),
        },
        limits: StatusLimits {
            max_message_chars: config.morpheus_max_message_chars,
            max_context_messages: config.morpheus_max_context_messages,
            max_react_steps: config.morpheus_max_react_steps,
            max_observations_per_turn: config.morpheus_max_observations_per_turn,
            max_queued_messages: config.morpheus_queue_max_per_conversation,
            approval_timeout_seconds: config.morpheus_approval_timeout_seconds,
        },
    }))
}

/// Ensures the UI can hide the launcher when disabled.
pub fn enabled_or_error(state: &AppState) -> Result<(), ApiError> {
    require_enabled(state)
}
