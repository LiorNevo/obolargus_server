//! Audit helpers.
//!
//! v1 has no authentication; every action is attributed to the configured
//! `AUTOMATION_ACTOR_ID` or to the trigger kind.

use obolargus_core::automation::error::AutomationError;
use obolargus_core::automation::runtime::persistence;
use serde_json::Value;
use uuid::Uuid;

use crate::AppState;

/// Returns the configured actor identifier.
pub fn actor(state: &AppState) -> &str {
    &state.config.automation_actor_id
}

/// Writes one audit event, ignoring storage failures at the call site when the
/// primary operation already succeeded.
pub async fn record(
    state: &AppState,
    action: &str,
    target_type: &str,
    target_id: Uuid,
    flow_id: Option<Uuid>,
    run_id: Option<Uuid>,
    metadata: Value,
) -> Result<(), AutomationError> {
    let Some(pool) = state.pool.as_ref() else {
        return Ok(());
    };
    persistence::insert_audit(
        pool,
        Some(actor(state)),
        action,
        target_type,
        target_id,
        flow_id,
        run_id,
        metadata,
    )
    .await
}

/// Records an audit event and logs failures without failing the request.
pub async fn record_best_effort(
    state: &AppState,
    action: &str,
    target_type: &str,
    target_id: Uuid,
    flow_id: Option<Uuid>,
    run_id: Option<Uuid>,
    metadata: Value,
) {
    if let Err(error) = record(
        state,
        action,
        target_type,
        target_id,
        flow_id,
        run_id,
        metadata,
    )
    .await
    {
        tracing::warn!(%error, action, "automation audit write failed");
    }
}
