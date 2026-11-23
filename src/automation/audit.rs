//! Audit helpers.
//!
//! Every action is attributed to the authenticated actor; scheduled work is
//! attributed to the owning flow's owner.

use obolargus_core::automation::error::AutomationError;
use obolargus_core::automation::runtime::persistence;
use serde_json::Value;
use uuid::Uuid;

use crate::AppState;

/// Writes one audit event for `actor_id`, ignoring storage failures at the
/// call site when the primary operation already succeeded.
#[allow(clippy::too_many_arguments)]
pub async fn record(
    state: &AppState,
    actor_id: &str,
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
        Some(actor_id),
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
#[allow(clippy::too_many_arguments)]
pub async fn record_best_effort(
    state: &AppState,
    actor_id: &str,
    action: &str,
    target_type: &str,
    target_id: Uuid,
    flow_id: Option<Uuid>,
    run_id: Option<Uuid>,
    metadata: Value,
) {
    if let Err(error) = record(
        state,
        actor_id,
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
