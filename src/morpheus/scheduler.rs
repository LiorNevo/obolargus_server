//! Per-conversation queued-turn dispatch.

use uuid::Uuid;

use obolargus_core::morpheus::persistence;

use crate::AppState;

/// Starts queued turns for a conversation, one at a time.
///
/// The scheduler is idempotent: if another loop already holds the active turn,
/// `claim_next_turn` returns `None` and this loop exits.
pub fn kick(state: AppState, conversation_id: Uuid) {
    tokio::spawn(async move {
        let Some(pool) = state.pool.clone() else {
            return;
        };
        loop {
            let Ok(permit) = state.morpheus.semaphore.clone().acquire_owned().await else {
                break;
            };
            match persistence::claim_next_turn(&pool, conversation_id).await {
                Ok(Some(turn)) => {
                    crate::morpheus::engine::run(state.clone(), turn).await;
                    drop(permit);
                }
                Ok(None) => {
                    drop(permit);
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, conversation_id = %conversation_id, "morpheus scheduler claim failed");
                    drop(permit);
                    break;
                }
            }
        }
    });
}
