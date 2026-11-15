//! Startup and periodic cleanup for Morpheus turns, approvals, and images.

use sqlx::PgPool;

use obolargus_core::morpheus::error::MorpheusError;
use obolargus_core::morpheus::persistence;

/// Marks interrupted turns, expires approvals, clears leaked images, and
/// removes stale leases.
pub async fn run(pool: &PgPool) -> Result<(), MorpheusError> {
    let interrupted = persistence::mark_interrupted(pool).await?;
    if interrupted > 0 {
        tracing::warn!(interrupted, "marked interrupted Morpheus turns");
    }
    sqlx::query(
        "UPDATE morpheus_approval_requests SET status = 'expired', decided_at = NOW() \
         WHERE status = 'pending'",
    )
    .execute(pool)
    .await
    .map_err(MorpheusError::db)?;
    sqlx::query(
        "UPDATE morpheus_observations SET status = 'expired', image = NULL, elements = NULL, \
                deleted_at = COALESCE(deleted_at, NOW()) \
         WHERE status = 'requested' AND expires_at < NOW()",
    )
    .execute(pool)
    .await
    .map_err(MorpheusError::db)?;
    sqlx::query(
        "UPDATE morpheus_observations SET image = NULL, elements = NULL, \
                deleted_at = COALESCE(deleted_at, NOW()) \
         WHERE image IS NOT NULL AND (expires_at < NOW() OR turn_id IN ( \
             SELECT id FROM morpheus_turns WHERE status IN ('completed','stopped','failed')))",
    )
    .execute(pool)
    .await
    .map_err(MorpheusError::db)?;
    sqlx::query("DELETE FROM morpheus_control_leases WHERE expires_at < NOW()")
        .execute(pool)
        .await
        .map_err(MorpheusError::db)?;
    Ok(())
}
