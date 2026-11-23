//! Startup actor bootstrap and legacy ownership backfill.
//!
//! Runs after migrations and before the HTTP listener. It seeds the primary
//! actor from `PRIMARY_OWNER_EMAIL` with an unbound Google subject, reassigns
//! legacy `dev-user` / `NULL` ownership to that actor when authentication is
//! enabled, and loads the actor cache. All statements are idempotent, and any
//! failure aborts startup rather than serving with half-loaded identity state.
//!
//! Contract: `specs/034-production-hardening/data-model.md` §9.

use std::fmt;

use sqlx::PgPool;

use super::AuthState;

/// Why startup bootstrap failed.
#[derive(Debug)]
pub enum BootstrapError {
    /// Actor registry or ownership backfill failed.
    Database(String),
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(detail) => write!(f, "authentication bootstrap failed: {detail}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl From<sqlx::Error> for BootstrapError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error.to_string())
    }
}

/// An ownership table and the statement that reassigns legacy owners to the
/// primary actor. The table names are internal constants, never user input.
const REASSIGN_STATEMENTS: &[(&str, &str)] = &[
    (
        "automation_flows",
        "UPDATE automation_flows SET owner_id = $1
         WHERE owner_id IS NULL OR owner_id = 'dev-user'",
    ),
    (
        "automation_context",
        "UPDATE automation_context SET owner_id = $1
         WHERE owner_id IS NULL OR owner_id = 'dev-user'",
    ),
    (
        "morpheus_conversations",
        "UPDATE morpheus_conversations SET owner_id = $1
         WHERE owner_id IS NULL OR owner_id = 'dev-user'",
    ),
    (
        "reference_data_load_runs",
        "UPDATE reference_data_load_runs SET owner_id = $1
         WHERE owner_id IS NULL OR owner_id = 'dev-user'",
    ),
    (
        "automation_runs",
        "UPDATE automation_runs SET owner_id = $1
         WHERE owner_id IS NULL OR owner_id = 'dev-user'",
    ),
];

/// Seeds the primary actor, backfills legacy ownership, and loads the cache.
pub async fn run(pool: &PgPool, auth: &AuthState) -> Result<(), BootstrapError> {
    if auth.settings.enabled {
        let primary = auth
            .store
            .seed_primary(&auth.settings.primary_owner_email)
            .await
            .map_err(|error| BootstrapError::Database(error.to_string()))?;
        let owner = primary.id.to_string();

        // Runs without an owner inherit their flow's owner first; anything
        // still unowned is legacy data and moves to the primary actor.
        sqlx::query(
            "UPDATE automation_runs r SET owner_id = f.owner_id
             FROM automation_flows f
             WHERE r.flow_id = f.id AND r.owner_id IS NULL",
        )
        .execute(pool)
        .await?;

        for (table, statement) in REASSIGN_STATEMENTS {
            let result = sqlx::query(sqlx::AssertSqlSafe(*statement))
                .bind(&owner)
                .execute(pool)
                .await?;
            if result.rows_affected() > 0 {
                tracing::info!(
                    table = *table,
                    rows = result.rows_affected(),
                    owner_id = %owner,
                    "reassigned legacy ownership to the primary actor"
                );
            }
        }
        tracing::info!(
            actor_id = %primary.id,
            email = %primary.email,
            "seeded primary authentication actor"
        );
    }

    let actors = auth
        .store
        .load()
        .await
        .map_err(|error| BootstrapError::Database(error.to_string()))?;
    let count = actors.len();
    auth.actors.replace_all(actors);
    tracing::info!(count, "loaded authentication actor cache");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_error_names_the_phase() {
        let error = BootstrapError::Database("boom".to_string());
        assert!(
            error
                .to_string()
                .contains("authentication bootstrap failed")
        );
    }

    #[test]
    fn every_reassign_statement_targets_its_table() {
        for (table, statement) in REASSIGN_STATEMENTS {
            assert!(statement.contains(table), "statement must target {table}");
            assert!(statement.contains("dev-user"));
        }
    }
}
