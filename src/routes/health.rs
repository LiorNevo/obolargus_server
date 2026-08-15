//! `GET /health` — service health check.
//!
//! Full wire contract: `specs/001-boilerplate-submodules/contracts/health-check.md`.

use axum::{Json, extract::State};
use serde::Serialize;
use sqlx::PgPool;

use crate::AppState;

/// Health check response body.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct HealthResponse {
    status: &'static str,
    version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<&'static str>,
}

/// Serves `GET /health`.
///
/// Reports the service as `ok`. The `database` field is present only when a
/// pool is configured; a failed ping still returns HTTP 200 with
/// `database: "unavailable"` per the contract.
pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        database: database_status(&state.pool).await,
    })
}

async fn database_status(pool: &Option<PgPool>) -> Option<&'static str> {
    match pool {
        Some(pool) => Some(database_label(ping(pool).await)),
        None => None,
    }
}

async fn ping(pool: &PgPool) -> bool {
    sqlx::query("SELECT 1").execute(pool).await.is_ok()
}

fn database_label(ping_ok: bool) -> &'static str {
    if ping_ok { "ok" } else { "unavailable" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn response_omits_database_when_unconfigured() {
        let body = json!(HealthResponse {
            status: "ok",
            version: "0.1.0",
            database: None,
        });
        assert_eq!(body, json!({ "status": "ok", "version": "0.1.0" }));
    }

    #[test]
    fn response_includes_database_status_when_configured() {
        let body = json!(HealthResponse {
            status: "ok",
            version: "0.1.0",
            database: Some("unavailable"),
        });
        assert_eq!(
            body,
            json!({ "status": "ok", "version": "0.1.0", "database": "unavailable" })
        );
        assert_eq!(database_label(true), "ok");
        assert_eq!(database_label(false), "unavailable");
    }
}
