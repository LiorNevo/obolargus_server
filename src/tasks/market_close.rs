//! End-of-day refresh task. Thin wrapper around `run_refresh`.
//!
//! Implements US4 / FR-030..FR-032 of `specs/017-yahoo-data-source/spec.md`.

use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::refresh::{RefreshConfig, run_refresh};

use crate::AppState;

/// Runs one EOD refresh over every asset that has a supported identifier.
///
/// Returns a typed `RefreshSummary` on success. The scheduler in
/// `tasks::mod::run` logs and continues on typed errors — they are
/// never silently swallowed.
pub async fn run(
    state: &AppState,
) -> Result<obolargus_core::market_data::refresh::RefreshSummary, MarketDataProviderError> {
    let Some(pool) = state.pool.as_ref() else {
        tracing::warn!("scheduler invoked but no database pool is configured; skipping refresh");
        return Ok(obolargus_core::market_data::refresh::RefreshSummary {
            run_id: uuid::Uuid::nil(),
            assets_attempted: 0,
            assets_refreshed: 0,
            assets_no_op: 0,
            assets_missing_identifier: 0,
            assets_source_unavailable: 0,
        });
    };
    run_refresh(
        pool,
        &state.market_data,
        RefreshConfig {
            assets: None,
            trigger: "scheduler".to_string(),
        },
    )
    .await
}
