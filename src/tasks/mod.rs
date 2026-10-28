//! Scheduled background tasks for the obolargus-server.
//!
//! Implements US4 of `specs/017-yahoo-data-source/spec.md`. Currently a
//! single EOD refresh (`market_close::run`) wired from `serve()` under the
//! `RUN_SCHEDULED_TASKS` env flag.

#![forbid(unsafe_code)]

pub mod market_close;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use obolargus_core::market_data::config::MarketDataConfig;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::registry::DataSourceRegistry;
use tokio::sync::watch;

use crate::AppState;
use crate::market_data_provider::build_provider_from_config;

/// Runs the scheduled EOD refresh job on a configurable interval.
///
/// Implements US4 / FR-030 of spec 017.
pub async fn run(
    state: AppState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), MarketDataProviderError> {
    let interval_seconds = std::cmp::max(state.config.market_data_eod_interval_seconds, 1);
    // `tokio::time::interval` fires immediately on its first tick; the
    // tick is dropped below so the first *real* run happens one full
    // period after startup (research D5).
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_seconds));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Drop the very first immediate tick.
    ticker.tick().await;

    loop {
        // The signal may already have been sent before the loop started.
        if *shutdown.borrow() {
            tracing::info!("scheduler received shutdown signal; exiting");
            break;
        }
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("scheduler received shutdown signal; exiting");
                    break;
                }
            }
            _ = ticker.tick() => {
                if let Err(error) = market_close::run(&state).await {
                    tracing::error!(%error, "scheduled market_close run failed");
                    // Continue loop; the next tick will retry. Per constitution V,
                    // errors are never silently swallowed, only logged.
                }
            }
        }
    }
    Ok(())
}

/// Builds the data-source registry from configuration.
///
/// Used by both `serve()` (in-process tasks) and `obolargus_cli` (one-shot
/// `sync`). Implements FR-002 / FR-003 of spec 017.
pub fn build_registry_from_config(
    config: &MarketDataConfig,
) -> Result<Arc<DataSourceRegistry>, MarketDataProviderError> {
    let mut providers: Vec<
        Arc<dyn obolargus_core::market_data::trait_::MarketDataProvider + Send + Sync>,
    > = Vec::with_capacity(config.sources.len());
    for source in &config.sources {
        let provider = build_provider_from_config(source)?;
        providers.push(provider);
    }
    let registry = DataSourceRegistry::new(providers)?;
    Ok(Arc::new(registry))
}

/// Builds the registry and persists the configured sources into the
/// `data_sources` table.
///
/// Every configured source is built (including config-disabled ones) so it
/// can be enabled without a restart. The persisted `enabled` column is the
/// authority after the first insert; the configuration's flag is only the
/// initial value.
///
/// The `data_sources` table records the configuration *shape* — never the
/// credential value (FR-040). Fails if the registry cannot be built or the
/// rows cannot be persisted.
pub async fn build_registry_and_persist(
    config: &MarketDataConfig,
    pool: Option<&sqlx::PgPool>,
) -> Result<Arc<DataSourceRegistry>, MarketDataProviderError> {
    let mut providers: Vec<
        Arc<dyn obolargus_core::market_data::trait_::MarketDataProvider + Send + Sync>,
    > = Vec::with_capacity(config.sources.len());
    let mut persisted_states: Vec<(String, bool)> = Vec::with_capacity(config.sources.len());

    for source in &config.sources {
        let (provider, api_key_present) =
            crate::market_data_provider::build_provider_with_key_presence(source)?;
        let enabled = if let Some(pool) = pool {
            let row = obolargus_core::market_data::data_sources::upsert_data_source(
                pool,
                source,
                api_key_present,
            )
            .await?;
            row.enabled
        } else {
            source.enabled
        };
        persisted_states.push((source.source_id.clone(), enabled));
        providers.push(provider);
    }

    let registry = DataSourceRegistry::new(providers)?;
    for (source_id, enabled) in persisted_states {
        registry.set_enabled(&source_id, enabled)?;
    }
    Ok(Arc::new(registry))
}
