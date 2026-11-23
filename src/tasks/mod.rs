//! Scheduled background tasks for the obolargus-server.
//!
//! Implements US4 of `specs/034-production-hardening/spec.md`. Two loops run
//! concurrently for the lifetime of the server: the end-of-day refresh and
//! the maintenance pass (retention plus monthly partitions). Each runs once
//! immediately at startup and then on its configured interval, so a restart
//! never postpones work by a full period. Both loops are spawned by
//! `serve()`, so neither blocks the listener.

#![forbid(unsafe_code)]

pub mod market_close;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{Months, Utc};
use obolargus_core::automation::runtime::retention::{
    RetentionReport as AutomationRetentionReport, prune as prune_automation,
};
use obolargus_core::market_data::config::MarketDataConfig;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::registry::DataSourceRegistry;
use obolargus_core::morpheus::retention::{
    RetentionReport as MorpheusRetentionReport, prune as prune_morpheus,
};
use obolargus_core::temporal::observations::ensure_month_partitions;
use tokio::sync::watch;

use crate::AppState;
use crate::config::AppConfig;
use crate::market_data_provider::build_provider_from_config;

/// Counts from one maintenance pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaintenanceSummary {
    /// Whether the current-plus-ahead partition window was ensured.
    pub partitions_ok: bool,
    /// Automation retention counts when that sub-job succeeded.
    pub automation: Option<AutomationRetentionReport>,
    /// Morpheus retention counts when that sub-job succeeded.
    pub morpheus: Option<MorpheusRetentionReport>,
    /// Number of sub-jobs that failed during this pass.
    pub failures: usize,
}

/// Logs the prominent warning required when scheduled tasks are disabled.
///
/// `serve()` reaches this through [`build_registry_and_persist`] at startup;
/// callers may also invoke it directly. It logs at most once per process.
pub fn warn_when_scheduled_tasks_disabled(config: &AppConfig) {
    warn_if_scheduled_tasks_disabled(config.run_scheduled_tasks);
}

fn warn_if_scheduled_tasks_disabled(run_scheduled_tasks: bool) {
    use std::sync::Once;

    static WARNED: Once = Once::new();
    if !run_scheduled_tasks {
        WARNED.call_once(|| {
            tracing::warn!(
                "RUN_SCHEDULED_TASKS=false: market data will NOT refresh automatically, and \
                 retention pruning and monthly partition maintenance will NOT run. Set \
                 RUN_SCHEDULED_TASKS=true for any production deployment."
            );
        });
    }
}

/// Runs the scheduled loops until shutdown.
///
/// Implements FR-021 and FR-023 of spec 034. The EOD refresh and the
/// maintenance pass each run once before their interval loop starts and
/// continue with [`tokio::time::MissedTickBehavior::Skip`] so a slow run
/// cannot produce a burst of overlapping work.
pub async fn run(
    state: AppState,
    shutdown: watch::Receiver<bool>,
) -> Result<(), MarketDataProviderError> {
    let ((), ()) = tokio::join!(
        eod_loop(state.clone(), shutdown.clone()),
        maintenance_loop(state, shutdown),
    );
    Ok(())
}

/// Runs `job` immediately, then once per `interval_seconds` until shutdown.
///
/// The first scheduler tick is consumed so the immediate invocation is the
/// only startup run. Missed ticks are skipped, never replayed in a burst.
pub async fn run_interval<F, Fut>(
    interval_seconds: u64,
    mut shutdown: watch::Receiver<bool>,
    mut job: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    if *shutdown.borrow() {
        return;
    }
    job().await;

    let seconds = std::cmp::max(interval_seconds, 1);
    let mut ticker = tokio::time::interval(Duration::from_secs(seconds));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Drop the interval's own immediate first tick.
    ticker.tick().await;

    while !*shutdown.borrow() {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = ticker.tick() => {
                job().await;
            }
        }
    }
}

/// Runs the EOD refresh loop.
async fn eod_loop(state: AppState, shutdown: watch::Receiver<bool>) {
    let interval = state.config.market_data_eod_interval_seconds;
    run_interval(interval, shutdown, || {
        let state = state.clone();
        async move { run_eod_once(&state).await }
    })
    .await;
}

/// Runs one EOD refresh and logs its name, counts, duration, and error.
async fn run_eod_once(state: &AppState) {
    let started = Instant::now();
    match market_close::run(state).await {
        Ok(summary) => tracing::info!(
            job = "market_close",
            assets_attempted = summary.assets_attempted,
            assets_refreshed = summary.assets_refreshed,
            assets_no_op = summary.assets_no_op,
            assets_missing_identifier = summary.assets_missing_identifier,
            assets_source_unavailable = summary.assets_source_unavailable,
            duration_ms = started.elapsed().as_millis() as u64,
            "scheduled job completed"
        ),
        Err(error) => tracing::error!(
            job = "market_close",
            %error,
            duration_ms = started.elapsed().as_millis() as u64,
            "scheduled job failed"
        ),
    }
}

/// Runs the retention/partition loop.
async fn maintenance_loop(state: AppState, shutdown: watch::Receiver<bool>) {
    let interval = state.config.maintenance_interval_seconds;
    run_interval(interval, shutdown, || {
        let state = state.clone();
        async move {
            run_maintenance_once(&state).await;
        }
    })
    .await;
}

/// Runs one maintenance pass and logs every sub-job independently.
///
/// A failing sub-job is logged and counted; it never aborts the pass or the
/// server, and the next scheduled pass retries it safely.
pub async fn run_maintenance_once(state: &AppState) -> MaintenanceSummary {
    let started = Instant::now();
    let mut summary = MaintenanceSummary::default();

    let Some(pool) = state.pool.as_ref() else {
        tracing::warn!(
            job = "maintenance",
            "no database pool configured; skipping retention and partition maintenance"
        );
        return summary;
    };

    let partition_started = Instant::now();
    let today = Utc::now().date_naive();
    let ahead = state.config.temporal_partition_ahead_months;
    let end = today
        .checked_add_months(Months::new(ahead))
        .unwrap_or(today);
    match ensure_month_partitions(pool, today, end).await {
        Ok(()) => {
            summary.partitions_ok = true;
            tracing::info!(
                job = "observation_partitions",
                months_ahead = ahead,
                duration_ms = partition_started.elapsed().as_millis() as u64,
                "maintenance job completed"
            );
        }
        Err(error) => {
            summary.failures += 1;
            tracing::error!(
                job = "observation_partitions",
                %error,
                duration_ms = partition_started.elapsed().as_millis() as u64,
                "maintenance job failed"
            );
        }
    }

    let sweep_started = Instant::now();
    match obolargus_core::market_data::history::sweep_stale_groups(
        pool,
        state.config.data_load_stale_after_seconds,
    )
    .await
    {
        Ok(count) => tracing::info!(
            job = "load_group_sweep",
            groups_closed = count,
            duration_ms = sweep_started.elapsed().as_millis() as u64,
            "maintenance job completed"
        ),
        Err(error) => {
            summary.failures += 1;
            tracing::error!(
                job = "load_group_sweep",
                %error,
                duration_ms = sweep_started.elapsed().as_millis() as u64,
                "maintenance job failed"
            );
        }
    }

    let retention_started = Instant::now();
    match prune_automation(
        pool,
        state.config.automation_run_retention_days,
        state.config.automation_audit_retention_days,
    )
    .await
    {
        Ok(report) => {
            summary.automation = Some(report);
            tracing::info!(
                job = "automation_retention",
                runs_deleted = report.runs_deleted,
                node_runs_deleted = report.node_runs_deleted,
                messages_deleted = report.messages_deleted,
                logs_deleted = report.logs_deleted,
                audit_events_deleted = report.audit_events_deleted,
                duration_ms = retention_started.elapsed().as_millis() as u64,
                "maintenance job completed"
            );
        }
        Err(error) => {
            summary.failures += 1;
            tracing::error!(
                job = "automation_retention",
                %error,
                duration_ms = retention_started.elapsed().as_millis() as u64,
                "maintenance job failed"
            );
        }
    }

    let morpheus_started = Instant::now();
    match prune_morpheus(pool, state.config.morpheus_conversation_retention_days).await {
        Ok(report) => {
            summary.morpheus = Some(report);
            tracing::info!(
                job = "morpheus_retention",
                conversations_deleted = report.conversations_deleted,
                messages_deleted = report.messages_deleted,
                turns_deleted = report.turns_deleted,
                duration_ms = morpheus_started.elapsed().as_millis() as u64,
                "maintenance job completed"
            );
        }
        Err(error) => {
            summary.failures += 1;
            tracing::error!(
                job = "morpheus_retention",
                %error,
                duration_ms = morpheus_started.elapsed().as_millis() as u64,
                "maintenance job failed"
            );
        }
    }

    tracing::info!(
        job = "maintenance",
        failures = summary.failures,
        duration_ms = started.elapsed().as_millis() as u64,
        "maintenance pass finished"
    );
    summary
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
    let registry = DataSourceRegistry::new_with_budget(providers, config.provider_budget())?;
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
    warn_if_scheduled_tasks_disabled(config.run_scheduled_tasks);

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

    let registry = DataSourceRegistry::new_with_budget(providers, config.provider_budget())?;
    for (source_id, enabled) in persisted_states {
        registry.set_enabled(&source_id, enabled)?;
    }
    Ok(Arc::new(registry))
}
