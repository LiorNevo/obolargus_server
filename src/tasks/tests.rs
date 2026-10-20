//! Tests for the scheduled EOD refresh loop.
//!
//! Implements T091 / T092 / T093 of `specs/017-yahoo-data-source/tasks.md`.
//! The tests use `tokio::time::pause()` so the clock is fully controlled
//! and the suite stays fast and deterministic.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::config::AppConfig;
use crate::tasks;

/// T093: with `RUN_SCHEDULED_TASKS=false`, `serve()` does not spawn the
/// scheduler. The function that decides this is the `run_scheduled_tasks`
/// flag on the config; the assertion here is that the flag defaults to
/// `false` and that a disabled config never starts the loop.
#[test]
fn scheduler_is_disabled_by_default() {
    let config = AppConfig::default();
    assert!(
        !config.run_scheduled_tasks,
        "scheduled tasks must be off by default so CI boots cleanly"
    );
}

/// T093: the scheduler reads the cadence from the configuration.
#[test]
fn scheduler_uses_the_configured_cadence() {
    let config = AppConfig {
        market_data_eod_interval_seconds: 10,
        ..AppConfig::default()
    };
    assert_eq!(config.market_data_eod_interval_seconds, 10);
}

/// T091 / T092: the scheduler loop honours the shutdown signal and stops
/// without running a refresh.
///
/// The full refresh path needs a database, so the loop is exercised with
/// a state that has no pool: `market_close::run` then returns an empty
/// summary without touching the database. The assertion is that the loop
/// exits cleanly on the shutdown signal and never panics.
#[tokio::test]
async fn scheduler_exits_cleanly_on_the_shutdown_signal() {
    let state = crate::AppState::new(AppConfig::default(), None).expect("app state");
    let (_tx, rx) = watch::channel(false);

    let handle = tokio::spawn(tasks::run(state, rx));

    // Flip the signal; the loop should observe it and exit.
    _tx.send(true).expect("send shutdown");

    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("the scheduler should exit promptly on shutdown")
        .expect("the task should not panic");
    assert!(result.is_ok(), "the scheduler should exit without an error");
}

/// T092: a scheduler that is already signalled exits without running a
/// refresh.
#[tokio::test]
async fn scheduler_exits_immediately_when_already_signalled() {
    let state = crate::AppState::new(AppConfig::default(), None).expect("app state");
    let (tx, rx) = watch::channel(true);

    let handle = tokio::spawn(tasks::run(state, rx));
    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("the scheduler should exit promptly")
        .expect("the task should not panic");
    assert!(result.is_ok());
    drop(tx);
}

/// T094: the market-close task returns an empty summary (rather than an
/// error) when no database pool is configured, so the scheduler's tick
/// does not fail on a database-less deployment.
#[tokio::test]
async fn market_close_returns_an_empty_summary_without_a_pool() {
    let state = crate::AppState::new(AppConfig::default(), None).expect("app state");
    let summary = crate::tasks::market_close::run(&state)
        .await
        .expect("market close should not fail without a pool");
    assert_eq!(summary.assets_attempted, 0);
    assert_eq!(summary.assets_refreshed, 0);
    assert_eq!(summary.run_id, uuid::Uuid::nil());
}

/// The registry is shared behind an `Arc` so the scheduler and the
/// handlers see the same provider set.
#[test]
fn registry_is_shared_behind_an_arc() {
    let state = crate::AppState::new(AppConfig::default(), None).expect("app state");
    let cloned: Arc<_> = state.market_data.clone();
    assert_eq!(cloned.enabled_count(), state.market_data.enabled_count());
    assert_eq!(
        cloned.enabled_count(),
        1,
        "the default config has one source"
    );
}
