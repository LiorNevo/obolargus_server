//! Scheduled EOD refresh timing tests (US4).
//!
//! Proves the refresh runs once immediately at startup and again on the
//! configured interval. Database-gated like the other server integration
//! tests: when no test database is configured the test is a passing no-op.

use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::{Mutex, watch};

use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, tasks};

/// Serializes the two database tests in this binary.
static DB_LOCK: Mutex<()> = Mutex::const_new(());

fn database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
}

async fn migrated_pool() -> Option<PgPool> {
    let url = database_url()?;
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!("SKIPPED: could not connect to the test database: {error}");
            return None;
        }
    };
    if let Err(error) = sqlx::migrate!("../obolargus_core/migrations")
        .run(&pool)
        .await
    {
        eprintln!("SKIPPED: could not migrate the test database: {error}");
        return None;
    }
    Some(pool)
}

/// Removes provider-triggering identifiers and prior scheduler audit rows,
/// so the refresh scans an empty asset set and the counts are deterministic.
async fn reset(pool: &PgPool) {
    sqlx::query("DELETE FROM data_source_refresh_runs")
        .execute(pool)
        .await
        .expect("clear refresh runs");
    sqlx::query("DELETE FROM asset_identifiers")
        .execute(pool)
        .await
        .expect("clear identifiers");
}

async fn scheduler_runs(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM data_source_refresh_runs WHERE trigger = 'scheduler'")
        .fetch_one(pool)
        .await
        .expect("count refresh runs")
}

async fn wait_for_runs(pool: &PgPool, expected: i64, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if scheduler_runs(pool).await >= expected {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn config(interval_seconds: u64) -> AppConfig {
    AppConfig {
        market_data_eod_interval_seconds: interval_seconds,
        maintenance_interval_seconds: 86_400,
        ..AppConfig::default()
    }
}

#[tokio::test]
async fn eod_refresh_runs_immediately_at_startup() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let _guard = DB_LOCK.lock().await;
    reset(&pool).await;

    let state = AppState::new(config(86_400), Some(pool.clone())).expect("app state");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(tasks::run(state, shutdown_rx));

    // The interval is a full day; only the startup run can satisfy this.
    assert!(
        wait_for_runs(&pool, 1, Duration::from_secs(15)).await,
        "the EOD refresh must run before the configured interval elapses"
    );

    shutdown_tx.send(true).expect("send shutdown");
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
async fn eod_refresh_repeats_on_the_configured_interval() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let _guard = DB_LOCK.lock().await;
    reset(&pool).await;

    let state = AppState::new(config(1), Some(pool.clone())).expect("app state");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(tasks::run(state, shutdown_rx));

    assert!(
        wait_for_runs(&pool, 2, Duration::from_secs(20)).await,
        "the EOD refresh must repeat on the configured interval"
    );

    shutdown_tx.send(true).expect("send shutdown");
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}
