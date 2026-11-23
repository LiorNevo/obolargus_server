//! Cancellation truthfulness: a stopped range reports `cancelled` with the
//! original requested count and only the dates that fully persisted.

mod data_load_common;

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use uuid::Uuid;

use data_load_common::{
    SlowProvider, cleanup_asset, cleanup_group, identifier, migrated_pool, range_body, seed_asset,
    send, state_with, wait_terminal,
};

async fn completed_dates(pool: &sqlx::PgPool, group_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM reference_data_load_runs \
         WHERE range_group_id = $1 AND ended_at IS NOT NULL",
    )
    .bind(group_id)
    .fetch_one(pool)
    .await
    .expect("count completed dates")
}

#[tokio::test]
async fn stopping_after_the_first_date_reports_cancelled_and_keeps_requested_count() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let asset_id = seed_asset(&pool, "TICKER", "SLOW034").await;
    let provider: Arc<dyn obolargus_core::market_data::trait_::MarketDataProvider + Send + Sync> =
        Arc::new(SlowProvider {
            delay: Duration::from_millis(200),
        });
    let state = state_with(Some(pool.clone()), Some(provider));
    let client_group_id = Uuid::new_v4();
    let response = send(
        state.clone(),
        "POST",
        "/api/market-data/load-range/async",
        Some(range_body(
            vec![identifier("SLOW034")],
            "2025-01-01",
            "2025-03-01",
            1,
            client_group_id,
        )),
    )
    .await;
    assert_eq!(response.status, StatusCode::ACCEPTED, "{}", response.body);
    let group_id: Uuid = response.body["group_id"]
        .as_str()
        .expect("group id")
        .parse()
        .expect("uuid");

    // Wait until at least one date has persisted, then stop while later
    // dates are still pending.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while completed_dates(&pool, group_id).await < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no date persisted before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let stop = send(
        state.clone(),
        "POST",
        &format!("/api/data-load/history/groups/{group_id}/stop"),
        None,
    )
    .await;
    assert_eq!(stop.status, StatusCode::OK);
    assert_eq!(stop.body["stopped"], true);

    let status = wait_terminal(&pool, group_id, Duration::from_secs(10))
        .await
        .expect("terminal status");
    assert_eq!(status, "cancelled");

    let completed = completed_dates(&pool, group_id).await;
    assert!(completed >= 1, "the in-flight date may finish and count");
    assert!(completed < 60, "not every requested date persisted");

    let progress = send(
        state.clone(),
        "GET",
        &format!("/api/data-load/history/groups/{group_id}/progress"),
        None,
    )
    .await;
    assert_eq!(progress.status, StatusCode::OK, "{}", progress.body);
    assert_eq!(progress.body["status"], "cancelled");
    assert_eq!(progress.body["dates_requested"], 60);
    assert_eq!(progress.body["dates_completed"], completed);

    let detail = send(
        state,
        "GET",
        &format!("/api/data-load/history/{group_id}"),
        None,
    )
    .await;
    assert_eq!(detail.status, StatusCode::OK, "{}", detail.body);
    assert_eq!(detail.body["entry"]["status"], "cancelled");
    assert_eq!(detail.body["range"]["dates_requested"], 60);
    assert_eq!(detail.body["entry"]["dates_requested"], 60);

    let stored = data_load_common::read_group(&pool, group_id)
        .await
        .expect("group row");
    assert_eq!(stored.0, "cancelled");
    assert!(stored.1.is_some(), "the stop marker is durable");
    assert!(stored.2.is_some(), "the group has an end time");

    cleanup_group(&pool, group_id).await;
    cleanup_asset(&pool, asset_id).await;
}
