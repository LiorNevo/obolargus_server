//! A stop accepted while the last outstanding date is being written must end
//! the run `complete`, not `cancelled`.

mod data_load_common;

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use uuid::Uuid;

use data_load_common::{
    GroupFixture, SlowProvider, cleanup_asset, cleanup_group, identifier, insert_child,
    migrated_pool, range_body, read_group, seed_asset, send, state_with, wait_terminal,
};

#[tokio::test]
async fn a_late_stop_during_the_last_date_ends_complete() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let asset_id = seed_asset(&pool, "TICKER", "LATE034").await;
    let provider: Arc<dyn obolargus_core::market_data::trait_::MarketDataProvider + Send + Sync> =
        Arc::new(SlowProvider {
            delay: Duration::from_millis(400),
        });
    let state = state_with(Some(pool.clone()), Some(provider));
    let response = send(
        state.clone(),
        "POST",
        "/api/market-data/load-range/async",
        Some(range_body(
            vec![identifier("LATE034")],
            "2025-01-01",
            "2025-01-01",
            1,
            Uuid::new_v4(),
        )),
    )
    .await;
    assert_eq!(response.status, StatusCode::ACCEPTED, "{}", response.body);
    let group_id: Uuid = response.body["group_id"]
        .as_str()
        .expect("group id")
        .parse()
        .expect("uuid");

    // The single date is in flight: it has started but has not ended.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let stop = send(
        state,
        "POST",
        &format!("/api/data-load/history/groups/{group_id}/stop"),
        None,
    )
    .await;
    assert_eq!(stop.body["stopped"], true);
    let marker = read_group(&pool, group_id).await.expect("group");
    assert!(marker.1.is_some(), "stop marker persisted");

    let status = wait_terminal(&pool, group_id, Duration::from_secs(10))
        .await
        .expect("terminal status");
    assert_eq!(
        status, "complete",
        "the last date persisted before the stop took effect"
    );
    let after = read_group(&pool, group_id).await.expect("group");
    assert!(
        after.1.is_some(),
        "the marker remains, but does not relabel"
    );

    cleanup_group(&pool, group_id).await;
    cleanup_asset(&pool, asset_id).await;
}

#[tokio::test]
async fn finalization_prefers_complete_when_every_date_persisted() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let group = GroupFixture::running(1);
    group.insert(&pool).await;
    sqlx::query("UPDATE load_range_groups SET cancel_requested_at = NOW() WHERE id = $1")
        .bind(group.id)
        .execute(&pool)
        .await
        .expect("set marker");
    insert_child(&pool, &group, group.from, true, false).await;

    let status = obolargus_core::market_data::history::finalize_group_from_dates(&pool, group.id)
        .await
        .expect("finalize");
    assert_eq!(status.as_deref(), Some("complete"));
    let stored = read_group(&pool, group.id).await.expect("group");
    assert_eq!(stored.0, "complete");
    assert!(
        stored.1.is_some(),
        "the late marker stays for the audit trail"
    );

    cleanup_group(&pool, group.id).await;
}
