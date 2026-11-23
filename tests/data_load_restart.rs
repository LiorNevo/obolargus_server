//! Restart and stale-cutoff recovery: open groups and children are finalized
//! from their persisted dates, never left `running`.

mod data_load_common;

use axum::http::StatusCode;

use data_load_common::{
    GroupFixture, cleanup_group, insert_child, migrated_pool, send, state_with,
};

#[tokio::test]
async fn startup_recovery_finalizes_open_groups_and_children() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };

    let complete_group = GroupFixture::running(2);
    complete_group.insert(&pool).await;
    insert_child(&pool, &complete_group, complete_group.from, true, false).await;
    insert_child(
        &pool,
        &complete_group,
        complete_group.from + chrono::Duration::days(1),
        true,
        false,
    )
    .await;

    let cancelled_group = GroupFixture::running(2);
    cancelled_group.insert(&pool).await;
    sqlx::query("UPDATE load_range_groups SET cancel_requested_at = NOW() WHERE id = $1")
        .bind(cancelled_group.id)
        .execute(&pool)
        .await
        .expect("set stop marker");
    insert_child(&pool, &cancelled_group, cancelled_group.from, true, false).await;
    insert_child(
        &pool,
        &cancelled_group,
        cancelled_group.from + chrono::Duration::days(1),
        false,
        false,
    )
    .await;

    let interrupted_group = GroupFixture::running(2);
    interrupted_group.insert(&pool).await;
    insert_child(
        &pool,
        &interrupted_group,
        interrupted_group.from,
        true,
        false,
    )
    .await;
    let open_child = insert_child(
        &pool,
        &interrupted_group,
        interrupted_group.from,
        false,
        false,
    )
    .await;

    let closed = obolargus_core::market_data::history::finalize_open_groups(&pool)
        .await
        .expect("finalize open groups");
    assert!(closed >= 3);

    let complete = data_load_common::read_group(&pool, complete_group.id)
        .await
        .expect("complete group");
    assert_eq!(complete.0, "complete");
    assert!(complete.2.is_some());

    let cancelled = data_load_common::read_group(&pool, cancelled_group.id)
        .await
        .expect("cancelled group");
    assert_eq!(cancelled.0, "cancelled");

    let interrupted = data_load_common::read_group(&pool, interrupted_group.id)
        .await
        .expect("interrupted group");
    assert_eq!(interrupted.0, "interrupted");

    let _ = obolargus_core::market_data::audit::finalize_open_child_runs(&pool)
        .await
        .expect("finalize child runs");
    let child_state: (Option<chrono::DateTime<chrono::Utc>>, String) = sqlx::query_as(
        "SELECT ended_at, terminal_status FROM reference_data_load_runs WHERE id = $1",
    )
    .bind(open_child)
    .fetch_one(&pool)
    .await
    .expect("child row");
    assert!(child_state.0.is_some(), "the child has an end time");
    assert_eq!(child_state.1, "interrupted");

    let progress = send(
        state_with(Some(pool.clone()), None),
        "GET",
        &format!(
            "/api/data-load/history/groups/{}/progress",
            interrupted_group.id
        ),
        None,
    )
    .await;
    assert_eq!(progress.status, StatusCode::OK);
    assert_eq!(progress.body["status"], "interrupted");
    assert_eq!(progress.body["dates_requested"], 2);

    for group in [complete_group, cancelled_group, interrupted_group] {
        cleanup_group(&pool, group.id).await;
    }
}

#[tokio::test]
async fn progress_applies_the_stale_cutoff_without_a_restart() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let stale =
        GroupFixture::running(2).started_at(chrono::Utc::now() - chrono::Duration::hours(3));
    stale.insert(&pool).await;
    insert_child(&pool, &stale, stale.from, true, false).await;
    insert_child(
        &pool,
        &stale,
        stale.from + chrono::Duration::days(1),
        false,
        false,
    )
    .await;

    let response = send(
        state_with(Some(pool.clone()), None),
        "GET",
        &format!("/api/data-load/history/groups/{}/progress", stale.id),
        None,
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(response.body["status"], "interrupted");
    assert_eq!(response.body["dates_requested"], 2);
    assert_eq!(response.body["dates_completed"], 1);

    let stored = data_load_common::read_group(&pool, stale.id)
        .await
        .expect("stale group");
    assert_eq!(stored.0, "interrupted");
    assert!(stored.2.is_some());

    cleanup_group(&pool, stale.id).await;
}
