//! Durable range-group registration and duplicate-conflict tests.

mod data_load_common;

use std::time::Duration;

use axum::http::StatusCode;
use uuid::Uuid;

use data_load_common::{
    DEV_OWNER, GroupFixture, cleanup_group, identifier, migrated_pool, range_body, read_group,
    send, state_with, wait_terminal,
};

#[tokio::test]
async fn concurrent_duplicate_groups_produce_one_run_and_one_conflict() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let client_group_id = Uuid::new_v4();
    let body = range_body(
        vec![identifier("NO_SUCH_ASSET")],
        "2026-01-01",
        "2026-01-03",
        2,
        client_group_id,
    );
    let first = send(
        state_with(Some(pool.clone()), None),
        "POST",
        "/api/market-data/load-range/async",
        Some(body.clone()),
    );
    let second = send(
        state_with(Some(pool.clone()), None),
        "POST",
        "/api/market-data/load-range/async",
        Some(body),
    );
    let (first, second) = tokio::join!(first, second);

    let mut statuses = [first.status, second.status];
    statuses.sort();
    assert_eq!(
        statuses,
        [StatusCode::ACCEPTED, StatusCode::CONFLICT],
        "exactly one registration wins"
    );
    let (accepted, conflict) = if first.status == StatusCode::ACCEPTED {
        (first, second)
    } else {
        (second, first)
    };
    let group_id: Uuid = accepted.body["group_id"]
        .as_str()
        .expect("group id")
        .parse()
        .expect("uuid");
    assert_eq!(conflict.error_code(), Some("group_id_conflict"));
    assert_eq!(
        conflict
            .error_details()
            .and_then(|details| details.get("group_id"))
            .and_then(|value| value.as_str()),
        Some(group_id.to_string().as_str())
    );

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM load_range_groups \
         WHERE owner_id = $1 AND client_group_id = $2",
    )
    .bind(DEV_OWNER)
    .bind(client_group_id)
    .fetch_one(&pool)
    .await
    .expect("count groups");
    assert_eq!(count, 1, "a duplicate creates no second group");

    let status = wait_terminal(&pool, group_id, Duration::from_secs(10)).await;
    assert!(matches!(
        status.as_deref(),
        Some("complete" | "partial" | "failed")
    ));
    cleanup_group(&pool, group_id).await;
}

#[tokio::test]
async fn the_same_client_group_id_under_another_owner_is_independent() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let other = GroupFixture::running(1)
        .owner("another-owner")
        .status("complete");
    other.insert(&pool).await;

    let response = send(
        state_with(Some(pool.clone()), None),
        "POST",
        "/api/market-data/load-range/async",
        Some(range_body(
            vec![identifier("NO_SUCH_ASSET")],
            "2026-01-01",
            "2026-01-01",
            1,
            other.client_group_id,
        )),
    )
    .await;
    assert_eq!(response.status, StatusCode::ACCEPTED);
    let group_id: Uuid = response.body["group_id"]
        .as_str()
        .expect("group id")
        .parse()
        .expect("uuid");
    assert_ne!(group_id, other.id);

    let _ = wait_terminal(&pool, group_id, Duration::from_secs(10)).await;
    cleanup_group(&pool, group_id).await;
    cleanup_group(&pool, other.id).await;
}

#[tokio::test]
async fn a_terminal_group_cannot_be_reused() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let existing = GroupFixture::running(1).status("complete");
    existing.insert(&pool).await;

    let response = send(
        state_with(Some(pool.clone()), None),
        "POST",
        "/api/market-data/load-range/async",
        Some(range_body(
            vec![identifier("NO_SUCH_ASSET")],
            "2026-01-01",
            "2026-01-01",
            1,
            existing.client_group_id,
        )),
    )
    .await;
    assert_eq!(response.status, StatusCode::CONFLICT);
    assert_eq!(
        response
            .error_details()
            .and_then(|details| details.get("group_id"))
            .and_then(|value| value.as_str()),
        Some(existing.id.to_string().as_str())
    );

    let stored = read_group(&pool, existing.id).await.expect("group");
    assert_eq!(stored.0, "complete", "the existing run is unchanged");
    cleanup_group(&pool, existing.id).await;
}
