//! Stop semantics: only the owner of an active, non-terminal group with
//! missing dates may stop it; every other case returns `stopped:false`.

mod data_load_common;

use axum::http::StatusCode;

use data_load_common::{
    GroupFixture, cleanup_group, insert_child, migrated_pool, read_group, send, state_with,
};

async fn stop(group_id: uuid::Uuid) -> data_load_common::TestResponse {
    send(
        state_with(None, None),
        "POST",
        &format!("/api/data-load/history/groups/{group_id}/stop"),
        None,
    )
    .await
}

#[tokio::test]
async fn an_unknown_group_returns_false() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let response = send(
        state_with(Some(pool), None),
        "POST",
        &format!(
            "/api/data-load/history/groups/{}/stop",
            uuid::Uuid::new_v4()
        ),
        None,
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body["stopped"], false);
}

#[tokio::test]
async fn a_terminal_group_returns_false_and_is_unchanged() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let terminal = GroupFixture::running(1).status("complete");
    terminal.insert(&pool).await;
    let before = read_group(&pool, terminal.id).await.expect("group");

    let response = send(
        state_with(Some(pool.clone()), None),
        "POST",
        &format!("/api/data-load/history/groups/{}/stop", terminal.id),
        None,
    )
    .await;
    assert_eq!(response.body["stopped"], false);
    let after = read_group(&pool, terminal.id).await.expect("group");
    assert_eq!(before.0, after.0);
    assert_eq!(before.1, after.1);
    assert_eq!(before.2, after.2);

    cleanup_group(&pool, terminal.id).await;
}

#[tokio::test]
async fn another_owners_group_returns_false_and_is_unchanged() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let other = GroupFixture::running(2).owner("another-owner");
    other.insert(&pool).await;

    let response = send(
        state_with(Some(pool.clone()), None),
        "POST",
        &format!("/api/data-load/history/groups/{}/stop", other.id),
        None,
    )
    .await;
    assert_eq!(response.body["stopped"], false);
    let after = read_group(&pool, other.id).await.expect("group");
    assert_eq!(after.0, "running");
    assert!(after.1.is_none(), "no stop marker is written");

    cleanup_group(&pool, other.id).await;
}

#[tokio::test]
async fn an_active_owned_group_is_stopped_once() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let group = GroupFixture::running(3);
    group.insert(&pool).await;
    insert_child(&pool, &group, group.from, true, false).await;

    let response = send(
        state_with(Some(pool.clone()), None),
        "POST",
        &format!("/api/data-load/history/groups/{}/stop", group.id),
        None,
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body["stopped"], true);
    let after = read_group(&pool, group.id).await.expect("group");
    assert!(
        after.1.is_some(),
        "the marker is durable before the response"
    );
    assert_eq!(
        after.0, "running",
        "finalization belongs to the runner/sweep"
    );

    let repeated = send(
        state_with(Some(pool.clone()), None),
        "POST",
        &format!("/api/data-load/history/groups/{}/stop", group.id),
        None,
    )
    .await;
    assert_eq!(repeated.body["stopped"], false);

    cleanup_group(&pool, group.id).await;
}

#[tokio::test]
async fn a_group_with_every_date_persisted_returns_false() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    let group = GroupFixture::running(1);
    group.insert(&pool).await;
    insert_child(&pool, &group, group.from, true, false).await;

    let response = send(
        state_with(Some(pool.clone()), None),
        "POST",
        &format!("/api/data-load/history/groups/{}/stop", group.id),
        None,
    )
    .await;
    assert_eq!(
        response.body["stopped"], false,
        "a late stop must not relabel"
    );
    let after = read_group(&pool, group.id).await.expect("group");
    assert!(after.1.is_none());

    cleanup_group(&pool, group.id).await;
}

#[tokio::test]
async fn the_unknown_group_case_uses_an_empty_pool_state() {
    // A no-database state returns 503 rather than claiming a stop.
    let response = stop(uuid::Uuid::new_v4()).await;
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
}
