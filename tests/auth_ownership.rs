//! Cross-user ownership matrix (T021).
//!
//! Seeds rows owned by two actors in every table that now carries ownership
//! and proves, through the authenticated HTTP surface, that actor A cannot
//! read, update, or delete actor B's rows, does not see them in lists, and
//! cannot stop B's range load. Skips when no test database is configured,
//! like the other database-backed suites.

mod common;

use std::sync::Arc;

use axum::http::{Method, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

use common::auth::{self, ALICE, BOB, access_token, seed_actor};
use obolargus_server::AppState;
use obolargus_server::auth::AuthState;
use obolargus_server::auth::actor::ActorRecord;
use obolargus_server::config::AppConfig;

fn database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
        .filter(|url| !url.trim().is_empty())
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

struct Fixture {
    state: AppState,
    a: ActorRecord,
    b: ActorRecord,
    a_flow: Uuid,
    b_flow: Uuid,
    a_run: Uuid,
    b_run: Uuid,
    a_conversation: Uuid,
    b_conversation: Uuid,
    a_load_run: Uuid,
    b_load_run: Uuid,
    a_group: Uuid,
    b_group: Uuid,
}

async fn fixture() -> Option<Fixture> {
    let pool = migrated_pool().await?;
    let config = AppConfig {
        morpheus_enabled: true,
        ..AppConfig::default()
    };
    let mut state = AppState::new(config, Some(pool.clone())).expect("state with pool");

    let verifier = Arc::new(common::auth::MockGoogleVerifier::default());
    let auth = Arc::new(AuthState::new_in_memory(auth::settings(), verifier));
    let a = seed_actor(&auth, ALICE, Some("google-a"));
    let b = seed_actor(&auth, BOB, Some("google-b"));
    state.auth = auth;

    let a_owner = a.id.to_string();
    let b_owner = b.id.to_string();
    let suffix = Uuid::new_v4();

    let a_flow = insert_flow(&pool, &a_owner, &format!("a-flow-{suffix}")).await;
    let b_flow = insert_flow(&pool, &b_owner, &format!("b-flow-{suffix}")).await;
    let a_version = insert_version(&pool, a_flow).await;
    let b_version = insert_version(&pool, b_flow).await;
    let a_run = insert_run(&pool, &a_owner, a_flow, a_version).await;
    let b_run = insert_run(&pool, &b_owner, b_flow, b_version).await;
    insert_context(&pool, &a_owner, a_flow).await;
    insert_context(&pool, &b_owner, b_flow).await;
    let a_conversation = insert_conversation(&pool, &a_owner).await;
    let b_conversation = insert_conversation(&pool, &b_owner).await;
    let a_load_run = insert_load_run(&pool, &a_owner).await;
    let b_load_run = insert_load_run(&pool, &b_owner).await;
    let a_group = insert_group(&pool, &a_owner).await;
    let b_group = insert_group(&pool, &b_owner).await;

    Some(Fixture {
        state,
        a,
        b,
        a_flow,
        b_flow,
        a_run,
        b_run,
        a_conversation,
        b_conversation,
        a_load_run,
        b_load_run,
        a_group,
        b_group,
    })
}

async fn insert_flow(pool: &PgPool, owner: &str, name: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO automation_flows (owner_id, name) VALUES ($1, $2) RETURNING id")
        .bind(owner)
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("insert flow")
}

async fn insert_version(pool: &PgPool, flow_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO automation_flow_versions (flow_id, status, flow_json, created_by)
         VALUES ($1, 'draft', '{}'::jsonb, 'ownership-test') RETURNING id",
    )
    .bind(flow_id)
    .fetch_one(pool)
    .await
    .expect("insert version")
}

async fn insert_run(pool: &PgPool, owner: &str, flow_id: Uuid, version_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO automation_runs
             (flow_id, flow_version_id, trigger_kind, status, owner_id, actor_id)
         VALUES ($1, $2, 'manual', 'succeeded', $3, $3) RETURNING id",
    )
    .bind(flow_id)
    .bind(version_id)
    .bind(owner)
    .fetch_one(pool)
    .await
    .expect("insert run")
}

async fn insert_context(pool: &PgPool, owner: &str, flow_id: Uuid) {
    sqlx::query(
        "INSERT INTO automation_context (owner_id, scope, scope_id, key, value)
         VALUES ($1, 'flow', $2, 'owner-only', 'true'::jsonb)",
    )
    .bind(owner)
    .bind(flow_id.to_string())
    .execute(pool)
    .await
    .expect("insert context");
}

async fn insert_conversation(pool: &PgPool, owner: &str) -> Uuid {
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO morpheus_conversations (owner_id, client_conversation_id, title)
         VALUES ($1, $2, 'owned') RETURNING id",
    )
    .bind(owner)
    .bind(Uuid::new_v4())
    .fetch_one(pool)
    .await
    .expect("insert conversation");
    sqlx::query(
        "INSERT INTO morpheus_messages (conversation_id, seq, role, status, content)
         VALUES ($1, 1, 'user', 'complete', 'hello')",
    )
    .bind(id)
    .execute(pool)
    .await
    .expect("insert conversation message");
    id
}

async fn insert_load_run(pool: &PgPool, owner: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO reference_data_load_runs
             (owner_id, trigger, identifiers_requested_raw, identifiers_attempted, loaded)
         VALUES ($1, 'manual', 1, 1, 1) RETURNING id",
    )
    .bind(owner)
    .fetch_one(pool)
    .await
    .expect("insert load run")
}

async fn insert_group(pool: &PgPool, owner: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO load_range_groups
             (owner_id, client_group_id, from_date, to_date, concurrency, status, dates_requested)
         VALUES ($1, $2, DATE '2026-10-01', DATE '2026-10-02', 2, 'running', 2)
         RETURNING id",
    )
    .bind(owner)
    .bind(Uuid::new_v4())
    .fetch_one(pool)
    .await
    .expect("insert group")
}

async fn send(
    state: &AppState,
    token: &str,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let response = auth::send(state, method.clone(), uri, body, Some(token)).await;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|_| {
            panic!(
                "non-JSON body for {method} {uri}: status {status}: {}",
                String::from_utf8_lossy(&bytes)
            )
        })
    };
    (status, value)
}

fn ids(page: &Value, field: &str) -> Vec<String> {
    page["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item[field].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn a_cannot_reach_b_rows_and_lists_are_owner_scoped() {
    let Some(fixture) = fixture().await else {
        return;
    };
    let a_token = access_token(&fixture.state.auth, &fixture.a);
    let b_token = access_token(&fixture.state.auth, &fixture.b);
    assert_ne!(a_token, b_token);
    let a_owner = fixture.a.id.to_string();
    let b_owner = fixture.b.id.to_string();
    let a_flow = fixture.a_flow.to_string();
    let b_flow = fixture.b_flow.to_string();
    let a_run = fixture.a_run.to_string();
    let b_run = fixture.b_run.to_string();
    let a_conversation = fixture.a_conversation.to_string();
    let b_conversation = fixture.b_conversation.to_string();
    let state = &fixture.state;

    // ---- automation flows -------------------------------------------------
    let (status, page) = send(
        state,
        &a_token,
        Method::GET,
        "/api/automation/flows?limit=100",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let flow_ids = ids(&page, "id");
    assert!(flow_ids.contains(&a_flow), "A's flow is missing: {page}");
    assert!(!flow_ids.contains(&b_flow), "B's flow leaked into A's list");

    let (status, _) = send(
        state,
        &a_token,
        Method::GET,
        &format!("/api/automation/flows/{b_flow}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state,
        &a_token,
        Method::PATCH,
        &format!("/api/automation/flows/{b_flow}"),
        Some(json!({ "name": "hijacked" })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state,
        &a_token,
        Method::DELETE,
        &format!("/api/automation/flows/{b_flow}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state,
        &a_token,
        Method::GET,
        &format!("/api/automation/flows/{b_flow}/context"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state,
        &a_token,
        Method::DELETE,
        &format!("/api/automation/flows/{b_flow}/context"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, context) = send(
        state,
        &a_token,
        Method::GET,
        &format!("/api/automation/flows/{a_flow}/context"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{context}");
    assert_eq!(context["flow"]["owner-only"], json!(true));

    // ---- automation runs --------------------------------------------------
    let (status, page) = send(
        state,
        &a_token,
        Method::GET,
        "/api/automation/runs?limit=100",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let run_ids = ids(&page, "id");
    assert!(run_ids.contains(&a_run), "A's run is missing: {page}");
    assert!(!run_ids.contains(&b_run), "B's run leaked into A's list");

    let (status, _) = send(
        state,
        &a_token,
        Method::GET,
        &format!("/api/automation/runs/{b_run}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state,
        &a_token,
        Method::POST,
        &format!("/api/automation/runs/{b_run}/cancel"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // ---- Morpheus conversations ------------------------------------------
    let (status, page) = send(
        state,
        &a_token,
        Method::GET,
        "/api/morpheus/conversations?limit=100",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let conversation_ids = ids(&page, "id");
    assert!(
        conversation_ids.contains(&a_conversation),
        "A's conversation is missing: {page}"
    );
    assert!(
        !conversation_ids.contains(&b_conversation),
        "B's conversation leaked into A's list"
    );

    let (status, _) = send(
        state,
        &a_token,
        Method::GET,
        &format!("/api/morpheus/conversations/{b_conversation}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state,
        &a_token,
        Method::PATCH,
        &format!("/api/morpheus/conversations/{b_conversation}"),
        Some(json!({ "title": "hijacked" })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state,
        &a_token,
        Method::DELETE,
        &format!("/api/morpheus/conversations/{b_conversation}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // ---- B's rows still exist and are readable by B ----------------------
    let (status, _) = send(
        state,
        &b_token,
        Method::GET,
        &format!("/api/automation/flows/{b_flow}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        state,
        &b_token,
        Method::GET,
        &format!("/api/morpheus/conversations/{b_conversation}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Owner ids are stable UUID strings, not client-supplied values.
    assert!(Uuid::parse_str(&a_owner).is_ok());
    assert!(Uuid::parse_str(&b_owner).is_ok());
}

#[tokio::test]
async fn load_history_detail_progress_and_stop_are_owner_scoped() {
    let Some(fixture) = fixture().await else {
        return;
    };
    let a_token = access_token(&fixture.state.auth, &fixture.a);
    let b_token = access_token(&fixture.state.auth, &fixture.b);
    let state = &fixture.state;
    let b_load_run = fixture.b_load_run.to_string();
    let b_group = fixture.b_group.to_string();

    for uri in [
        format!("/api/data-load/history/{b_load_run}"),
        format!("/api/data-load/history/{b_group}"),
        format!("/api/data-load/history/groups/{b_group}/progress"),
    ] {
        let (status, body) = send(state, &a_token, Method::GET, &uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {body}");
    }

    let (status, result) = send(
        state,
        &a_token,
        Method::POST,
        &format!("/api/data-load/history/groups/{b_group}/stop"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["stopped"], json!(false));

    let cancel_requested: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT cancel_requested_at FROM load_range_groups WHERE id = $1")
            .bind(fixture.b_group)
            .fetch_one(fixture.state.pool.as_ref().expect("fixture pool"))
            .await
            .expect("read marker");
    assert!(cancel_requested.is_none(), "B's group must not be stopped");

    let (status, _) = send(
        state,
        &b_token,
        Method::GET,
        &format!("/api/data-load/history/{b_load_run}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn load_history_lists_only_the_actors_rows() {
    let Some(fixture) = fixture().await else {
        return;
    };
    let a_token = access_token(&fixture.state.auth, &fixture.a);
    let state = &fixture.state;
    let a_load_run = fixture.a_load_run.to_string();
    let b_load_run = fixture.b_load_run.to_string();
    let a_group = fixture.a_group.to_string();
    let b_group = fixture.b_group.to_string();

    let (status, page) = send(
        state,
        &a_token,
        Method::GET,
        "/api/data-load/history?limit=200",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let entry_ids: Vec<String> = page["entries"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry["entry_id"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        entry_ids.contains(&a_load_run),
        "A's load run is missing: {page}"
    );
    assert!(entry_ids.contains(&a_group), "A's range is missing: {page}");
    assert!(
        !entry_ids.contains(&b_load_run),
        "B's load run leaked into A's history"
    );
    assert!(
        !entry_ids.contains(&b_group),
        "B's range leaked into A's history"
    );
}
