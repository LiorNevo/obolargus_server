//! Run creation branches: archived versions, inputs, limits, and lookups.

mod automation_common;

use axum::http::{Method, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

use obolargus_server::AppState;
use obolargus_server::config::AppConfig;

use automation_common::{migrated_pool, send, send_with_headers};

fn roomy_state(pool: sqlx::PgPool) -> AppState {
    AppState::new(
        AppConfig {
            automation_max_concurrent_runs: 100,
            automation_max_concurrent_runs_per_flow: 20,
            ..AppConfig::default()
        },
        Some(pool),
    )
    .expect("state")
}

fn simple(seconds: u64) -> Value {
    json!({
        "schema_version": 1,
        "name": "runs",
        "nodes": [
            { "id": "n1", "type": "inject", "type_version": 1, "config": { "repeat": "none", "once": true } },
            { "id": "n2", "type": "delay", "type_version": 1, "config": { "mode": "delay", "seconds": seconds } },
            { "id": "n3", "type": "debug", "type_version": 1, "config": { "target": "msg" } }
        ],
        "wires": [
            { "id": "w1", "source": "n1", "source_port": "out", "target": "n2", "target_port": "in" },
            { "id": "w2", "source": "n2", "source_port": "out", "target": "n3", "target_port": "in" }
        ]
    })
}

fn with_inputs() -> Value {
    json!({
        "schema_version": 1,
        "name": "inputs",
        "inputs": [
            { "name": "symbol", "type": "string", "required": true, "secret": false },
            { "name": "token", "type": "string", "required": true, "secret": true }
        ],
        "nodes": [
            { "id": "n1", "type": "inject", "type_version": 1, "config": { "repeat": "none", "once": true } },
            { "id": "n2", "type": "debug", "type_version": 1, "config": { "target": "msg" } }
        ],
        "wires": [
            { "id": "w1", "source": "n1", "source_port": "out", "target": "n2", "target_port": "in" }
        ]
    })
}

async fn published_flow(state: &AppState, flow: Value) -> String {
    let (status, created) = send(
        state.clone(),
        Method::POST,
        "/api/automation/flows",
        Some(json!({ "name": format!("runs-{}", Uuid::new_v4()) })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let flow_id = created["id"].as_str().expect("flow id").to_string();
    let (status, draft) = send(
        state.clone(),
        Method::PUT,
        &format!("/api/automation/flows/{flow_id}/draft"),
        Some(json!({ "flow_json": flow })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{draft}");
    let revision = draft["revision"].as_i64().expect("revision");
    let (status, published) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/publish"),
        Some(json!({ "expected_revision": revision, "change_note": "runs" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{published}");
    flow_id
}

async fn start_run(state: &AppState, flow_id: &str, body: Value) -> (StatusCode, Value) {
    send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(body),
    )
    .await
}

async fn wait_for_terminal(state: &AppState, run_id: &str) -> Value {
    for _ in 0..200 {
        let (status, body) = send(
            state.clone(),
            Method::GET,
            &format!("/api/automation/runs/{run_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if matches!(
            body["status"].as_str().unwrap_or_default(),
            "succeeded" | "partial" | "failed" | "cancelled" | "interrupted"
        ) {
            return body;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("run {run_id} did not finish");
}

async fn wait_for_running(state: &AppState, run_id: &str) {
    for _ in 0..200 {
        let (status, body) = send(
            state.clone(),
            Method::GET,
            &format!("/api/automation/runs/{run_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if body["status"] == "running" {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("run {run_id} did not start");
}

#[tokio::test]
async fn create_run_rejects_archived_flows_and_versions() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = roomy_state(pool);
    let flow_id = published_flow(&state, simple(0)).await;

    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/archive"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, archived) = start_run(&state, &flow_id, json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{archived}");
    assert_eq!(archived["error"]["message"], "flow.archived");

    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/restore"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/versions/1/archive"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, archived) = start_run(&state, &flow_id, json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{archived}");
    assert_eq!(archived["error"]["message"], "version.not_published");

    let (status, accepted) = start_run(&state, &flow_id, json!({ "version_number": 1 })).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    wait_for_terminal(&state, accepted["run_id"].as_str().expect("run id")).await;
}

#[tokio::test]
async fn create_run_validates_inject_nodes_inputs_and_secret_merge() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = roomy_state(pool.clone());
    let flow_id = published_flow(&state, with_inputs()).await;

    let (status, body) = start_run(
        &state,
        &flow_id,
        json!({ "inject_node_id": "ghost", "inputs": { "symbol": "AAPL" } }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["message"], "run.inject_node_not_found");

    let (status, body) = start_run(
        &state,
        &flow_id,
        json!({ "inject_node_id": "n2", "inputs": { "symbol": "AAPL" } }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["message"], "run.inject_node_invalid");

    let (status, body) = start_run(&state, &flow_id, json!({ "inputs": {} })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .starts_with("run.input_missing")
    );

    let (status, body) = start_run(&state, &flow_id, json!({ "inputs": null })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, accepted) = start_run(
        &state,
        &flow_id,
        json!({
            "inputs": { "symbol": "AAPL" },
            "secret_inputs": { "token": "s3cret" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    let run_id = accepted["run_id"].as_str().expect("run id");
    let run = wait_for_terminal(&state, run_id).await;
    assert_eq!(run["inputs"]["symbol"], "AAPL");
    assert_eq!(run["inputs"]["token"], "[redacted]");

    let small = AppState::new(
        AppConfig {
            automation_max_input_bytes: 10,
            automation_max_concurrent_runs: 100,
            automation_max_concurrent_runs_per_flow: 20,
            ..AppConfig::default()
        },
        Some(pool),
    )
    .expect("small state");
    let (status, body) = start_run(
        &small,
        &flow_id,
        json!({ "inputs": { "symbol": "ABCDEFGHIJ" } }),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(body["error"]["message"], "run.input_too_large");
}

#[tokio::test]
async fn create_run_enforces_idempotency_and_rate_limits() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = roomy_state(pool.clone());
    let flow_id = published_flow(&state, simple(60)).await;
    let uri = format!("/api/automation/flows/{flow_id}/runs");
    let key = format!("idem-{}", Uuid::new_v4());

    let (status, first) = send_with_headers(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "inputs": { "x": 1 } })),
        &[("idempotency-key", key.as_str())],
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{first}");
    let first_id = first["run_id"].as_str().expect("run id").to_string();

    let (status, mismatch) = send_with_headers(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "inputs": { "x": 2 } })),
        &[("idempotency-key", key.as_str())],
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{mismatch}");
    assert_eq!(mismatch["error"]["message"], "idempotency.payload_mismatch");

    let zero_global = AppState::new(
        AppConfig {
            automation_max_concurrent_runs: 0,
            ..AppConfig::default()
        },
        Some(pool.clone()),
    )
    .expect("global limit state");
    let (status, limited) = start_run(&zero_global, &flow_id, json!({})).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{limited}");
    assert_eq!(limited["error"]["message"], "run.concurrency_limit");

    let zero_flow = AppState::new(
        AppConfig {
            automation_max_concurrent_runs: 100,
            automation_max_concurrent_runs_per_flow: 0,
            ..AppConfig::default()
        },
        Some(pool.clone()),
    )
    .expect("flow limit state");
    let (status, limited) = start_run(&zero_flow, &flow_id, json!({})).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{limited}");
    assert_eq!(limited["error"]["message"], "run.flow_concurrency_limit");

    let (status, trigger) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/triggers"),
        Some(json!({ "kind": "api" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{trigger}");
    let trigger_id = trigger["id"].as_str().expect("trigger id");
    let rate_limited = AppState::new(
        AppConfig {
            automation_max_concurrent_runs: 100,
            automation_max_concurrent_runs_per_flow: 10,
            automation_api_trigger_rate_limit_per_minute: 0,
            ..AppConfig::default()
        },
        Some(pool),
    )
    .expect("rate limit state");
    let (status, body) = send(
        rate_limited,
        Method::POST,
        &format!("/api/automation/triggers/{trigger_id}/invoke"),
        Some(json!({ "inputs": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["error"]["message"], "trigger.rate_limited");

    wait_for_running(&state, &first_id).await;
    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/runs/{first_id}/cancel"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    wait_for_terminal(&state, &first_id).await;
}

#[tokio::test]
async fn run_listing_node_lookup_cancel_and_retry_branches() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = roomy_state(pool);
    let flow_id = published_flow(&state, simple(0)).await;

    let (status, accepted) = start_run(&state, &flow_id, json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    let terminal_id = accepted["run_id"].as_str().expect("run id").to_string();
    wait_for_terminal(&state, &terminal_id).await;

    let (status, page) = send(
        state.clone(),
        Method::GET,
        &format!(
            "/api/automation/runs?flow_id={flow_id}&status=succeeded&trigger_kind=manual&limit=0&offset=-1"
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["limit"], 1);
    assert_eq!(page["offset"], 0);
    let item = page["items"]
        .as_array()
        .expect("items")
        .iter()
        .find(|item| item["id"] == terminal_id)
        .expect("terminal run");
    assert!(item["duration_ms"].as_i64().is_some());
    assert!(item["flow_name"].is_string());

    let (status, page) = send(state.clone(), Method::GET, "/api/automation/runs", None).await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert!(page["total"].as_i64().unwrap_or(0) >= 1);

    let (status, node) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/runs/{terminal_id}/nodes/n2?attempt=1"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{node}");
    assert_eq!(node["node_id"], "n2");

    let (status, _) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/runs/{terminal_id}/nodes/n2?attempt=99"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/runs/{terminal_id}/nodes/ghost"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/runs/{}", Uuid::new_v4()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/runs/{}/cancel", Uuid::new_v4()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let slow_flow = published_flow(&state, simple(60)).await;
    let (status, running) = start_run(&state, &slow_flow, json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{running}");
    let running_id = running["run_id"].as_str().expect("run id");
    wait_for_running(&state, running_id).await;
    let (status, not_terminal) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/runs/{running_id}/retry"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{not_terminal}");
    assert_eq!(not_terminal["error"]["message"], "run.not_terminal");
    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/runs/{running_id}/cancel"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    wait_for_terminal(&state, running_id).await;

    let (status, retried) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/runs/{terminal_id}/retry"),
        Some(json!({ "secret_inputs": { "token": "retry" } })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{retried}");
    assert_eq!(retried["parent_run_id"], terminal_id);
    wait_for_terminal(&state, retried["run_id"].as_str().expect("run id")).await;
}
