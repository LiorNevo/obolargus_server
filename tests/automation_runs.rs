//! Manual run lifecycle, messages, logs, cancel, and retry tests (US2).

mod automation_common;

use axum::http::{Method, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

use obolargus_server::AppState;

use automation_common::{migrated_pool, send, send_with_headers, state};

fn flow_with_delay(seconds: u64) -> Value {
    json!({
        "schema_version": 1,
        "name": "run flow",
        "nodes": [
            { "id": "n1", "type": "inject", "type_version": 1, "config": { "repeat": "none", "once": true } },
            {
                "id": "n2",
                "type": "delay",
                "type_version": 1,
                "config": { "mode": "delay", "seconds": seconds }
            },
            { "id": "n3", "type": "debug", "type_version": 1, "config": { "target": "msg" } }
        ],
        "wires": [
            { "id": "w1", "source": "n1", "source_port": "out", "target": "n2", "target_port": "in" },
            { "id": "w2", "source": "n2", "source_port": "out", "target": "n3", "target_port": "in" }
        ]
    })
}

async fn published_flow(state: &AppState, flow: Value) -> String {
    let (status, created) = send(
        state.clone(),
        Method::POST,
        "/api/automation/flows",
        Some(json!({ "name": format!("run-{}", Uuid::new_v4()) })),
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
        Some(json!({ "expected_revision": revision, "change_note": "test" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{published}");
    flow_id
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
        let run_status = body["status"].as_str().unwrap_or_default();
        if matches!(
            run_status,
            "succeeded" | "partial" | "failed" | "cancelled" | "interrupted"
        ) {
            return body;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("run {run_id} did not reach a terminal state");
}

#[tokio::test]
async fn manual_run_exposes_detail_nodes_messages_and_logs() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = published_flow(&state, flow_with_delay(0)).await;
    let (status, accepted) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({ "inputs": { "symbol": "AAPL" } })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    let run_id = accepted["run_id"].as_str().expect("run id").to_string();

    let run = wait_for_terminal(&state, &run_id).await;
    assert_eq!(run["status"], "succeeded", "{run}");
    assert_eq!(run["trigger_kind"], "manual");
    assert_eq!(run["flow_version_number"], 1);
    assert_eq!(run["inputs"]["symbol"], "AAPL");
    assert!(run["summary"]["nodes_total"].as_i64().unwrap_or(0) >= 3);

    let (status, node) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/runs/{run_id}/nodes/n2"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{node}");
    assert_eq!(node["node_id"], "n2");
    assert_eq!(node["status"], "succeeded");
    assert!(node["messages_in"].as_i64().unwrap_or(0) >= 1);

    let (status, messages) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/runs/{run_id}/messages"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{messages}");
    assert!(!messages["items"].as_array().expect("messages").is_empty());

    let (status, logs) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/runs/{run_id}/logs"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");

    let (status, listed) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/runs?flow_id={flow_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert!(
        listed["items"]
            .as_array()
            .expect("runs")
            .iter()
            .any(|item| item["id"] == run_id)
    );
}

#[tokio::test]
async fn missing_run_returns_not_found() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let (status, body) = send(
        state,
        Method::GET,
        &format!("/api/automation/runs/{}", Uuid::new_v4()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn cancelling_a_running_run_stops_it() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = published_flow(&state, flow_with_delay(60)).await;
    let (status, accepted) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    let run_id = accepted["run_id"].as_str().expect("run id").to_string();

    // Wait until the executor has claimed the run so the cancellation token is
    // registered, then request cancellation.
    for _ in 0..100 {
        let (_, body) = send(
            state.clone(),
            Method::GET,
            &format!("/api/automation/runs/{run_id}"),
            None,
        )
        .await;
        if body["status"] == "running" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let (status, cancelled) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/runs/{run_id}/cancel"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{cancelled}");

    let run = wait_for_terminal(&state, &run_id).await;
    assert_eq!(run["status"], "cancelled", "{run}");

    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/runs/{run_id}/cancel"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn retrying_a_terminal_run_creates_a_linked_run() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = published_flow(&state, flow_with_delay(0)).await;
    let (status, accepted) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    let original = accepted["run_id"].as_str().expect("run id").to_string();
    wait_for_terminal(&state, &original).await;

    let (status, retried) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/runs/{original}/retry"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{retried}");
    assert_eq!(retried["parent_run_id"], original);
    assert_ne!(retried["run_id"], original);
}

#[tokio::test]
async fn runs_require_a_published_version() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let (status, created) = send(
        state.clone(),
        Method::POST,
        "/api/automation/flows",
        Some(json!({ "name": "unpublished" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let flow_id = created["id"].as_str().expect("flow id");
    let (status, body) = send(
        state,
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], "conflict");
}

#[tokio::test]
async fn manual_run_idempotency_key_prevents_duplicates() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = published_flow(&state, flow_with_delay(0)).await;
    let key = format!("manual-{}", Uuid::new_v4());
    let uri = format!("/api/automation/flows/{flow_id}/runs");
    let (status, first) = send_with_headers(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "inputs": { "x": 1 } })),
        &[("idempotency-key", key.as_str())],
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{first}");
    let (status, second) = send_with_headers(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "inputs": { "x": 1 } })),
        &[("idempotency-key", key.as_str())],
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{second}");
    assert_eq!(first["run_id"], second["run_id"]);
}

#[tokio::test]
async fn run_start_and_sse_endpoint_respond_quickly() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = published_flow(&state, flow_with_delay(0)).await;

    let started = std::time::Instant::now();
    let (status, accepted) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "run start took {:?}",
        started.elapsed()
    );
    let run_id = accepted["run_id"].as_str().expect("run id");

    let started = std::time::Instant::now();
    let response = obolargus_server::router(state)
        .oneshot(
            Request::builder()
                .uri(format!("/api/automation/runs/{run_id}/events"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream")),
        "missing SSE content type"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "SSE start took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn concurrent_runs_per_flow_are_limited() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = published_flow(&state, flow_with_delay(60)).await;
    let (status, first) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{first}");
    let (status, limited) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{limited}");
    assert_eq!(limited["error"]["code"], "rate_limited");
    let run_id = first["run_id"].as_str().expect("run id");
    let _ = send(
        state,
        Method::POST,
        &format!("/api/automation/runs/{run_id}/cancel"),
        None,
    )
    .await;
}
