//! Server-Sent Events projection coverage.

mod automation_common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

use obolargus_core::automation::runtime::persistence;

use automation_common::{migrated_pool, send};

fn document(seconds: u64) -> Value {
    json!({
        "schema_version": 1,
        "name": "events",
        "nodes": [
            { "id": "n1", "type": "inject", "type_version": 1, "config": { "repeat": "none", "once": true } },
            { "id": "n2", "type": "delay", "type_version": 1, "config": { "mode": "delay", "seconds": seconds } },
            { "id": "n3", "type": "log", "type_version": 1, "config": { "level": "info", "message": "event coverage" } },
            { "id": "n4", "type": "debug", "type_version": 1, "config": { "target": "msg" } }
        ],
        "wires": [
            { "id": "w1", "source": "n1", "source_port": "out", "target": "n2", "target_port": "in" },
            { "id": "w2", "source": "n2", "source_port": "out", "target": "n3", "target_port": "in" },
            { "id": "w3", "source": "n3", "source_port": "out", "target": "n4", "target_port": "in" }
        ]
    })
}

async fn published_flow(state: &AppState, seconds: u64) -> String {
    let (status, created) = send(
        state.clone(),
        Method::POST,
        "/api/automation/flows",
        Some(json!({ "name": format!("events-{}", Uuid::new_v4()) })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let flow_id = created["id"].as_str().expect("flow id").to_string();
    let (status, draft) = send(
        state.clone(),
        Method::PUT,
        &format!("/api/automation/flows/{flow_id}/draft"),
        Some(json!({ "flow_json": document(seconds) })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{draft}");
    let revision = draft["revision"].as_i64().expect("revision");
    let (status, published) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/publish"),
        Some(json!({ "expected_revision": revision, "change_note": "events" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{published}");
    flow_id
}

async fn start_run(state: &AppState, flow_id: &str) -> String {
    let (status, accepted) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    accepted["run_id"].as_str().expect("run id").to_string()
}

fn parse_events(body: &str) -> Vec<(String, Value)> {
    let mut events = Vec::new();
    for block in body.split("\n\n") {
        let mut kind = None;
        let mut data = None;
        for line in block.lines() {
            if let Some(rest) = line.strip_prefix("event:") {
                kind = Some(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("data:") {
                data = Some(rest.trim().to_string());
            }
        }
        if let (Some(kind), Some(data)) = (kind, data) {
            events.push((kind, serde_json::from_str(&data).expect("event json")));
        }
    }
    events
}

#[tokio::test]
async fn sse_stream_delivers_snapshot_messages_logs_and_terminal_events() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = AppState::new(
        AppConfig {
            automation_sse_poll_interval_ms: 100,
            automation_max_concurrent_runs: 100,
            automation_max_concurrent_runs_per_flow: 20,
            ..AppConfig::default()
        },
        Some(pool.clone()),
    )
    .expect("state");
    let flow_id = published_flow(&state, 2).await;
    let run_id = start_run(&state, &flow_id).await;
    let run_uuid: Uuid = run_id.parse().expect("run uuid");
    persistence::insert_log(
        &pool,
        run_uuid,
        Some("n1"),
        "info",
        "event coverage",
        Some(&json!({ "source": "test" })),
    )
    .await
    .expect("log row");

    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/api/automation/runs/{run_id}/events"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-cache")
    );

    let body = tokio::time::timeout(Duration::from_secs(15), response.into_body().collect())
        .await
        .expect("stream finished")
        .expect("body")
        .to_bytes();
    let events = parse_events(&String::from_utf8_lossy(&body));

    let kinds: Vec<&str> = events.iter().map(|(kind, _)| kind.as_str()).collect();
    for expected in [
        "snapshot",
        "message",
        "log",
        "node.status",
        "run.status",
        "end",
    ] {
        assert!(kinds.contains(&expected), "missing {expected}: {kinds:?}");
    }
    let snapshot = &events
        .iter()
        .find(|(kind, _)| kind == "snapshot")
        .expect("snapshot")
        .1;
    assert_eq!(snapshot["run_id"], run_id);
    assert_eq!(snapshot["flow_version_number"], 1);
    let end = &events
        .iter()
        .find(|(kind, _)| kind == "end")
        .expect("end")
        .1;
    assert_eq!(end["status"], "succeeded");
    assert_eq!(end["run_id"], run_id);
}

#[tokio::test]
async fn sse_stream_rejects_unknown_runs_missing_pool_and_the_stream_cap() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = AppState::new(
        AppConfig {
            automation_max_concurrent_runs: 100,
            automation_max_concurrent_runs_per_flow: 20,
            ..AppConfig::default()
        },
        Some(pool.clone()),
    )
    .expect("state");
    let flow_id = published_flow(&state, 0).await;
    let run_id = start_run(&state, &flow_id).await;

    let (status, missing) = request_events(state.clone(), &Uuid::new_v4().to_string()).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");

    let (status, no_pool) = request_events(automation_common::state(None), &run_id).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{no_pool}");

    let disabled = AppState::new(
        AppConfig {
            automation_enabled: false,
            ..AppConfig::default()
        },
        Some(pool.clone()),
    )
    .expect("disabled state");
    let (status, disabled) = request_events(disabled, &run_id).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{disabled}");

    let capped = AppState::new(
        AppConfig {
            automation_max_sse_streams: 0,
            ..AppConfig::default()
        },
        Some(pool),
    )
    .expect("capped state");
    let (status, limited) = request_events(capped, &run_id).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{limited}");
    assert_eq!(limited["error"]["message"], "too many concurrent streams");
}

async fn request_events(state: AppState, run_id: &str) -> (StatusCode, Value) {
    let response = router(state)
        .oneshot(
            Request::builder()
                .uri(format!("/api/automation/runs/{run_id}/events"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, value)
}

#[tokio::test]
async fn sse_stream_ends_when_the_maximum_duration_elapses() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = AppState::new(
        AppConfig {
            automation_sse_poll_interval_ms: 100,
            automation_sse_max_duration_seconds: 1,
            automation_max_concurrent_runs: 100,
            automation_max_concurrent_runs_per_flow: 20,
            ..AppConfig::default()
        },
        Some(pool),
    )
    .expect("state");
    let flow_id = published_flow(&state, 60).await;
    let run_id = start_run(&state, &flow_id).await;

    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/api/automation/runs/{run_id}/events"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = tokio::time::timeout(Duration::from_secs(10), response.into_body().collect())
        .await
        .expect("stream ended at the duration cap")
        .expect("body")
        .to_bytes();
    let events = parse_events(&String::from_utf8_lossy(&body));
    assert!(
        events.iter().any(|(kind, _)| kind == "snapshot"),
        "missing snapshot"
    );
    assert!(
        !events.iter().any(|(kind, _)| kind == "end"),
        "non-terminal run must not end"
    );

    for _ in 0..100 {
        let (status, body) = send(
            state.clone(),
            Method::GET,
            &format!("/api/automation/runs/{run_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if body["status"] == "running" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/runs/{run_id}/cancel"),
        None,
    )
    .await;
    for _ in 0..200 {
        let (status, body) = send(
            state.clone(),
            Method::GET,
            &format!("/api/automation/runs/{run_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if body["status"] == "cancelled" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("run {run_id} did not stop after cancellation");
}
