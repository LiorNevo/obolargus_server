//! Scheduler policy, trigger CRUD, and API-trigger tests (US5).

mod automation_common;

use axum::http::{Method, StatusCode};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

use obolargus_core::automation::triggers::{next_cron, next_interval, next_occurrence};
use obolargus_server::AppState;

use automation_common::{migrated_pool, send, send_with_headers, state};

#[test]
fn interval_schedules_advance_by_the_configured_step() {
    let from = Utc.with_ymd_and_hms(2026, 11, 8, 10, 0, 0).unwrap();
    let next = next_interval(from, 90).expect("next");
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 11, 8, 10, 1, 30).unwrap());
    assert!(next_interval(from, 0).is_err());
}

#[test]
fn cron_schedules_preserve_the_local_time_across_dst() {
    // US Eastern springs forward on 2026-03-08 at 02:00 local. A 06:00 local
    // schedule stays at 06:00 local, so the UTC instant moves from 11:00Z to
    // 10:00Z.
    let before = Utc.with_ymd_and_hms(2026, 3, 6, 12, 0, 0).unwrap();
    let first = next_cron("0 0 6 * * *", "America/New_York", before).expect("first");
    assert_eq!(first, Utc.with_ymd_and_hms(2026, 3, 7, 11, 0, 0).unwrap());
    let second = next_cron("0 0 6 * * *", "America/New_York", first).expect("second");
    assert_eq!(second, Utc.with_ymd_and_hms(2026, 3, 8, 10, 0, 0).unwrap());
}

#[test]
fn invalid_schedules_are_rejected() {
    let from = Utc.with_ymd_and_hms(2026, 11, 8, 10, 0, 0).unwrap();
    assert!(next_cron("not a cron", "UTC", from).is_err());
    assert!(next_cron("0 0 6 * * *", "Mars/Olympus", from).is_err());
    assert!(next_occurrence("weekly", None, None, "UTC", from).is_err());
    assert!(next_occurrence("interval", None, None, "UTC", from).is_err());
    assert!(next_occurrence("cron", None, None, "UTC", from).is_err());
}

#[test]
fn overlap_and_missed_policies_are_classified() {
    use obolargus_server::automation::scheduler::{
        cancel_previous_on_overlap, is_missed, skip_on_overlap,
    };

    assert!(skip_on_overlap("skip", 1));
    assert!(!skip_on_overlap("skip", 0));
    assert!(!skip_on_overlap("queue", 1));
    assert!(cancel_previous_on_overlap("cancel_previous", 2));
    assert!(!cancel_previous_on_overlap("cancel_previous", 0));
    assert!(!cancel_previous_on_overlap("skip", 1));

    let now = Utc.with_ymd_and_hms(2026, 11, 8, 10, 0, 0).unwrap();
    assert!(is_missed(now - chrono::Duration::minutes(5), now));
    assert!(!is_missed(now, now));
    assert!(!is_missed(now + chrono::Duration::seconds(30), now));
}

fn sample_flow() -> Value {
    json!({
        "schema_version": 1,
        "name": "trigger flow",
        "nodes": [
            { "id": "n1", "type": "inject", "type_version": 1, "config": { "repeat": "none", "once": true } },
            { "id": "n2", "type": "debug", "type_version": 1, "config": { "target": "msg" } }
        ],
        "wires": [
            { "id": "w1", "source": "n1", "source_port": "out", "target": "n2", "target_port": "in" }
        ]
    })
}

async fn create_and_publish(state: &AppState, flow: Value) -> Option<String> {
    let (status, created) = send(
        state.clone(),
        Method::POST,
        "/api/automation/flows",
        Some(json!({ "name": format!("trigger-{}", uuid::Uuid::new_v4()) })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let flow_id = created["id"].as_str()?.to_string();
    let (status, draft) = send(
        state.clone(),
        Method::PUT,
        &format!("/api/automation/flows/{flow_id}/draft"),
        Some(json!({ "flow_json": flow })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{draft}");
    let revision = draft["revision"].as_i64()?;
    let (status, published) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/publish"),
        Some(json!({ "expected_revision": revision, "change_note": "test" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{published}");
    Some(flow_id)
}

#[tokio::test]
async fn publish_reconciles_repeating_injects_into_durable_triggers() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let mut flow = sample_flow();
    flow["nodes"][0]["config"] = json!({
        "repeat": "interval",
        "interval_seconds": 60,
        "once": true,
        "timezone": "UTC"
    });
    let Some(flow_id) = create_and_publish(&state, flow).await else {
        panic!("flow setup failed");
    };
    let (status, body) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/triggers"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let items = body["items"].as_array().expect("items");
    assert!(
        items
            .iter()
            .any(|item| item["kind"] == "inject" && item["repeat_kind"] == "interval"),
        "{body}"
    );
}

#[tokio::test]
async fn api_trigger_invocation_is_idempotent_and_honors_disable() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let Some(flow_id) = create_and_publish(&state, sample_flow()).await else {
        panic!("flow setup failed");
    };
    let (status, trigger) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/triggers"),
        Some(json!({ "kind": "api" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{trigger}");
    let trigger_id = trigger["id"].as_str().expect("trigger id");

    let key = format!("idem-{}", uuid::Uuid::new_v4());
    let (status, first) = send_with_headers(
        state.clone(),
        Method::POST,
        &format!("/api/automation/triggers/{trigger_id}/invoke"),
        Some(json!({ "inputs": { "a": 1 } })),
        &[("idempotency-key", key.as_str())],
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{first}");
    let (status, second) = send_with_headers(
        state.clone(),
        Method::POST,
        &format!("/api/automation/triggers/{trigger_id}/invoke"),
        Some(json!({ "inputs": { "a": 1 } })),
        &[("idempotency-key", key.as_str())],
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{second}");
    assert_eq!(first["run_id"], second["run_id"]);

    let (status, _) = send_with_headers(
        state.clone(),
        Method::POST,
        &format!("/api/automation/triggers/{trigger_id}/invoke"),
        Some(json!({ "inputs": { "a": 2 } })),
        &[("idempotency-key", key.as_str())],
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, updated) = send(
        state.clone(),
        Method::PATCH,
        &format!("/api/automation/triggers/{trigger_id}"),
        Some(json!({ "enabled": false })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    let (status, disabled) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/triggers/{trigger_id}/invoke"),
        Some(json!({ "inputs": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(disabled["error"]["code"], "conflict");
}

#[tokio::test]
async fn trigger_updates_validate_policies() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let Some(flow_id) = create_and_publish(&state, sample_flow()).await else {
        panic!("flow setup failed");
    };
    let (status, trigger) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/triggers"),
        Some(json!({ "kind": "api" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{trigger}");
    let trigger_id = trigger["id"].as_str().expect("trigger id");
    let (status, body) = send(
        state.clone(),
        Method::PATCH,
        &format!("/api/automation/triggers/{trigger_id}"),
        Some(json!({ "overlap_policy": "sometimes" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "validation_error");
    let (status, _) = send(
        state.clone(),
        Method::DELETE,
        &format!("/api/automation/triggers/{trigger_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn inject_triggers_cannot_be_created_manually() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let Some(flow_id) = create_and_publish(&state, sample_flow()).await else {
        panic!("flow setup failed");
    };
    let (status, body) = send(
        state,
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/triggers"),
        Some(json!({ "kind": "inject" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "validation_error");
}

#[allow(dead_code)]
fn parse_instant(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("instant")
        .with_timezone(&Utc)
}
