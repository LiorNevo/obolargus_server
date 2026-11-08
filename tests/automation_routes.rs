//! Publish, version, audit, and JSON interchange tests (US6).

mod automation_common;

use axum::http::{Method, StatusCode};
use serde_json::{Value, json};

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use tower::ServiceExt;

use automation_common::{migrated_pool, send, state};

fn valid_flow(name: &str) -> Value {
    json!({
        "schema_version": 1,
        "name": name,
        "nodes": [
            { "id": "n1", "type": "inject", "type_version": 1, "config": { "repeat": "none", "once": true } },
            { "id": "n2", "type": "debug", "type_version": 1, "config": { "target": "msg" } }
        ],
        "wires": [
            { "id": "w1", "source": "n1", "source_port": "out", "target": "n2", "target_port": "in" }
        ]
    })
}

async fn create_flow(state: &obolargus_server::AppState, name: &str) -> String {
    let (status, created) = send(
        state.clone(),
        Method::POST,
        "/api/automation/flows",
        Some(json!({ "name": name, "tags": ["test"] })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    created["id"].as_str().expect("flow id").to_string()
}

async fn save_draft(state: &obolargus_server::AppState, flow_id: &str, flow: Value) -> i64 {
    let (status, body) = send(
        state.clone(),
        Method::PUT,
        &format!("/api/automation/flows/{flow_id}/draft"),
        Some(json!({ "flow_json": flow })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["revision"].as_i64().expect("revision")
}

async fn publish(state: &obolargus_server::AppState, flow_id: &str, revision: i64) -> Value {
    let (status, body) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/publish"),
        Some(json!({ "expected_revision": revision, "change_note": "publish" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

#[tokio::test]
async fn versions_are_immutable_and_runs_pin_the_published_version() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = create_flow(&state, "versions").await;
    let revision = save_draft(&state, &flow_id, valid_flow("versions v1")).await;
    let v1 = publish(&state, &flow_id, revision).await;
    assert_eq!(v1["version_number"], 1);

    let revision = save_draft(&state, &flow_id, valid_flow("versions v2")).await;
    let v2 = publish(&state, &flow_id, revision).await;
    assert_eq!(v2["version_number"], 2);

    let (status, versions) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/versions"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{versions}");
    assert_eq!(versions["items"].as_array().expect("items").len(), 2);

    let (status, fetched) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/versions/1"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{fetched}");
    assert_eq!(fetched["flow_json"]["name"], "versions v1");

    // New runs execute the newly published version.
    let (status, accepted) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({ "inputs": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    assert_eq!(accepted["flow_version_number"], 2);
}

#[tokio::test]
async fn export_import_round_trips_known_nodes_and_reports_unknown_types() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = create_flow(&state, "export").await;
    let revision = save_draft(&state, &flow_id, valid_flow("export v1")).await;
    publish(&state, &flow_id, revision).await;

    let response = obolargus_server::router(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/api/automation/flows/{flow_id}/versions/1/export"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get("content-disposition")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("attachment"))
    );
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let exported: Value = serde_json::from_slice(&bytes).expect("exported json");
    assert_eq!(exported["name"], "export v1");

    let target = create_flow(&state, "import").await;
    let (status, imported) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{target}/import"),
        Some(json!({ "flow_json": exported })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{imported}");
    assert_eq!(
        imported["flow_json"]["nodes"]
            .as_array()
            .expect("nodes")
            .len(),
        2
    );
    assert!(
        imported["unknown_node_types"]
            .as_array()
            .expect("unknown")
            .is_empty()
    );

    let mut with_unknown = valid_flow("import unknown");
    with_unknown["nodes"]
        .as_array_mut()
        .expect("nodes")
        .push(json!({
            "id": "x1",
            "type": "mystery",
            "type_version": 9,
            "config": {}
        }));
    with_unknown["wires"]
        .as_array_mut()
        .expect("wires")
        .push(json!({
            "id": "wx",
            "source": "n2",
            "source_port": "out",
            "target": "x1",
            "target_port": "in"
        }));
    let (status, imported) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{target}/import"),
        Some(json!({ "flow_json": with_unknown, "replace_draft": true })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{imported}");
    let unknown = imported["unknown_node_types"].as_array().expect("unknown");
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0]["type"], "mystery");
    assert_eq!(unknown[0]["version"], 9);
    let node_ids: Vec<&str> = imported["flow_json"]["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .filter_map(|node| node["id"].as_str())
        .collect();
    assert_eq!(node_ids, vec!["n1", "n2"]);
    let wire_ids: Vec<&str> = imported["flow_json"]["wires"]
        .as_array()
        .expect("wires")
        .iter()
        .filter_map(|wire| wire["id"].as_str())
        .collect();
    assert_eq!(wire_ids, vec!["w1"]);
}

#[tokio::test]
async fn publish_validates_the_draft_and_requires_a_change_note() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = create_flow(&state, "publish validation").await;
    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/publish"),
        Some(json!({ "expected_revision": 0, "change_note": "   " })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let mut invalid = valid_flow("invalid");
    invalid["nodes"][1]["type"] = json!("does-not-exist");
    let revision = save_draft(&state, &flow_id, invalid).await;
    let (status, body) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/publish"),
        Some(json!({ "expected_revision": revision, "change_note": "bad" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "validation_error");
    assert!(body["error"]["details"]["errors"].is_array());
}

#[tokio::test]
async fn audit_history_records_flow_lifecycle_actions() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = create_flow(&state, "audit").await;
    let revision = save_draft(&state, &flow_id, valid_flow("audit v1")).await;
    publish(&state, &flow_id, revision).await;

    let (status, audit) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/audit"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{audit}");
    let actions: Vec<&str> = audit["items"]
        .as_array()
        .expect("items")
        .iter()
        .filter_map(|event| event["action"].as_str())
        .collect();
    for expected in ["flow.create", "version.publish"] {
        assert!(
            actions.contains(&expected),
            "missing {expected}: {actions:?}"
        );
    }
}
