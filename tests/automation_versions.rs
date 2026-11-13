//! Version, export, import, and publish-reconciliation coverage.

mod automation_common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use obolargus_core::automation::runtime::persistence;
use obolargus_server::AppState;
use obolargus_server::config::AppConfig;

use automation_common::{migrated_pool, send, state};

fn document(name: &str) -> Value {
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

async fn create_flow(state: &AppState, name: &str) -> (String, i64) {
    let name = format!("{name}-{}", Uuid::new_v4());
    let (status, created) = send(
        state.clone(),
        Method::POST,
        "/api/automation/flows",
        Some(json!({ "name": name })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    (
        created["id"].as_str().expect("flow id").to_string(),
        created["draft"]["revision"].as_i64().expect("revision"),
    )
}

async fn save_draft(state: &AppState, flow_id: &str, flow: Value) -> i64 {
    let (status, draft) = send(
        state.clone(),
        Method::PUT,
        &format!("/api/automation/flows/{flow_id}/draft"),
        Some(json!({ "flow_json": flow })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{draft}");
    draft["revision"].as_i64().expect("revision")
}

async fn publish(state: &AppState, flow_id: &str, revision: i64) -> (StatusCode, Value) {
    send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/publish"),
        Some(json!({ "expected_revision": revision, "change_note": "coverage" })),
    )
    .await
}

async fn published_flow(state: &AppState, name: &str) -> String {
    let (flow_id, revision) = create_flow(state, name).await;
    let revision = save_draft(state, &flow_id, document(name))
        .await
        .max(revision);
    let (status, body) = publish(state, &flow_id, revision).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    flow_id
}

#[tokio::test]
async fn publish_rejects_missing_drafts_invalid_json_and_validation_errors() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool.clone()));
    let (flow_id, revision) = create_flow(&state, "publish").await;

    let (status, body) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/publish"),
        Some(json!({ "expected_revision": revision, "change_note": "  " })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["message"], "version.change_note_required");

    let revision = save_draft(&state, &flow_id, document("publish")).await;
    let (status, body) = publish(&state, &flow_id, revision + 50).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "conflict");

    let no_draft = Uuid::new_v4();
    sqlx::query("INSERT INTO automation_flows (id, owner_id, name) VALUES ($1, $2, $3)")
        .bind(no_draft)
        .bind("dev-user")
        .bind(format!("no-draft-{no_draft}"))
        .execute(&pool)
        .await
        .expect("insert flow");
    let (status, body) = publish(&state, &no_draft.to_string(), 1).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["message"], "draft.none");

    let flow_uuid: Uuid = flow_id.parse().expect("uuid");
    let saved = persistence::upsert_draft(
        &pool,
        flow_uuid,
        None,
        &json!({ "schema_version": 1 }),
        "dev-user",
    )
    .await
    .expect("invalid draft");
    let (status, body) = publish(&state, &flow_id, saved.revision).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .starts_with("flow.json_invalid"),
        "{body}"
    );

    let validation_flow = published_flow(&state, "validation").await;
    let mut invalid = document("validation");
    invalid["nodes"][0]["type"] = json!("not_a_real_node");
    let revision = save_draft(&state, &validation_flow, invalid).await;
    let (status, body) = publish(&state, &validation_flow, revision).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["message"], "the flow has validation errors");
    assert!(body["error"]["details"]["errors"].is_array());
}

#[tokio::test]
async fn versions_list_get_export_and_archive_round_trip() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool.clone()));
    let name = format!("Export Flow! {}", Uuid::new_v4());
    let flow_id = published_flow(&state, &name).await;

    let (status, page) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/versions?limit=0&offset=-1"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["limit"], 1);
    assert_eq!(page["offset"], 0);
    assert_eq!(page["total"], 1);
    assert_eq!(page["items"][0]["version_number"], 1);

    let (status, version) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/versions/1"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{version}");
    assert_eq!(version["status"], "published");
    assert!(version["flow_json"].is_object());
    assert!(version["validation"].is_object());

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
    let disposition = response
        .headers()
        .get(header::CONTENT_DISPOSITION)
        .and_then(|value| value.to_str().ok())
        .expect("content disposition");
    assert!(disposition.starts_with("attachment; filename=\"Export-Flow"));
    assert!(disposition.ends_with("-v1.json\""));
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let exported: Value = serde_json::from_slice(&bytes).expect("export json");
    assert_eq!(exported["schema_version"], 1);

    let (status, _) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/versions/99"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/versions/99/export"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, archived) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/versions/1/archive"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived}");
    assert_eq!(archived["status"], "archived");

    let (status, conflict) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/versions/1/archive"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    assert_eq!(conflict["error"]["message"], "version.not_published");

    // A stored version whose JSON no longer parses reports empty validation.
    let flow_uuid: Uuid = flow_id.parse().expect("uuid");
    sqlx::query(
        "INSERT INTO automation_flow_versions \
         (id, flow_id, version_number, status, flow_json, created_by, published_by, published_at) \
         VALUES ($1, $2, 42, 'archived', $3, 'test', 'test', NOW())",
    )
    .bind(Uuid::new_v4())
    .bind(flow_uuid)
    .bind(json!({ "schema_version": 1 }))
    .execute(&pool)
    .await
    .expect("insert invalid version");
    let (status, version) = send(
        state,
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/versions/42"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{version}");
    assert_eq!(version["validation"]["errors"], json!([]));
    assert_eq!(version["validation"]["warnings"], json!([]));
}

#[tokio::test]
async fn import_rejects_sizes_versions_and_unknown_only_documents() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool.clone()));
    let (flow_id, _) = create_flow(&state, "import").await;
    let uri = format!("/api/automation/flows/{flow_id}/import");

    let (status, body) = send(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "flow_json": { "foo": 1 }, "replace_draft": true })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .starts_with("flow.schema_invalid"),
        "{body}"
    );

    let (status, body) = send(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "flow_json": { "schema_version": 999, "name": "future" }, "replace_draft": true })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["message"], "flow.unknown_version");

    let unknown_only = json!({
        "schema_version": 1,
        "name": "unknown only",
        "nodes": [
            { "id": "a", "type": "ghost", "type_version": 1, "config": {} }
        ]
    });
    let (status, body) = send(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "flow_json": unknown_only, "replace_draft": true })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["error"]["message"],
        "flow.unknown_type: no supported nodes remain"
    );

    let small = AppState::new(
        AppConfig {
            automation_max_flow_bytes: 32,
            ..AppConfig::default()
        },
        Some(pool),
    )
    .expect("small state");
    let (status, body) = send(
        small,
        Method::POST,
        &uri,
        Some(json!({ "flow_json": document("oversized"), "replace_draft": true })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(body["error"]["message"], "flow.definition_too_large");

    let mixed = json!({
        "schema_version": 1,
        "name": "mixed import",
        "nodes": [
            { "id": "n1", "type": "inject", "type_version": 1, "config": { "repeat": "none" } },
            { "id": "a", "type": "ghost", "type_version": 1, "config": {} },
            { "id": "b", "type": "ghost", "type_version": 1, "config": {} },
            { "id": "c", "type": "ghost", "type_version": 2, "config": {} }
        ],
        "wires": [
            { "id": "w1", "source": "a", "target": "n1" },
            { "id": "w2", "source": "n1", "target": "n1" }
        ]
    });
    let (status, imported) = send(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "flow_json": mixed, "replace_draft": true })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{imported}");
    let unknown = imported["unknown_node_types"]
        .as_array()
        .expect("unknown types");
    assert_eq!(unknown.len(), 2);
    assert!(
        unknown
            .iter()
            .any(|entry| entry["type"] == "ghost" && entry["version"] == 1 && entry["count"] == 2)
    );
    assert!(
        unknown
            .iter()
            .any(|entry| entry["type"] == "ghost" && entry["version"] == 2 && entry["count"] == 1)
    );
    assert_eq!(
        imported["flow_json"]["nodes"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        imported["flow_json"]["wires"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(imported["flow_json"]["wires"][0]["source"], "n1");
}

#[tokio::test]
async fn publish_reconciles_inject_schedules_into_triggers() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let (flow_id, revision) = create_flow(&state, "schedules").await;
    let flow = json!({
        "schema_version": 1,
        "name": "schedules",
        "nodes": [
            { "id": "i1", "type": "inject", "type_version": 1, "config": {
                "repeat": "interval", "interval_seconds": 60, "once": true
            } },
            { "id": "i2", "type": "inject", "type_version": 1, "config": {
                "repeat": "cron", "cron": "0 0 6 * * *", "timezone": "America/New_York"
            } },
            { "id": "i3", "type": "inject", "type_version": 1, "config": {
                "repeat": "none", "once": true
            } },
            { "id": "d1", "type": "debug", "type_version": 1, "config": { "target": "msg" } }
        ],
        "wires": [
            { "id": "w1", "source": "i1", "source_port": "out", "target": "d1", "target_port": "in" },
            { "id": "w2", "source": "i2", "source_port": "out", "target": "d1", "target_port": "in" },
            { "id": "w3", "source": "i3", "source_port": "out", "target": "d1", "target_port": "in" }
        ]
    });
    let revision = save_draft(&state, &flow_id, flow).await.max(revision);
    let (status, published) = publish(&state, &flow_id, revision).await;
    assert_eq!(status, StatusCode::CREATED, "{published}");

    let (status, triggers) = send(
        state,
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/triggers"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{triggers}");
    let items = triggers["items"].as_array().expect("items");
    let nodes: Vec<&str> = items
        .iter()
        .filter(|item| item["kind"] == "inject")
        .filter_map(|item| item["node_id"].as_str())
        .collect();
    assert!(nodes.contains(&"i1"), "{triggers}");
    assert!(nodes.contains(&"i2"), "{triggers}");
    assert!(!nodes.contains(&"i3"), "{triggers}");
    assert!(
        items
            .iter()
            .any(|item| item["node_id"] == "i1" && item["repeat_kind"] == "interval")
    );
    assert!(
        items
            .iter()
            .any(|item| item["node_id"] == "i2" && item["repeat_kind"] == "cron")
    );
}
