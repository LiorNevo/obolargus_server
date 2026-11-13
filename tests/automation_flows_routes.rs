//! Flow CRUD, draft, validation, and context route coverage.

mod automation_common;

use axum::http::{Method, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

use obolargus_core::automation::runtime::persistence;
use obolargus_server::AppState;
use obolargus_server::config::AppConfig;

use automation_common::{migrated_pool, send, state};

fn document(name: &str, repeat: &str) -> Value {
    let inject = match repeat {
        "interval" => json!({
            "repeat": "interval",
            "interval_seconds": 60,
            "once": true,
            "timezone": "UTC"
        }),
        _ => json!({ "repeat": "none", "once": true }),
    };
    json!({
        "schema_version": 1,
        "name": name,
        "description": "coverage",
        "tags": ["coverage"],
        "nodes": [
            { "id": "n1", "type": "inject", "type_version": 1, "config": inject },
            { "id": "n2", "type": "delay", "type_version": 1, "config": { "mode": "delay", "seconds": 0 } },
            { "id": "n3", "type": "debug", "type_version": 1, "config": { "target": "msg" } }
        ],
        "wires": [
            { "id": "w1", "source": "n1", "source_port": "out", "target": "n2", "target_port": "in" },
            { "id": "w2", "source": "n2", "source_port": "out", "target": "n3", "target_port": "in" }
        ]
    })
}

async fn create_flow(state: &AppState, name: &str) -> String {
    let name = format!("{name}-{}", Uuid::new_v4());
    let (status, created) = send(
        state.clone(),
        Method::POST,
        "/api/automation/flows",
        Some(json!({ "name": name, "tags": ["coverage"] })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    created["id"].as_str().expect("flow id").to_string()
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

async fn publish(state: &AppState, flow_id: &str, revision: i64) {
    let (status, published) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/publish"),
        Some(json!({ "expected_revision": revision, "change_note": "coverage" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{published}");
}

async fn published_flow(state: &AppState, repeat: &str) -> String {
    let flow_id = create_flow(state, "flow").await;
    let revision = save_draft(state, &flow_id, document("flow", repeat)).await;
    publish(state, &flow_id, revision).await;
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

#[tokio::test]
async fn create_flow_validates_inputs_and_templates() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let uri = "/api/automation/flows";

    for body in [
        json!({ "name": "   " }),
        json!({ "name": "n".repeat(201) }),
        json!({ "name": "long description", "description": "d".repeat(4_001) }),
        json!({ "name": "long tag", "tags": ["t".repeat(51)] }),
    ] {
        let (status, error) = send(state.clone(), Method::POST, uri, Some(body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        assert_eq!(error["error"]["code"], "validation_error");
    }

    let (status, missing) = send(
        state.clone(),
        Method::POST,
        uri,
        Some(json!({ "name": "template", "template_id": "does-not-exist" })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
    assert_eq!(missing["error"]["code"], "not_found");

    let name = format!("templated-{}", Uuid::new_v4());
    let (status, created) = send(
        state.clone(),
        Method::POST,
        uri,
        Some(json!({
            "name": name,
            "description": "from template",
            "tags": ["market-data"],
            "template_id": "data-load-single-date"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["name"], name);
    assert_eq!(created["description"], "from template");
    assert_eq!(created["tags"], json!(["market-data"]));
    assert_eq!(created["draft"]["flow_json"]["name"], name);
    assert_eq!(
        created["draft"]["flow_json"]["nodes"]
            .as_array()
            .map(Vec::len),
        Some(3)
    );
    assert!(created["current_version"].is_null());
}

#[tokio::test]
async fn list_flows_reports_versions_runs_and_schedules() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = AppState::new(
        AppConfig {
            automation_max_concurrent_runs: 100,
            automation_max_concurrent_runs_per_flow: 20,
            ..AppConfig::default()
        },
        Some(pool),
    )
    .expect("state");
    let flow_id = published_flow(&state, "interval").await;

    let (status, accepted) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/runs"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    wait_for_terminal(&state, accepted["run_id"].as_str().expect("run id")).await;

    let name = format!("flow-{}", Uuid::new_v4());
    let bare = create_flow(&state, &name).await;

    let (status, page) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows?search={name}&limit=0&offset=-1"),
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
        .find(|item| item["id"] == bare)
        .expect("created flow in page");
    assert!(item["current_version_number"].is_null());
    assert!(item["last_run"].is_null());
    assert!(item["next_scheduled_run_at"].is_null());

    let (status, page) = send(
        state.clone(),
        Method::GET,
        "/api/automation/flows?status=active&limit=200",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let item = page["items"]
        .as_array()
        .expect("items")
        .iter()
        .find(|item| item["id"] == flow_id)
        .expect("published flow in page");
    assert_eq!(item["current_version_number"], 1);
    assert_eq!(item["last_run"]["status"], "succeeded");
    assert!(item["next_scheduled_run_at"].is_string());

    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{bare}/archive"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, page) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows?status=archived&search={name}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["items"].as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn flow_detail_update_archive_restore_and_delete_round_trip() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool));
    let flow_id = create_flow(&state, "lifecycle").await;

    let (status, detail) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert!(detail["draft"]["flow_json"].is_object());
    assert!(detail["current_version"].is_null());

    let renamed = format!("renamed-{}", Uuid::new_v4());
    let (status, updated) = send(
        state.clone(),
        Method::PATCH,
        &format!("/api/automation/flows/{flow_id}"),
        Some(json!({
            "name": renamed,
            "description": "updated",
            "tags": ["one", "two"]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["name"], renamed);
    assert_eq!(updated["description"], "updated");
    assert_eq!(updated["tags"], json!(["one", "two"]));

    let (status, invalid) = send(
        state.clone(),
        Method::PATCH,
        &format!("/api/automation/flows/{flow_id}"),
        Some(json!({ "name": "  " })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
    assert_eq!(invalid["error"]["message"], "flow.name_empty");

    let unknown = Uuid::new_v4();
    let (status, _) = send(
        state.clone(),
        Method::PATCH,
        &format!("/api/automation/flows/{unknown}"),
        Some(json!({ "description": "missing" })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, archived) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/archive"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived}");
    assert_eq!(archived["status"], "archived");

    let (status, restored) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{flow_id}/restore"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{restored}");
    assert_eq!(restored["status"], "active");

    let (status, _) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{unknown}/restore"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state.clone(),
        Method::DELETE,
        &format!("/api/automation/flows/{flow_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        state.clone(),
        Method::DELETE,
        &format!("/api/automation/flows/{flow_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn save_draft_covers_validation_conflicts_and_size_limits() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool.clone()));
    let flow_id = create_flow(&state, "draft").await;
    let uri = format!("/api/automation/flows/{flow_id}/draft");

    let revision = save_draft(&state, &flow_id, document("draft", "none")).await;
    assert!(revision >= 2);

    let (status, conflict) = send(
        state.clone(),
        Method::PUT,
        &uri,
        Some(json!({
            "expected_revision": revision + 10,
            "flow_json": document("draft", "none")
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    assert_eq!(conflict["error"]["code"], "conflict");

    let (status, invalid) = send(
        state.clone(),
        Method::PUT,
        &uri,
        Some(json!({ "flow_json": { "schema_version": 1 } })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
    assert_eq!(invalid["error"]["code"], "validation_error");

    let (status, _) = send(
        state.clone(),
        Method::PUT,
        &format!("/api/automation/flows/{}/draft", Uuid::new_v4()),
        Some(json!({ "flow_json": document("missing", "none") })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let small = AppState::new(
        AppConfig {
            automation_max_flow_bytes: 32,
            ..AppConfig::default()
        },
        Some(pool),
    )
    .expect("small state");
    let (status, large) = send(
        small,
        Method::PUT,
        &uri,
        Some(json!({ "flow_json": document("oversized", "none") })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{large}");
    assert_eq!(large["error"]["message"], "flow.definition_too_large");
}

#[tokio::test]
async fn validate_uses_the_request_body_or_stored_draft() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool.clone()));
    let flow_id = create_flow(&state, "validate").await;
    let uri = format!("/api/automation/flows/{flow_id}/validate");
    save_draft(&state, &flow_id, document("validate", "none")).await;

    let (status, stored) = send(state.clone(), Method::POST, &uri, Some(json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{stored}");
    assert_eq!(stored["valid"], true);

    let (status, explicit) = send(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "flow_json": document("explicit", "none") })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{explicit}");
    assert_eq!(explicit["valid"], true);

    let (status, invalid_json) = send(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "flow_json": { "schema_version": 1 } })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid_json}");
    assert_eq!(invalid_json["error"]["code"], "validation_error");

    let mut invalid_graph = document("invalid", "none");
    invalid_graph["nodes"][0]["type"] = json!("not_a_real_node");
    let (status, invalid) = send(
        state.clone(),
        Method::POST,
        &uri,
        Some(json!({ "flow_json": invalid_graph })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{invalid}");
    assert_eq!(invalid["valid"], false);
    assert!(!invalid["errors"].as_array().expect("errors").is_empty());

    let no_draft = Uuid::new_v4();
    sqlx::query("INSERT INTO automation_flows (id, owner_id, name) VALUES ($1, $2, $3)")
        .bind(no_draft)
        .bind("dev-user")
        .bind(format!("no-draft-{no_draft}"))
        .execute(&pool)
        .await
        .expect("insert flow");
    let (status, missing) = send(
        state.clone(),
        Method::POST,
        &format!("/api/automation/flows/{no_draft}/validate"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{missing}");
    assert_eq!(missing["error"]["message"], "draft.none");

    let (status, detail) = send(
        state,
        Method::GET,
        &format!("/api/automation/flows/{no_draft}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert!(detail["draft"].is_null());
}

#[tokio::test]
async fn context_reads_and_deletes_flow_and_global_values() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let state = state(Some(pool.clone()));
    let flow_id = create_flow(&state, "context").await;
    persistence::set_context(&pool, "flow", &flow_id, "alpha", &json!(1), None)
        .await
        .expect("flow context");
    persistence::set_context(&pool, "global", "workspace", "beta", &json!(2), None)
        .await
        .expect("global context");

    let (status, view) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/context"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["flow"]["alpha"], 1);
    assert_eq!(view["global"]["beta"], 2);

    let (status, _) = send(
        state.clone(),
        Method::DELETE,
        &format!("/api/automation/flows/{flow_id}/context"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, view) = send(
        state.clone(),
        Method::GET,
        &format!("/api/automation/flows/{flow_id}/context"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert!(view["flow"].as_object().expect("flow map").is_empty());
    assert_eq!(view["global"]["beta"], 2);

    let (status, _) = send(
        state,
        Method::DELETE,
        &format!("/api/automation/flows/{flow_id}/context"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}
