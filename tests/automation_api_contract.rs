//! Automation catalog and contract-surface tests (no database required).

mod automation_common;

use axum::http::{Method, StatusCode};
use serde_json::Value;

use obolargus_core::automation::flow::document::FlowDocument;
use obolargus_core::automation::flow::validate::validate_flow;
use obolargus_core::automation::nodes::default_registry;
use obolargus_server::config::AppConfig;

use automation_common::{send, state};

#[tokio::test]
async fn node_type_catalog_lists_executable_types_only() {
    let (status, body) = send(state(None), Method::GET, "/api/automation/node-types", None).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().expect("items");
    assert!(items.len() >= 15);
    let ids: Vec<&str> = items
        .iter()
        .filter_map(|item| item["id"].as_str())
        .collect();
    for expected in ["inject", "debug", "data_load", "data_load_range", "subflow"] {
        assert!(ids.contains(&expected), "missing {expected}");
    }
    assert!(!ids.contains(&"subflow_input"));
    assert!(!ids.contains(&"subflow_output"));
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "catalog must be deterministic");
    let data_load = items
        .iter()
        .find(|item| item["id"] == "data_load")
        .expect("data_load");
    assert!(data_load["config_schema"].is_object());
    assert!(data_load["capabilities"].is_array());
}

#[tokio::test]
async fn template_catalog_exposes_a_valid_data_load_flow() {
    let (status, body) = send(state(None), Method::GET, "/api/automation/templates", None).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().expect("items");
    assert!(
        items
            .iter()
            .any(|item| item["id"] == "data-load-single-date")
    );

    let (status, detail) = send(
        state(None),
        Method::GET,
        "/api/automation/templates/data-load-single-date",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let document: FlowDocument =
        serde_json::from_value(detail["flow_json"].clone()).expect("template flow parses");
    let report = validate_flow(&document, &default_registry());
    assert!(report.is_valid(), "{:?}", report.errors);
    assert!(detail["inputs_schema"]["properties"].is_object());
}

#[tokio::test]
async fn unknown_template_returns_not_found() {
    let (status, body) = send(
        state(None),
        Method::GET,
        "/api/automation/templates/does-not-exist",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn automation_disabled_returns_service_unavailable() {
    let config = AppConfig {
        automation_enabled: false,
        ..AppConfig::default()
    };
    let state = obolargus_server::AppState::new(config, None).expect("state");
    let (status, body) = send(state, Method::GET, "/api/automation/node-types", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "service_unavailable");
}

#[tokio::test]
async fn flow_routes_require_a_database() {
    let (status, body) = send(state(None), Method::GET, "/api/automation/flows", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "service_unavailable");

    let (status, _) = send(
        state(None),
        Method::POST,
        "/api/automation/flows",
        Some(serde_json::json!({ "name": "no database" })),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn invalid_flow_json_is_rejected_before_storage() {
    let pool = match automation_common::migrated_pool().await {
        Some(pool) => pool,
        None => return,
    };
    let (status, flow) = send(
        state(Some(pool.clone())),
        Method::POST,
        "/api/automation/flows",
        Some(serde_json::json!({ "name": "invalid draft" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let flow_id = flow["id"].as_str().expect("flow id");

    let (status, body) = send(
        state(Some(pool)),
        Method::PUT,
        &format!("/api/automation/flows/{flow_id}/draft"),
        Some(serde_json::json!({ "flow_json": Value::Null })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "validation_error");
}

#[test]
fn automation_operations_are_in_the_openapi_document() {
    use std::collections::BTreeSet;

    let document = serde_json::to_value(obolargus_server::openapi_document()).expect("document");
    let paths = document["paths"].as_object().expect("paths");
    let mut actual: BTreeSet<(String, String)> = BTreeSet::new();
    for (path, item) in paths {
        if !path.starts_with("/api/automation/") {
            continue;
        }
        for method in ["get", "post", "put", "patch", "delete"] {
            if item.get(method).is_some() {
                actual.insert((method.to_string(), path.clone()));
            }
        }
    }
    let expected: BTreeSet<(String, String)> = [
        ("delete", "/api/automation/flows/{flow_id}"),
        ("delete", "/api/automation/flows/{flow_id}/context"),
        ("delete", "/api/automation/triggers/{trigger_id}"),
        ("get", "/api/automation/flows"),
        ("get", "/api/automation/flows/{flow_id}"),
        ("get", "/api/automation/flows/{flow_id}/audit"),
        ("get", "/api/automation/flows/{flow_id}/context"),
        ("get", "/api/automation/flows/{flow_id}/triggers"),
        ("get", "/api/automation/flows/{flow_id}/versions"),
        (
            "get",
            "/api/automation/flows/{flow_id}/versions/{version_number}",
        ),
        (
            "get",
            "/api/automation/flows/{flow_id}/versions/{version_number}/export",
        ),
        ("get", "/api/automation/node-types"),
        ("get", "/api/automation/runs"),
        ("get", "/api/automation/runs/{run_id}"),
        ("get", "/api/automation/runs/{run_id}/events"),
        ("get", "/api/automation/runs/{run_id}/logs"),
        ("get", "/api/automation/runs/{run_id}/messages"),
        ("get", "/api/automation/runs/{run_id}/nodes/{node_id}"),
        ("get", "/api/automation/templates"),
        ("get", "/api/automation/templates/{template_id}"),
        ("patch", "/api/automation/flows/{flow_id}"),
        ("patch", "/api/automation/triggers/{trigger_id}"),
        ("post", "/api/automation/flows"),
        ("post", "/api/automation/flows/{flow_id}/archive"),
        ("post", "/api/automation/flows/{flow_id}/import"),
        ("post", "/api/automation/flows/{flow_id}/publish"),
        ("post", "/api/automation/flows/{flow_id}/restore"),
        ("post", "/api/automation/flows/{flow_id}/runs"),
        ("post", "/api/automation/flows/{flow_id}/triggers"),
        ("post", "/api/automation/flows/{flow_id}/validate"),
        (
            "post",
            "/api/automation/flows/{flow_id}/versions/{version_number}/archive",
        ),
        ("post", "/api/automation/runs/{run_id}/cancel"),
        ("post", "/api/automation/runs/{run_id}/retry"),
        ("post", "/api/automation/triggers/{trigger_id}/invoke"),
        ("put", "/api/automation/flows/{flow_id}/draft"),
    ]
    .into_iter()
    .map(|(method, path)| (method.to_string(), path.to_string()))
    .collect();
    assert_eq!(actual, expected);
    for (_, item) in paths
        .iter()
        .filter(|(path, _)| path.starts_with("/api/automation/"))
    {
        for method in ["get", "post", "put", "patch", "delete"] {
            if let Some(operation) = item.get(method) {
                assert_eq!(operation["tags"], serde_json::json!(["automation"]));
            }
        }
    }
}
