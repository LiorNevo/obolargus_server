//! Contract tests for the interactive Swagger UI (US1).
//!
//! Proves the page, its assets and the machine-readable document are served
//! without a database, and that the page uses mount-relative URLs so it works
//! behind the gateway's `/obolargus/api` prefix.

mod common;

use axum::http::{StatusCode, header};
use serde_json::Value;

use common::{assert_status, body_text, get};

#[tokio::test]
async fn swagger_ui_index_renders_html_without_a_database() {
    let response = assert_status(get("/swagger-ui/").await, StatusCode::OK);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("text/html"),
        "expected text/html, got {content_type}"
    );

    let html = body_text(response).await;
    assert!(
        html.contains("swagger-ui"),
        "Swagger UI index did not reference its UI root: {html}"
    );
    assert!(
        html.contains("./swagger-ui-bundle.js"),
        "Swagger UI index must reference assets relatively: {html}"
    );
}

#[tokio::test]
async fn swagger_ui_uses_a_mount_relative_specification_url() {
    let response = assert_status(
        get("/swagger-ui/swagger-initializer.js").await,
        StatusCode::OK,
    );
    let initializer = body_text(response).await;

    assert!(
        initializer.contains("../api-docs/openapi.json"),
        "initializer must fetch the spec relative to the page: {initializer}"
    );
    assert!(
        !initializer.contains("http://127.0.0.1:8000")
            && !initializer.contains("http://localhost:8000"),
        "initializer must not name the internal server address: {initializer}"
    );
}

#[tokio::test]
async fn openapi_document_is_served_as_json_with_every_registered_path() {
    let response = assert_status(get("/api-docs/openapi.json").await, StatusCode::OK);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("application/json"),
        "expected application/json, got {content_type}"
    );

    let body = body_text(response).await;
    let document: Value = serde_json::from_str(&body).expect("valid JSON document");
    assert_eq!(document["openapi"], "3.1.0");
    assert_eq!(document["info"]["title"], "Obolargus API");
    assert_eq!(document["servers"], serde_json::json!([{ "url": "../.." }]));
    assert!(
        document["info"]["description"]
            .as_str()
            .is_some_and(|description| description.contains("../../api/domain/schema")),
        "the served document must link to the canonical data dictionary"
    );
    assert!(
        document["components"]["schemas"]["DataDictionary.Asset"].is_object(),
        "the served document must expose the data dictionary in Swagger UI"
    );
    assert_eq!(
        document["paths"]["/api/domain/schema"]["get"]["tags"],
        serde_json::json!(["data-dictionary"]),
        "the served document must expose a dedicated data-dictionary GET operation"
    );

    let mut paths: Vec<&str> = document["paths"]
        .as_object()
        .expect("paths object")
        .keys()
        .map(String::as_str)
        .collect();
    paths.sort_unstable();
    assert_eq!(
        paths,
        [
            "/api/assets",
            "/api/automation/flows",
            "/api/automation/flows/{flow_id}",
            "/api/automation/flows/{flow_id}/archive",
            "/api/automation/flows/{flow_id}/audit",
            "/api/automation/flows/{flow_id}/context",
            "/api/automation/flows/{flow_id}/draft",
            "/api/automation/flows/{flow_id}/import",
            "/api/automation/flows/{flow_id}/publish",
            "/api/automation/flows/{flow_id}/restore",
            "/api/automation/flows/{flow_id}/runs",
            "/api/automation/flows/{flow_id}/triggers",
            "/api/automation/flows/{flow_id}/validate",
            "/api/automation/flows/{flow_id}/versions",
            "/api/automation/flows/{flow_id}/versions/{version_number}",
            "/api/automation/flows/{flow_id}/versions/{version_number}/archive",
            "/api/automation/flows/{flow_id}/versions/{version_number}/export",
            "/api/automation/node-types",
            "/api/automation/runs",
            "/api/automation/runs/{run_id}",
            "/api/automation/runs/{run_id}/cancel",
            "/api/automation/runs/{run_id}/events",
            "/api/automation/runs/{run_id}/logs",
            "/api/automation/runs/{run_id}/messages",
            "/api/automation/runs/{run_id}/nodes/{node_id}",
            "/api/automation/runs/{run_id}/retry",
            "/api/automation/templates",
            "/api/automation/templates/{template_id}",
            "/api/automation/triggers/{trigger_id}",
            "/api/automation/triggers/{trigger_id}/invoke",
            "/api/data-load/domains",
            "/api/data-load/history",
            "/api/data-load/history/groups/{group_id}/progress",
            "/api/data-load/history/groups/{group_id}/stop",
            "/api/data-load/history/{entry_id}",
            "/api/data-load/identifier-search",
            "/api/data-load/sources",
            "/api/data-load/sources/{source_id}",
            "/api/domain/fact-types",
            "/api/domain/schema",
            "/api/domain/{subject}/{subject_id}/facts/{fact_type}",
            "/api/domain/{subject}/{subject_id}/facts/{fact_type}/history",
            "/api/domain/{subject}/{subject_id}/facts/{fact_type}/retract",
            "/api/domain/{subject}/{subject_id}/facts/{fact_type}/series",
            "/api/market-data/load",
            "/api/market-data/load-range",
            "/api/market-data/load-range/async",
            "/api/market-data/lookup",
            "/api/market-data/reconcile",
            "/api/market-data/refresh",
            "/api/market-data/sources",
            "/health",
        ],
        "the served document must list every registered path"
    );
}

#[tokio::test]
async fn unknown_documentation_asset_returns_404() {
    assert_status(
        get("/swagger-ui/does-not-exist.txt").await,
        StatusCode::NOT_FOUND,
    );
}
