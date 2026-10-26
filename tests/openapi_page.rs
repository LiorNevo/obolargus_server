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
async fn openapi_document_is_served_as_json_with_seven_paths() {
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
    assert_eq!(document["servers"], serde_json::json!([{ "url": ".." }]));

    let paths = document["paths"].as_object().expect("paths object");
    assert_eq!(
        paths.len(),
        7,
        "expected seven documented paths, got {:?}",
        paths.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn unknown_documentation_asset_returns_404() {
    assert_status(
        get("/swagger-ui/does-not-exist.txt").await,
        StatusCode::NOT_FOUND,
    );
}
