//! Cross-surface equality for the system data-dictionary schema.
//!
//! Library, HTTP, and the committed artifact must all return the same
//! canonical bytes. The CLI surfaces are checked from the CLI crate, where
//! the test harness exposes the built binary.

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use tower::ServiceExt;

use obolargus_core::data_dictionary::export_system_schema;
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

const ARTIFACT: &str = include_str!("../../obolargus_core/schema/system-data-dictionary.json");

async fn http_body() -> String {
    let app = router(AppState::new(AppConfig::default(), None).expect("state"));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/domain/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request");
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    String::from_utf8(bytes.to_vec()).expect("utf8")
}

#[tokio::test]
async fn library_http_and_artifact_return_identical_bytes() {
    let library = export_system_schema()
        .expect("schema")
        .canonical_json()
        .to_string();
    let http = http_body().await;

    assert_eq!(http, library);
    assert_eq!(ARTIFACT, library);
}
