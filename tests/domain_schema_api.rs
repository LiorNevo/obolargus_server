//! Contract tests for `GET /api/domain/schema`.

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use tower::ServiceExt;

use obolargus_core::data_dictionary::export_system_schema;
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

fn state_without_database() -> AppState {
    AppState::new(AppConfig::default(), None).expect("app state")
}

async fn body_text(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn schema_returns_canonical_bytes_without_a_database() {
    let expected = export_system_schema()
        .expect("schema")
        .canonical_json()
        .to_string();
    let app = router(state_without_database());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/domain/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/schema+json; charset=utf-8")
    );
    assert_eq!(body_text(response).await, expected);
}

#[tokio::test]
async fn schema_response_is_one_json_object_with_a_trailing_newline() {
    let app = router(state_without_database());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/domain/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_text(response).await;
    assert!(body.starts_with("{\n  \"$defs\""));
    assert!(body.ends_with("}\n"));
    assert!(serde_json::from_str::<serde_json::Value>(&body).is_ok());
}
