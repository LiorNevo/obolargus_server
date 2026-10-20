//! Contract tests for `GET /health`.
//!
//! See `contracts/health-check.md`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

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
async fn health_returns_200_ok_without_database_field() {
    let app = router(state_without_database());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let text = body_text(response).await;
    assert!(text.contains("\"status\":\"ok\""), "body: {text}");
    assert!(text.contains("\"version\":"), "body: {text}");
    assert!(!text.contains("database"), "body: {text}");
}

#[tokio::test]
async fn unknown_path_returns_404() {
    let app = router(state_without_database());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
