//! Navigation contract tests for the documentation entry points (US2).

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt;

use obolargus_server::{AppState, config::AppConfig, router};

fn app() -> axum::Router {
    router(AppState::new(AppConfig::default(), None).expect("app state"))
}

async fn request(method: &str, uri: &str) -> axum::response::Response {
    app()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response")
}

fn location(response: &axum::response::Response) -> Option<&str> {
    response
        .headers()
        .get(header::LOCATION)
        .and_then(|value| value.to_str().ok())
}

#[tokio::test]
async fn server_root_redirects_to_the_documentation_page() {
    let response = request("GET", "/").await;

    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(location(&response), Some("/swagger-ui/"));
}

#[tokio::test]
async fn bare_swagger_path_redirects_to_the_trailing_slash_form() {
    let response = request("GET", "/swagger-ui").await;

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(&response), Some("/swagger-ui/"));
}

#[tokio::test]
async fn canonical_swagger_path_serves_the_page() {
    assert_eq!(
        request("GET", "/swagger-ui/").await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn server_root_redirect_is_get_only() {
    let response = request("POST", "/").await;

    assert_ne!(
        response.status(),
        StatusCode::TEMPORARY_REDIRECT,
        "POST / must not be converted into a documentation redirect"
    );
}
