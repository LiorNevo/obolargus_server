//! Shared helpers for the documentation integration tests.
//!
//! The Swagger UI build serves its static assets gzip-compressed. These
//! helpers keep the tests focused on the response contract by decoding the
//! body when the response says it is gzip-encoded.

use std::io::Read;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::response::Response;
use flate2::read::GzDecoder;
use http_body_util::BodyExt;
use tower::ServiceExt;

use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

/// Builds router state without a database pool.
pub fn state_without_database() -> AppState {
    AppState::new(AppConfig::default(), None).expect("app state")
}

/// Sends a GET request to the router and returns the response.
pub async fn get(uri: &str) -> Response {
    router(state_without_database())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response")
}

/// Reads a response body as text, decoding gzip when required.
pub async fn body_text(response: Response) -> String {
    let is_gzip = response
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("gzip"));
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();

    if !is_gzip {
        return String::from_utf8_lossy(&bytes).into_owned();
    }

    let mut decoder = GzDecoder::new(bytes.as_ref());
    let mut text = String::new();
    decoder.read_to_string(&mut text).expect("gunzip body");
    text
}

/// Asserts a response status and returns the response for further checks.
pub fn assert_status(response: Response, expected: StatusCode) -> Response {
    assert_eq!(response.status(), expected);
    response
}
