//! Contract test: every registered API operation is protected by default.
//!
//! The inventory comes from the generated OpenAPI document, so a route that is
//! added without an authentication story fails this test. Health, sign-in,
//! refresh, CORS preflight, and the documentation surfaces are checked
//! separately.

mod common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::auth::{self, ALICE};
use obolargus_server::openapi_document;
use obolargus_server::router;

fn concrete_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if segment.starts_with('{') && segment.ends_with('}') {
                "test-id"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn is_public(path: &str, method: &str) -> bool {
    (path == "/health" && method == "get")
        || (path == "/api/auth/login" && method == "post")
        || (path == "/api/auth/refresh" && method == "post")
}

#[tokio::test]
async fn every_non_public_operation_returns_401_without_a_token() {
    let harness = auth::auth_harness();
    let document = serde_json::to_value(openapi_document()).expect("document json");
    let paths = document["paths"].as_object().expect("paths");
    let mut checked = 0;
    for (path, item) in paths {
        let Some(item) = item.as_object() else {
            continue;
        };
        for method in ["get", "post", "put", "patch", "delete"] {
            if !item.contains_key(method) || is_public(path, method) {
                continue;
            }
            let method = Method::from_bytes(method.to_uppercase().as_bytes()).expect("method");
            let response =
                auth::send(&harness.state, method, &concrete_path(path), None, None).await;
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "expected 401 for {path}"
            );
            let body = auth::json(response).await;
            assert_eq!(body["error"]["code"], "unauthorized", "path {path}");
            checked += 1;
        }
    }
    assert!(
        checked > 50,
        "route inventory unexpectedly small: {checked}"
    );
}

#[tokio::test]
async fn health_is_public_but_discloses_no_configuration() {
    let harness = auth::auth_harness();
    let response = auth::send(&harness.state, Method::GET, "/health", None, None).await;
    let response = auth::assert_status(response, StatusCode::OK);
    let body = auth::json(response).await;
    assert_eq!(body["status"], "ok");
    let text = body.to_string();
    for secret in ["test-jwt-secret", "test-client", ALICE] {
        assert!(!text.contains(secret), "health leaked {secret}");
    }
}

#[tokio::test]
async fn login_and_refresh_reach_their_handlers_without_a_token() {
    let harness = auth::auth_harness();
    let login = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "" })),
        None,
    )
    .await;
    let login = auth::assert_status(login, StatusCode::BAD_REQUEST);
    assert_eq!(auth::json(login).await["error"]["code"], "validation_error");

    let refresh = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/refresh",
        Some(json!({ "refresh_token": "" })),
        None,
    )
    .await;
    let refresh = auth::assert_status(refresh, StatusCode::UNAUTHORIZED);
    assert_eq!(
        auth::json(refresh).await["error"]["code"],
        "invalid_refresh_token"
    );
}

#[tokio::test]
async fn cors_preflight_bypasses_authentication() {
    let harness = auth::auth_harness();
    let request = Request::builder()
        .method(Method::OPTIONS)
        .uri("/api/automation/flows")
        .header(header::ORIGIN, "http://127.0.0.1:8081")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "authorization")
        .body(Body::empty())
        .expect("request");
    let response = router(harness.state)
        .oneshot(request)
        .await
        .expect("response");
    assert!(
        response.status() == StatusCode::OK || response.status() == StatusCode::NO_CONTENT,
        "preflight failed with {}",
        response.status()
    );
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("http://127.0.0.1:8081")
    );
}

#[tokio::test]
async fn documentation_surfaces_require_authentication() {
    let harness = auth::auth_harness();
    for uri in ["/api-docs/openapi.json", "/swagger-ui/", "/"] {
        let response = auth::send(&harness.state, Method::GET, uri, None, None).await;
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "expected 401 for {uri}"
        );
    }
}

#[tokio::test]
async fn a_valid_access_token_reaches_the_handler() {
    let harness = auth::auth_harness();
    let actor = auth::seed_actor(&harness.auth, ALICE, Some("google-alice"));
    let token = auth::access_token(&harness.auth, &actor);
    let response = auth::send(
        &harness.state,
        Method::GET,
        "/api/market-data/sources",
        None,
        Some(&token),
    )
    .await;
    auth::assert_status(response, StatusCode::OK);
}

#[tokio::test]
async fn the_document_declares_bearer_security_globally() {
    let document = serde_json::to_value(openapi_document()).expect("document json");
    let scheme = &document["components"]["securitySchemes"]["bearer_auth"];
    assert_eq!(scheme["type"], "http");
    assert_eq!(scheme["scheme"], "bearer");
    assert_eq!(scheme["bearerFormat"], "JWT");

    let global = document["security"].as_array().expect("global security");
    assert!(
        global
            .iter()
            .any(|requirement| requirement.get("bearer_auth").is_some()),
        "bearer_auth is not a global requirement"
    );

    for path in ["/health", "/api/auth/login", "/api/auth/refresh"] {
        let method = if path == "/health" { "get" } else { "post" };
        let security = &document["paths"][path][method]["security"];
        let empty_requirement = security.as_array().is_some_and(|requirements| {
            requirements.is_empty() || requirements.iter().all(Value::is_object)
        });
        assert!(
            empty_requirement,
            "{method} {path} must opt out of security"
        );
    }
}

#[tokio::test]
async fn calendar_writes_require_the_primary_owner() {
    let harness = auth::auth_harness();
    let reader = auth::seed_actor(&harness.auth, common::auth::BOB, Some("google-bob"));
    let token = auth::access_token(&harness.auth, &reader);
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/market-data/calendar-assignments",
        Some(json!({
            "asset_id": "00000000-0000-4000-8000-000000000000",
            "mic": "XNAS",
            "valid_from": "2026-01-01",
            "valid_to": null
        })),
        Some(&token),
    )
    .await;
    let response = auth::assert_status(response, StatusCode::FORBIDDEN);
    assert_eq!(
        auth::json(response).await["error"]["code"],
        "primary_owner_required"
    );

    let owner = auth::seed_actor(&harness.auth, ALICE, Some("google-alice"));
    let token = auth::access_token(&harness.auth, &owner);
    let response = auth::send(
        &harness.state,
        Method::GET,
        "/api/market-data/calendars",
        None,
        Some(&token),
    )
    .await;
    // No database is configured in the auth harness: the owner reaches the
    // handler and gets the explicit unavailable state, not 401/403.
    auth::assert_status(response, StatusCode::SERVICE_UNAVAILABLE);
}
