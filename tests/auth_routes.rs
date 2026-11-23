//! Integration tests for Google sign-in, token refresh, allowlisting, and the
//! failed-exchange rate limit.

mod common;

use axum::http::{Method, StatusCode, header};
use serde_json::json;

use common::auth::{self, ALICE, BOB, EVE};
use obolargus_server::auth::jwt::TokenService;

#[tokio::test]
async fn login_issues_tokens_and_refresh_returns_a_new_access_token() {
    let harness = auth::auth_harness();
    harness
        .verifier
        .allow("alice-google-token", "google-alice", ALICE);
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "alice-google-token" })),
        None,
    )
    .await;
    let response = auth::assert_status(response, StatusCode::OK);
    let body = auth::json(response).await;
    assert_eq!(body["token_type"], "Bearer");
    assert_eq!(body["expires_in"], 900);
    assert_eq!(body["actor"]["email"], ALICE);
    let actor_id = body["actor"]["id"].as_str().expect("actor id").to_string();
    assert!(uuid::Uuid::parse_str(&actor_id).is_ok());
    let access = body["access_token"].as_str().expect("access").to_string();
    let refresh = body["refresh_token"].as_str().expect("refresh").to_string();
    assert_ne!(access, refresh);

    let protected = auth::send(
        &harness.state,
        Method::GET,
        "/api/market-data/sources",
        None,
        Some(&access),
    )
    .await;
    auth::assert_status(protected, StatusCode::OK);

    let refreshed = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/refresh",
        Some(json!({ "refresh_token": refresh })),
        None,
    )
    .await;
    let refreshed = auth::assert_status(refreshed, StatusCode::OK);
    let body = auth::json(refreshed).await;
    assert_eq!(body["token_type"], "Bearer");
    assert_eq!(body["expires_in"], 900);
    assert!(body.get("refresh_token").is_none());
    let new_access = body["access_token"].as_str().expect("access");
    let protected = auth::send(
        &harness.state,
        Method::GET,
        "/api/market-data/sources",
        None,
        Some(new_access),
    )
    .await;
    auth::assert_status(protected, StatusCode::OK);
}

#[tokio::test]
async fn an_invalid_google_credential_is_401() {
    let harness = auth::auth_harness();
    harness.verifier.reject("bad-google-token");
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "bad-google-token" })),
        None,
    )
    .await;
    let response = auth::assert_status(response, StatusCode::UNAUTHORIZED);
    assert_eq!(
        auth::json(response).await["error"]["code"],
        "invalid_google_credential"
    );
}

#[tokio::test]
async fn a_non_allowlisted_account_is_403_without_a_session() {
    let harness = auth::auth_harness();
    harness
        .verifier
        .allow("eve-google-token", "google-eve", EVE);
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "eve-google-token" })),
        None,
    )
    .await;
    let response = auth::assert_status(response, StatusCode::FORBIDDEN);
    let body = auth::json(response).await;
    assert_eq!(body["error"]["code"], "account_not_allowed");
    assert!(body.get("access_token").is_none());
    assert!(harness.auth.actors.is_empty() || harness.auth.actors.by_email(EVE).is_none());
}

#[tokio::test]
async fn a_verification_outage_fails_closed_with_500() {
    let harness = auth::auth_harness();
    harness.verifier.unavailable("outage-token");
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "outage-token" })),
        None,
    )
    .await;
    let response = auth::assert_status(response, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        auth::json(response).await["error"]["code"],
        "auth_unavailable"
    );
}

#[tokio::test]
async fn a_bound_email_cannot_be_inherited_by_a_new_subject() {
    let harness = auth::auth_harness();
    auth::seed_actor(&harness.auth, ALICE, Some("google-alice-original"));
    harness
        .verifier
        .allow("recreated-google-token", "google-alice-recreated", ALICE);
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "recreated-google-token" })),
        None,
    )
    .await;
    let response = auth::assert_status(response, StatusCode::FORBIDDEN);
    assert_eq!(
        auth::json(response).await["error"]["code"],
        "account_not_allowed"
    );
}

#[tokio::test]
async fn a_matching_subject_may_change_its_allowlisted_email() {
    let harness = auth::auth_harness();
    let actor = auth::seed_actor(&harness.auth, ALICE, Some("google-bob"));
    harness
        .verifier
        .allow("bob-google-token", "google-bob", BOB);
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "bob-google-token" })),
        None,
    )
    .await;
    let response = auth::assert_status(response, StatusCode::OK);
    assert_eq!(
        auth::json(response).await["actor"]["id"],
        actor.id.to_string()
    );
}

#[tokio::test]
async fn expired_wrong_type_and_unknown_subject_tokens_are_rejected() {
    let harness = auth::auth_harness();
    let actor = auth::seed_actor(&harness.auth, ALICE, Some("google-alice"));

    let expired = auth::expired_access_token(&harness.auth, &actor);
    let response = auth::send(
        &harness.state,
        Method::GET,
        "/api/market-data/sources",
        None,
        Some(&expired),
    )
    .await;
    auth::assert_status(response, StatusCode::UNAUTHORIZED);

    let refresh = auth::refresh_token(&harness.auth, &actor);
    let response = auth::send(
        &harness.state,
        Method::GET,
        "/api/market-data/sources",
        None,
        Some(&refresh),
    )
    .await;
    auth::assert_status(response, StatusCode::UNAUTHORIZED);

    let access = auth::access_token(&harness.auth, &actor);
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/refresh",
        Some(json!({ "refresh_token": access })),
        None,
    )
    .await;
    let response = auth::assert_status(response, StatusCode::UNAUTHORIZED);
    assert_eq!(
        auth::json(response).await["error"]["code"],
        "invalid_refresh_token"
    );

    let unknown_subject =
        TokenService::with_ttl_seconds(&harness.auth.settings.jwt_secret, 900, 900)
            .issue_access(&uuid::Uuid::new_v4().to_string())
            .expect("token");
    let response = auth::send(
        &harness.state,
        Method::GET,
        "/api/market-data/sources",
        None,
        Some(&unknown_subject),
    )
    .await;
    auth::assert_status(response, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn refresh_rechecks_the_allowlist() {
    let harness = auth::auth_harness();
    let removed = auth::seed_actor(&harness.auth, EVE, Some("google-eve"));
    let refresh = TokenService::with_ttl_seconds(&harness.auth.settings.jwt_secret, 900, 86_400)
        .issue_refresh(&removed.id.to_string())
        .expect("token");
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/refresh",
        Some(json!({ "refresh_token": refresh })),
        None,
    )
    .await;
    let response = auth::assert_status(response, StatusCode::UNAUTHORIZED);
    assert_eq!(
        auth::json(response).await["error"]["code"],
        "invalid_refresh_token"
    );
}

#[tokio::test]
async fn an_access_token_stays_valid_after_the_allowlist_removal_until_it_expires() {
    let harness = auth::auth_harness();
    let removed = auth::seed_actor(&harness.auth, EVE, Some("google-eve"));
    let access = auth::access_token(&harness.auth, &removed);
    let response = auth::send(
        &harness.state,
        Method::GET,
        "/api/market-data/sources",
        None,
        Some(&access),
    )
    .await;
    auth::assert_status(response, StatusCode::OK);

    let refresh = auth::refresh_token(&harness.auth, &removed);
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/refresh",
        Some(json!({ "refresh_token": refresh })),
        None,
    )
    .await;
    auth::assert_status(response, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn expired_refresh_tokens_are_rejected() {
    let harness = auth::auth_harness();
    let actor = auth::seed_actor(&harness.auth, ALICE, Some("google-alice"));
    let refresh = TokenService::with_ttl_seconds(&harness.auth.settings.jwt_secret, 900, -60)
        .issue_refresh(&actor.id.to_string())
        .expect("token");
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/refresh",
        Some(json!({ "refresh_token": refresh })),
        None,
    )
    .await;
    auth::assert_status(response, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn five_failed_exchanges_trigger_429_with_retry_after() {
    let harness = auth::auth_harness();
    for _ in 0..5 {
        let response = auth::send(
            &harness.state,
            Method::POST,
            "/api/auth/login",
            Some(json!({ "id_token": "unknown-google-token" })),
            None,
        )
        .await;
        auth::assert_status(response, StatusCode::UNAUTHORIZED);
    }
    harness
        .verifier
        .allow("alice-google-token", "google-alice", ALICE);
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "alice-google-token" })),
        None,
    )
    .await;
    let response = auth::assert_status(response, StatusCode::TOO_MANY_REQUESTS);
    assert!(
        response.headers().get(header::RETRY_AFTER).is_some(),
        "429 must carry Retry-After"
    );
    assert_eq!(auth::json(response).await["error"]["code"], "rate_limited");
}

#[tokio::test]
async fn a_successful_sign_in_resets_the_failure_counter() {
    let harness = auth::auth_harness();
    for _ in 0..4 {
        let response = auth::send(
            &harness.state,
            Method::POST,
            "/api/auth/login",
            Some(json!({ "id_token": "unknown-google-token" })),
            None,
        )
        .await;
        auth::assert_status(response, StatusCode::UNAUTHORIZED);
    }
    harness
        .verifier
        .allow("alice-google-token", "google-alice", ALICE);
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "alice-google-token" })),
        None,
    )
    .await;
    auth::assert_status(response, StatusCode::OK);

    for _ in 0..5 {
        let response = auth::send(
            &harness.state,
            Method::POST,
            "/api/auth/login",
            Some(json!({ "id_token": "unknown-google-token" })),
            None,
        )
        .await;
        auth::assert_status(response, StatusCode::UNAUTHORIZED);
    }
    let response = auth::send(
        &harness.state,
        Method::POST,
        "/api/auth/login",
        Some(json!({ "id_token": "unknown-google-token" })),
        None,
    )
    .await;
    auth::assert_status(response, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn blank_id_tokens_are_validation_errors() {
    let harness = auth::auth_harness();
    for body in [json!({ "id_token": "   " }), json!({})] {
        let response = auth::send(
            &harness.state,
            Method::POST,
            "/api/auth/login",
            Some(body),
            None,
        )
        .await;
        let response = auth::assert_status(response, StatusCode::BAD_REQUEST);
        assert_eq!(
            auth::json(response).await["error"]["code"],
            "validation_error"
        );
    }
}
