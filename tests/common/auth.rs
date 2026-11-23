//! Authentication test harness: a mock Google verifier, token factory, and
//! helpers that build an auth-enabled router without a database.
//!
//! Production binds actors in PostgreSQL; tests use the cache-only actor store
//! so Google verification, token typing, allowlisting, and middleware can be
//! exercised offline.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use axum::response::Response;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use obolargus_server::auth::actor::ActorRecord;
use obolargus_server::auth::google::{GoogleAuthError, GoogleVerifier, VerifiedGoogleIdentity};
use obolargus_server::auth::jwt::TokenService;
use obolargus_server::auth::{AuthSettings, AuthState};
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

/// Allowlisted test accounts.
pub const ALICE: &str = "alice@example.com";
/// A second allowlisted test account.
pub const BOB: &str = "bob@example.com";
/// A valid Google account that is not allowlisted.
pub const EVE: &str = "eve@example.com";

/// Google verifier with an explicit token → identity map.
#[derive(Default)]
pub struct MockGoogleVerifier {
    responses: Mutex<HashMap<String, Result<VerifiedGoogleIdentity, GoogleAuthError>>>,
}

impl MockGoogleVerifier {
    /// Registers a successful verification for `token`.
    pub fn allow(&self, token: &str, sub: &str, email: &str) {
        self.insert(
            token,
            Ok(VerifiedGoogleIdentity {
                sub: sub.to_string(),
                email: email.to_string(),
            }),
        );
    }

    /// Registers an invalid-credential result for `token`.
    pub fn reject(&self, token: &str) {
        self.insert(
            token,
            Err(GoogleAuthError::InvalidCredential("fixture".to_string())),
        );
    }

    /// Registers a verification outage for `token`.
    pub fn unavailable(&self, token: &str) {
        self.insert(
            token,
            Err(GoogleAuthError::Unavailable("fixture outage".to_string())),
        );
    }

    fn insert(&self, token: &str, result: Result<VerifiedGoogleIdentity, GoogleAuthError>) {
        if let Ok(mut responses) = self.responses.lock() {
            responses.insert(token.to_string(), result);
        }
    }
}

#[async_trait]
impl GoogleVerifier for MockGoogleVerifier {
    async fn verify(&self, id_token: &str) -> Result<VerifiedGoogleIdentity, GoogleAuthError> {
        self.responses
            .lock()
            .map_err(|_| GoogleAuthError::Unavailable("poisoned mock".to_string()))?
            .get(id_token)
            .cloned()
            .unwrap_or_else(|| {
                Err(GoogleAuthError::InvalidCredential(
                    "unknown fixture token".to_string(),
                ))
            })
    }
}

/// Authentication settings used by the tests.
pub fn settings() -> AuthSettings {
    AuthSettings {
        enabled: true,
        google_client_id: "test-client.apps.googleusercontent.com".to_string(),
        allowed_emails: [ALICE.to_string(), BOB.to_string()].into_iter().collect(),
        primary_owner_email: ALICE.to_string(),
        jwt_secret: "test-jwt-secret".to_string(),
        access_ttl_minutes: 15,
        refresh_ttl_days: 7,
    }
}

/// An auth-enabled application state plus its verifier.
pub struct AuthHarness {
    /// State wired into the router.
    pub state: AppState,
    /// Authentication state, for seeding actors and issuing tokens.
    pub auth: Arc<AuthState>,
    /// Mock Google verifier.
    pub verifier: Arc<MockGoogleVerifier>,
}

/// Builds an auth-enabled state with an in-memory actor registry.
pub fn auth_harness() -> AuthHarness {
    let verifier = Arc::new(MockGoogleVerifier::default());
    let dyn_verifier: Arc<dyn GoogleVerifier> = verifier.clone();
    let auth = Arc::new(AuthState::new_in_memory(settings(), dyn_verifier));
    let mut state = AppState::new(AppConfig::default(), None).expect("app state");
    state.auth = auth.clone();
    AuthHarness {
        state,
        auth,
        verifier,
    }
}

/// Seeds an actor into the in-memory registry.
pub fn seed_actor(auth: &AuthState, email: &str, sub: Option<&str>) -> ActorRecord {
    auth.seed_actor(email, sub)
}

/// Issues a valid access token for `actor`.
pub fn access_token(auth: &AuthState, actor: &ActorRecord) -> String {
    auth.issue_tokens(actor).expect("test tokens").access_token
}

/// Issues a valid refresh token for `actor`.
pub fn refresh_token(auth: &AuthState, actor: &ActorRecord) -> String {
    auth.issue_tokens(actor).expect("test tokens").refresh_token
}

/// Issues an access token that is already expired.
pub fn expired_access_token(auth: &AuthState, actor: &ActorRecord) -> String {
    TokenService::with_ttl_seconds(&auth.settings.jwt_secret, -60, -60)
        .issue_access(&actor.id.to_string())
        .expect("expired token")
}

/// Sends one request through the production router and returns the response.
pub async fn send(
    state: &AppState,
    method: Method,
    uri: &str,
    body: Option<Value>,
    bearer: Option<&str>,
) -> Response {
    let mut builder = Request::builder().method(method).uri(uri);
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let body = body.map_or_else(Body::empty, |value| Body::from(value.to_string()));
    router(state.clone())
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("response")
}

/// Reads a response body as JSON.
pub async fn json(response: Response) -> Value {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("json body")
}

/// Asserts a status and returns the response.
pub fn assert_status(response: Response, expected: StatusCode) -> Response {
    assert_eq!(response.status(), expected);
    response
}
