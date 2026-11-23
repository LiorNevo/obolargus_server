//! Default-deny authentication middleware.
//!
//! Runs after CORS and before every handler. The documented public paths and
//! CORS preflight pass through; every other request needs a valid application
//! access token. A successful check inserts [`AuthUser`] into request
//! extensions so handlers never parse tokens or trust client identity fields.
//! When authentication is disabled in loopback development mode, the legacy
//! `dev-user` actor is inserted instead.

use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::AppState;

use super::{AuthUser, is_public_request};

/// Enforces authentication for every non-public request.
pub async fn require_auth(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    if is_public_request(request.method(), request.uri().path()) {
        return next.run(request).await;
    }

    if !state.auth.settings.enabled {
        let mut actor = AuthUser::dev();
        let path = request.uri().path();
        let dev_owner = if path.starts_with("/api/morpheus") {
            state.config.morpheus_actor_id.trim()
        } else if path.starts_with("/api/automation") {
            state.config.automation_actor_id.trim()
        } else {
            ""
        };
        if !dev_owner.is_empty() {
            actor.actor_id = dev_owner.to_string();
            actor.owner_id = dev_owner.to_string();
            actor.email = format!("{dev_owner}@localhost");
        }
        request.extensions_mut().insert(actor);
        return next.run(request).await;
    }

    let Some(token) = bearer_token(request.headers()) else {
        return unauthorized("missing bearer token");
    };
    match state.auth.authenticate_access_token(token) {
        Ok(user) => {
            request.extensions_mut().insert(user);
            next.run(request).await
        }
        Err(_) => unauthorized("invalid or expired access token"),
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

fn unauthorized(_detail: &'static str) -> Response {
    // The detail is deliberately generic: a rejection must not reveal whether
    // a subject or email exists.
    (
        StatusCode::UNAUTHORIZED,
        [(WWW_AUTHENTICATE, "Bearer")],
        axum::Json(json!({
            "error": {
                "code": "unauthorized",
                "message": "authentication required"
            }
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::Extension;
    use axum::http::Method;
    use axum::http::Request as HttpRequest;
    use axum::routing::{get, post};
    use tower::ServiceExt;

    use crate::auth::AuthSettings;
    use crate::config::AppConfig;
    use std::sync::Arc;

    use super::super::google::{GoogleAuthError, GoogleVerifier, VerifiedGoogleIdentity};
    use async_trait::async_trait;

    struct NeverVerifier;

    #[async_trait]
    impl GoogleVerifier for NeverVerifier {
        async fn verify(&self, _id_token: &str) -> Result<VerifiedGoogleIdentity, GoogleAuthError> {
            Err(GoogleAuthError::InvalidCredential("unused".to_string()))
        }
    }

    fn state(enabled: bool) -> AppState {
        let mut state = AppState::new(AppConfig::default(), None).expect("state");
        let settings = AuthSettings {
            enabled,
            google_client_id: "client".to_string(),
            allowed_emails: ["alice@example.com".to_string()].into_iter().collect(),
            primary_owner_email: "alice@example.com".to_string(),
            jwt_secret: "secret".to_string(),
            access_ttl_minutes: 15,
            refresh_ttl_days: 7,
        };
        state.auth = Arc::new(super::super::AuthState::new_in_memory(
            settings,
            Arc::new(NeverVerifier),
        ));
        state
    }

    fn router(state: AppState) -> Router {
        Router::new()
            .route(
                "/api/protected",
                get(|Extension(user): Extension<AuthUser>| async move { user.owner_id }),
            )
            .route("/api/auth/login", post(|| async { "public" }))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                require_auth,
            ))
            .with_state(state)
    }

    async fn call(router: Router, method: Method, uri: &str) -> (StatusCode, String) {
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn missing_or_malformed_tokens_are_rejected() {
        let router = router(state(true));
        assert_eq!(
            call(router.clone(), Method::GET, "/api/protected").await.0,
            StatusCode::UNAUTHORIZED
        );
        let request = HttpRequest::builder()
            .uri("/api/protected")
            .header(AUTHORIZATION, "Token abc")
            .body(Body::empty())
            .expect("request");
        let response = router.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn public_login_passes_without_a_token() {
        let (status, body) = call(router(state(true)), Method::POST, "/api/auth/login").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "public");
    }

    #[tokio::test]
    async fn dev_mode_inserts_the_legacy_actor() {
        let (status, body) = call(router(state(false)), Method::GET, "/api/protected").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "dev-user");
    }
}
