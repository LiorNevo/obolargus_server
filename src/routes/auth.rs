//! Public authentication routes: Google sign-in and token refresh.
//!
//! Contract: `specs/034-production-hardening/contracts/auth-api.md`.
//! Tokens, Google credentials, and allowlist entries never appear in URLs,
//! logs, or error bodies.

use std::convert::Infallible;
use std::net::SocketAddr;

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequestParts, State};
use axum::http::StatusCode;
#[cfg(test)]
use axum::http::header::RETRY_AFTER;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;
use crate::auth::rate_limit::retry_after_seconds;
use crate::auth::{AuthUser, LoginError, RefreshError};
use crate::routes::ErrorEnvelope;

/// Client address used as the sign-in rate-limit key.
///
/// Prefers the gateway-supplied `X-Real-IP` / `X-Forwarded-For` hop, then the
/// socket peer when the router is served with connection info, and finally a
/// shared placeholder. Forwarding headers are trusted only from a loopback
/// peer. The value is used only for rate limiting, never as identity.
pub struct ClientAddr(String);

impl<S> FromRequestParts<S> for ClientAddr
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let forwarded = parts
            .headers
            .get("x-real-ip")
            .or_else(|| parts.headers.get("x-forwarded-for"))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(',').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let peer = parts
            .extensions
            .get::<axum::extract::ConnectInfo<SocketAddr>>()
            .map(|info| info.0.ip());
        // Only trust forwarding headers from a loopback peer (the gateway).
        let address = match peer {
            Some(ip) if ip.is_loopback() => forwarded.unwrap_or_else(|| ip.to_string()),
            Some(ip) => ip.to_string(),
            None => forwarded.unwrap_or_else(|| "unknown".to_string()),
        };
        Ok(Self(address))
    }
}

/// Sign-in request body.
#[derive(Debug, Deserialize, ToSchema)]
pub struct LoginRequest {
    /// Google Identity Services ID token.
    pub id_token: String,
}

/// The signed-in actor summary.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ActorView {
    /// Stable actor/owner id.
    pub id: String,
    /// Verified Google email.
    pub email: String,
    /// Whether this actor may change shared reference data.
    pub is_primary_owner: bool,
}

/// Sign-in response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct LoginResponse {
    /// Short-lived application access token.
    pub access_token: String,
    /// Refresh token with the absolute session lifetime.
    pub refresh_token: String,
    /// Always `Bearer`.
    pub token_type: &'static str,
    /// Access token lifetime in seconds.
    pub expires_in: u64,
    /// The actor that was signed in.
    pub actor: ActorView,
}

/// Refresh request body.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RefreshRequest {
    /// Application refresh token issued at sign-in.
    pub refresh_token: String,
}

/// Refresh response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct RefreshResponse {
    /// New short-lived application access token.
    pub access_token: String,
    /// Always `Bearer`.
    pub token_type: &'static str,
    /// Access token lifetime in seconds.
    pub expires_in: u64,
}

/// Error returned by the public authentication routes.
pub enum AuthApiError {
    /// Authentication is disabled in local development mode.
    AuthDisabled,
    /// The request body is missing required data.
    Validation,
    /// The Google credential did not verify.
    InvalidGoogleCredential,
    /// The verified account is not allowed.
    AccountNotAllowed,
    /// Too many failed exchanges; the payload is the retry delay.
    RateLimited(u64),
    /// The refresh token did not verify or is no longer allowed.
    InvalidRefreshToken,
    /// Google verification or actor persistence is unavailable.
    Unavailable,
}

impl IntoResponse for AuthApiError {
    fn into_response(self) -> Response {
        let (status, code, message, retry_after) = match self {
            Self::AuthDisabled => (
                StatusCode::BAD_REQUEST,
                "auth_disabled",
                "authentication is disabled",
                None,
            ),
            Self::Validation => (
                StatusCode::BAD_REQUEST,
                "validation_error",
                "id_token is required",
                None,
            ),
            Self::InvalidGoogleCredential => (
                StatusCode::UNAUTHORIZED,
                "invalid_google_credential",
                "the Google credential is invalid or expired",
                None,
            ),
            Self::AccountNotAllowed => (
                StatusCode::FORBIDDEN,
                "account_not_allowed",
                "this account is not allowed",
                None,
            ),
            Self::RateLimited(seconds) => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "too many failed sign-in attempts",
                Some(seconds),
            ),
            Self::InvalidRefreshToken => (
                StatusCode::UNAUTHORIZED,
                "invalid_refresh_token",
                "the refresh token is invalid or expired",
                None,
            ),
            Self::Unavailable => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "auth_unavailable",
                "authentication is temporarily unavailable",
                None,
            ),
        };
        ErrorEnvelope::new(code, message).response(status, retry_after)
    }
}

/// `POST /api/auth/login` — exchange a verified Google ID token for
/// application tokens.
#[utoipa::path(
    post,
    path = "/api/auth/login",
    tag = "auth",
    summary = "Sign in with a Google ID token",
    description = "Verifies the Google Identity Services ID token (signature, issuer, audience, expiry, verified email), checks the account allowlist, and issues application access and refresh tokens. Failed exchanges are limited to five per minute per client.",
    security(()),
    request_body = LoginRequest,
    responses(
        (status = 200, description = "Application tokens for the signed-in actor", body = LoginResponse),
        (status = 400, description = "Missing or blank id_token", body = crate::routes::ErrorEnvelope),
        (status = 401, description = "The Google credential is invalid, expired, wrong-audience, wrong-issuer, or unverified", body = crate::routes::ErrorEnvelope),
        (status = 403, description = "The verified account is not allowed", body = crate::routes::ErrorEnvelope),
        (status = 429, description = "Too many failed sign-in exchanges; retry after the Retry-After delay", body = crate::routes::ErrorEnvelope),
        (status = 500, description = "Google verification or the actor registry is unavailable", body = crate::routes::ErrorEnvelope)
    )
)]
pub async fn login(
    State(state): State<AppState>,
    client: ClientAddr,
    payload: Result<Json<LoginRequest>, JsonRejection>,
) -> Result<Json<LoginResponse>, AuthApiError> {
    if !state.auth.settings.enabled {
        return Err(AuthApiError::AuthDisabled);
    }
    let Json(body) = payload.map_err(|_| AuthApiError::Validation)?;
    let id_token = body.id_token.trim();
    if id_token.is_empty() {
        return Err(AuthApiError::Validation);
    }

    let client = client_key(client);
    if let Some(delay) = state.auth.sign_in_limiter.check(&client) {
        return Err(AuthApiError::RateLimited(retry_after_seconds(delay)));
    }

    match state.auth.login_with_google(id_token).await {
        Ok(actor) => {
            state.auth.sign_in_limiter.reset(&client);
            let tokens = state
                .auth
                .issue_tokens(&actor)
                .map_err(|_| AuthApiError::Unavailable)?;
            let user = AuthUser::from_actor(&actor);
            let is_primary_owner = state.auth.is_primary_owner(&user);
            Ok(Json(LoginResponse {
                access_token: tokens.access_token,
                refresh_token: tokens.refresh_token,
                token_type: "Bearer",
                expires_in: tokens.expires_in,
                actor: ActorView {
                    id: actor.id.to_string(),
                    email: actor.email,
                    is_primary_owner,
                },
            }))
        }
        Err(LoginError::InvalidCredential) => {
            state.auth.sign_in_limiter.record(&client);
            Err(AuthApiError::InvalidGoogleCredential)
        }
        Err(LoginError::NotAllowed) => {
            state.auth.sign_in_limiter.record(&client);
            Err(AuthApiError::AccountNotAllowed)
        }
        Err(LoginError::Unavailable(detail)) => {
            tracing::error!(%detail, "Google credential verification unavailable");
            Err(AuthApiError::Unavailable)
        }
    }
}

/// `POST /api/auth/refresh` — exchange a refresh token for a new access token.
#[utoipa::path(
    post,
    path = "/api/auth/refresh",
    tag = "auth",
    summary = "Refresh an application access token",
    description = "Verifies the refresh token, re-checks that the owner still exists and is still allowlisted, and returns a new access token. The refresh token's absolute expiry is not extended.",
    security(()),
    request_body = RefreshRequest,
    responses(
        (status = 200, description = "A new access token", body = RefreshResponse),
        (status = 401, description = "The refresh token is invalid, expired, wrong-type, or no longer allowed", body = crate::routes::ErrorEnvelope)
    )
)]
pub async fn refresh(
    State(state): State<AppState>,
    payload: Result<Json<RefreshRequest>, JsonRejection>,
) -> Result<Json<RefreshResponse>, AuthApiError> {
    if !state.auth.settings.enabled {
        return Err(AuthApiError::AuthDisabled);
    }
    let Json(body) = payload.map_err(|_| AuthApiError::InvalidRefreshToken)?;
    let token = body.refresh_token.trim();
    if token.is_empty() {
        return Err(AuthApiError::InvalidRefreshToken);
    }
    match state.auth.refresh_access_token(token) {
        Ok(issued) => Ok(Json(RefreshResponse {
            access_token: issued.access_token,
            token_type: "Bearer",
            expires_in: issued.expires_in,
        })),
        Err(RefreshError::Invalid) => Err(AuthApiError::InvalidRefreshToken),
    }
}

fn client_key(client: ClientAddr) -> String {
    client.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_key_uses_the_extracted_address() {
        assert_eq!(client_key(ClientAddr("127.0.0.1".to_string())), "127.0.0.1");
        assert_eq!(client_key(ClientAddr("unknown".to_string())), "unknown");
    }

    #[test]
    fn rate_limit_error_carries_retry_after() {
        let response = AuthApiError::RateLimited(42).into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("42")
        );
    }

    #[tokio::test]
    async fn error_bodies_use_the_contract_codes() {
        for (error, expected) in [
            (AuthApiError::AuthDisabled, "auth_disabled"),
            (AuthApiError::Validation, "validation_error"),
            (
                AuthApiError::InvalidGoogleCredential,
                "invalid_google_credential",
            ),
            (AuthApiError::AccountNotAllowed, "account_not_allowed"),
            (AuthApiError::RateLimited(1), "rate_limited"),
            (AuthApiError::InvalidRefreshToken, "invalid_refresh_token"),
            (AuthApiError::Unavailable, "auth_unavailable"),
        ] {
            let body = axum::body::to_bytes(error.into_response().into_body(), usize::MAX)
                .await
                .expect("body");
            let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
            assert_eq!(value["error"]["code"], expected);
        }
    }
}
