//! Application access and refresh tokens (HS256).
//!
//! Both token kinds are signed with `JWT_SECRET`, carry the actor's stable
//! owner id in `sub`, and are separated by the `typ` claim. Refresh tokens are
//! never accepted as access tokens and Google ID tokens (RS256) never verify
//! here.

use std::fmt;

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

/// `typ` claim of an access token.
pub const ACCESS_TYPE: &str = "access";

/// `typ` claim of a refresh token.
pub const REFRESH_TYPE: &str = "refresh";

/// Claims carried by both application token kinds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppClaims {
    /// Actor owner id (UUID string).
    pub sub: String,
    /// Token kind: `access` or `refresh`.
    pub typ: String,
    /// Issued-at, Unix seconds.
    pub iat: i64,
    /// Expiry, Unix seconds.
    pub exp: i64,
}

/// Why an application token was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenError {
    /// The signature or shape is invalid.
    Invalid,
    /// The token expired.
    Expired,
    /// The token kind is not the one the caller requires.
    WrongType,
}

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid => write!(f, "invalid token"),
            Self::Expired => write!(f, "expired token"),
            Self::WrongType => write!(f, "wrong token type"),
        }
    }
}

impl std::error::Error for TokenError {}

/// Issues and verifies application tokens.
pub struct TokenService {
    encoding: EncodingKey,
    decoding: DecodingKey,
    access_ttl_seconds: i64,
    refresh_ttl_seconds: i64,
}

impl TokenService {
    /// Builds the service from configuration.
    pub fn new(secret: &str, access_ttl_minutes: u64, refresh_ttl_days: u64) -> Self {
        Self::with_ttl_seconds(
            secret,
            minutes_to_seconds(access_ttl_minutes),
            days_to_seconds(refresh_ttl_days),
        )
    }

    /// Builds the service with explicit lifetimes in seconds.
    ///
    /// Used by tests to produce expired tokens.
    pub fn with_ttl_seconds(
        secret: &str,
        access_ttl_seconds: i64,
        refresh_ttl_seconds: i64,
    ) -> Self {
        Self {
            encoding: EncodingKey::from_secret(secret.as_bytes()),
            decoding: DecodingKey::from_secret(secret.as_bytes()),
            access_ttl_seconds,
            refresh_ttl_seconds,
        }
    }

    /// Access token lifetime in seconds.
    pub fn access_ttl_seconds(&self) -> u64 {
        u64::try_from(self.access_ttl_seconds.max(0)).unwrap_or(0)
    }

    /// Issues an access token for `actor_id`.
    pub fn issue_access(&self, actor_id: &str) -> Result<String, TokenError> {
        self.issue(actor_id, ACCESS_TYPE, self.access_ttl_seconds)
    }

    /// Issues a refresh token for `actor_id`.
    pub fn issue_refresh(&self, actor_id: &str) -> Result<String, TokenError> {
        self.issue(actor_id, REFRESH_TYPE, self.refresh_ttl_seconds)
    }

    /// Verifies an access token and returns its claims.
    pub fn verify_access(&self, token: &str) -> Result<AppClaims, TokenError> {
        self.verify(token, ACCESS_TYPE)
    }

    /// Verifies a refresh token and returns its claims.
    pub fn verify_refresh(&self, token: &str) -> Result<AppClaims, TokenError> {
        self.verify(token, REFRESH_TYPE)
    }

    fn issue(&self, actor_id: &str, typ: &str, ttl_seconds: i64) -> Result<String, TokenError> {
        if actor_id.trim().is_empty() {
            return Err(TokenError::Invalid);
        }
        let issued_at = chrono::Utc::now().timestamp();
        let claims = AppClaims {
            sub: actor_id.to_string(),
            typ: typ.to_string(),
            iat: issued_at,
            exp: issued_at.saturating_add(ttl_seconds),
        };
        encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)
            .map_err(|_| TokenError::Invalid)
    }

    fn verify(&self, token: &str, expected_type: &str) -> Result<AppClaims, TokenError> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 0;
        validation.set_required_spec_claims(&["exp", "sub"]);
        let claims = decode::<AppClaims>(token, &self.decoding, &validation)
            .map_err(token_error)?
            .claims;
        if claims.typ != expected_type {
            return Err(TokenError::WrongType);
        }
        Ok(claims)
    }
}

fn token_error(error: jsonwebtoken::errors::Error) -> TokenError {
    if error.kind() == &jsonwebtoken::errors::ErrorKind::ExpiredSignature {
        TokenError::Expired
    } else {
        TokenError::Invalid
    }
}

fn minutes_to_seconds(minutes: u64) -> i64 {
    i64::try_from(minutes.saturating_mul(60)).unwrap_or(i64::MAX)
}

fn days_to_seconds(days: u64) -> i64 {
    i64::try_from(days.saturating_mul(86_400)).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "test-secret";

    fn service() -> TokenService {
        TokenService::new(SECRET, 15, 7)
    }

    #[test]
    fn access_round_trip_carries_subject_and_type() {
        let service = service();
        let token = service.issue_access("actor-1").expect("token");
        let claims = service.verify_access(&token).expect("valid");
        assert_eq!(claims.sub, "actor-1");
        assert_eq!(claims.typ, ACCESS_TYPE);
        assert_eq!(service.access_ttl_seconds(), 900);
        assert!(claims.exp - claims.iat == 900);
    }

    #[test]
    fn refresh_is_not_accepted_as_an_access_token() {
        let service = service();
        let refresh = service.issue_refresh("actor-1").expect("token");
        assert_eq!(service.verify_access(&refresh), Err(TokenError::WrongType));
        let access = service.issue_access("actor-1").expect("token");
        assert_eq!(service.verify_refresh(&access), Err(TokenError::WrongType));
    }

    #[test]
    fn expired_tokens_are_rejected() {
        let service = TokenService::with_ttl_seconds(SECRET, -60, -60);
        let access = service.issue_access("actor-1").expect("token");
        assert_eq!(service.verify_access(&access), Err(TokenError::Expired));
        let refresh = service.issue_refresh("actor-1").expect("token");
        assert_eq!(service.verify_refresh(&refresh), Err(TokenError::Expired));
    }

    #[test]
    fn wrong_secret_and_rs256_tokens_are_rejected() {
        let service = service();
        let token = service.issue_access("actor-1").expect("token");
        let other = TokenService::new("other-secret", 15, 7);
        assert_eq!(other.verify_access(&token), Err(TokenError::Invalid));

        let google_like = jsonwebtoken::encode(
            &Header::new(Algorithm::HS512),
            &AppClaims {
                sub: "actor-1".to_string(),
                typ: ACCESS_TYPE.to_string(),
                iat: chrono::Utc::now().timestamp(),
                exp: chrono::Utc::now().timestamp() + 600,
            },
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .expect("token");
        assert_eq!(
            service.verify_access(&google_like),
            Err(TokenError::Invalid)
        );
    }

    #[test]
    fn blank_subject_is_never_issued() {
        let service = service();
        assert_eq!(service.issue_access(" "), Err(TokenError::Invalid));
    }
}
