//! Application authentication.
//!
//! Google Identity Services establishes the browser identity; the server
//! verifies the Google ID token (see [`google`]), maps it to a stable actor
//! (see [`actor`]), and issues its own short-lived HS256 access and refresh
//! tokens (see [`jwt`]). [`middleware`] enforces default-deny access for every
//! route except the documented public paths.
//!
//! Contract: `specs/034-production-hardening/contracts/auth-api.md`.

pub mod actor;
pub mod bootstrap;
pub mod google;
pub mod jwt;
pub mod middleware;
pub mod rate_limit;

use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::Method;
use sqlx::PgPool;
use uuid::Uuid;

use crate::config::AppConfig;
use google::{GoogleAuthError, GoogleVerifier, JwksVerifier};

/// OpenAPI security scheme name for application bearer tokens.
pub const BEARER_SCHEME_NAME: &str = "bearer_auth";

/// Legacy owner id used only when authentication is disabled.
pub const DEV_USER_ID: &str = "dev-user";

/// Sign-in failures allowed per client per minute before a 429.
pub const SIGN_IN_FAILURES_PER_MINUTE: u32 = 5;

/// Fixed rate-limit window.
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);

/// Validated authentication settings derived from [`AppConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthSettings {
    /// Whether default-deny authentication is enforced.
    pub enabled: bool,
    /// Google OAuth web client id used as the token audience.
    pub google_client_id: String,
    /// Lower-cased allowlisted Google email addresses.
    pub allowed_emails: BTreeSet<String>,
    /// Email of the actor that owns seeded and scheduled data.
    pub primary_owner_email: String,
    /// HMAC secret for application access and refresh tokens.
    pub jwt_secret: String,
    /// Access token lifetime in minutes.
    pub access_ttl_minutes: u64,
    /// Refresh token lifetime in days.
    pub refresh_ttl_days: u64,
}

impl AuthSettings {
    /// Validates and derives settings from the environment configuration.
    ///
    /// Fails closed when authentication is enabled without its required
    /// settings, and when authentication is disabled on a non-loopback bind
    /// without the explicit `ALLOW_UNAUTHENTICATED` override.
    pub fn from_config(config: &AppConfig) -> Result<Self, AuthConfigError> {
        if !config.auth_enabled {
            if !is_loopback_host(&config.host) && !config.allow_unauthenticated {
                return Err(AuthConfigError::UnauthenticatedPublicBind);
            }
            return Ok(Self {
                enabled: false,
                google_client_id: String::new(),
                allowed_emails: BTreeSet::new(),
                primary_owner_email: String::new(),
                jwt_secret: String::new(),
                access_ttl_minutes: config.jwt_access_ttl_minutes,
                refresh_ttl_days: config.jwt_refresh_ttl_days,
            });
        }

        let google_client_id = config.google_client_id.trim().to_string();
        if google_client_id.is_empty() {
            return Err(AuthConfigError::MissingGoogleClientId);
        }
        if config.jwt_secret.trim().is_empty() {
            return Err(AuthConfigError::MissingJwtSecret);
        }

        let emails = parse_allowlist(&config.google_allowed_emails)?;
        let first = emails
            .first()
            .cloned()
            .ok_or(AuthConfigError::MissingAllowlist)?;
        let primary_owner_email = {
            let configured = config.primary_owner_email.trim();
            if configured.is_empty() {
                first
            } else {
                configured.to_ascii_lowercase()
            }
        };
        if !emails.contains(&primary_owner_email) {
            return Err(AuthConfigError::PrimaryOwnerNotAllowlisted);
        }

        Ok(Self {
            enabled: true,
            google_client_id,
            allowed_emails: emails,
            primary_owner_email,
            jwt_secret: config.jwt_secret.clone(),
            access_ttl_minutes: config.jwt_access_ttl_minutes,
            refresh_ttl_days: config.jwt_refresh_ttl_days,
        })
    }

    /// Whether `email` is on the allowlist (case-insensitive).
    pub fn allows(&self, email: &str) -> bool {
        self.allowed_emails.contains(&normalize_email(email))
    }
}

/// Errors produced while deriving [`AuthSettings`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthConfigError {
    /// Auth is disabled on a non-loopback bind without the override.
    UnauthenticatedPublicBind,
    /// `AUTH_ENABLED=true` but `GOOGLE_CLIENT_ID` is empty.
    MissingGoogleClientId,
    /// `AUTH_ENABLED=true` but the allowlist is empty.
    MissingAllowlist,
    /// The allowlist is malformed or outside the 2–5 account bound.
    InvalidAllowlist(String),
    /// `AUTH_ENABLED=true` but `JWT_SECRET` is empty.
    MissingJwtSecret,
    /// The primary owner email is not on the allowlist.
    PrimaryOwnerNotAllowlisted,
}

impl fmt::Display for AuthConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnauthenticatedPublicBind => write!(
                f,
                "authentication is disabled on a non-loopback bind; set ALLOW_UNAUTHENTICATED=true only for a trusted deployment or enable AUTH_ENABLED"
            ),
            Self::MissingGoogleClientId => {
                write!(f, "AUTH_ENABLED is true but GOOGLE_CLIENT_ID is not set")
            }
            Self::MissingAllowlist => write!(
                f,
                "AUTH_ENABLED is true but GOOGLE_ALLOWED_EMAILS is not set"
            ),
            Self::InvalidAllowlist(detail) => {
                write!(f, "GOOGLE_ALLOWED_EMAILS is invalid: {detail}")
            }
            Self::MissingJwtSecret => {
                write!(f, "AUTH_ENABLED is true but JWT_SECRET is not set")
            }
            Self::PrimaryOwnerNotAllowlisted => write!(
                f,
                "PRIMARY_OWNER_EMAIL must be one of GOOGLE_ALLOWED_EMAILS"
            ),
        }
    }
}

impl std::error::Error for AuthConfigError {}

/// Why an [`AuthState`] could not be built.
#[derive(Debug)]
pub enum AuthStateError {
    /// Invalid or incomplete authentication configuration.
    Config(AuthConfigError),
    /// Authentication is enabled but no database pool is configured.
    MissingDatabase,
}

impl fmt::Display for AuthStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => error.fmt(f),
            Self::MissingDatabase => write!(
                f,
                "AUTH_ENABLED is true but DATABASE_URL is not set; the actor registry requires PostgreSQL"
            ),
        }
    }
}

impl std::error::Error for AuthStateError {}

/// The authenticated caller for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthUser {
    /// Stable actor id (a UUID string, or `dev-user` in local mode).
    pub actor_id: String,
    /// Ownership id stored on workspace rows; equals `actor_id`.
    pub owner_id: String,
    /// Verified Google email, or the local placeholder in dev mode.
    pub email: String,
}

impl AuthUser {
    /// The legacy unauthenticated local actor.
    pub fn dev() -> Self {
        Self {
            actor_id: DEV_USER_ID.to_string(),
            owner_id: DEV_USER_ID.to_string(),
            email: "dev-user@localhost".to_string(),
        }
    }

    /// Builds the request actor for a bound registry record.
    pub fn from_actor(record: &actor::ActorRecord) -> Self {
        let owner_id = record.id.to_string();
        Self {
            actor_id: owner_id.clone(),
            owner_id,
            email: record.email.clone(),
        }
    }
}

/// Shared authentication state held by [`crate::AppState`].
pub struct AuthState {
    /// Validated settings.
    pub settings: AuthSettings,
    /// Google ID-token verifier.
    pub verifier: Arc<dyn GoogleVerifier>,
    /// In-memory actor registry used by every request.
    pub actors: actor::ActorCache,
    /// Persistence used for sign-in binding.
    store: actor::ActorStore,
    /// Application token issuer and verifier.
    tokens: jwt::TokenService,
    /// Failed sign-in exchanges per client address.
    pub sign_in_limiter: rate_limit::RateLimiter,
    /// New load starts per actor.
    pub load_limiter: rate_limit::RateLimiter,
}

impl AuthState {
    /// Builds authentication state from configuration.
    ///
    /// The actor cache is empty until [`bootstrap::run`] loads it.
    pub fn from_config(config: &AppConfig, pool: Option<PgPool>) -> Result<Self, AuthStateError> {
        let settings = AuthSettings::from_config(config).map_err(AuthStateError::Config)?;
        if settings.enabled && pool.is_none() {
            return Err(AuthStateError::MissingDatabase);
        }
        let verifier: Arc<dyn GoogleVerifier> =
            Arc::new(JwksVerifier::new(settings.google_client_id.clone()));
        let store = match pool {
            Some(pool) => actor::ActorStore::Postgres(pool),
            None => actor::ActorStore::Memory,
        };
        Ok(Self {
            tokens: jwt::TokenService::new(
                &settings.jwt_secret,
                settings.access_ttl_minutes,
                settings.refresh_ttl_days,
            ),
            sign_in_limiter: rate_limit::RateLimiter::new(
                SIGN_IN_FAILURES_PER_MINUTE,
                RATE_LIMIT_WINDOW,
            ),
            load_limiter: rate_limit::RateLimiter::new(
                config.data_load_starts_per_minute.max(1),
                RATE_LIMIT_WINDOW,
            ),
            settings,
            verifier,
            actors: actor::ActorCache::default(),
            store,
        })
    }

    /// Builds in-memory authentication state for tests and local harnesses.
    ///
    /// No database is used; actor binding lives only in the cache.
    pub fn new_in_memory(settings: AuthSettings, verifier: Arc<dyn GoogleVerifier>) -> Self {
        Self {
            tokens: jwt::TokenService::new(
                &settings.jwt_secret,
                settings.access_ttl_minutes,
                settings.refresh_ttl_days,
            ),
            sign_in_limiter: rate_limit::RateLimiter::new(
                SIGN_IN_FAILURES_PER_MINUTE,
                RATE_LIMIT_WINDOW,
            ),
            load_limiter: rate_limit::RateLimiter::new(10, RATE_LIMIT_WINDOW),
            settings,
            verifier,
            actors: actor::ActorCache::default(),
            store: actor::ActorStore::Memory,
        }
    }

    /// Builds cache-only authentication state for an unauthenticated local
    /// deployment and for tests that construct [`crate::AppState`] directly.
    pub fn dev() -> Self {
        Self::new_in_memory(
            AuthSettings {
                enabled: false,
                google_client_id: String::new(),
                allowed_emails: BTreeSet::new(),
                primary_owner_email: String::new(),
                jwt_secret: String::new(),
                access_ttl_minutes: 15,
                refresh_ttl_days: 7,
            },
            Arc::new(JwksVerifier::new("")),
        )
    }

    /// Adds a known actor without a Google exchange, for fixtures.
    pub fn seed_actor(&self, email: &str, google_sub: Option<&str>) -> actor::ActorRecord {
        let record = actor::ActorRecord {
            id: Uuid::new_v4(),
            google_sub: google_sub.map(str::to_string),
            email: normalize_email(email),
        };
        self.actors.insert(record.clone());
        record
    }

    /// Verifies a Google identity and binds it to an allowlisted actor.
    pub async fn login_with_google(
        &self,
        id_token: &str,
    ) -> Result<actor::ActorRecord, LoginError> {
        let identity = self.verifier.verify(id_token).await?;
        let email = normalize_email(&identity.email);
        if !self.settings.allows(&email) {
            return Err(LoginError::NotAllowed);
        }
        let record = actor::bind_or_create(&self.store, &self.actors, &identity.sub, &email)
            .await
            .map_err(|error| match error {
                actor::ActorError::Forbidden => LoginError::NotAllowed,
                actor::ActorError::Unavailable(detail) => LoginError::Unavailable(detail),
            })?;
        Ok(record)
    }

    /// Issues the application access and refresh tokens for one actor.
    pub fn issue_tokens(
        &self,
        actor: &actor::ActorRecord,
    ) -> Result<IssuedTokens, jwt::TokenError> {
        let owner = actor.id.to_string();
        Ok(IssuedTokens {
            access_token: self.tokens.issue_access(&owner)?,
            refresh_token: self.tokens.issue_refresh(&owner)?,
            expires_in: self.tokens.access_ttl_seconds(),
        })
    }

    /// Exchanges a refresh token for a new access token.
    ///
    /// Re-checks that the subject still exists and its email is still on the
    /// allowlist. The refresh token's own expiry is never extended.
    pub fn refresh_access_token(&self, refresh_token: &str) -> Result<IssuedToken, RefreshError> {
        let claims = self
            .tokens
            .verify_refresh(refresh_token)
            .map_err(|_| RefreshError::Invalid)?;
        let actor_id = Uuid::parse_str(&claims.sub).map_err(|_| RefreshError::Invalid)?;
        let record = self.actors.get(actor_id).ok_or(RefreshError::Invalid)?;
        if !self.settings.allows(&record.email) {
            return Err(RefreshError::Invalid);
        }
        Ok(IssuedToken {
            access_token: self
                .tokens
                .issue_access(&actor_id.to_string())
                .map_err(|_| RefreshError::Invalid)?,
            expires_in: self.tokens.access_ttl_seconds(),
        })
    }

    /// Verifies an application access token and returns the request actor.
    ///
    /// Only the actor's existence is re-checked here, so an access token
    /// issued before an allowlist removal stays valid until its short expiry;
    /// refresh re-checks the allowlist.
    pub fn authenticate_access_token(&self, token: &str) -> Result<AuthUser, AccessError> {
        let claims = self
            .tokens
            .verify_access(token)
            .map_err(|_| AccessError::Invalid)?;
        let actor_id = Uuid::parse_str(&claims.sub).map_err(|_| AccessError::Invalid)?;
        let record = self.actors.get(actor_id).ok_or(AccessError::Invalid)?;
        Ok(AuthUser::from_actor(&record))
    }

    /// The actor record that owns seeded and scheduled data, when loaded.
    pub fn primary_actor(&self) -> Option<actor::ActorRecord> {
        self.actors.by_email(&self.settings.primary_owner_email)
    }
}

/// Tokens returned by a successful sign-in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedTokens {
    /// Short-lived access token.
    pub access_token: String,
    /// Refresh token with the absolute session lifetime.
    pub refresh_token: String,
    /// Access token lifetime in seconds.
    pub expires_in: u64,
}

/// Tokens returned by a refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedToken {
    /// New short-lived access token.
    pub access_token: String,
    /// Access token lifetime in seconds.
    pub expires_in: u64,
}

/// Why a Google sign-in did not produce tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginError {
    /// The Google credential was invalid, expired, or unverified.
    InvalidCredential,
    /// The Google identity is valid but not allowed.
    NotAllowed,
    /// Google verification or persistence is unavailable.
    Unavailable(String),
}

impl From<GoogleAuthError> for LoginError {
    fn from(error: GoogleAuthError) -> Self {
        match error {
            GoogleAuthError::InvalidCredential(_) => Self::InvalidCredential,
            GoogleAuthError::Unavailable(detail) => Self::Unavailable(detail),
        }
    }
}

/// Why a refresh was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshError {
    /// The token is invalid, expired, wrong-type, or no longer allowed.
    Invalid,
}

/// Why an access token was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessError {
    /// The token is missing a valid subject, expired, or wrong-type.
    Invalid,
}

/// Returns whether a request path is exempt from authentication.
///
/// Only exact paths documented in `contracts/auth-api.md` are public. The
/// bare `/health` alias is kept because the server and gateway health probes
/// call it directly.
pub fn is_public_request(method: &Method, path: &str) -> bool {
    if method == Method::OPTIONS {
        return true;
    }
    if method == Method::GET && (path == "/health" || path == "/api/health") {
        return true;
    }
    method == Method::POST && matches!(path, "/api/auth/login" | "/api/auth/refresh")
}

/// Lower-cases and trims an email address for comparison.
pub fn normalize_email(email: &str) -> String {
    email.trim().to_ascii_lowercase()
}

fn parse_allowlist(raw: &str) -> Result<BTreeSet<String>, AuthConfigError> {
    let emails: Vec<String> = raw
        .split(',')
        .map(normalize_email)
        .filter(|email| !email.is_empty())
        .collect();
    if emails.is_empty() {
        return Err(AuthConfigError::MissingAllowlist);
    }
    if !(2..=5).contains(&emails.len()) {
        return Err(AuthConfigError::InvalidAllowlist(format!(
            "expected 2-5 unique addresses, got {}",
            emails.len()
        )));
    }
    for email in &emails {
        if !is_plausible_email(email) {
            return Err(AuthConfigError::InvalidAllowlist(
                "each entry must be an email address".to_string(),
            ));
        }
    }
    let unique: BTreeSet<String> = emails.iter().cloned().collect();
    if unique.len() != emails.len() {
        return Err(AuthConfigError::InvalidAllowlist(
            "addresses must be unique".to_string(),
        ));
    }
    Ok(unique)
}

fn is_plausible_email(email: &str) -> bool {
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && !domain.is_empty()
        && !email.contains(char::is_whitespace)
        && !domain.contains('@')
}

fn is_loopback_host(host: &str) -> bool {
    let host = host.trim().trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(emails: &str) -> AppConfig {
        AppConfig {
            host: "127.0.0.1".to_string(),
            auth_enabled: true,
            google_client_id: "client.apps.googleusercontent.com".to_string(),
            google_allowed_emails: emails.to_string(),
            primary_owner_email: String::new(),
            jwt_secret: "test-secret".to_string(),
            ..AppConfig::default()
        }
    }

    #[test]
    fn disabled_auth_on_loopback_is_allowed() {
        let settings = AuthSettings::from_config(&AppConfig::default()).expect("loopback dev");
        assert!(!settings.enabled);
    }

    #[test]
    fn disabled_auth_on_public_bind_is_refused_unless_overridden() {
        let config = AppConfig {
            host: "0.0.0.0".to_string(),
            ..AppConfig::default()
        };
        assert_eq!(
            AuthSettings::from_config(&config),
            Err(AuthConfigError::UnauthenticatedPublicBind)
        );

        let override_config = AppConfig {
            allow_unauthenticated: true,
            ..config
        };
        assert!(AuthSettings::from_config(&override_config).is_ok());
    }

    #[test]
    fn enabled_auth_is_allowed_on_a_public_bind() {
        let config = AppConfig {
            host: "0.0.0.0".to_string(),
            ..config("a@example.com,b@example.com")
        };
        assert!(AuthSettings::from_config(&config).is_ok());
    }

    #[test]
    fn enabled_auth_requires_its_settings() {
        let mut missing_client = config("a@example.com,b@example.com");
        missing_client.google_client_id = " ".to_string();
        assert_eq!(
            AuthSettings::from_config(&missing_client),
            Err(AuthConfigError::MissingGoogleClientId)
        );

        let missing_allowlist = config(" , ");
        assert_eq!(
            AuthSettings::from_config(&missing_allowlist),
            Err(AuthConfigError::MissingAllowlist)
        );

        let mut missing_secret = config("a@example.com,b@example.com");
        missing_secret.jwt_secret = String::new();
        assert_eq!(
            AuthSettings::from_config(&missing_secret),
            Err(AuthConfigError::MissingJwtSecret)
        );
    }

    #[test]
    fn allowlist_is_normalized_and_bounded() {
        let settings = AuthSettings::from_config(&config(" Alice@Example.com , BOB@example.com "))
            .expect("valid allowlist");
        assert!(settings.allows("alice@example.com"));
        assert_eq!(settings.primary_owner_email, "alice@example.com");

        assert!(matches!(
            AuthSettings::from_config(&config("only@example.com")),
            Err(AuthConfigError::InvalidAllowlist(_))
        ));
        assert!(matches!(
            AuthSettings::from_config(&config("a@x,b@x,c@x,d@x,e@x,f@x")),
            Err(AuthConfigError::InvalidAllowlist(_))
        ));
        assert!(matches!(
            AuthSettings::from_config(&config("a@x,b@x,not-an-email")),
            Err(AuthConfigError::InvalidAllowlist(_))
        ));
    }

    #[test]
    fn primary_owner_must_be_allowlisted() {
        let mut config = config("a@example.com,b@example.com");
        config.primary_owner_email = "c@example.com".to_string();
        assert_eq!(
            AuthSettings::from_config(&config),
            Err(AuthConfigError::PrimaryOwnerNotAllowlisted)
        );
    }

    #[test]
    fn public_paths_are_exact() {
        assert!(is_public_request(&Method::GET, "/health"));
        assert!(is_public_request(&Method::GET, "/api/health"));
        assert!(is_public_request(&Method::POST, "/api/auth/login"));
        assert!(is_public_request(&Method::POST, "/api/auth/refresh"));
        assert!(is_public_request(&Method::OPTIONS, "/api/automation/flows"));
        assert!(!is_public_request(&Method::GET, "/api/health/details"));
        assert!(!is_public_request(&Method::GET, "/api/auth/login"));
        assert!(!is_public_request(&Method::GET, "/api/automation/flows"));
    }

    #[test]
    fn dev_user_is_the_legacy_owner() {
        let user = AuthUser::dev();
        assert_eq!(user.actor_id, DEV_USER_ID);
        assert_eq!(user.owner_id, DEV_USER_ID);
    }
}
