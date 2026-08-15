//! Typed environment-based configuration via `envconfig`.
//!
//! Every runtime value is read from the environment (with `dotenvy` support
//! for a local `.env` file) and is never hardcoded. The variable names,
//! defaults, and the fail-fast validation rule are specified in
//! `specs/001-boilerplate-submodules/contracts/config-contract.md`.

use envconfig::Envconfig;
use std::fmt;

/// Application configuration for the API server.
#[derive(Debug, Clone, PartialEq, Eq, Envconfig)]
pub struct AppConfig {
    /// Bind address.
    #[envconfig(from = "APP_HOST", default = "127.0.0.1")]
    pub host: String,
    /// Bind port (the gateway forwards `/api` to this port).
    #[envconfig(from = "APP_PORT", default = "8000")]
    pub port: u16,
    /// `tracing` filter level.
    #[envconfig(from = "RUST_LOG", default = "info")]
    pub rust_log: String,
    /// PostgreSQL connection string. Empty when not configured.
    #[envconfig(from = "DATABASE_URL", default = "")]
    pub database_url: String,
    /// JWT signing secret. Required when [`AppConfig::auth_enabled`] is true.
    #[envconfig(from = "JWT_SECRET", default = "")]
    pub jwt_secret: String,
    /// Access token lifetime in minutes (per architecture security model).
    #[envconfig(from = "JWT_ACCESS_TTL_MINUTES", default = "15")]
    pub jwt_access_ttl_minutes: u64,
    /// Refresh token lifetime in days (per architecture security model).
    #[envconfig(from = "JWT_REFRESH_TTL_DAYS", default = "7")]
    pub jwt_refresh_ttl_days: u64,
    /// Comma-separated CORS allowed origins for the UI.
    #[envconfig(from = "CORS_ALLOWED_ORIGINS", default = "http://127.0.0.1:8081")]
    pub cors_allowed_origins: String,
    /// Enables JWT auth middleware. Requires a non-empty `JWT_SECRET`.
    #[envconfig(from = "AUTH_ENABLED", default = "false")]
    pub auth_enabled: bool,
}

impl AppConfig {
    /// Loads configuration from the environment and validates it.
    ///
    /// Fails fast (naming the problem) when a validation rule is violated.
    pub fn from_env() -> Result<Self, ConfigError> {
        let config = Self::init_from_env().map_err(|error| ConfigError::Load(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// The configured database URL, if any.
    pub fn database_url(&self) -> Option<&str> {
        if self.database_url.trim().is_empty() {
            None
        } else {
            Some(&self.database_url)
        }
    }

    /// CORS allowed origins, split on commas.
    pub fn cors_origins(&self) -> Vec<String> {
        self.cors_allowed_origins
            .split(',')
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(str::to_owned)
            .collect()
    }

    /// Validates cross-field configuration rules.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.auth_enabled && self.jwt_secret.trim().is_empty() {
            return Err(ConfigError::MissingJwtSecret);
        }
        Ok(())
    }
}

/// Errors produced while loading [`AppConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// The environment value was missing or failed to parse.
    Load(String),
    /// Auth is enabled but no `JWT_SECRET` was provided.
    MissingJwtSecret,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load(detail) => write!(f, "failed to load configuration: {detail}"),
            Self::MissingJwtSecret => {
                write!(f, "AUTH_ENABLED is true but JWT_SECRET is not set")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use envconfig::Envconfig;

    use super::AppConfig;
    use std::collections::HashMap;

    fn map() -> HashMap<String, String> {
        HashMap::new()
    }

    #[test]
    fn applies_defaults_when_no_variables_are_set() {
        let config = AppConfig::init_from_hashmap(&map()).unwrap();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8000);
        assert_eq!(config.rust_log, "info");
        assert_eq!(config.database_url(), None);
        assert!(!config.auth_enabled);
    }

    #[test]
    fn applies_environment_overrides() {
        let env = HashMap::from([
            ("APP_HOST".to_string(), "0.0.0.0".to_string()),
            ("APP_PORT".to_string(), "9000".to_string()),
            ("RUST_LOG".to_string(), "debug".to_string()),
            (
                "CORS_ALLOWED_ORIGINS".to_string(),
                "http://a, http://b".to_string(),
            ),
        ]);
        let config = AppConfig::init_from_hashmap(&env).unwrap();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 9000);
        assert_eq!(config.rust_log, "debug");
        assert_eq!(config.cors_origins(), vec!["http://a", "http://b"]);
    }

    #[test]
    fn database_url_is_absent_when_empty() {
        let config = AppConfig::init_from_hashmap(&map()).unwrap();
        assert_eq!(config.database_url(), None);

        let env = HashMap::from([("DATABASE_URL".to_string(), "postgres://u@h/db".to_string())]);
        let config = AppConfig::init_from_hashmap(&env).unwrap();
        assert_eq!(config.database_url(), Some("postgres://u@h/db"));
    }

    #[test]
    fn fails_to_load_when_a_value_does_not_parse() {
        let env = HashMap::from([("APP_PORT".to_string(), "not-a-port".to_string())]);
        assert!(AppConfig::init_from_hashmap(&env).is_err());
    }

    #[test]
    fn validation_requires_jwt_secret_when_auth_enabled() {
        let config = AppConfig::init_from_hashmap(&map()).unwrap();
        assert!(!config.auth_enabled);
        assert_eq!(config.validate(), Ok(()));

        let mut auth_enabled = config.clone();
        auth_enabled.auth_enabled = true;
        assert_eq!(
            auth_enabled.validate(),
            Err(super::ConfigError::MissingJwtSecret)
        );

        let mut with_secret = config.clone();
        with_secret.auth_enabled = true;
        with_secret.jwt_secret = "s3cret".to_string();
        assert_eq!(with_secret.validate(), Ok(()));
    }
}
