//! Startup fail-closed tests for authentication configuration.
//!
//! Building [`AppState`] is the startup gate: invalid auth settings must
//! return an error instead of serving an unprotected or half-configured API.

use obolargus_server::AppState;
use obolargus_server::auth::AuthSettings;
use obolargus_server::config::AppConfig;

fn enabled_config() -> AppConfig {
    AppConfig {
        auth_enabled: true,
        google_client_id: "test-client.apps.googleusercontent.com".to_string(),
        google_allowed_emails: "alice@example.com,bob@example.com".to_string(),
        primary_owner_email: "alice@example.com".to_string(),
        jwt_secret: "test-secret".to_string(),
        ..AppConfig::default()
    }
}

fn startup_error(config: AppConfig) -> String {
    match AppState::new(config, None) {
        Ok(_) => panic!("startup must fail for this configuration"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn missing_google_client_id_refuses_to_start() {
    let config = AppConfig {
        google_client_id: String::new(),
        ..enabled_config()
    };
    assert!(startup_error(config).contains("GOOGLE_CLIENT_ID"));
}

#[test]
fn an_empty_allowlist_starts_with_the_primary_owner() {
    let config = AppConfig {
        google_allowed_emails: String::new(),
        ..enabled_config()
    };
    let settings = AuthSettings::from_config(&config).expect("empty allowlist is allowed");
    assert!(settings.allows("alice@example.com"));
    assert!(!settings.allows("bob@example.com"));
}

#[test]
fn an_invalid_allowlist_refuses_to_start() {
    let config = AppConfig {
        google_allowed_emails: "not-an-email".to_string(),
        ..enabled_config()
    };
    assert!(startup_error(config).contains("GOOGLE_ALLOWED_EMAILS"));
}

#[test]
fn missing_jwt_secret_refuses_to_start() {
    let config = AppConfig {
        jwt_secret: String::new(),
        ..enabled_config()
    };
    assert!(startup_error(config).contains("JWT_SECRET"));
}

#[test]
fn a_primary_owner_outside_the_allowlist_is_still_allowed() {
    let config = AppConfig {
        primary_owner_email: "carol@example.com".to_string(),
        ..enabled_config()
    };
    let settings = AuthSettings::from_config(&config).expect("primary is always allowed");
    assert!(settings.allows("carol@example.com"));
    assert!(settings.allows("alice@example.com"));
}

#[test]
fn auth_enabled_without_a_database_refuses_to_start() {
    let error = startup_error(enabled_config());
    assert!(error.contains("DATABASE_URL"), "unexpected error: {error}");
}

#[test]
fn a_public_bind_with_auth_disabled_refuses_to_start() {
    let config = AppConfig {
        host: "0.0.0.0".to_string(),
        auth_enabled: false,
        ..AppConfig::default()
    };
    assert!(startup_error(config).contains("ALLOW_UNAUTHENTICATED"));
}

#[test]
fn the_explicit_unauthenticated_override_starts() {
    let config = AppConfig {
        host: "0.0.0.0".to_string(),
        auth_enabled: false,
        allow_unauthenticated: true,
        ..AppConfig::default()
    };
    AppState::new(config, None).expect("explicit override starts");
}

#[test]
fn auth_enabled_is_allowed_on_a_public_bind() {
    let config = AppConfig {
        host: "0.0.0.0".to_string(),
        ..enabled_config()
    };
    let settings = AuthSettings::from_config(&config).expect("public bind with auth");
    assert!(settings.enabled);
}

#[test]
fn the_default_configuration_stays_loopback_and_unauthenticated() {
    AppState::new(AppConfig::default(), None).expect("test-safe defaults");
}
