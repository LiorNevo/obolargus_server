//! Typed environment-based configuration via `envconfig`.
//!
//! Every runtime value is read from the environment (with `dotenvy` support
//! for a local `.env` file) and is never hardcoded. The variable names,
//! defaults, and the fail-fast validation rule are specified in
//! `specs/001-boilerplate-submodules/contracts/config-contract.md` and the
//! market-data additions in
//! `specs/017-yahoo-data-source/contracts/config-contract-additions.md`.

use envconfig::Envconfig;
use std::fmt;

use obolargus_core::market_data::config::{DataSourceConfig, MarketDataConfig};

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
    /// Whether to spawn the scheduled-task loop on `serve()`. Off by
    /// default so tests / CI can boot without scheduled work
    /// (FR-030 + spec 017 US4).
    #[envconfig(from = "RUN_SCHEDULED_TASKS", default = "false")]
    pub run_scheduled_tasks: bool,
    /// End-of-day refresh interval in seconds. Default 86400 (24 h).
    /// Implements FR-030 of spec 017.
    #[envconfig(from = "MARKET_DATA_EOD_INTERVAL_SECONDS", default = "86400")]
    pub market_data_eod_interval_seconds: u64,
    /// Reconciliation deferred flag. When `true`, `run_reconcile` is
    /// opt-in via the API; the scheduled task does NOT trigger it
    /// (FR-021 of spec 017).
    #[envconfig(from = "MARKET_DATA_RECONCILE_DEFERRED", default = "true")]
    pub market_data_reconcile_deferred: bool,
    /// Reserved batch size for the parallel-batch follow-on. Sequential
    /// in v1.
    #[envconfig(from = "MARKET_DATA_REFRESH_BATCH_SIZE", default = "50")]
    pub market_data_refresh_batch_size: u64,
    /// JSON array of per-source configs. Default = single Yahoo source.
    /// Implements FR-002 of spec 017.
    #[envconfig(from = "MARKET_DATA_SOURCES", default = "[]")]
    pub market_data_sources: String,
    /// Yahoo Finance API key (optional in v1; reserved for premium tier).
    #[envconfig(from = "YAHOO_FINANCE_API_KEY", default = "")]
    pub yahoo_finance_api_key: String,
    /// Yahoo Finance user-agent override.
    #[envconfig(from = "YAHOO_FINANCE_USER_AGENT", default = "")]
    pub yahoo_finance_user_agent: String,
    /// Yahoo Finance upstream timeout in seconds.
    #[envconfig(from = "YAHOO_FINANCE_TIMEOUT_SECONDS", default = "30")]
    pub yahoo_finance_timeout_seconds: u64,

    // ---- Temporal / intraday additions (spec 021) ----
    /// Reject observations whose valid time exceeds now by more than this.
    #[envconfig(from = "OBO_TEMPORAL_FUTURE_TOLERANCE_SECONDS", default = "300")]
    pub temporal_future_tolerance_seconds: u64,
    /// Polling cadence for intraday bars.
    #[envconfig(from = "OBO_TEMPORAL_INGEST_INTERVAL_SECONDS", default = "60")]
    pub temporal_ingest_interval_seconds: u64,
    /// Maximum in-flight fetch/storage tasks.
    #[envconfig(from = "OBO_TEMPORAL_INGEST_CONCURRENCY", default = "4")]
    pub temporal_ingest_concurrency: u32,
    /// Bounded queue size before backpressure is surfaced.
    #[envconfig(from = "OBO_TEMPORAL_INGEST_MAX_QUEUE", default = "10000")]
    pub temporal_ingest_max_queue: u32,
    /// Partitions created ahead of now.
    #[envconfig(from = "OBO_TEMPORAL_PARTITION_AHEAD_MONTHS", default = "2")]
    pub temporal_partition_ahead_months: u32,
    /// Default full-resolution retention window, in days.
    #[envconfig(from = "OBO_TEMPORAL_RETENTION_INTRADAY_DAYS", default = "90")]
    pub temporal_retention_intraday_days: u32,
    /// Default archive threshold, in days.
    #[envconfig(from = "OBO_TEMPORAL_RETENTION_ARCHIVE_DAYS", default = "365")]
    pub temporal_retention_archive_days: u32,
    /// `priority` | `named` | `first_non_empty`.
    #[envconfig(from = "OBO_TEMPORAL_PRICE_SOURCE_POLICY", default = "priority")]
    pub temporal_price_source_policy: String,

    // ---- Data Load page (spec 022) ----
    /// A running history entry older than this is labelled incomplete.
    #[envconfig(from = "DATA_LOAD_STALE_AFTER_SECONDS", default = "7200")]
    pub data_load_stale_after_seconds: i64,

    // ---- Automation flow engine (spec 030) ----
    /// Enables automation routes and background loops.
    #[envconfig(from = "AUTOMATION_ENABLED", default = "true")]
    pub automation_enabled: bool,
    /// Actor recorded in automation audit/run metadata (no auth in v1).
    #[envconfig(from = "AUTOMATION_ACTOR_ID", default = "dev-user")]
    pub automation_actor_id: String,
    /// Run/message/log retention in days.
    #[envconfig(from = "AUTOMATION_RUN_RETENTION_DAYS", default = "90")]
    pub automation_run_retention_days: u32,
    /// Audit retention in days.
    #[envconfig(from = "AUTOMATION_AUDIT_RETENTION_DAYS", default = "365")]
    pub automation_audit_retention_days: u32,
    /// Global concurrent run limit.
    #[envconfig(from = "AUTOMATION_MAX_CONCURRENT_RUNS", default = "10")]
    pub automation_max_concurrent_runs: u32,
    /// Per-flow concurrent run limit.
    #[envconfig(from = "AUTOMATION_MAX_CONCURRENT_RUNS_PER_FLOW", default = "1")]
    pub automation_max_concurrent_runs_per_flow: u32,
    /// Schedule poll cadence in seconds.
    #[envconfig(from = "AUTOMATION_SCHEDULER_INTERVAL_SECONDS", default = "30")]
    pub automation_scheduler_interval_seconds: u64,
    /// SSE database poll cadence in milliseconds.
    #[envconfig(from = "AUTOMATION_SSE_POLL_INTERVAL_MS", default = "1000")]
    pub automation_sse_poll_interval_ms: u64,
    /// Maximum SSE stream duration in seconds.
    #[envconfig(from = "AUTOMATION_SSE_MAX_DURATION_SECONDS", default = "3600")]
    pub automation_sse_max_duration_seconds: u64,
    /// Maximum concurrent SSE streams per actor.
    #[envconfig(from = "AUTOMATION_MAX_SSE_STREAMS", default = "10")]
    pub automation_max_sse_streams: u32,
    /// Maximum flow JSON size in bytes.
    #[envconfig(from = "AUTOMATION_MAX_FLOW_BYTES", default = "1048576")]
    pub automation_max_flow_bytes: usize,
    /// Maximum run input size in bytes.
    #[envconfig(from = "AUTOMATION_MAX_INPUT_BYTES", default = "1048576")]
    pub automation_max_input_bytes: usize,
    /// Maximum single message size in bytes.
    #[envconfig(from = "AUTOMATION_MAX_MESSAGE_BYTES", default = "1048576")]
    pub automation_max_message_bytes: usize,
    /// Maximum inline node output size in bytes.
    #[envconfig(from = "AUTOMATION_MAX_OUTPUT_BYTES", default = "1048576")]
    pub automation_max_output_bytes: usize,
    /// Maximum context value size in bytes.
    #[envconfig(from = "AUTOMATION_MAX_CONTEXT_BYTES", default = "262144")]
    pub automation_max_context_bytes: usize,
    /// Comma-separated capability allowlist (`all` for every built-in).
    #[envconfig(from = "AUTOMATION_ENABLED_CAPABILITIES", default = "all")]
    pub automation_enabled_capabilities: String,
    /// Loop protection: maximum messages per run.
    #[envconfig(from = "AUTOMATION_MAX_MESSAGES_PER_RUN", default = "10000")]
    pub automation_max_messages_per_run: u64,
    /// Loop protection: maximum messages per node.
    #[envconfig(from = "AUTOMATION_MAX_MESSAGES_PER_NODE", default = "1000")]
    pub automation_max_messages_per_node: u64,
    /// Loop protection: maximum run duration in seconds.
    #[envconfig(from = "AUTOMATION_MAX_RUN_DURATION_SECONDS", default = "3600")]
    pub automation_max_run_duration_seconds: u64,
    /// Maximum subflow nesting depth.
    #[envconfig(from = "AUTOMATION_MAX_SUBFLOW_DEPTH", default = "10")]
    pub automation_max_subflow_depth: u32,
    /// Run quiescence window in milliseconds.
    #[envconfig(from = "AUTOMATION_IDLE_TIMEOUT_MS", default = "2000")]
    pub automation_idle_timeout_ms: u64,
    /// Debug message buffer per run.
    #[envconfig(from = "AUTOMATION_DEBUG_BUFFER_SIZE", default = "1000")]
    pub automation_debug_buffer_size: usize,
    /// API trigger rate limit per minute.
    #[envconfig(from = "AUTOMATION_API_TRIGGER_RATE_LIMIT_PER_MINUTE", default = "60")]
    pub automation_api_trigger_rate_limit_per_minute: u32,
}

impl Default for AppConfig {
    /// The default configuration, matching the `envconfig` defaults
    /// declared on each field. Used by tests and by callers that build
    /// an `AppConfig` programmatically.
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8000,
            rust_log: "info".to_string(),
            database_url: String::new(),
            jwt_secret: String::new(),
            jwt_access_ttl_minutes: 15,
            jwt_refresh_ttl_days: 7,
            cors_allowed_origins: "http://127.0.0.1:8081".to_string(),
            auth_enabled: false,
            run_scheduled_tasks: false,
            market_data_eod_interval_seconds: 86_400,
            market_data_reconcile_deferred: true,
            market_data_refresh_batch_size: 50,
            market_data_sources: "[]".to_string(),
            yahoo_finance_api_key: String::new(),
            yahoo_finance_user_agent: String::new(),
            yahoo_finance_timeout_seconds: 30,
            temporal_future_tolerance_seconds: 300,
            temporal_ingest_interval_seconds: 60,
            temporal_ingest_concurrency: 4,
            temporal_ingest_max_queue: 10_000,
            temporal_partition_ahead_months: 2,
            temporal_retention_intraday_days: 90,
            temporal_retention_archive_days: 365,
            temporal_price_source_policy: "priority".to_string(),
            data_load_stale_after_seconds: 7_200,
            automation_enabled: true,
            automation_actor_id: "dev-user".to_string(),
            automation_run_retention_days: 90,
            automation_audit_retention_days: 365,
            automation_max_concurrent_runs: 10,
            automation_max_concurrent_runs_per_flow: 1,
            automation_scheduler_interval_seconds: 30,
            automation_sse_poll_interval_ms: 1_000,
            automation_sse_max_duration_seconds: 3_600,
            automation_max_sse_streams: 10,
            automation_max_flow_bytes: 1_048_576,
            automation_max_input_bytes: 1_048_576,
            automation_max_message_bytes: 1_048_576,
            automation_max_output_bytes: 1_048_576,
            automation_max_context_bytes: 262_144,
            automation_enabled_capabilities: "all".to_string(),
            automation_max_messages_per_run: 10_000,
            automation_max_messages_per_node: 1_000,
            automation_max_run_duration_seconds: 3_600,
            automation_max_subflow_depth: 10,
            automation_idle_timeout_ms: 2_000,
            automation_debug_buffer_size: 1_000,
            automation_api_trigger_rate_limit_per_minute: 60,
        }
    }
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

    /// Parses the `MARKET_DATA_SOURCES` JSON into typed configs.
    pub fn market_data_config(&self) -> Result<MarketDataConfig, ConfigError> {
        let sources_json = self.market_data_sources.trim();
        let sources: Vec<DataSourceConfig> = if sources_json.is_empty() || sources_json == "[]" {
            vec![DataSourceConfig::default_yahoo()]
        } else {
            serde_json::from_str(sources_json)
                .map_err(|e| ConfigError::Load(format!("MARKET_DATA_SOURCES: {e}")))?
        };
        let config = MarketDataConfig {
            sources,
            eod_interval_seconds: self.market_data_eod_interval_seconds,
            refresh_batch_size: self.market_data_refresh_batch_size,
            reconcile_deferred: self.market_data_reconcile_deferred,
            run_scheduled_tasks: self.run_scheduled_tasks,
        };
        config
            .validate()
            .map_err(|e| ConfigError::Load(format!("MARKET_DATA_SOURCES validation: {e}")))?;
        Ok(config)
    }

    /// Validates cross-field configuration rules.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.auth_enabled && self.jwt_secret.trim().is_empty() {
            return Err(ConfigError::MissingJwtSecret);
        }
        // Length-validate the optional API key per spec's v1 contract.
        if !self.yahoo_finance_api_key.is_empty() {
            let len = self.yahoo_finance_api_key.len();
            if !(16..=256).contains(&len) {
                return Err(ConfigError::Load(format!(
                    "YAHOO_FINANCE_API_KEY length must be in 16..=256 (got {len})"
                )));
            }
        }
        // Bounds-check the timeout.
        if !(1..=600).contains(&self.yahoo_finance_timeout_seconds) {
            return Err(ConfigError::Load(format!(
                "YAHOO_FINANCE_TIMEOUT_SECONDS must be in 1..=600 (got {})",
                self.yahoo_finance_timeout_seconds
            )));
        }
        self.validate_temporal()?;
        if self.data_load_stale_after_seconds <= 0 {
            return Err(ConfigError::Load(
                "DATA_LOAD_STALE_AFTER_SECONDS must be >= 1".to_string(),
            ));
        }
        self.validate_automation()?;
        Ok(())
    }

    /// Fail-fast validation for the spec-030 automation keys.
    fn validate_automation(&self) -> Result<(), ConfigError> {
        if self.automation_actor_id.trim().is_empty() {
            return Err(ConfigError::Load(
                "AUTOMATION_ACTOR_ID must be non-empty".to_string(),
            ));
        }
        for (name, value) in [
            (
                "AUTOMATION_MAX_CONCURRENT_RUNS",
                self.automation_max_concurrent_runs,
            ),
            (
                "AUTOMATION_MAX_CONCURRENT_RUNS_PER_FLOW",
                self.automation_max_concurrent_runs_per_flow,
            ),
            (
                "AUTOMATION_MAX_SSE_STREAMS",
                self.automation_max_sse_streams,
            ),
            (
                "AUTOMATION_API_TRIGGER_RATE_LIMIT_PER_MINUTE",
                self.automation_api_trigger_rate_limit_per_minute,
            ),
        ] {
            if value == 0 {
                return Err(ConfigError::Load(format!("{name} must be >= 1")));
            }
        }
        for (name, value) in [
            (
                "AUTOMATION_SCHEDULER_INTERVAL_SECONDS",
                self.automation_scheduler_interval_seconds,
            ),
            (
                "AUTOMATION_SSE_POLL_INTERVAL_MS",
                self.automation_sse_poll_interval_ms,
            ),
            (
                "AUTOMATION_MAX_MESSAGES_PER_RUN",
                self.automation_max_messages_per_run,
            ),
            (
                "AUTOMATION_MAX_MESSAGES_PER_NODE",
                self.automation_max_messages_per_node,
            ),
            (
                "AUTOMATION_MAX_RUN_DURATION_SECONDS",
                self.automation_max_run_duration_seconds,
            ),
            (
                "AUTOMATION_IDLE_TIMEOUT_MS",
                self.automation_idle_timeout_ms,
            ),
        ] {
            if value == 0 {
                return Err(ConfigError::Load(format!("{name} must be >= 1")));
            }
        }
        for (name, value) in [
            ("AUTOMATION_MAX_FLOW_BYTES", self.automation_max_flow_bytes),
            (
                "AUTOMATION_MAX_INPUT_BYTES",
                self.automation_max_input_bytes,
            ),
            (
                "AUTOMATION_MAX_MESSAGE_BYTES",
                self.automation_max_message_bytes,
            ),
            (
                "AUTOMATION_MAX_OUTPUT_BYTES",
                self.automation_max_output_bytes,
            ),
            (
                "AUTOMATION_MAX_CONTEXT_BYTES",
                self.automation_max_context_bytes,
            ),
            (
                "AUTOMATION_DEBUG_BUFFER_SIZE",
                self.automation_debug_buffer_size,
            ),
        ] {
            if value == 0 {
                return Err(ConfigError::Load(format!("{name} must be >= 1")));
            }
        }
        if self.automation_max_subflow_depth == 0 {
            return Err(ConfigError::Load(
                "AUTOMATION_MAX_SUBFLOW_DEPTH must be >= 1".to_string(),
            ));
        }
        Ok(())
    }
}

impl AppConfig {
    /// Fail-fast validation for the spec-021 keys.
    ///
    /// A zero interval, concurrency, or queue would either spin or drop
    /// data, so none of them is accepted.
    pub fn validate_temporal(&self) -> Result<(), ConfigError> {
        if self.temporal_future_tolerance_seconds > 86_400 {
            return Err(ConfigError::Load(format!(
                "OBO_TEMPORAL_FUTURE_TOLERANCE_SECONDS must be <= 86400 (got {})",
                self.temporal_future_tolerance_seconds
            )));
        }
        if self.temporal_ingest_interval_seconds == 0 {
            return Err(ConfigError::Load(
                "OBO_TEMPORAL_INGEST_INTERVAL_SECONDS must be >= 1".to_string(),
            ));
        }
        if !(1..=1024).contains(&self.temporal_ingest_concurrency) {
            return Err(ConfigError::Load(format!(
                "OBO_TEMPORAL_INGEST_CONCURRENCY must be in 1..=1024 (got {})",
                self.temporal_ingest_concurrency
            )));
        }
        if self.temporal_ingest_max_queue == 0 {
            return Err(ConfigError::Load(
                "OBO_TEMPORAL_INGEST_MAX_QUEUE must be >= 1".to_string(),
            ));
        }
        if self.temporal_partition_ahead_months > 120 {
            return Err(ConfigError::Load(format!(
                "OBO_TEMPORAL_PARTITION_AHEAD_MONTHS must be <= 120 (got {})",
                self.temporal_partition_ahead_months
            )));
        }
        if self.temporal_retention_archive_days < self.temporal_retention_intraday_days {
            return Err(ConfigError::Load(format!(
                "OBO_TEMPORAL_RETENTION_ARCHIVE_DAYS ({}) must be >= OBO_TEMPORAL_RETENTION_INTRADAY_DAYS ({})",
                self.temporal_retention_archive_days, self.temporal_retention_intraday_days
            )));
        }
        if !matches!(
            self.temporal_price_source_policy.as_str(),
            "priority" | "named" | "first_non_empty"
        ) {
            return Err(ConfigError::Load(format!(
                "OBO_TEMPORAL_PRICE_SOURCE_POLICY must be one of priority|named|first_non_empty (got {:?})",
                self.temporal_price_source_policy
            )));
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
        assert!(!config.run_scheduled_tasks);
        assert_eq!(config.market_data_eod_interval_seconds, 86_400);
        assert!(config.market_data_reconcile_deferred);
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
            (
                "MARKET_DATA_EOD_INTERVAL_SECONDS".to_string(),
                "60".to_string(),
            ),
        ]);
        let config = AppConfig::init_from_hashmap(&env).unwrap();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 9000);
        assert_eq!(config.rust_log, "debug");
        assert_eq!(config.cors_origins(), vec!["http://a", "http://b"]);
        assert_eq!(config.market_data_eod_interval_seconds, 60);
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

    #[test]
    fn api_key_length_is_validated() {
        let mut config = AppConfig::init_from_hashmap(&map()).unwrap();
        config.yahoo_finance_api_key = "short".to_string();
        assert!(config.validate().is_err());
        config.yahoo_finance_api_key = "x".repeat(257);
        assert!(config.validate().is_err());
        config.yahoo_finance_api_key = "0123456789abcdef".to_string();
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn timeout_bounds_are_validated() {
        let mut config = AppConfig::init_from_hashmap(&map()).unwrap();
        config.yahoo_finance_timeout_seconds = 0;
        assert!(config.validate().is_err());
        config.yahoo_finance_timeout_seconds = 60;
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn market_data_config_default_is_valid() {
        let app_config = AppConfig::init_from_hashmap(&map()).unwrap();
        let market_data = app_config.market_data_config().expect("market_data config");
        assert_eq!(market_data.sources.len(), 1);
        assert_eq!(market_data.sources[0].source_id, "yahoo_finance");
    }

    #[test]
    fn market_data_sources_invalid_json_fails_fast() {
        let mut app_config = AppConfig::init_from_hashmap(&map()).unwrap();
        app_config.market_data_sources = "not json".to_string();
        let result = app_config.market_data_config();
        assert!(matches!(
            result,
            Err(super::ConfigError::Load(ref msg)) if msg.contains("MARKET_DATA_SOURCES")
        ));
    }

    #[test]
    fn market_data_sources_with_duplicate_ids_validates_uniqueness() {
        let json = r#"[{"source_id":"a","display_name":"A","provider_type":"yahoo_finance","enabled":true,"priority":1,"supported_schemes":["TICKER"]},{"source_id":"a","display_name":"A2","provider_type":"yahoo_finance","enabled":true,"priority":2,"supported_schemes":["TICKER"]}]"#;
        let mut app_config = AppConfig::init_from_hashmap(&map()).unwrap();
        app_config.market_data_sources = json.to_string();
        let result = app_config.market_data_config();
        assert!(
            matches!(result, Err(super::ConfigError::Load(ref msg)) if msg.contains("duplicate"))
        );
    }

    #[test]
    fn temporal_defaults_match_the_contract() {
        let config = AppConfig::init_from_hashmap(&map()).unwrap();
        assert_eq!(config.temporal_future_tolerance_seconds, 300);
        assert_eq!(config.temporal_ingest_interval_seconds, 60);
        assert_eq!(config.temporal_ingest_concurrency, 4);
        assert_eq!(config.temporal_ingest_max_queue, 10_000);
        assert_eq!(config.temporal_partition_ahead_months, 2);
        assert_eq!(config.temporal_retention_intraday_days, 90);
        assert_eq!(config.temporal_retention_archive_days, 365);
        assert_eq!(config.temporal_price_source_policy, "priority");
        config.validate_temporal().expect("defaults are valid");
    }

    #[test]
    fn temporal_validation_rejects_zero_and_inverted_values() {
        let base = AppConfig::default();

        let mut zero_interval = base.clone();
        zero_interval.temporal_ingest_interval_seconds = 0;
        assert!(zero_interval.validate_temporal().is_err());

        let mut zero_concurrency = base.clone();
        zero_concurrency.temporal_ingest_concurrency = 0;
        assert!(zero_concurrency.validate_temporal().is_err());

        let mut zero_queue = base.clone();
        zero_queue.temporal_ingest_max_queue = 0;
        assert!(zero_queue.validate_temporal().is_err());

        let mut inverted = base.clone();
        inverted.temporal_retention_intraday_days = 400;
        inverted.temporal_retention_archive_days = 90;
        assert!(inverted.validate_temporal().is_err());

        let mut bad_policy = base.clone();
        bad_policy.temporal_price_source_policy = "loudest".to_string();
        assert!(bad_policy.validate_temporal().is_err());

        let mut absurd_tolerance = base;
        absurd_tolerance.temporal_future_tolerance_seconds = 86_401;
        assert!(absurd_tolerance.validate_temporal().is_err());
    }

    #[test]
    fn temporal_keys_accept_environment_overrides() {
        let env = HashMap::from([
            (
                "OBO_TEMPORAL_INGEST_CONCURRENCY".to_string(),
                "8".to_string(),
            ),
            (
                "OBO_TEMPORAL_PRICE_SOURCE_POLICY".to_string(),
                "named".to_string(),
            ),
        ]);
        let config = AppConfig::init_from_hashmap(&env).unwrap();
        assert_eq!(config.temporal_ingest_concurrency, 8);
        assert_eq!(config.temporal_price_source_policy, "named");
    }
}
