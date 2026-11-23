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
#[derive(Debug, Clone, PartialEq, Envconfig)]
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
    /// Enables Google/JWT auth middleware. Requires the Google client id,
    /// allowlist, primary owner, and `JWT_SECRET`.
    #[envconfig(from = "AUTH_ENABLED", default = "true")]
    pub auth_enabled: bool,
    /// Google OAuth web client id used for the ID-token audience check.
    #[envconfig(from = "GOOGLE_CLIENT_ID", default = "")]
    pub google_client_id: String,
    /// Comma-separated allowlist of verified Google email addresses; may be
    /// empty. The primary owner is always allowed.
    #[envconfig(from = "GOOGLE_ALLOWED_EMAILS", default = "")]
    pub google_allowed_emails: String,
    /// Email of the primary actor that owns legacy data and scheduled jobs.
    /// Always allowed to sign in, even when not on the allowlist.
    #[envconfig(from = "PRIMARY_OWNER_EMAIL", default = "")]
    pub primary_owner_email: String,
    /// Explicit override allowing unauthenticated access on a non-loopback bind.
    #[envconfig(from = "ALLOW_UNAUTHENTICATED", default = "false")]
    pub allow_unauthenticated: bool,
    /// Whether to spawn the scheduled-task loop on `serve()`.
    #[envconfig(from = "RUN_SCHEDULED_TASKS", default = "true")]
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
    /// Global and per-provider concurrent call limit.
    #[envconfig(from = "MARKET_DATA_PROVIDER_MAX_CONCURRENCY", default = "4")]
    pub market_data_provider_max_concurrency: u32,
    /// Maximum provider attempts per identifier/date.
    #[envconfig(from = "MARKET_DATA_PROVIDER_MAX_RETRIES", default = "3")]
    pub market_data_provider_max_retries: u32,
    /// Exponential backoff cap in seconds.
    #[envconfig(from = "MARKET_DATA_PROVIDER_MAX_BACKOFF_SECONDS", default = "30")]
    pub market_data_provider_max_backoff_seconds: u64,
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
    /// `priority` | `named` | `first_non_empty`.
    #[envconfig(from = "OBO_TEMPORAL_PRICE_SOURCE_POLICY", default = "priority")]
    pub temporal_price_source_policy: String,

    // ---- Data Load page (spec 022) ----
    /// A running history entry older than this is labelled incomplete.
    #[envconfig(from = "DATA_LOAD_STALE_AFTER_SECONDS", default = "7200")]
    pub data_load_stale_after_seconds: i64,
    /// Maximum identifiers in one range request.
    #[envconfig(from = "DATA_LOAD_MAX_IDENTIFIERS", default = "500")]
    pub data_load_max_identifiers: usize,
    /// Maximum inclusive days in one range request.
    #[envconfig(from = "DATA_LOAD_MAX_RANGE_DAYS", default = "1826")]
    pub data_load_max_range_days: i64,
    /// Maximum unique identifiers × days in one request.
    #[envconfig(from = "DATA_LOAD_MAX_IDENTIFIER_DAYS", default = "100000")]
    pub data_load_max_identifier_days: i64,
    /// Maximum range concurrency.
    #[envconfig(from = "DATA_LOAD_MAX_CONCURRENCY", default = "32")]
    pub data_load_max_concurrency: usize,
    /// New range starts allowed per actor per minute.
    #[envconfig(from = "DATA_LOAD_STARTS_PER_MINUTE", default = "10")]
    pub data_load_starts_per_minute: u32,
    /// Retention/partition maintenance cadence.
    #[envconfig(from = "MAINTENANCE_INTERVAL_SECONDS", default = "86400")]
    pub maintenance_interval_seconds: u64,

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

    // ---- Morpheus agent chat (spec 032) ----
    /// Enables Morpheus routes, engine, and UI entry point.
    #[envconfig(from = "MORPHEUS_ENABLED", default = "false")]
    pub morpheus_enabled: bool,
    /// Conversation/message retention in days.
    #[envconfig(from = "MORPHEUS_CONVERSATION_RETENTION_DAYS", default = "365")]
    pub morpheus_conversation_retention_days: u32,
    /// OmniRoute OpenAI-compatible base URL.
    #[envconfig(from = "OMNIROUTE_BASE_URL", default = "http://omniroute:20128/v1")]
    pub omniroute_base_url: String,
    /// OmniRoute bearer credential. Lives beside DATABASE_URL in `.env`.
    #[envconfig(from = "OMNIROUTE_API_KEY", default = "")]
    pub omniroute_api_key: String,
    /// Provider display label.
    #[envconfig(from = "MORPHEUS_PROVIDER_LABEL", default = "OmniRoute")]
    pub morpheus_provider_label: String,
    /// Text/tool routing alias.
    #[envconfig(from = "MORPHEUS_MODEL", default = "auto")]
    pub morpheus_model: String,
    /// Vision routing alias; empty means `MORPHEUS_MODEL`.
    #[envconfig(from = "MORPHEUS_VISION_MODEL", default = "")]
    pub morpheus_vision_model: String,
    /// Additional system prompt text appended after the fixed preamble.
    #[envconfig(from = "MORPHEUS_SYSTEM_PROMPT", default = "")]
    pub morpheus_system_prompt: String,
    /// Acting user identifier until authentication is enabled.
    #[envconfig(from = "MORPHEUS_ACTOR_ID", default = "dev-user")]
    pub morpheus_actor_id: String,
    /// Comma-separated tool ids or `all`.
    #[envconfig(from = "MORPHEUS_TOOL_CATALOG", default = "all")]
    pub morpheus_tool_catalog: String,
    /// Allows "approve all and remember".
    #[envconfig(from = "MORPHEUS_STANDING_APPROVALS_ENABLED", default = "true")]
    pub morpheus_standing_approvals_enabled: bool,
    /// Sampling temperature.
    #[envconfig(from = "MORPHEUS_TEMPERATURE", default = "0.2")]
    pub morpheus_temperature: f64,
    /// Answer token cap.
    #[envconfig(from = "MORPHEUS_MAX_TOKENS", default = "2048")]
    pub morpheus_max_tokens: u32,
    /// Maximum user message characters.
    #[envconfig(from = "MORPHEUS_MAX_MESSAGE_CHARS", default = "8000")]
    pub morpheus_max_message_chars: usize,
    /// Maximum user message bytes.
    #[envconfig(from = "MORPHEUS_MAX_MESSAGE_BYTES", default = "32768")]
    pub morpheus_max_message_bytes: usize,
    /// Maximum prior turns in the prompt.
    #[envconfig(from = "MORPHEUS_MAX_CONTEXT_MESSAGES", default = "20")]
    pub morpheus_max_context_messages: usize,
    /// Prompt character budget.
    #[envconfig(from = "MORPHEUS_MAX_CONTEXT_CHARS", default = "48000")]
    pub morpheus_max_context_chars: usize,
    /// Maximum stored answer bytes.
    #[envconfig(from = "MORPHEUS_MAX_ANSWER_BYTES", default = "262144")]
    pub morpheus_max_answer_bytes: usize,
    /// Maximum ReACT steps per turn.
    #[envconfig(from = "MORPHEUS_MAX_REACT_STEPS", default = "12")]
    pub morpheus_max_react_steps: u32,
    /// Maximum provider calls per turn.
    #[envconfig(from = "MORPHEUS_MAX_PROVIDER_CALLS", default = "12")]
    pub morpheus_max_provider_calls: u32,
    /// Maximum tool calls per turn.
    #[envconfig(from = "MORPHEUS_MAX_TOOL_CALLS_PER_TURN", default = "16")]
    pub morpheus_max_tool_calls_per_turn: u32,
    /// Maximum UI actions per turn.
    #[envconfig(from = "MORPHEUS_MAX_UI_ACTIONS_PER_TURN", default = "20")]
    pub morpheus_max_ui_actions_per_turn: u32,
    /// Maximum screen observations per turn.
    #[envconfig(from = "MORPHEUS_MAX_OBSERVATIONS_PER_TURN", default = "12")]
    pub morpheus_max_observations_per_turn: u32,
    /// Active-work wall clock per turn.
    #[envconfig(from = "MORPHEUS_TURN_MAX_SECONDS", default = "300")]
    pub morpheus_turn_max_seconds: u64,
    /// One tool call timeout.
    #[envconfig(from = "MORPHEUS_TOOL_TIMEOUT_SECONDS", default = "30")]
    pub morpheus_tool_timeout_seconds: u64,
    /// Tool result cap sent to the model.
    #[envconfig(from = "MORPHEUS_TOOL_MAX_RESULT_BYTES", default = "32768")]
    pub morpheus_tool_max_result_bytes: usize,
    /// One UI action result timeout.
    #[envconfig(from = "MORPHEUS_UI_ACTION_TIMEOUT_SECONDS", default = "15")]
    pub morpheus_ui_action_timeout_seconds: u64,
    /// Approval decision window.
    #[envconfig(from = "MORPHEUS_APPROVAL_TIMEOUT_SECONDS", default = "300")]
    pub morpheus_approval_timeout_seconds: u64,
    /// Maximum pending approvals per turn.
    #[envconfig(from = "MORPHEUS_MAX_PENDING_APPROVALS", default = "10")]
    pub morpheus_max_pending_approvals: u32,
    /// Maximum queued messages per conversation.
    #[envconfig(from = "MORPHEUS_QUEUE_MAX_PER_CONVERSATION", default = "5")]
    pub morpheus_queue_max_per_conversation: u32,
    /// Global concurrent turns.
    #[envconfig(from = "MORPHEUS_MAX_CONCURRENT_TURNS", default = "4")]
    pub morpheus_max_concurrent_turns: u32,
    /// Sends per actor per minute.
    #[envconfig(from = "MORPHEUS_SEND_RATE_LIMIT_PER_MINUTE", default = "30")]
    pub morpheus_send_rate_limit_per_minute: u32,
    /// Maximum concurrent SSE viewers.
    #[envconfig(from = "MORPHEUS_MAX_SSE_STREAMS", default = "20")]
    pub morpheus_max_sse_streams: u32,
    /// SSE projection poll cadence.
    #[envconfig(from = "MORPHEUS_SSE_POLL_MS", default = "250")]
    pub morpheus_sse_poll_ms: u64,
    /// Maximum SSE stream lifetime.
    #[envconfig(from = "MORPHEUS_SSE_MAX_DURATION_SECONDS", default = "1800")]
    pub morpheus_sse_max_duration_seconds: u64,
    /// Provider connect timeout.
    #[envconfig(from = "MORPHEUS_CONNECT_TIMEOUT_SECONDS", default = "5")]
    pub morpheus_connect_timeout_seconds: u64,
    /// Maximum gap between provider chunks.
    #[envconfig(from = "MORPHEUS_IDLE_TIMEOUT_SECONDS", default = "30")]
    pub morpheus_idle_timeout_seconds: u64,
    /// Answer persistence cadence.
    #[envconfig(from = "MORPHEUS_STREAM_FLUSH_MS", default = "250")]
    pub morpheus_stream_flush_ms: u64,
    /// Provider status cache TTL.
    #[envconfig(from = "MORPHEUS_STATUS_PROBE_SECONDS", default = "30")]
    pub morpheus_status_probe_seconds: u64,
    /// Enables screen capture/control.
    #[envconfig(from = "MORPHEUS_VISION_ENABLED", default = "true")]
    pub morpheus_vision_enabled: bool,
    /// Image long edge.
    #[envconfig(from = "MORPHEUS_VISION_MAX_DIMENSION", default = "1280")]
    pub morpheus_vision_max_dimension: i32,
    /// JPEG quality.
    #[envconfig(from = "MORPHEUS_VISION_JPEG_QUALITY", default = "70")]
    pub morpheus_vision_jpeg_quality: u8,
    /// Upload byte cap.
    #[envconfig(from = "MORPHEUS_VISION_MAX_BYTES", default = "524288")]
    pub morpheus_vision_max_bytes: i32,
    /// Element-map cap.
    #[envconfig(from = "MORPHEUS_VISION_MAX_ELEMENTS", default = "200")]
    pub morpheus_vision_max_elements: usize,
    /// Mask-manifest cap.
    #[envconfig(from = "MORPHEUS_VISION_MAX_MASK_REGIONS", default = "200")]
    pub morpheus_vision_max_mask_regions: usize,
    /// Capture result timeout.
    #[envconfig(from = "MORPHEUS_VISION_CAPTURE_TIMEOUT_SECONDS", default = "20")]
    pub morpheus_vision_capture_timeout_seconds: u64,
    /// Observation image sweep age.
    #[envconfig(from = "MORPHEUS_OBSERVATION_TTL_SECONDS", default = "600")]
    pub morpheus_observation_ttl_seconds: u64,
    /// Control lease TTL.
    #[envconfig(from = "MORPHEUS_CONTROL_LEASE_SECONDS", default = "30")]
    pub morpheus_control_lease_seconds: u64,
    /// Test-only vision injector.
    #[envconfig(from = "MORPHEUS_VISION_TEST_MODE", default = "false")]
    pub morpheus_vision_test_mode: bool,
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
            google_client_id: String::new(),
            google_allowed_emails: String::new(),
            primary_owner_email: String::new(),
            allow_unauthenticated: false,
            run_scheduled_tasks: false,
            market_data_eod_interval_seconds: 86_400,
            market_data_reconcile_deferred: true,
            market_data_refresh_batch_size: 50,
            market_data_provider_max_concurrency: 4,
            market_data_provider_max_retries: 3,
            market_data_provider_max_backoff_seconds: 30,
            market_data_sources: "[]".to_string(),
            yahoo_finance_api_key: String::new(),
            yahoo_finance_user_agent: String::new(),
            yahoo_finance_timeout_seconds: 30,
            temporal_future_tolerance_seconds: 300,
            temporal_ingest_interval_seconds: 60,
            temporal_ingest_concurrency: 4,
            temporal_ingest_max_queue: 10_000,
            temporal_partition_ahead_months: 2,
            temporal_price_source_policy: "priority".to_string(),
            data_load_stale_after_seconds: 7_200,
            data_load_max_identifiers: 500,
            data_load_max_range_days: 1_826,
            data_load_max_identifier_days: 100_000,
            data_load_max_concurrency: 32,
            data_load_starts_per_minute: 10,
            maintenance_interval_seconds: 86_400,
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
            morpheus_enabled: false,
            morpheus_conversation_retention_days: 365,
            omniroute_base_url: "http://omniroute:20128/v1".to_string(),
            omniroute_api_key: String::new(),
            morpheus_provider_label: "OmniRoute".to_string(),
            morpheus_model: "auto".to_string(),
            morpheus_vision_model: String::new(),
            morpheus_system_prompt: String::new(),
            morpheus_actor_id: "dev-user".to_string(),
            morpheus_tool_catalog: "all".to_string(),
            morpheus_standing_approvals_enabled: true,
            morpheus_temperature: 0.2,
            morpheus_max_tokens: 2048,
            morpheus_max_message_chars: 8_000,
            morpheus_max_message_bytes: 32_768,
            morpheus_max_context_messages: 20,
            morpheus_max_context_chars: 48_000,
            morpheus_max_answer_bytes: 262_144,
            morpheus_max_react_steps: 12,
            morpheus_max_provider_calls: 12,
            morpheus_max_tool_calls_per_turn: 16,
            morpheus_max_ui_actions_per_turn: 20,
            morpheus_max_observations_per_turn: 12,
            morpheus_turn_max_seconds: 300,
            morpheus_tool_timeout_seconds: 30,
            morpheus_tool_max_result_bytes: 32_768,
            morpheus_ui_action_timeout_seconds: 15,
            morpheus_approval_timeout_seconds: 300,
            morpheus_max_pending_approvals: 10,
            morpheus_queue_max_per_conversation: 5,
            morpheus_max_concurrent_turns: 4,
            morpheus_send_rate_limit_per_minute: 30,
            morpheus_max_sse_streams: 20,
            morpheus_sse_poll_ms: 250,
            morpheus_sse_max_duration_seconds: 1_800,
            morpheus_connect_timeout_seconds: 5,
            morpheus_idle_timeout_seconds: 30,
            morpheus_stream_flush_ms: 250,
            morpheus_status_probe_seconds: 30,
            morpheus_vision_enabled: true,
            morpheus_vision_max_dimension: 1280,
            morpheus_vision_jpeg_quality: 70,
            morpheus_vision_max_bytes: 524_288,
            morpheus_vision_max_elements: 200,
            morpheus_vision_max_mask_regions: 200,
            morpheus_vision_capture_timeout_seconds: 20,
            morpheus_observation_ttl_seconds: 600,
            morpheus_control_lease_seconds: 30,
            morpheus_vision_test_mode: false,
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
            provider_max_concurrency: self.market_data_provider_max_concurrency,
            provider_max_retries: self.market_data_provider_max_retries,
            provider_max_backoff_seconds: self.market_data_provider_max_backoff_seconds,
            run_scheduled_tasks: self.run_scheduled_tasks,
        };
        config
            .validate()
            .map_err(|e| ConfigError::Load(format!("MARKET_DATA_SOURCES validation: {e}")))?;
        Ok(config)
    }

    /// Validates cross-field configuration rules.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.auth_enabled {
            if self.jwt_secret.trim().is_empty() {
                return Err(ConfigError::MissingJwtSecret);
            }
            if self.google_client_id.trim().is_empty() {
                return Err(ConfigError::Load(
                    "GOOGLE_CLIENT_ID must be set when AUTH_ENABLED=true".to_string(),
                ));
            }
            let emails = self.google_allowlist();
            if emails.iter().any(|email| !is_valid_email(email)) {
                return Err(ConfigError::Load(
                    "GOOGLE_ALLOWED_EMAILS contains an invalid email address".to_string(),
                ));
            }
            let primary = self.primary_owner_email.trim().to_lowercase();
            if primary.is_empty() {
                return Err(ConfigError::Load(
                    "PRIMARY_OWNER_EMAIL must be set when AUTH_ENABLED=true".to_string(),
                ));
            }
            if !is_valid_email(&primary) {
                return Err(ConfigError::Load(
                    "PRIMARY_OWNER_EMAIL must be a valid email address".to_string(),
                ));
            }
        } else if !self.host_is_loopback() && !self.allow_unauthenticated {
            return Err(ConfigError::Load(
                "AUTH_ENABLED=false on a non-loopback bind requires ALLOW_UNAUTHENTICATED=true"
                    .to_string(),
            ));
        }

        if !(1..=5_000).contains(&self.data_load_max_identifiers) {
            return Err(ConfigError::Load(format!(
                "DATA_LOAD_MAX_IDENTIFIERS must be in 1..=5000 (got {})",
                self.data_load_max_identifiers
            )));
        }
        if !(1..=36_500).contains(&self.data_load_max_range_days) {
            return Err(ConfigError::Load(format!(
                "DATA_LOAD_MAX_RANGE_DAYS must be in 1..=36500 (got {})",
                self.data_load_max_range_days
            )));
        }
        if !(1..=10_000_000).contains(&self.data_load_max_identifier_days) {
            return Err(ConfigError::Load(format!(
                "DATA_LOAD_MAX_IDENTIFIER_DAYS must be in 1..=10000000 (got {})",
                self.data_load_max_identifier_days
            )));
        }
        if !(1..=32).contains(&self.data_load_max_concurrency) {
            return Err(ConfigError::Load(format!(
                "DATA_LOAD_MAX_CONCURRENCY must be in 1..=32 (got {})",
                self.data_load_max_concurrency
            )));
        }
        if !(1..=600).contains(&self.data_load_starts_per_minute) {
            return Err(ConfigError::Load(format!(
                "DATA_LOAD_STARTS_PER_MINUTE must be in 1..=600 (got {})",
                self.data_load_starts_per_minute
            )));
        }
        if !(1..=64).contains(&self.market_data_provider_max_concurrency) {
            return Err(ConfigError::Load(format!(
                "MARKET_DATA_PROVIDER_MAX_CONCURRENCY must be in 1..=64 (got {})",
                self.market_data_provider_max_concurrency
            )));
        }
        if !(1..=10).contains(&self.market_data_provider_max_retries) {
            return Err(ConfigError::Load(format!(
                "MARKET_DATA_PROVIDER_MAX_RETRIES must be in 1..=10 (got {})",
                self.market_data_provider_max_retries
            )));
        }
        if !(1..=300).contains(&self.market_data_provider_max_backoff_seconds) {
            return Err(ConfigError::Load(format!(
                "MARKET_DATA_PROVIDER_MAX_BACKOFF_SECONDS must be in 1..=300 (got {})",
                self.market_data_provider_max_backoff_seconds
            )));
        }
        for (name, value) in [
            (
                "AUTOMATION_RUN_RETENTION_DAYS",
                self.automation_run_retention_days,
            ),
            (
                "AUTOMATION_AUDIT_RETENTION_DAYS",
                self.automation_audit_retention_days,
            ),
            (
                "MORPHEUS_CONVERSATION_RETENTION_DAYS",
                self.morpheus_conversation_retention_days,
            ),
        ] {
            if value == 0 {
                return Err(ConfigError::Load(format!("{name} must be > 0")));
            }
        }
        if self.maintenance_interval_seconds == 0 {
            return Err(ConfigError::Load(
                "MAINTENANCE_INTERVAL_SECONDS must be > 0".to_string(),
            ));
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
        self.validate_morpheus()?;
        Ok(())
    }

    /// Parsed, lower-cased Google allowlist.
    pub fn google_allowlist(&self) -> Vec<String> {
        self.google_allowed_emails
            .split(',')
            .map(|email| email.trim().to_lowercase())
            .filter(|email| !email.is_empty())
            .collect()
    }

    /// Whether the configured bind address is loopback-only.
    fn host_is_loopback(&self) -> bool {
        let host = self.host.trim();
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .map(|ip| ip.is_loopback())
                .unwrap_or(false)
    }

    /// Fail-fast validation for the spec-032 Morpheus keys.
    fn validate_morpheus(&self) -> Result<(), ConfigError> {
        if self.morpheus_enabled && self.omniroute_api_key.trim().is_empty() {
            return Err(ConfigError::Load(
                "OMNIROUTE_API_KEY must be set when MORPHEUS_ENABLED=true".to_string(),
            ));
        }
        let url = reqwest::Url::parse(self.omniroute_base_url.trim()).map_err(|error| {
            ConfigError::Load(format!("OMNIROUTE_BASE_URL is not a valid URL: {error}"))
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ConfigError::Load(
                "OMNIROUTE_BASE_URL must use http or https".to_string(),
            ));
        }
        for (name, value) in [
            ("MORPHEUS_PROVIDER_LABEL", &self.morpheus_provider_label),
            ("MORPHEUS_MODEL", &self.morpheus_model),
            ("MORPHEUS_ACTOR_ID", &self.morpheus_actor_id),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::Load(format!("{name} must be non-empty")));
            }
        }
        if !(0.0..=2.0).contains(&self.morpheus_temperature) {
            return Err(ConfigError::Load(
                "MORPHEUS_TEMPERATURE must be in 0.0..=2.0".to_string(),
            ));
        }
        if self.morpheus_tool_catalog.trim() != "all" {
            let ids: Vec<String> = self
                .morpheus_tool_catalog
                .split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_string)
                .collect();
            obolargus_core::morpheus::tools::ToolCatalog::new(&ids)
                .map_err(|error| ConfigError::Load(format!("MORPHEUS_TOOL_CATALOG: {error}")))?;
        }
        for (name, value, min, max) in [
            (
                "MORPHEUS_MAX_MESSAGE_CHARS",
                self.morpheus_max_message_chars as u64,
                1,
                100_000,
            ),
            (
                "MORPHEUS_MAX_MESSAGE_BYTES",
                self.morpheus_max_message_bytes as u64,
                1,
                1_048_576,
            ),
            (
                "MORPHEUS_MAX_CONTEXT_MESSAGES",
                self.morpheus_max_context_messages as u64,
                1,
                100,
            ),
            (
                "MORPHEUS_MAX_CONTEXT_CHARS",
                self.morpheus_max_context_chars as u64,
                1_000,
                400_000,
            ),
            (
                "MORPHEUS_MAX_REACT_STEPS",
                u64::from(self.morpheus_max_react_steps),
                1,
                50,
            ),
            (
                "MORPHEUS_MAX_PROVIDER_CALLS",
                u64::from(self.morpheus_max_provider_calls),
                1,
                50,
            ),
            (
                "MORPHEUS_QUEUE_MAX_PER_CONVERSATION",
                u64::from(self.morpheus_queue_max_per_conversation),
                1,
                50,
            ),
            (
                "MORPHEUS_MAX_CONCURRENT_TURNS",
                u64::from(self.morpheus_max_concurrent_turns),
                1,
                64,
            ),
            (
                "MORPHEUS_APPROVAL_TIMEOUT_SECONDS",
                self.morpheus_approval_timeout_seconds,
                30,
                3_600,
            ),
        ] {
            if !(min..=max).contains(&value) {
                return Err(ConfigError::Load(format!(
                    "{name} must be in {min}..={max} (got {value})"
                )));
            }
        }
        if self.morpheus_tool_timeout_seconds > self.morpheus_turn_max_seconds
            || self.morpheus_ui_action_timeout_seconds > self.morpheus_turn_max_seconds
        {
            return Err(ConfigError::Load(
                "tool/UI timeout must not exceed MORPHEUS_TURN_MAX_SECONDS".to_string(),
            ));
        }
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
        if !(1..=24).contains(&self.temporal_partition_ahead_months) {
            return Err(ConfigError::Load(format!(
                "OBO_TEMPORAL_PARTITION_AHEAD_MONTHS must be in 1..=24 (got {})",
                self.temporal_partition_ahead_months
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

/// Minimal email-shape check for the allowlist; Google is the real verifier.
fn is_valid_email(email: &str) -> bool {
    let email = email.trim();
    if email.is_empty() || email.chars().any(char::is_whitespace) {
        return false;
    }
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && !domain.is_empty()
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
}

#[cfg(test)]
mod tests {
    use envconfig::Envconfig;

    use super::AppConfig;
    use std::collections::HashMap;

    fn map() -> HashMap<String, String> {
        HashMap::from([
            ("AUTH_ENABLED".to_string(), "false".to_string()),
            ("RUN_SCHEDULED_TASKS".to_string(), "false".to_string()),
        ])
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
        with_secret.google_client_id = "client.apps.googleusercontent.com".to_string();
        with_secret.google_allowed_emails = "a@example.com, b@example.com".to_string();
        with_secret.primary_owner_email = "a@example.com".to_string();
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
    fn auth_allowlist_accepts_empty_and_unbounded_lists() {
        let mut config = AppConfig {
            auth_enabled: true,
            jwt_secret: "secret".to_string(),
            google_client_id: "client.apps.googleusercontent.com".to_string(),
            google_allowed_emails: String::new(),
            primary_owner_email: "primary@example.com".to_string(),
            ..AppConfig::default()
        };

        assert_eq!(config.validate(), Ok(()), "empty allowlist is allowed");

        config.google_allowed_emails = "a@example.com".to_string();
        assert_eq!(config.validate(), Ok(()), "one account is allowed");

        config.google_allowed_emails = (1..=20)
            .map(|index| format!("user{index}@example.com"))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(config.validate(), Ok(()), "no upper bound");

        config.google_allowed_emails = "not-an-email".to_string();
        assert!(config.validate().is_err());

        config.google_allowed_emails = String::new();
        config.primary_owner_email = String::new();
        assert!(config.validate().is_err(), "primary owner is required");

        config.primary_owner_email = "not-an-email".to_string();
        assert!(config.validate().is_err(), "primary owner must be an email");

        config.primary_owner_email = "carol@example.com".to_string();
        assert_eq!(
            config.validate(),
            Ok(()),
            "primary owner need not be allowlisted"
        );
    }

    #[test]
    fn unauthenticated_non_loopback_bind_requires_override() {
        let mut config = AppConfig {
            host: "0.0.0.0".to_string(),
            auth_enabled: false,
            ..AppConfig::default()
        };
        assert!(config.validate().is_err());
        config.allow_unauthenticated = true;
        assert_eq!(config.validate(), Ok(()));
        config.host = "127.0.0.1".to_string();
        config.allow_unauthenticated = false;
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn load_and_provider_limits_are_bounded() {
        let mut config = AppConfig {
            data_load_max_identifiers: 0,
            ..AppConfig::default()
        };
        assert!(config.validate().is_err());
        config.data_load_max_identifiers = 500;
        config.data_load_max_concurrency = 33;
        assert!(config.validate().is_err());
        config.data_load_max_concurrency = 32;
        config.market_data_provider_max_retries = 0;
        assert!(config.validate().is_err());
        config.market_data_provider_max_retries = 3;
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn temporal_defaults_match_the_contract() {
        let config = AppConfig::init_from_hashmap(&map()).unwrap();
        assert_eq!(config.temporal_future_tolerance_seconds, 300);
        assert_eq!(config.temporal_ingest_interval_seconds, 60);
        assert_eq!(config.temporal_ingest_concurrency, 4);
        assert_eq!(config.temporal_ingest_max_queue, 10_000);
        assert_eq!(config.temporal_partition_ahead_months, 2);
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

    #[test]
    fn morpheus_requires_key_when_enabled() {
        let config = AppConfig {
            morpheus_enabled: true,
            omniroute_api_key: String::new(),
            ..AppConfig::default()
        };
        let error = config.validate().expect_err("missing key must fail");
        assert!(format!("{error}").contains("OMNIROUTE_API_KEY"));
    }

    #[test]
    fn morpheus_accepts_a_valid_provider_configuration() {
        let config = AppConfig {
            morpheus_enabled: true,
            omniroute_api_key: "test-key".to_string(),
            omniroute_base_url: "http://omniroute:20128/v1".to_string(),
            morpheus_model: "auto".to_string(),
            ..AppConfig::default()
        };
        config.validate().expect("valid Morpheus config");
    }

    #[test]
    fn morpheus_rejects_unknown_catalog_ids() {
        let config = AppConfig {
            morpheus_tool_catalog: "assets.search,nope".to_string(),
            ..AppConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn cors_origins_trim_and_drop_empty_entries() {
        let config = AppConfig {
            cors_allowed_origins: " , http://a ,, http://b , ".to_string(),
            ..AppConfig::default()
        };
        assert_eq!(config.cors_origins(), vec!["http://a", "http://b"]);
    }

    #[test]
    fn blank_database_url_is_absent() {
        let config = AppConfig {
            database_url: "   ".to_string(),
            ..AppConfig::default()
        };
        assert_eq!(config.database_url(), None);
    }

    #[test]
    fn custom_market_data_sources_are_parsed() {
        let config = AppConfig {
            market_data_sources: r#"[{"source_id":"custom","display_name":"Custom","provider_type":"yahoo_finance","enabled":true,"priority":1,"supported_schemes":["TICKER"]}]"#.to_string(),
            ..AppConfig::default()
        };
        let market_data = config.market_data_config().expect("custom source parses");
        assert_eq!(market_data.sources.len(), 1);
        assert_eq!(market_data.sources[0].source_id, "custom");
    }

    #[test]
    fn automation_validation_covers_actor_and_zero_bounds() {
        let base = AppConfig::default();
        base.validate().expect("defaults are valid");

        let mut empty_actor = base.clone();
        empty_actor.automation_actor_id = "  ".to_string();
        assert!(empty_actor.validate().is_err());

        let mut zero_runs = base.clone();
        zero_runs.automation_max_concurrent_runs = 0;
        assert!(zero_runs.validate().is_err());

        let mut zero_interval = base.clone();
        zero_interval.automation_scheduler_interval_seconds = 0;
        assert!(zero_interval.validate().is_err());

        let mut zero_bytes = base.clone();
        zero_bytes.automation_max_flow_bytes = 0;
        assert!(zero_bytes.validate().is_err());

        let mut zero_depth = base;
        zero_depth.automation_max_subflow_depth = 0;
        assert!(zero_depth.validate().is_err());
    }

    #[test]
    fn morpheus_validation_covers_url_labels_temperature_and_bounds() {
        let base = AppConfig {
            morpheus_enabled: true,
            omniroute_api_key: "test-key".to_string(),
            ..AppConfig::default()
        };
        base.validate().expect("valid Morpheus defaults");

        let mut bad_url = base.clone();
        bad_url.omniroute_base_url = "not a url".to_string();
        assert!(bad_url.validate().is_err());

        let mut bad_scheme = base.clone();
        bad_scheme.omniroute_base_url = "ftp://omniroute/v1".to_string();
        assert!(bad_scheme.validate().is_err());

        let mut empty_label = base.clone();
        empty_label.morpheus_provider_label = " ".to_string();
        assert!(empty_label.validate().is_err());

        let mut empty_model = base.clone();
        empty_model.morpheus_model = String::new();
        assert!(empty_model.validate().is_err());

        let mut empty_actor = base.clone();
        empty_actor.morpheus_actor_id = String::new();
        assert!(empty_actor.validate().is_err());

        let mut hot = base.clone();
        hot.morpheus_temperature = 2.1;
        assert!(hot.validate().is_err());

        let mut too_many_chars = base.clone();
        too_many_chars.morpheus_max_message_chars = 100_001;
        assert!(too_many_chars.validate().is_err());

        let mut too_many_steps = base.clone();
        too_many_steps.morpheus_max_react_steps = 51;
        assert!(too_many_steps.validate().is_err());

        let mut timeout_overflow = base;
        timeout_overflow.morpheus_tool_timeout_seconds = 301;
        assert!(timeout_overflow.validate().is_err());
    }

    #[test]
    fn config_error_display_covers_both_variants() {
        let load = super::ConfigError::Load("detail".to_string());
        assert_eq!(load.to_string(), "failed to load configuration: detail");
        let missing = super::ConfigError::MissingJwtSecret;
        assert_eq!(
            missing.to_string(),
            "AUTH_ENABLED is true but JWT_SECRET is not set"
        );
    }
}
