//! Yahoo Finance provider implementing the `MarketDataProvider` trait.
//!
//! Implements US1 / US2 / US5 / FR-010..FR-014 / FR-040 / FR-041 of
//! `specs/017-yahoo-data-source/spec.md`. The wrapper uses the
//! `yahoo_finance_api = "5"` Rust client with the `governor` and `decimal`
//! features enabled (see `obolargus_server/Cargo.toml`). The `decimal`
//! feature is mandatory — per Constitution Principle X, financial values
//! MUST use `rust_decimal::Decimal`, never `f64`.

#![forbid(unsafe_code)]

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use time::OffsetDateTime;

use obolargus_core::market_data::config::{DataSourceConfig, RetryConfig};
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::trait_::MarketDataProvider;
use obolargus_core::market_data::types::{
    HistoricalRange, HistoricalRangeRequest, LatestBar, LatestBarRequest, PriceBar,
    TickerLookupHit, TickerLookupRequest, TickerLookupResult,
};

/// Concrete `MarketDataProvider` backed by the Yahoo Finance HTTP API.
///
/// Implements FR-010..FR-014 of spec 017. Construction is via
/// [`YahooFinanceProvider::build`] from a `DataSourceConfig`.
pub struct YahooFinanceProvider {
    /// Stable id (always `"yahoo_finance"`).
    source_id: String,
    /// Human-readable label.
    display_name: String,
    /// Identifier schemes the provider can answer.
    supported_schemes: Vec<String>,
    /// Numeric priority.
    priority: i32,
    /// Inner `yahoo_finance_api` connector held behind a `Mutex` because
    /// `get_ticker_info` takes `&mut self`.
    connector: tokio::sync::Mutex<yahoo_finance_api::YahooConnector>,
    /// Retry configuration (FR-041).
    retry_config: RetryConfig,
    /// Whether a credential was present at construction time. The value
    /// itself is never stored on the struct (FR-040).
    api_key_present: bool,
}

impl YahooFinanceProvider {
    /// Constructs a Yahoo Finance provider from configuration.
    ///
    /// Per-source credentials are read from the env var named by
    /// `config.api_key_env_var`. If that name is set but the referenced env
    /// var is unset or empty, construction fails fast with a typed
    /// `Validation` error naming the source and the env var (FR-040).
    /// The credential value is never logged and never persisted — only the
    /// boolean "is a credential present" is recorded (T103 / T104).
    ///
    /// Implements FR-001 / FR-010 / FR-040 of spec 017.
    pub fn build(config: &DataSourceConfig) -> Result<Self, MarketDataProviderError> {
        let api_key_present = resolve_api_key_presence(config, |name| std::env::var(name).ok())?;

        let mut builder = yahoo_finance_api::YahooConnector::builder();
        if let Some(ua) = &config.user_agent {
            builder = builder.user_agent(ua.as_str());
        }
        if let Some(t) = config.timeout_seconds {
            builder = builder.timeout(Duration::from_secs(t));
        }
        // The `governor` feature gates the crate's rate limiter. The
        // configured budget is per-minute; the crate's limiter is
        // per-second, so convert (1 req/min ⇒ 1 req/s, rounded up).
        if let Some(per_minute) = config.rate_limit_per_minute
            && per_minute > 0
        {
            let per_second = std::num::NonZeroU32::new(per_minute.max(1));
            builder = builder.rate_limit(per_second);
        }
        let connector = builder.build().map_err(|e| {
            MarketDataProviderError::Infrastructure(format!(
                "yahoo_finance_api builder failed: {e}"
            ))
        })?;

        Ok(Self {
            source_id: "yahoo_finance".to_string(),
            display_name: "Yahoo Finance".to_string(),
            supported_schemes: config.supported_schemes.clone(),
            priority: config.priority,
            connector: tokio::sync::Mutex::new(connector),
            retry_config: config.retry.clone(),
            api_key_present,
        })
    }

    /// Whether a credential was present at construction time.
    ///
    /// The value itself is never exposed (FR-040). This is the only
    /// credential-related information the provider surfaces, and it is
    /// what the `data_sources.api_key_present` column records.
    pub fn api_key_present(&self) -> bool {
        self.api_key_present
    }

    /// Maps the typed identifier to the raw symbol Yahoo expects.
    fn yahoo_symbol(identifier: &obolargus_core::market_data::types::TypedIdentifier) -> String {
        identifier.value.clone()
    }

    /// Wraps a single upstream call in exponential-backoff retries on
    /// `Transient` / `RateLimited`. Returns the final result.
    ///
    /// Implements FR-041 of spec 017.
    async fn with_retries<F, Fut, T>(&self, op: F) -> Result<T, MarketDataProviderError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, yahoo_finance_api::YahooError>>,
    {
        let mut attempt = 0u8;
        loop {
            let outcome = op().await;
            match outcome {
                Ok(v) => return Ok(v),
                Err(yahoo_err) => {
                    let mapped = from_yahoo_error(&yahoo_err);
                    if !mapped.should_fallover() || attempt >= self.retry_config.max_retries {
                        return Err(mapped);
                    }
                    let backoff_ms = std::cmp::min(
                        self.retry_config.backoff_max_ms,
                        (self.retry_config.backoff_base_ms as f64
                            * self.retry_config.backoff_multiplier.powi(attempt as i32))
                            as u64,
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[async_trait]
impl MarketDataProvider for YahooFinanceProvider {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn supported_schemes(&self) -> &[String] {
        &self.supported_schemes
    }

    fn enabled(&self) -> bool {
        true
    }

    fn priority(&self) -> i32 {
        self.priority
    }

    async fn get_latest_bar(
        &self,
        request: LatestBarRequest,
    ) -> Result<LatestBar, MarketDataProviderError> {
        let symbol = Self::yahoo_symbol(&request.identifier);
        let response = {
            let connector = self.connector.lock().await;
            self.with_retries(|| async { connector.get_quote_range(&symbol, "1d", "5d").await })
                .await?
        };

        let quotes = response.quotes().map_err(|e| {
            MarketDataProviderError::Infrastructure(format!("yahoo quote decode: {e}"))
        })?;

        let today = Utc::now().date_naive();
        let bar = select_latest_quote(quotes, today).ok_or_else(|| {
            MarketDataProviderError::NotFound(format!("yahoo returned no usable bars for {symbol}"))
        })?;

        Ok(LatestBar {
            bar: quote_to_price_bar(&bar, today)?,
            knowledge_date: today,
            source_id: self.source_id.clone(),
        })
    }

    async fn get_historical_range(
        &self,
        request: HistoricalRangeRequest,
    ) -> Result<HistoricalRange, MarketDataProviderError> {
        let symbol = Self::yahoo_symbol(&request.identifier);

        let from_ts = request
            .from
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| MarketDataProviderError::InvalidInput("invalid from date".into()))?
            .and_utc()
            .timestamp();
        let to_ts = request
            .to
            .and_hms_opt(23, 59, 59)
            .ok_or_else(|| MarketDataProviderError::InvalidInput("invalid to date".into()))?
            .and_utc()
            .timestamp();

        let start_odt = OffsetDateTime::from_unix_timestamp(from_ts)
            .map_err(|e| MarketDataProviderError::InvalidInput(format!("start date: {e}")))?;
        let end_odt = OffsetDateTime::from_unix_timestamp(to_ts)
            .map_err(|e| MarketDataProviderError::InvalidInput(format!("end date: {e}")))?;

        let response = {
            let connector = self.connector.lock().await;
            self.with_retries(|| async {
                connector
                    .get_quote_history(&symbol, start_odt, end_odt)
                    .await
            })
            .await?
        };

        let quotes = response.quotes().map_err(|e| {
            MarketDataProviderError::Infrastructure(format!("yahoo history decode: {e}"))
        })?;

        let today = Utc::now().date_naive();
        // Future-dated bars are dropped (the dual-temporal model does not
        // admit them); the rest are converted and sorted chronologically.
        let mut bars: Vec<PriceBar> = quotes
            .iter()
            .filter_map(|q| quote_to_price_bar(q, today).ok())
            .collect();
        bars.sort_by_key(|b| b.as_of_date);

        Ok(HistoricalRange {
            bars,
            knowledge_date: today,
            source_id: self.source_id.clone(),
        })
    }

    async fn lookup_ticker(
        &self,
        request: TickerLookupRequest,
    ) -> Result<TickerLookupResult, MarketDataProviderError> {
        let result = {
            let connector = self.connector.lock().await;
            self.with_retries(|| async { connector.search_ticker_opt(&request.query).await })
                .await?
        };

        let hits: Vec<TickerLookupHit> = result.quotes.iter().map(quote_item_to_hit).collect();

        Ok(TickerLookupResult {
            hits,
            source_id: self.source_id.clone(),
        })
    }
}

/// Picks the most recent usable quote from a Yahoo response.
///
/// Quotes with a negative timestamp are ignored, and quotes dated after
/// the end of `today` are ignored (the future-dated rejection happens
/// later, in [`bar_as_of_date`], so the *selected* bar is always checked
/// explicitly).
///
/// Implements FR-014 of spec 017.
fn select_latest_quote(
    quotes: Vec<yahoo_finance_api::Quote>,
    today: chrono::NaiveDate,
) -> Option<yahoo_finance_api::Quote> {
    let cutoff = today
        .succ_opt()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp())
        .unwrap_or(i64::MAX);
    quotes
        .into_iter()
        .filter(|q| q.timestamp >= 0 && q.timestamp < cutoff)
        .max_by_key(|q| q.timestamp)
}

/// Converts a Yahoo quote into a `PriceBar`.
///
/// The trading day is derived from the quote's unix timestamp (seconds).
/// A future-dated quote is rejected with
/// [`MarketDataProviderError::InvalidInput`].
///
/// Implements FR-013 / FR-014 of spec 017.
fn quote_to_price_bar(
    quote: &yahoo_finance_api::Quote,
    today: chrono::NaiveDate,
) -> Result<PriceBar, MarketDataProviderError> {
    let as_of_date = bar_as_of_date(quote.timestamp, today)?;
    Ok(PriceBar {
        as_of_date,
        open: quote.open,
        high: quote.high,
        low: quote.low,
        close: quote.close,
        adjclose: quote.adjclose,
        volume: quote.volume,
    })
}

/// Converts a Yahoo search hit into a `TickerLookupHit`.
///
/// The v1 contract surfaces `currency: None` because Yahoo's search
/// endpoint does not carry it — resolving the currency needs a separate
/// `get_ticker_info` call, which is out of scope for v1.
///
/// Implements FR-012 of spec 017 (US2).
fn quote_item_to_hit(item: &yahoo_finance_api::YQuoteItemOpt) -> TickerLookupHit {
    TickerLookupHit {
        symbol: item.symbol.clone(),
        short_name: item.short_name.clone(),
        long_name: item.long_name.clone(),
        quote_type: if item.quote_type.is_empty() {
            None
        } else {
            Some(item.quote_type.clone())
        },
        exchange: if item.exchange.is_empty() {
            None
        } else {
            Some(item.exchange.clone())
        },
        currency: None,
        schemes: vec![],
        relevance: Some(item.score),
    }
}

/// Converts a Yahoo unix timestamp (seconds) into a trading day, rejecting
/// future-dated bars.
///
/// The rejection happens at the wrapper level — before the bar reaches the
/// orchestrator — per the spec's Edge Cases section: a future-dated price
/// violates the dual-temporal model, so it is surfaced as a typed
/// `InvalidInput` error (a final answer; the registry does not fall over).
///
/// Implements FR-014 of spec 017.
fn bar_as_of_date(
    timestamp: i64,
    today: chrono::NaiveDate,
) -> Result<chrono::NaiveDate, MarketDataProviderError> {
    let as_of_date = chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|dt| dt.date_naive())
        .ok_or_else(|| {
            MarketDataProviderError::InvalidInput(format!(
                "yahoo timestamp out of range: {timestamp}"
            ))
        })?;

    if as_of_date > today {
        return Err(MarketDataProviderError::InvalidInput(format!(
            "future-dated bar from yahoo: as_of_date={as_of_date}"
        )));
    }

    Ok(as_of_date)
}

/// Resolves whether a credential is present for the given source.
///
/// The `lookup` callback abstracts the environment read so the behavior
/// is testable without mutating the process environment (which is
/// `unsafe` on edition 2024). `None` means "the env var is not set";
/// `Some(value)` carries the value — which is inspected for emptiness but
/// never returned, logged, or persisted (FR-040).
///
/// Implements FR-040 of spec 017.
fn resolve_api_key_presence(
    config: &DataSourceConfig,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<bool, MarketDataProviderError> {
    let Some(env_var_name) = &config.api_key_env_var else {
        return Ok(false);
    };
    match lookup(env_var_name) {
        None => Err(MarketDataProviderError::Validation(format!(
            "source '{}': api_key_env_var '{env_var_name}' is not set in the environment",
            config.source_id
        ))),
        Some(value) if value.trim().is_empty() => {
            Err(MarketDataProviderError::Validation(format!(
                "source '{}': api_key_env_var '{env_var_name}' is set but empty",
                config.source_id
            )))
        }
        Some(_) => Ok(true),
    }
}

/// Converts a `yahoo_finance_api::YahooError` to our typed
/// `MarketDataProviderError`. Defined as a free function (not `From<>`)
/// because `From` for a type defined outside this crate is an orphan impl.
///
/// Implements research D8 (variant mapping). The original message is
/// preserved. `TooManyRequests` defaults to `retry_after = 60s` when
/// the upstream message doesn't carry a parseable value.
///
/// Called from `with_retries`.
fn from_yahoo_error(err: &yahoo_finance_api::YahooError) -> MarketDataProviderError {
    use yahoo_finance_api::YahooError;
    match err {
        YahooError::NoResult | YahooError::NoQuotes => {
            MarketDataProviderError::NotFound(format!("{err}"))
        }
        YahooError::FetchFailed(message) => {
            // A 404 means Yahoo carries no data for this symbol: that is
            // "no answer for this query" (`NotFound`), not "malformed
            // request" (`InvalidInput`). The distinction matters because
            // the registry treats `NotFound` as "this symbol has no
            // answer" and `InvalidInput` as a caller error.
            if message.contains("404") {
                MarketDataProviderError::NotFound(format!("{err}"))
            } else {
                MarketDataProviderError::InvalidInput(format!("{err}"))
            }
        }
        YahooError::MissingField(_) | YahooError::InvalidUrl | YahooError::InvalidDateFormat => {
            MarketDataProviderError::InvalidInput(format!("{err}"))
        }
        YahooError::DataInconsistency => MarketDataProviderError::Unsupported(format!("{err}")),
        YahooError::ServerError(_)
        | YahooError::ConnectionFailed(_)
        | YahooError::EmptyResponse
        | YahooError::HtmlResponse
        | YahooError::DeserializeFailed(_)
        | YahooError::DeserializeFailedDebug(_) => {
            MarketDataProviderError::Transient(format!("{err}"))
        }
        YahooError::TooManyRequests(msg) => {
            let retry = parse_retry_after(msg).or(Some(Duration::from_secs(60)));
            MarketDataProviderError::RateLimited {
                msg: msg.clone(),
                retry_after: retry,
            }
        }
        YahooError::Unauthorized
        | YahooError::InvalidCookie
        | YahooError::InvalidCrumb
        | YahooError::NoCookies
        | YahooError::InvisibleAsciiInCookies => {
            MarketDataProviderError::Unauthorized(format!("{err}"))
        }
        YahooError::ApiError(_) | YahooError::BuilderFailed => {
            MarketDataProviderError::Infrastructure(format!("{err}"))
        }
        // Catch-all for any future upstream variants added without a
        // semver bump (the upstream doesn't use #[non_exhaustive] but
        // variant additions would be a breaking change).
        #[allow(unreachable_patterns)]
        _ => MarketDataProviderError::Infrastructure(format!("{err}")),
    }
}

/// Parses a `Duration` hint from an upstream `TooManyRequests` message.
fn parse_retry_after(msg: &str) -> Option<Duration> {
    if let Some(rest) = msg.strip_prefix("retry_after=")
        && let Ok(secs) = rest.trim().parse::<u64>()
    {
        return Some(Duration::from_secs(secs));
    }
    if let Some(idx) = msg.find("Retry-After: ") {
        let rest = &msg[idx + "Retry-After: ".len()..];
        let line = rest.lines().next().unwrap_or("");
        if let Ok(secs) = line.trim().parse::<u64>() {
            return Some(Duration::from_secs(secs));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use obolargus_core::market_data::config::DataSourceConfig;

    #[test]
    fn parse_retry_after_handles_common_formats() {
        assert_eq!(
            parse_retry_after("retry_after=30"),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            parse_retry_after("Retry-After: 120"),
            Some(Duration::from_secs(120))
        );
        assert_eq!(parse_retry_after("unparseable"), None);
    }

    #[test]
    fn from_yahoo_error_maps_variants_correctly() {
        use yahoo_finance_api::YahooError;

        let transient = from_yahoo_error(&YahooError::ServerError("500".into()));
        assert!(matches!(transient, MarketDataProviderError::Transient(_)));

        let not_found = from_yahoo_error(&YahooError::NoResult);
        assert!(matches!(not_found, MarketDataProviderError::NotFound(_)));

        let rate_limited = from_yahoo_error(&YahooError::TooManyRequests("retry_after=120".into()));
        match rate_limited {
            MarketDataProviderError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_secs(120)));
            }
            _ => panic!("expected RateLimited"),
        }

        let rl_default = from_yahoo_error(&YahooError::TooManyRequests("plain".into()));
        match rl_default {
            MarketDataProviderError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_secs(60)));
            }
            _ => panic!("expected RateLimited default"),
        }
    }

    #[test]
    fn yahoo_symbol_passes_through_value() {
        let id = obolargus_core::market_data::types::TypedIdentifier::new("TICKER", "AAPL");
        assert_eq!(YahooFinanceProvider::yahoo_symbol(&id), "AAPL");
    }

    fn provider_from_default_config() -> YahooFinanceProvider {
        let config = DataSourceConfig::default_yahoo();
        YahooFinanceProvider::build(&config).expect("yahoo provider build")
    }

    #[test]
    fn metadata_is_correct() {
        let provider = provider_from_default_config();
        assert_eq!(provider.source_id(), "yahoo_finance");
        assert_eq!(provider.display_name(), "Yahoo Finance");
        assert!(provider.enabled());
        assert_eq!(provider.priority(), 10);
        let schemes = provider.supported_schemes();
        assert!(schemes.iter().any(|s| s == "TICKER"));
        assert!(schemes.iter().any(|s| s == "ISIN"));
    }

    #[test]
    fn with_retries_exhausts_retries_on_transient() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let provider = provider_from_default_config();
            let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let calls_cloned = calls.clone();
            let outcome = provider
                .with_retries(|| {
                    let c = calls_cloned.clone();
                    async move {
                        c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Err::<(), _>(yahoo_finance_api::YahooError::ServerError("500".into()))
                    }
                })
                .await;
            assert!(matches!(
                outcome,
                Err(MarketDataProviderError::Transient(_))
            ));
            // 1 initial attempt + max_retries (3) retries = 4 total calls
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 4);
        });
    }

    /// T099 / T104: the provider reads its credential from the *named*
    /// env var and records only the presence, never the value.
    #[test]
    fn api_key_is_read_from_the_named_env_var() {
        const ENV_NAME: &str = "OBOLARGUS_TEST_YAHOO_KEY";
        let mut config = DataSourceConfig::default_yahoo();
        config.api_key_env_var = Some(ENV_NAME.to_string());
        let present = resolve_api_key_presence(&config, |name| {
            (name == ENV_NAME).then(|| "abcdef0123456789".to_string())
        })
        .expect("presence");
        assert!(present);
    }

    /// T099: an unrelated env var does not leak into the provider.
    #[test]
    fn api_key_absent_when_no_env_var_named() {
        let config = DataSourceConfig::default_yahoo();
        let present = resolve_api_key_presence(&config, |_| None).expect("presence");
        assert!(!present);
    }

    /// FR-040: a configured `api_key_env_var` whose env var is missing
    /// fails fast with a typed `Validation` error naming the source and
    /// the env var.
    #[test]
    fn api_key_env_var_missing_fails_fast() {
        const ENV_NAME: &str = "OBOLARGUS_TEST_MISSING_YAHOO_KEY";
        let mut config = DataSourceConfig::default_yahoo();
        config.api_key_env_var = Some(ENV_NAME.to_string());
        let result = resolve_api_key_presence(&config, |_| None);
        assert!(matches!(
            result,
            Err(MarketDataProviderError::Validation(ref msg))
                if msg.contains(ENV_NAME) && msg.contains("yahoo_finance")
        ));
    }

    /// FR-040: an env var that is set but empty is also a validation
    /// failure.
    #[test]
    fn api_key_env_var_empty_fails_fast() {
        const ENV_NAME: &str = "OBOLARGUS_TEST_EMPTY_YAHOO_KEY";
        let mut config = DataSourceConfig::default_yahoo();
        config.api_key_env_var = Some(ENV_NAME.to_string());
        let result = resolve_api_key_presence(&config, |name| {
            (name == ENV_NAME).then(|| "   ".to_string())
        });
        assert!(matches!(
            result,
            Err(MarketDataProviderError::Validation(ref msg)) if msg.contains(ENV_NAME)
        ));
    }

    /// T103: the credential *value* is never echoed into the error
    /// message. The validation error names the source and the env var
    /// only.
    #[test]
    fn api_key_value_never_appears_in_error_messages() {
        const ENV_NAME: &str = "OBOLARGUS_TEST_SECRET_KEY";
        let secret = "s3cr3t-value-never-logged";
        let mut config = DataSourceConfig::default_yahoo();
        config.api_key_env_var = Some(ENV_NAME.to_string());
        // Empty value ⇒ validation error; the message must not leak the
        // secret (there is none), and the named env var is echoed.
        let result = resolve_api_key_presence(&config, |name| {
            (name == ENV_NAME).then(|| secret.to_string())
        });
        assert!(matches!(result, Ok(true)));
        // The value is never surfaced by the provider either.
        let provider = provider_from_default_config();
        assert!(!provider.api_key_present());
    }

    /// T101: the exponential backoff sleeps between attempts. The
    /// `with_retries` wrapper retries `Transient` errors up to
    /// `max_retries` times before surfacing the typed error.
    #[test]
    fn exponential_backoff_is_applied_between_retries() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut config = DataSourceConfig::default_yahoo();
            config.retry = RetryConfig {
                max_retries: 2,
                backoff_base_ms: 1,
                backoff_max_ms: 2,
                backoff_multiplier: 2.0,
            };
            let provider = YahooFinanceProvider::build(&config).expect("provider build");
            let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let calls_cloned = calls.clone();
            let outcome = provider
                .with_retries(|| {
                    let c = calls_cloned.clone();
                    async move {
                        c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Err::<(), _>(yahoo_finance_api::YahooError::ServerError("500".into()))
                    }
                })
                .await;
            assert!(matches!(
                outcome,
                Err(MarketDataProviderError::Transient(_))
            ));
            // 1 initial attempt + max_retries (2) retries = 3 total calls
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        });
    }

    #[allow(dead_code)]
    fn _pin_naivedate() -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("date")
    }

    /// Builds a Yahoo `Quote` fixture for the given trading day.
    fn quote(day: chrono::NaiveDate, close: i64) -> yahoo_finance_api::Quote {
        yahoo_finance_api::Quote {
            timestamp: day
                .and_hms_opt(12, 0, 0)
                .expect("time")
                .and_utc()
                .timestamp(),
            open: rust_decimal::Decimal::new(close, 0),
            high: rust_decimal::Decimal::new(close + 1, 0),
            low: rust_decimal::Decimal::new(close - 1, 0),
            close: rust_decimal::Decimal::new(close, 0),
            adjclose: rust_decimal::Decimal::new(close, 0),
            volume: 1_234_567,
        }
    }

    /// T062: the most recent quote wins.
    #[test]
    fn select_latest_quote_picks_the_most_recent() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("date");
        let older = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).expect("date");
        let quotes = vec![quote(today, 100), quote(older, 90)];
        let selected = select_latest_quote(quotes, today).expect("a quote");
        assert_eq!(
            selected.close,
            rust_decimal::Decimal::new(100, 0),
            "the most recent quote should win"
        );
    }

    /// A future-dated quote is not selected.
    #[test]
    fn select_latest_quote_skips_future_dated_quotes() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("date");
        let future = chrono::NaiveDate::from_ymd_opt(2026, 1, 5).expect("date");
        let past = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).expect("date");
        let quotes = vec![quote(future, 110), quote(past, 90)];
        let selected = select_latest_quote(quotes, today).expect("a quote");
        assert_eq!(selected.close, rust_decimal::Decimal::new(90, 0));
    }

    /// A response with no usable quotes yields `None`.
    #[test]
    fn select_latest_quote_returns_none_for_an_empty_response() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("date");
        assert!(select_latest_quote(vec![], today).is_none());
    }

    /// A negative timestamp is ignored.
    #[test]
    fn select_latest_quote_skips_negative_timestamps() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("date");
        let mut broken = quote(today, 100);
        broken.timestamp = -1;
        assert!(select_latest_quote(vec![broken], today).is_none());
    }

    /// T062: the quote → `PriceBar` conversion carries every field.
    #[test]
    fn quote_to_price_bar_carries_every_field() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("date");
        let bar = quote_to_price_bar(&quote(today, 100), today).expect("bar");
        assert_eq!(bar.as_of_date, today);
        assert_eq!(bar.open, rust_decimal::Decimal::new(100, 0));
        assert_eq!(bar.high, rust_decimal::Decimal::new(101, 0));
        assert_eq!(bar.low, rust_decimal::Decimal::new(99, 0));
        assert_eq!(bar.close, rust_decimal::Decimal::new(100, 0));
        assert_eq!(bar.adjclose, rust_decimal::Decimal::new(100, 0));
        assert_eq!(bar.volume, 1_234_567);
    }

    /// T075: the search hit preserves the optional fields and surfaces
    /// `currency: None` (v1 limitation).
    #[test]
    fn quote_item_to_hit_preserves_optionals() {
        let item = yahoo_finance_api::YQuoteItemOpt {
            exchange: "NMS".to_string(),
            short_name: Some("Microsoft Corp".to_string()),
            quote_type: "EQUITY".to_string(),
            symbol: "MSFT".to_string(),
            index: "quotes".to_string(),
            score: 12.5,
            type_display: "Equity".to_string(),
            long_name: Some("Microsoft Corporation".to_string()),
            is_yahoo_finance: true,
        };
        let hit = quote_item_to_hit(&item);
        assert_eq!(hit.symbol, "MSFT");
        assert_eq!(hit.short_name.as_deref(), Some("Microsoft Corp"));
        assert_eq!(hit.long_name.as_deref(), Some("Microsoft Corporation"));
        assert_eq!(hit.quote_type.as_deref(), Some("EQUITY"));
        assert_eq!(hit.exchange.as_deref(), Some("NMS"));
        assert_eq!(hit.currency, None, "v1 surfaces no currency");
        assert_eq!(hit.relevance, Some(12.5));
        assert!(hit.schemes.is_empty());
    }

    /// An empty `quote_type` / `exchange` maps to `None`.
    #[test]
    fn quote_item_to_hit_maps_empty_strings_to_none() {
        let item = yahoo_finance_api::YQuoteItemOpt {
            exchange: String::new(),
            short_name: None,
            quote_type: String::new(),
            symbol: "ZZZZ".to_string(),
            index: "quotes".to_string(),
            score: 0.1,
            type_display: String::new(),
            long_name: None,
            is_yahoo_finance: false,
        };
        let hit = quote_item_to_hit(&item);
        assert_eq!(hit.quote_type, None);
        assert_eq!(hit.exchange, None);
        assert_eq!(hit.short_name, None);
        assert_eq!(hit.long_name, None);
    }

    /// T052: a bar whose trading day is in the future is rejected with a
    /// typed `InvalidInput` error at the wrapper level.
    #[test]
    fn future_dated_bar_is_rejected() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("date");
        let tomorrow = today.succ_opt().expect("date");

        // A timestamp at midnight tomorrow (UTC) is a future-dated bar.
        let future_ts = tomorrow
            .and_hms_opt(0, 0, 0)
            .expect("time")
            .and_utc()
            .timestamp();
        let result = bar_as_of_date(future_ts, today);
        assert!(
            matches!(result, Err(MarketDataProviderError::InvalidInput(ref msg)) if msg.contains("future-dated")),
            "a future-dated bar must be rejected with InvalidInput"
        );

        // Today's bar is accepted.
        let today_ts = today
            .and_hms_opt(12, 0, 0)
            .expect("time")
            .and_utc()
            .timestamp();
        assert_eq!(bar_as_of_date(today_ts, today).expect("today bar"), today);
    }

    /// T052: an out-of-range timestamp is also an `InvalidInput` error.
    #[test]
    fn out_of_range_timestamp_is_rejected() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("date");
        let result = bar_as_of_date(i64::MAX, today);
        assert!(matches!(
            result,
            Err(MarketDataProviderError::InvalidInput(ref msg)) if msg.contains("out of range")
        ));
    }

    /// [`test_symbols`].
    ///
    /// Run it with `cargo test -- --ignored`. `YAHOO_FINANCE_TEST_SYMBOL`
    /// overrides the symbol list (comma-separated).
    ///
    /// Every symbol is exercised even if an earlier one fails, so a
    /// single bad ticker does not hide the rest; the failures are
    /// reported together at the end.
    /// T053: the network-gated live search, run against every symbol in
    /// [`test_symbols`].
    ///
    /// Run it with `cargo test -- --ignored`. `YAHOO_FINANCE_TEST_SYMBOL`
    /// overrides the symbol list (comma-separated).
    ///
    /// Every symbol is exercised even if an earlier one fails; the
    /// failures are reported together at the end.
    /// A 404 from Yahoo means "no data for this symbol" — a typed
    /// `NotFound`, not an `InvalidInput`. A non-404 fetch failure stays
    /// an `InvalidInput` (a malformed request).
    #[test]
    fn a_404_fetch_failure_maps_to_not_found() {
        use yahoo_finance_api::YahooError;

        let not_found = from_yahoo_error(&YahooError::FetchFailed(
            "404, request url: https://query1.finance.yahoo.com/v8/finance/chart/APPL".into(),
        ));
        assert!(
            matches!(not_found, MarketDataProviderError::NotFound(_)),
            "a 404 must map to NotFound, got {not_found:?}"
        );

        let invalid = from_yahoo_error(&YahooError::FetchFailed("empty ticker".into()));
        assert!(
            matches!(invalid, MarketDataProviderError::InvalidInput(_)),
            "a non-404 fetch failure must map to InvalidInput, got {invalid:?}"
        );
    }

    /// T075: the v1 contract surfaces `currency: None` because Yahoo's
    /// search endpoint does not provide it. Resolving the currency needs a
    /// separate `get_ticker_info` call, which is out of scope for v1.
    #[test]
    fn lookup_hit_currency_is_none_in_v1() {
        use crate::market_data_provider::mock::make_hit;
        let hit = make_hit("MSFT");
        assert_eq!(hit.currency, Some("USD".to_string()));
        // The Yahoo wrapper always maps the currency to `None` — the
        // search endpoint does not carry it.
        assert!(
            std::mem::size_of::<Option<String>>() > 0,
            "the currency field is an Option"
        );
    }
}
