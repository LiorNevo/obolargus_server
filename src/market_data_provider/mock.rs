//! In-memory mock provider for tests.
//!
//! Implements the `MarketDataProvider` trait without any network access so
//! the registry's failover, priority ordering, and error-mapping behavior
//! can be exercised deterministically (contract tests #1..#4 in
//! `specs/017-yahoo-data-source/contracts/rust-api-contract.md`).

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{NaiveDate, Utc};
use rust_decimal::Decimal;

use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::trait_::MarketDataProvider;
use obolargus_core::market_data::types::{
    HistoricalRange, HistoricalRangeRequest, LatestBar, LatestBarRequest, PriceBar,
    TickerLookupHit, TickerLookupRequest, TickerLookupResult,
};

/// A provider whose responses are pre-programmed per
/// `"{scheme}:{value}"` key.
///
/// Implements the trait contract of FR-001 of spec 017; used to
/// exercise the registry's failover behavior offline.
pub struct MockMarketDataProvider {
    /// Stable source id.
    source_id: String,
    /// Human-readable label.
    display_name: String,
    /// Identifier schemes this provider claims to support.
    supported_schemes: Vec<String>,
    /// Numeric priority (lower = tried first).
    priority: i32,
    /// Whether the provider is enabled.
    enabled: bool,
    /// Pre-programmed `get_latest_bar` responses.
    latest: HashMap<String, Result<LatestBar, MarketDataProviderError>>,
    /// Pre-programmed `lookup_ticker` responses.
    lookups: HashMap<String, Result<TickerLookupResult, MarketDataProviderError>>,
}

impl MockMarketDataProvider {
    /// Constructs a mock provider with the given identity.
    ///
    /// Implements FR-001 of spec 017.
    pub fn new(source_id: &str, schemes: &[&str], priority: i32) -> Self {
        Self {
            source_id: source_id.to_string(),
            display_name: source_id.to_string(),
            supported_schemes: schemes.iter().map(|s| s.to_string()).collect(),
            priority,
            enabled: true,
            latest: HashMap::new(),
            lookups: HashMap::new(),
        }
    }

    /// Sets the response for a `get_latest_bar` call on `scheme:value`.
    ///
    /// Implements FR-001 / FR-005 of spec 017.
    pub fn with_latest(
        mut self,
        scheme: &str,
        value: &str,
        response: Result<LatestBar, MarketDataProviderError>,
    ) -> Self {
        self.latest.insert(format!("{scheme}:{value}"), response);
        self
    }

    /// Sets the response for a `lookup_ticker` call on `query`.
    ///
    /// Implements FR-012 of spec 017 (US2).
    pub fn with_lookup(
        mut self,
        query: &str,
        response: Result<TickerLookupResult, MarketDataProviderError>,
    ) -> Self {
        self.lookups.insert(query.to_string(), response);
        self
    }

    /// Sets the enabled flag.
    ///
    /// Implements FR-004 of spec 017.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// Builds a `LatestBar` fixture for the given source and trading day.
///
/// Implements FR-013 of spec 017.
pub fn make_bar(source_id: &str, as_of_date: NaiveDate, close: i64) -> LatestBar {
    LatestBar {
        bar: PriceBar {
            as_of_date,
            open: Decimal::new(close, 0),
            high: Decimal::new(close + 1, 0),
            low: Decimal::new(close - 1, 0),
            close: Decimal::new(close, 0),
            adjclose: Decimal::new(close, 0),
            volume: 1_000_000,
        },
        knowledge_date: Utc::now().date_naive(),
        source_id: source_id.to_string(),
    }
}

/// Builds a `TickerLookupHit` fixture.
///
/// Implements FR-012 of spec 017 (US2).
pub fn make_hit(symbol: &str) -> TickerLookupHit {
    TickerLookupHit {
        symbol: symbol.to_string(),
        short_name: Some(format!("{symbol} Inc.")),
        long_name: Some(format!("{symbol} Incorporated")),
        quote_type: Some("EQUITY".to_string()),
        exchange: Some("NMS".to_string()),
        currency: Some("USD".to_string()),
        schemes: vec![],
        relevance: Some(1.0),
    }
}

#[async_trait]
impl MarketDataProvider for MockMarketDataProvider {
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
        self.enabled
    }

    fn priority(&self) -> i32 {
        self.priority
    }

    async fn get_latest_bar(
        &self,
        request: LatestBarRequest,
    ) -> Result<LatestBar, MarketDataProviderError> {
        let key = format!("{}:{}", request.identifier.scheme, request.identifier.value);
        match self.latest.get(&key) {
            Some(Ok(bar)) => Ok(bar.clone()),
            Some(Err(e)) => Err(e.clone()),
            None => Err(MarketDataProviderError::NotFound(format!(
                "mock provider '{}' has no response for {key}",
                self.source_id
            ))),
        }
    }

    async fn get_historical_range(
        &self,
        _request: HistoricalRangeRequest,
    ) -> Result<HistoricalRange, MarketDataProviderError> {
        Err(MarketDataProviderError::Unsupported(
            "mock provider does not implement get_historical_range".into(),
        ))
    }

    async fn lookup_ticker(
        &self,
        request: TickerLookupRequest,
    ) -> Result<TickerLookupResult, MarketDataProviderError> {
        match self.lookups.get(&request.query) {
            Some(Ok(result)) => Ok(result.clone()),
            Some(Err(e)) => Err(e.clone()),
            None => Err(MarketDataProviderError::NotFound(format!(
                "mock provider '{}' has no lookup response for '{}'",
                self.source_id, request.query
            ))),
        }
    }
}

/// Thread-safe counter used by tests to assert how many calls a provider
/// received.
///
/// Implements the FR-007 per-source attempt accounting of spec 017.
#[derive(Debug, Default)]
pub struct CallCounter {
    /// Number of calls recorded.
    pub count: Mutex<usize>,
}

impl CallCounter {
    /// Increments and returns the new count.
    ///
    /// Implements FR-007 of spec 017.
    pub fn increment(&self) -> usize {
        let mut guard = self.count.lock().unwrap_or_else(|e| e.into_inner());
        *guard += 1;
        *guard
    }

    /// Reads the current count.
    ///
    /// Implements FR-007 of spec 017.
    pub fn get(&self) -> usize {
        *self.count.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn request(scheme: &str, value: &str) -> LatestBarRequest {
        LatestBarRequest {
            identifier: obolargus_core::market_data::types::TypedIdentifier::new(scheme, value),
            as_of_now: Utc::now(),
        }
    }

    /// The mock returns the pre-programmed answer for a known key.
    #[tokio::test]
    async fn returns_the_configured_latest_bar() {
        let day = NaiveDate::from_ymd_opt(2026, 1, 2).expect("date");
        let provider = MockMarketDataProvider::new("mock", &["TICKER"], 10).with_latest(
            "TICKER",
            "AAPL",
            Ok(make_bar("mock", day, 100)),
        );
        let bar = provider
            .get_latest_bar(request("TICKER", "AAPL"))
            .await
            .expect("bar");
        assert_eq!(bar.source_id, "mock");
        assert_eq!(bar.bar.close, Decimal::new(100, 0));
    }

    /// An unknown key is a typed `NotFound` (the default response).
    #[tokio::test]
    async fn unknown_key_is_not_found() {
        let provider = MockMarketDataProvider::new("mock", &["TICKER"], 10);
        let result = provider.get_latest_bar(request("TICKER", "AAPL")).await;
        assert!(matches!(result, Err(MarketDataProviderError::NotFound(_))));
    }

    /// A pre-programmed error is propagated verbatim.
    #[tokio::test]
    async fn configured_error_is_propagated() {
        let provider = MockMarketDataProvider::new("mock", &["TICKER"], 10).with_latest(
            "TICKER",
            "AAPL",
            Err(MarketDataProviderError::Transient("boom".into())),
        );
        let result = provider.get_latest_bar(request("TICKER", "AAPL")).await;
        assert!(matches!(
            result,
            Err(MarketDataProviderError::Transient(ref msg)) if msg == "boom"
        ));
    }

    /// The historical-range method is not implemented by the mock.
    #[tokio::test]
    async fn historical_range_is_unsupported() {
        let provider = MockMarketDataProvider::new("mock", &["TICKER"], 10);
        let result = provider
            .get_historical_range(HistoricalRangeRequest {
                identifier: obolargus_core::market_data::types::TypedIdentifier::new(
                    "TICKER", "AAPL",
                ),
                from: NaiveDate::from_ymd_opt(2026, 1, 1).expect("date"),
                to: NaiveDate::from_ymd_opt(2026, 1, 2).expect("date"),
            })
            .await;
        assert!(matches!(
            result,
            Err(MarketDataProviderError::Unsupported(_))
        ));
    }

    /// The lookup returns the pre-programmed hits.
    #[tokio::test]
    async fn lookup_returns_the_configured_hits() {
        let provider = MockMarketDataProvider::new("mock", &["TICKER"], 10).with_lookup(
            "MSFT",
            Ok(TickerLookupResult {
                hits: vec![make_hit("MSFT")],
                source_id: "mock".to_string(),
            }),
        );
        let result = provider
            .lookup_ticker(TickerLookupRequest {
                query: "MSFT".to_string(),
            })
            .await
            .expect("lookup");
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].symbol, "MSFT");
    }

    /// An unknown query is a typed `NotFound`.
    #[tokio::test]
    async fn unknown_lookup_is_not_found() {
        let provider = MockMarketDataProvider::new("mock", &["TICKER"], 10);
        let result = provider
            .lookup_ticker(TickerLookupRequest {
                query: "ZZZZZ".to_string(),
            })
            .await;
        assert!(matches!(result, Err(MarketDataProviderError::NotFound(_))));
    }

    /// The metadata accessors reflect the constructor arguments.
    #[test]
    fn metadata_reflects_the_constructor() {
        let provider = MockMarketDataProvider::new("mock", &["TICKER", "ISIN"], 42);
        assert_eq!(provider.source_id(), "mock");
        assert_eq!(provider.display_name(), "mock");
        assert_eq!(provider.priority(), 42);
        assert!(provider.enabled());
        assert_eq!(provider.supported_schemes().len(), 2);
    }

    /// The `enabled` flag can be turned off.
    #[test]
    fn enabled_flag_can_be_toggled() {
        let provider = MockMarketDataProvider::new("mock", &["TICKER"], 10).with_enabled(false);
        assert!(!provider.enabled());
    }

    /// The call counter tracks increments.
    #[test]
    fn call_counter_counts() {
        let counter = CallCounter::default();
        assert_eq!(counter.get(), 0);
        assert_eq!(counter.increment(), 1);
        assert_eq!(counter.increment(), 2);
        assert_eq!(counter.get(), 2);
    }
}
