//! Provider implementations for the source-agnostic market data interface.
//!
//! Implements FR-010 / US3 of `specs/017-yahoo-data-source/spec.md`. The trait
//! itself lives in `obolargus_core::market_data::trait`; this module hosts the
//! concrete providers (Yahoo Finance in v1) and the dispatcher that turns a
//! `DataSourceConfig` into a built `Box<dyn MarketDataProvider>`.
//!
//! The mock provider in `mock` is `#[cfg(test)]`-only and never ships in release
//! builds. The Yahoo wrapper in `yahoo` is the v1 first concrete provider.

#![forbid(unsafe_code)]

#[cfg(test)]
pub mod mock;
pub mod yahoo;

use std::sync::Arc;

use obolargus_core::market_data::config::DataSourceConfig;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::trait_::MarketDataProvider;

/// Builds a concrete provider from configuration.
///
/// Dispatches on `config.provider_type`. Returns
/// [`MarketDataProviderError::Unsupported`] for unrecognized types.
///
/// Implements FR-001 / FR-010 of spec 017.
pub fn build_provider_from_config(
    config: &DataSourceConfig,
) -> Result<Arc<dyn MarketDataProvider + Send + Sync>, MarketDataProviderError> {
    match config.provider_type.as_str() {
        "yahoo_finance" => yahoo::YahooFinanceProvider::build(config)
            .map(|p| Arc::new(p) as Arc<dyn MarketDataProvider + Send + Sync>),
        other => Err(MarketDataProviderError::Unsupported(format!(
            "unknown provider_type '{other}'"
        ))),
    }
}

/// Builds a provider and reports whether a credential was present.
///
/// The boolean is what the `data_sources.api_key_present` column records;
/// the credential value never leaves the provider (FR-040).
pub fn build_provider_with_key_presence(
    config: &DataSourceConfig,
) -> Result<(Arc<dyn MarketDataProvider + Send + Sync>, bool), MarketDataProviderError> {
    match config.provider_type.as_str() {
        "yahoo_finance" => {
            let provider = yahoo::YahooFinanceProvider::build(config)?;
            let present = provider.api_key_present();
            Ok((
                Arc::new(provider) as Arc<dyn MarketDataProvider + Send + Sync>,
                present,
            ))
        }
        other => Err(MarketDataProviderError::Unsupported(format!(
            "unknown provider_type '{other}'"
        ))),
    }
}
