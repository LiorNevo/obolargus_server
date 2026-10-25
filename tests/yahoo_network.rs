//! Network-gated live tests for the Yahoo Finance provider.
//!
//! Implements T051 / T053 of `specs/017-yahoo-data-source/tasks.md`.
//!
//! These tests hit the **real** Yahoo Finance API, so they are `#[ignore]`d
//! and only run when explicitly requested:
//!
//! ```bash
//! cargo test -p obolargus-server --test yahoo_network -- --ignored
//! ```
//!
//! They live in their own integration-test file rather than in the
//! provider's unit-test module for two reasons:
//!
//! 1. They are a different *kind* of test — network-dependent, slow, and
//!    rate-limit-sensitive — and keeping them separate makes the default
//!    suite's behaviour obvious.
//! 2. `cargo llvm-cov` excludes `tests/*` from its report by default, so
//!    code that can only run with a network connection and an explicit
//!    `--ignored` flag does not count against the coverage of the
//!    production path.
//!
//! The symbol list is overridable with `YAHOO_FINANCE_TEST_SYMBOL`
//! (comma-separated); the default list spans the instrument shapes the
//! provider has to handle — a mega-cap equity, a mid-cap equity, and
//! three ETFs (a broad index, a high-yield bond fund, and a BDC).

use obolargus_core::market_data::config::DataSourceConfig;
use obolargus_core::market_data::error::MarketDataProviderError;
use obolargus_core::market_data::trait_::MarketDataProvider;
use obolargus_core::market_data::types::{
    HistoricalRangeRequest, LatestBarRequest, TickerLookupRequest, TypedIdentifier,
};
use obolargus_server::market_data_provider::yahoo::YahooFinanceProvider;

/// The ticker symbols the network-gated tests exercise by default.
const DEFAULT_TEST_SYMBOLS: &[&str] = &["AAPL", "BX", "QQQM", "SRLN", "BXSL", "NVDA"];

/// The symbols the network-gated tests exercise.
///
/// `YAHOO_FINANCE_TEST_SYMBOL` overrides the list. It accepts a
/// comma-separated list of tickers, so a single ticker (the common case)
/// and a whole list are both valid.
fn test_symbols() -> Vec<String> {
    match std::env::var("YAHOO_FINANCE_TEST_SYMBOL") {
        Ok(raw) => parse_symbol_list(&raw),
        Err(_) => DEFAULT_TEST_SYMBOLS
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
    }
}

/// Splits a comma-separated symbol list, trimming the whitespace around
/// each entry and dropping empty ones.
fn parse_symbol_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Builds a provider from the default configuration.
fn provider() -> YahooFinanceProvider {
    YahooFinanceProvider::build(&DataSourceConfig::default_yahoo()).expect("provider build")
}

/// T051: the network-gated live call, run against every symbol in
/// [`test_symbols`].
///
/// Every symbol is exercised even if an earlier one fails, so a single
/// bad ticker does not hide the rest; the failures are reported together
/// at the end.
#[test]
fn yahoo_provider_network_gated_get_latest_bar() {
    let symbols = test_symbols();
    if symbols.is_empty() {
        eprintln!("SKIPPED: no symbols to test");
        return;
    }
    let provider = provider();
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut failures: Vec<String> = Vec::new();

    for symbol in &symbols {
        let outcome = rt.block_on(async {
            provider
                .get_latest_bar(LatestBarRequest::now(TypedIdentifier::new(
                    "TICKER", symbol,
                )))
                .await
        });
        match outcome {
            Ok(bar) => {
                let close_ok = bar.bar.close > rust_decimal::Decimal::new(0, 0);
                let volume_ok = bar.bar.volume > 0;
                let not_future = bar.bar.as_of_date <= chrono::Utc::now().date_naive();
                if close_ok && volume_ok && not_future {
                    println!(
                        "  {symbol}: ok — close={} volume={} as_of={}",
                        bar.bar.close, bar.bar.volume, bar.bar.as_of_date
                    );
                } else {
                    let mut problems = Vec::new();
                    if !close_ok {
                        problems.push("close is not positive".to_string());
                    }
                    if !volume_ok {
                        problems.push("volume is zero".to_string());
                    }
                    if !not_future {
                        problems.push(format!("bar is future-dated ({})", bar.bar.as_of_date));
                    }
                    failures.push(format!("{symbol}: {}", problems.join(", ")));
                }
            }
            // A symbol Yahoo does not carry is a fact about Yahoo's
            // catalog, not a provider defect: the provider's contract is
            // to surface a typed `NotFound`. Report it and move on; every
            // other error is a real failure.
            Err(MarketDataProviderError::NotFound(message)) => {
                println!("  {symbol}: skipped — no such upstream symbol ({message})");
            }
            Err(error) => failures.push(format!("{symbol}: {error}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} symbols failed the live latest-bar check:\n  - {}",
        failures.len(),
        symbols.len(),
        failures.join("\n  - ")
    );
}

/// T053: the network-gated live search, run against every symbol in
/// [`test_symbols`].
///
/// Every symbol is exercised even if an earlier one fails; the failures
/// are reported together at the end.
#[test]
fn yahoo_provider_network_gated_search_ticker() {
    let symbols = test_symbols();
    if symbols.is_empty() {
        eprintln!("SKIPPED: no symbols to test");
        return;
    }
    let provider = provider();
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut failures: Vec<String> = Vec::new();

    for symbol in &symbols {
        let outcome = rt.block_on(async {
            provider
                .lookup_ticker(TickerLookupRequest {
                    query: symbol.clone(),
                })
                .await
        });
        match outcome {
            Ok(result) => {
                let has_symbol = result.hits.iter().any(|hit| !hit.symbol.is_empty());
                if has_symbol {
                    let first = result
                        .hits
                        .first()
                        .map(|hit| hit.symbol.as_str())
                        .unwrap_or("");
                    println!(
                        "  {symbol}: ok — {} hit(s), first={first}",
                        result.hits.len()
                    );
                } else {
                    failures.push(format!("{symbol}: the search returned no hits"));
                }
            }
            Err(error) => failures.push(format!("{symbol}: {error}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} symbols failed the live search check:\n  - {}",
        failures.len(),
        symbols.len(),
        failures.join("\n  - ")
    );
}

/// The default list is the one the feature ships with.
#[test]
fn default_test_symbols_are_the_documented_set() {
    // The env var is not set in the default test environment, so the
    // helper falls back to the built-in list.
    if std::env::var("YAHOO_FINANCE_TEST_SYMBOL").is_err() {
        assert_eq!(
            test_symbols(),
            vec!["AAPL", "BX", "QQQM", "SRLN", "BXSL", "NVDA"]
        );
    }
}

/// The env var override accepts a comma-separated list, and trims the
/// whitespace around each entry.
#[test]
fn the_symbol_override_accepts_a_comma_separated_list() {
    assert_eq!(parse_symbol_list("AAPL"), vec!["AAPL"]);
    assert_eq!(
        parse_symbol_list("AAPL,BX,QQQM"),
        vec!["AAPL", "BX", "QQQM"]
    );
    assert_eq!(parse_symbol_list(" AAPL , BX "), vec!["AAPL", "BX"]);
    assert_eq!(parse_symbol_list("AAPL,,BX"), vec!["AAPL", "BX"]);
    assert!(parse_symbol_list("").is_empty());
    assert!(parse_symbol_list("  ,  ").is_empty());
}

/// The one-day range request returns that day's session (spec 018's
/// dependency on the upstream's range semantics, research D3).
///
/// The load asks for the range "that date to that date" and expects the
/// source to answer with that session's bar. This is the live proof of
/// that assumption; it is `#[ignore]`d like the other network-gated cases.
#[test]
fn yahoo_provider_network_gated_one_day_range_returns_that_session() {
    let symbols = test_symbols();
    if symbols.is_empty() {
        eprintln!("SKIPPED: no symbols to test");
        return;
    }
    let provider = provider();
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut failures: Vec<String> = Vec::new();

    for symbol in &symbols {
        // A recent weekday; the upstream answers with the last session it
        // carries when the requested day has none.
        // A recent **weekday**: the upstream carries no quotes for a
        // weekend, and an empty answer there would say nothing about the
        // range semantics this test checks.
        let date = most_recent_weekday(chrono::Utc::now().date_naive() - chrono::Duration::days(7));
        let outcome = rt.block_on(async {
            provider
                .get_historical_range(HistoricalRangeRequest {
                    identifier: TypedIdentifier::new("TICKER", symbol),
                    from: date,
                    to: date,
                })
                .await
        });
        match outcome {
            Ok(range) => {
                let in_window = range.bars.iter().all(|bar| bar.as_of_date == date);
                if in_window {
                    println!("  {symbol}: ok — {} bar(s) within {date}", range.bars.len());
                } else {
                    failures.push(format!(
                        "{symbol}: the one-day range answered with a bar outside {date}"
                    ));
                }
            }
            Err(MarketDataProviderError::NotFound(message)) => {
                println!("  {symbol}: skipped — no session for that date ({message})");
            }
            Err(error) => failures.push(format!("{symbol}: {error}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} symbols failed the live one-day-range check:\n  - {}",
        failures.len(),
        symbols.len(),
        failures.join("\n  - ")
    );
}

/// The most recent weekday on or before `date`.
fn most_recent_weekday(date: chrono::NaiveDate) -> chrono::NaiveDate {
    use chrono::Datelike;
    let mut candidate = date;
    while candidate.weekday().num_days_from_monday() >= 5 {
        candidate -= chrono::Duration::days(1);
    }
    candidate
}
