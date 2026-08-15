//! obolargus-server entry point.
//!
//! Loads environment configuration (fail-fast) and delegates to
//! [`obolargus_server::serve`].

#![forbid(unsafe_code)]

use dotenvy::dotenv;
use obolargus_server::config::AppConfig;
use obolargus_server::serve;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv().ok();
    let config = AppConfig::from_env().map_err(|error| format!("config error: {error}"))?;

    tracing_subscriber::fmt()
        .with_env_filter(config.rust_log.clone())
        .init();

    serve(config).await
}
