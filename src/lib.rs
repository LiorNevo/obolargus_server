//! obolargus-server: Axum REST API backend.
//!
//! Skeleton crate for the Obolargus backend. Business endpoints and scheduled
//! tasks are specified in `specs/architecture/architecture.md`; this crate
//! currently provides the typed configuration layer, the health endpoint, and
//! the router wiring. As of spec 017 it also hosts the Yahoo Finance provider
//! and the source-agnostic market-data API.

#![forbid(unsafe_code)]

pub mod config;
pub mod market_data_provider;
pub mod routes;
pub mod tasks;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::http::header::HeaderValue;
use sqlx::PgPool;
use tokio::sync::watch;

use config::AppConfig;

/// Shared application state handed to handlers.
#[derive(Clone)]
pub struct AppState {
    /// Loaded environment configuration.
    pub config: AppConfig,
    /// Optional PostgreSQL pool, created at startup when `DATABASE_URL` is set.
    pub pool: Option<PgPool>,
    /// Source-agnostic market data registry. Always present (built from the
    /// default Yahoo config when `MARKET_DATA_SOURCES` is unset).
    pub market_data: Arc<obolargus_core::market_data::registry::DataSourceRegistry>,
}

impl AppState {
    /// Builds the application state, constructing the market-data registry
    /// from `config`.
    ///
    /// Fails fast if the configured sources are invalid or a provider
    /// cannot be constructed (FR-002).
    pub fn new(
        config: AppConfig,
        pool: Option<PgPool>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let market_data = build_market_data_registry(&config)?;
        Ok(Self {
            config,
            pool,
            market_data,
        })
    }

    /// Builds the application state and persists the configured sources
    /// into the `data_sources` table.
    ///
    /// The persisted rows record the configuration shape only — never the
    /// credential value (FR-040).
    pub async fn new_with_persistence(
        config: AppConfig,
        pool: Option<PgPool>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let market_data_config = config
            .market_data_config()
            .map_err(|e| format!("market data config: {e}"))?;
        let market_data =
            tasks::build_registry_and_persist(&market_data_config, pool.as_ref()).await?;
        Ok(Self {
            config,
            pool,
            market_data,
        })
    }
}

/// Builds the API [`Router`] with tracing and CORS middleware.
pub fn router(state: AppState) -> Router {
    use axum::routing::{get, post};
    use tower_http::cors::{AllowOrigin, CorsLayer};
    use tower_http::trace::TraceLayer;

    Router::new()
        .route("/health", get(routes::health::health))
        .route(
            "/api/market-data/sources",
            get(routes::market_data::list_sources),
        )
        .route(
            "/api/market-data/refresh",
            post(routes::market_data::refresh),
        )
        .route("/api/market-data/lookup", get(routes::market_data::lookup))
        .route(
            "/api/market-data/reconcile",
            post(routes::market_data::reconcile),
        )
        .route("/api/market-data/load", post(routes::market_data::load))
        .route(
            "/api/market-data/load-range",
            post(routes::market_data::load_range),
        )
        .layer(CorsLayer::new().allow_origin(AllowOrigin::list(cors_origins(&state.config))))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn cors_origins(config: &AppConfig) -> Vec<HeaderValue> {
    config
        .cors_origins()
        .into_iter()
        .filter_map(|origin| HeaderValue::from_str(&origin).ok())
        .collect()
}

/// Connects to PostgreSQL when a URL is configured.
///
/// A failed connection is logged and degrades to `None` so the service still
/// boots without the database (see `contracts/health-check.md`).
pub async fn connect_database(url: Option<&str>) -> Option<PgPool> {
    match url {
        Some(url) => match sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(url)
            .await
        {
            Ok(pool) => {
                // Apply any pending migrations before the first request can
                // reach a missing table. `sqlx` tracks applied versions in
                // `_sqlx_migrations`, so an up-to-date database is untouched
                // and only the pending, additive migrations run.
                match obolargus_core::bin_helpers::run_migrations(&pool).await {
                    Ok(()) => Some(pool),
                    Err(error) => {
                        tracing::error!(
                            "database migration failed at startup: {error}; running without database"
                        );
                        None
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    "database connection failed at startup: {error}; continuing without database"
                );
                None
            }
        },
        None => {
            tracing::info!("DATABASE_URL not set; running without database-backed features");
            None
        }
    }
}

/// Builds the market-data registry from configuration. Returns a boxed
/// error if any source fails to construct (fail-fast at startup per FR-002).
pub fn build_market_data_registry(
    config: &AppConfig,
) -> Result<
    Arc<obolargus_core::market_data::registry::DataSourceRegistry>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let market_data = config
        .market_data_config()
        .map_err(|e| format!("market data config: {e}"))?;
    tasks::build_registry_from_config(&market_data)
        .map_err(|e| format!("registry build: {e}").into())
}

/// Binds `APP_HOST:APP_PORT` and serves the API until shutdown.
pub async fn serve(config: AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pool = connect_database(config.database_url()).await;

    let state = AppState::new_with_persistence(config.clone(), pool).await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    if config.run_scheduled_tasks {
        let task_state = state.clone();
        let task_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(error) = tasks::run(task_state, task_shutdown).await {
                tracing::error!(%error, "scheduler exited with error");
            }
        });
    }

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "obolargus-server listening");

    let server = axum::serve(listener, router(state)).with_graceful_shutdown(async move {
        // Honor an external SIGTERM / Ctrl-C by flipping the watch.
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });

    server.await?;
    Ok(())
}
