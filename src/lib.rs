//! obolargus-server: Axum REST API backend.
//!
//! Skeleton crate for the Obolargus backend. Business endpoints and scheduled
//! tasks are specified in `specs/architecture/architecture.md`; this crate
//! currently provides the typed configuration layer, the health endpoint, and
//! the router wiring.

#![forbid(unsafe_code)]

pub mod config;
pub mod routes;

use std::net::SocketAddr;

use axum::Router;
use axum::http::header::HeaderValue;
use sqlx::PgPool;

use config::AppConfig;

/// Shared application state handed to handlers.
#[derive(Clone)]
pub struct AppState {
    /// Loaded environment configuration.
    pub config: AppConfig,
    /// Optional PostgreSQL pool, created at startup when `DATABASE_URL` is set.
    pub pool: Option<PgPool>,
}

/// Builds the API [`Router`] with tracing and CORS middleware.
pub fn router(state: AppState) -> Router {
    use axum::routing::get;
    use tower_http::cors::{AllowOrigin, CorsLayer};
    use tower_http::trace::TraceLayer;

    Router::new()
        .route("/health", get(routes::health::health))
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
            Ok(pool) => Some(pool),
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

/// Binds `APP_HOST:APP_PORT` and serves the API until shutdown.
pub async fn serve(config: AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pool = connect_database(config.database_url()).await;
    let app = router(AppState {
        config: config.clone(),
        pool,
    });

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "obolargus-server listening");

    axum::serve(listener, app).await?;
    Ok(())
}
