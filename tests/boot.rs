//! End-to-end and resilience tests for the server lifecycle (spec US4).

use std::time::{Duration, Instant};

use obolargus_server::config::AppConfig;
use obolargus_server::{connect_database, serve};

fn config(port: u16) -> AppConfig {
    AppConfig {
        host: "127.0.0.1".to_string(),
        port,
        rust_log: "info".to_string(),
        database_url: String::new(),
        jwt_secret: String::new(),
        jwt_access_ttl_minutes: 15,
        jwt_refresh_ttl_days: 7,
        cors_allowed_origins: "http://127.0.0.1:8081".to_string(),
        auth_enabled: false,
    }
}

#[tokio::test]
async fn serve_binds_and_answers_health() {
    let port = 18401;
    let task = tokio::spawn(serve(config(port)));
    let url = format!("http://127.0.0.1:{port}/health");
    let deadline = Instant::now() + Duration::from_secs(10);

    let mut healthy = false;
    while Instant::now() < deadline {
        match reqwest::get(&url).await {
            Ok(response) if response.status().is_success() => {
                healthy = true;
                break;
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    task.abort();
    let _ = task.await;

    assert!(healthy, "server did not serve /health within 10s");
}

#[tokio::test]
async fn connect_database_without_url_returns_none() {
    assert!(connect_database(None).await.is_none());
}

#[tokio::test]
async fn connect_database_with_unreachable_url_degrades_to_none() {
    let unreachable = "postgres://obolargus:invalid@127.0.0.1:59999/nope";
    assert!(connect_database(Some(unreachable)).await.is_none());
}
