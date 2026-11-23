//! Startup bootstrap: seed the primary actor and load the registry cache.
//!
//! Skips when no test database is configured, like the other database-backed
//! suites.

use obolargus_server::auth::AuthState;
use obolargus_server::auth::bootstrap;
use obolargus_server::config::AppConfig;

fn database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
        .filter(|url| !url.trim().is_empty())
}

async fn migrated_pool() -> Option<sqlx::PgPool> {
    let url = database_url()?;
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!("SKIPPED: could not connect to the test database: {error}");
            return None;
        }
    };
    if let Err(error) = sqlx::migrate!("../obolargus_core/migrations")
        .run(&pool)
        .await
    {
        eprintln!("SKIPPED: could not migrate the test database: {error}");
        return None;
    }
    Some(pool)
}

#[tokio::test]
async fn bootstrap_seeds_the_primary_actor_and_loads_the_cache() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let suffix = uuid::Uuid::new_v4();
    let primary_email = format!("primary-{suffix}@example.com");
    let other_email = format!("other-{suffix}@example.com");
    let config = AppConfig {
        auth_enabled: true,
        google_client_id: "test-client.apps.googleusercontent.com".to_string(),
        google_allowed_emails: format!("{primary_email},{other_email}"),
        primary_owner_email: primary_email.clone(),
        jwt_secret: "test-secret".to_string(),
        ..AppConfig::default()
    };
    let auth = AuthState::from_config(&config, Some(pool.clone())).expect("auth state");

    bootstrap::run(&pool, &auth).await.expect("bootstrap");
    let primary = auth.primary_actor().expect("primary actor cached");
    assert_eq!(primary.email, primary_email);
    assert!(primary.google_sub.is_none(), "seeded actor must be unbound");
    assert_eq!(auth.actors.len(), 1);

    // A restart reruns the same seed without creating a duplicate.
    bootstrap::run(&pool, &auth)
        .await
        .expect("idempotent bootstrap");
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM actors WHERE lower(email) = lower($1)")
            .bind(&primary_email)
            .fetch_one(&pool)
            .await
            .expect("actor count");
    assert_eq!(count, 1);
    assert_eq!(auth.primary_actor().map(|actor| actor.id), Some(primary.id));

    sqlx::query("DELETE FROM actors WHERE lower(email) = lower($1)")
        .bind(&primary_email)
        .execute(&pool)
        .await
        .expect("cleanup");
}
