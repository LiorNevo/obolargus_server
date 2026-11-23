//! Asset-search index and p95 release-gate tests (FR-032).
//!
//! Measurement method for the ignored test:
//!   * seed 100,000 assets and one identifier each, then run `ANALYZE`;
//!   * run the domain search (`search_asset_options`) 40 times after three
//!     warm-up calls and take the p95 wall-clock latency;
//!   * assert p95 <= 200 ms and assert the `EXPLAIN (ANALYZE, BUFFERS)` plan
//!     uses one of the trigram indexes created by migration 0015.
//!
//! Run the release gate with:
//!   cargo test -p obolargus-server --release --test asset_search_p95 \
//!     -- --ignored --nocapture

use std::time::{Duration, Instant};

use obolargus_core::bin_helpers::run_migrations;
use obolargus_core::domain::asset::{AssetSearchQuery, search_asset_options};
use sqlx::PgPool;

// Mirrors the non-empty branch of `search_asset_options` in
// `obolargus_core::domain::asset::read`.
const EXPLAIN_SQL: &str = r#"
    EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
    WITH matched AS (
        SELECT a.id, a.display_name, a.asset_type, a.currency
          FROM assets a
         WHERE a.display_name ILIKE $2 ESCAPE '\'
        UNION
        SELECT a.id, a.display_name, a.asset_type, a.currency
          FROM asset_identifiers ai
          JOIN assets a ON a.id = ai.asset_id
         WHERE ai.value ILIKE $2 ESCAPE '\'
    )
    SELECT m.id, m.display_name, m.asset_type, m.currency,
           (SELECT ai.value
              FROM asset_identifiers ai
             WHERE ai.asset_id = m.id
             ORDER BY (upper(ai.scheme) = 'TICKER') DESC, ai.created_at ASC, ai.id ASC
             LIMIT 1) AS primary_identifier
      FROM matched m
     ORDER BY
       CASE
         WHEN EXISTS (
           SELECT 1 FROM asset_identifiers ai3
            WHERE ai3.asset_id = m.id AND upper(ai3.value) = upper($1)
         ) THEN 0
         WHEN EXISTS (
           SELECT 1 FROM asset_identifiers ai4
            WHERE ai4.asset_id = m.id AND ai4.value ILIKE $3 ESCAPE '\'
         ) THEN 1
         WHEN m.display_name ILIKE $3 ESCAPE '\' THEN 2
         WHEN m.display_name ILIKE $2 ESCAPE '\' THEN 3
         ELSE 4
       END,
       m.display_name ASC,
       m.id ASC
     LIMIT $4
"#;

async fn migrated_pool() -> Option<PgPool> {
    let url = std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .ok()?;
    run_migrations(&pool).await.ok()?;
    Some(pool)
}

#[tokio::test]
async fn asset_search_indexes_exist() {
    let Some(pool) = migrated_pool().await else {
        return;
    };

    let names = [
        "idx_assets_display_name_trgm",
        "idx_asset_identifiers_value_trgm",
    ];
    let found: Vec<String> = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes \
         WHERE schemaname = current_schema() AND indexname = ANY($1)",
    )
    .bind(&names[..])
    .fetch_all(&pool)
    .await
    .expect("index catalog");
    assert_eq!(
        found.len(),
        names.len(),
        "migration 0015 must create both trigram indexes; found {found:?}"
    );

    let extension: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm')")
            .fetch_one(&pool)
            .await
            .expect("extension catalog");
    assert!(extension, "pg_trgm must be installed");
}

async fn cleanup_benchmark_rows(pool: &PgPool) {
    sqlx::query("DELETE FROM asset_identifiers WHERE value LIKE 'BENCH%'")
        .execute(pool)
        .await
        .expect("cleanup identifiers");
    sqlx::query("DELETE FROM assets WHERE display_name LIKE 'Bench Asset %'")
        .execute(pool)
        .await
        .expect("cleanup assets");
}

#[ignore = "release gate: seeds 100k assets and identifiers"]
#[tokio::test]
async fn asset_search_p95_at_100k_identifiers() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    cleanup_benchmark_rows(&pool).await;

    // Seed in two statements so this test never holds `assets` while waiting
    // on `asset_identifiers`; that lock order can deadlock with the shared
    // test fixture's TRUNCATE when another suite runs concurrently.
    sqlx::query(
        "INSERT INTO assets (id, asset_type, display_name, currency) \
         SELECT gen_random_uuid(), 'EQUITY', \
                'Bench Asset ' || lpad(g::text, 6, '0'), 'USD' \
         FROM generate_series(1, 100000) AS g",
    )
    .execute(&pool)
    .await
    .expect("seed assets");
    sqlx::query(
        "INSERT INTO asset_identifiers (id, asset_id, scheme, value) \
         SELECT gen_random_uuid(), a.id, 'TICKER', \
                'BENCH' || right(a.display_name, 6) \
         FROM assets a WHERE a.display_name LIKE 'Bench Asset %'",
    )
    .execute(&pool)
    .await
    .expect("seed identifiers");
    sqlx::query("ANALYZE assets")
        .execute(&pool)
        .await
        .expect("analyze");
    sqlx::query("ANALYZE asset_identifiers")
        .execute(&pool)
        .await
        .expect("analyze");

    let contains = "%BENCH099999%".to_string();
    let prefix = "BENCH099999%".to_string();
    let plan: Vec<String> = sqlx::query_scalar(EXPLAIN_SQL)
        .bind("BENCH099999")
        .bind(&contains)
        .bind(&prefix)
        .bind(10_i64)
        .fetch_all(&pool)
        .await
        .expect("explain");
    let plan_text = plan.join("\n");
    println!("EXPLAIN plan:\n{plan_text}");
    assert!(
        plan_text.contains("idx_assets_display_name_trgm")
            || plan_text.contains("idx_asset_identifiers_value_trgm"),
        "the search plan must use a trigram index:\n{plan_text}"
    );

    let query = AssetSearchQuery {
        q: Some("BENCH099999".to_string()),
        limit: 10,
    };
    for _ in 0..3 {
        let warmup = search_asset_options(&pool, query.clone())
            .await
            .expect("warmup search");
        assert_eq!(warmup.len(), 1, "the benchmark query matches one asset");
    }

    let mut durations = Vec::with_capacity(40);
    for _ in 0..40 {
        let start = Instant::now();
        let rows = search_asset_options(&pool, query.clone())
            .await
            .expect("measured search");
        durations.push(start.elapsed());
        assert_eq!(rows.len(), 1);
    }
    durations.sort_unstable();
    let index = (95 * durations.len()).div_ceil(100).saturating_sub(1);
    let p95 = durations[index];
    println!("asset search p95: {p95:?} over {} calls", durations.len());
    assert!(
        p95 <= Duration::from_millis(200),
        "asset search p95 {p95:?} exceeds 200 ms"
    );

    cleanup_benchmark_rows(&pool).await;
}
