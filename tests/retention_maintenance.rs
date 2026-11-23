//! Retention and maintenance integration tests (US4).
//!
//! Seeds expired and in-window rows, runs the real prune functions and the
//! server maintenance pass, and asserts only expired rows disappear. The
//! returned counts are the counts the scheduler logs. Database-gated: the
//! test is a passing no-op when no test database is configured.

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use obolargus_core::automation::runtime::retention;
use obolargus_core::morpheus;
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, tasks};

fn database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
}

async fn migrated_pool() -> Option<PgPool> {
    let url = database_url()?;
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
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

async fn reset(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE TABLE automation_runs, automation_flows, \
         automation_audit_events, morpheus_conversations RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset automation/morpheus tables");
}

struct AutomationSeed {
    flow_id: Uuid,
    version_id: Uuid,
}

async fn seed_flow(pool: &PgPool) -> AutomationSeed {
    let flow_id = Uuid::new_v4();
    let version_id = Uuid::new_v4();
    sqlx::query("INSERT INTO automation_flows (id, owner_id, name) VALUES ($1, 'dev-user', $2)")
        .bind(flow_id)
        .bind(format!("retention-{flow_id}"))
        .execute(pool)
        .await
        .expect("seed flow");
    sqlx::query(
        "INSERT INTO automation_flow_versions (id, flow_id, status, flow_json, created_by) \
         VALUES ($1, $2, 'draft', '{}'::jsonb, 'dev-user')",
    )
    .bind(version_id)
    .bind(flow_id)
    .execute(pool)
    .await
    .expect("seed version");
    AutomationSeed {
        flow_id,
        version_id,
    }
}

/// Inserts a run and one node run, message, and log. Returns the run id.
async fn seed_run(
    pool: &PgPool,
    flow: &AutomationSeed,
    status: &str,
    at: DateTime<Utc>,
    ended: bool,
) -> Uuid {
    let run_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO automation_runs \
         (id, flow_id, flow_version_id, trigger_kind, status, queued_at, started_at, \
          ended_at, created_at) \
         VALUES ($1, $2, $3, 'manual', $4, $5, $5, $6, $5)",
    )
    .bind(run_id)
    .bind(flow.flow_id)
    .bind(flow.version_id)
    .bind(status)
    .bind(at)
    .bind(ended.then_some(at))
    .execute(pool)
    .await
    .expect("seed run");
    sqlx::query(
        "INSERT INTO automation_node_runs \
         (run_id, node_id, node_type_id, node_type_version, status) \
         VALUES ($1, 'n1', 'inject', 1, 'succeeded')",
    )
    .bind(run_id)
    .execute(pool)
    .await
    .expect("seed node run");
    sqlx::query(
        "INSERT INTO automation_messages (run_id, direction, msg_id, ts) \
         VALUES ($1, 'in', 'm1', $2)",
    )
    .bind(run_id)
    .bind(at)
    .execute(pool)
    .await
    .expect("seed message");
    sqlx::query(
        "INSERT INTO automation_run_logs (run_id, level, message, ts) \
         VALUES ($1, 'info', 'retention fixture', $2)",
    )
    .bind(run_id)
    .bind(at)
    .execute(pool)
    .await
    .expect("seed log");
    run_id
}

async fn seed_audit(pool: &PgPool, flow_id: Uuid, at: DateTime<Utc>) {
    sqlx::query(
        "INSERT INTO automation_audit_events \
         (actor_id, action, target_type, target_id, outcome, created_at) \
         VALUES ('dev-user', 'flow.created', 'flow', $1, 'success', $2)",
    )
    .bind(flow_id)
    .bind(at)
    .execute(pool)
    .await
    .expect("seed audit event");
}

/// Seeds a conversation with a user message, an agent message, and a turn.
async fn seed_conversation(pool: &PgPool, at: DateTime<Utc>, turn_status: Option<&str>) -> Uuid {
    let conversation_id = Uuid::new_v4();
    let user_message = Uuid::new_v4();
    let agent_message = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO morpheus_conversations \
         (id, owner_id, client_conversation_id, title, created_at, updated_at) \
         VALUES ($1, 'dev-user', $2, 'retention fixture', $3, $3)",
    )
    .bind(conversation_id)
    .bind(Uuid::new_v4())
    .bind(at)
    .execute(pool)
    .await
    .expect("seed conversation");
    for (id, seq, role) in [(user_message, 1_i64, "user"), (agent_message, 2, "agent")] {
        sqlx::query(
            "INSERT INTO morpheus_messages \
             (id, conversation_id, seq, role, status, content, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, 'complete', 'fixture', $5, $5)",
        )
        .bind(id)
        .bind(conversation_id)
        .bind(seq)
        .bind(role)
        .bind(at)
        .execute(pool)
        .await
        .expect("seed message");
    }
    if let Some(status) = turn_status {
        sqlx::query(
            "INSERT INTO morpheus_turns \
             (conversation_id, user_message_id, agent_message_id, status, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $5)",
        )
        .bind(conversation_id)
        .bind(user_message)
        .bind(agent_message)
        .bind(status)
        .bind(at)
        .execute(pool)
        .await
        .expect("seed turn");
    }
    conversation_id
}

async fn scalar(pool: &PgPool, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .expect("scalar query")
}

#[tokio::test]
async fn retention_deletes_only_expired_rows_and_counts_match() {
    let Some(pool) = migrated_pool().await else {
        eprintln!("SKIPPED: no test database configured");
        return;
    };
    reset(&pool).await;

    let now = Utc::now();
    let old = now - ChronoDuration::days(400);
    let recent = now - ChronoDuration::days(1);
    let flow = seed_flow(&pool).await;

    // One expired terminal run (with children), one old running run, one
    // recent terminal run.
    let expired_run = seed_run(&pool, &flow, "succeeded", old, true).await;
    let running_run = seed_run(&pool, &flow, "running", old, false).await;
    let recent_run = seed_run(&pool, &flow, "succeeded", recent, true).await;
    seed_audit(&pool, flow.flow_id, old).await;
    seed_audit(&pool, flow.flow_id, recent).await;

    let report = retention::prune(&pool, 90, 365)
        .await
        .expect("prune automation");
    assert_eq!(report.runs_deleted, 1);
    assert_eq!(report.node_runs_deleted, 1);
    assert_eq!(report.messages_deleted, 1);
    assert_eq!(report.logs_deleted, 1);
    assert_eq!(report.audit_events_deleted, 1);

    assert_eq!(
        scalar(&pool, "SELECT count(*) FROM automation_runs").await,
        2,
        "queued/running and in-window runs must survive"
    );
    let expired_remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM automation_runs WHERE id = $1")
            .bind(expired_run)
            .fetch_one(&pool)
            .await
            .expect("expired run lookup");
    assert_eq!(expired_remaining, 0);
    let running_remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM automation_runs WHERE id = $1")
            .bind(running_run)
            .fetch_one(&pool)
            .await
            .expect("running run lookup");
    assert_eq!(running_remaining, 1);
    let recent_remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM automation_runs WHERE id = $1")
            .bind(recent_run)
            .fetch_one(&pool)
            .await
            .expect("recent run lookup");
    assert_eq!(recent_remaining, 1);
    assert_eq!(
        scalar(&pool, "SELECT count(*) FROM automation_audit_events").await,
        1
    );

    // Idempotent: a second pass removes nothing.
    let second = retention::prune(&pool, 90, 365)
        .await
        .expect("second prune");
    assert_eq!(second, retention::RetentionReport::default());

    // Morpheus: expired conversation is cascaded; active-turn and recent
    // conversations survive.
    let expired_conversation = seed_conversation(&pool, old, Some("completed")).await;
    let active_conversation = seed_conversation(&pool, old, Some("running")).await;
    let recent_conversation = seed_conversation(&pool, recent, None).await;

    let morpheus_report = morpheus::retention::prune(&pool, 365)
        .await
        .expect("prune morpheus");
    assert_eq!(morpheus_report.conversations_deleted, 1);
    assert_eq!(morpheus_report.messages_deleted, 2);
    assert_eq!(morpheus_report.turns_deleted, 1);
    assert_eq!(
        scalar(&pool, "SELECT count(*) FROM morpheus_conversations").await,
        2,
        "active-turn and in-window conversations must survive"
    );
    assert_eq!(
        scalar(&pool, "SELECT count(*) FROM morpheus_messages").await,
        4
    );
    let remaining_seeded: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM morpheus_conversations \
         WHERE id = ANY($1)",
    )
    .bind(vec![
        expired_conversation,
        active_conversation,
        recent_conversation,
    ])
    .fetch_one(&pool)
    .await
    .expect("conversation lookup");
    assert_eq!(remaining_seeded, 2);
    assert_eq!(
        scalar(&pool, "SELECT count(*) FROM morpheus_turns").await,
        1
    );
    let second = morpheus::retention::prune(&pool, 365)
        .await
        .expect("second morpheus prune");
    assert_eq!(second, morpheus::retention::RetentionReport::default());

    // The maintenance pass wires both prunes and the partition window.
    reset(&pool).await;
    let flow = seed_flow(&pool).await;
    seed_run(&pool, &flow, "succeeded", old, true).await;
    seed_audit(&pool, flow.flow_id, old).await;
    seed_conversation(&pool, old, Some("completed")).await;
    let state = AppState::new(
        AppConfig {
            automation_run_retention_days: 90,
            automation_audit_retention_days: 365,
            morpheus_conversation_retention_days: 365,
            ..AppConfig::default()
        },
        Some(pool.clone()),
    )
    .expect("app state");

    let summary = tasks::run_maintenance_once(&state).await;
    assert_eq!(summary.failures, 0);
    assert!(summary.partitions_ok);
    let automation = summary.automation.expect("automation counts");
    assert_eq!(automation.runs_deleted, 1);
    assert_eq!(automation.audit_events_deleted, 1);
    let morpheus = summary.morpheus.expect("morpheus counts");
    assert_eq!(morpheus.conversations_deleted, 1);
    assert_eq!(
        scalar(&pool, "SELECT count(*) FROM automation_runs").await,
        0
    );
    assert_eq!(
        scalar(&pool, "SELECT count(*) FROM morpheus_conversations").await,
        0
    );

    let today = Utc::now().date_naive();
    let partition = format!("market_observations_{}_{:02}", today.year(), today.month());
    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(&partition)
        .fetch_one(&pool)
        .await
        .expect("partition lookup");
    assert_eq!(
        exists.as_deref(),
        Some(partition.as_str()),
        "the maintenance pass must ensure the current monthly partition"
    );
}
