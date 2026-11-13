//! Durable schedule loop for inject triggers.

use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use tokio::sync::watch;

use obolargus_core::automation::error::AutomationError;
use obolargus_core::automation::model::TriggerRecord;
use obolargus_core::automation::runtime::persistence;
use obolargus_core::automation::triggers::next_occurrence;

use crate::AppState;

use super::{audit, executor};

/// Returns true when an active run means this tick must be skipped.
pub fn skip_on_overlap(overlap_policy: &str, active_runs: usize) -> bool {
    overlap_policy == "skip" && active_runs > 0
}

/// Returns true when previous active runs must be cancelled before firing.
pub fn cancel_previous_on_overlap(overlap_policy: &str, active_runs: usize) -> bool {
    overlap_policy == "cancel_previous" && active_runs > 0
}

/// Returns true when `occurrence` is old enough to count as missed.
///
/// A one-second grace absorbs scheduler tick jitter while still treating a
/// genuinely overdue occurrence as missed.
pub fn is_missed(occurrence: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    occurrence < now - Duration::seconds(1)
}

/// Runs the scheduler until the shutdown signal flips.
pub async fn run(state: AppState, mut shutdown: watch::Receiver<bool>) {
    let interval =
        std::time::Duration::from_secs(state.config.automation_scheduler_interval_seconds.max(1));
    loop {
        if *shutdown.borrow() {
            return;
        }
        if let Err(error) = tick(&state).await {
            tracing::warn!(%error, "automation scheduler tick failed");
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

async fn tick(state: &AppState) -> Result<(), AutomationError> {
    let Some(pool) = state.pool.as_ref() else {
        return Ok(());
    };
    let due = persistence::due_triggers(pool, Utc::now(), 50).await?;
    for trigger in due {
        if let Err(error) = fire(state, &trigger).await {
            tracing::warn!(%error, trigger_id = %trigger.id, "trigger fire failed");
        }
    }
    Ok(())
}

async fn fire(state: &AppState, trigger: &TriggerRecord) -> Result<(), AutomationError> {
    let pool = state
        .pool
        .as_ref()
        .ok_or_else(|| AutomationError::Infrastructure("database unavailable".to_string()))?;
    let now = Utc::now();
    let (Some(kind), Some(next_at)) = (trigger.repeat_kind.as_deref(), trigger.next_occurrence_at)
    else {
        return Ok(());
    };
    let interval = trigger.interval_seconds.map(|value| value.max(0) as u64);

    // Missed-occurrence policy: `skip` advances without firing; `run_once`
    // fires a single catch-up run and re-anchors at the recovery time.
    if is_missed(next_at, now) && trigger.missed_policy == "skip" {
        let next = next_occurrence(
            kind,
            interval,
            trigger.cron_expression.as_deref(),
            &trigger.timezone,
            now,
        )?
        .unwrap_or(now);
        persistence::advance_trigger(pool, trigger.id, now, next).await?;
        audit::record_best_effort(
            state,
            "trigger.missed_skip",
            "trigger",
            trigger.id,
            Some(trigger.flow_id),
            None,
            json!({ "next_occurrence_at": next }),
        )
        .await;
        return Ok(());
    }

    let global_active = persistence::count_active_runs(pool, None).await?;
    let flow_active = persistence::count_active_runs(pool, Some(trigger.flow_id)).await?;
    if global_active >= i64::from(state.config.automation_max_concurrent_runs)
        || flow_active >= i64::from(state.config.automation_max_concurrent_runs_per_flow)
    {
        let next = next_occurrence(
            kind,
            interval,
            trigger.cron_expression.as_deref(),
            &trigger.timezone,
            now,
        )?
        .unwrap_or(now);
        persistence::advance_trigger(pool, trigger.id, now, next).await?;
        audit::record_best_effort(
            state,
            "trigger.queue_rejected",
            "trigger",
            trigger.id,
            Some(trigger.flow_id),
            None,
            json!({ "global_active": global_active, "flow_active": flow_active }),
        )
        .await;
        return Ok(());
    }

    let active = active_run_ids(pool, trigger.id).await?;
    if skip_on_overlap(&trigger.overlap_policy, active.len()) {
        let next = next_occurrence(
            kind,
            interval,
            trigger.cron_expression.as_deref(),
            &trigger.timezone,
            now,
        )?
        .unwrap_or(now);
        persistence::advance_trigger(pool, trigger.id, now, next).await?;
        audit::record_best_effort(
            state,
            "trigger.overlap_skip",
            "trigger",
            trigger.id,
            Some(trigger.flow_id),
            None,
            json!({ "active_runs": active.len(), "next_occurrence_at": next }),
        )
        .await;
        return Ok(());
    }
    if cancel_previous_on_overlap(&trigger.overlap_policy, active.len()) {
        for run_id in &active {
            let _ = persistence::request_cancel(pool, *run_id).await;
            state.automation.cancel(*run_id);
        }
        audit::record_best_effort(
            state,
            "trigger.overlap_cancel_previous",
            "trigger",
            trigger.id,
            Some(trigger.flow_id),
            None,
            json!({ "cancelled_runs": active.len() }),
        )
        .await;
    }

    let Some(version_id) = trigger.flow_version_id else {
        return Ok(());
    };
    let next = next_occurrence(
        kind,
        interval,
        trigger.cron_expression.as_deref(),
        &trigger.timezone,
        now,
    )?
    .unwrap_or(now);
    let run = persistence::insert_run(
        pool,
        trigger.flow_id,
        version_id,
        Some(trigger.id),
        "schedule",
        trigger.node_id.as_deref(),
        Some(&state.config.automation_actor_id),
        &json!({}),
        None,
        None,
    )
    .await?;
    persistence::advance_trigger(pool, trigger.id, now, next).await?;
    audit::record_best_effort(
        state,
        "trigger.fire",
        "trigger",
        trigger.id,
        Some(trigger.flow_id),
        Some(run.id),
        json!({ "next_occurrence_at": next }),
    )
    .await;
    executor::spawn(state.clone(), run.id);
    Ok(())
}

async fn active_run_ids(
    pool: &sqlx::PgPool,
    trigger_id: uuid::Uuid,
) -> Result<Vec<uuid::Uuid>, AutomationError> {
    sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT id FROM automation_runs \
         WHERE trigger_id = $1 AND status IN ('queued','running')",
    )
    .bind(trigger_id)
    .fetch_all(pool)
    .await
    .map_err(|error| AutomationError::Infrastructure(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;
    use uuid::Uuid;

    use crate::config::AppConfig;

    async fn test_pool() -> Option<sqlx::PgPool> {
        let url = std::env::var("TEST_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .ok()?;
        sqlx::migrate!("../obolargus_core/migrations")
            .run(&pool)
            .await
            .ok()?;
        Some(pool)
    }

    async fn make_flow(pool: &sqlx::PgPool, owner: &str) -> (Uuid, Uuid) {
        let document = json!({
            "schema_version": 1,
            "name": format!("scheduler-{}", Uuid::new_v4()),
            "nodes": [
                { "id": "n1", "type": "inject", "type_version": 1, "config": { "repeat": "none", "once": true } },
                { "id": "n2", "type": "debug", "type_version": 1, "config": { "target": "msg" } }
            ],
            "wires": [
                { "id": "w1", "source": "n1", "source_port": "out", "target": "n2", "target_port": "in" }
            ]
        });
        let name = format!("scheduler-{}", Uuid::new_v4());
        let flow = persistence::create_flow(pool, owner, &name, "", &[], &document)
            .await
            .expect("create flow");
        let version = persistence::publish_flow(pool, flow.id, flow.draft_revision, "test", owner)
            .await
            .expect("publish flow");
        (flow.id, version.id)
    }

    async fn insert_trigger(
        pool: &sqlx::PgPool,
        flow_id: Uuid,
        version_id: Uuid,
        next_occurrence_at: DateTime<Utc>,
        missed_policy: &str,
    ) -> TriggerRecord {
        let trigger = persistence::insert_inject_trigger(
            pool,
            flow_id,
            version_id,
            "n1",
            "interval",
            Some(60),
            None,
            "UTC",
            next_occurrence_at,
            "test",
        )
        .await
        .expect("insert trigger");
        persistence::update_trigger(pool, trigger.id, None, None, Some(missed_policy))
            .await
            .expect("update trigger")
    }

    fn manual_trigger(flow_id: Uuid) -> TriggerRecord {
        let now = Utc::now();
        TriggerRecord {
            id: Uuid::new_v4(),
            flow_id,
            flow_version_id: None,
            node_id: Some("n1".to_string()),
            kind: "inject".to_string(),
            enabled: true,
            repeat_kind: Some("interval".to_string()),
            interval_seconds: Some(60),
            cron_expression: None,
            timezone: "UTC".to_string(),
            overlap_policy: "queue".to_string(),
            missed_policy: "run_once".to_string(),
            next_occurrence_at: Some(now + ChronoDuration::minutes(1)),
            last_occurrence_at: None,
            created_by: "test".to_string(),
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn scheduler_paths_cover_missed_overlap_limits_and_ticks() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let state = AppState::new(AppConfig::default(), Some(pool.clone())).expect("app state");
        let owner = state.config.automation_actor_id.clone();

        // A missed `skip` occurrence advances without firing.
        let (flow_id, version_id) = make_flow(&pool, &owner).await;
        let trigger = insert_trigger(
            &pool,
            flow_id,
            version_id,
            Utc::now() - ChronoDuration::minutes(5),
            "skip",
        )
        .await;
        fire(&state, &trigger).await.expect("missed skip");
        let advanced = persistence::get_trigger(&pool, trigger.id)
            .await
            .expect("advanced trigger");
        assert!(
            advanced.next_occurrence_at.expect("next occurrence") > Utc::now(),
            "missed skip must advance the schedule"
        );
        assert!(
            persistence::latest_run(&pool, flow_id)
                .await
                .expect("latest run")
                .is_none(),
            "missed skip must not create a run"
        );

        // The global concurrency cap rejects the tick.
        let zero_global = AppState::new(
            AppConfig {
                automation_max_concurrent_runs: 0,
                ..AppConfig::default()
            },
            Some(pool.clone()),
        )
        .expect("zero global state");
        let (flow_id, version_id) = make_flow(&pool, &owner).await;
        let trigger = insert_trigger(
            &pool,
            flow_id,
            version_id,
            Utc::now() + ChronoDuration::minutes(1),
            "run_once",
        )
        .await;
        fire(&zero_global, &trigger).await.expect("queue rejection");
        assert!(
            persistence::latest_run(&pool, flow_id)
                .await
                .expect("latest run")
                .is_none()
        );

        // The per-flow concurrency cap rejects the tick as well.
        let zero_flow = AppState::new(
            AppConfig {
                automation_max_concurrent_runs: 100,
                automation_max_concurrent_runs_per_flow: 0,
                ..AppConfig::default()
            },
            Some(pool.clone()),
        )
        .expect("zero flow state");
        let (flow_id, version_id) = make_flow(&pool, &owner).await;
        let trigger = insert_trigger(
            &pool,
            flow_id,
            version_id,
            Utc::now() + ChronoDuration::minutes(1),
            "run_once",
        )
        .await;
        fire(&zero_flow, &trigger)
            .await
            .expect("flow queue rejection");
        assert!(
            persistence::latest_run(&pool, flow_id)
                .await
                .expect("latest run")
                .is_none()
        );

        // An active run causes `skip` to advance without firing.
        let overlap_state = AppState::new(
            AppConfig {
                automation_max_concurrent_runs: 100,
                automation_max_concurrent_runs_per_flow: 10,
                ..AppConfig::default()
            },
            Some(pool.clone()),
        )
        .expect("overlap state");
        let (flow_id, version_id) = make_flow(&pool, &owner).await;
        let trigger = insert_trigger(
            &pool,
            flow_id,
            version_id,
            Utc::now() + ChronoDuration::minutes(1),
            "run_once",
        )
        .await;
        let trigger = persistence::update_trigger(&pool, trigger.id, None, Some("skip"), None)
            .await
            .expect("skip policy");
        let active = persistence::insert_run(
            &pool,
            flow_id,
            version_id,
            Some(trigger.id),
            "schedule",
            Some("n1"),
            Some(&owner),
            &json!({}),
            None,
            None,
        )
        .await
        .expect("active run");
        fire(&overlap_state, &trigger).await.expect("overlap skip");
        assert_eq!(
            persistence::latest_run(&pool, flow_id)
                .await
                .expect("latest run")
                .expect("run")
                .id,
            active.id
        );
        assert_eq!(
            persistence::count_runs(&pool, Some(flow_id), None, None)
                .await
                .expect("count"),
            1
        );

        // `cancel_previous` cancels the active run and starts a new one.
        let trigger =
            persistence::update_trigger(&pool, trigger.id, None, Some("cancel_previous"), None)
                .await
                .expect("cancel policy");
        fire(&overlap_state, &trigger)
            .await
            .expect("overlap cancel");
        let cancelled = persistence::get_run(&pool, active.id)
            .await
            .expect("cancelled run");
        assert!(cancelled.cancel_requested_at.is_some());
        assert_eq!(
            persistence::count_runs(&pool, Some(flow_id), None, None)
                .await
                .expect("count"),
            2
        );

        // Triggers without a version or schedule fields are ignored.
        let mut manual = manual_trigger(flow_id);
        fire(&state, &manual).await.expect("no version");
        manual.repeat_kind = None;
        fire(&state, &manual).await.expect("no repeat kind");
        let mut no_next = manual_trigger(flow_id);
        no_next.next_occurrence_at = None;
        fire(&state, &no_next).await.expect("no next occurrence");

        let no_pool = AppState::new(AppConfig::default(), None).expect("no pool state");
        assert!(fire(&no_pool, &manual).await.is_err());

        // A due trigger fires through the tick entry point.
        let (flow_id, version_id) = make_flow(&pool, &owner).await;
        let trigger = insert_trigger(
            &pool,
            flow_id,
            version_id,
            Utc::now() - ChronoDuration::milliseconds(100),
            "run_once",
        )
        .await;
        tick(&state).await.expect("tick");
        let fired = persistence::latest_run(&pool, flow_id)
            .await
            .expect("latest run")
            .expect("fired run");
        assert_eq!(fired.trigger_id, Some(trigger.id));

        // The loop returns immediately when shutdown is already requested.
        let (_shutdown_tx, shutdown_rx) = watch::channel(true);
        run(state.clone(), shutdown_rx).await;

        // The loop also returns when the shutdown signal flips while sleeping.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run(state, shutdown_rx));
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        shutdown_tx.send(true).expect("shutdown");
        tokio::time::timeout(std::time::Duration::from_secs(3), handle)
            .await
            .expect("loop stopped")
            .expect("loop task");
    }
}
