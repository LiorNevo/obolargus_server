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
