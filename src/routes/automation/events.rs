//! Server-Sent Events projection of a run.
//!
//! The database remains the source of truth; this stream polls changes and
//! emits the event types defined in `contracts/run-events.md`.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::response::sse::Event;
use axum::response::{IntoResponse, Sse};
use chrono::Utc;
use serde_json::json;
use tokio::time::sleep;
use uuid::Uuid;

use obolargus_core::automation::runtime::persistence;

use super::{ApiError, api_error, require_enabled, require_pool};
use crate::AppState;

impl Drop for StreamState {
    fn drop(&mut self) {
        // Covers normal completion, max-duration exit, and client disconnect.
        self.state
            .automation
            .sse_streams
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

struct StreamState {
    state: AppState,
    run_id: Uuid,
    queue: VecDeque<Event>,
    sequence: u64,
    done: bool,
    last_status: String,
    last_nodes: HashMap<String, String>,
    last_message_id: i64,
    last_log_id: i64,
    started: Instant,
    last_ping: Instant,
    snapshot_sent: bool,
}

fn event(sequence: u64, kind: &str, data: serde_json::Value) -> Event {
    let event = Event::default()
        .id(sequence.to_string())
        .event(kind)
        .data(data.to_string());
    if sequence == 1 {
        event.retry(Duration::from_secs(3))
    } else {
        event
    }
}

async fn fill(state: &mut StreamState) {
    let Some(pool) = state.state.pool.as_ref() else {
        state.done = true;
        return;
    };
    let run = match persistence::get_run(pool, state.run_id).await {
        Ok(run) => run,
        Err(_) => {
            state.done = true;
            return;
        }
    };
    let nodes = persistence::list_node_runs(pool, state.run_id)
        .await
        .unwrap_or_default();
    let version_number = persistence::get_version_by_id(pool, run.flow_version_id)
        .await
        .ok()
        .flatten()
        .and_then(|version| version.version_number);

    if !state.snapshot_sent {
        state.snapshot_sent = true;
        let node_json: Vec<serde_json::Value> = nodes
            .iter()
            .map(|node| {
                json!({
                    "node_id": node.node_id,
                    "node_type_id": node.node_type_id,
                    "node_type_version": node.node_type_version,
                    "attempt": node.attempt,
                    "status": node.status,
                    "started_at": node.started_at,
                    "ended_at": node.ended_at,
                    "duration_ms": node.duration_ms,
                    "messages_in": node.messages_in,
                    "messages_out": node.messages_out,
                })
            })
            .collect();
        let snapshot = json!({
            "run_id": run.id,
            "flow_id": run.flow_id,
            "flow_version_id": run.flow_version_id,
            "flow_version_number": version_number,
            "status": run.status,
            "trigger_kind": run.trigger_kind,
            "trigger_node_id": run.trigger_node_id,
            "queued_at": run.queued_at,
            "started_at": run.started_at,
            "ended_at": run.ended_at,
            "cancel_requested_at": run.cancel_requested_at,
            "message_count": run.message_count,
            "summary": run.summary,
            "nodes": node_json,
        });
        state.sequence += 1;
        let sequence = state.sequence;
        state.queue.push_back(event(sequence, "snapshot", snapshot));
        state.last_status = run.status.clone();
        for node in &nodes {
            state.last_nodes.insert(
                node.node_id.clone(),
                format!("{}:{}:{}", node.attempt, node.status, node.messages_out),
            );
        }
    }

    let messages = persistence::list_messages(
        pool,
        state.run_id,
        None,
        None,
        Some(state.last_message_id),
        200,
    )
    .await
    .unwrap_or_default();
    for message in messages {
        state.last_message_id = state.last_message_id.max(message.id);
        state.sequence += 1;
        let sequence = state.sequence;
        let mut payload = json!({
            "run_id": message.run_id,
            "message_id": message.id,
            "node_id": message.node_id,
            "direction": message.direction,
            "output_port": message.output_port,
            "msg_id": message.msg_id,
            "ts": message.ts,
            "size_bytes": message.size_bytes,
            "truncated": message.truncated,
        });
        if !message.truncated {
            payload["payload"] = message.payload.unwrap_or(serde_json::Value::Null);
        }
        state.queue.push_back(event(sequence, "message", payload));
    }

    let logs = persistence::list_logs(pool, state.run_id, None, Some(state.last_log_id), 200)
        .await
        .unwrap_or_default();
    for log in logs {
        state.last_log_id = state.last_log_id.max(log.id);
        state.sequence += 1;
        let sequence = state.sequence;
        let payload = json!({
            "run_id": log.run_id,
            "log_id": log.id,
            "node_id": log.node_id,
            "ts": log.ts,
            "level": log.level,
            "message": log.message,
            "fields": log.fields,
        });
        state.queue.push_back(event(sequence, "log", payload));
    }

    for node in &nodes {
        let signature = format!("{}:{}:{}", node.attempt, node.status, node.messages_out);
        if state.last_nodes.get(&node.node_id) == Some(&signature) {
            continue;
        }
        state.last_nodes.insert(node.node_id.clone(), signature);
        state.sequence += 1;
        let sequence = state.sequence;
        let payload = json!({
            "run_id": node.run_id,
            "node_id": node.node_id,
            "node_type_id": node.node_type_id,
            "node_type_version": node.node_type_version,
            "attempt": node.attempt,
            "status": node.status,
            "started_at": node.started_at,
            "ended_at": node.ended_at,
            "duration_ms": node.duration_ms,
            "messages_in": node.messages_in,
            "messages_out": node.messages_out,
            "output_truncated": node.output_truncated,
            "error": node.error,
        });
        state
            .queue
            .push_back(event(sequence, "node.status", payload));
    }

    if run.status != state.last_status {
        state.last_status = run.status.clone();
        state.sequence += 1;
        let sequence = state.sequence;
        let payload = json!({
            "run_id": run.id,
            "status": run.status,
            "cancel_requested_at": run.cancel_requested_at,
            "ended_at": run.ended_at,
            "message_count": run.message_count,
            "summary": run.summary,
            "error": run.error,
        });
        state
            .queue
            .push_back(event(sequence, "run.status", payload));
    }

    let terminal = matches!(
        run.status.as_str(),
        "succeeded" | "partial" | "failed" | "cancelled" | "interrupted"
    );
    if terminal {
        state.sequence += 1;
        let sequence = state.sequence;
        state.queue.push_back(event(
            sequence,
            "end",
            json!({ "run_id": run.id, "status": run.status, "ended_at": run.ended_at }),
        ));
        state.done = true;
    }

    if !state.done && state.last_ping.elapsed() >= Duration::from_secs(15) {
        state.last_ping = Instant::now();
        state.sequence += 1;
        let sequence = state.sequence;
        state
            .queue
            .push_back(event(sequence, "ping", json!({ "ts": Utc::now() })));
    }
}

/// `GET /api/automation/runs/{run_id}/events`
#[utoipa::path(
    get,
    path = "/api/automation/runs/{run_id}/events",
    tag = "automation",
    params(("run_id" = Uuid, Path, description = "Run id")),
    responses((status = 200, description = "Server-sent event stream"), (status = 404, body = super::ApiErrorBody))
)]
pub async fn stream_events(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
) -> Result<axum::response::Response, ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::get_run(pool, run_id)
        .await
        .map_err(|_| api_error(axum::http::StatusCode::NOT_FOUND, "not_found", "run"))?;

    let max_streams = state.config.automation_max_sse_streams;
    let current = state
        .automation
        .sse_streams
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        + 1;
    if current > max_streams {
        state
            .automation
            .sse_streams
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        return Err(api_error(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "too many concurrent streams",
        ));
    }

    let interval = Duration::from_millis(state.config.automation_sse_poll_interval_ms.max(100));
    let max_duration = Duration::from_secs(state.config.automation_sse_max_duration_seconds.max(1));
    let initial = StreamState {
        state: state.clone(),
        run_id,
        queue: VecDeque::new(),
        sequence: 0,
        done: false,
        last_status: String::new(),
        last_nodes: HashMap::new(),
        last_message_id: 0,
        last_log_id: 0,
        started: Instant::now(),
        last_ping: Instant::now(),
        snapshot_sent: false,
    };

    let stream = futures::stream::unfold(initial, move |mut current| async move {
        loop {
            if let Some(next) = current.queue.pop_front() {
                return Some((Ok::<Event, Infallible>(next), current));
            }
            if current.done || current.started.elapsed() >= max_duration {
                return None;
            }
            fill(&mut current).await;
            if current.queue.is_empty() {
                sleep(interval).await;
            }
        }
    });

    let mut response = Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response();
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-cache"),
    );
    headers.insert(
        axum::http::header::CONNECTION,
        axum::http::HeaderValue::from_static("keep-alive"),
    );
    headers.insert(
        "x-accel-buffering",
        axum::http::HeaderValue::from_static("no"),
    );
    Ok(response)
}
