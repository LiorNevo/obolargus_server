//! Server-sent events projection for a turn.

use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Extension, Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use serde_json::json;
use tokio::time::sleep;
use uuid::Uuid;

use obolargus_core::morpheus::persistence;

use super::{ApiError, MessageView, TurnSummaryView, require_enabled, require_pool};
use crate::AppState;
use crate::auth::AuthUser;
use crate::morpheus::state::MorpheusState;

struct StreamGuard(Arc<MorpheusState>);

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.0.release_stream();
    }
}

struct StreamState {
    pool: sqlx::PgPool,
    catalog: std::sync::Arc<obolargus_core::morpheus::tools::ToolCatalog>,
    turn_id: Uuid,
    sent_chars: usize,
    last_status: String,
    snapshot_sent: bool,
    done: bool,
    started: Instant,
    max_duration: Duration,
    poll: Duration,
    pending: VecDeque<Event>,
    pending_approvals: HashSet<Uuid>,
    _guard: StreamGuard,
}

async fn fill(current: &mut StreamState) {
    let Ok(turn) = persistence::get_turn(&current.pool, current.turn_id).await else {
        current.done = true;
        return;
    };
    let Ok(message) = persistence::get_turn_agent_message(&current.pool, current.turn_id).await
    else {
        current.done = true;
        return;
    };

    if !current.snapshot_sent {
        current.snapshot_sent = true;
        current.sent_chars = message.content.chars().count();
        current.last_status = turn.status.clone();
        let payload = json!({
            "turn": TurnSummaryView::from(turn.clone()),
            "agent_message": MessageView::from(message),
        });
        current
            .pending
            .push_back(Event::default().event("snapshot").data(payload.to_string()));
        return;
    }

    let content_chars = message.content.chars().count();
    if content_chars > current.sent_chars {
        let delta: String = message.content.chars().skip(current.sent_chars).collect();
        current.sent_chars = content_chars;
        current.pending.push_back(
            Event::default().event("delta").data(
                json!({ "turn_id": current.turn_id, "message_id": message.id, "text": delta })
                    .to_string(),
            ),
        );
        return;
    }

    let approvals = persistence::list_pending_approvals(&current.pool, current.turn_id)
        .await
        .unwrap_or_default();
    let pending_ids: HashSet<Uuid> = approvals.iter().map(|approval| approval.id).collect();
    if pending_ids != current.pending_approvals {
        for removed in current
            .pending_approvals
            .difference(&pending_ids)
            .copied()
            .collect::<Vec<_>>()
        {
            if let Ok(resolved) = persistence::get_approval(&current.pool, removed).await {
                current.pending.push_back(
                    Event::default().event("approval.resolved").data(
                        json!({
                            "approval_id": resolved.id,
                            "decision": resolved.status,
                            "decision_mode": resolved.decision_mode,
                            "decided_at": resolved.decided_at,
                        })
                        .to_string(),
                    ),
                );
            }
        }
        for approval in &approvals {
            let summary = if approval.action_kind == "tool" {
                persistence::get_tool_call(&current.pool, current.turn_id, approval.action_id)
                    .await
                    .ok()
                    .map(|call| {
                        let title = current
                            .catalog
                            .get(&call.tool_id)
                            .map(|spec| spec.display_name.to_string())
                            .unwrap_or_else(|| call.tool_id.clone());
                        json!({
                            "title": title,
                            "target": call.arguments_summary,
                            "risk": call.risk,
                        })
                    })
            } else {
                None
            };
            current.pending.push_back(
                Event::default().event("approval.request").data(
                    json!({
                        "approval": super::ApprovalView::from(approval.clone()),
                        "action_summary": summary,
                        "turn_status": "awaiting_approval",
                    })
                    .to_string(),
                ),
            );
        }
        current.pending_approvals = pending_ids;
    }
    if !current.pending.is_empty() {
        return;
    }

    if turn.status != current.last_status {
        current.last_status = turn.status.clone();
        let terminal = matches!(turn.status.as_str(), "completed" | "stopped" | "failed");
        let payload = json!({
            "turn_id": current.turn_id,
            "status": turn.status,
            "limit_reached": turn.limit_reached,
            "error": turn.error_code.map(|code| json!({
                "code": code,
                "message": turn.error_message,
            })),
        });
        current
            .pending
            .push_back(Event::default().event("status").data(payload.to_string()));
        if terminal {
            current.pending.push_back(
                Event::default()
                    .event("end")
                    .data(json!({ "turn_id": current.turn_id, "status": turn.status }).to_string()),
            );
            current.done = true;
        }
    }
}

/// Streams one turn.
#[utoipa::path(
    get,
    path = "/api/morpheus/turns/{turn_id}/events",
    params(("turn_id" = Uuid, Path, description = "Turn id")),
    responses((status = 200, description = "Server-sent event stream"), (status = 404, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn stream_events(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthUser>,
    Path(turn_id): Path<Uuid>,
) -> Result<Response, ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::get_owned_turn(&pool, &auth.owner_id, turn_id).await?;
    let max_streams = state.config.morpheus_max_sse_streams;
    if !state.morpheus.acquire_stream(max_streams) {
        return Err(ApiError::new(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Too many concurrent streams.",
        ));
    }

    let initial = StreamState {
        pool,
        catalog: state.morpheus.catalog.clone(),
        turn_id,
        sent_chars: 0,
        last_status: String::new(),
        snapshot_sent: false,
        done: false,
        started: Instant::now(),
        max_duration: Duration::from_secs(state.config.morpheus_sse_max_duration_seconds.max(1)),
        poll: Duration::from_millis(state.config.morpheus_sse_poll_ms.max(100)),
        pending: VecDeque::new(),
        pending_approvals: HashSet::new(),
        _guard: StreamGuard(state.morpheus.clone()),
    };

    let stream = futures::stream::unfold(initial, |mut current| async move {
        loop {
            if let Some(event) = current.pending.pop_front() {
                return Some((Ok::<Event, Infallible>(event), current));
            }
            if current.done || current.started.elapsed() >= current.max_duration {
                return None;
            }
            fill(&mut current).await;
            if current.pending.is_empty() {
                sleep(current.poll).await;
            }
        }
    });

    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
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
