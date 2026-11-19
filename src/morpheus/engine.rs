//! Durable ReACT turn executor.
//!
//! The engine alternates provider calls with approved tool execution. Approval
//! waits are persisted as `awaiting_approval`; the approval route resumes the
//! same turn. Content streams into the agent message as it is produced.

use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::time::timeout;
use uuid::Uuid;

use obolargus_core::morpheus::approvals::{approval_context_hash, hash_json, standing_covers_tool};
use obolargus_core::morpheus::context::build_context;
use obolargus_core::morpheus::error::{ErrorCode, MorpheusError};
use obolargus_core::morpheus::model::{
    MessageStatus, StepKind, StepStatus, ToolCallRecord, ToolCallStatus, TurnRecord, TurnStatus,
};
use obolargus_core::morpheus::persistence;

use crate::AppState;
use crate::morpheus::provider::tools_payload;
use crate::morpheus::stream::{SseDecoder, StreamEvent, ToolCallDelta};

#[derive(Debug, Default, Clone)]
struct AccumulatedTool {
    provider_id: String,
    name: String,
    arguments: String,
}

/// Runs one claimed turn until it terminates or waits for approval.
pub async fn run(state: AppState, turn: TurnRecord) {
    let Some(pool) = state.pool.clone() else {
        return;
    };
    let token = state.morpheus.register_cancellation(turn.id);
    match drive(&state, &pool, turn.id, token.clone()).await {
        Ok(()) => {}
        Err(error) if error.code() == ErrorCode::ApprovalDenied => {}
        Err(error) => {
            tracing::warn!(
                turn_id = %turn.id,
                code = error.code().as_str(),
                detail = %error,
                "morpheus turn failed"
            );
            let agent_id = persistence::get_turn(&pool, turn.id)
                .await
                .map(|value| value.agent_message_id)
                .unwrap_or(turn.agent_message_id);
            let _ = persistence::set_message_status(
                &pool,
                agent_id,
                MessageStatus::Failed.as_str(),
                Some(error.code().as_str()),
                Some(&error.user_message()),
            )
            .await;
            let _ = persistence::set_turn_status(
                &pool,
                turn.id,
                TurnStatus::Failed.as_str(),
                Some(error.code().as_str()),
                Some(&error.user_message()),
                None,
            )
            .await;
        }
    }
    state.morpheus.remove_cancellation(turn.id);
}

/// Resumes a turn after an approval decision.
pub fn resume(state: AppState, turn_id: Uuid) {
    tokio::spawn(async move {
        let Some(pool) = state.pool.clone() else {
            return;
        };
        let Ok(turn) = persistence::get_turn(&pool, turn_id).await else {
            return;
        };
        if matches!(turn.status.as_str(), "completed" | "stopped" | "failed") {
            return;
        }
        let _ = persistence::set_turn_status(
            &pool,
            turn_id,
            TurnStatus::Running.as_str(),
            None,
            None,
            None,
        )
        .await;
        let token = state.morpheus.register_cancellation(turn_id);
        match drive(&state, &pool, turn_id, token.clone()).await {
            Ok(()) => {}
            Err(error) if error.code() == ErrorCode::ApprovalDenied => {}
            Err(error) => {
                let _ = persistence::set_turn_status(
                    &pool,
                    turn_id,
                    TurnStatus::Failed.as_str(),
                    Some(error.code().as_str()),
                    Some(&error.user_message()),
                    None,
                )
                .await;
            }
        }
        state.morpheus.remove_cancellation(turn_id);
        crate::morpheus::scheduler::kick(state, turn.conversation_id);
    });
}

async fn drive(
    state: &AppState,
    pool: &sqlx::PgPool,
    turn_id: Uuid,
    token: tokio_util::sync::CancellationToken,
) -> Result<(), MorpheusError> {
    let config = &state.config;
    let mut rounds = 0_u32;

    loop {
        if token.is_cancelled() {
            return finish_stopped(pool, turn_id).await;
        }
        let turn = persistence::get_turn(pool, turn_id).await?;
        if matches!(turn.status.as_str(), "completed" | "stopped" | "failed") {
            return Ok(());
        }
        if turn.provider_calls >= config.morpheus_max_provider_calls as i32 {
            return finish_limit(pool, &turn, "provider_calls").await;
        }
        rounds += 1;
        if rounds > config.morpheus_max_react_steps {
            return finish_limit(pool, &turn, "steps").await;
        }

        let pending = persistence::list_pending_approvals(pool, turn_id).await?;
        if !pending.is_empty() {
            persistence::set_turn_status(
                pool,
                turn_id,
                TurnStatus::AwaitingApproval.as_str(),
                None,
                None,
                None,
            )
            .await?;
            return Ok(());
        }

        execute_approved_tools(state, pool, &turn).await?;

        let agent = persistence::get_turn_agent_message(pool, turn_id).await?;
        let user = persistence::get_turn_user_message(pool, turn_id).await?;
        let messages = build_provider_messages(state, pool, &turn, &user, &agent).await?;
        let response = state
            .morpheus
            .provider
            .chat(
                &messages,
                Some(tools_payload(&state.morpheus.catalog)),
                false,
            )
            .await?;
        persistence::increment_turn_provider_calls(pool, turn_id).await?;
        state.morpheus.set_health(true, None);
        persistence::set_message_status(
            pool,
            agent.id,
            MessageStatus::Streaming.as_str(),
            None,
            None,
        )
        .await?;

        let mut decoder = SseDecoder::new();
        let mut stream = response.bytes_stream();
        let mut buffer = String::new();
        let mut tools: Vec<AccumulatedTool> = Vec::new();
        let mut last_flush = Instant::now();
        let flush_every = Duration::from_millis(config.morpheus_stream_flush_ms.max(50));
        let idle = Duration::from_secs(config.morpheus_idle_timeout_seconds.max(1));
        let max_bytes = config.morpheus_max_answer_bytes;
        let mut finished = false;

        while !finished {
            if token.is_cancelled() {
                flush(pool, agent.id, &mut buffer).await?;
                return finish_stopped(pool, turn_id).await;
            }
            let next = match timeout(idle, stream.next()).await {
                Ok(next) => next,
                Err(_) => {
                    flush(pool, agent.id, &mut buffer).await?;
                    return Err(MorpheusError::provider(
                        ErrorCode::ProviderTimeout,
                        "provider idle timeout",
                    ));
                }
            };
            let Some(chunk) = next else {
                break;
            };
            let chunk = chunk.map_err(|error| {
                MorpheusError::provider(ErrorCode::ProviderUnavailable, error.to_string())
            })?;
            for event in decoder.push(&chunk) {
                match event? {
                    StreamEvent::Content(text) => {
                        buffer.push_str(&text);
                        if buffer.len() > max_bytes {
                            let mut end = max_bytes;
                            while end > 0 && !buffer.is_char_boundary(end) {
                                end -= 1;
                            }
                            buffer.truncate(end);
                            flush(pool, agent.id, &mut buffer).await?;
                            return finish_limit(pool, &turn, "answer_bytes").await;
                        }
                    }
                    StreamEvent::ToolCall(delta) => {
                        accumulate_tool(&mut tools, delta);
                    }
                    StreamEvent::FinishReason(_) | StreamEvent::Done => {
                        finished = true;
                        break;
                    }
                    StreamEvent::Usage(_) => {}
                }
            }
            if last_flush.elapsed() >= flush_every {
                flush(pool, agent.id, &mut buffer).await?;
                last_flush = Instant::now();
            }
        }
        flush(pool, agent.id, &mut buffer).await?;

        if tools.is_empty() {
            persistence::set_message_status(
                pool,
                agent.id,
                MessageStatus::Complete.as_str(),
                None,
                None,
            )
            .await?;
            persistence::set_turn_status(
                pool,
                turn_id,
                TurnStatus::Completed.as_str(),
                None,
                None,
                None,
            )
            .await?;
            persistence::touch_conversation(pool, turn.conversation_id).await?;
            return Ok(());
        }

        // A tool-call response can carry text; keep it out of the final answer.
        if !agent.content.is_empty() {
            persistence::set_message_content(pool, agent.id, "").await?;
        }
        handle_tool_calls(state, pool, &turn, &tools).await?;
        let pending = persistence::list_pending_approvals(pool, turn_id).await?;
        if !pending.is_empty() {
            persistence::set_turn_status(
                pool,
                turn_id,
                TurnStatus::AwaitingApproval.as_str(),
                None,
                None,
                None,
            )
            .await?;
            return Ok(());
        }
        // Auto-approved calls are executed at the top of the next iteration.
    }
}

fn accumulate_tool(tools: &mut Vec<AccumulatedTool>, delta: ToolCallDelta) {
    while tools.len() <= delta.index {
        tools.push(AccumulatedTool::default());
    }
    let entry = &mut tools[delta.index];
    if let Some(id) = delta.id {
        entry.provider_id = id;
    }
    if let Some(name) = delta.name {
        entry.name.push_str(&name);
    }
    if let Some(arguments) = delta.arguments {
        entry.arguments.push_str(&arguments);
    }
}

async fn handle_tool_calls(
    state: &AppState,
    pool: &sqlx::PgPool,
    turn: &TurnRecord,
    tools: &[AccumulatedTool],
) -> Result<(), MorpheusError> {
    let standing = persistence::get_active_standing(pool, &state.config.morpheus_actor_id).await?;
    let existing = persistence::list_tool_calls(pool, turn.id).await?;
    let mut count = existing.len() as u32;

    for tool in tools.iter().filter(|tool| !tool.name.is_empty()) {
        let tool_id = tool.name.replace("__", ".");
        let arguments: Value = if tool.arguments.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str(&tool.arguments).unwrap_or_else(|_| json!({ "__invalid": true }))
        };
        let spec = state.morpheus.catalog.get(&tool_id);
        let (risk, version, summary) = match spec {
            Some(spec) => (spec.risk.as_str(), spec.version, spec.summarize(&arguments)),
            None => ("read", 1, "Unknown action".to_string()),
        };
        let provider_call_id = if tool.provider_id.is_empty() {
            format!("call_{count}")
        } else {
            tool.provider_id.clone()
        };
        let record = persistence::insert_tool_call(
            pool,
            turn.id,
            &provider_call_id,
            &tool_id,
            version,
            arguments.clone(),
            &summary,
            risk,
            "proposed",
        )
        .await?;
        count += 1;
        if count > state.config.morpheus_max_tool_calls_per_turn {
            persistence::finish_tool_call(
                pool,
                record.id,
                ToolCallStatus::Failed.as_str(),
                None,
                false,
                Some(ErrorCode::TurnLimit.as_str()),
                Some(ErrorCode::TurnLimit.message()),
            )
            .await?;
            continue;
        }

        let validated = state.morpheus.catalog.validate(&tool_id, &arguments);
        if let Err(error) = validated {
            persistence::finish_tool_call(
                pool,
                record.id,
                ToolCallStatus::Failed.as_str(),
                None,
                false,
                Some(error.code().as_str()),
                Some(&error.user_message()),
            )
            .await?;
            state.morpheus.set_health(true, None);
            continue;
        }

        let spec = spec.expect("validated spec");
        let spec_hash = spec.spec_hash();
        let context_hash = approval_context_hash(&tool_id, &hash_json(&arguments));
        let approval = persistence::create_approval(
            pool,
            turn.id,
            "tool",
            record.id,
            &context_hash,
            state.config.morpheus_approval_timeout_seconds,
        )
        .await?;
        persistence::set_tool_call_approval(pool, record.id, approval.id).await?;

        let auto_mode = if turn.approve_all_turn {
            Some("turn")
        } else if standing
            .as_ref()
            .is_some_and(|value| standing_covers_tool(value, &tool_id, &spec_hash))
        {
            Some("standing")
        } else {
            None
        };

        if let Some(mode) = auto_mode {
            persistence::decide_approval(pool, approval.id, "approved", mode).await?;
            persistence::set_tool_call_status(pool, record.id, ToolCallStatus::Approved.as_str())
                .await?;
        } else {
            persistence::set_tool_call_status(
                pool,
                record.id,
                ToolCallStatus::AwaitingApproval.as_str(),
            )
            .await?;
            persistence::insert_step(
                pool,
                turn.id,
                StepKind::Tool.as_str(),
                StepStatus::Started.as_str(),
                &format!("Waiting for approval: {summary}"),
                Some(record.id),
                json!({}),
            )
            .await?;
        }
    }
    Ok(())
}

async fn execute_approved_tools(
    state: &AppState,
    pool: &sqlx::PgPool,
    turn: &TurnRecord,
) -> Result<(), MorpheusError> {
    let calls = persistence::list_tool_calls(pool, turn.id).await?;
    for call in calls {
        match call.status.as_str() {
            "approved" => {
                run_tool(state, pool, &call).await?;
            }
            "denied" | "expired" => {
                let message = if call.status == "denied" {
                    "The user denied this action."
                } else {
                    "The approval expired before the action ran."
                };
                persistence::finish_tool_call(
                    pool,
                    call.id,
                    &call.status,
                    Some(message),
                    false,
                    None,
                    None,
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn run_tool(
    state: &AppState,
    pool: &sqlx::PgPool,
    call: &ToolCallRecord,
) -> Result<(), MorpheusError> {
    persistence::start_tool_call(pool, call.id).await?;
    let step = persistence::insert_step(
        pool,
        call.turn_id,
        StepKind::Tool.as_str(),
        StepStatus::Started.as_str(),
        &format!("Running {}", call.tool_id),
        Some(call.id),
        json!({}),
    )
    .await?;
    let outcome = timeout(
        Duration::from_secs(state.config.morpheus_tool_timeout_seconds.max(1)),
        crate::morpheus::tools::execute(state, pool, &call.tool_id, &call.arguments),
    )
    .await;
    match outcome {
        Ok(Ok(outcome)) => {
            persistence::finish_tool_call(
                pool,
                call.id,
                ToolCallStatus::Succeeded.as_str(),
                Some(&outcome.summary),
                outcome.truncated,
                None,
                None,
            )
            .await?;
            persistence::finish_step(
                pool,
                step.id,
                StepStatus::Completed.as_str(),
                &outcome.summary,
            )
            .await?;
        }
        Ok(Err(error)) => {
            persistence::finish_tool_call(
                pool,
                call.id,
                ToolCallStatus::Failed.as_str(),
                Some(&error.user_message()),
                false,
                Some(error.code().as_str()),
                Some(&error.user_message()),
            )
            .await?;
            persistence::finish_step(
                pool,
                step.id,
                StepStatus::Failed.as_str(),
                &error.user_message(),
            )
            .await?;
        }
        Err(_) => {
            persistence::finish_tool_call(
                pool,
                call.id,
                ToolCallStatus::Failed.as_str(),
                Some(ErrorCode::ToolTimeout.message()),
                false,
                Some(ErrorCode::ToolTimeout.as_str()),
                Some(ErrorCode::ToolTimeout.message()),
            )
            .await?;
            persistence::finish_step(
                pool,
                step.id,
                StepStatus::Failed.as_str(),
                ErrorCode::ToolTimeout.message(),
            )
            .await?;
        }
    }
    Ok(())
}

async fn build_provider_messages(
    state: &AppState,
    pool: &sqlx::PgPool,
    turn: &TurnRecord,
    user: &obolargus_core::morpheus::model::MessageRecord,
    agent: &obolargus_core::morpheus::model::MessageRecord,
) -> Result<Value, MorpheusError> {
    let history = persistence::list_messages(pool, turn.conversation_id, 200)
        .await?
        .0
        .into_iter()
        .filter(|message| message.id != user.id && message.id != agent.id)
        .collect::<Vec<_>>();
    let context = build_context(
        &state.config.morpheus_system_prompt,
        &history,
        &user.content,
        state.config.morpheus_max_context_messages,
        state.config.morpheus_max_context_chars,
    );
    if context.trimmed {
        persistence::set_context_trimmed(pool, agent.id, true).await?;
    }
    let mut messages: Vec<Value> = context
        .messages
        .iter()
        .map(|message| json!({ "role": message.role, "content": message.content }))
        .collect();

    let calls = persistence::list_tool_calls(pool, turn.id).await?;
    let terminal: Vec<&ToolCallRecord> = calls
        .iter()
        .filter(|call| {
            matches!(
                call.status.as_str(),
                "succeeded" | "failed" | "denied" | "expired" | "cancelled"
            )
        })
        .collect();
    if !terminal.is_empty() {
        let tool_calls: Vec<Value> = terminal
            .iter()
            .map(|call| {
                json!({
                    "id": call.provider_call_id,
                    "type": "function",
                    "function": {
                        "name": call.tool_id.replace('.', "__"),
                        "arguments": call.arguments.to_string(),
                    }
                })
            })
            .collect();
        messages.push(json!({
            "role": "assistant",
            "content": null,
            "tool_calls": tool_calls,
        }));
        for call in terminal {
            let content = call
                .result_summary
                .clone()
                .or_else(|| call.error_message.clone())
                .unwrap_or_else(|| "No result.".to_string());
            let content: String = content.chars().take(8_000).collect();
            messages.push(json!({
                "role": "tool",
                "tool_call_id": call.provider_call_id,
                "content": content,
            }));
        }
    }
    Ok(Value::Array(messages))
}

async fn flush(
    pool: &sqlx::PgPool,
    message_id: Uuid,
    buffer: &mut String,
) -> Result<(), MorpheusError> {
    if buffer.is_empty() {
        return Ok(());
    }
    let delta = std::mem::take(buffer);
    persistence::append_message_content(pool, message_id, &delta).await?;
    Ok(())
}

async fn finish_stopped(pool: &sqlx::PgPool, turn_id: Uuid) -> Result<(), MorpheusError> {
    let turn = persistence::get_turn(pool, turn_id).await?;
    persistence::set_message_status(
        pool,
        turn.agent_message_id,
        MessageStatus::Stopped.as_str(),
        None,
        None,
    )
    .await?;
    persistence::set_turn_status(
        pool,
        turn_id,
        TurnStatus::Stopped.as_str(),
        None,
        None,
        None,
    )
    .await?;
    persistence::touch_conversation(pool, turn.conversation_id).await?;
    Ok(())
}

async fn finish_limit(
    pool: &sqlx::PgPool,
    turn: &TurnRecord,
    limit: &str,
) -> Result<(), MorpheusError> {
    persistence::set_message_status(
        pool,
        turn.agent_message_id,
        MessageStatus::Complete.as_str(),
        Some(ErrorCode::TurnLimit.as_str()),
        Some(ErrorCode::TurnLimit.message()),
    )
    .await?;
    persistence::set_turn_status(
        pool,
        turn.id,
        TurnStatus::Completed.as_str(),
        Some(ErrorCode::TurnLimit.as_str()),
        Some(ErrorCode::TurnLimit.message()),
        Some(limit),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulate_tool_merges_stream_fragments() {
        let mut tools = Vec::new();
        accumulate_tool(
            &mut tools,
            ToolCallDelta {
                index: 0,
                id: Some("call-1".to_string()),
                name: Some("assets__search".to_string()),
                arguments: Some("{\"query\":".to_string()),
            },
        );
        accumulate_tool(
            &mut tools,
            ToolCallDelta {
                index: 0,
                id: None,
                name: None,
                arguments: Some("\"Apple\"}".to_string()),
            },
        );
        accumulate_tool(
            &mut tools,
            ToolCallDelta {
                index: 2,
                id: Some("call-2".to_string()),
                name: Some("market_data__lookup".to_string()),
                arguments: None,
            },
        );
        assert_eq!(tools.len(), 3);
        assert_eq!(tools[0].provider_id, "call-1");
        assert_eq!(tools[0].name, "assets__search");
        assert_eq!(tools[0].arguments, "{\"query\":\"Apple\"}");
        assert_eq!(tools[1].name, "");
        assert_eq!(tools[2].provider_id, "call-2");
    }
}
