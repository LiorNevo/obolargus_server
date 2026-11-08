//! Run executor: claims a queued run, executes its published flow, and
//! persists progress through the core execution observer.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use obolargus_core::automation::error::AutomationError;
use obolargus_core::automation::flow::document::FlowDocument;
use obolargus_core::automation::nodes::NodeOutput;
use obolargus_core::automation::runtime::bus::RunMetadata;
use obolargus_core::automation::runtime::context::ContextStore;
use obolargus_core::automation::runtime::executor::{
    ExecutionObserver, RunExecutionConfig, execute_flow_with_context,
};
use obolargus_core::automation::runtime::persistence;

use crate::AppState;

/// Builds node-run rows for the top-level flow and expanded subflows.
fn node_rows(flow: &FlowDocument, max_depth: u32) -> Vec<(String, String, u32)> {
    let mut rows = Vec::new();
    for node in &flow.nodes {
        rows.push((node.id.clone(), node.node_type.clone(), node.type_version));
        collect_subflow_rows(flow, node, &node.id, &mut rows, 0, max_depth);
    }
    rows
}

fn collect_subflow_rows(
    flow: &FlowDocument,
    node: &obolargus_core::automation::flow::document::FlowNode,
    prefix: &str,
    rows: &mut Vec<(String, String, u32)>,
    depth: u32,
    max_depth: u32,
) {
    if node.node_type != "subflow" || depth >= max_depth {
        return;
    }
    let Some(id) = node.config.get("subflow_id").and_then(Value::as_str) else {
        return;
    };
    let Some(definition) = flow.subflow(id) else {
        return;
    };
    for child in &definition.nodes {
        let qualified = format!("{prefix}/{}", child.id);
        rows.push((
            qualified.clone(),
            child.node_type.clone(),
            child.type_version,
        ));
        collect_subflow_rows(flow, child, &qualified, rows, depth + 1, max_depth);
    }
}

/// Spawns background execution for a queued run.
pub fn spawn(state: AppState, run_id: Uuid) {
    tokio::spawn(async move {
        if let Err(error) = execute_run(state.clone(), run_id).await {
            tracing::error!(%error, %run_id, "automation run execution failed");
        }
    });
}

/// Executes one queued run to a terminal state.
pub async fn execute_run(state: AppState, run_id: Uuid) -> Result<(), AutomationError> {
    let pool = state
        .pool
        .as_ref()
        .ok_or_else(|| AutomationError::Infrastructure("database unavailable".to_string()))?
        .clone();
    let run = persistence::get_run(&pool, run_id).await?;
    let version = persistence::get_version_by_id(&pool, run.flow_version_id)
        .await?
        .ok_or_else(|| AutomationError::NotFound(format!("version {}", run.flow_version_id)))?;
    let flow: FlowDocument = serde_json::from_value(version.flow_json.clone())
        .map_err(|error| AutomationError::Validation(format!("flow.json_invalid: {error}")))?;

    persistence::mark_run_running(&pool, run_id).await?;
    let nodes = node_rows(&flow, state.config.automation_max_subflow_depth);
    persistence::insert_pending_node_runs(&pool, run_id, &nodes).await?;

    let token = CancellationToken::new();
    state.automation.register_cancel(run_id, token.clone());
    if run.cancel_requested_at.is_some() {
        token.cancel();
    }
    let heartbeat_pool = pool.clone();
    let heartbeat_token = token.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(error) = persistence::heartbeat(&heartbeat_pool, run_id).await {
                        tracing::warn!(%error, %run_id, "run heartbeat failed");
                    }
                }
                _ = heartbeat_token.cancelled() => return,
            }
        }
    });
    let node_types: HashMap<String, (String, u32)> = nodes
        .iter()
        .map(|(id, node_type, version)| (id.clone(), (node_type.clone(), *version)))
        .collect();
    let observer = DbObserver {
        pool: pool.clone(),
        run_id,
        node_types: Arc::new(node_types),
        started: Mutex::new(HashMap::new()),
        max_message_bytes: state.config.automation_max_message_bytes,
        max_output_bytes: state.config.automation_max_output_bytes,
        max_context_bytes: state.config.automation_max_context_bytes,
    };
    let metadata = RunMetadata {
        id: run.id,
        flow_id: run.flow_id,
        flow_version_id: run.flow_version_id,
        trigger_kind: run.trigger_kind.clone(),
        trigger_node_id: run.trigger_node_id.clone(),
        started_at: Utc::now(),
        actor_id: run.actor_id.clone(),
        inputs: run.inputs.clone(),
    };
    let config = RunExecutionConfig {
        max_messages: state.config.automation_max_messages_per_run,
        max_messages_per_node: state.config.automation_max_messages_per_node,
        max_duration: Duration::from_secs(state.config.automation_max_run_duration_seconds),
        max_subflow_depth: state.config.automation_max_subflow_depth,
    };
    let global_scope_id = "workspace".to_string();
    let flow_scope_id = run.flow_id.to_string();
    let (_, global_records) =
        persistence::get_context(&pool, &flow_scope_id, &global_scope_id).await?;
    let scoped_records = persistence::get_scoped_flow_context(&pool, &flow_scope_id).await?;
    let mut flow_context: BTreeMap<String, Value> = BTreeMap::new();
    let mut scoped_context: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for record in scoped_records {
        if record.scope_id == flow_scope_id {
            flow_context.insert(record.key, record.value);
        } else {
            scoped_context
                .entry(record.scope_id)
                .or_default()
                .insert(record.key, record.value);
        }
    }
    let global_context: BTreeMap<String, Value> = global_records
        .into_iter()
        .map(|record| (record.key, record.value))
        .collect();
    let context = ContextStore::new(flow_scope_id, global_scope_id, flow_context, global_context)
        .with_scoped(scoped_context);
    let outcome = execute_flow_with_context(
        &flow,
        &state.automation.registry,
        metadata,
        Some(&pool),
        &state.market_data,
        token,
        config,
        &observer,
        context,
    )
    .await;
    heartbeat.abort();
    state.automation.release_cancel(run_id);

    let node_runs = persistence::list_node_runs(&pool, run_id)
        .await
        .unwrap_or_default();
    let mut summary = json!({
        "nodes_total": node_runs.len() as i64,
        "nodes_succeeded": node_runs.iter().filter(|row| row.status == "succeeded").count() as i64,
        "nodes_failed": node_runs.iter().filter(|row| row.status == "failed").count() as i64,
        "nodes_skipped": node_runs.iter().filter(|row| row.status == "skipped").count() as i64,
        "messages_total": outcome.messages_total as i64,
    });
    if let Some(report) = outcome.outputs.get("data_load_report") {
        summary["data_load"] = report.clone();
    }
    persistence::finish_run(
        &pool,
        run_id,
        outcome.status.as_str(),
        &outcome.outputs,
        &summary,
        outcome.error.as_ref(),
        outcome.messages_total as i64,
    )
    .await?;
    tracing::info!(%run_id, status = outcome.status.as_str(), "automation run finished");
    Ok(())
}

struct DbObserver {
    pool: sqlx::PgPool,
    run_id: Uuid,
    node_types: Arc<HashMap<String, (String, u32)>>,
    started: Mutex<HashMap<(String, u32), Instant>>,
    max_message_bytes: usize,
    max_output_bytes: usize,
    max_context_bytes: usize,
}

impl DbObserver {
    /// Ensures the attempt row exists before it is updated.
    async fn ensure(&self, node_id: &str, attempt: u32) {
        let Some((node_type, version)) = self.node_types.get(node_id) else {
            return;
        };
        let attempt = attempt.max(1) as i32;
        if let Err(error) = persistence::ensure_node_run(
            &self.pool,
            self.run_id,
            node_id,
            node_type,
            *version,
            attempt,
        )
        .await
        {
            tracing::warn!(%error, node_id, attempt, "node run insert failed");
        }
    }
}

#[async_trait::async_trait]
impl ExecutionObserver for DbObserver {
    async fn node_started(&self, node_id: &str, attempt: u32, message: &Value) {
        let now = Utc::now();
        self.ensure(node_id, attempt).await;
        if let Ok(mut guard) = self.started.lock() {
            guard.insert((node_id.to_string(), attempt), Instant::now());
        }
        let (payload, size, truncated) = cap(message, self.max_message_bytes);
        if let Err(error) = persistence::update_node_run(
            &self.pool,
            self.run_id,
            node_id,
            attempt.max(1) as i32,
            "running",
            Some(now),
            None,
            None,
            payload.as_ref(),
            None,
            false,
            1,
            0,
            None,
        )
        .await
        {
            tracing::warn!(%error, node_id, "node run update failed");
        }
        let msg_id = message
            .get("_msgid")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let _ = persistence::insert_message(
            &self.pool,
            self.run_id,
            Some(node_id),
            "in",
            None,
            &msg_id,
            payload.as_ref(),
            size,
            truncated,
            None,
        )
        .await;
    }

    async fn node_finished(
        &self,
        node_id: &str,
        attempt: u32,
        output: Option<&NodeOutput>,
        error: Option<&obolargus_core::automation::error::NodeExecutionError>,
    ) {
        let now = Utc::now();
        self.ensure(node_id, attempt).await;
        let duration = self
            .started
            .lock()
            .ok()
            .and_then(|mut guard| guard.remove(&(node_id.to_string(), attempt)))
            .map(|started| started.elapsed().as_millis().min(i64::MAX as u128) as i64);
        let (serialized, truncated) = match output {
            Some(output) => {
                let (capped, _size, truncated) = cap(
                    &serde_json::to_value(output).unwrap_or(Value::Null),
                    self.max_output_bytes,
                );
                (capped, truncated)
            }
            None => (None, false),
        };
        let messages_out = output
            .map(|value| value.messages.values().map(Vec::len).sum::<usize>() as i64)
            .unwrap_or(0);
        let status = if error.is_some() {
            "failed"
        } else {
            "succeeded"
        };
        let error_value = error.and_then(|value| serde_json::to_value(value).ok());
        if let Err(db_error) = persistence::update_node_run(
            &self.pool,
            self.run_id,
            node_id,
            attempt.max(1) as i32,
            status,
            None,
            Some(now),
            duration,
            None,
            serialized.as_ref(),
            truncated,
            0,
            messages_out,
            error_value.as_ref(),
        )
        .await
        {
            tracing::warn!(%db_error, node_id, "node run update failed");
        }
    }

    async fn message(&self, node_id: &str, port: &str, message: &Value) {
        let (payload, size, truncated) = cap(message, self.max_message_bytes);
        let msg_id = message
            .get("_msgid")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let _ = persistence::insert_message(
            &self.pool,
            self.run_id,
            Some(node_id),
            "out",
            Some(port),
            &msg_id,
            payload.as_ref(),
            size,
            truncated,
            None,
        )
        .await;
    }

    async fn context_changed(&self, scope: &str, scope_id: &str, key: &str, value: &Value) {
        let (capped, _size, _truncated) = cap(value, self.max_context_bytes);
        let Some(value) = capped else {
            tracing::warn!(scope, scope_id, key, "context value exceeds the size limit");
            return;
        };
        if let Err(error) =
            persistence::set_context(&self.pool, scope, scope_id, key, &value, Some(self.run_id))
                .await
        {
            tracing::warn!(%error, scope, scope_id, key, "context write failed");
        }
    }
}

/// Caps a JSON value at `max_bytes`, redacting secret-shaped keys.
///
/// Returns the (possibly redacted/capped) value, its serialized size before
/// capping, and whether the value was truncated.
pub fn cap(value: &Value, max_bytes: usize) -> (Option<Value>, i32, bool) {
    let redacted = redact(value);
    let size = serde_json::to_vec(&redacted)
        .map(|bytes| bytes.len())
        .unwrap_or(0) as i32;
    if max_bytes > 0 && size as usize > max_bytes {
        (None, size, true)
    } else {
        (Some(redacted), size, false)
    }
}

/// Recursively replaces secret-shaped fields with `"[redacted]"`.
pub fn redact(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut output = BTreeMap::new();
            for (key, entry) in map {
                let lower = key.to_ascii_lowercase();
                if lower.contains("secret")
                    || lower.contains("password")
                    || lower.contains("authorization")
                    || lower.contains("api_key")
                    || lower.contains("apikey")
                    || lower == "token"
                {
                    output.insert(key.clone(), Value::String("[redacted]".to_string()));
                } else {
                    output.insert(key.clone(), redact(entry));
                }
            }
            Value::Object(output.into_iter().collect())
        }
        Value::Array(items) => Value::Array(items.iter().map(redact).collect()),
        other => other.clone(),
    }
}

/// Registry handle re-exported for route modules.
pub type RegistryHandle = Arc<obolargus_core::automation::nodes::NodeRegistry>;
