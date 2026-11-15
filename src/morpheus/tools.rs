//! Tool execution adapters.
//!
//! Handlers call existing core services directly; they never call the app's
//! HTTP API or execute arbitrary code. Unimplemented catalog entries fail
//! clearly instead of pretending to run.

use serde_json::{Value, json};

use obolargus_core::domain::asset::entity::AssetSearchQuery;
use obolargus_core::domain::asset::search_asset_options;
use obolargus_core::market_data::history::{HistoryFilter, list_history};
use obolargus_core::market_data::types::{LatestBarRequest, TypedIdentifier};
use obolargus_core::morpheus::error::{ErrorCode, MorpheusError};

use crate::AppState;

/// One tool result.
#[derive(Debug, Clone)]
pub struct ToolOutcome {
    /// Bounded, user-facing summary.
    pub summary: String,
    /// Structured data added to the model context.
    pub data: Value,
    /// Whether the data was truncated.
    pub truncated: bool,
}

/// Executes one validated tool call.
pub async fn execute(
    state: &AppState,
    pool: &sqlx::PgPool,
    tool_id: &str,
    arguments: &Value,
) -> Result<ToolOutcome, MorpheusError> {
    match tool_id {
        "assets.search" => assets_search(pool, arguments).await,
        "market_data.lookup" => market_lookup(state, arguments).await,
        "data_load.history" => data_load_history(state, pool, arguments).await,
        other => Err(MorpheusError::tool(
            ErrorCode::ToolFailed,
            format!("tool is not implemented: {other}"),
        )),
    }
}

async fn assets_search(
    pool: &sqlx::PgPool,
    arguments: &Value,
) -> Result<ToolOutcome, MorpheusError> {
    let query = arguments
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let limit = arguments
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(10)
        .clamp(1, 50) as u32;
    let options = search_asset_options(
        pool,
        AssetSearchQuery {
            q: Some(query.to_string()),
            limit,
        },
    )
    .await
    .map_err(|error| {
        MorpheusError::tool(ErrorCode::ToolFailed, format!("asset search: {error}"))
    })?;
    let items: Vec<Value> = options
        .iter()
        .map(|option| {
            json!({
                "id": option.id,
                "display_name": option.display_name,
                "asset_type": option.asset_type,
                "currency": option.currency,
                "primary_identifier": option.primary_identifier,
            })
        })
        .collect();
    Ok(ToolOutcome {
        summary: format!("Found {} instruments.", items.len()),
        data: json!({ "items": items }),
        truncated: false,
    })
}

async fn data_load_history(
    state: &AppState,
    pool: &sqlx::PgPool,
    arguments: &Value,
) -> Result<ToolOutcome, MorpheusError> {
    let limit = arguments
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(10)
        .clamp(1, 50) as i64;
    let filter = HistoryFilter {
        from: date_argument(arguments, "from")?,
        to: date_argument(arguments, "to")?,
        domain: string_argument(arguments, "domain"),
        source: string_argument(arguments, "source"),
        status: string_argument(arguments, "status"),
        entry_type: string_argument(arguments, "entry_type"),
        limit,
        offset: 0,
    };
    let page = list_history(pool, &filter, state.config.data_load_stale_after_seconds)
        .await
        .map_err(|error| {
            MorpheusError::tool(ErrorCode::ToolFailed, format!("load history: {error}"))
        })?;
    let items: Vec<Value> = page
        .entries
        .iter()
        .map(|entry| {
            json!({
                "entry_id": entry.entry_id,
                "entry_type": entry.entry_type,
                "status": entry.status,
                "started_at": entry.started_at,
                "ended_at": entry.ended_at,
                "duration_ms": entry.duration_ms,
                "domain": entry.data_domain,
                "requested_source": entry.requested_source,
                "answering_sources": entry.answering_sources,
                "identifiers_attempted": entry.identifiers_attempted,
                "loaded": entry.loaded,
                "no_op": entry.no_op,
                "not_resolved": entry.not_resolved,
                "no_data_for_date": entry.no_data_for_date,
                "rejected": entry.rejected,
                "source_unavailable": entry.source_unavailable,
                "rate_limited": entry.rate_limited,
                "storage_failure": entry.storage_failure,
                "date_from": entry.date_from.map(|value| value.to_string()),
                "date_to": entry.date_to.map(|value| value.to_string()),
                "stale": entry.stale,
            })
        })
        .collect();
    let truncated = page.total > items.len() as i64;
    let summary = if page.total == 0 {
        "No load runs match that filter.".to_string()
    } else {
        format!("Found {} of {} load runs.", items.len(), page.total)
    };
    Ok(ToolOutcome {
        summary,
        data: json!({ "items": items, "total": page.total, "truncated": truncated }),
        truncated,
    })
}

fn string_argument(arguments: &Value, key: &str) -> Option<String> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn date_argument(arguments: &Value, key: &str) -> Result<Option<chrono::NaiveDate>, MorpheusError> {
    let Some(value) = string_argument(arguments, key) else {
        return Ok(None);
    };
    chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d")
        .map(Some)
        .map_err(|error| {
            MorpheusError::tool(
                ErrorCode::ToolInvalidArguments,
                format!("{key} must be YYYY-MM-DD: {error}"),
            )
        })
}

async fn market_lookup(state: &AppState, arguments: &Value) -> Result<ToolOutcome, MorpheusError> {
    let identifier = arguments
        .get("identifier")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let latest = state
        .market_data
        .get_latest_bar(LatestBarRequest::now(TypedIdentifier::new(
            "TICKER", identifier,
        )))
        .await
        .map_err(|error| {
            MorpheusError::tool(ErrorCode::ToolFailed, format!("market lookup: {error}"))
        })?;
    let summary = format!(
        "{identifier} closed at {} on {}.",
        latest.bar.close, latest.bar.as_of_date
    );
    Ok(ToolOutcome {
        summary,
        data: json!({
            "identifier": identifier,
            "as_of_date": latest.bar.as_of_date.to_string(),
            "close": latest.bar.close.to_string(),
            "source_id": latest.source_id,
        }),
        truncated: false,
    })
}
