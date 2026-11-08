//! Version, publish, export, and import routes.

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use obolargus_core::automation::flow::document::FlowDocument;
use obolargus_core::automation::flow::validate::validate_flow;
use obolargus_core::automation::model::FlowVersionRecord;
use obolargus_core::automation::runtime::persistence;

use super::{
    ApiResult, Page, Pagination, api_error, flows::ValidationView, map_error, require_enabled,
    require_pool,
};
use crate::AppState;
use crate::automation::audit;

/// Version list item.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct VersionSummary {
    /// Version id.
    pub id: Uuid,
    /// Version number.
    pub version_number: i32,
    /// Status.
    pub status: String,
    /// Change note.
    pub change_note: Option<String>,
    /// Publisher.
    pub published_by: Option<String>,
    /// Publication instant.
    pub published_at: Option<DateTime<Utc>>,
}

impl From<FlowVersionRecord> for VersionSummary {
    fn from(value: FlowVersionRecord) -> Self {
        Self {
            id: value.id,
            version_number: value.version_number.unwrap_or_default(),
            status: value.status,
            change_note: value.change_note,
            published_by: value.published_by,
            published_at: value.published_at,
        }
    }
}

/// Published version detail.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct VersionDetail {
    /// Version id.
    pub id: Uuid,
    /// Version number.
    pub version_number: i32,
    /// Status.
    pub status: String,
    /// Change note.
    pub change_note: Option<String>,
    /// Publisher.
    pub published_by: Option<String>,
    /// Publication instant.
    pub published_at: Option<DateTime<Utc>>,
    /// Canonical flow document.
    pub flow_json: Value,
    /// Validation result.
    pub validation: ValidationView,
}

/// Body of `POST /api/automation/flows/{flow_id}/publish`.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct PublishRequest {
    /// Expected current draft revision.
    pub expected_revision: i64,
    /// Required change note.
    pub change_note: String,
}

/// `POST /api/automation/flows/{flow_id}/publish`
#[utoipa::path(
    post,
    path = "/api/automation/flows/{flow_id}/publish",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    request_body = PublishRequest,
    responses(
        (status = 201, body = VersionDetail),
        (status = 400, body = super::ApiErrorBody),
        (status = 409, body = super::ApiErrorBody)
    )
)]
pub async fn publish(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    Json(body): Json<PublishRequest>,
) -> Result<(StatusCode, Json<VersionDetail>), super::ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    if body.change_note.trim().is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "version.change_note_required",
        ));
    }
    let (_, draft) = persistence::get_flow(pool, flow_id)
        .await
        .map_err(map_error)?;
    let Some(draft) = draft else {
        return Err(api_error(StatusCode::CONFLICT, "conflict", "draft.none"));
    };
    let document: FlowDocument = serde_json::from_value(draft.flow_json).map_err(|error| {
        api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("flow.json_invalid: {error}"),
        )
    })?;
    let report = validate_flow(&document, &state.automation.registry);
    if !report.is_valid() {
        let details = serde_json::to_value(&report).unwrap_or(Value::Null);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(Box::new(super::ApiErrorBody {
                error: super::ApiErrorDetail {
                    code: "validation_error".to_string(),
                    message: "the flow has validation errors".to_string(),
                    details: Some(details),
                },
            })),
        ));
    }
    let version = persistence::publish_flow(
        pool,
        flow_id,
        body.expected_revision,
        &body.change_note,
        &state.config.automation_actor_id,
    )
    .await
    .map_err(map_error)?;
    reconcile_inject_triggers(&state, flow_id, &version).await;
    audit::record_best_effort(
        &state,
        "version.publish",
        "version",
        version.id,
        Some(flow_id),
        None,
        json!({ "version_number": version.version_number }),
    )
    .await;
    Ok((
        StatusCode::CREATED,
        Json(VersionDetail {
            id: version.id,
            version_number: version.version_number.unwrap_or_default(),
            status: version.status,
            change_note: version.change_note,
            published_by: version.published_by,
            published_at: version.published_at,
            flow_json: version.flow_json,
            validation: super::flows::validation_view_for(&report),
        }),
    ))
}

async fn reconcile_inject_triggers(state: &AppState, flow_id: Uuid, version: &FlowVersionRecord) {
    let Some(pool) = state.pool.as_ref() else {
        return;
    };
    if let Err(error) = persistence::delete_inject_triggers(pool, flow_id).await {
        tracing::warn!(%error, "failed to clear inject triggers");
        return;
    }
    let Ok(document) = serde_json::from_value::<FlowDocument>(version.flow_json.clone()) else {
        return;
    };
    for node in &document.nodes {
        if node.node_type != "inject" {
            continue;
        }
        let repeat = node
            .config
            .get("repeat")
            .and_then(Value::as_str)
            .unwrap_or("none");
        let (repeat_kind, interval_seconds, cron_expression) = match repeat {
            "interval" => (
                Some("interval".to_string()),
                node.config
                    .get("interval_seconds")
                    .and_then(Value::as_i64)
                    .map(|value| value as i32),
                None,
            ),
            "cron" => (
                Some("cron".to_string()),
                None,
                node.config
                    .get("cron_expression")
                    .or_else(|| node.config.get("cron"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            _ => continue,
        };
        if repeat_kind.is_none() {
            continue;
        }
        let zone = node
            .config
            .get("timezone")
            .and_then(Value::as_str)
            .unwrap_or("UTC");
        let next = obolargus_core::automation::triggers::next_occurrence(
            repeat_kind.as_deref().unwrap_or("interval"),
            interval_seconds.map(|value| value.max(0) as u64),
            cron_expression.as_deref(),
            zone,
            Utc::now(),
        )
        .unwrap_or(None);
        let Some(next) = next else {
            continue;
        };
        let _ = persistence::insert_inject_trigger(
            pool,
            flow_id,
            version.id,
            &node.id,
            repeat_kind.as_deref().unwrap_or("interval"),
            interval_seconds,
            cron_expression.as_deref(),
            zone,
            next,
            &state.config.automation_actor_id,
        )
        .await;
    }
}

/// `GET /api/automation/flows/{flow_id}/versions`
#[utoipa::path(
    get,
    path = "/api/automation/flows/{flow_id}/versions",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id"), Pagination),
    responses((status = 200, body = Page<VersionSummary>))
)]
pub async fn list_versions(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> ApiResult<Page<VersionSummary>> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let (limit, offset) = pagination.bounds();
    let versions = persistence::list_versions(pool, flow_id, limit, offset)
        .await
        .map_err(map_error)?;
    let total = persistence::count_versions(pool, flow_id)
        .await
        .map_err(map_error)?;
    let items = versions.into_iter().map(VersionSummary::from).collect();
    Ok(Json(Page::new(items, total, limit, offset)))
}

/// `GET /api/automation/flows/{flow_id}/versions/{version_number}`
#[utoipa::path(
    get,
    path = "/api/automation/flows/{flow_id}/versions/{version_number}",
    tag = "automation",
    params(
        ("flow_id" = Uuid, Path, description = "Flow id"),
        ("version_number" = i32, Path, description = "Version number")
    ),
    responses((status = 200, body = VersionDetail))
)]
pub async fn get_version(
    State(state): State<AppState>,
    Path((flow_id, version_number)): Path<(Uuid, i32)>,
) -> ApiResult<VersionDetail> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let version = persistence::get_version(pool, flow_id, version_number)
        .await
        .map_err(map_error)?;
    let report = match serde_json::from_value::<FlowDocument>(version.flow_json.clone()) {
        Ok(document) => validate_flow(&document, &state.automation.registry),
        Err(_) => Default::default(),
    };
    Ok(Json(VersionDetail {
        id: version.id,
        version_number: version.version_number.unwrap_or_default(),
        status: version.status,
        change_note: version.change_note,
        published_by: version.published_by,
        published_at: version.published_at,
        flow_json: version.flow_json,
        validation: super::flows::validation_view_for(&report),
    }))
}

/// `POST /api/automation/flows/{flow_id}/versions/{version_number}/archive`
#[utoipa::path(
    post,
    path = "/api/automation/flows/{flow_id}/versions/{version_number}/archive",
    tag = "automation",
    params(
        ("flow_id" = Uuid, Path, description = "Flow id"),
        ("version_number" = i32, Path, description = "Version number")
    ),
    responses((status = 200, body = VersionSummary))
)]
pub async fn archive_version(
    State(state): State<AppState>,
    Path((flow_id, version_number)): Path<(Uuid, i32)>,
) -> ApiResult<VersionSummary> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let version = persistence::archive_version(pool, flow_id, version_number)
        .await
        .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "version.archive",
        "version",
        version.id,
        Some(flow_id),
        None,
        json!({ "version_number": version_number }),
    )
    .await;
    Ok(Json(VersionSummary::from(version)))
}

/// `GET /api/automation/flows/{flow_id}/versions/{version_number}/export`
#[utoipa::path(
    get,
    path = "/api/automation/flows/{flow_id}/versions/{version_number}/export",
    tag = "automation",
    params(
        ("flow_id" = Uuid, Path, description = "Flow id"),
        ("version_number" = i32, Path, description = "Version number")
    ),
    responses((status = 200, body = Value))
)]
pub async fn export_version(
    State(state): State<AppState>,
    Path((flow_id, version_number)): Path<(Uuid, i32)>,
) -> Result<Response, super::ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let version = persistence::get_version(pool, flow_id, version_number)
        .await
        .map_err(map_error)?;
    let body = serde_json::to_vec_pretty(&version.flow_json).unwrap_or_default();
    let (flow, _) = persistence::get_flow(pool, flow_id)
        .await
        .map_err(map_error)?;
    let filename = format!("{}-v{}.json", sanitize_filename(&flow.name), version_number);
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    if let Ok(disposition) = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
    {
        response
            .headers_mut()
            .insert(header::CONTENT_DISPOSITION, disposition);
    }
    Ok(response)
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

/// Body of `POST /api/automation/flows/{flow_id}/import`.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct ImportRequest {
    /// Flow document to import.
    pub flow_json: Value,
    /// Replace an existing draft.
    #[serde(default)]
    pub replace_draft: bool,
}

/// Unknown node type report entry.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct UnknownNodeType {
    /// Type id.
    #[serde(rename = "type")]
    pub node_type: String,
    /// Version.
    pub version: u32,
    /// Occurrence count.
    pub count: usize,
}

/// Import response.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ImportResponse {
    /// New revision.
    pub revision: i64,
    /// Imported document.
    pub flow_json: Value,
    /// Validation result.
    pub validation: ValidationView,
    /// Omitted unknown node types.
    pub unknown_node_types: Vec<UnknownNodeType>,
}

/// `POST /api/automation/flows/{flow_id}/import`
#[utoipa::path(
    post,
    path = "/api/automation/flows/{flow_id}/import",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    request_body = ImportRequest,
    responses(
        (status = 200, body = ImportResponse),
        (status = 409, body = super::ApiErrorBody),
        (status = 422, body = super::ApiErrorBody)
    )
)]
pub async fn import_flow(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    Json(body): Json<ImportRequest>,
) -> ApiResult<ImportResponse> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let (_, existing_draft) = persistence::get_flow(pool, flow_id)
        .await
        .map_err(map_error)?;
    if existing_draft.is_some() && !body.replace_draft {
        return Err(api_error(StatusCode::CONFLICT, "conflict", "draft.exists"));
    }
    let size = serde_json::to_vec(&body.flow_json)
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    if size > state.config.automation_max_flow_bytes {
        return Err(api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "flow.definition_too_large",
        ));
    }
    let mut document: FlowDocument = serde_json::from_value(body.flow_json).map_err(|error| {
        api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_flow",
            format!("flow.schema_invalid: {error}"),
        )
    })?;
    if document.schema_version != obolargus_core::automation::flow::document::FLOW_SCHEMA_VERSION {
        return Err(api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_flow",
            "flow.unknown_version",
        ));
    }

    let mut unknown: Vec<UnknownNodeType> = Vec::new();
    let mut kept_nodes = Vec::new();
    for node in document.nodes.drain(..) {
        if state
            .automation
            .registry
            .get(&node.node_type_key())
            .is_some()
        {
            kept_nodes.push(node);
        } else {
            match unknown.iter_mut().find(|entry| {
                entry.node_type == node.node_type && entry.version == node.type_version
            }) {
                Some(entry) => entry.count += 1,
                None => unknown.push(UnknownNodeType {
                    node_type: node.node_type.clone(),
                    version: node.type_version,
                    count: 1,
                }),
            }
        }
    }
    document.nodes = kept_nodes;
    let kept: std::collections::BTreeSet<String> =
        document.nodes.iter().map(|node| node.id.clone()).collect();
    document
        .wires
        .retain(|wire| kept.contains(&wire.source) && kept.contains(&wire.target));
    if document.nodes.is_empty() && !unknown.is_empty() {
        return Err(api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_flow",
            "flow.unknown_type: no supported nodes remain",
        ));
    }
    let report = validate_flow(&document, &state.automation.registry);
    let flow_json = serde_json::to_value(&document).unwrap_or(Value::Null);
    let saved = persistence::upsert_draft(
        pool,
        flow_id,
        None,
        &flow_json,
        &state.config.automation_actor_id,
    )
    .await
    .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "flow.import",
        "flow",
        flow_id,
        Some(flow_id),
        None,
        json!({ "unknown_node_types": unknown }),
    )
    .await;
    Ok(Json(ImportResponse {
        revision: saved.revision,
        flow_json: saved.version.flow_json,
        validation: super::flows::validation_view_for(&report),
        unknown_node_types: unknown,
    }))
}
