//! Flow CRUD, draft, validation, and context routes.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use obolargus_core::automation::flow::document::FlowDocument;
use obolargus_core::automation::flow::validate::validate_flow;
use obolargus_core::automation::model::{FlowRecord, FlowVersionRecord};
use obolargus_core::automation::runtime::persistence;
use obolargus_core::automation::templates;

use super::{ApiResult, Page, Pagination, api_error, map_error, require_enabled, require_pool};
use crate::AppState;
use crate::automation::audit;

/// Last-run projection in a flow summary.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct LastRunView {
    /// Run id.
    pub id: Uuid,
    /// Status.
    pub status: String,
    /// End instant.
    pub ended_at: Option<DateTime<Utc>>,
}

/// Flow list item.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct FlowSummary {
    /// Flow id.
    pub id: Uuid,
    /// Display name.
    pub name: String,
    /// Description.
    pub description: String,
    /// Tags.
    pub tags: Vec<String>,
    /// Lifecycle status.
    pub status: String,
    /// Owner metadata.
    pub owner_id: String,
    /// Current published version number.
    pub current_version_number: Option<i32>,
    /// Optimistic concurrency token.
    pub draft_revision: i64,
    /// Most recent run.
    pub last_run: Option<LastRunView>,
    /// Next scheduled occurrence.
    pub next_scheduled_run_at: Option<DateTime<Utc>>,
    /// Last update instant.
    pub updated_at: DateTime<Utc>,
}

/// Draft projection in flow detail.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DraftView {
    /// Revision token.
    pub revision: i64,
    /// Canonical flow document.
    pub flow_json: Value,
    /// Last update instant.
    pub updated_at: DateTime<Utc>,
}

/// Current-version projection.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct CurrentVersionView {
    /// Version id.
    pub id: Uuid,
    /// Version number.
    pub version_number: i32,
    /// Publication instant.
    pub published_at: Option<DateTime<Utc>>,
    /// Change note.
    pub change_note: Option<String>,
}

/// Flow detail.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct FlowDetail {
    /// Flow id.
    pub id: Uuid,
    /// Display name.
    pub name: String,
    /// Description.
    pub description: String,
    /// Tags.
    pub tags: Vec<String>,
    /// Lifecycle status.
    pub status: String,
    /// Owner metadata.
    pub owner_id: String,
    /// Editable draft, if any.
    pub draft: Option<DraftView>,
    /// Current published version.
    pub current_version: Option<CurrentVersionView>,
    /// Creation instant.
    pub created_at: DateTime<Utc>,
    /// Last update instant.
    pub updated_at: DateTime<Utc>,
}

/// Query for `GET /api/automation/flows`.
#[derive(Debug, Clone, Deserialize, utoipa::IntoParams)]
pub struct FlowListQuery {
    /// Lifecycle status.
    #[serde(default)]
    pub status: Option<String>,
    /// Name/description search.
    #[serde(default)]
    pub search: Option<String>,
    /// Page size.
    #[serde(default)]
    pub limit: Option<i64>,
    /// Offset.
    #[serde(default)]
    pub offset: Option<i64>,
}

/// Body of `POST /api/automation/flows`.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct CreateFlowRequest {
    /// Display name.
    pub name: String,
    /// Description.
    #[serde(default)]
    pub description: String,
    /// Tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Optional template id.
    #[serde(default)]
    pub template_id: Option<String>,
}

/// Body of `PATCH /api/automation/flows/{flow_id}`.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct UpdateFlowRequest {
    /// New name.
    #[serde(default)]
    pub name: Option<String>,
    /// New description.
    #[serde(default)]
    pub description: Option<String>,
    /// New tags.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
}

async fn flow_detail(
    pool: &sqlx::PgPool,
    flow: FlowRecord,
    draft: Option<FlowVersionRecord>,
) -> Result<FlowDetail, super::ApiError> {
    let current_version = match flow.current_published_version_id {
        Some(id) => persistence::get_version_by_id(pool, id)
            .await
            .map_err(map_error)?,
        None => None,
    };
    Ok(FlowDetail {
        id: flow.id,
        name: flow.name,
        description: flow.description,
        tags: flow.tags,
        status: flow.status,
        owner_id: flow.owner_id,
        draft: draft.map(|draft| DraftView {
            revision: flow.draft_revision,
            flow_json: draft.flow_json,
            updated_at: draft.created_at,
        }),
        current_version: current_version.and_then(|version| {
            version.version_number.map(|number| CurrentVersionView {
                id: version.id,
                version_number: number,
                published_at: version.published_at,
                change_note: version.change_note,
            })
        }),
        created_at: flow.created_at,
        updated_at: flow.updated_at,
    })
}

/// `GET /api/automation/flows`
#[utoipa::path(
    get,
    path = "/api/automation/flows",
    tag = "automation",
    params(FlowListQuery),
    responses((status = 200, body = Page<FlowSummary>))
)]
pub async fn list_flows(
    State(state): State<AppState>,
    Query(query): Query<FlowListQuery>,
) -> ApiResult<Page<FlowSummary>> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let (limit, offset) = Pagination {
        limit: query.limit,
        offset: query.offset,
    }
    .bounds();
    let flows = persistence::list_flows(
        pool,
        query.status.as_deref(),
        query.search.as_deref(),
        limit,
        offset,
    )
    .await
    .map_err(map_error)?;
    let total = persistence::count_flows(pool, query.status.as_deref(), query.search.as_deref())
        .await
        .map_err(map_error)?;
    let mut items = Vec::with_capacity(flows.len());
    for flow in flows {
        let current_version_number = match flow.current_published_version_id {
            Some(id) => persistence::get_version_by_id(pool, id)
                .await
                .map_err(map_error)?
                .and_then(|version| version.version_number),
            None => None,
        };
        let last_run = persistence::latest_run(pool, flow.id)
            .await
            .map_err(map_error)?
            .map(|run| LastRunView {
                id: run.id,
                status: run.status,
                ended_at: run.ended_at,
            });
        let next_scheduled_run_at = persistence::next_scheduled_run(pool, flow.id)
            .await
            .map_err(map_error)?;
        items.push(FlowSummary {
            id: flow.id,
            name: flow.name,
            description: flow.description,
            tags: flow.tags,
            status: flow.status,
            owner_id: flow.owner_id,
            current_version_number,
            draft_revision: flow.draft_revision,
            last_run,
            next_scheduled_run_at,
            updated_at: flow.updated_at,
        });
    }
    Ok(Json(Page::new(items, total, limit, offset)))
}

/// `POST /api/automation/flows`
#[utoipa::path(
    post,
    path = "/api/automation/flows",
    tag = "automation",
    request_body = CreateFlowRequest,
    responses(
        (status = 201, body = FlowDetail),
        (status = 400, body = super::ApiErrorBody),
        (status = 409, body = super::ApiErrorBody)
    )
)]
pub async fn create_flow(
    State(state): State<AppState>,
    Json(body): Json<CreateFlowRequest>,
) -> Result<(StatusCode, Json<FlowDetail>), super::ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let name = body.name.trim();
    if name.is_empty() || name.chars().count() > 200 {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "flow.name_empty: name must be 1-200 characters",
        ));
    }
    if body.description.chars().count() > 4_000 {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "flow.description_too_long",
        ));
    }
    if body.tags.iter().any(|tag| tag.chars().count() > 50) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "flow.tag_too_long",
        ));
    }
    let template = match body.template_id.as_deref() {
        Some(id) => Some(templates::get_template(id).ok_or_else(|| {
            api_error(StatusCode::NOT_FOUND, "not_found", format!("template {id}"))
        })?),
        None => None,
    };
    let flow_json = match template {
        Some(template) => {
            let mut document: FlowDocument =
                serde_json::from_value(template.flow_json).map_err(|error| {
                    api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_error",
                        error.to_string(),
                    )
                })?;
            document.name = name.to_string();
            document.description = body.description.clone();
            document.tags = body.tags.clone();
            serde_json::to_value(document).map_err(|error| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    error.to_string(),
                )
            })?
        }
        None => serde_json::to_value(FlowDocument::empty(name)).unwrap_or_else(|_| json!({})),
    };
    let flow = persistence::create_flow(
        pool,
        &state.config.automation_actor_id,
        name,
        &body.description,
        &body.tags,
        &flow_json,
    )
    .await
    .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "flow.create",
        "flow",
        flow.id,
        Some(flow.id),
        None,
        json!({ "name": flow.name }),
    )
    .await;
    let (flow, draft) = persistence::get_flow(pool, flow.id)
        .await
        .map_err(map_error)?;
    let detail = flow_detail(pool, flow, draft).await?;
    Ok((StatusCode::CREATED, Json(detail)))
}

/// `GET /api/automation/flows/{flow_id}`
#[utoipa::path(
    get,
    path = "/api/automation/flows/{flow_id}",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    responses(
        (status = 200, body = FlowDetail),
        (status = 404, body = super::ApiErrorBody)
    )
)]
pub async fn get_flow(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
) -> ApiResult<FlowDetail> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let (flow, draft) = persistence::get_flow(pool, flow_id)
        .await
        .map_err(map_error)?;
    Ok(Json(flow_detail(pool, flow, draft).await?))
}

/// `PATCH /api/automation/flows/{flow_id}`
#[utoipa::path(
    patch,
    path = "/api/automation/flows/{flow_id}",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    request_body = UpdateFlowRequest,
    responses(
        (status = 200, body = FlowDetail),
        (status = 409, body = super::ApiErrorBody)
    )
)]
pub async fn update_flow(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    Json(body): Json<UpdateFlowRequest>,
) -> ApiResult<FlowDetail> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    if let Some(name) = body.name.as_deref()
        && (name.trim().is_empty() || name.chars().count() > 200)
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "flow.name_empty",
        ));
    }
    let flow = persistence::update_flow_metadata(
        pool,
        flow_id,
        body.name.as_deref(),
        body.description.as_deref(),
        body.tags.as_deref(),
    )
    .await
    .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "flow.update",
        "flow",
        flow_id,
        Some(flow_id),
        None,
        json!({}),
    )
    .await;
    let (flow, draft) = persistence::get_flow(pool, flow.id)
        .await
        .map_err(map_error)?;
    Ok(Json(flow_detail(pool, flow, draft).await?))
}

async fn set_status(state: AppState, flow_id: Uuid, status: &str) -> ApiResult<FlowDetail> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let flow = persistence::set_flow_status(pool, flow_id, status)
        .await
        .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        &format!("flow.{status}"),
        "flow",
        flow_id,
        Some(flow_id),
        None,
        json!({}),
    )
    .await;
    let (flow, draft) = persistence::get_flow(pool, flow.id)
        .await
        .map_err(map_error)?;
    Ok(Json(flow_detail(pool, flow, draft).await?))
}

/// `POST /api/automation/flows/{flow_id}/archive`
#[utoipa::path(
    post,
    path = "/api/automation/flows/{flow_id}/archive",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    responses((status = 200, body = FlowDetail))
)]
pub async fn archive_flow(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
) -> ApiResult<FlowDetail> {
    set_status(state, flow_id, "archived").await
}

/// `POST /api/automation/flows/{flow_id}/restore`
#[utoipa::path(
    post,
    path = "/api/automation/flows/{flow_id}/restore",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    responses((status = 200, body = FlowDetail))
)]
pub async fn restore_flow(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
) -> ApiResult<FlowDetail> {
    set_status(state, flow_id, "active").await
}

/// `DELETE /api/automation/flows/{flow_id}`
#[utoipa::path(
    delete,
    path = "/api/automation/flows/{flow_id}",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    responses((status = 204), (status = 404, body = super::ApiErrorBody))
)]
pub async fn delete_flow(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
) -> Result<StatusCode, super::ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::soft_delete_flow(pool, flow_id)
        .await
        .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "flow.delete",
        "flow",
        flow_id,
        Some(flow_id),
        None,
        json!({}),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

/// Body of `PUT /api/automation/flows/{flow_id}/draft`.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct SaveDraftRequest {
    /// Expected current revision, when the client has one.
    #[serde(default)]
    pub expected_revision: Option<i64>,
    /// Canonical flow document.
    pub flow_json: Value,
}

/// Validation projection.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ValidationView {
    /// Blocking errors.
    pub errors: Vec<Value>,
    /// Non-blocking warnings.
    pub warnings: Vec<Value>,
}

/// Draft save response.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DraftResponse {
    /// New revision.
    pub revision: i64,
    /// Stored flow document.
    pub flow_json: Value,
    /// Validation result.
    pub validation: ValidationView,
    /// Last update instant.
    pub updated_at: DateTime<Utc>,
}

/// Converts a core validation report to the HTTP projection.
pub fn validation_view_for(
    report: &obolargus_core::automation::error::ValidationReport,
) -> ValidationView {
    ValidationView {
        errors: report
            .errors
            .iter()
            .filter_map(|finding| serde_json::to_value(finding).ok())
            .collect(),
        warnings: report
            .warnings
            .iter()
            .filter_map(|finding| serde_json::to_value(finding).ok())
            .collect(),
    }
}

fn check_size(state: &AppState, value: &Value) -> Result<(), super::ApiError> {
    let size = serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    if size > state.config.automation_max_flow_bytes {
        return Err(api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "flow.definition_too_large",
        ));
    }
    Ok(())
}

/// `PUT /api/automation/flows/{flow_id}/draft`
#[utoipa::path(
    put,
    path = "/api/automation/flows/{flow_id}/draft",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    request_body = SaveDraftRequest,
    responses(
        (status = 200, body = DraftResponse),
        (status = 409, body = super::ApiErrorBody),
        (status = 413, body = super::ApiErrorBody)
    )
)]
pub async fn save_draft(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    Json(body): Json<SaveDraftRequest>,
) -> ApiResult<DraftResponse> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    check_size(&state, &body.flow_json)?;
    let document: FlowDocument =
        serde_json::from_value(body.flow_json.clone()).map_err(|error| {
            api_error(
                StatusCode::BAD_REQUEST,
                "validation_error",
                format!("flow.json_invalid: {error}"),
            )
        })?;
    let report = validate_flow(&document, &state.automation.registry);
    let saved = persistence::upsert_draft(
        pool,
        flow_id,
        body.expected_revision,
        &body.flow_json,
        &state.config.automation_actor_id,
    )
    .await
    .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "flow.draft.save",
        "flow",
        flow_id,
        Some(flow_id),
        None,
        json!({ "revision": saved.revision }),
    )
    .await;
    Ok(Json(DraftResponse {
        revision: saved.revision,
        flow_json: saved.version.flow_json,
        validation: validation_view_for(&report),
        updated_at: saved.version.created_at,
    }))
}

/// Body of `POST /api/automation/flows/{flow_id}/validate`.
#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
pub struct ValidateRequest {
    /// Document to validate; defaults to the stored draft.
    #[serde(default)]
    pub flow_json: Option<Value>,
}

/// Validation response.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ValidateResponse {
    /// True when there are no errors.
    pub valid: bool,
    /// Blocking errors.
    pub errors: Vec<Value>,
    /// Non-blocking warnings.
    pub warnings: Vec<Value>,
}

/// `POST /api/automation/flows/{flow_id}/validate`
#[utoipa::path(
    post,
    path = "/api/automation/flows/{flow_id}/validate",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    request_body = ValidateRequest,
    responses((status = 200, body = ValidateResponse))
)]
pub async fn validate(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    Json(body): Json<ValidateRequest>,
) -> ApiResult<ValidateResponse> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let value = match body.flow_json {
        Some(value) => value,
        None => {
            let (_, draft) = persistence::get_flow(pool, flow_id)
                .await
                .map_err(map_error)?;
            draft
                .map(|draft| draft.flow_json)
                .ok_or_else(|| api_error(StatusCode::CONFLICT, "conflict", "draft.none"))?
        }
    };
    let document: FlowDocument = serde_json::from_value(value).map_err(|error| {
        api_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("flow.json_invalid: {error}"),
        )
    })?;
    let report = validate_flow(&document, &state.automation.registry);
    Ok(Json(ValidateResponse {
        valid: report.is_valid(),
        errors: report
            .errors
            .iter()
            .filter_map(|finding| serde_json::to_value(finding).ok())
            .collect(),
        warnings: report
            .warnings
            .iter()
            .filter_map(|finding| serde_json::to_value(finding).ok())
            .collect(),
    }))
}

/// Context response.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ContextView {
    /// Flow-scoped context.
    pub flow: Value,
    /// Global context.
    pub global: Value,
}

/// `GET /api/automation/flows/{flow_id}/context`
#[utoipa::path(
    get,
    path = "/api/automation/flows/{flow_id}/context",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    responses((status = 200, body = ContextView))
)]
pub async fn get_context(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
) -> ApiResult<ContextView> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let (flow_rows, global_rows) =
        persistence::get_context(pool, &flow_id.to_string(), "workspace")
            .await
            .map_err(map_error)?;
    let to_map = |rows: Vec<obolargus_core::automation::model::ContextRecord>| {
        let mut map = serde_json::Map::new();
        for row in rows {
            map.insert(row.key, row.value);
        }
        Value::Object(map)
    };
    Ok(Json(ContextView {
        flow: to_map(flow_rows),
        global: to_map(global_rows),
    }))
}

/// `DELETE /api/automation/flows/{flow_id}/context`
#[utoipa::path(
    delete,
    path = "/api/automation/flows/{flow_id}/context",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id")),
    responses((status = 204))
)]
pub async fn delete_context(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
) -> Result<StatusCode, super::ApiError> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    persistence::delete_context(pool, "flow", &flow_id.to_string())
        .await
        .map_err(map_error)?;
    audit::record_best_effort(
        &state,
        "context.delete",
        "flow",
        flow_id,
        Some(flow_id),
        None,
        json!({}),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}
