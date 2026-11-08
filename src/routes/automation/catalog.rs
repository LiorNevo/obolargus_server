//! Node-type and template catalog routes.

use axum::Json;
use axum::extract::{Path, State};

use obolargus_core::automation::templates;

use super::{ApiResult, api_error, require_enabled};
use crate::AppState;

/// Response of `GET /api/automation/node-types`.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct NodeTypeList {
    /// Executable node types.
    pub items: Vec<serde_json::Value>,
}

/// `GET /api/automation/node-types`
#[utoipa::path(
    get,
    path = "/api/automation/node-types",
    tag = "automation",
    responses((status = 200, body = NodeTypeList))
)]
pub async fn list_node_types(State(state): State<AppState>) -> ApiResult<NodeTypeList> {
    require_enabled(&state)?;
    let mut items = state.automation.registry.latest_descriptors();
    items.sort_by(|left, right| left.id.cmp(&right.id));
    let items = items
        .into_iter()
        .filter_map(|descriptor| serde_json::to_value(descriptor).ok())
        .collect();
    Ok(Json(NodeTypeList { items }))
}

/// Template summary returned by the list endpoint.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct TemplateSummary {
    /// Stable template id.
    pub id: String,
    /// Template version.
    pub version: u32,
    /// Display name.
    pub name: String,
    /// Description.
    pub description: String,
    /// Category.
    pub category: String,
    /// Number of nodes.
    pub node_count: usize,
}

/// Response of `GET /api/automation/templates`.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct TemplateList {
    /// Templates.
    pub items: Vec<TemplateSummary>,
}

/// Full template document.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct TemplateDetail {
    /// Stable template id.
    pub id: String,
    /// Template version.
    pub version: u32,
    /// Display name.
    pub name: String,
    /// Description.
    pub description: String,
    /// Category.
    pub category: String,
    /// Number of nodes.
    pub node_count: usize,
    /// Canonical flow document.
    pub flow_json: serde_json::Value,
    /// JSON Schema for the declared runtime inputs.
    pub inputs_schema: serde_json::Value,
}

/// `GET /api/automation/templates`
#[utoipa::path(
    get,
    path = "/api/automation/templates",
    tag = "automation",
    responses((status = 200, body = TemplateList))
)]
pub async fn list_templates(State(state): State<AppState>) -> ApiResult<TemplateList> {
    require_enabled(&state)?;
    let items = templates::templates()
        .into_iter()
        .map(|template| TemplateSummary {
            id: template.id,
            version: template.version,
            name: template.name,
            description: template.description,
            category: template.category,
            node_count: template.node_count,
        })
        .collect();
    Ok(Json(TemplateList { items }))
}

/// `GET /api/automation/templates/{template_id}`
#[utoipa::path(
    get,
    path = "/api/automation/templates/{template_id}",
    tag = "automation",
    params(("template_id" = String, Path, description = "Template id")),
    responses(
        (status = 200, body = TemplateDetail),
        (status = 404, body = super::ApiErrorBody)
    )
)]
pub async fn get_template(
    State(state): State<AppState>,
    Path(template_id): Path<String>,
) -> ApiResult<TemplateDetail> {
    require_enabled(&state)?;
    let Some(template) = templates::get_template(&template_id) else {
        return Err(api_error(
            axum::http::StatusCode::NOT_FOUND,
            "not_found",
            format!("template {template_id}"),
        ));
    };
    let inputs = template
        .flow_json
        .get("inputs")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for input in inputs {
        let Some(name) = input.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let mut schema = serde_json::json!({
            "type": input
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("string"),
        });
        if let Some(description) = input.get("description") {
            schema["description"] = description.clone();
        }
        if input.get("secret").and_then(serde_json::Value::as_bool) == Some(true) {
            schema["writeOnly"] = serde_json::Value::Bool(true);
        }
        if input.get("required").and_then(serde_json::Value::as_bool) == Some(true) {
            required.push(serde_json::Value::String(name.to_string()));
        }
        properties.insert(name.to_string(), schema);
    }
    let inputs_schema = serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required,
    });
    Ok(Json(TemplateDetail {
        id: template.id,
        version: template.version,
        name: template.name,
        description: template.description,
        category: template.category,
        node_count: template.node_count,
        flow_json: template.flow_json,
        inputs_schema,
    }))
}
