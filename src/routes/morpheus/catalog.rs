//! Tool catalog route.

use axum::Json;
use axum::extract::State;
use serde::Serialize;
use utoipa::ToSchema;

use super::{ApiResult, require_enabled};
use crate::AppState;

/// One catalog item.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ToolItem {
    /// Tool id.
    pub id: String,
    /// Version.
    pub version: i32,
    /// Human label.
    pub display_name: String,
    /// Description.
    pub description: String,
    /// Risk class.
    pub risk: String,
}

/// Tools response.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ToolsResponse {
    /// Catalog fingerprint.
    pub fingerprint: String,
    /// Descriptors.
    pub items: Vec<ToolItem>,
}

/// Lists the active catalog without handlers.
#[utoipa::path(
    get,
    path = "/api/morpheus/tools",
    responses((status = 200, body = ToolsResponse), (status = 503, body = super::ErrorEnvelope)),
    tag = "morpheus"
)]
pub async fn list_tools(State(state): State<AppState>) -> ApiResult<ToolsResponse> {
    require_enabled(&state)?;
    let catalog = &state.morpheus.catalog;
    Ok(Json(ToolsResponse {
        fingerprint: catalog.fingerprint(),
        items: catalog
            .specs()
            .iter()
            .map(|spec| ToolItem {
                id: spec.id.to_string(),
                version: spec.version,
                display_name: spec.display_name.to_string(),
                description: spec.description.to_string(),
                risk: spec.risk.as_str().to_string(),
            })
            .collect(),
    }))
}
