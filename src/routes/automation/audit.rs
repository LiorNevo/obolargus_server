//! Audit event route.

use axum::Json;
use axum::extract::{Path, Query, State};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use obolargus_core::automation::model::AuditEventRecord;
use obolargus_core::automation::runtime::persistence;

use super::{ApiResult, Page, Pagination, map_error, require_enabled, require_pool};
use crate::AppState;

/// Audit list query.
#[derive(Debug, Clone, Deserialize, utoipa::IntoParams)]
pub struct AuditQuery {
    /// Action filter.
    #[serde(default)]
    pub action: Option<String>,
    /// Actor filter.
    #[serde(default)]
    pub actor_id: Option<String>,
    /// Start instant.
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    /// End instant.
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
    /// Page size.
    #[serde(default)]
    pub limit: Option<i64>,
    /// Offset.
    #[serde(default)]
    pub offset: Option<i64>,
}

/// Audit event view.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct AuditEventView {
    /// Event id.
    pub id: Uuid,
    /// Actor.
    pub actor_id: Option<String>,
    /// Action.
    pub action: String,
    /// Target type.
    pub target_type: String,
    /// Target id.
    pub target_id: Uuid,
    /// Outcome.
    pub outcome: String,
    /// Metadata.
    pub metadata: Value,
    /// Creation instant.
    pub created_at: DateTime<Utc>,
}

impl From<AuditEventRecord> for AuditEventView {
    fn from(value: AuditEventRecord) -> Self {
        Self {
            id: value.id,
            actor_id: value.actor_id,
            action: value.action,
            target_type: value.target_type,
            target_id: value.target_id,
            outcome: value.outcome,
            metadata: value.metadata,
            created_at: value.created_at,
        }
    }
}

/// `GET /api/automation/flows/{flow_id}/audit`
#[utoipa::path(
    get,
    path = "/api/automation/flows/{flow_id}/audit",
    tag = "automation",
    params(("flow_id" = Uuid, Path, description = "Flow id"), AuditQuery),
    responses((status = 200, body = Page<AuditEventView>))
)]
pub async fn list_audit(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    Query(query): Query<AuditQuery>,
) -> ApiResult<Page<AuditEventView>> {
    require_enabled(&state)?;
    let pool = require_pool(&state)?;
    let (limit, offset) = Pagination {
        limit: query.limit,
        offset: query.offset,
    }
    .bounds();
    let events = persistence::list_audit(
        pool,
        flow_id,
        query.action.as_deref(),
        query.actor_id.as_deref(),
        query.from,
        query.to,
        limit,
        offset,
    )
    .await
    .map_err(map_error)?;
    let total = persistence::count_audit(
        pool,
        flow_id,
        query.action.as_deref(),
        query.actor_id.as_deref(),
        query.from,
        query.to,
    )
    .await
    .map_err(map_error)?;
    let items: Vec<AuditEventView> = events.into_iter().map(AuditEventView::from).collect();
    Ok(Json(Page::new(items, total, limit, offset)))
}
