//! HTTP handlers for asset search.
//!
//! Implements `GET /api/assets` from
//! `specs/023-dashboard-widgets/contracts/http-api-contract.md` §2. The handler
//! validates the request and delegates to
//! [`obolargus_core::domain::asset::search_asset_options`]; no query logic
//! lives here.

#![forbid(unsafe_code)]

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use obolargus_core::domain::asset::entity::{AssetOption, AssetSearchQuery};
use obolargus_core::domain::asset::search_asset_options;
use obolargus_core::domain::error::DomainError;

use crate::AppState;

/// One asset-search result.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AssetOptionBody {
    /// The asset id.
    pub id: Uuid,
    /// The human-readable display name.
    pub display_name: String,
    /// The asset type (e.g. `stock`).
    pub asset_type: String,
    /// The ISO 4217 currency code.
    pub currency: String,
    /// The preferred ticker or the oldest identifier.
    pub primary_identifier: Option<String>,
}

impl AssetOptionBody {
    fn from_option(option: AssetOption) -> Self {
        Self {
            id: option.id,
            display_name: option.display_name,
            asset_type: option.asset_type,
            currency: option.currency,
            primary_identifier: option.primary_identifier,
        }
    }
}

/// The asset-search envelope.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AssetSearchResponse {
    /// Matching assets, deterministically ordered.
    pub items: Vec<AssetOptionBody>,
}

/// The shared error envelope.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AssetErrorEnvelope {
    /// The wrapped error.
    pub error: AssetErrorBody,
}

/// The error code and message.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AssetErrorBody {
    /// Stable machine code.
    pub code: String,
    /// Human-readable message.
    pub message: String,
}

/// Query for the asset search.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct AssetSearchParams {
    /// Free-text search over display name and identifier values.
    pub q: Option<String>,
    /// Maximum results, 1..=50; defaults to 20.
    pub limit: Option<u32>,
}

fn error_response(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(AssetErrorEnvelope {
            error: AssetErrorBody {
                code: code.to_string(),
                message: message.into(),
            },
        }),
    )
        .into_response()
}

fn domain_error_response(error: &DomainError) -> Response {
    match error {
        DomainError::Validation(message) => {
            error_response(StatusCode::BAD_REQUEST, "invalid_query", message)
        }
        DomainError::Infrastructure(_) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "infrastructure",
            "The asset search could not be completed.",
        ),
        _ => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "infrastructure",
            error.to_string(),
        ),
    }
}

/// `GET /api/assets` — search assets for the instrument picker.
#[utoipa::path(
    get,
    path = "/api/assets",
    tag = "domain",
    summary = "Search assets by name or identifier",
    params(AssetSearchParams),
    responses(
        (status = 200, description = "Matching assets", body = AssetSearchResponse),
        (status = 400, description = "invalid_query or invalid_limit", body = AssetErrorEnvelope),
        (status = 500, description = "infrastructure", body = AssetErrorEnvelope)
    )
)]
pub async fn search_assets(
    State(state): State<AppState>,
    Query(params): Query<AssetSearchParams>,
) -> Response {
    if params
        .q
        .as_ref()
        .is_some_and(|query| query.chars().count() > 200)
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_query",
            "The search query must be at most 200 characters.",
        );
    }
    let limit = params.limit.unwrap_or(20);
    if !(1..=50).contains(&limit) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_limit",
            "limit must be between 1 and 50.",
        );
    }
    let Some(pool) = state.pool.as_ref() else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "infrastructure",
            "The asset search is unavailable without a database.",
        );
    };
    let query = AssetSearchQuery { q: params.q, limit };
    match search_asset_options(pool, query).await {
        Ok(options) => Json(AssetSearchResponse {
            items: options
                .into_iter()
                .map(AssetOptionBody::from_option)
                .collect(),
        })
        .into_response(),
        Err(error) => domain_error_response(&error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obolargus_core::domain::error::DomainError;

    #[test]
    fn maps_an_asset_option_to_the_wire_body() {
        let body = AssetOptionBody::from_option(AssetOption {
            id: Uuid::from_u128(1),
            display_name: "Example".to_string(),
            asset_type: "stock".to_string(),
            currency: "USD".to_string(),
            primary_identifier: Some("EX".to_string()),
        });
        assert_eq!(body.display_name, "Example");
        assert_eq!(body.primary_identifier.as_deref(), Some("EX"));
    }

    #[tokio::test]
    async fn maps_domain_errors_to_contract_statuses() {
        let validation = domain_error_response(&DomainError::Validation("bad".into()));
        assert_eq!(validation.status(), StatusCode::BAD_REQUEST);

        let infrastructure =
            domain_error_response(&DomainError::Infrastructure(sqlx::Error::PoolClosed));
        assert_eq!(infrastructure.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let other = domain_error_response(&DomainError::not_found("Asset", "id"));
        assert_eq!(other.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
