//! `GET /api/domain/schema` — the complete system data dictionary.
//!
//! Contract: `specs/028-data-dictionary-json-schema/contracts/export-surfaces.md`
//! §2. The endpoint reads no database and no environment state.

#![forbid(unsafe_code)]

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// The JSON error envelope for a failed schema export.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SchemaErrorBody {
    /// Stable machine-readable error class.
    pub error: &'static str,
    /// Operator-facing diagnostic.
    pub message: String,
}

/// Serves the canonical system data-dictionary schema.
///
/// The response body is exactly `SystemSchema::canonical_json()`. A build
/// failure maps to `500` with a JSON error envelope and never emits a partial
/// document.
#[utoipa::path(
    get,
    path = "/api/domain/schema",
    tag = "data-dictionary",
    summary = "Get the complete system data dictionary",
    description = "Returns the complete Obolargus data dictionary as one JSON \
                   Schema 2020-12 document. This is the authoritative \
                   persisted-data contract; use it to enumerate every entity, \
                   fact type, field, relationship, and invariant. Requires no \
                   database.",
    responses(
        (
            status = 200,
            description = "The canonical system data-dictionary schema",
            body = serde_json::Value,
            content_type = "application/schema+json"
        ),
        (
            status = 500,
            description = "The schema could not be generated",
            body = SchemaErrorBody,
            content_type = "application/json"
        )
    )
)]
pub async fn get_schema() -> Response {
    match obolargus_core::data_dictionary::export_system_schema() {
        Ok(schema) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "application/schema+json; charset=utf-8",
            )],
            schema.canonical_json().to_string(),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(SchemaErrorBody {
                error: "infrastructure",
                message: error.to_string(),
            }),
        )
            .into_response(),
    }
}
