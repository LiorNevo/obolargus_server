//! Interactive and machine-readable API documentation.
//!
//! The documentation is generated from the same [`utoipa`] annotations that
//! describe the API handlers; see [`crate::api_router`] and
//! [`crate::openapi_document`].
//!
//! All documentation URLs are deliberately **mount-relative**. The gateway
//! serves the Swagger UI and the specification under
//! `/obolargus/api/swagger-ui/` and `/obolargus/api/api-docs/`, stripping the
//! full `/obolargus/api` mount for those two surfaces. Business API calls use
//! `/obolargus/api/{*rest}` and strip only `/obolargus`, so the upstream sees
//! `/api/{*rest}`. The UI therefore fetches the document through
//! `../api-docs/openapi.json`, and the document declares
//! `servers: [{ "url": "../.." }]`; the extra level cancels the UI page's
//! `swagger-ui/` segment and leaves `/obolargus/api/...` through the gateway
//! or `/api/...` when the server is accessed directly.

use axum::Router;
use axum::response::Redirect;
use axum::routing::get;
use serde_json::{Map, Value, json};
use utoipa::openapi::OpenApi;
use utoipa_swagger_ui::{Config, SwaggerUi, Url};

use crate::AppState;

/// Mount point of the interactive Swagger UI.
///
/// The bare path redirects to [`SWAGGER_UI_INDEX`], which is the canonical
/// document base for relative asset and specification URLs.
pub const SWAGGER_UI_PATH: &str = "/swagger-ui";

/// Canonical trailing-slash URL of the interactive Swagger UI.
pub const SWAGGER_UI_INDEX: &str = "/swagger-ui/";

/// Stable URL of the machine-readable OpenAPI document.
pub const OPENAPI_DOC_PATH: &str = "/api-docs/openapi.json";

/// Specification URL used by the Swagger UI page.
///
/// Relative to the page at `/swagger-ui/`, this resolves to
/// `/api-docs/openapi.json` directly and to
/// `/obolargus/api/api-docs/openapi.json` through the gateway. Both map to
/// [`OPENAPI_DOC_PATH`] at the server.
const OPENAPI_UI_URL: &str = "../api-docs/openapi.json";

/// Mount-relative URL of the canonical system data-dictionary export.
///
/// The OpenAPI document description links to this URL. It resolves to
/// `/api/domain/schema` directly and to `/obolargus/api/domain/schema`
/// through the gateway, whose business-API route strips only `/obolargus`
/// and forwards the remaining `/api/domain/schema` to the server.
pub const DATA_DICTIONARY_URL: &str = "../../api/domain/schema";

/// Prefix that separates data-dictionary definitions from transport schemas in
/// the OpenAPI `components.schemas` map.
///
/// The prefix keeps the persisted-data definitions visibly distinct from the
/// request/response DTOs while remaining a valid OpenAPI component name.
pub const DATA_DICTIONARY_SCHEMA_PREFIX: &str = "DataDictionary.";

/// Builds the documentation and navigation routes.
///
/// Routes added when the caller merges this into the application router:
/// - `GET /` → `307 Temporary Redirect` to [`SWAGGER_UI_INDEX`].
/// - `GET /swagger-ui` → `303 See Other` to [`SWAGGER_UI_INDEX`] (provided by
///   the Swagger UI library).
/// - `GET /swagger-ui/` and its relative assets → the interactive page.
/// - `GET /api-docs/openapi.json` → the supplied OpenAPI document.
///
/// The returned router still requires [`AppState`] so the caller can merge it
/// before applying state and middleware, exactly like the API router.
pub fn swagger_routes(api: OpenApi) -> Router<AppState> {
    let config = Config::new([Url::new("Obolargus API", OPENAPI_UI_URL)])
        .try_it_out_enabled(true)
        .validator_url("none")
        .persist_authorization(false)
        // Data-dictionary components carry their semantics in `x-obolargus-*`
        // extensions; keeping them visible makes the dictionary usable from
        // the interactive page instead of only through the raw export.
        .show_extensions(true)
        .show_common_extensions(true);

    let swagger = SwaggerUi::new(SWAGGER_UI_PATH)
        .url(OPENAPI_DOC_PATH, api)
        .config(config);

    Router::<AppState>::new()
        .route("/", get(root_redirect))
        .merge(swagger)
}

/// Redirects direct server-root navigation to the documentation page.
///
/// A temporary redirect avoids pinning the documentation path in development
/// caches. Only `GET` is registered, so other methods against `/` are not
/// silently converted into documentation responses.
async fn root_redirect() -> Redirect {
    Redirect::temporary(SWAGGER_UI_INDEX)
}

/// Projects the canonical system data dictionary into an OpenAPI document so
/// it is browsable from Swagger UI.
///
/// Each `$defs` entry becomes an OpenAPI component named
/// `DataDictionary.<definition>`, and internal `#/$defs/...` references are
/// rewritten to the corresponding `#/components/schemas/...` target. The
/// document also receives a top-level `x-obolargus-data-dictionary` metadata
/// block, and its description links to the raw canonical export.
///
/// This is a projection for documentation, not a second source of truth: the
/// definitions come from [`obolargus_core::data_dictionary::export_system_schema`]
/// and the canonical document is left byte-for-byte unchanged. JSON Schema
/// `const` is rendered as a single-value `enum` because Swagger UI's schema
/// renderer does not model `const`; the two are semantically equivalent.
///
/// Generation failure is logged and leaves the transport-only document intact
/// rather than panicking the server or emitting a partial projection.
pub fn with_data_dictionary(mut document: OpenApi) -> OpenApi {
    use utoipa::openapi::RefOr;
    use utoipa::openapi::schema::Schema;

    let schema = match obolargus_core::data_dictionary::export_system_schema() {
        Ok(schema) => schema,
        Err(error) => {
            tracing::error!(%error, "could not generate data dictionary for OpenAPI document");
            return document;
        }
    };

    let Some(definitions) = schema.document().get("$defs").and_then(Value::as_object) else {
        tracing::error!("system data dictionary has no $defs object");
        return document;
    };

    let mut components = document.components.clone().unwrap_or_default();
    for (name, definition) in definitions {
        let projected = project_definition(definition);
        match serde_json::from_value::<RefOr<Schema>>(projected) {
            Ok(component) => {
                components
                    .schemas
                    .insert(format!("{DATA_DICTIONARY_SCHEMA_PREFIX}{name}"), component);
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    definition = %name,
                    "could not project data-dictionary definition into OpenAPI"
                );
                return document;
            }
        }
    }

    let metadata = json!({
        "$id": schema.document().get("$id"),
        "title": schema.document().get("title"),
        "description": schema.document().get("description"),
        "x-obolargus-schema-version": schema.document().get("x-obolargus-schema-version"),
        "x-obolargus-model-version": schema.document().get("x-obolargus-model-version"),
        "x-obolargus-relationships": schema.document().get("x-obolargus-relationships"),
        "x-obolargus-invariants": schema.document().get("x-obolargus-invariants"),
        "x-obolargus-cross-field-constraints": schema.document().get("x-obolargus-cross-field-constraints"),
        "url": DATA_DICTIONARY_URL,
    });

    document.components = Some(components);
    document
        .extensions
        .get_or_insert_with(Default::default)
        .insert("x-obolargus-data-dictionary".to_string(), metadata);
    document
}

/// Rewrites one canonical `$defs` definition for use as an OpenAPI component.
fn project_definition(definition: &Value) -> Value {
    match definition {
        Value::Object(object) => {
            let mut projected = Map::new();
            for (key, item) in object {
                match key.as_str() {
                    "$ref" => {
                        let reference = item.as_str().map_or_else(
                            || item.clone(),
                            |reference| match reference.strip_prefix("#/$defs/") {
                                Some(name) => Value::String(format!(
                                    "#/components/schemas/{DATA_DICTIONARY_SCHEMA_PREFIX}{name}"
                                )),
                                None => item.clone(),
                            },
                        );
                        projected.insert(key.clone(), reference);
                    }
                    "const" => {
                        projected.insert(
                            "type".to_string(),
                            Value::String(json_schema_type(item).to_string()),
                        );
                        projected.insert("enum".to_string(), Value::Array(vec![item.clone()]));
                    }
                    _ => {
                        projected.insert(key.clone(), project_definition(item));
                    }
                }
            }
            Value::Object(projected)
        }
        Value::Array(items) => Value::Array(items.iter().map(project_definition).collect()),
        other => other.clone(),
    }
}

/// Returns the JSON Schema type name for a value used as a single-value `enum`.
fn json_schema_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn project_definition_rewrites_defs_refs_and_renders_const_as_enum() {
        let definition = json!({
            "type": "object",
            "properties": {
                "type": { "const": "market_price" },
                "value": { "$ref": "#/$defs/FactValue.market_price" },
                "external": { "$ref": "#/components/schemas/AlreadyProjected" }
            },
            "examples": [true, null]
        });

        let projected = project_definition(&definition);

        assert_eq!(
            projected["properties"]["type"],
            json!({ "type": "string", "enum": ["market_price"] })
        );
        assert_eq!(
            projected["properties"]["value"]["$ref"],
            "#/components/schemas/DataDictionary.FactValue.market_price"
        );
        assert_eq!(
            projected["properties"]["external"]["$ref"],
            "#/components/schemas/AlreadyProjected"
        );
        assert_eq!(projected["examples"], json!([true, null]));
    }

    #[test]
    fn json_schema_type_covers_every_json_value() {
        assert_eq!(json_schema_type(&Value::Null), "null");
        assert_eq!(json_schema_type(&json!(true)), "boolean");
        assert_eq!(json_schema_type(&json!(1)), "number");
        assert_eq!(json_schema_type(&json!("text")), "string");
        assert_eq!(json_schema_type(&json!([])), "array");
        assert_eq!(json_schema_type(&json!({})), "object");
    }

    #[test]
    fn with_data_dictionary_adds_every_definition_and_metadata() {
        let canonical = obolargus_core::data_dictionary::export_system_schema()
            .expect("system data dictionary");
        let document = with_data_dictionary(OpenApi::default());

        let schemas = document.components.expect("components").schemas;
        for name in canonical.document()["$defs"]
            .as_object()
            .expect("definitions")
            .keys()
        {
            assert!(
                schemas.contains_key(&format!("{DATA_DICTIONARY_SCHEMA_PREFIX}{name}")),
                "missing component for {name}"
            );
        }

        let extensions = document.extensions.expect("document extensions");
        assert_eq!(
            extensions["x-obolargus-data-dictionary"]["url"],
            DATA_DICTIONARY_URL
        );
    }
}
