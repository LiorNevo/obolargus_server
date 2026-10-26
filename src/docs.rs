//! Interactive and machine-readable API documentation.
//!
//! The documentation is generated from the same [`utoipa`] annotations that
//! describe the API handlers; see [`crate::api_router`] and
//! [`crate::openapi_document`].
//!
//! All documentation URLs are deliberately **mount-relative**. The gateway
//! proxies `/obolargus/api/{*rest}` to the server after stripping the
//! `/obolargus/api` prefix, so an absolute `/swagger-ui/` or
//! `/api-docs/openapi.json` URL would escape the mount. The UI therefore
//! fetches the document through `../api-docs/openapi.json`, and the document
//! declares `servers: [{ "url": ".." }]`; both resolve correctly whether the
//! browser is on the server port or behind the gateway.

use axum::Router;
use axum::response::Redirect;
use axum::routing::get;
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
        .persist_authorization(false);

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
