# obolargus-server

Axum REST API backend for Obolargus. A git submodule of the parent Obolargus
repository. Design: `specs/architecture/`; feature: `specs/001-boilerplate-submodules/`.

## Contents

- `config.rs` — `AppConfig` via `envconfig` (`APP_HOST`, `APP_PORT`,
  `DATABASE_URL`, `JWT_*`, `CORS_ALLOWED_ORIGINS`, `AUTH_ENABLED`) with
  fail-fast validation per `contracts/config-contract.md`.
- `routes/health.rs` — `GET /health` → `{status, version, [database]}` per
  `contracts/health-check.md`.
- `lib.rs` — `AppState`, `api_router()`, `openapi_document()`, `router()`,
  `connect_database()`, `serve()`.
- `docs.rs` — interactive Swagger UI, OpenAPI JSON document and the `/` →
  `/swagger-ui/` redirect.
- `main.rs` — fail-fast config load, tracing init, bind and serve.

## API documentation

Generated from the handler/type annotations (Constitution XI) and served
without a database:

- `http://127.0.0.1:8000/swagger-ui/` — interactive Swagger UI with
  "Try it out".
- `http://127.0.0.1:8000/api-docs/openapi.json` — OpenAPI 3.1 document.
- `http://127.0.0.1:8000/` — `307` redirect to `/swagger-ui/`.

Through the develop gateway (`make develop`), open
`http://127.0.0.1:8080/obolargus/api`; it redirects to the mounted page at
`/obolargus/api/swagger-ui/`. Documentation URLs are mount-relative, so
"Try it out" works both on port 8000 and through the gateway.

## Development

- Tests: `cargo test --all-targets` (includes contract and boot tests)
- Lint: `cargo clippy --all-targets -- -D warnings`
- Coverage: `cargo llvm-cov --all-targets` (threshold 90%+)
- Docs: `cargo doc --no-deps`

From the parent repo: `make test|lint|test-coverage PROJ=obolargus-server`.
