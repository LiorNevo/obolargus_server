# obolargus-server

Axum REST API backend for Obolargus. A git submodule of the parent Obolargus
repository. Design: `specs/architecture/`; feature: `specs/001-boilerplate-submodules/`.

## Contents

- `config.rs` — `AppConfig` via `envconfig` (`APP_HOST`, `APP_PORT`,
  `DATABASE_URL`, `JWT_*`, `CORS_ALLOWED_ORIGINS`, `AUTH_ENABLED`,
  `GOOGLE_CLIENT_ID`, `GOOGLE_ALLOWED_EMAILS`, `PRIMARY_OWNER_EMAIL`,
  `ALLOW_UNAUTHENTICATED`) with fail-fast validation per
  `contracts/config-contract.md`.
- `auth/` — Google ID-token verification (`google.rs`), application HS256
  access/refresh tokens (`jwt.rs`), the actor registry (`actor.rs`),
  default-deny middleware (`middleware.rs`), the failed-sign-in limiter
  (`rate_limit.rs`), and the startup owner bootstrap (`bootstrap.rs`).
- `routes/auth.rs` — public `POST /api/auth/login` and
  `POST /api/auth/refresh`.
- `routes/health.rs` — `GET /health` → `{status, version, [database]}` per
  `contracts/health-check.md`.
- `lib.rs` — `AppState`, `api_router()`, `openapi_document()`, `router()`,
  `connect_database()`, `serve()`.
- `docs.rs` — interactive Swagger UI, OpenAPI JSON document and the `/` →
  `/swagger-ui/` redirect.
- `main.rs` — fail-fast config load, tracing init, bind and serve.

## Authentication

Authentication is default-deny. The browser obtains a Google Identity
Services ID token; `POST /api/auth/login` verifies it (RS256 signature against
Google's JWKS, issuer, audience, expiry, and `email_verified`), checks the
2–5 account allowlist, binds the stable Google subject to an actor row, and
issues a 15-minute HS256 access token plus a 7-day refresh token. There is no
password storage and no registration surface.

`GET /health`, `POST /api/auth/login`, `POST /api/auth/refresh`, and CORS
preflight are public; Swagger UI, the OpenAPI document, and every API route
require a bearer access token. When authentication is enabled the server
refuses to start without `GOOGLE_CLIENT_ID`, `GOOGLE_ALLOWED_EMAILS`, a
`PRIMARY_OWNER_EMAIL` on that allowlist, `JWT_SECRET`, and a database.
`AUTH_ENABLED=false` is loopback-only local development; a non-loopback bind
also needs `ALLOW_UNAUTHENTICATED=true`. Rotating `JWT_SECRET` invalidates all
application sessions.

## API documentation

Generated from the handler/type annotations (Constitution XI) and served
without a database. When authentication is enabled, the documentation
surfaces require a bearer token; sign in through the UI first.

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
