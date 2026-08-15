# obolargus-server

Axum REST API backend for Obolargus. A git submodule of the parent Obolargus
repository. Design: `specs/architecture/`; feature: `specs/001-boilerplate-submodules/`.

## Contents

- `config.rs` — `AppConfig` via `envconfig` (`APP_HOST`, `APP_PORT`,
  `DATABASE_URL`, `JWT_*`, `CORS_ALLOWED_ORIGINS`, `AUTH_ENABLED`) with
  fail-fast validation per `contracts/config-contract.md`.
- `routes/health.rs` — `GET /health` → `{status, version, [database]}` per
  `contracts/health-check.md`.
- `lib.rs` — `AppState`, `router()`, `connect_database()`, `serve()`.
- `main.rs` — fail-fast config load, tracing init, bind and serve.

## Development

- Tests: `cargo test --all-targets` (includes contract and boot tests)
- Lint: `cargo clippy --all-targets -- -D warnings`
- Coverage: `cargo llvm-cov --all-targets` (threshold 90%+)
- Docs: `cargo doc --no-deps`

From the parent repo: `make test|lint|test-coverage PROJ=obolargus-server`.