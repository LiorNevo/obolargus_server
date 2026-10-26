# AGENTS.md — obolargus-server

Operational rules for contributors and AI agents working in this crate.

## Conventions (from `.specify/memory/constitution.md`)

- No `unsafe`, no `unwrap`, no `panic` in production code — use `Result`.
- All configuration MUST flow through the env-config mechanism
  (`AppConfig`/`envconfig`); never hardcode credentials or endpoints.
- Coverage must stay 90%+ (verified with `cargo-llvm-cov`).
- Every public item needs a rustdoc comment; documentation is code-driven.
- `rustfmt` and `clippy --all-targets -- -D warnings` must stay clean.

## Contract rules

- Endpoints must honor `contracts/health-check.md` (failed DB ping still
  returns HTTP 200 with `database: "unavailable"`).
- Config behavior must honor `contracts/config-contract.md` (`AUTH_ENABLED`
  without `JWT_SECRET` fails fast).
- Wire behavior is covered by integration tests; keep the boot test green.
- Every public API route MUST be registered through `api_router()` with
  `utoipa_axum::routes!(...)`. Do not add API endpoints with a plain
  `Router::route`; that would omit them from the OpenAPI document. The
  documentation and redirect routes in `docs.rs` are the only exempt
  surfaces.
- Keep documentation URLs mount-relative (`../api-docs/openapi.json`,
  `servers: [{ "url": ".." }]`); never introduce absolute `http://` URLs or
  internal host/port values into the document or page.

## Commands

- Tests: `cargo test --all-targets --no-fail-fast`
- Lint: `cargo clippy --all-targets -- -D warnings`
- Coverage: `cargo llvm-cov --all-targets --no-fail-fast`
- Boot the skeleton: `cargo run` (then `curl localhost:8000/health`).
