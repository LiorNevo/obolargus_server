# Migrations

This directory is the reserved home for SQLx database migrations for the
obolargus-server PostgreSQL schema (see `plan.md` "Storage").

The skeleton ships with **no schema**, so no migration files exist yet (this
note and `.gitkeep` only). When the first table lands, add migration files here
following the SQLx convention `{version}_{description}.sql` and wire the runner:

```rust
sqlx::migrate!("./migrations").run(&pool).await
```

in `connect_database` (`src/lib.rs`) for the `DATABASE_URL`-configured path.
The runner is intentionally deferred: wiring it on a schema-less skeleton would
add database-only code paths that cannot be exercised by the test suite and
would violate the constitution's 90%+ coverage MUST (Constitution III /
Convergence T052, documented-exclusion branch).