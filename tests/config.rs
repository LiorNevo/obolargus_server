//! Configuration contract tests.
//!
//! Executes the compiled server binary with hostile environment variables and
//! asserts the fail-fast behavior from `contracts/config-contract.md`.

use std::process::Command;

#[test]
fn refuses_to_start_when_auth_enabled_without_jwt_secret() {
    let output = Command::new(env!("CARGO_BIN_EXE_obolargus-server"))
        .env("AUTH_ENABLED", "true")
        // An empty value overrides any JWT_SECRET in a local `.env`, which
        // dotenvy loads but does not let override the process environment.
        .env("JWT_SECRET", "")
        .output()
        .unwrap();

    assert!(!output.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("JWT_SECRET"),
        "stderr should name JWT_SECRET, got: {stderr}"
    );
}
