//! Redaction and size-cap tests for persisted automation payloads.

use serde_json::json;

use obolargus_server::automation::executor::{cap, redact};

#[test]
fn secret_shaped_fields_are_redacted_recursively() {
    let value = json!({
        "payload": {
            "user": "alice",
            "password": "hunter2",
            "nested": { "api_key": "abc", "keep": 1 },
            "items": [{ "Authorization": "Bearer token" }, { "token": "t" }]
        }
    });
    let redacted = redact(&value);
    assert_eq!(redacted["payload"]["user"], "alice");
    assert_eq!(redacted["payload"]["password"], "[redacted]");
    assert_eq!(redacted["payload"]["nested"]["api_key"], "[redacted]");
    assert_eq!(redacted["payload"]["nested"]["keep"], 1);
    assert_eq!(
        redacted["payload"]["items"][0]["Authorization"],
        "[redacted]"
    );
    assert_eq!(redacted["payload"]["items"][1]["token"], "[redacted]");
}

#[test]
fn payloads_within_the_limit_pass_through_redacted() {
    let value = json!({ "secret": "x", "payload": 1 });
    let (payload, size, truncated) = cap(&value, 1_024);
    assert!(!truncated);
    assert!(size > 0);
    let payload = payload.expect("payload");
    assert_eq!(payload["secret"], "[redacted]");
}

#[test]
fn oversized_payloads_are_flagged_and_dropped() {
    let value = json!({ "payload": "x".repeat(256) });
    let (payload, size, truncated) = cap(&value, 32);
    assert!(truncated);
    assert!(payload.is_none());
    assert!(size > 32);
}

#[test]
fn zero_limit_disables_capping() {
    let value = json!({ "payload": "x".repeat(256) });
    let (payload, _size, truncated) = cap(&value, 0);
    assert!(!truncated);
    assert!(payload.is_some());
}
