//! OpenAPI document validity, inventory and drift tests (US3).
//!
//! The document is generated from the same `routes!` registrations the server
//! serves. These assertions pin the public contract so an operation cannot be
//! removed, an undocumented endpoint cannot appear, and the mount-relative
//! server URL cannot regress.

use std::collections::BTreeSet;

use serde_json::{Value, json};

use obolargus_server::openapi_document;

/// The exact public operation inventory. Documentation and redirect routes
/// are intentionally excluded.
const EXPECTED_OPERATIONS: &[(&str, &str)] = &[
    ("get", "/health"),
    ("get", "/api/assets"),
    ("get", "/api/market-data/sources"),
    ("post", "/api/market-data/refresh"),
    ("get", "/api/market-data/lookup"),
    ("post", "/api/market-data/reconcile"),
    ("post", "/api/market-data/load"),
    ("post", "/api/market-data/load-range"),
    ("get", "/api/data-load/domains"),
    ("get", "/api/data-load/sources"),
    ("patch", "/api/data-load/sources/{source_id}"),
    ("get", "/api/data-load/history"),
    ("get", "/api/data-load/history/{entry_id}"),
    ("get", "/api/data-load/history/groups/{group_id}/progress"),
    (
        "get",
        "/api/domain/{subject}/{subject_id}/facts/{fact_type}",
    ),
    (
        "post",
        "/api/domain/{subject}/{subject_id}/facts/{fact_type}",
    ),
    (
        "post",
        "/api/domain/{subject}/{subject_id}/facts/{fact_type}/retract",
    ),
    (
        "get",
        "/api/domain/{subject}/{subject_id}/facts/{fact_type}/history",
    ),
    (
        "get",
        "/api/domain/{subject}/{subject_id}/facts/{fact_type}/series",
    ),
];

fn document() -> Value {
    serde_json::to_value(openapi_document()).expect("serialize OpenAPI document")
}

#[test]
fn document_parses_as_openapi_3_1() {
    let document = document();
    let json = serde_json::to_string(&document).expect("document JSON");

    oas3::from_json(&json).expect("document must parse as OpenAPI 3.1");

    assert_eq!(document["openapi"], "3.1.0");
    assert_eq!(document["info"]["title"], "Obolargus API");
    assert!(
        document["info"]["version"]
            .as_str()
            .is_some_and(|version| !version.is_empty()),
        "info.version must be non-empty"
    );
    assert!(
        document["info"]["description"]
            .as_str()
            .is_some_and(|description| description.contains("/swagger-ui/")),
        "info.description must link to the interactive page"
    );
}

#[test]
fn document_has_the_exact_operation_inventory() {
    let document = document();
    let paths = document["paths"].as_object().expect("paths object");

    let mut documented = BTreeSet::new();
    for (path, item) in paths {
        for method in [
            "get", "post", "put", "patch", "delete", "head", "options", "trace",
        ] {
            if item.get(method).is_some() {
                documented.insert((method.to_string(), path.clone()));
            }
        }
    }

    let expected: BTreeSet<(String, String)> = EXPECTED_OPERATIONS
        .iter()
        .map(|(method, path)| ((*method).to_string(), (*path).to_string()))
        .collect();

    assert_eq!(documented, expected, "documented operations drifted");
}

#[test]
fn document_uses_the_mount_relative_server_url() {
    let document = document();

    assert_eq!(document["servers"], json!([{ "url": ".." }]));

    let serialized = serde_json::to_string(&document).expect("document JSON");
    assert!(
        !serialized.contains("http://") && !serialized.contains("https://"),
        "document must not contain absolute URLs: {serialized}"
    );
    assert!(
        !serialized.contains(":8000") && !serialized.contains("127.0.0.1"),
        "document must not contain internal host/port values"
    );
}

#[test]
fn operations_carry_tags_summaries_and_documented_statuses() {
    let document = document();
    let paths = document["paths"].as_object().expect("paths object");

    for (method, path) in EXPECTED_OPERATIONS {
        let operation = &paths[*path][*method];
        assert!(
            operation.is_object(),
            "missing operation {method} {path} in document"
        );

        let tags = operation["tags"].as_array().expect("operation tags");
        assert!(
            tags.iter().any(|tag| tag == "health"
                || tag == "market-data"
                || tag == "temporal"
                || tag == "domain"
                || tag == "data-load"),
            "{method} {path} must carry a health, market-data, temporal, or data-load tag"
        );
        assert!(
            operation["summary"]
                .as_str()
                .is_some_and(|summary| !summary.is_empty()),
            "{method} {path} must have a non-empty summary"
        );

        let responses = operation["responses"]
            .as_object()
            .expect("operation responses");
        let statuses: BTreeSet<&str> = responses.keys().map(String::as_str).collect();

        // Asset search is an additive JSON read with its own status set.
        if *path == "/api/assets" {
            let expected: BTreeSet<&str> = ["200", "400", "500"].into_iter().collect();
            assert_eq!(
                statuses, expected,
                "{method} {path} has the wrong documented statuses"
            );
            for status in &expected {
                assert!(
                    responses[*status]["content"]["application/json"].is_object(),
                    "{method} {path} {status} must document a JSON body"
                );
            }
            continue;
        }

        // The Data Load surface uses a JSON error envelope and its own status
        // set, distinct from the legacy market-data text/plain surface.
        if path.starts_with("/api/data-load/") {
            let expected: BTreeSet<&str> = match (*method, *path) {
                ("get", "/api/data-load/domains") => ["200"].into_iter().collect(),
                ("get", "/api/data-load/sources") => ["200", "400"].into_iter().collect(),
                ("patch", "/api/data-load/sources/{source_id}") => {
                    ["200", "400", "404", "409", "500", "503"]
                        .into_iter()
                        .collect()
                }
                ("get", "/api/data-load/history") => ["200", "400", "503"].into_iter().collect(),
                ("get", "/api/data-load/history/{entry_id}") => {
                    ["200", "400", "404", "503"].into_iter().collect()
                }
                _ => ["200", "400", "503"].into_iter().collect(),
            };
            assert_eq!(
                statuses, expected,
                "{method} {path} has the wrong documented statuses"
            );
            assert!(
                responses["200"]["content"]["application/json"].is_object(),
                "{method} {path} must document a JSON success body"
            );
            for status in expected.iter().filter(|status| **status != "200") {
                assert!(
                    responses[*status]["content"]["application/json"].is_object(),
                    "{method} {path} {status} must document a JSON error envelope"
                );
            }
            continue;
        }

        // The temporal surface documents a JSON error envelope and uses the
        // contract's status set, which differs from the market-data surface.
        if path.starts_with("/api/domain/") {
            let expected: BTreeSet<&str> = match *method {
                "get" if path.ends_with("/history") => ["200", "404"].into_iter().collect(),
                "get" if path.ends_with("/series") => {
                    ["200", "400", "404", "500"].into_iter().collect()
                }
                "get" => ["200", "400", "404", "410"].into_iter().collect(),
                _ if path.ends_with("/retract") => ["201", "409"].into_iter().collect(),
                _ => ["201", "400", "409"].into_iter().collect(),
            };
            assert_eq!(
                statuses, expected,
                "{method} {path} has the wrong documented statuses"
            );
            let success = if *method == "get" { "200" } else { "201" };
            assert!(
                responses[success]["content"]["application/json"].is_object(),
                "{method} {path} must document a JSON success body"
            );
            for status in expected.iter().filter(|status| **status != success) {
                assert!(
                    responses[*status]["content"]["application/json"].is_object(),
                    "{method} {path} {status} must document a JSON error envelope"
                );
            }
            continue;
        }

        let expected: BTreeSet<&str> = match (*method, *path) {
            ("get", "/health") | ("get", "/api/market-data/sources") => {
                ["200"].into_iter().collect()
            }
            ("get", "/api/market-data/lookup") => ["200", "400"].into_iter().collect(),
            _ => ["200", "400", "401", "415", "422", "500", "502", "503"]
                .into_iter()
                .collect(),
        };
        assert_eq!(
            statuses, expected,
            "{method} {path} has the wrong documented statuses"
        );

        assert!(
            responses["200"]["content"]["application/json"].is_object(),
            "{method} {path} must document a JSON success body"
        );
        for status in expected.iter().filter(|status| **status != "200") {
            assert!(
                responses[*status]["content"]["text/plain"].is_object(),
                "{method} {path} {status} must document a text/plain error body"
            );
        }
    }

    for path in ["/api/market-data/load", "/api/market-data/load-range"] {
        let description = paths[path]["post"]["description"]
            .as_str()
            .expect("load description");
        assert!(
            description.contains("Per-identifier") && description.contains("200"),
            "{path} must explain that per-identifier failures stay in the 200 report"
        );
    }

    let parameters = paths["/api/market-data/lookup"]["get"]["parameters"]
        .as_array()
        .expect("lookup parameters");
    assert!(
        parameters.iter().any(|parameter| {
            parameter["name"] == "q" && parameter["in"] == "query" && parameter["required"] == true
        }),
        "lookup must document the required q query parameter"
    );
}

#[test]
fn document_contains_the_expected_schemas_and_shapes() {
    let document = document();
    let schemas = document["components"]["schemas"]
        .as_object()
        .expect("component schemas");

    for schema in [
        "HealthResponse",
        "RefreshBody",
        "RefreshResponse",
        "LookupResponse",
        "ReconcileBody",
        "ReconcileResponse",
        "IdentifierInput",
        "LoadBody",
        "LoadRangeBody",
        "SourceDescriptor",
        "TypedIdentifier",
        "TickerLookupHit",
        "LoadOutcome",
        "AssetCreation",
        "CurrencySource",
        "CreationDecline",
        "SourceOutcome",
        "LoadIdentifierOutcome",
        "RunStatus",
        "IdentifierLoadReport",
        "DateLoadSection",
        "RangeLoadTotals",
        "IdentifierRangeLoadReport",
    ] {
        assert!(
            schemas.contains_key(schema),
            "missing component schema {schema}"
        );
    }

    let identifier_input = &schemas["IdentifierInput"];
    let variants = identifier_input["oneOf"]
        .as_array()
        .expect("IdentifierInput oneOf");
    assert_eq!(variants.len(), 2, "IdentifierInput must have two forms");
    assert_eq!(variants[0]["type"], "string");
    assert_eq!(variants[1]["type"], "object");
    assert_eq!(
        variants[1]["required"],
        json!(["type", "value"]),
        "object identifier form must require type and value"
    );

    let expected_outcomes: BTreeSet<&str> = [
        "loaded",
        "no_op",
        "not_resolved",
        "no_data_for_date",
        "unsupported_scheme",
        "rejected",
        "source_unavailable",
        "rate_limited",
        "storage_failure",
    ]
    .into_iter()
    .collect();
    let documented_outcomes: BTreeSet<&str> = schemas["LoadOutcome"]["enum"]
        .as_array()
        .expect("LoadOutcome enum")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert_eq!(documented_outcomes, expected_outcomes);

    let expected_statuses: BTreeSet<&str> = ["complete", "partial", "failed"].into_iter().collect();
    let documented_statuses: BTreeSet<&str> = schemas["RunStatus"]["enum"]
        .as_array()
        .expect("RunStatus enum")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert_eq!(documented_statuses, expected_statuses);

    let creation_variants = schemas["AssetCreation"]["oneOf"]
        .as_array()
        .expect("AssetCreation oneOf");
    let creation_statuses: BTreeSet<&str> = creation_variants
        .iter()
        .filter_map(|variant| variant["properties"]["status"]["enum"][0].as_str())
        .collect();
    assert_eq!(
        creation_statuses,
        BTreeSet::from([
            "not_requested",
            "would_attempt",
            "created",
            "adopted",
            "declined",
            "failed",
        ]),
        "AssetCreation variants must keep the status discriminator"
    );
}
