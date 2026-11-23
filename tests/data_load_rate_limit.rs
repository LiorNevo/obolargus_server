//! Per-actor load-start rate-limit tests.

mod data_load_common;

use axum::http::StatusCode;
use uuid::Uuid;

use data_load_common::{identifier, range_body, send, state_with};

#[tokio::test]
async fn ten_starts_per_minute_then_429_with_retry_after() {
    // Rejected requests never consume a start token.
    for _ in 0..5 {
        let response = send(
            state_with(None, None),
            "POST",
            "/api/market-data/load-range/async",
            Some(range_body(
                Vec::new(),
                "2026-01-01",
                "2026-01-02",
                4,
                Uuid::new_v4(),
            )),
        )
        .await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
    }

    // The first ten valid starts are accepted by the limiter and then fail
    // only because this test state has no database pool.
    for _ in 0..10 {
        let response = send(
            state_with(None, None),
            "POST",
            "/api/market-data/load-range/async",
            Some(range_body(
                vec![identifier("AAPL")],
                "2026-01-01",
                "2026-01-02",
                4,
                Uuid::new_v4(),
            )),
        )
        .await;
        assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    }

    let limited = send(
        state_with(None, None),
        "POST",
        "/api/market-data/load-range/async",
        Some(range_body(
            vec![identifier("AAPL")],
            "2026-01-01",
            "2026-01-02",
            4,
            Uuid::new_v4(),
        )),
    )
    .await;
    assert_eq!(limited.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(limited.error_code(), Some("rate_limited"));
    let retry_after = limited.retry_after().expect("Retry-After header");
    assert!((1..=60).contains(&retry_after), "got {retry_after}");

    // Invalid input still reports its own validation error, not 429.
    let invalid = send(
        state_with(None, None),
        "POST",
        "/api/market-data/load-range/async",
        Some(range_body(
            Vec::new(),
            "2026-01-01",
            "2026-01-02",
            4,
            Uuid::new_v4(),
        )),
    )
    .await;
    assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
}
