//! SSE projection tests for Morpheus turns.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use obolargus_core::common::tx_pool;
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

async fn body_json(response: axum::response::Response) -> Value {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

#[tokio::test]
async fn turn_stream_emits_snapshot_and_end() {
    let Some((pool, _guard)) = tx_pool().await else {
        return;
    };
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"Hello from Morpheus.\"}}]}\n\n",
                    "data: [DONE]\n\n"
                )),
        )
        .mount(&provider)
        .await;

    let config = AppConfig {
        morpheus_enabled: true,
        omniroute_api_key: "test-key".to_string(),
        omniroute_base_url: format!("{}/v1", provider.uri()),
        morpheus_actor_id: "morpheus-events-test".to_string(),
        ..AppConfig::default()
    };
    let app = router(AppState::new(config, Some(pool)).expect("state"));

    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/morpheus/conversations")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "client_conversation_id": uuid::Uuid::new_v4() }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create");
    let conversation_id = body_json(created).await["id"]
        .as_str()
        .expect("conversation id")
        .to_string();

    let sent = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/morpheus/conversations/{conversation_id}/messages"
                ))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "content": "Hi", "client_message_id": "events-test-1" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("send");
    assert_eq!(sent.status(), StatusCode::ACCEPTED);
    let turn_id = body_json(sent).await["turn"]["id"]
        .as_str()
        .expect("turn id")
        .to_string();

    let events = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/morpheus/turns/{turn_id}/events"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("events");
    assert_eq!(events.status(), StatusCode::OK);
    let body = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        events.into_body().collect(),
    )
    .await
    .expect("stream timeout")
    .expect("stream body")
    .to_bytes();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("event: snapshot"));
    assert!(text.contains("Hello from Morpheus."));
    assert!(text.contains("event: end"));
}
