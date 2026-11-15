//! HTTP contract tests for the Morpheus chat.

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

async fn state_with_provider(
    base_url: String,
) -> Option<(AppState, obolargus_core::common::PoolGuard)> {
    let (pool, guard) = tx_pool().await?;
    let config = AppConfig {
        morpheus_enabled: true,
        omniroute_api_key: "test-key".to_string(),
        omniroute_base_url: format!("{base_url}/v1"),
        morpheus_actor_id: "morpheus-api-test".to_string(),
        ..AppConfig::default()
    };
    let state = AppState::new(config, Some(pool)).expect("state");
    Some((state, guard))
}

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
async fn send_message_streams_and_persists_an_answer() {
    let Some(_) = tx_pool().await else {
        return;
    };
    let server = MockServer::start().await;
    let stream = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"A bitemporal \"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"fact has two axes.\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n"
    );
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(stream),
        )
        .mount(&server)
        .await;

    let Some((state, _guard)) = state_with_provider(server.uri()).await else {
        return;
    };
    let app = router(state);

    let create = app
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
    assert_eq!(create.status(), StatusCode::CREATED);
    let created = body_json(create).await;
    let conversation_id = created["id"].as_str().expect("conversation id");

    let send = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/morpheus/conversations/{conversation_id}/messages"
                ))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "content": "What is a bitemporal fact?",
                        "client_message_id": "api-test-1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("send");
    assert_eq!(send.status(), StatusCode::ACCEPTED);
    let sent = body_json(send).await;
    let turn_id = sent["turn"]["id"].as_str().expect("turn id").to_string();

    let mut completed = false;
    for _ in 0..50 {
        let detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/morpheus/turns/{turn_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("turn detail");
        assert_eq!(detail.status(), StatusCode::OK);
        let detail = body_json(detail).await;
        if detail["turn"]["status"] == "completed" {
            assert_eq!(
                detail["agent_message"]["content"],
                "A bitemporal fact has two axes."
            );
            completed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(completed, "turn did not complete");
}
