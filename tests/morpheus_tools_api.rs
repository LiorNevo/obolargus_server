//! Tool-call and approval loop tests.

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

async fn get_json(app: &axum::Router, uri: &str) -> Value {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);
    body_json(response).await
}

#[tokio::test]
async fn tool_call_waits_for_approval_then_completes() {
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
                    "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"assets__search\",\"arguments\":\"{\\\"query\\\":\\\"AAPL\\\"}\"}}]}}]}\n\n",
                    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
                    "data: [DONE]\n\n"
                )),
        )
        .up_to_n_times(1)
        .mount(&provider)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"I searched the catalog.\"}}]}\n\n",
                    "data: [DONE]\n\n"
                )),
        )
        .mount(&provider)
        .await;

    let config = AppConfig {
        morpheus_enabled: true,
        omniroute_api_key: "test-key".to_string(),
        omniroute_base_url: format!("{}/v1", provider.uri()),
        morpheus_actor_id: "morpheus-tools-test".to_string(),
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
        .expect("conversation")
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
                    json!({ "content": "Find AAPL", "client_message_id": "tool-test-1" })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("send");
    let turn_id = body_json(sent).await["turn"]["id"]
        .as_str()
        .expect("turn")
        .to_string();

    let mut approval_id = None;
    for _ in 0..50 {
        let detail = get_json(&app, &format!("/api/morpheus/turns/{turn_id}")).await;
        if detail["turn"]["status"] == "awaiting_approval" {
            let call = &detail["tool_calls"][0];
            assert_eq!(call["status"], "awaiting_approval");
            approval_id = call["approval_id"].as_str().map(str::to_string);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let approval_id = approval_id.expect("pending approval");

    let decided = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/morpheus/approvals/{approval_id}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({ "decision": "approve" }).to_string()))
                .unwrap(),
        )
        .await
        .expect("decide");
    assert_eq!(decided.status(), StatusCode::OK);

    let mut completed = false;
    for _ in 0..60 {
        let detail = get_json(&app, &format!("/api/morpheus/turns/{turn_id}")).await;
        if detail["turn"]["status"] == "completed" {
            assert_eq!(
                detail["agent_message"]["content"],
                "I searched the catalog."
            );
            assert_eq!(detail["tool_calls"][0]["status"], "succeeded");
            completed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(completed, "turn did not complete after approval");
}

#[tokio::test]
async fn data_load_history_tool_executes_against_the_database() {
    let Some((pool, _guard)) = tx_pool().await else {
        return;
    };
    let state = AppState::new(AppConfig::default(), Some(pool.clone())).expect("state");
    let outcome = obolargus_server::morpheus::tools::execute(
        &state,
        &pool,
        "dev-user",
        "data_load.history",
        &json!({ "limit": 5 }),
    )
    .await
    .expect("history tool");
    assert!(
        outcome.summary.contains("No load runs") || outcome.summary.starts_with("Found "),
        "unexpected summary: {}",
        outcome.summary
    );
}
