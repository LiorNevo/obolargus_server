//! Coverage for the Morpheus route surface that does not need a live provider.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use obolargus_core::common::tx_pool;
use obolargus_core::morpheus::persistence;
use obolargus_server::config::AppConfig;
use obolargus_server::{AppState, router};

async fn body_json(response: axum::response::Response) -> Value {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    if bytes.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(&bytes).expect("json")
}

async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let request = builder
        .body(match body {
            Some(body) => Body::from(body.to_string()),
            None => Body::empty(),
        })
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let value = body_json(response).await;
    (status, value)
}

async fn get(app: &Router, uri: &str) -> (StatusCode, Value) {
    call(app, "GET", uri, None).await
}

fn enabled_config(actor: &str) -> AppConfig {
    AppConfig {
        morpheus_enabled: true,
        omniroute_api_key: "test-key".to_string(),
        omniroute_base_url: "http://127.0.0.1:1/v1".to_string(),
        morpheus_actor_id: actor.to_string(),
        morpheus_connect_timeout_seconds: 1,
        morpheus_status_probe_seconds: 3_600,
        morpheus_queue_max_per_conversation: 1,
        ..AppConfig::default()
    }
}

#[tokio::test]
async fn disabled_and_missing_pool_routes_report_service_unavailable() {
    let disabled = router(
        AppState::new(
            AppConfig {
                morpheus_enabled: false,
                ..AppConfig::default()
            },
            None,
        )
        .expect("state"),
    );
    let (status, body) = get(&disabled, "/api/morpheus/tools").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "morpheus_disabled");

    let (status, status_body) = get(&disabled, "/api/morpheus/status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status_body["enabled"], false);
    assert_eq!(status_body["available"], false);

    let no_pool = router(AppState::new(enabled_config("no-pool"), None).expect("state"));
    let (status, body) = get(&no_pool, "/api/morpheus/conversations").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "internal_error");
}

#[tokio::test]
async fn conversation_routes_cover_crud_and_validation() {
    let Some((pool, guard)) = tx_pool().await else {
        return;
    };
    let actor = format!("morpheus-routes-{}", Uuid::new_v4());
    let state = AppState::new(enabled_config(&actor), Some(pool.clone())).expect("state");
    let app = router(state);
    let client_id = Uuid::new_v4();

    let (status, created) = call(
        &app,
        "POST",
        "/api/morpheus/conversations",
        Some(json!({ "client_conversation_id": client_id })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let conversation_id = created["id"].as_str().expect("id").to_string();

    let (status, duplicate) = call(
        &app,
        "POST",
        "/api/morpheus/conversations",
        Some(json!({ "client_conversation_id": client_id })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(duplicate["id"], created["id"]);

    let (status, list) = get(&app, "/api/morpheus/conversations?limit=0&offset=-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["limit"], 1);
    assert_eq!(list["offset"], 0);

    let (user, agent, turn) = persistence::create_turn(
        &pool,
        &actor,
        Uuid::parse_str(&conversation_id).expect("uuid"),
        "Hello Morpheus",
        "route-message-1",
    )
    .await
    .expect("turn");

    let (status, detail) = get(
        &app,
        &format!("/api/morpheus/conversations/{conversation_id}?message_limit=0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["conversation"]["message_count"], 2);
    assert_eq!(detail["messages"].as_array().expect("messages").len(), 1);

    let (status, page) = get(
        &app,
        &format!(
            "/api/morpheus/conversations/{conversation_id}/messages?limit=1&before={}",
            agent.id
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["items"].as_array().expect("items").len(), 1);
    assert_eq!(page["items"][0]["id"], user.id.to_string());

    let (status, _) = get(
        &app,
        &format!("/api/morpheus/conversations/{}", Uuid::new_v4()),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, renamed) = call(
        &app,
        "PATCH",
        &format!("/api/morpheus/conversations/{conversation_id}"),
        Some(json!({ "title": "Renamed" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(renamed["title"], "Renamed");

    for title in [String::new(), "x".repeat(121)] {
        let (status, _) = call(
            &app,
            "PATCH",
            &format!("/api/morpheus/conversations/{conversation_id}"),
            Some(json!({ "title": title })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    let (status, _) = call(
        &app,
        "POST",
        &format!("/api/morpheus/conversations/{conversation_id}/messages"),
        Some(json!({ "content": "   ", "client_message_id": "x" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = call(
        &app,
        "POST",
        &format!("/api/morpheus/conversations/{conversation_id}/messages"),
        Some(json!({ "content": "hello", "client_message_id": " " })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = call(
        &app,
        "POST",
        &format!("/api/morpheus/conversations/{conversation_id}/messages"),
        Some(json!({ "content": "hello", "client_message_id": "route-message-2" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, detail) = get(&app, &format!("/api/morpheus/turns/{}", turn.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["turn"]["id"], turn.id.to_string());

    let (status, stopped) = call(
        &app,
        "POST",
        &format!("/api/morpheus/turns/{}/stop", turn.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stopped["status"], "stopped");

    let (status, _) = call(
        &app,
        "POST",
        &format!("/api/morpheus/turns/{}/stop", turn.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = call(
        &app,
        "POST",
        &format!("/api/morpheus/turns/{}/retry", turn.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, _) = get(&app, &format!("/api/morpheus/turns/{}", Uuid::new_v4())).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = call(
        &app,
        "POST",
        &format!("/api/morpheus/approvals/{}", Uuid::new_v4()),
        Some(json!({ "decision": "approve" })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, standing) = get(&app, "/api/morpheus/approvals/standing").await;
    assert_eq!(status, StatusCode::OK);
    assert!(standing["history"].is_array());

    let (status, _) = call(
        &app,
        "DELETE",
        &format!("/api/morpheus/approvals/standing/{}", Uuid::new_v4()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, tools) = get(&app, "/api/morpheus/tools").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!tools["items"].as_array().expect("items").is_empty());

    let (status, status_body) = get(&app, "/api/morpheus/status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status_body["enabled"], true);
    assert_eq!(status_body["catalog"]["tool_count"], 6);

    let (status, _) = call(
        &app,
        "DELETE",
        &format!("/api/morpheus/conversations/{conversation_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = get(
        &app,
        &format!("/api/morpheus/conversations/{conversation_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    drop(guard);
}

#[tokio::test]
async fn database_bootstrap_and_tool_paths_are_covered() {
    let url = std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok());
    let Some(url) = url else {
        return;
    };
    let Some((_gate, guard)) = tx_pool().await else {
        return;
    };
    let pool = obolargus_server::connect_database(Some(&url))
        .await
        .expect("database connects");
    let state = AppState::new_with_persistence(enabled_config("bootstrap"), Some(pool.clone()))
        .await
        .expect("state with persistence");
    assert!(state.pool.is_some());

    obolargus_server::morpheus::recovery::run(&pool)
        .await
        .expect("recovery");

    let outcome = obolargus_server::morpheus::tools::execute(
        &state,
        &pool,
        "assets.search",
        &json!({ "query": "AAPL", "limit": 5 }),
    )
    .await
    .expect("asset search");
    assert!(outcome.data["items"].is_array());

    let today = chrono::Utc::now().date_naive();
    let run = obolargus_core::market_data::audit::start_load_run_with_context(
        &pool,
        "api",
        Some("yahoo_finance"),
        &obolargus_core::market_data::audit::LoadRunContext::market_data(Some(today)),
    )
    .await
    .expect("start load run");
    obolargus_core::market_data::audit::finish_load_run(
        &pool,
        run.id,
        0,
        0,
        &obolargus_core::market_data::load::DataSourceLoadCounters::default(),
        &json!({}),
        &json!({}),
        &json!([]),
    )
    .await
    .expect("finish load run");

    let history = obolargus_server::morpheus::tools::execute(
        &state,
        &pool,
        "data_load.history",
        &json!({
            "from": today.to_string(),
            "to": today.to_string(),
            "domain": "market_data",
            "source": "yahoo_finance",
            "entry_type": "load_run",
            "limit": 1
        }),
    )
    .await
    .expect("load history");
    assert!(history.data["items"].is_array());
    assert!(!history.data["items"].as_array().expect("items").is_empty());

    let standing = persistence::upsert_standing(
        &pool,
        "bootstrap",
        json!({ "assets.search": "hash" }),
        &["click".to_string()],
    )
    .await
    .expect("standing grant");
    let app = router(state);
    let (status, listed) = get(&app, "/api/morpheus/approvals/standing").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listed["active"]["id"], standing.id.to_string());
    assert_eq!(listed["active"]["tool_ids"][0], "assets.search");

    let (status, _) = call(
        &app,
        "DELETE",
        &format!("/api/morpheus/approvals/standing/{}", standing.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, listed) = get(&app, "/api/morpheus/approvals/standing").await;
    assert_eq!(status, StatusCode::OK);
    assert!(listed["active"].is_null());
    assert_eq!(listed["history"].as_array().expect("history").len(), 1);
    drop(guard);
}

#[tokio::test]
async fn provider_failure_marks_the_turn_failed() {
    let Some((pool, guard)) = tx_pool().await else {
        return;
    };
    let provider = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/chat/completions"))
        .respond_with(wiremock::ResponseTemplate::new(500).set_body_string("upstream"))
        .mount(&provider)
        .await;

    let config = AppConfig {
        omniroute_base_url: format!("{}/v1", provider.uri()),
        ..enabled_config("provider-failure")
    };
    let state = AppState::new(config, Some(pool.clone())).expect("state");
    let app = router(state);

    let (status, created) = call(
        &app,
        "POST",
        "/api/morpheus/conversations",
        Some(json!({ "client_conversation_id": Uuid::new_v4() })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let conversation_id = created["id"].as_str().expect("id").to_string();

    let (status, sent) = call(
        &app,
        "POST",
        &format!("/api/morpheus/conversations/{conversation_id}/messages"),
        Some(json!({ "content": "fail", "client_message_id": "provider-failure-1" })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let turn_id = sent["turn"]["id"].as_str().expect("turn").to_string();

    let mut failed = false;
    for _ in 0..80 {
        let (status, detail) = get(&app, &format!("/api/morpheus/turns/{turn_id}")).await;
        if status == StatusCode::OK && detail["turn"]["status"] == "failed" {
            failed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed, "provider failure did not fail the turn");
    drop(guard);
}

#[tokio::test]
async fn provider_call_limit_finishes_the_turn() {
    let Some((pool, guard)) = tx_pool().await else {
        return;
    };
    let config = AppConfig {
        morpheus_max_provider_calls: 0,
        ..enabled_config("provider-limit")
    };
    let state = AppState::new(config, Some(pool)).expect("state");
    let app = router(state);

    let (status, created) = call(
        &app,
        "POST",
        "/api/morpheus/conversations",
        Some(json!({ "client_conversation_id": Uuid::new_v4() })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let conversation_id = created["id"].as_str().expect("id").to_string();

    let (status, sent) = call(
        &app,
        "POST",
        &format!("/api/morpheus/conversations/{conversation_id}/messages"),
        Some(json!({ "content": "limit", "client_message_id": "provider-limit-1" })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let turn_id = sent["turn"]["id"].as_str().expect("turn").to_string();

    let mut limited = false;
    for _ in 0..80 {
        let (status, detail) = get(&app, &format!("/api/morpheus/turns/{turn_id}")).await;
        if status == StatusCode::OK
            && detail["turn"]["status"] == "completed"
            && detail["turn"]["limit_reached"] == "provider_calls"
        {
            limited = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(limited, "provider-call limit did not finish the turn");
    drop(guard);
}

#[tokio::test]
async fn status_reports_a_healthy_provider() {
    let Some((pool, guard)) = tx_pool().await else {
        return;
    };
    let provider = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/models"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string(r#"{"data":[]}"#),
        )
        .mount(&provider)
        .await;
    let config = AppConfig {
        omniroute_base_url: format!("{}/v1", provider.uri()),
        morpheus_vision_model: "vision-model".to_string(),
        ..enabled_config("status-healthy")
    };
    let app = router(AppState::new(config, Some(pool)).expect("state"));

    let (status, body) = get(&app, "/api/morpheus/status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["available"], true);
    assert!(body["checked_at"].is_string());
    assert!(body["last_error"].is_null());
    assert_eq!(body["vision_model"], "vision-model");
    drop(guard);
}
