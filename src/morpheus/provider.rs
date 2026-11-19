//! OmniRoute OpenAI-compatible provider client.
//!
//! Only the server talks to the provider. Credentials never reach the browser,
//! and provider bodies are never returned to callers.

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value, json};

use obolargus_core::morpheus::error::{ErrorCode, MorpheusError};

use crate::config::AppConfig;

/// A configured provider client.
#[derive(Debug, Clone)]
pub struct OmniRouteClient {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    text_model: String,
    vision_model: String,
    temperature: f64,
    max_tokens: u32,
}

impl OmniRouteClient {
    /// Builds the client from configuration.
    pub fn new(config: &AppConfig) -> Result<Self, MorpheusError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(
                config.morpheus_connect_timeout_seconds.max(1),
            ))
            .timeout(Duration::from_secs(config.morpheus_turn_max_seconds.max(1)))
            .build()
            .map_err(|error| {
                MorpheusError::provider(ErrorCode::ProviderUnavailable, error.to_string())
            })?;
        let vision_model = if config.morpheus_vision_model.trim().is_empty() {
            config.morpheus_model.clone()
        } else {
            config.morpheus_vision_model.clone()
        };
        Ok(Self {
            client,
            base_url: config.omniroute_base_url.trim_end_matches('/').to_string(),
            api_key: config.omniroute_api_key.clone(),
            text_model: config.morpheus_model.clone(),
            vision_model,
            temperature: config.morpheus_temperature,
            max_tokens: config.morpheus_max_tokens,
        })
    }

    /// Sends a chat-completions request and returns the streaming response.
    pub async fn chat(
        &self,
        messages: &Value,
        tools: Option<Value>,
        vision: bool,
    ) -> Result<reqwest::Response, MorpheusError> {
        let mut body = json!({
            "model": if vision { &self.vision_model } else { &self.text_model },
            "messages": messages,
            "stream": true,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        });
        if let Some(tools) = tools
            && let Some(object) = body.as_object_mut()
        {
            object.insert("tools".to_string(), tools);
            object.insert("tool_choice".to_string(), Value::String("auto".to_string()));
        }
        let url = format!("{}/chat/completions", self.base_url);
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(map_reqwest)?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let detail = response.text().await.unwrap_or_default();
        Err(map_status(status, &detail))
    }

    /// Probes the models endpoint for the status surface.
    pub async fn list_models(&self) -> Result<Value, MorpheusError> {
        let response = self
            .client
            .get(format!("{}/models", self.base_url))
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(map_reqwest)?;
        let status = response.status();
        if !status.is_success() {
            let detail = response.text().await.unwrap_or_default();
            return Err(map_status(status, &detail));
        }
        response.json().await.map_err(|error| {
            MorpheusError::provider(ErrorCode::ProviderBadResponse, error.to_string())
        })
    }
}

fn map_reqwest(error: reqwest::Error) -> MorpheusError {
    if error.is_timeout() {
        MorpheusError::provider(ErrorCode::ProviderTimeout, error.to_string())
    } else {
        MorpheusError::provider(ErrorCode::ProviderUnavailable, error.to_string())
    }
}

fn map_status(status: StatusCode, detail: &str) -> MorpheusError {
    let code = match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ErrorCode::ProviderAuth,
        StatusCode::TOO_MANY_REQUESTS => ErrorCode::ProviderRateLimited,
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => ErrorCode::ProviderTimeout,
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => ErrorCode::ProviderCapability,
        status if status.is_server_error() => ErrorCode::ProviderUnavailable,
        _ => ErrorCode::ProviderBadResponse,
    };
    MorpheusError::provider(code, format!("provider status {status}: {detail}"))
}

/// Builds the OpenAI tool array from the catalog.
pub fn tools_payload(catalog: &obolargus_core::morpheus::tools::ToolCatalog) -> Value {
    let items: Vec<Value> = catalog
        .specs()
        .iter()
        .map(|spec| {
            json!({
                "type": "function",
                "function": {
                    "name": spec.id.replace('.', "__"),
                    "description": format!("{} ({})", spec.description, spec.display_name),
                    "parameters": spec.parameters,
                }
            })
        })
        .collect();
    Value::Array(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use obolargus_core::morpheus::tools::ToolCatalog;

    #[test]
    fn status_mapping_covers_provider_failures() {
        let cases = [
            (StatusCode::UNAUTHORIZED, ErrorCode::ProviderAuth),
            (StatusCode::FORBIDDEN, ErrorCode::ProviderAuth),
            (
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::ProviderRateLimited,
            ),
            (StatusCode::REQUEST_TIMEOUT, ErrorCode::ProviderTimeout),
            (StatusCode::GATEWAY_TIMEOUT, ErrorCode::ProviderTimeout),
            (StatusCode::BAD_REQUEST, ErrorCode::ProviderCapability),
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                ErrorCode::ProviderCapability,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::ProviderUnavailable,
            ),
            (StatusCode::NOT_FOUND, ErrorCode::ProviderBadResponse),
        ];
        for (status, expected) in cases {
            assert_eq!(map_status(status, "detail").code(), expected);
        }
    }

    #[test]
    fn tools_payload_uses_openai_function_names() {
        let catalog = ToolCatalog::new(&[]).expect("catalog");
        let payload = tools_payload(&catalog);
        let items = payload.as_array().expect("array");
        assert!(!items.is_empty());
        for item in items {
            assert_eq!(item["type"], "function");
            let name = item["function"]["name"].as_str().expect("name");
            assert!(!name.contains('.'));
            assert!(item["function"]["parameters"].is_object());
        }
    }
}
