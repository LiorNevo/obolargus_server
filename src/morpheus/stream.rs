//! Incremental parser for the provider's OpenAI-compatible SSE stream.
//!
//! Chunks may split UTF-8 characters, `data:` lines, and JSON payloads. The
//! decoder buffers bytes until a complete event is available and emits typed
//! events for the engine.

use serde_json::Value;

use obolargus_core::morpheus::error::{ErrorCode, MorpheusError};

/// A tool-call fragment carried by a stream delta.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolCallDelta {
    /// Index within the assistant message.
    pub index: usize,
    /// Provider tool-call id, present on the first fragment.
    pub id: Option<String>,
    /// Function name, present on the first fragment.
    pub name: Option<String>,
    /// JSON argument fragment.
    pub arguments: Option<String>,
}

/// One decoded stream event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    /// Answer content.
    Content(String),
    /// A tool-call fragment.
    ToolCall(ToolCallDelta),
    /// Provider finish reason (`stop`, `tool_calls`, `length`, ...).
    FinishReason(String),
    /// Usage-only final chunk.
    Usage(Value),
    /// End marker.
    Done,
}

/// Stateful SSE decoder.
#[derive(Debug, Default)]
pub struct SseDecoder {
    bytes: Vec<u8>,
    data: String,
}

impl SseDecoder {
    /// Creates an empty decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds a network chunk and returns every complete event it produced.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<Result<StreamEvent, MorpheusError>> {
        let mut events = Vec::new();
        self.bytes.extend_from_slice(chunk);
        while let Some(position) = self.bytes.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.bytes.drain(..=position).collect();
            let line = String::from_utf8_lossy(&line);
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                if !self.data.is_empty() {
                    events.push(decode_data(&self.data));
                    self.data.clear();
                }
                continue;
            }
            if let Some(rest) = line.strip_prefix("data:") {
                self.data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
                self.data.push('\n');
            }
            // Comments (`: ping`) and other SSE fields are ignored.
        }
        events
    }

    /// Flushes a final partial event.
    pub fn finish(&mut self) -> Option<Result<StreamEvent, MorpheusError>> {
        if self.data.is_empty() {
            return None;
        }
        let data = std::mem::take(&mut self.data);
        Some(decode_data(&data))
    }
}

fn decode_data(data: &str) -> Result<StreamEvent, MorpheusError> {
    let data = data.trim();
    if data == "[DONE]" {
        return Ok(StreamEvent::Done);
    }
    let value: Value = serde_json::from_str(data).map_err(|error| {
        MorpheusError::provider(
            ErrorCode::ProviderBadResponse,
            format!("invalid stream JSON: {error}"),
        )
    })?;

    if let Some(usage) = value.get("usage").filter(|usage| !usage.is_null())
        && value
            .get("choices")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty)
    {
        return Ok(StreamEvent::Usage(usage.clone()));
    }

    let choice = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first());

    if let Some(reason) = choice
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(Value::as_str)
        .filter(|reason| !reason.is_empty())
    {
        return Ok(StreamEvent::FinishReason(reason.to_string()));
    }

    if let Some(tool_calls) = choice
        .and_then(|choice| choice.get("delta"))
        .and_then(|delta| delta.get("tool_calls"))
        .and_then(Value::as_array)
        && let Some(call) = tool_calls.first()
    {
        let function = call.get("function");
        return Ok(StreamEvent::ToolCall(ToolCallDelta {
            index: call.get("index").and_then(Value::as_u64).unwrap_or(0) as usize,
            id: call.get("id").and_then(Value::as_str).map(str::to_string),
            name: function
                .and_then(|value| value.get("name"))
                .and_then(Value::as_str)
                .map(str::to_string),
            arguments: function
                .and_then(|value| value.get("arguments"))
                .and_then(Value::as_str)
                .map(str::to_string),
        }));
    }

    if let Some(content) = choice
        .and_then(|choice| choice.get("delta"))
        .and_then(|delta| delta.get("content"))
        .and_then(Value::as_str)
        .filter(|content| !content.is_empty())
    {
        return Ok(StreamEvent::Content(content.to_string()));
    }

    // A keep-alive or empty delta.
    Ok(StreamEvent::Content(String::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_content_split_across_chunks() {
        let mut decoder = SseDecoder::new();
        assert!(
            decoder
                .push(b"data: {\"choices\":[{\"delta\":{\"cont")
                .is_empty()
        );
        let events = decoder.push(b"ent\":\"Hel\"}}]}\n\n");
        assert!(matches!(
            events.as_slice(),
            [Ok(StreamEvent::Content(text))] if text == "Hel"
        ));
        let events = decoder.push(b"data: [DONE]\n\n");
        assert!(matches!(events.as_slice(), [Ok(StreamEvent::Done)]));
    }

    #[test]
    fn parses_tool_call_fragments() {
        let mut decoder = SseDecoder::new();
        let events = decoder.push(
            b"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"assets.search\",\"arguments\":\"{\\\"query\\\":\"}}]}}]}\n\n",
        );
        assert!(matches!(events[0], Ok(StreamEvent::ToolCall(_))));
    }

    #[test]
    fn rejects_malformed_json() {
        let mut decoder = SseDecoder::new();
        let events = decoder.push(b"data: {nope}\n\n");
        assert!(events[0].is_err());
    }

    #[test]
    fn ignores_comments_and_handles_multiline_data() {
        let mut decoder = SseDecoder::new();
        let events = decoder.push(b": ping\n\ndata: {\"choices\":[]}\n\n");
        assert_eq!(events.len(), 1);
    }
}
