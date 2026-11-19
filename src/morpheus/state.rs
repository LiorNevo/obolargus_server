//! Shared Morpheus runtime state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use obolargus_core::morpheus::error::MorpheusError;
use obolargus_core::morpheus::tools::ToolCatalog;

use crate::config::AppConfig;
use crate::morpheus::provider::OmniRouteClient;

/// Cached provider availability.
#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    /// Whether the last probe succeeded.
    pub available: bool,
    /// Probe instant.
    pub checked_at: DateTime<Utc>,
    /// User-safe failure code/message.
    pub last_error: Option<(String, String)>,
}

/// Shared state for the agent runtime.
pub struct MorpheusState {
    /// Active tool catalog.
    pub catalog: Arc<ToolCatalog>,
    /// Global generation slots.
    pub semaphore: Arc<Semaphore>,
    /// Per-turn cancellation handles.
    cancellations: Arc<Mutex<HashMap<Uuid, CancellationToken>>>,
    /// Concurrent SSE viewers.
    pub sse_streams: AtomicU32,
    /// Cached provider health.
    pub health: Mutex<Option<HealthSnapshot>>,
    /// Provider client.
    pub provider: OmniRouteClient,
}

impl MorpheusState {
    /// Builds the state from configuration.
    pub fn new(config: &AppConfig) -> Result<Self, MorpheusError> {
        let catalog = build_catalog(&config.morpheus_tool_catalog)?;
        Ok(Self {
            catalog: Arc::new(catalog),
            semaphore: Arc::new(Semaphore::new(
                config.morpheus_max_concurrent_turns.max(1) as usize
            )),
            cancellations: Arc::new(Mutex::new(HashMap::new())),
            sse_streams: AtomicU32::new(0),
            health: Mutex::new(None),
            provider: OmniRouteClient::new(config)?,
        })
    }

    /// Registers a cancellation handle for a turn.
    pub fn register_cancellation(&self, turn_id: Uuid) -> CancellationToken {
        let token = CancellationToken::new();
        if let Ok(mut map) = self.cancellations.lock() {
            map.insert(turn_id, token.clone());
        }
        token
    }

    /// Cancels a turn if it is live.
    pub fn cancel(&self, turn_id: Uuid) {
        if let Ok(map) = self.cancellations.lock()
            && let Some(token) = map.get(&turn_id)
        {
            token.cancel();
        }
    }

    /// Removes a finished turn's cancellation handle.
    pub fn remove_cancellation(&self, turn_id: Uuid) {
        if let Ok(mut map) = self.cancellations.lock() {
            map.remove(&turn_id);
        }
    }

    /// Reserves an SSE slot.
    pub fn acquire_stream(&self, limit: u32) -> bool {
        let current = self.sse_streams.fetch_add(1, Ordering::SeqCst) + 1;
        if current > limit {
            self.sse_streams.fetch_sub(1, Ordering::SeqCst);
            false
        } else {
            true
        }
    }

    /// Releases an SSE slot.
    pub fn release_stream(&self) {
        self.sse_streams.fetch_sub(1, Ordering::SeqCst);
    }

    /// Records a provider observation.
    pub fn set_health(&self, available: bool, last_error: Option<(String, String)>) {
        if let Ok(mut health) = self.health.lock() {
            *health = Some(HealthSnapshot {
                available,
                checked_at: Utc::now(),
                last_error,
            });
        }
    }
}

fn build_catalog(value: &str) -> Result<ToolCatalog, MorpheusError> {
    if value.trim() == "all" || value.trim().is_empty() {
        return ToolCatalog::new(&[]);
    }
    let ids: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .collect();
    ToolCatalog::new(&ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_builds_all_empty_and_selected() {
        assert_eq!(build_catalog("all").expect("all").specs().len(), 6);
        assert_eq!(build_catalog("  ").expect("empty").specs().len(), 6);
        assert_eq!(
            build_catalog("assets.search,market_data.lookup")
                .expect("selected")
                .specs()
                .len(),
            2
        );
        assert!(build_catalog("nope").is_err());
    }

    #[test]
    fn cancellation_registry_registers_cancels_and_removes() {
        let state = MorpheusState::new(&crate::config::AppConfig::default()).expect("state");
        let turn = Uuid::new_v4();
        let token = state.register_cancellation(turn);
        assert!(!token.is_cancelled());
        state.cancel(turn);
        assert!(token.is_cancelled());
        state.remove_cancellation(turn);
        state.cancel(turn);
    }

    #[test]
    fn stream_slots_are_bounded_and_released() {
        let state = MorpheusState::new(&crate::config::AppConfig::default()).expect("state");
        assert!(state.acquire_stream(1));
        assert!(!state.acquire_stream(1));
        state.release_stream();
        assert!(state.acquire_stream(1));
        state.release_stream();
    }

    #[test]
    fn health_snapshots_are_recorded() {
        let state = MorpheusState::new(&crate::config::AppConfig::default()).expect("state");
        state.set_health(true, None);
        let health = state.health.lock().expect("health");
        assert!(health.as_ref().expect("snapshot").available);
        assert!(health.as_ref().expect("snapshot").last_error.is_none());
    }
}
