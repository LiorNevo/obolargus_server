//! Shared automation server state: node registry and cancellation handles.

use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use obolargus_core::automation::nodes::{NodeRegistry, default_registry};

/// Process-wide automation handles stored in [`crate::AppState`].
pub struct AutomationState {
    /// Executable node registry.
    pub registry: Arc<NodeRegistry>,
    cancellations: Mutex<HashMap<Uuid, CancellationToken>>,
    /// Active SSE stream count.
    pub sse_streams: Arc<AtomicU32>,
}

impl AutomationState {
    /// Builds state with the built-in node registry.
    pub fn new() -> Self {
        Self {
            registry: Arc::new(default_registry()),
            cancellations: Mutex::new(HashMap::new()),
            sse_streams: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Builds state with a deployment capability allowlist.
    ///
    /// The allowlist is the comma-separated `AUTOMATION_ENABLED_CAPABILITIES`
    /// value; `all` accepts every built-in node capability.
    pub fn with_capabilities(allowlist: &str) -> Self {
        Self {
            registry: Arc::new(default_registry().with_capabilities(allowlist)),
            cancellations: Mutex::new(HashMap::new()),
            sse_streams: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Registers (or replaces) a run cancellation token.
    pub fn register_cancel(&self, run_id: Uuid, token: CancellationToken) {
        if let Ok(mut guard) = self.cancellations.lock() {
            guard.insert(run_id, token);
        }
    }

    /// Cancels a run if it is being executed in this process.
    pub fn cancel(&self, run_id: Uuid) {
        if let Ok(guard) = self.cancellations.lock()
            && let Some(token) = guard.get(&run_id)
        {
            token.cancel();
        }
    }

    /// Removes a run's cancellation token.
    pub fn release_cancel(&self, run_id: Uuid) {
        if let Ok(mut guard) = self.cancellations.lock() {
            guard.remove(&run_id);
        }
    }
}

impl Default for AutomationState {
    fn default() -> Self {
        Self::new()
    }
}
