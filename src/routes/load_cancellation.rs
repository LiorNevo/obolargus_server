//! In-process cancellation flags for background range loads.
//!
//! The registry is intentionally process-local: a background task cannot
//! survive a server restart, so a new process starts with no cancellable
//! runs. The stop endpoint reports `stopped: false` for unknown groups and
//! the UI uses that to dismiss an interrupted run.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use uuid::Uuid;

fn registry() -> &'static Mutex<HashMap<Uuid, Arc<AtomicBool>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<Uuid, Arc<AtomicBool>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Registers a cancellable range load.
pub fn register(group_id: Uuid) -> Result<Arc<AtomicBool>, ()> {
    let flag = Arc::new(AtomicBool::new(false));
    registry()
        .lock()
        .map_err(|_| ())?
        .insert(group_id, Arc::clone(&flag));
    Ok(flag)
}

/// Requests cancellation and reports whether the group was active.
pub fn cancel(group_id: Uuid) -> bool {
    let Ok(registry) = registry().lock() else {
        return false;
    };
    match registry.get(&group_id) {
        Some(flag) => {
            flag.store(true, Ordering::Relaxed);
            true
        }
        None => false,
    }
}

/// Removes a finished group from the registry.
pub fn finish(group_id: Uuid) {
    if let Ok(mut registry) = registry().lock() {
        registry.remove(&group_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn registered_groups_can_be_cancelled_and_removed() {
        let group_id = Uuid::new_v4();
        let flag = register(group_id).expect("register");
        assert!(!flag.load(Ordering::Relaxed));

        assert!(cancel(group_id));
        assert!(flag.load(Ordering::Relaxed));

        finish(group_id);
        assert!(!cancel(group_id));
    }

    #[test]
    fn cancelling_an_unknown_group_reports_false() {
        assert!(!cancel(Uuid::new_v4()));
    }
}
