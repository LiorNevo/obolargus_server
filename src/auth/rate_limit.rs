//! Fixed-window rate limiting.
//!
//! One implementation serves both documented limits: failed sign-in
//! exchanges per client address and new load starts per actor. A caller checks
//! the window before doing work and records a failure only when the exchange
//! actually failed.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

struct Window {
    count: u32,
    started_at: Instant,
}

/// A fixed-window limiter keyed by an arbitrary string.
pub struct RateLimiter {
    limit: u32,
    window: Duration,
    windows: Mutex<HashMap<String, Window>>,
}

impl RateLimiter {
    /// Builds a limiter allowing `limit` events per `window` per key.
    pub fn new(limit: u32, window: Duration) -> Self {
        Self {
            limit: limit.max(1),
            window,
            windows: Mutex::new(HashMap::new()),
        }
    }

    /// Returns `Some(retry_after)` when `key` must wait, or `None` when the
    /// next event is allowed.
    pub fn check(&self, key: &str) -> Option<Duration> {
        let mut windows = self.windows.lock().ok()?;
        let now = Instant::now();
        let window = match windows.get(key) {
            Some(window) if now.duration_since(window.started_at) < self.window => window,
            Some(_) => {
                windows.remove(key);
                return None;
            }
            None => return None,
        };
        if window.count < self.limit {
            return None;
        }
        let elapsed = now.duration_since(window.started_at);
        Some(self.window.saturating_sub(elapsed))
    }

    /// Records one event for `key`.
    pub fn record(&self, key: &str) {
        let Ok(mut windows) = self.windows.lock() else {
            return;
        };
        let now = Instant::now();
        match windows.get_mut(key) {
            Some(window) if now.duration_since(window.started_at) < self.window => {
                window.count = window.count.saturating_add(1);
            }
            Some(window) => {
                window.count = 1;
                window.started_at = now;
            }
            None => {
                windows.insert(
                    key.to_string(),
                    Window {
                        count: 1,
                        started_at: now,
                    },
                );
            }
        }
    }

    /// Clears the window for `key`, for example after a successful sign-in.
    pub fn reset(&self, key: &str) {
        if let Ok(mut windows) = self.windows.lock() {
            windows.remove(key);
        }
    }
}

/// Rounds a retry delay up to whole seconds for a `Retry-After` header.
pub fn retry_after_seconds(delay: Duration) -> u64 {
    delay.as_secs().max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn allows_the_limit_then_blocks() {
        let limiter = RateLimiter::new(5, Duration::from_secs(60));
        for _ in 0..5 {
            assert!(limiter.check("client").is_none());
            limiter.record("client");
        }
        assert!(limiter.check("client").is_some());
        assert!(limiter.check("other-client").is_none());
    }

    #[test]
    fn reset_clears_the_counter() {
        let limiter = RateLimiter::new(1, Duration::from_secs(60));
        limiter.record("client");
        assert!(limiter.check("client").is_some());
        limiter.reset("client");
        assert!(limiter.check("client").is_none());
    }

    #[tokio::test]
    async fn an_expired_window_starts_over() {
        let limiter = RateLimiter::new(1, Duration::from_millis(20));
        limiter.record("client");
        assert!(limiter.check("client").is_some());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(limiter.check("client").is_none());
    }

    #[test]
    fn retry_after_is_at_least_one_second() {
        assert_eq!(retry_after_seconds(Duration::from_millis(10)), 1);
        assert_eq!(retry_after_seconds(Duration::from_secs(3)), 3);
    }
}
