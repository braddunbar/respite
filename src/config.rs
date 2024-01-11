use std::sync::atomic::{AtomicUsize, Ordering};
use triomphe::Arc;

/// Configuration of limits for reading a RESP stream.
/// All values are shared across threads to prevent canceling futures.
#[derive(Debug, Clone)]
pub struct RespConfig {
    /// The maximum blob frame size.
    blob_limit: Arc<AtomicUsize>,

    /// The maximum inline request size.
    inline_limit: Arc<AtomicUsize>,
}

impl Default for RespConfig {
    fn default() -> Self {
        Self {
            inline_limit: Arc::new(AtomicUsize::new(1024 * 64)),
            blob_limit: Arc::new(AtomicUsize::new(512 * 1024 * 1024)),
        }
    }
}

impl RespConfig {
    /// Get the blog frame size limit.
    pub fn blob_limit(&self) -> usize {
        self.blob_limit.load(Ordering::Relaxed)
    }

    /// Set the blob frame size limit.
    pub fn set_blob_limit(&mut self, value: usize) {
        self.blob_limit.store(value, Ordering::Relaxed)
    }

    /// Get the inline request size limit.
    pub fn inline_limit(&self) -> usize {
        self.inline_limit.load(Ordering::Relaxed)
    }

    /// Set the inline request size limit.
    pub fn set_inline_limit(&mut self, value: usize) {
        self.inline_limit.store(value, Ordering::Relaxed)
    }
}
