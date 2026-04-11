//! Read-write lock primitives for collection-level concurrency.
//!
//! `InlineRwLock` is a plain Rust struct (no PyO3) used internally on
//! collection- and cursor-related hot paths.  Storing it behind an
//! `Arc` avoids `PyCell` borrows on the hot path -- the root cause of
//! `PyBorrowMutError` under concurrent Tokio connections.
//!
//! `ReadWriteLock` is the `#[pyclass]` wrapper exposed to Python.  It holds
//! an `Arc<InlineRwLock>` so that Python callers and Rust callers coordinate
//! on the same underlying state.

use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use pyo3::prelude::*;

struct LockState {
    readers: u32,
    writer: bool,
}

// ---------------------------------------------------------------------------
// InlineRwLock -- plain Rust, no PyO3, no GIL dependency
// ---------------------------------------------------------------------------

/// Concurrent-reader / exclusive-writer lock backed by `parking_lot`.
///
/// All methods block the calling OS thread (no GIL interaction).  Callers
/// that want to release the GIL while waiting should wrap the call in
/// `py.allow_threads` / `py.detach`.
pub struct InlineRwLock {
    state: Mutex<LockState>,
    cond: Condvar,
}

impl Default for InlineRwLock {
    fn default() -> Self {
        Self::new()
    }
}

impl InlineRwLock {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(LockState {
                readers: 0,
                writer: false,
            }),
            cond: Condvar::new(),
        }
    }

    pub fn acquire_read(&self) {
        let mut state = self.state.lock();
        while state.writer {
            self.cond.wait(&mut state);
        }
        state.readers += 1;
    }

    pub fn release_read(&self) {
        let mut state = self.state.lock();
        state.readers -= 1;
        if state.readers == 0 {
            self.cond.notify_all();
        }
    }

    pub fn acquire_write(&self) {
        let mut state = self.state.lock();
        while state.writer || state.readers > 0 {
            self.cond.wait(&mut state);
        }
        state.writer = true;
    }

    pub fn release_write(&self) {
        let mut state = self.state.lock();
        state.writer = false;
        self.cond.notify_all();
    }
}

// ---------------------------------------------------------------------------
// ReadWriteLock -- #[pyclass] wrapper that delegates to an Arc<InlineRwLock>
// ---------------------------------------------------------------------------

/// Python-visible read-write lock.  Shares the same underlying state as the
/// `Arc<InlineRwLock>` used by `RustLocalCollection`, so Python callers
/// (e.g. the legacy `StreamingCursor`) and Rust callers coordinate correctly.
#[pyclass(module = "smongo._smongo_core")]
pub struct ReadWriteLock {
    pub(crate) inner: Arc<InlineRwLock>,
}

impl Default for ReadWriteLock {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl ReadWriteLock {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InlineRwLock::new()),
        }
    }

    pub fn acquire_read(&self, py: Python<'_>) {
        let inner = Arc::clone(&self.inner);
        py.detach(|| inner.acquire_read());
    }

    pub fn release_read(&self) {
        self.inner.release_read();
    }

    pub fn acquire_write(&self, py: Python<'_>) {
        let inner = Arc::clone(&self.inner);
        py.detach(|| inner.acquire_write());
    }

    pub fn release_write(&self) {
        self.inner.release_write();
    }
}

impl ReadWriteLock {
    /// Wrap an existing `Arc<InlineRwLock>` so that Python callers share the
    /// same lock state as Rust callers.
    pub fn from_arc(inner: Arc<InlineRwLock>) -> Self {
        Self { inner }
    }
}
