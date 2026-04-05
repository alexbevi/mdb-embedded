//! Read-write lock primitives for collection-level concurrency.
//!
//! `InlineRwLock` is a plain Rust struct (no PyO3) used internally by
//! `RustLocalCollection` and `RustStreamingCursor`.  Storing it behind an
//! `Arc` avoids `PyCell` borrows on the hot path -- the root cause of
//! `PyBorrowMutError` under concurrent Tokio connections.
//!
//! `ReadWriteLock` is the `#[pyclass]` wrapper exposed to Python.  It holds
//! an `Arc<InlineRwLock>` so that Python callers and Rust callers coordinate
//! on the same underlying state.

use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// MutexForceGuard -- RAII guard for the forget(guard) / force_unlock pattern
// ---------------------------------------------------------------------------

/// RAII guard that releases a `parking_lot::Mutex<()>` on drop.
///
/// The `parking_lot::MutexGuard` is deliberately forgotten after acquisition
/// (so the GIL can be released while the mutex stays locked).  This guard
/// captures the `Arc<Mutex<()>>` and calls [`force_unlock`] in its `Drop`
/// impl, ensuring the mutex is released even on early returns or panics.
pub(crate) struct MutexForceGuard(Arc<Mutex<()>>);

impl MutexForceGuard {
    /// Lock `mutex` (releasing the Python GIL while blocking), forget the
    /// native guard, and return a [`MutexForceGuard`] that will unlock on
    /// drop.
    pub fn acquire(py: Python<'_>, mutex: &Arc<Mutex<()>>) -> Self {
        let m = Arc::clone(mutex);
        py.detach(|| {
            std::mem::forget(m.lock());
        });
        Self(Arc::clone(mutex))
    }

}

impl Drop for MutexForceGuard {
    fn drop(&mut self) {
        // SAFETY: The mutex was locked (guard forgotten) before this type was
        // constructed.  We are on the same thread that acquired the lock.
        unsafe { self.0.force_unlock() };
    }
}

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
