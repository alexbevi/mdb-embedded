//! Server-side cursor registry for batched query results.
//!
//! Thread-safe registry mapping int64 cursor IDs to batched result sets,
//! with idle expiration and LRU eviction.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;

pub const MAX_BSON_OBJECT_SIZE: i64 = 16 * 1024 * 1024;
pub const MAX_MESSAGE_SIZE: i64 = 48 * 1024 * 1024;
pub const MAX_WRITE_BATCH_SIZE: i64 = 100_000;

struct CursorState {
    _ns: String,
    docs: Py<PyList>,
    docs_len: usize,
    offset: usize,
    batch_size: usize,
    change_stream: Option<Py<PyAny>>,
    _tailable: bool,
    _created_at: Instant,
    last_accessed: Instant,
}

struct Inner {
    cursors: HashMap<i64, CursorState>,
    default_batch_size: usize,
    idle_timeout_secs: u64,
    max_cursors: usize,
}

impl Inner {
    fn generate_id(&self) -> i64 {
        loop {
            let cid = (rand::random::<u64>() >> 1) as i64 | 1;
            if !self.cursors.contains_key(&cid) {
                return cid;
            }
        }
    }

    fn evict_oldest(&mut self) {
        if self.cursors.is_empty() {
            return;
        }
        #[allow(clippy::expect_used)]
        let oldest = *self
            .cursors
            .iter()
            .min_by_key(|(_, s)| s.last_accessed)
            .expect("checked non-empty above")
            .0;
        self.cursors.remove(&oldest);
    }

    fn expire_idle(&mut self) {
        let now = Instant::now();
        let timeout = std::time::Duration::from_secs(self.idle_timeout_secs);
        let expired: Vec<i64> = self
            .cursors
            .iter()
            .filter(|(_, s)| now.duration_since(s.last_accessed) > timeout)
            .map(|(k, _)| *k)
            .collect();
        for k in expired {
            self.cursors.remove(&k);
        }
    }
}

/// Handle exposing whether the background cursor idle-reaper thread is alive.
#[pyclass(module = "smongo._smongo_core")]
struct _ReaperHandle {
    alive: bool,
}

#[pymethods]
impl _ReaperHandle {
    #[new]
    fn new(alive: bool) -> Self {
        Self { alive }
    }

    fn is_alive(&self) -> bool {
        self.alive
    }
}

/// Thread-safe registry of server-side query cursors with batching and expiry.
#[pyclass(module = "smongo._smongo_core")]
pub struct CursorRegistry {
    inner: Arc<Mutex<Inner>>,
    stop: Arc<(Mutex<bool>, Condvar)>,
    reaper_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl CursorRegistry {
    pub(crate) fn create(
        &self,
        _py: Python<'_>,
        ns: &str,
        docs: &Bound<'_, PyList>,
        batch_size: Option<usize>,
    ) -> PyResult<(i64, Py<PyList>)> {
        let mut inner = self.inner.lock();
        let bs = batch_size
            .filter(|&b| b > 0)
            .unwrap_or(inner.default_batch_size);
        let total = docs.len();

        let first_batch: Bound<'_, PyList> = docs.get_slice(0, bs.min(total));

        if total <= bs {
            return Ok((0, first_batch.unbind()));
        }

        if inner.cursors.len() >= inner.max_cursors {
            inner.evict_oldest();
        }
        let cursor_id = inner.generate_id();
        inner.cursors.insert(
            cursor_id,
            CursorState {
                _ns: ns.to_string(),
                docs: docs.clone().unbind(),
                docs_len: total,
                offset: bs,
                batch_size: bs,
                change_stream: None,
                _tailable: false,
                _created_at: Instant::now(),
                last_accessed: Instant::now(),
            },
        );

        Ok((cursor_id, first_batch.unbind()))
    }

    pub(crate) fn get_more(
        &self,
        py: Python<'_>,
        cursor_id: i64,
        batch_size: Option<usize>,
    ) -> PyResult<(Option<i64>, Option<Py<PyList>>)> {
        let mut inner = self.inner.lock();
        let state = match inner.cursors.get_mut(&cursor_id) {
            Some(s) => s,
            None => return Ok((None, None)),
        };
        state.last_accessed = Instant::now();

        let bs = batch_size.unwrap_or(state.batch_size);
        let start = state.offset;
        let end = (start + bs).min(state.docs_len);
        let docs_bound = state.docs.bind(py);
        let batch: Bound<'_, PyList> = docs_bound.get_slice(start, end);
        state.offset = end;

        if end >= state.docs_len {
            inner.cursors.remove(&cursor_id);
            return Ok((Some(0), Some(batch.unbind())));
        }

        Ok((Some(cursor_id), Some(batch.unbind())))
    }

    pub(crate) fn is_tailable(&self, cursor_id: i64) -> bool {
        let inner = self.inner.lock();
        inner
            .cursors
            .get(&cursor_id)
            .is_some_and(|s| s._tailable)
    }

    pub(crate) fn kill(&self, cursor_ids: Vec<i64>) -> Vec<i64> {
        let mut inner = self.inner.lock();
        let mut killed = Vec::new();
        for cid in cursor_ids {
            if inner.cursors.remove(&cid).is_some() {
                killed.push(cid);
            }
        }
        killed
    }

    pub(crate) fn create_change_stream(
        &self,
        py: Python<'_>,
        ns: &str,
        stream: Py<PyAny>,
        batch_size: Option<usize>,
    ) -> PyResult<i64> {
        let mut inner = self.inner.lock();
        let bs = batch_size
            .filter(|&b| b > 0)
            .unwrap_or(inner.default_batch_size);
        if inner.cursors.len() >= inner.max_cursors {
            inner.evict_oldest();
        }
        let cursor_id = inner.generate_id();
        let empty_list = PyList::empty(py);
        inner.cursors.insert(
            cursor_id,
            CursorState {
                _ns: ns.to_string(),
                docs: empty_list.unbind(),
                docs_len: 0,
                offset: 0,
                batch_size: bs,
                change_stream: Some(stream),
                _tailable: true,
                _created_at: Instant::now(),
                last_accessed: Instant::now(),
            },
        );
        Ok(cursor_id)
    }

    pub(crate) fn get_more_change_stream(
        &self,
        py: Python<'_>,
        cursor_id: i64,
        batch_size: Option<usize>,
        max_await_ms: u64,
    ) -> PyResult<(Option<i64>, Option<Py<PyList>>)> {
        let (cs, bs) = {
            let mut inner = self.inner.lock();
            let state = match inner.cursors.get_mut(&cursor_id) {
                Some(s) => s,
                None => return Ok((None, None)),
            };
            let Some(cs_py) = state.change_stream.as_ref() else {
                return Ok((None, None));
            };
            state.last_accessed = Instant::now();
            let cs = cs_py.clone_ref(py);
            let bs = batch_size.unwrap_or(state.batch_size);
            (cs, bs)
        };
        // Lock released -- poll the Python change stream without holding Mutex
        let events = PyList::empty(py);
        let deadline = Instant::now() + std::time::Duration::from_millis(max_await_ms);
        while events.len() < bs {
            let event = cs.bind(py).call_method0("try_next")?;
            if event.is_truthy()? {
                events.append(event)?;
            } else if Instant::now() >= deadline {
                break;
            } else {
                std::thread::sleep(std::time::Duration::from_millis(50));
                if Instant::now() >= deadline {
                    break;
                }
            }
        }
        Ok((Some(cursor_id), Some(events.unbind())))
    }
}

#[pymethods]
impl CursorRegistry {
    #[new]
    #[pyo3(signature = (default_batch_size=101, idle_timeout_sec=600, max_cursors=10_000))]
    fn new(default_batch_size: usize, idle_timeout_sec: u64, max_cursors: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                cursors: HashMap::new(),
                default_batch_size,
                idle_timeout_secs: idle_timeout_sec,
                max_cursors,
            })),
            stop: Arc::new((Mutex::new(false), Condvar::new())),
            reaper_handle: Mutex::new(None),
        }
    }

    fn start_reaper(&self) -> PyResult<()> {
        let mut handle = self.reaper_handle.lock();
        if let Some(h) = handle.as_ref() {
            if !h.is_finished() {
                return Ok(());
            }
        }
        let inner = Arc::clone(&self.inner);
        let stop = Arc::clone(&self.stop);

        {
            let mut stopped = stop.0.lock();
            *stopped = false;
        }

        let h = std::thread::Builder::new()
            .name("cursor-reaper".into())
            .spawn(move || {
                let (lock, cvar) = &*stop;
                loop {
                    let mut stopped = lock.lock();
                    cvar.wait_for(&mut stopped, std::time::Duration::from_secs(60));
                    if *stopped {
                        break;
                    }
                    inner.lock().expire_idle();
                }
            })
            .map_err(|e| PyRuntimeError::new_err(format!("failed to spawn thread: {e}")))?;
        *handle = Some(h);
        Ok(())
    }

    fn _expire_idle(&self) {
        self.inner.lock().expire_idle();
    }

    #[getter]
    fn _reaper_thread(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let handle = self.reaper_handle.lock();
        match handle.as_ref() {
            Some(h) if !h.is_finished() => {
                let obj = _ReaperHandle::new(true);
                Ok(Some(Py::new(py, obj)?.into_any()))
            }
            _ => Ok(None),
        }
    }

    fn stop_reaper(&self) {
        {
            let (lock, cvar) = &*self.stop;
            let mut stopped = lock.lock();
            *stopped = true;
            cvar.notify_all();
        }
        let mut handle = self.reaper_handle.lock();
        if let Some(h) = handle.take() {
            let _ = h.join();
        }
    }

    #[pyo3(name = "create", signature = (ns, docs, batch_size=None))]
    fn create_py(
        &self,
        py: Python<'_>,
        ns: &str,
        docs: &Bound<'_, PyList>,
        batch_size: Option<usize>,
    ) -> PyResult<(i64, Py<PyList>)> {
        self.create(py, ns, docs, batch_size)
    }

    #[pyo3(name = "get_more", signature = (cursor_id, batch_size=None))]
    fn get_more_py(
        &self,
        py: Python<'_>,
        cursor_id: i64,
        batch_size: Option<usize>,
    ) -> PyResult<(Option<i64>, Option<Py<PyList>>)> {
        self.get_more(py, cursor_id, batch_size)
    }

    #[pyo3(name = "is_tailable")]
    fn is_tailable_py(&self, cursor_id: i64) -> bool {
        self.is_tailable(cursor_id)
    }

    #[pyo3(name = "kill")]
    fn kill_py(&self, cursor_ids: Vec<i64>) -> Vec<i64> {
        self.kill(cursor_ids)
    }

    #[pyo3(name = "create_change_stream", signature = (ns, stream, batch_size=None))]
    fn create_change_stream_py(
        &self,
        py: Python<'_>,
        ns: &str,
        stream: Py<PyAny>,
        batch_size: Option<usize>,
    ) -> PyResult<i64> {
        self.create_change_stream(py, ns, stream, batch_size)
    }

    #[pyo3(
        name = "get_more_change_stream",
        signature = (cursor_id, batch_size=None, max_await_ms=1000)
    )]
    fn get_more_change_stream_py(
        &self,
        py: Python<'_>,
        cursor_id: i64,
        batch_size: Option<usize>,
        max_await_ms: u64,
    ) -> PyResult<(Option<i64>, Option<Py<PyList>>)> {
        self.get_more_change_stream(py, cursor_id, batch_size, max_await_ms)
    }
}
