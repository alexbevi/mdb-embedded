//! Logical session registry for the wire protocol.
//!
//! Thread-safe registry of logical sessions across all connections, with
//! background reaper for idle expiry.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

pub const MAX_SESSIONS: usize = 10_000;

pyo3::create_exception!(smongo._smongo_core, TooManySessions, pyo3::exceptions::PyRuntimeError);

struct SessionEntry {
    _session_id: Py<PyAny>,
    _created_at: Instant,
    last_use: Instant,
}

struct Inner {
    sessions: HashMap<String, SessionEntry>,
    timeout_secs: u64,
    max_sessions: usize,
}

fn extract_key(lsid: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(d) = lsid.cast::<PyDict>() {
        if let Some(id_val) = d.get_item("id")? {
            return Ok(id_val.str()?.to_string());
        }
        return Ok(String::new());
    }
    Ok(lsid.str()?.to_string())
}

/// Handle exposing whether the background logical-session reaper is alive.
#[pyclass(module = "smongo._smongo_core")]
struct _SessionReaperHandle {
    alive: bool,
}

#[pymethods]
impl _SessionReaperHandle {
    fn is_alive(&self) -> bool {
        self.alive
    }
}

/// Thread-safe registry of logical sessions with idle timeout and a reaper thread.
#[pyclass(module = "smongo._smongo_core")]
pub struct SessionRegistry {
    inner: Arc<Mutex<Inner>>,
    stop: Arc<(Mutex<bool>, Condvar)>,
    reaper_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[pymethods]
impl SessionRegistry {
    #[new]
    #[pyo3(signature = (timeout_minutes=30, max_sessions=MAX_SESSIONS))]
    pub fn new(timeout_minutes: u64, max_sessions: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                sessions: HashMap::new(),
                timeout_secs: timeout_minutes * 60,
                max_sessions,
            })),
            stop: Arc::new((Mutex::new(false), Condvar::new())),
            reaper_handle: Mutex::new(None),
        }
    }

    #[getter]
    fn _reaper_thread(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let handle = self.reaper_handle.lock();
        match handle.as_ref() {
            Some(h) if !h.is_finished() => {
                let obj = _SessionReaperHandle { alive: true };
                Ok(Some(Py::new(py, obj)?.into_any()))
            }
            _ => Ok(None),
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
            .name("session-reaper".into())
            .spawn(move || {
                let (lock, cvar) = &*stop;
                loop {
                    let mut stopped = lock.lock();
                    cvar.wait_for(&mut stopped, std::time::Duration::from_secs(60));
                    if *stopped {
                        break;
                    }
                    let now = Instant::now();
                    let mut inn = inner.lock();
                    let timeout = std::time::Duration::from_secs(inn.timeout_secs);
                    let expired: Vec<String> = inn
                        .sessions
                        .iter()
                        .filter(|(_, e)| now.duration_since(e.last_use) > timeout)
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in expired {
                        inn.sessions.remove(&k);
                    }
                }
            })
            .map_err(|e| PyRuntimeError::new_err(format!("failed to spawn thread: {e}")))?;
        *handle = Some(h);
        Ok(())
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

    fn create(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let uuid_mod = crate::cached_modules::uuid_mod(py)?;
        let sid = uuid_mod.call_method0("uuid4")?;
        let key = sid.str()?.to_string();

        let mut inner = self.inner.lock();
        if inner.sessions.len() >= inner.max_sessions {
            return Err(TooManySessions::new_err(format!(
                "session limit {} reached",
                inner.max_sessions
            )));
        }
        inner.sessions.insert(
            key,
            SessionEntry {
                _session_id: sid.clone().unbind(),
                _created_at: Instant::now(),
                last_use: Instant::now(),
            },
        );
        Ok(sid.unbind())
    }

    fn touch(&self, lsid: &Bound<'_, PyAny>) -> PyResult<()> {
        if lsid.is_none() {
            return Ok(());
        }
        let key = extract_key(lsid)?;
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.sessions.get_mut(&key) {
            entry.last_use = Instant::now();
        } else if inner.sessions.len() < inner.max_sessions {
            inner.sessions.insert(
                key,
                SessionEntry {
                    _session_id: lsid.clone().unbind(),
                    _created_at: Instant::now(),
                    last_use: Instant::now(),
                },
            );
        }
        Ok(())
    }

    fn refresh(&self, session_ids: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut inner = self.inner.lock();
        for item in session_ids.try_iter()? {
            let sid = item?;
            let key = extract_key(&sid)?;
            if let Some(entry) = inner.sessions.get_mut(&key) {
                entry.last_use = Instant::now();
            }
        }
        Ok(())
    }

    fn end(&self, session_ids: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut inner = self.inner.lock();
        for item in session_ids.try_iter()? {
            let sid = item?;
            let key = extract_key(&sid)?;
            inner.sessions.remove(&key);
        }
        Ok(())
    }

    fn kill(&self, session_ids: &Bound<'_, PyAny>) -> PyResult<()> {
        self.end(session_ids)
    }

    fn expire(&self) {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        let timeout = std::time::Duration::from_secs(inner.timeout_secs);
        let expired: Vec<String> = inner
            .sessions
            .iter()
            .filter(|(_, e)| now.duration_since(e.last_use) > timeout)
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired {
            inner.sessions.remove(&k);
        }
    }

    fn expire_all(&self) {
        let mut inner = self.inner.lock();
        inner.sessions.clear();
    }

    #[getter]
    fn count(&self) -> usize {
        self.inner.lock().sessions.len()
    }
}
