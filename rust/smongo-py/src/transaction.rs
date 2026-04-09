//! Rust port of `smongo.storage.transaction.TransactionSession`.
//!
//! Uses Python `threading.local()` for the thread-local state so that
//! both Rust and Python code can read `get_active_txn_session()`.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

use crate::wt_bridge::RustWtSession;
use crate::wt_bridge::WtResultExt;

static TXN_STATE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn get_txn_state(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let cached = TXN_STATE.get_or_try_init(py, || {
        let m = crate::cached_modules::smongo_storage_txn(py)?;
        Ok::<_, PyErr>(m.getattr("_txn_state")?.unbind())
    })?;
    Ok(cached.bind(py).clone())
}

/// Active transaction handle bound to a WiredTiger session.
#[pyclass]
pub struct RustTransactionSession {
    session: Py<RustWtSession>,
}

// SAFETY: RustTransactionSession holds a Py<RustWtSession>.  Send+Sync is
// required by PyO3 for #[pyclass].  The session is bound to a single logical
// transaction and accessed only by the thread that owns the transaction.
// Under free-threaded Python, PyO3's borrow checking prevents concurrent access.
unsafe impl Send for RustTransactionSession {}
unsafe impl Sync for RustTransactionSession {}

#[pymethods]
impl RustTransactionSession {
    #[new]
    pub fn new(py: Python<'_>, conn: &Bound<'_, PyAny>) -> PyResult<Self> {
        let rs_session = conn.call_method0("open_session")?;
        let session: Py<RustWtSession> = rs_session.extract()?;
        {
            let bound = session.bind(py).borrow();
            let s = bound.get().py()?;
            s.begin_transaction(None).py()?;
        }
        Ok(Self { session })
    }

    #[getter]
    fn session(&self, py: Python<'_>) -> Py<RustWtSession> {
        self.session.clone_ref(py)
    }

    fn activate(&self, py: Python<'_>) -> PyResult<()> {
        let state = get_txn_state(py)?;
        state.setattr("session", self.session.bind(py))?;
        let raw = self
            .session
            .bind(py)
            .borrow()
            .get()
            .map(|s| s.raw_ptr())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.message))?;
        crate::wt_bridge::set_txn_session_override(raw);
        Ok(())
    }

    fn deactivate(&self, py: Python<'_>) -> PyResult<()> {
        let state = get_txn_state(py)?;
        state.setattr("session", py.None())?;
        crate::wt_bridge::clear_txn_session_override();
        Ok(())
    }

    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        {
            let bound = self.session.bind(py).borrow();
            let s = bound.get().py()?;
            s.commit_transaction(None).py()?;
        }
        self.deactivate(py)?;
        self.session.bind(py).call_method0("close")?;
        Ok(())
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        {
            let bound = self.session.bind(py).borrow();
            let s = bound.get().py()?;
            s.rollback_transaction(None).py()?;
        }
        self.deactivate(py)?;
        self.session.bind(py).call_method0("close")?;
        Ok(())
    }
}

#[pyfunction]
pub fn get_active_txn_session(py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
    let state = get_txn_state(py)?;
    match state.getattr("session") {
        Ok(s) if !s.is_none() => Ok(Some(s.unbind())),
        _ => Ok(None),
    }
}
