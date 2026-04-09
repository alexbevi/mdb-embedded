//! Thread-local transaction session marker (Python `smongo.storage.transaction`).

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

static TXN_STATE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn get_txn_state(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let cached = TXN_STATE.get_or_try_init(py, || {
        let m = crate::cached_modules::smongo_storage_txn(py)?;
        Ok::<_, PyErr>(m.getattr("_txn_state")?.unbind())
    })?;
    Ok(cached.bind(py).clone())
}

#[pyfunction]
pub fn get_active_txn_session(py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
    let state = get_txn_state(py)?;
    match state.getattr("session") {
        Ok(s) if !s.is_none() => Ok(Some(s.unbind())),
        _ => Ok(None),
    }
}
