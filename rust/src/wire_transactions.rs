//! Transaction state and commit/abort helpers for wire protocol sessions.
//!
//! Each active transaction holds a reference to a Python `TransactionSession`
//! whose underlying WiredTiger session spans all collection operations.

use std::time::Instant;

use pyo3::prelude::*;

pyo3::create_exception!(smongo._smongo_core, TransactionError, pyo3::exceptions::PyRuntimeError);

// PyO3 simple enums use int discriminants for equality; the Python code only
// ever compares variants (== TransactionState.ACTIVE), never reads .value.
/// Wire-session transaction lifecycle state (none, active, committed, aborted).
#[pyclass(module = "smongo._smongo_core", eq, eq_int, from_py_object)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(clippy::upper_case_acronyms)]
pub enum TransactionState {
    NONE = 0,
    ACTIVE = 1,
    COMMITTED = 2,
    ABORTED = 3,
}

/// Active wire transaction: state, txn number, start time, and Python `TransactionSession`.
#[pyclass(module = "smongo._smongo_core")]
pub struct SessionTransaction {
    #[pyo3(get, set)]
    pub state: TransactionState,
    #[pyo3(get)]
    pub txn_number: i64,
    #[pyo3(get)]
    pub start_time: f64,
    #[pyo3(get)]
    pub txn_session: Py<PyAny>,
    _start_instant: Instant,
}

#[pymethods]
impl SessionTransaction {
    #[new]
    pub fn new(txn_number: i64, txn_session: Py<PyAny>) -> Self {
        Self {
            state: TransactionState::ACTIVE,
            txn_number,
            start_time: 0.0, // monotonic epoch — matches Python's time.monotonic()
            txn_session,
            _start_instant: Instant::now(),
        }
    }
}

#[pyfunction]
pub fn commit_active_transaction(
    py: Python<'_>,
    _local_client: &Bound<'_, PyAny>,
    txn: Option<&Bound<'_, SessionTransaction>>,
) -> PyResult<()> {
    let txn = txn.ok_or_else(|| TransactionError::new_err("No transaction in progress"))?;
    let mut inner = txn.borrow_mut();
    if inner.state != TransactionState::ACTIVE {
        return Err(TransactionError::new_err("No transaction in progress"));
    }
    inner.txn_session.bind(py).call_method0("commit")?;
    inner.state = TransactionState::COMMITTED;
    Ok(())
}

#[pyfunction]
pub fn abort_active_transaction(
    py: Python<'_>,
    txn: Option<&Bound<'_, SessionTransaction>>,
) -> PyResult<i64> {
    let txn = txn.ok_or_else(|| TransactionError::new_err("No transaction in progress"))?;
    let mut inner = txn.borrow_mut();
    if inner.state != TransactionState::ACTIVE {
        return Err(TransactionError::new_err("No transaction in progress"));
    }
    inner.txn_session.bind(py).call_method0("rollback")?;
    inner.state = TransactionState::ABORTED;
    Ok(0)
}
