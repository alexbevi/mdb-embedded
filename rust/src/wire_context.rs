//! Per-connection server context helpers.
//!
//! Provides namespace validation, parameter store, connection counter,
//! free-monitoring state, last-write tracking, and the full `ConnectionContext`
//! that wraps per-connection state including DB cache and transaction lifecycle.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::local_collection::RustLocalCollection;
use crate::storage_engine::{RustLocalClient, RustLocalDB};
use crate::transaction::RustTransactionSession;
use crate::wire_profiler::{OperationTracker, Profiler, TopStats};
use crate::wire_sessions::SessionRegistry;
use crate::wire_transactions::{SessionTransaction, TransactionError, TransactionState};

pyo3::create_exception!(
    smongo._smongo_core,
    NamespaceError,
    pyo3::exceptions::PyValueError
);

// ── Cached Python imports (resolved once at server startup) ──────────

/// Pre-resolved Python module/object references that would otherwise
/// require a `py.import()` on every command.  Built once in
/// `RustWireServer::new()`, shared across all connections via `Arc`.
pub(crate) struct CachedImports {
    pub user_store: Py<PyAny>,
    pub user_store_lock: Py<PyAny>,
    pub audit_mod: Py<PyAny>,
    pub topology_pid: Py<PyAny>,
    pub git_version: Py<PyAny>,
    pub server_start: f64,
    pub help_dict: Py<PyAny>,
    pub handlers: Py<PyDict>,
}

const MAX_DB_NAME_LEN: usize = 64;
const MAX_COLL_NAME_LEN: usize = 120;

fn has_invalid_ns_char(s: &str) -> bool {
    s.bytes().any(|b| b == 0 || b == b'/' || b == b'\\')
}

#[pyfunction]
pub fn validate_namespace(db_name: &str, coll_name: &str) -> PyResult<()> {
    // Database name checks
    if db_name.is_empty() || db_name != db_name.trim() {
        return Err(NamespaceError::new_err(format!(
            "invalid database name: {db_name:?}"
        )));
    }
    if db_name.len() > MAX_DB_NAME_LEN {
        return Err(NamespaceError::new_err(format!(
            "database name exceeds {MAX_DB_NAME_LEN} characters"
        )));
    }
    if has_invalid_ns_char(db_name) {
        return Err(NamespaceError::new_err(format!(
            "database name contains forbidden characters: {db_name:?}"
        )));
    }
    if db_name.contains('.') {
        return Err(NamespaceError::new_err(format!(
            "database name cannot contain '.': {db_name:?}"
        )));
    }
    if db_name.starts_with('$') {
        return Err(NamespaceError::new_err(format!(
            "database name cannot start with '$': {db_name:?}"
        )));
    }

    // Collection name checks
    if coll_name.is_empty() || coll_name != coll_name.trim() {
        return Err(NamespaceError::new_err(format!(
            "invalid collection name: {coll_name:?}"
        )));
    }
    if coll_name.len() > MAX_COLL_NAME_LEN {
        return Err(NamespaceError::new_err(format!(
            "collection name exceeds {MAX_COLL_NAME_LEN} characters"
        )));
    }
    if has_invalid_ns_char(coll_name) {
        return Err(NamespaceError::new_err(format!(
            "collection name contains forbidden characters: {coll_name:?}"
        )));
    }
    if coll_name.starts_with('$') && coll_name != "$cmd" && coll_name != "$external" {
        return Err(NamespaceError::new_err(format!(
            "collection name cannot start with '$': {coll_name:?}"
        )));
    }
    if coll_name.contains("..") {
        return Err(NamespaceError::new_err(format!(
            "collection name cannot contain '..': {coll_name:?}"
        )));
    }
    Ok(())
}

// ── LastWriteResult ─────────────────────────────────────────────────

/// Last write operation summary: counts, errors, upserted id, and writeErrors list.
#[pyclass(module = "smongo._smongo_core")]
pub struct LastWriteResult {
    #[pyo3(get, set)]
    pub op: String,
    #[pyo3(get, set)]
    pub n: i64,
    #[pyo3(get, set)]
    pub n_modified: i64,
    #[pyo3(get, set)]
    pub err: Option<String>,
    #[pyo3(get, set)]
    pub upserted_id: Py<PyAny>,
    #[pyo3(get, set)]
    pub write_errors: Py<PyList>,
}

#[pymethods]
impl LastWriteResult {
    #[new]
    #[pyo3(signature = (*, op="unknown".to_string(), n=0, n_modified=0, err=None, upserted_id=None, write_errors=None))]
    fn new(
        py: Python<'_>,
        op: String,
        n: i64,
        n_modified: i64,
        err: Option<String>,
        upserted_id: Option<Py<PyAny>>,
        write_errors: Option<&Bound<'_, PyList>>,
    ) -> Self {
        Self {
            op,
            n,
            n_modified,
            err,
            upserted_id: upserted_id.unwrap_or_else(|| py.None()),
            write_errors: write_errors
                .map(|l| l.clone().unbind())
                .unwrap_or_else(|| PyList::empty(py).unbind()),
        }
    }
}

// ── ParameterStore ──────────────────────────────────────────────────

/// Mutable server parameter map (FCV, thresholds, limits) shared across connections.
#[pyclass(module = "smongo._smongo_core")]
pub struct ParameterStore {
    params: Mutex<HashMap<String, Py<PyAny>>>,
}

#[pymethods]
impl ParameterStore {
    #[new]
    fn new(py: Python<'_>) -> PyResult<Self> {
        let mut map = HashMap::new();
        // Seed with default values matching the Python implementation
        let fcv = PyDict::new(py);
        fcv.set_item("version", "7.0")?;
        map.insert(
            "featureCompatibilityVersion".into(),
            fcv.into_any().unbind(),
        );
        let insert_int = |m: &mut HashMap<String, Py<PyAny>>, k: &str, v: i64| -> PyResult<()> {
            m.insert(k.into(), v.into_pyobject(py)?.unbind().into());
            Ok(())
        };
        let insert_float = |m: &mut HashMap<String, Py<PyAny>>, k: &str, v: f64| -> PyResult<()> {
            m.insert(k.into(), v.into_pyobject(py)?.unbind().into());
            Ok(())
        };
        let insert_bool = |m: &mut HashMap<String, Py<PyAny>>, k: &str, v: bool| -> PyResult<()> {
            m.insert(k.into(), v.into_pyobject(py)?.to_owned().unbind().into());
            Ok(())
        };
        insert_int(&mut map, "logLevel", 0)?;
        map.insert(
            "authenticationMechanisms".into(),
            PyList::empty(py).into_any().unbind(),
        );
        insert_bool(&mut map, "quiet", false)?;
        insert_bool(&mut map, "notablescan", false)?;
        insert_int(&mut map, "maxTransactionLockRequestTimeoutMillis", 5000)?;
        insert_int(&mut map, "transactionLifetimeLimitSeconds", 60)?;
        insert_int(&mut map, "cursorTimeoutMillis", 600_000)?;
        insert_int(
            &mut map,
            "internalQueryExecMaxBlockingSortBytes",
            104_857_600,
        )?;
        insert_bool(&mut map, "failIndexKeyTooLong", true)?;
        insert_int(&mut map, "slowOpThresholdMs", 100)?;
        insert_float(&mut map, "slowOpSampleRate", 1.0)?;
        Ok(Self {
            params: Mutex::new(map),
        })
    }

    fn get(&self, py: Python<'_>, name: &str) -> Py<PyAny> {
        let params = self.params.lock();
        params
            .get(name)
            .map(|v| v.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    fn get_all(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let params = self.params.lock();
        let d = PyDict::new(py);
        for (k, v) in params.iter() {
            d.set_item(k, v.bind(py))?;
        }
        Ok(d.unbind())
    }

    fn set(&self, _py: Python<'_>, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut params = self.params.lock();
        params.insert(name.to_string(), value.clone().unbind());
        Ok(())
    }
}

// ── ConnectionCounter ───────────────────────────────────────────────

/// Live, cumulative, and max-allowed wire connection counts for the server.
#[pyclass(module = "smongo._smongo_core")]
pub struct ConnectionCounter {
    state: Mutex<(i64, i64, i64)>, // (current, total, max)
}

#[pymethods]
impl ConnectionCounter {
    #[new]
    #[pyo3(signature = (max_connections=1024))]
    fn new(max_connections: i64) -> Self {
        Self {
            state: Mutex::new((0, 0, max_connections)),
        }
    }

    fn connect(&self) {
        let mut s = self.state.lock();
        s.0 += 1;
        s.1 += 1;
    }

    fn disconnect(&self) {
        let mut s = self.state.lock();
        s.0 = (s.0 - 1).max(0);
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let s = self.state.lock();
        let d = PyDict::new(py);
        d.set_item("current", s.0)?;
        d.set_item("available", s.2 - s.0)?;
        d.set_item("totalCreated", s.1)?;
        Ok(d.unbind())
    }
}

// ── FreeMonitoringState ─────────────────────────────────────────────

/// Free monitoring enrollment state (`enabled` / `disabled`) for client metadata.
#[pyclass(module = "smongo._smongo_core")]
pub struct FreeMonitoringState {
    state_val: Mutex<String>,
}

#[pymethods]
impl FreeMonitoringState {
    #[new]
    fn new() -> Self {
        Self {
            state_val: Mutex::new("disabled".to_string()),
        }
    }

    #[getter]
    fn state(&self) -> String {
        self.state_val.lock().clone()
    }

    fn set(&self, action: &str) {
        let mut s = self.state_val.lock();
        match action {
            "enable" => *s = "enabled".to_string(),
            "disable" => *s = "disabled".to_string(),
            _ => {}
        }
    }
}

// ── ConnectionContext ───────────────────────────────────────────────

use std::sync::atomic::{AtomicI64, Ordering};

static TXN_NUMBER_GEN: AtomicI64 = AtomicI64::new(1);

/// Per-TCP-connection context: DB cache, transaction, registries, and shared services.
#[pyclass(module = "smongo._smongo_core")]
pub struct ConnectionContext {
    #[pyo3(get)]
    pub local_client: Py<PyAny>,
    #[pyo3(get)]
    pub connection_id: i64,
    #[pyo3(get)]
    pub address: Py<PyAny>,
    #[pyo3(get)]
    pub cursor_registry: Py<PyAny>,
    #[pyo3(get)]
    pub sync_mgr: Py<PyAny>,
    dbs: Mutex<HashMap<String, Py<RustLocalDB>>>,
    #[pyo3(get, set)]
    pub compressor_id: Py<PyAny>,
    #[pyo3(get)]
    pub session_registry: Py<PyAny>,
    #[pyo3(get)]
    pub op_tracker: Py<PyAny>,
    #[pyo3(get)]
    pub param_store: Py<PyAny>,
    #[pyo3(get)]
    pub top_stats: Py<PyAny>,
    #[pyo3(get)]
    pub profiler: Py<PyAny>,
    #[pyo3(get)]
    pub log_buffer: Py<PyAny>,
    #[pyo3(get)]
    pub conn_counter: Py<PyAny>,
    #[pyo3(get)]
    pub free_monitoring: Py<PyAny>,
    #[pyo3(get, set)]
    pub last_write: Py<PyAny>,
    #[pyo3(get, set)]
    pub last_plan_summary: String,
    txn_sessions: Mutex<HashMap<String, Py<PyAny>>>,
    pub(crate) authenticated_user: Mutex<Option<String>>,
    pub(crate) authenticated_db: Mutex<Option<String>>,
    pub(crate) authenticated_roles: Mutex<Vec<(String, String)>>,
    pub(crate) scram_conversation: Mutex<Option<crate::scram::ScramConversation>>,
    pub(crate) cached: Option<Arc<CachedImports>>,
}

impl ConnectionContext {
    pub(crate) fn cached_imports(&self) -> PyResult<&CachedImports> {
        self.cached.as_deref().ok_or_else(|| {
            PyRuntimeError::new_err("CachedImports not available outside wire server")
        })
    }

    /// Typed DB accessor -- downcasts `local_client` to `RustLocalClient` and
    /// calls `get_db` directly, bypassing Python method dispatch.
    pub(crate) fn get_db_typed(&self, py: Python<'_>, db_name: &str) -> PyResult<Py<RustLocalDB>> {
        let mut dbs = self.dbs.lock();
        if let Some(db) = dbs.get(db_name) {
            return Ok(db.clone_ref(py));
        }
        let lc = self.local_client.bind(py);
        let rs_client: &Bound<'_, RustLocalClient> = lc.cast()?;
        let db = rs_client.borrow().get_db_inner(py, db_name)?;
        dbs.insert(db_name.to_string(), db.clone_ref(py));
        Ok(db)
    }

    /// Typed collection accessor -- downcasts through `RustLocalClient` →
    /// `RustLocalDB` → `RustLocalCollection`, zero Python dispatch.
    pub(crate) fn get_collection_typed(
        &self,
        py: Python<'_>,
        db_name: &str,
        coll_name: &str,
    ) -> PyResult<Py<RustLocalCollection>> {
        validate_namespace(db_name, coll_name)?;
        let db_py = self.get_db_typed(py, db_name)?;
        let db = db_py.bind(py).borrow();
        db.get_collection_typed(py, coll_name)
    }
}

#[pymethods]
impl ConnectionContext {
    #[new]
    #[pyo3(signature = (
        local_client, connection_id, address, cursor_registry,
        sync_mgr=None, session_registry=None, op_tracker=None,
        param_store=None, top_stats=None, profiler=None,
        log_buffer=None, conn_counter=None, free_monitoring=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        local_client: &Bound<'_, PyAny>,
        connection_id: i64,
        address: &Bound<'_, PyAny>,
        cursor_registry: &Bound<'_, PyAny>,
        sync_mgr: Option<&Bound<'_, PyAny>>,
        session_registry: Option<&Bound<'_, PyAny>>,
        op_tracker: Option<&Bound<'_, PyAny>>,
        param_store: Option<&Bound<'_, PyAny>>,
        top_stats: Option<&Bound<'_, PyAny>>,
        profiler: Option<&Bound<'_, PyAny>>,
        log_buffer: Option<&Bound<'_, PyAny>>,
        conn_counter: Option<&Bound<'_, PyAny>>,
        free_monitoring: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let sr = match session_registry {
            Some(s) => s.clone().unbind(),
            None => Py::new(
                py,
                SessionRegistry::new(30, crate::wire_sessions::MAX_SESSIONS),
            )?
            .into_any(),
        };
        let ot = match op_tracker {
            Some(o) => o.clone().unbind(),
            None => Py::new(py, OperationTracker::new())?.into_any(),
        };
        let ps = match param_store {
            Some(p) => p.clone().unbind(),
            None => Py::new(py, ParameterStore::new(py)?)?.into_any(),
        };
        let ts = match top_stats {
            Some(t) => t.clone().unbind(),
            None => Py::new(py, TopStats::new())?.into_any(),
        };
        let prof = match profiler {
            Some(p) => p.clone().unbind(),
            None => Py::new(py, Profiler::new(0, 100, 4096))?.into_any(),
        };
        let lb = match log_buffer {
            Some(l) => l.clone().unbind(),
            None => crate::cached_modules::smongo_wire_context(py)?
                .getattr("LogBuffer")?
                .call0()?
                .unbind(),
        };
        let cc = match conn_counter {
            Some(c) => c.clone().unbind(),
            None => Py::new(py, ConnectionCounter::new(1024))?.into_any(),
        };
        let fm = match free_monitoring {
            Some(f) => f.clone().unbind(),
            None => Py::new(py, FreeMonitoringState::new())?.into_any(),
        };

        Ok(Self {
            local_client: local_client.clone().unbind(),
            connection_id,
            address: address.clone().unbind(),
            cursor_registry: cursor_registry.clone().unbind(),
            sync_mgr: sync_mgr
                .map(|s| s.clone().unbind())
                .unwrap_or_else(|| py.None()),
            dbs: Mutex::new(HashMap::new()),
            compressor_id: py.None(),
            session_registry: sr,
            op_tracker: ot,
            param_store: ps,
            top_stats: ts,
            profiler: prof,
            log_buffer: lb,
            conn_counter: cc,
            free_monitoring: fm,
            last_write: py.None(),
            last_plan_summary: String::new(),
            txn_sessions: Mutex::new(HashMap::new()),
            authenticated_user: Mutex::new(None),
            authenticated_db: Mutex::new(None),
            authenticated_roles: Mutex::new(Vec::new()),
            scram_conversation: Mutex::new(None),
            cached: None,
        })
    }

    #[getter]
    fn authenticated_user(&self) -> Option<String> {
        self.authenticated_user.lock().clone()
    }

    #[setter]
    fn set_authenticated_user(&self, val: Option<String>) {
        *self.authenticated_user.lock() = val;
    }

    #[getter]
    fn authenticated_db(&self) -> Option<String> {
        self.authenticated_db.lock().clone()
    }

    #[setter]
    fn set_authenticated_db(&self, val: Option<String>) {
        *self.authenticated_db.lock() = val;
    }

    fn is_authenticated(&self) -> bool {
        self.authenticated_user.lock().is_some()
    }

    #[getter]
    fn authenticated_roles(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let roles = self.authenticated_roles.lock();
        let items: Vec<(String, String)> = roles.clone();
        let list = PyList::new(py, items.iter().map(|(r, d)| (r.as_str(), d.as_str())))?;
        Ok(list.unbind())
    }

    fn get_db(&self, py: Python<'_>, db_name: &str) -> PyResult<Py<PyAny>> {
        Ok(self.get_db_typed(py, db_name)?.into_any())
    }

    fn get_collection(
        &self,
        py: Python<'_>,
        db_name: &str,
        coll_name: &str,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .get_collection_typed(py, db_name, coll_name)?
            .into_any())
    }

    fn list_known_dbs(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let dbs = self.dbs.lock();
        let keys: Vec<&str> = dbs.keys().map(|s| s.as_str()).collect();
        let list = PyList::new(py, &keys)?;
        Ok(list.unbind())
    }

    fn start_transaction(&self, py: Python<'_>, lsid: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let key = session_key(py, lsid)?;
        let mut sessions = self.txn_sessions.lock();
        if let Some(existing) = sessions.get(&key) {
            if let Ok(st) = existing.bind(py).cast::<SessionTransaction>() {
                if st.borrow().state == TransactionState::ACTIVE {
                    return Err(TransactionError::new_err(
                        "Transaction already in progress on this session",
                    ));
                }
            }
        }
        let lc = self.local_client.bind(py);
        let conn = lc.getattr("conn")?;

        // Use RustTransactionSession when the connection is a Rust type,
        // otherwise fall back to the Python TransactionSession.
        let storage_txn = if let Ok(rs_txn) = RustTransactionSession::new(py, &conn) {
            Py::new(py, rs_txn)?.into_any()
        } else {
            let cls =
                crate::cached_modules::smongo_storage_txn(py)?.getattr("TransactionSession")?;
            cls.call1((&conn,))?.unbind()
        };
        storage_txn.bind(py).call_method0("activate")?;

        let txn_number = TXN_NUMBER_GEN.fetch_add(1, Ordering::Relaxed);
        let txn = Py::new(py, SessionTransaction::new(txn_number, storage_txn))?;
        let txn_any = txn.clone_ref(py).into_any();
        sessions.insert(key, txn_any);
        Ok(txn.into_any())
    }

    fn get_transaction(&self, py: Python<'_>, lsid: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if lsid.is_none() {
            return Ok(py.None());
        }
        let key = session_key(py, lsid)?;
        let sessions = self.txn_sessions.lock();
        if let Some(txn) = sessions.get(&key) {
            if let Ok(st) = txn.bind(py).cast::<SessionTransaction>() {
                if st.borrow().state == TransactionState::ACTIVE {
                    return Ok(txn.clone_ref(py));
                }
            }
        }
        Ok(py.None())
    }

    fn commit_transaction(&self, py: Python<'_>, lsid: &Bound<'_, PyAny>) -> PyResult<()> {
        let key = session_key(py, lsid)?;
        let sessions = self.txn_sessions.lock();
        let txn = sessions.get(&key).map(|t| t.clone_ref(py));
        drop(sessions);
        let lc = self.local_client.bind(py);
        match txn {
            Some(t) => {
                let bound = t.bind(py);
                let st: &Bound<'_, SessionTransaction> = bound.cast()?;
                crate::wire_transactions::commit_active_transaction(py, lc, Some(st))?;
            }
            None => {
                crate::wire_transactions::commit_active_transaction(py, lc, None)?;
            }
        }
        Ok(())
    }

    fn abort_transaction(&self, py: Python<'_>, lsid: &Bound<'_, PyAny>) -> PyResult<i64> {
        let key = session_key(py, lsid)?;
        let sessions = self.txn_sessions.lock();
        let txn = sessions.get(&key).map(|t| t.clone_ref(py));
        drop(sessions);
        match txn {
            Some(t) => {
                let bound = t.bind(py);
                let st: &Bound<'_, SessionTransaction> = bound.cast()?;
                crate::wire_transactions::abort_active_transaction(py, Some(st))
            }
            None => crate::wire_transactions::abort_active_transaction(py, None),
        }
    }

    #[getter]
    fn _dbs(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dbs = self.dbs.lock();
        let d = PyDict::new(py);
        for (k, v) in dbs.iter() {
            d.set_item(k, v.bind(py).as_any())?;
        }
        Ok(d.unbind())
    }

    #[getter]
    fn _collections(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        Ok(PyDict::new(py).unbind())
    }
}

fn session_key(_py: Python<'_>, lsid: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(d) = lsid.cast::<PyDict>() {
        let id_val = d.get_item("id")?;
        match id_val {
            Some(v) => Ok(v.str()?.to_string()),
            None => Ok(String::new()),
        }
    } else {
        Ok(lsid.str()?.to_string())
    }
}
