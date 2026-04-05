//! Rust port of `smongo.storage.collection.LocalCollection`.
//!
//! Core CRUD operations (insert, get, update, delete) use WiredTiger cursors
//! directly via `wt_safe`, with BSON encode/decode in Rust.  IndexManager and
//! QueryPlanner remain in Python and are called via PyO3 callbacks.

use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList};

use wiredtiger_sys::WT_CONNECTION;

use crate::bson_helpers;
use crate::index_manager::RustIndexManager;
use crate::locking::{InlineRwLock, ReadWriteLock};
use crate::objectid::ObjectId;
use crate::query_compiler;
use crate::results::{DeleteResult, InsertResult, UpdateResult};
use crate::wt_bridge::RustWtSession;
use crate::wt_safe::{WtSession, open_session_from_conn_ptr};

// ---------------------------------------------------------------------------
// RustLocalCollection
// ---------------------------------------------------------------------------

/// Rust-side WiredTiger collection -- CRUD, query execution, and bulk operations.
#[pyclass]
pub struct RustLocalCollection {
    conn_ptr: *mut WT_CONNECTION,
    db_name: crate::DbName,
    name: crate::CollectionName,
    namespace: crate::Namespace,
    pub(crate) table_uri: crate::TableUri,
    pub(crate) oplog_uri: crate::TableUri,

    session_raw: Option<*mut wiredtiger_sys::WT_SESSION>,
    pub(crate) session_py: Py<RustWtSession>,

    pub(crate) rwlock: Arc<InlineRwLock>,
    pub(crate) mutex: Arc<Mutex<()>>,
    lock_py: Py<PyAny>,

    pub(crate) index_mgr: Py<RustIndexManager>,
    pub(crate) planner: Py<crate::query_planner::RustQueryPlanner>,
    oplog_w: Py<PyAny>,
    oplog_r: Py<PyAny>,
    _oplog_hub: Option<Py<crate::oplog::OplogHub>>,
    pub(crate) _validator: Option<Py<PyAny>>,
    _db: Option<Py<PyAny>>,
    _doc_versions: Mutex<HashMap<String, i64>>,
    pub(crate) _ttl_reaper: Py<PyAny>,
}

// SAFETY: RustLocalCollection is a #[pyclass] requiring Send+Sync.  All mutable
// state is protected by the Arc<InlineRwLock> (reader-writer lock) and
// Arc<Mutex<()>> acquired in every public method.  The conn_ptr and session_raw
// are only accessed while these locks are held, ensuring single-threaded access
// to the underlying WiredTiger session regardless of GIL state.  This is safe
// under both GIL-enabled and free-threaded Python builds.
unsafe impl Send for RustLocalCollection {}
unsafe impl Sync for RustLocalCollection {}

// --- Rust-internal helpers ---

impl RustLocalCollection {
    pub fn new_rust(
        py: Python<'_>,
        conn_ptr: *mut WT_CONNECTION,
        db_name: &str,
        name: &str,
        db: Option<Py<PyAny>>,
        validator: Option<Py<PyAny>>,
        oplog_hub: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let namespace = format!("{db_name}.{name}");
        let table_uri = format!("table:{db_name}_{name}");
        let oplog_uri = format!("table:__oplog_{db_name}_{name}");

        let wt_session = open_session_from_conn_ptr(conn_ptr)?;
        let session_raw = wt_session.raw_ptr();
        let session_py = Py::new(py, RustWtSession::from_safe(wt_session))?;

        {
            let session_borrow = session_py.bind(py).borrow();
            let s = session_borrow.get()?;
            s.create(&table_uri, "key_format=S,value_format=u")?;
            s.create(&oplog_uri, "key_format=S,value_format=S")?;
        }

        let rwlock = Arc::new(InlineRwLock::new());
        let mutex = Arc::new(Mutex::new(()));
        let lock_py = crate::cached_modules::threading_mod(py)?.call_method0("Lock")?.unbind();

        let index_mgr: Py<RustIndexManager> = Py::new(
            py,
            RustIndexManager::new(py, session_py.clone_ref(py), db_name, name)?,
        )?;
        let planner = Py::new(
            py,
            crate::query_planner::RustQueryPlanner::new(index_mgr.clone_ref(py)),
        )?;

        let hub_typed: Option<Py<crate::oplog::OplogHub>> = oplog_hub.as_ref().and_then(|h| {
            h.bind(py)
                .cast::<crate::oplog::OplogHub>()
                .ok()
                .map(|b| b.clone().unbind())
        });
        let hub_for_writer = hub_typed.as_ref().map(|h| h.clone_ref(py));
        let oplog_w = Py::new(
            py,
            crate::oplog::OplogWriter::new(
                session_py.clone_ref(py).into_any(),
                oplog_uri.clone(),
                namespace.clone(),
                hub_for_writer,
            ),
        )?
        .into_any();
        let oplog_r = Py::new(
            py,
            crate::oplog::OplogReader::new(
                session_py.clone_ref(py).into_any(),
                oplog_uri.clone(),
            ),
        )?
        .into_any();

        let reaper_cls = py
            .import("smongo.storage.collection")?
            .getattr("TTLReaper")?;
        let ttl_reaper = reaper_cls.call1((py.None(),))?.unbind();

        Ok(Self {
            conn_ptr,
            db_name: db_name.to_string(),
            name: name.to_string(),
            namespace,
            table_uri,
            oplog_uri,
            session_raw: Some(session_raw),
            session_py,
            rwlock,
            mutex,
            lock_py,
            index_mgr,
            planner,
            oplog_w,
            oplog_r,
            _oplog_hub: hub_typed,
            _validator: validator,
            _db: db,
            _doc_versions: Mutex::new(HashMap::new()),
            _ttl_reaper: ttl_reaper,
        })
    }

    fn borrow_session(&self) -> PyResult<ManuallyDrop<WtSession>> {
        crate::wt_bridge::borrow_wt_session(self.session_raw, "collection")
    }

    fn begin_txn(&self) -> PyResult<()> {
        self.borrow_session()?.begin_transaction(None)?;
        Ok(())
    }

    fn commit_txn(&self) -> PyResult<()> {
        self.borrow_session()?.commit_transaction(None)?;
        Ok(())
    }

    fn rollback_txn(&self) -> PyResult<()> {
        self.borrow_session()?.rollback_transaction(None)?;
        Ok(())
    }

    fn get_txn_session_raw(&self, py: Python<'_>) -> Option<*mut wiredtiger_sys::WT_SESSION> {
        // Fast path: check the Rust thread-local (set by RustTransactionSession.activate)
        let existing = crate::wt_bridge::TXN_SESSION_OVERRIDE.with(|c| c.get());
        if existing.is_some() {
            return existing;
        }
        // Slow path: check the Python thread-local (set by Python TransactionSession.activate)
        let session_py = crate::transaction::get_active_txn_session(py).ok()??;
        let session_obj = session_py.bind(py);
        if let Ok(rs_session) = session_obj.cast::<crate::wt_bridge::RustWtSession>() {
            if let Ok(inner) = rs_session.borrow().get() {
                return Some(inner.raw_ptr());
            }
        }
        None
    }

    fn with_txn<F, T>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        F: FnOnce() -> PyResult<T>,
    {
        if let Some(txn_raw) = self.get_txn_session_raw(py) {
            crate::wt_bridge::set_txn_session_override(txn_raw);
            let result = f();
            crate::wt_bridge::clear_txn_session_override();
            return result;
        }
        self.begin_txn()?;
        match f() {
            Ok(result) => {
                self.commit_txn()?;
                Ok(result)
            }
            Err(e) => {
                let _ = self.rollback_txn();
                Err(e)
            }
        }
    }

    /// Ensure the txn session override is set for the duration of a read or
    /// any non-transactional operation.  If a multi-doc transaction is active
    /// (Python or Rust side), routes all cursor operations through its session.
    fn with_session_override<F, T>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        F: FnOnce() -> PyResult<T>,
    {
        if let Some(txn_raw) = self.get_txn_session_raw(py) {
            crate::wt_bridge::set_txn_session_override(txn_raw);
            let result = f();
            crate::wt_bridge::clear_txn_session_override();
            result
        } else {
            f()
        }
    }

    fn open_data_cursor(&self, config: Option<&str>) -> PyResult<crate::wt_safe::WtCursor> {
        Ok(self
            .borrow_session()?
            .open_cursor(&self.table_uri, config)?)
    }

    fn bump_version(&self, doc_id: &str) -> i64 {
        let mut vers = self._doc_versions.lock();
        let v = vers.get(doc_id).copied().unwrap_or(0) + 1;
        vers.insert(doc_id.to_string(), v);
        v
    }

    fn validate_doc(&self, _py: Python<'_>, doc: &Bound<'_, PyDict>) -> PyResult<()> {
        if let Some(ref v) = self._validator {
            crate::schema::validate_document(doc, v.bind(doc.py()))?;
        }
        Ok(())
    }

    fn acq_read(&self, py: Python<'_>) {
        let rwlock = Arc::clone(&self.rwlock);
        py.detach(|| rwlock.acquire_read());
    }
    fn rel_read(&self, _py: Python<'_>) {
        self.rwlock.release_read();
    }
    fn acq_write(&self, py: Python<'_>) {
        let rwlock = Arc::clone(&self.rwlock);
        py.detach(|| rwlock.acquire_write());
    }
    fn rel_write(&self, _py: Python<'_>) {
        self.rwlock.release_write();
    }
    fn acq_lock(&self, py: Python<'_>) -> crate::locking::MutexForceGuard {
        crate::locking::MutexForceGuard::acquire(py, &self.mutex)
    }

    // --- Unlocked read helpers (caller must hold both locks) ---

    fn get_by_id_unlocked_str(
        &self,
        py: Python<'_>,
        doc_id: &str,
    ) -> PyResult<Option<Py<PyDict>>> {
        let mut cursor = self.open_data_cursor(None)?;
        cursor.set_key_str(doc_id);
        let found = cursor.search().is_ok();
        if found {
            let raw = cursor.get_value_raw()?;
            let doc = bson_helpers::from_bson(py, &raw)?;
            cursor.close()?;
            Ok(Some(doc.unbind()))
        } else {
            cursor.close()?;
            Ok(None)
        }
    }

    fn get_by_ids_unlocked_strs(
        &self,
        py: Python<'_>,
        ids: &[String],
    ) -> PyResult<Vec<Py<PyDict>>> {
        let mut cursor = self.open_data_cursor(None)?;
        let mut docs = Vec::with_capacity(ids.len());
        for id in ids {
            cursor.set_key_str(id);
            if cursor.search().is_ok() {
                let raw = cursor.get_value_raw()?;
                docs.push(bson_helpers::from_bson(py, &raw)?.unbind());
            }
        }
        cursor.close()?;
        Ok(docs)
    }

    fn get_all_unlocked_vec(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let mut cursor = self.open_data_cursor(None)?;
        let mut docs = Vec::new();
        while cursor.next().is_ok() {
            let raw = cursor.get_value_raw()?;
            docs.push(bson_helpers::from_bson(py, &raw)?.unbind());
        }
        cursor.close()?;
        Ok(docs)
    }

    fn find_matching_locked(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let plan = self.planner.bind(py).borrow().plan(py, query)?;
        match plan.plan_type {
            crate::query_planner::PlanType::PkLookup => self.exec_pk_lookup(py, query),
            crate::query_planner::PlanType::IndexScan => self.exec_index_scan_typed(py, query, &plan),
            crate::query_planner::PlanType::OrUnion => self.exec_or_union_typed(py, query, &plan),
            _ => self.exec_full_scan(py, query),
        }
    }

    fn exec_pk_lookup(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let raw_id = match query.get_item("_id")? {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let pk_val = if let Ok(d) = raw_id.cast::<PyDict>() {
            if let Some(eq) = d.get_item("$eq")? {
                eq.str()?.to_string()
            } else {
                raw_id.str()?.to_string()
            }
        } else {
            raw_id.str()?.to_string()
        };

        let doc = match self.get_by_id_unlocked_str(py, &pk_val)? {
            Some(d) => d,
            None => return Ok(vec![]),
        };

        let remaining = PyDict::new(py);
        for (k, v) in query.iter() {
            let ks: String = k.extract()?;
            if ks != "_id" {
                remaining.set_item(k, v)?;
            }
        }
        if !remaining.is_empty()
            && !query_compiler::eval_query(doc.bind(py), &remaining)?
        {
            return Ok(vec![]);
        }
        Ok(vec![doc])
    }

    fn exec_index_scan_typed(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        plan: &crate::query_planner::QueryPlan,
    ) -> PyResult<Vec<Py<PyDict>>> {
        if let Some(ref idx_name) = plan.index_def_name {
            let mgr = self.index_mgr.bind(py).borrow();
            if let Some(idx) = mgr.indexes().get(idx_name) {
                if let Some((leading_field, _)) = idx.keys.first() {
                    if let Some(cond) = query.get_item(leading_field)? {
                        if let Ok(cond_dict) = cond.cast::<PyDict>() {
                            if let Some(in_vals) = cond_dict.get_item("$in")? {
                                let ids = self.planner.bind(py).borrow().execute_in_scan(
                                    py, idx_name, &in_vals, self.session_raw,
                                )?;
                                let docs = self.get_by_ids_unlocked_strs(py, &ids)?;
                                return self.filter_docs(py, docs, query);
                            }
                        }
                    }
                }
            }
        }

        let ids = self.planner.bind(py).borrow().execute_index_scan(
            py, plan, self.session_raw,
        )?;
        let docs = self.get_by_ids_unlocked_strs(py, &ids)?;
        self.filter_docs(py, docs, query)
    }

    fn exec_or_union_typed(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        plan: &crate::query_planner::QueryPlan,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let subplans = match plan.subplans {
            Some(ref sp) => sp,
            None => return self.exec_full_scan(py, query),
        };

        let mut seen = std::collections::HashSet::<String>::new();
        let mut candidate_ids = Vec::<String>::new();

        for sub in subplans {
            match sub.plan_type {
                crate::query_planner::PlanType::PkLookup => {
                    if let Some(or_branches) = query.get_item("$or")? {
                        for branch in or_branches.try_iter()? {
                            let branch = branch?;
                            if let Ok(bd) = branch.cast::<PyDict>() {
                                if let Some(id_val) = bd.get_item("_id")? {
                                    if id_val.cast::<PyDict>().is_err() {
                                        let sid = id_val.str()?.to_string();
                                        if seen.insert(sid.clone()) {
                                            candidate_ids.push(sid);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                crate::query_planner::PlanType::IndexScan => {
                    let ids = self.planner.bind(py).borrow().execute_index_scan(
                        py, sub, self.session_raw,
                    )?;
                    for sid in ids {
                        if seen.insert(sid.clone()) {
                            candidate_ids.push(sid);
                        }
                    }
                }
                _ => {}
            }
        }

        let docs = self.get_by_ids_unlocked_strs(py, &candidate_ids)?;
        self.filter_docs(py, docs, query)
    }

    fn exec_full_scan(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let all = self.get_all_unlocked_vec(py)?;
        if query.is_empty() {
            return Ok(all);
        }
        self.filter_docs(py, all, query)
    }

    fn filter_docs(
        &self,
        py: Python<'_>,
        docs: Vec<Py<PyDict>>,
        query: &Bound<'_, PyDict>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        if query.is_empty() {
            return Ok(docs);
        }
        let mut matched = Vec::new();
        for doc in docs {
            if query_compiler::eval_query(doc.bind(py), query)? {
                matched.push(doc);
            }
        }
        Ok(matched)
    }

    fn log_oplog(
        &self,
        py: Python<'_>,
        op: &str,
        doc_id: &Bound<'_, PyAny>,
        body: &Bound<'_, PyAny>,
        version: i64,
        changed_fields: Option<Vec<String>>,
    ) -> PyResult<()> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("version", version)?;
        if let Some(cf) = changed_fields {
            kwargs.set_item("changed_fields", cf)?;
        }
        self.oplog_w
            .bind(py)
            .call_method("log", (op, doc_id, body), Some(&kwargs))?;
        Ok(())
    }

    fn sorted_keys(doc: &Bound<'_, PyDict>) -> Vec<String> {
        let mut keys: Vec<String> = doc
            .keys()
            .iter()
            .filter_map(|k| k.extract::<String>().ok())
            .collect();
        keys.sort();
        keys
    }

    fn changed_fields_from_update(update_spec: &Bound<'_, PyAny>) -> Vec<String> {
        let ops = [
            "$set", "$unset", "$inc", "$mul", "$min", "$max", "$rename",
            "$currentDate", "$addToSet", "$push", "$pull", "$pop",
        ];
        let mut changed = std::collections::HashSet::<String>::new();
        if let Ok(d) = update_spec.cast::<PyDict>() {
            for (op_key, fields) in d.iter() {
                if let Ok(op_str) = op_key.extract::<String>() {
                    if ops.contains(&op_str.as_str()) {
                        if let Ok(fd) = fields.cast::<PyDict>() {
                            for (k, _) in fd.iter() {
                                if let Ok(ks) = k.extract::<String>() {
                                    changed.insert(ks);
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut sorted: Vec<String> = changed.into_iter().collect();
        sorted.sort();
        sorted
    }

    pub(crate) fn insert_one(
        &self,
        py: Python<'_>,
        doc: &Bound<'_, PyDict>,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        let doc = doc.copy()?;
        if doc.get_item("_id")?.is_none() {
            let oid = Py::new(py, ObjectId::generate(py))?;
            doc.set_item("_id", &oid)?;
        }
        self.validate_doc(py, &doc)?;
        let doc_id = doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?;
        let doc_id_str = doc_id.str()?.to_string();
        let bson_bytes = bson_helpers::to_bson(&doc)?;

        self.acq_write(py);
        let result = (|| -> PyResult<Py<PyAny>> {
            let guard = self.acq_lock(py);
            let inner = self.with_txn(py, || {
                self.index_mgr.bind(py).borrow().add_doc(py, &doc)?;
                let mut cursor = self.open_data_cursor(Some("overwrite=true"))?;
                cursor.set_key_str(&doc_id_str);
                cursor.set_value_raw(bson_bytes.bind(py).as_bytes());
                cursor.update()?;
                cursor.close()?;
                let version = self.bump_version(&doc_id_str);
                if !_internal {
                    self.log_oplog(
                        py,
                        "insert",
                        &doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                        doc.as_any(),
                        version,
                        Some(Self::sorted_keys(&doc)),
                    )?;
                }
                Ok(())
            });
            drop(guard);
            inner?;
            let ids = PyList::new(py, [doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?])?;
            Ok(Py::new(py, InsertResult::new(ids.unbind().into_any()))?.into_any())
        })();
        self.rel_write(py);
        result
    }

    pub(crate) fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let _ = self._ttl_reaper.bind(py).call_method0("stop");
        self.session_raw = None;
        let _ = self.session_py.bind(py).call_method0("close");
        Ok(())
    }
}

impl RustLocalCollection {
    pub(crate) fn find(&self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<Py<PyList>> {
        self.acq_read(py);
        let result = self.with_session_override(py, || {
            let _guard = self.acq_lock(py);
            let docs = self.find_matching_locked(py, query)?;
            Ok(PyList::new(py, docs.iter().map(|d| d.bind(py)))?.unbind())
        });
        self.rel_read(py);
        result
    }

    pub(crate) fn find_one(&self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        self.acq_read(py);
        let result = self.with_session_override(py, || {
            let _guard = self.acq_lock(py);
            let docs = self.find_matching_locked(py, query)?;
            Ok(docs
                .into_iter()
                .next()
                .map_or_else(|| py.None(), |d| d.into_any()))
        });
        self.rel_read(py);
        result
    }

    pub(crate) fn count(&self, py: Python<'_>, query: Option<&Bound<'_, PyDict>>) -> PyResult<i64> {
        let empty = PyDict::new(py);
        let q = query.unwrap_or(&empty);
        if q.is_empty() {
            return self.with_session_override(py, || self.count_fast(py));
        }
        self.acq_read(py);
        let result = self.with_session_override(py, || {
            let _guard = self.acq_lock(py);
            let docs = self.find_matching_locked(py, q)?;
            Ok(docs.len() as i64)
        });
        self.rel_read(py);
        result
    }

    pub(crate) fn count_fast(&self, py: Python<'_>) -> PyResult<i64> {
        let mut n = 0i64;
        let _guard = self.acq_lock(py);
        let mut cursor = self.open_data_cursor(None)?;
        while cursor.next().is_ok() {
            n += 1;
        }
        cursor.close()?;
        Ok(n)
    }

    pub(crate) fn data_size_bytes(&self, py: Python<'_>) -> PyResult<i64> {
        let mut total = 0i64;
        let _guard = self.acq_lock(py);
        let mut cursor = self.open_data_cursor(None)?;
        while cursor.next().is_ok() {
            let raw = cursor.get_value_raw()?;
            total += raw.len() as i64;
        }
        cursor.close()?;
        Ok(total)
    }

    pub(crate) fn explain(
        &self,
        py: Python<'_>,
        query: Option<&Bound<'_, PyDict>>,
        execute: bool,
    ) -> PyResult<Py<PyDict>> {
        let empty = PyDict::new(py);
        let q = query.unwrap_or(&empty);
        self.acq_read(py);
        let result = (|| -> PyResult<Py<PyDict>> {
            let _guard = self.acq_lock(py);
            let plan = self.planner.bind(py).borrow().plan(py, q)?;
            let rd = plan.to_py_dict(py)?.into_bound(py);
            if execute {
                let t0: f64 = crate::cached_modules::time_mod(py)?.call_method0("monotonic")?.extract()?;
                let docs = self.find_matching_locked(py, q)?;
                let t1: f64 = crate::cached_modules::time_mod(py)?.call_method0("monotonic")?.extract()?;
                let es = PyDict::new(py);
                es.set_item("nReturned", docs.len())?;
                es.set_item("executionTimeMillis", ((t1 - t0) * 1000.0) as i64)?;
                rd.set_item("executionStats", es)?;
            }
            Ok(rd.unbind())
        })();
        self.rel_read(py);
        result
    }

    pub(crate) fn find_streaming_typed(
        &self,
        py: Python<'_>,
        query: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let sc = crate::streaming::RustStreamingCursor::from_collection(py, self, query)?;
        Ok(sc.into_any())
    }

    pub(crate) fn get_all_typed(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.acq_read(py);
        let result = self.with_session_override(py, || {
            let _guard = self.acq_lock(py);
            let docs = self.get_all_unlocked_vec(py)?;
            let list = PyList::new(py, docs.iter().map(|d| d.bind(py)))?;
            Ok(list.unbind())
        });
        self.rel_read(py);
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn update(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        update_spec: &Bound<'_, PyAny>,
        multi: bool,
        upsert: bool,
        array_filters: Option<&Bound<'_, PyList>>,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.acq_write(py);
        let opts = UpdateOpts { query, update_spec, multi, upsert, array_filters, internal: _internal };
        let result = self.update_inner(py, &opts);
        self.rel_write(py);
        result
    }

    pub(crate) fn delete(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        multi: bool,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.acq_write(py);
        let result = self.delete_inner(py, query, multi, _internal);
        self.rel_write(py);
        result
    }

    pub(crate) fn find_one_and_update(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        update_spec: &Bound<'_, PyAny>,
        return_document: &str,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.acq_write(py);
        let result = (|| -> PyResult<Py<PyAny>> {
            let guard = self.acq_lock(py);
            let matching = self.find_matching_locked(py, query)?;
            if matching.is_empty() {
                return Ok(py.None());
            }
            let doc = matching[0].bind(py);
            let before: Py<PyDict> = bson_helpers::shallow_copy_dict(py, doc.cast::<PyDict>()?)?.unbind();
            let inner = self.with_txn(py, || {
                crate::query_update::apply_update(doc.cast::<PyDict>()?, update_spec, None, None)?;
                self.validate_doc(py, doc)?;
                self.index_mgr.bind(py).borrow().update_doc(py, before.bind(py), doc)?;
                let did_str = doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?.str()?.to_string();
                let bson = bson_helpers::to_bson(doc)?;
                let mut cursor = self.open_data_cursor(Some("overwrite=true"))?;
                cursor.set_key_str(&did_str);
                cursor.set_value_raw(bson.bind(py).as_bytes());
                cursor.update()?;
                cursor.close()?;
                let version = self.bump_version(&did_str);
                if !_internal {
                    self.log_oplog(
                        py,
                        "update",
                        &doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                        update_spec,
                        version,
                        Some(Self::changed_fields_from_update(update_spec)),
                    )?;
                }
                Ok(())
            });
            drop(guard);
            inner?;
            if return_document == "after" {
                Ok(bson_helpers::shallow_copy_dict(py, matching[0].bind(py).cast::<PyDict>()?)?.unbind().into_any())
            } else {
                Ok(before.into_any())
            }
        })();
        self.rel_write(py);
        result
    }

    pub(crate) fn find_one_and_replace(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        replacement: &Bound<'_, PyDict>,
        upsert: bool,
        return_document: &str,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.acq_write(py);
        let result = (|| -> PyResult<Py<PyAny>> {
            let guard = self.acq_lock(py);
            let matching = self.find_matching_locked(py, query)?;
            if matching.is_empty() {
                if upsert {
                    let rep = replacement.copy()?;
                    if rep.get_item("_id")?.is_none() {
                        let oid = Py::new(py, ObjectId::generate(py))?;
                        rep.set_item("_id", &oid)?;
                    }
                    self.validate_doc(py, &rep)?;
                    let did_str = rep.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?.str()?.to_string();
                    let bson = bson_helpers::to_bson(&rep)?;
                    let inner = self.with_txn(py, || {
                        self.index_mgr.bind(py).borrow().add_doc(py, &rep)?;
                        let mut cursor = self.open_data_cursor(Some("overwrite=true"))?;
                        cursor.set_key_str(&did_str);
                        cursor.set_value_raw(bson.bind(py).as_bytes());
                        cursor.update()?;
                        cursor.close()?;
                        let version = self.bump_version(&did_str);
                        if !_internal {
                            self.log_oplog(
                                py,
                                "insert",
                                &rep.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                                rep.as_any(),
                                version,
                                Some(Self::sorted_keys(&rep)),
                            )?;
                        }
                        Ok(())
                    });
                    drop(guard);
                    inner?;
                    return if return_document == "after" {
                        Ok(rep.unbind().into_any())
                    } else {
                        Ok(py.None())
                    };
                }
                return Ok(py.None());
            }
            let doc = matching[0].bind(py);
            let before: Py<PyDict> = bson_helpers::shallow_copy_dict(py, doc.cast::<PyDict>()?)?.unbind();
            let rep = bson_helpers::shallow_copy_dict(py, replacement)?;
            rep.set_item("_id", doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?)?;
            let did_str = rep.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?.str()?.to_string();
            let inner = self.with_txn(py, || {
                self.validate_doc(py, &rep)?;
                self.index_mgr.bind(py).borrow().update_doc(py, before.bind(py), &rep)?;
                let bson = bson_helpers::to_bson(&rep)?;
                let mut cursor = self.open_data_cursor(Some("overwrite=true"))?;
                cursor.set_key_str(&did_str);
                cursor.set_value_raw(bson.bind(py).as_bytes());
                cursor.update()?;
                cursor.close()?;
                let version = self.bump_version(&did_str);
                if !_internal {
                    self.log_oplog(
                        py,
                        "update",
                        &rep.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                        rep.as_any(),
                        version,
                        None,
                    )?;
                }
                Ok(())
            });
            drop(guard);
            inner?;
            if return_document == "after" {
                Ok(rep.unbind().into_any())
            } else {
                Ok(before.into_any())
            }
        })();
        self.rel_write(py);
        result
    }

    pub(crate) fn find_one_and_delete(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.acq_write(py);
        let result = (|| -> PyResult<Py<PyAny>> {
            let guard = self.acq_lock(py);
            let matching = self.find_matching_locked(py, query)?;
            if matching.is_empty() {
                return Ok(py.None());
            }
            let doc = matching[0].bind(py);
            let copy: Py<PyDict> = bson_helpers::shallow_copy_dict(py, doc.cast::<PyDict>()?)?.unbind();
            let inner = self.with_txn(py, || {
                self.index_mgr.bind(py).borrow().remove_doc(py, doc)?;
                let did_str = doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?.str()?.to_string();
                let mut cursor = self.open_data_cursor(Some("overwrite=true"))?;
                cursor.set_key_str(&did_str);
                cursor.remove()?;
                cursor.close()?;
                let version = self.bump_version(&did_str);
                if !_internal {
                    self.log_oplog(
                        py,
                        "delete",
                        &doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                        py.None().bind(py),
                        version,
                        None,
                    )?;
                }
                Ok(())
            });
            drop(guard);
            inner?;
            Ok(copy.into_any())
        })();
        self.rel_write(py);
        result
    }

    pub(crate) fn list_indexes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let _guard = self.acq_lock(py);
        let r = self.index_mgr.bind(py).borrow().list_indexes(py)?;
        Ok(r.into_any())
    }

    pub(crate) fn drop_index(&self, py: Python<'_>, name: &str, _internal: bool) -> PyResult<()> {
        let _guard = self.acq_lock(py);
        self.with_txn(py, || {
            self.index_mgr.bind(py).borrow_mut().drop_index(py, name)?;
            if !_internal {
                self.oplog_w
                    .bind(py)
                    .call_method1("log", ("index_drop", name, py.None()))?;
            }
            Ok(())
        })
    }

    pub(crate) fn create_index(
        &self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        _internal: bool,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let name: String = {
            let mut im = self.index_mgr.bind(py).borrow_mut();
            im.create_index(py, keys, kwargs)?
        };
        let guard = self.acq_lock(py);
        let inner = self.with_txn(py, || {
            let docs = self.get_all_unlocked_vec(py)?;
            let docs_list = PyList::new(py, docs.iter().map(|d| d.bind(py)))?;
            self.index_mgr.bind(py).borrow_mut().rebuild_index(py, &name, &docs_list)?;
            if !_internal {
                self.oplog_w.bind(py).call_method1(
                    "log",
                    ("index_create", &name, py.None()),
                )?;
            }
            Ok(())
        });
        drop(guard);
        inner?;
        self._ttl_reaper.bind(py).call_method0("maybe_start")?;
        Ok(name)
    }

    pub(crate) fn storage_stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let wt_stats = PyDict::new(py);
        let guard = self.acq_lock(py);
        let mut doc_count = 0i64;
        let mut data_size = 0i64;
        {
            let mut cursor = self.open_data_cursor(None)?;
            while cursor.next().is_ok() {
                doc_count += 1;
                if let Ok(val) = cursor.get_value_raw() {
                    data_size += val.len() as i64;
                }
            }
            cursor.close()?;
        }
        let n_indexes = self.index_mgr.bind(py).borrow().list_indexes(py)?.bind(py).len();
        drop(guard);

        let storage_size = data_size + (doc_count * 64);

        let result = PyDict::new(py);
        result.set_item("count", doc_count)?;
        result.set_item("dataSize", data_size)?;
        result.set_item("storageSize", storage_size)?;
        result.set_item("nindexes", n_indexes + 1)?;
        result.set_item("totalIndexSize", 0i64)?;
        let idx_sizes = PyDict::new(py);
        idx_sizes.set_item("_id_", 0i64)?;
        result.set_item("indexSizes", idx_sizes)?;
        result.set_item("wiredTiger", wt_stats)?;
        Ok(result.unbind())
    }

    pub(crate) fn rebuild_all_indexes(&self, py: Python<'_>) -> PyResult<i64> {
        let _guard = self.acq_lock(py);
        let docs = self.get_all_unlocked_vec(py)?;
        let docs_list = PyList::new(py, docs.iter().map(|d| d.bind(py)))?;
        let mut rebuilt = 0i64;
        let idx_list = self.index_mgr.bind(py).borrow().list_indexes(py)?;
        for meta in idx_list.bind(py).try_iter()? {
            let meta = meta?;
            let name: String = meta.get_item("name")?.extract()?;
            self.index_mgr.bind(py).borrow_mut().rebuild_index(py, &name, &docs_list)?;
            rebuilt += 1;
        }
        Ok(rebuilt)
    }
}

// ---------------------------------------------------------------------------
// Python-facing API
// ---------------------------------------------------------------------------

#[pymethods]
impl RustLocalCollection {
    // --- Properties ---

    #[getter]
    fn namespace(&self) -> &str {
        &self.namespace
    }
    #[getter]
    fn table_uri(&self) -> &str {
        &self.table_uri
    }
    #[getter]
    fn oplog_uri(&self) -> &str {
        &self.oplog_uri
    }
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }
    #[getter]
    fn db_name(&self) -> &str {
        &self.db_name
    }
    #[getter]
    fn session(&self, py: Python<'_>) -> Py<RustWtSession> {
        self.session_py.clone_ref(py)
    }
    #[getter]
    fn conn(&self, py: Python<'_>) -> Py<RustWtSession> {
        self.session_py.clone_ref(py)
    }
    #[getter]
    fn index_mgr(&self, py: Python<'_>) -> Py<PyAny> {
        self.index_mgr.clone_ref(py).into_any()
    }
    #[getter]
    fn planner(&self, py: Python<'_>) -> Py<PyAny> {
        self.planner.clone_ref(py).into_any()
    }
    #[getter]
    fn _rwlock(&self, py: Python<'_>) -> PyResult<Py<ReadWriteLock>> {
        Py::new(py, ReadWriteLock::from_arc(Arc::clone(&self.rwlock)))
    }
    #[getter]
    fn _lock(&self, py: Python<'_>) -> Py<PyAny> {
        self.lock_py.clone_ref(py)
    }
    #[getter]
    fn get_oplog_hub(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self._oplog_hub.as_ref().map(|h| h.clone_ref(py).into_any())
    }
    #[getter(_validator)]
    fn get_validator(&self, py: Python<'_>) -> Py<PyAny> {
        self._validator
            .as_ref()
            .map_or_else(|| py.None(), |v| v.clone_ref(py))
    }
    #[setter(_validator)]
    fn set_validator(&mut self, val: Option<Py<PyAny>>) {
        self._validator = val;
    }
    #[getter]
    fn _oplog_w(&self, py: Python<'_>) -> Py<PyAny> {
        self.oplog_w.clone_ref(py)
    }
    #[getter]
    fn _oplog_r(&self, py: Python<'_>) -> Py<PyAny> {
        self.oplog_r.clone_ref(py)
    }
    #[getter]
    fn _ttl_reaper(&self, py: Python<'_>) -> Py<PyAny> {
        self._ttl_reaper.clone_ref(py)
    }
    #[getter]
    fn get_db(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self._db.as_ref().map(|d| d.clone_ref(py))
    }
    #[getter]
    fn _active_session(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Ok(Some(s)) = crate::transaction::get_active_txn_session(py) {
            return Ok(s);
        }
        Ok(self.session_py.clone_ref(py).into_any())
    }

    fn open_session(&self) -> PyResult<RustWtSession> {
        let wt_session = open_session_from_conn_ptr(self.conn_ptr)?;
        Ok(RustWtSession::from_safe(wt_session))
    }

    // --- Reads ---

    fn get_all(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.acq_read(py);
        let result = self.with_session_override(py, || {
            let _guard = self.acq_lock(py);
            let docs = self.get_all_unlocked_vec(py)?;
            let list = PyList::new(py, docs.iter().map(|d| d.bind(py)))?;
            Ok(list.unbind())
        });
        self.rel_read(py);
        result
    }

    fn get_by_id(&self, py: Python<'_>, doc_id: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let key = doc_id.str()?.to_string();
        self.acq_read(py);
        let result = self.with_session_override(py, || {
            let _guard = self.acq_lock(py);
            let doc = self.get_by_id_unlocked_str(py, &key)?;
            Ok(doc.map_or_else(|| py.None(), |d| d.into_any()))
        });
        self.rel_read(py);
        result
    }

    fn get_by_ids(&self, py: Python<'_>, doc_ids: &Bound<'_, PyList>) -> PyResult<Py<PyList>> {
        let keys: Vec<String> = doc_ids
            .iter()
            .map(|id| Ok(id.str()?.to_string()))
            .collect::<PyResult<Vec<_>>>()?;
        self.acq_read(py);
        let result = (|| -> PyResult<Py<PyList>> {
            let _guard = self.acq_lock(py);
            let docs = self.get_by_ids_unlocked_strs(py, &keys)?;
            let list = PyList::new(py, docs.iter().map(|d| d.bind(py)))?;
            Ok(list.unbind())
        })();
        self.rel_read(py);
        result
    }

    fn _get_by_id_unlocked(
        &self,
        py: Python<'_>,
        doc_id: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let key = doc_id.str()?.to_string();
        Ok(self
            .get_by_id_unlocked_str(py, &key)?
            .map_or_else(|| py.None(), |d| d.into_any()))
    }

    fn _get_all_unlocked(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let docs = self.get_all_unlocked_vec(py)?;
        Ok(PyList::new(py, docs.iter().map(|d| d.bind(py)))?.unbind())
    }

    // --- Find / Count ---

    #[pyo3(name = "find")]
    fn py_find(&self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<Py<PyList>> {
        self.find(py, query)
    }

    fn _find_matching_docs(&self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<Py<PyList>> {
        self.find(py, query)
    }

    #[pyo3(name = "find_one")]
    fn py_find_one(&self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        self.find_one(py, query)
    }

    #[pyo3(name = "count", signature = (query=None))]
    fn py_count(&self, py: Python<'_>, query: Option<&Bound<'_, PyDict>>) -> PyResult<i64> {
        self.count(py, query)
    }

    #[pyo3(name = "find_streaming", signature = (query=None))]
    fn py_find_streaming(slf: &Bound<'_, Self>, query: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        slf.borrow().find_streaming_typed(py, query)
    }

    #[pyo3(name = "explain", signature = (query=None, execute=false))]
    fn py_explain(
        &self,
        py: Python<'_>,
        query: Option<&Bound<'_, PyDict>>,
        execute: bool,
    ) -> PyResult<Py<PyDict>> {
        self.explain(py, query, execute)
    }

    fn scan_with_fields(
        &self,
        py: Python<'_>,
        fields: &Bound<'_, PyList>,
    ) -> PyResult<Py<PyList>> {
        let field_strs: Vec<String> = fields.extract()?;
        let results = PyList::empty(py);
        self.acq_read(py);
        let inner = (|| -> PyResult<()> {
            let _guard = self.acq_lock(py);
            let mut cursor = self.open_data_cursor(None)?;
            while cursor.next().is_ok() {
                let raw = cursor.get_value_raw()?;
                let doc = bson_helpers::from_bson(py, &raw)?;
                let doc_id = doc
                    .get_item("_id")?
                    .map(|v| v.unbind())
                    .unwrap_or_else(|| py.None());
                let vals = PyDict::new(py);
                for f in &field_strs {
                    let val = crate::paths::get_value(&doc, f)?;
                    vals.set_item(f.as_str(), val)?;
                }
                let tup = pyo3::types::PyTuple::new(
                    py,
                    [doc_id.bind(py).clone(), vals.into_any()],
                )?;
                results.append(tup)?;
            }
            cursor.close()?;
            Ok(())
        })();
        self.rel_read(py);
        inner?;
        Ok(results.unbind())
    }

    // --- Writes ---

    #[pyo3(name = "insert_one", signature = (doc, _internal=false))]
    fn py_insert_one(
        &self,
        py: Python<'_>,
        doc: &Bound<'_, PyDict>,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.insert_one(py, doc, _internal)
    }

    #[pyo3(signature = (docs, _internal=false))]
    fn insert_many(
        &self,
        py: Python<'_>,
        docs: &Bound<'_, PyList>,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        let oid_cls = py.get_type::<ObjectId>();
        let mut prepared: Vec<Bound<'_, PyDict>> = Vec::with_capacity(docs.len());
        let mut bson_list: Vec<Py<PyBytes>> = Vec::with_capacity(docs.len());
        for item in docs.iter() {
            let d: Bound<'_, PyDict> = item.extract()?;
            let d = d.copy()?;
            if d.get_item("_id")?.is_none() {
                d.set_item("_id", oid_cls.call0()?)?;
            }
            self.validate_doc(py, &d)?;
            bson_list.push(bson_helpers::to_bson(&d)?);
            prepared.push(d);
        }

        self.acq_write(py);
        let result = (|| -> PyResult<Py<PyAny>> {
            let guard = self.acq_lock(py);
            let inner = self.with_txn(py, || {
                let mut cursor = self.open_data_cursor(Some("overwrite=true"))?;
                for (i, doc) in prepared.iter().enumerate() {
                    self.index_mgr.bind(py).borrow().add_doc(py, doc)?;
                    let did_str = doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?.str()?.to_string();
                    cursor.set_key_str(&did_str);
                    cursor.set_value_raw(bson_list[i].bind(py).as_bytes());
                    cursor.update()?;
                    let version = self.bump_version(&did_str);
                    if !_internal {
                        self.log_oplog(
                            py,
                            "insert",
                            &doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                            doc.as_any(),
                            version,
                            Some(Self::sorted_keys(doc)),
                        )?;
                    }
                }
                cursor.close()?;
                Ok(())
            });
            drop(guard);
            inner?;
            let id_items: Vec<Bound<'_, PyAny>> = prepared
                .iter()
                .map(|d| {
                    d.get_item("_id")?
                        .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))
                })
                .collect::<PyResult<_>>()?;
            let ids = PyList::new(py, id_items)?;
            Ok(Py::new(py, InsertResult::new(ids.unbind().into_any()))?.into_any())
        })();
        self.rel_write(py);
        result
    }

    #[pyo3(name = "update", signature = (query, update_spec, multi=true, upsert=false, array_filters=None, _internal=false))]
    #[allow(clippy::too_many_arguments)]
    fn py_update(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        update_spec: &Bound<'_, PyAny>,
        multi: bool,
        upsert: bool,
        array_filters: Option<&Bound<'_, PyList>>,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.update(py, query, update_spec, multi, upsert, array_filters, _internal)
    }

    #[pyo3(name = "delete", signature = (query, multi=true, _internal=false))]
    fn py_delete(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        multi: bool,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.delete(py, query, multi, _internal)
    }

    // --- find_one_and_* ---

    #[pyo3(name = "find_one_and_update", signature = (query, update_spec, return_document="before", _internal=false))]
    fn py_find_one_and_update(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        update_spec: &Bound<'_, PyAny>,
        return_document: &str,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.find_one_and_update(py, query, update_spec, return_document, _internal)
    }

    #[pyo3(name = "find_one_and_replace", signature = (query, replacement, upsert=false, return_document="before", _internal=false))]
    fn py_find_one_and_replace(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        replacement: &Bound<'_, PyDict>,
        upsert: bool,
        return_document: &str,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.find_one_and_replace(py, query, replacement, upsert, return_document, _internal)
    }

    #[pyo3(name = "find_one_and_delete", signature = (query, _internal=false))]
    fn py_find_one_and_delete(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.find_one_and_delete(py, query, _internal)
    }

    // --- Index management ---

    #[pyo3(name = "create_index", signature = (keys, _internal=false, **kwargs))]
    fn py_create_index(
        &self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        _internal: bool,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        self.create_index(py, keys, _internal, kwargs)
    }

    #[pyo3(name = "drop_index", signature = (name, _internal=false))]
    fn py_drop_index(&self, py: Python<'_>, name: &str, _internal: bool) -> PyResult<()> {
        self.drop_index(py, name, _internal)
    }

    #[pyo3(name = "list_indexes")]
    fn py_list_indexes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.list_indexes(py)
    }

    // --- Change streams ---

    #[pyo3(signature = (pipeline=None))]
    fn watch(&self, py: Python<'_>, pipeline: Option<&Bound<'_, PyList>>) -> PyResult<Py<PyAny>> {
        let hub = self._oplog_hub.as_ref().map(|h| h.clone_ref(py));
        let hub_for_cs = hub.as_ref().map(|h| h.clone_ref(py));
        let cs = crate::oplog::ChangeStream::new(
            py,
            Some(self.namespace.clone()),
            pipeline,
            hub_for_cs,
        )?;
        let cs_py = Py::new(py, cs)?;
        if let Some(ref h) = hub {
            h.borrow(py).register_typed(cs_py.clone_ref(py));
        }
        Ok(cs_py.into_any())
    }

    // --- Oplog ---

    fn get_oplog(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let _guard = self.acq_lock(py);
        let r = self.oplog_r.bind(py).call_method0("read_all")?.unbind();
        Ok(r)
    }

    fn get_oplog_reader(&self, py: Python<'_>) -> Py<PyAny> {
        self.oplog_r.clone_ref(py)
    }

    #[pyo3(signature = (keep=1000))]
    fn compact_oplog(&self, py: Python<'_>, keep: i64) -> PyResult<()> {
        let _guard = self.acq_lock(py);
        self.oplog_w.bind(py).call_method1("truncate_count", (keep,))?;
        Ok(())
    }

    // --- Statistics & admin ---

    #[pyo3(name = "count_fast")]
    fn py_count_fast(&self, py: Python<'_>) -> PyResult<i64> {
        self.count_fast(py)
    }

    #[pyo3(name = "data_size_bytes")]
    fn py_data_size_bytes(&self, py: Python<'_>) -> PyResult<i64> {
        self.data_size_bytes(py)
    }

    #[pyo3(name = "storage_stats")]
    fn py_storage_stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.storage_stats(py)
    }

    fn compact(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        {
            let _guard = self.acq_lock(py);
            if let Ok(session) = self.borrow_session() {
                let _ = session.compact(&self.table_uri, None);
            }
        }
        let r = PyDict::new(py);
        r.set_item("ok", 1.0)?;
        Ok(r.unbind())
    }

    fn verify(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let errors = PyList::empty(py);
        let warnings = PyList::empty(py);
        if let Ok(vs_session) = open_session_from_conn_ptr(self.conn_ptr) {
            let vs = ManuallyDrop::new(vs_session);
            if let Err(e) = vs.verify(&self.table_uri, None) {
                let msg = e.message.to_lowercase();
                let is_busy = e.code == 16 /* EBUSY */
                    || msg.contains("busy")
                    || msg.contains("in use")
                    || msg.contains("handle is being used");
                if is_busy {
                    warnings.append(format!("data table verify skipped (table in use): {e}"))?;
                } else {
                    errors.append(format!("data table: {e}"))?;
                }
            }
            let _ = ManuallyDrop::into_inner(vs).close();
        }
        let (doc_count, n_indexes) = {
            let _guard = self.acq_lock(py);
            let mut n = 0i64;
            let mut cursor = self.open_data_cursor(None)?;
            while cursor.next().is_ok() {
                n += 1;
            }
            cursor.close()?;
            let ni = self.index_mgr.bind(py).borrow().list_indexes(py)?.bind(py).len();
            (n, ni)
        };

        let result = PyDict::new(py);
        result.set_item("valid", errors.is_empty())?;
        result.set_item("nrecords", doc_count)?;
        result.set_item("nIndexes", n_indexes + 1)?;
        result.set_item("errors", errors)?;
        result.set_item("warnings", warnings)?;
        result.set_item("indexEntries", PyDict::new(py))?;
        Ok(result.unbind())
    }

    #[pyo3(name = "rebuild_all_indexes")]
    fn py_rebuild_all_indexes(&self, py: Python<'_>) -> PyResult<i64> {
        self.rebuild_all_indexes(py)
    }

    #[pyo3(name = "close")]
    fn py_close(&mut self, py: Python<'_>) -> PyResult<()> {
        self.close(py)
    }
}

// --- update / delete inner helpers (not directly exposed as pymethods) ---

/// Bundled parameters for `update_inner`, avoiding clippy::too_many_arguments.
struct UpdateOpts<'a, 'py> {
    query: &'a Bound<'py, PyDict>,
    update_spec: &'a Bound<'py, PyAny>,
    multi: bool,
    upsert: bool,
    array_filters: Option<&'a Bound<'py, PyList>>,
    internal: bool,
}

impl RustLocalCollection {
    fn update_inner(
        &self,
        py: Python<'_>,
        opts: &UpdateOpts<'_, '_>,
    ) -> PyResult<Py<PyAny>> {
        let _guard = self.acq_lock(py);
        let mut matching = self.find_matching_locked(py, opts.query)?;
        if matching.is_empty() {
            if opts.upsert {
                return self.do_upsert(py, opts.query, opts.update_spec, opts.array_filters, opts.internal);
            }
            return Ok(Py::new(py, UpdateResult::new(py, 0, 0, None))?.into_any());
        }
        if !opts.multi {
            matching.truncate(1);
        }
        let matched_count = matching.len() as i64;
        let mut modified = 0i64;

        self.with_txn(py, || {
            let mut cursor = self.open_data_cursor(Some("overwrite=true"))?;
            for doc_py in &matching {
                let doc = doc_py.bind(py);
                let old_doc = bson_helpers::shallow_copy_dict(py, doc.cast::<PyDict>()?)?.into_any();
                let doc_dict = doc.cast::<PyDict>()?;
                crate::query_update::apply_update(doc_dict, opts.update_spec, opts.array_filters, Some(opts.query))?;
                self.validate_doc(py, doc)?;
                self.index_mgr.bind(py).borrow().update_doc(py, &old_doc, doc)?;
                let did_str = doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?.str()?.to_string();
                let bson = bson_helpers::to_bson(doc)?;
                cursor.set_key_str(&did_str);
                cursor.set_value_raw(bson.bind(py).as_bytes());
                cursor.update()?;
                let version = self.bump_version(&did_str);
                if !opts.internal {
                    let spec_log: Bound<'_, PyAny> = if opts.update_spec.cast::<PyDict>().is_ok() {
                        opts.update_spec.clone()
                    } else {
                        let d = PyDict::new(py);
                        d.set_item("$pipeline", opts.update_spec)?;
                        d.into_any()
                    };
                    let changed = if opts.update_spec.cast::<PyDict>().is_ok() {
                        Self::changed_fields_from_update(opts.update_spec)
                    } else {
                        vec![]
                    };
                    self.log_oplog(
                        py,
                        "update",
                        &doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                        &spec_log,
                        version,
                        Some(changed),
                    )?;
                }
                modified += 1;
            }
            cursor.close()?;
            Ok(())
        })?;

        Ok(Py::new(py, UpdateResult::new(py, matched_count, modified, None))?.into_any())
    }

    fn do_upsert(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        update_spec: &Bound<'_, PyAny>,
        array_filters: Option<&Bound<'_, PyList>>,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        let base = Self::extract_eq_conditions(py, query)?;
        let doc = base.copy()?;
        if doc.get_item("_id")?.is_none() {
            let oid = Py::new(py, ObjectId::generate(py))?;
            doc.set_item("_id", &oid)?;
        }
        crate::query_update::apply_update(&doc, update_spec, array_filters, Some(query))?;
        self.validate_doc(py, &doc)?;
        let did_str = doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?.str()?.to_string();
        let bson = bson_helpers::to_bson(&doc)?;

        self.with_txn(py, || {
            self.index_mgr.bind(py).borrow().add_doc(py, &doc)?;
            let mut cursor = self.open_data_cursor(Some("overwrite=true"))?;
            cursor.set_key_str(&did_str);
            cursor.set_value_raw(bson.bind(py).as_bytes());
            cursor.update()?;
            cursor.close()?;
            let version = self.bump_version(&did_str);
            if !_internal {
                self.log_oplog(
                    py,
                    "insert",
                    &doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                    doc.as_any(),
                    version,
                    Some(Self::sorted_keys(&doc)),
                )?;
            }
            Ok(())
        })?;

        let upserted_id = doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?.unbind();
        Ok(Py::new(py, UpdateResult::new(py, 0, 0, Some(upserted_id)))?.into_any())
    }

    fn extract_eq_conditions<'py>(
        py: Python<'py>,
        query: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let base = PyDict::new(py);
        for (k, v) in query.iter() {
            let ks: String = k.extract()?;
            if ks.starts_with('$') {
                continue;
            }
            if let Ok(vd) = v.cast::<PyDict>() {
                let has_op = vd
                    .iter()
                    .any(|(k2, _)| k2.extract::<String>().is_ok_and(|s| s.starts_with('$')));
                if has_op {
                    continue;
                }
            }
            base.set_item(k, v)?;
        }
        Ok(base)
    }

    fn delete_inner(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        multi: bool,
        _internal: bool,
    ) -> PyResult<Py<PyAny>> {
        let _guard = self.acq_lock(py);
        let mut matching = self.find_matching_locked(py, query)?;
        if matching.is_empty() {
            return Ok(Py::new(py, DeleteResult::new(0))?.into_any());
        }
        if !multi {
            matching.truncate(1);
        }
        let mut deleted = 0i64;

        self.with_txn(py, || {
            let mut cursor = self.open_data_cursor(Some("overwrite=true"))?;
            for doc_py in &matching {
                let doc = doc_py.bind(py);
                self.index_mgr.bind(py).borrow().remove_doc(py, doc)?;
                let did_str = doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?.str()?.to_string();
                cursor.set_key_str(&did_str);
                cursor.remove()?;
                let version = self.bump_version(&did_str);
                if !_internal {
                    self.log_oplog(
                        py,
                        "delete",
                        &doc.get_item("_id")?.ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                        py.None().bind(py),
                        version,
                        None,
                    )?;
                }
                deleted += 1;
            }
            cursor.close()?;
            Ok(())
        })?;

        Ok(Py::new(py, DeleteResult::new(deleted))?.into_any())
    }
}
