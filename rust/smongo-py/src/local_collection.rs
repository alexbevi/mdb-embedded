//! Rust port of `smongo.storage.collection.LocalCollection`.
//!
//! Core CRUD operations delegate to `smongo_engine::collection::Collection`.
//! Locking, oplog, transaction overrides, and Python type conversion remain
//! in this shim layer.

use std::collections::{HashMap, HashSet};
use std::mem::ManuallyDrop;
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};

use wiredtiger_sys::WT_CONNECTION;

use crate::bson_helpers;
use crate::wt_bridge::WtResultExt;
use crate::index_manager::RustIndexManager;
use crate::locking::{InlineRwLock, ReadWriteLock};
use crate::objectid::ObjectId;
use crate::query_compiler;
use crate::results::{DeleteResult, InsertResult, UpdateResult};
use crate::wt_bridge::RustWtSession;
use smongo_engine::collection::Collection as EngineCollection;
use crate::wt_safe::WtSession as EngineWtSession;
use crate::wt_safe::{open_session_from_conn_ptr, WtSession};

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

    engine_col: EngineCollection<EngineWtSession>,
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

#[allow(dead_code)]
impl RustLocalCollection {
    /// Special index kinds maintained in [`RustIndexManager`] (not the engine btree layer).
    fn keys_use_rust_index_manager(keys: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<bool> {
        let keys_list: Vec<(String, Py<PyAny>)> = if let Ok(s) = keys.extract::<String>() {
            vec![(
                s,
                1i32.into_pyobject(py)?.unbind().into_any(),
            )]
        } else if let Ok(v) = keys.extract::<Vec<(String, Py<PyAny>)>>() {
            v
        } else {
            return Ok(false);
        };
        for (f, d) in &keys_list {
            if f == "$**" {
                return Ok(true);
            }
            if let Ok(s) = d.bind(py).extract::<String>() {
                if matches!(s.as_str(), "text" | "hashed" | "2d") {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn keys_to_bson_index_doc(keys: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<bson::Document> {
        if let Ok(d) = keys.cast::<PyDict>() {
            return bson_helpers::pydict_to_doc(&d);
        }
        if let Ok(pairs) = keys.extract::<Vec<(String, Py<PyAny>)>>() {
            let mut doc = bson::Document::new();
            for (k, v) in pairs {
                let el = bson_helpers::py_to_bson(&v.bind(py))?;
                doc.insert(k, el);
            }
            return Ok(doc);
        }
        Err(PyRuntimeError::new_err(
            "index keys must be a dict or list of (field, direction) tuples",
        ))
    }

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

        // SAFETY: conn_ptr is valid and obtained from RustLocalClient
        let wt_session = unsafe { open_session_from_conn_ptr(conn_ptr) }.py()?;
        let session_raw = wt_session.raw_ptr();
        let session_py = Py::new(py, RustWtSession::from_safe(wt_session))?;

        {
            let session_borrow = session_py.bind(py).borrow();
            let s = session_borrow.get().py()?;
            s.create(&table_uri, "key_format=S,value_format=u").py()?;
            s.create(&oplog_uri, "key_format=S,value_format=S").py()?;
        }

        let rwlock = Arc::new(InlineRwLock::new());
        let mutex = Arc::new(Mutex::new(()));
        let lock_py = crate::cached_modules::threading_mod(py)?
            .call_method0("Lock")?
            .unbind();

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
            crate::oplog::OplogReader::new(session_py.clone_ref(py).into_any(), oplog_uri.clone()),
        )?
        .into_any();

        let reaper_cls = py
            .import("smongo.storage.collection")?
            .getattr("TTLReaper")?;
        let ttl_reaper = reaper_cls.call1((py.None(),))?.unbind();

        // SAFETY: conn_ptr is valid — open a dedicated session for the engine Collection
        let engine_session = unsafe { open_session_from_conn_ptr(conn_ptr) }.py()?;
        let engine_col = EngineCollection::with_table_uri(engine_session, name, &table_uri)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

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
            engine_col,
        })
    }

    fn borrow_session(&self) -> PyResult<ManuallyDrop<WtSession>> {
        crate::wt_bridge::borrow_wt_session(self.session_raw, "collection")
    }

    fn begin_txn(&self) -> PyResult<()> {
        self.borrow_session()?.begin_transaction(None).py()?;
        Ok(())
    }

    fn commit_txn(&self) -> PyResult<()> {
        self.borrow_session()?.commit_transaction(None).py()?;
        Ok(())
    }

    fn rollback_txn(&self) -> PyResult<()> {
        self.borrow_session()?.rollback_transaction(None).py()?;
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
            .open_cursor(&self.table_uri, config).py()?)
    }

    fn bump_version(&self, doc_id: &str) -> i64 {
        let mut vers = self._doc_versions.lock();
        let v = vers.get(doc_id).copied().unwrap_or(0) + 1;
        vers.insert(doc_id.to_string(), v);
        v
    }

    fn validate_doc(&self, _py: Python<'_>, doc: &Bound<'_, PyDict>) -> PyResult<()> {
        if let Some(ref v) = self._validator {
            let schema_bound = v.bind(doc.py());
            if schema_bound.is_none() || !schema_bound.is_truthy()? {
                return Ok(());
            }
            let schema_dict = schema_bound.cast::<PyDict>().map_err(|_| {
                crate::schema::ValidationError::new_err("schema must be a dict")
            })?;
            if schema_dict.is_empty() {
                return Ok(());
            }
            let bson_doc = bson_helpers::pydict_to_doc(doc)?;
            let bson_schema = bson_helpers::pydict_to_doc(schema_dict)?;
            smongo_engine::schema::validate_document(&bson_doc, &bson_schema).map_err(|e| {
                crate::schema::ValidationError::new_err(e.to_string())
            })?;
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

    fn get_by_id_unlocked_str(&self, py: Python<'_>, doc_id: &str) -> PyResult<Option<Py<PyDict>>> {
        let mut cursor = self.open_data_cursor(None)?;
        cursor.set_key_str(doc_id);
        let found = cursor.search().is_ok();
        if found {
            let raw = cursor.get_value_raw().py()?;
            let doc = bson_helpers::from_bson(py, &raw)?;
            cursor.close().py()?;
            Ok(Some(doc.unbind()))
        } else {
            cursor.close().py()?;
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
                let raw = cursor.get_value_raw().py()?;
                docs.push(bson_helpers::from_bson(py, &raw)?.unbind());
            }
        }
        cursor.close().py()?;
        Ok(docs)
    }

    fn get_all_unlocked_vec(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let mut cursor = self.open_data_cursor(None)?;
        let mut docs = Vec::new();
        while cursor.next().is_ok() {
            let raw = cursor.get_value_raw().py()?;
            docs.push(bson_helpers::from_bson(py, &raw)?.unbind());
        }
        cursor.close().py()?;
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
            crate::query_planner::PlanType::IndexScan => {
                self.exec_index_scan_typed(py, query, &plan)
            }
            crate::query_planner::PlanType::GeoNear
            | crate::query_planner::PlanType::GeoWithin
            | crate::query_planner::PlanType::GeoIntersects
            | crate::query_planner::PlanType::OrUnion => self.find_via_engine(py, query),
            _ => self.exec_full_scan(py, query),
        }
    }

    /// Run `find` through `smongo-engine` (2dsphere, `$or` unions, same semantics as native redb).
    pub(crate) fn find_via_engine(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let filter = bson_helpers::pydict_to_doc(query)?;
        let docs = self
            .engine_col
            .find(filter)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        docs.into_iter()
            .map(|d| Ok(bson_helpers::doc_to_pydict(py, &d)?.unbind()))
            .collect()
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
        if !remaining.is_empty() && !query_compiler::eval_query(doc.bind(py), &remaining)? {
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
                                    py,
                                    idx_name,
                                    &in_vals,
                                    self.session_raw,
                                )?;
                                let docs = self.get_by_ids_unlocked_strs(py, &ids)?;
                                return self.filter_docs(py, docs, query);
                            }
                        }
                    }
                }
            }
        }

        let ids = self
            .planner
            .bind(py)
            .borrow()
            .execute_index_scan(py, plan, self.session_raw)?;
        let docs = self.get_by_ids_unlocked_strs(py, &ids)?;
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
            "$set",
            "$unset",
            "$inc",
            "$mul",
            "$min",
            "$max",
            "$rename",
            "$currentDate",
            "$addToSet",
            "$push",
            "$pull",
            "$pop",
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
        let bson_doc = bson_helpers::pydict_to_doc(&doc)?;

        self.acq_write(py);
        let result = (|| -> PyResult<Py<PyAny>> {
            let guard = self.acq_lock(py);
            let engine_result = self.engine_col.insert_one(bson_doc)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            self.index_mgr
                .bind(py)
                .borrow()
                .add_doc(py, doc.as_any())?;
            if !_internal {
                let doc_id_str = format!("{}", engine_result.inserted_id);
                let version = self.bump_version(&doc_id_str);
                self.log_oplog(
                    py,
                    "insert",
                    &doc.get_item("_id")?
                        .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                    doc.as_any(),
                    version,
                    Some(Self::sorted_keys(&doc)),
                )?;
            }
            drop(guard);
            let id_py = bson_helpers::bson_to_py(py, &engine_result.inserted_id)?;
            let ids = PyList::new(py, [id_py])?;
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
        let result = (|| -> PyResult<Py<PyList>> {
            let _guard = self.acq_lock(py);
            let docs = self.find_matching_locked(py, query)?;
            let items: Vec<Bound<'_, PyDict>> =
                docs.into_iter().map(|d| d.into_bound(py)).collect();
            Ok(PyList::new(py, items)?.unbind())
        })();
        self.rel_read(py);
        result
    }

    pub(crate) fn find_one(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyAny>> {
        self.acq_read(py);
        let result = (|| -> PyResult<Py<PyAny>> {
            let _guard = self.acq_lock(py);
            let mut docs = self.find_matching_locked(py, query)?;
            if docs.is_empty() {
                Ok(py.None())
            } else {
                Ok(docs.remove(0).into_bound(py).unbind().into_any())
            }
        })();
        self.rel_read(py);
        result
    }

    pub(crate) fn count(&self, py: Python<'_>, query: Option<&Bound<'_, PyDict>>) -> PyResult<i64> {
        self.acq_read(py);
        let result = (|| -> PyResult<i64> {
            let _guard = self.acq_lock(py);
            let filter = query.map(bson_helpers::pydict_to_doc).transpose()?;
            let n = self.engine_col.count_documents(filter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(n as i64)
        })();
        self.rel_read(py);
        result
    }

    pub(crate) fn count_fast(&self, py: Python<'_>) -> PyResult<i64> {
        let _guard = self.acq_lock(py);
        let n = self.engine_col.count_documents(None)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(n as i64)
    }

    pub(crate) fn data_size_bytes(&self, py: Python<'_>) -> PyResult<i64> {
        let mut total = 0i64;
        let _guard = self.acq_lock(py);
        let mut cursor = self.open_data_cursor(None)?;
        while cursor.next().is_ok() {
            let raw = cursor.get_value_raw().py()?;
            total += raw.len() as i64;
        }
        cursor.close().py()?;
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
            let filter = bson_helpers::pydict_to_doc(q)?;
            let explain = self.engine_col.explain_find(filter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let rd = PyDict::new(py);
            rd.set_item("plan", format!("{:?}", explain.execution_plan))?;
            rd.set_item("reason", &explain.plan_reason)?;
            rd.set_item("indexUsed", explain.index_used.as_deref())?;
            if execute {
                let t0: f64 = crate::cached_modules::time_mod(py)?
                    .call_method0("monotonic")?
                    .extract()?;
                let filter2 = bson_helpers::pydict_to_doc(q)?;
                let docs = self.engine_col.find(filter2)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                let t1: f64 = crate::cached_modules::time_mod(py)?
                    .call_method0("monotonic")?
                    .extract()?;
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
        coll: Py<Self>,
        py: Python<'_>,
        query: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let sc = crate::streaming::RustStreamingCursor::from_collection(py, coll, query)?;
        Ok(sc.into_any())
    }

    pub(crate) fn get_all_typed(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.acq_read(py);
        let result = (|| -> PyResult<Py<PyList>> {
            let _guard = self.acq_lock(py);
            let docs = self.engine_col.find(bson::Document::new())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let py_docs: Vec<Bound<'_, PyDict>> = docs.iter()
                .map(|d| bson_helpers::doc_to_pydict(py, d))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_docs)?.unbind())
        })();
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
        let opts = UpdateOpts {
            query,
            update_spec,
            multi,
            upsert,
            array_filters,
            internal: _internal,
        };
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
            let _guard = self.acq_lock(py);
            let filter = bson_helpers::pydict_to_doc(query)?;
            let existing = self.engine_col.find_one(filter.clone())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let before_doc = match existing {
                Some(d) => d,
                None => return Ok(py.None()),
            };
            let before_py = bson_helpers::doc_to_pydict(py, &before_doc)?;

            let update_doc = bson_helpers::pydict_to_doc(
                update_spec.cast::<PyDict>()
                    .map_err(|_| PyRuntimeError::new_err("update spec must be a dict"))?,
            )?;
            self.engine_col.update_one(filter, update_doc)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let id_filter = bson::doc! { "_id": before_doc.get("_id").cloned().unwrap_or(bson::Bson::Null) };
            if let Some(adoc) = self
                .engine_col
                .find_one(id_filter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            {
                let after_py = bson_helpers::doc_to_pydict(py, &adoc)?;
                self.index_mgr.bind(py).borrow().update_doc(
                    py,
                    &before_py.as_any(),
                    &after_py.as_any(),
                )?;
            }

            if !_internal {
                let changed = Self::changed_fields_from_update(update_spec);
                let id_py = before_py.get_item("_id")?
                    .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?;
                self.log_oplog(py, "update", &id_py, update_spec, 1, Some(changed))?;
            }

            if return_document == "after" {
                let id_filter = bson::doc! { "_id": before_doc.get("_id").cloned().unwrap_or(bson::Bson::Null) };
                let after = self.engine_col.find_one(id_filter)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                match after {
                    Some(d) => Ok(bson_helpers::doc_to_pydict(py, &d)?.unbind().into_any()),
                    None => Ok(py.None()),
                }
            } else {
                Ok(before_py.unbind().into_any())
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
            let _guard = self.acq_lock(py);
            let filter = bson_helpers::pydict_to_doc(query)?;
            let existing = self.engine_col.find_one(filter.clone())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            if existing.is_none() {
                if upsert {
                    let rep = replacement.copy()?;
                    if rep.get_item("_id")?.is_none() {
                        let oid = Py::new(py, ObjectId::generate(py))?;
                        rep.set_item("_id", &oid)?;
                    }
                    self.validate_doc(py, &rep)?;
                    let bson_doc = bson_helpers::pydict_to_doc(&rep)?;
                    self.engine_col.insert_one(bson_doc)
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                    self.index_mgr
                        .bind(py)
                        .borrow()
                        .add_doc(py, rep.as_any())?;
                    if !_internal {
                        let id_py = rep.get_item("_id")?
                            .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?;
                        self.log_oplog(py, "insert", &id_py, rep.as_any(), 1, Some(Self::sorted_keys(&rep)))?;
                    }
                    return if return_document == "after" {
                        Ok(rep.unbind().into_any())
                    } else {
                        Ok(py.None())
                    };
                }
                return Ok(py.None());
            }
            let before_doc = existing.unwrap();
            let before_py = bson_helpers::doc_to_pydict(py, &before_doc)?;

            // Build replacement with the original _id
            let rep = bson_helpers::shallow_copy_dict(py, replacement)?;
            let id_bson = before_doc.get("_id").cloned().unwrap_or(bson::Bson::Null);
            rep.set_item("_id", bson_helpers::bson_to_py(py, &id_bson)?)?;
            self.validate_doc(py, &rep)?;

            self.index_mgr
                .bind(py)
                .borrow()
                .remove_doc(py, before_py.as_any())?;

            // Delete old, insert replacement (engine has no replace API)
            let id_filter = bson::doc! { "_id": id_bson.clone() };
            self.engine_col.delete_one(id_filter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let bson_rep = bson_helpers::pydict_to_doc(&rep)?;
            self.engine_col.insert_one(bson_rep)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            self.index_mgr
                .bind(py)
                .borrow()
                .add_doc(py, rep.as_any())?;

            if !_internal {
                let id_py = rep.get_item("_id")?
                    .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?;
                self.log_oplog(py, "update", &id_py, rep.as_any(), 1, None)?;
            }

            if return_document == "after" {
                Ok(rep.unbind().into_any())
            } else {
                Ok(before_py.unbind().into_any())
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
            let _guard = self.acq_lock(py);
            let filter = bson_helpers::pydict_to_doc(query)?;
            let existing = self.engine_col.find_one(filter.clone())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let doc = match existing {
                Some(d) => d,
                None => return Ok(py.None()),
            };
            let py_doc = bson_helpers::doc_to_pydict(py, &doc)?;

            self.index_mgr
                .bind(py)
                .borrow()
                .remove_doc(py, py_doc.as_any())?;

            self.engine_col.delete_one(filter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            if !_internal {
                let id_py = py_doc.get_item("_id")?
                    .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?;
                self.log_oplog(py, "delete", &id_py, py.None().bind(py), 1, None)?;
            }

            Ok(py_doc.unbind().into_any())
        })();
        self.rel_write(py);
        result
    }

    pub(crate) fn list_indexes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let _guard = self.acq_lock(py);
        let specs = self.engine_col.list_indexes()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut seen: HashSet<String> = HashSet::new();
        let result = PyList::empty(py);
        for s in &specs {
            seen.insert(s.name.clone());
            let d = PyDict::new(py);
            d.set_item("name", &s.name).ok();
            let keys_dict = PyDict::new(py);
            for (k, v) in s.keys.iter() {
                keys_dict
                    .set_item(
                        k,
                        bson_helpers::bson_to_py(py, v)
                            .unwrap_or_else(|_| py.None().into_bound(py)),
                    )
                    .ok();
            }
            d.set_item("keys", keys_dict).ok();
            d.set_item("unique", s.options.unique).ok();
            d.set_item("sparse", s.options.sparse).ok();
            result.append(d)?;
        }
        let rust_list = self.index_mgr.bind(py).borrow().list_indexes(py)?;
        let rl = rust_list.bind(py);
        for i in 0..rl.len() {
            let item = rl.get_item(i)?;
            let d = item.cast::<PyDict>()?;
            let n: String = d
                .get_item("name")?
                .ok_or_else(|| PyRuntimeError::new_err("index dict missing name"))?
                .extract()?;
            if !seen.contains(&n) {
                seen.insert(n);
                result.append(&d)?;
            }
        }
        Ok(result.unbind().into_any())
    }

    pub(crate) fn drop_index(&self, py: Python<'_>, name: &str, _internal: bool) -> PyResult<()> {
        {
            let _guard = self.acq_lock(py);
            let _ = self.index_mgr.bind(py).borrow_mut().drop_index(py, name);
            let _ = self.engine_col.drop_index(name);
        }
        if !_internal {
            self.oplog_w
                .bind(py)
                .call_method1("log", ("index_drop", name, py.None()))?;
        }
        Ok(())
    }

    pub(crate) fn create_index(
        &self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        _internal: bool,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let mut opts = smongo_engine::index::IndexOptions::default();
        if let Some(kw) = kwargs {
            if let Some(n) = kw.get_item("name")? {
                opts.name = Some(n.extract::<String>()?);
            }
            if let Some(u) = kw.get_item("unique")? {
                opts.unique = u.extract()?;
            }
            if let Some(s) = kw.get_item("sparse")? {
                opts.sparse = s.extract()?;
            }
        }
        // Do not call Python oplog / TTLReaper while holding `acq_lock`: `TTLReaper.maybe_start`
        // invokes `_collection.list_indexes()`, which acquires the same mutex (non-reentrant).
        let name = {
            let _guard = self.acq_lock(py);
            if Self::keys_use_rust_index_manager(keys, py)? {
                let name = self
                    .index_mgr
                    .bind(py)
                    .borrow_mut()
                    .create_index(py, keys, kwargs)?;
                let all = self.get_all_unlocked_vec(py)?;
                let list = PyList::empty(py);
                for d in &all {
                    list.append(d.bind(py))?;
                }
                self.index_mgr
                    .bind(py)
                    .borrow_mut()
                    .rebuild_index(py, &name, &list)?;
                name
            } else {
                let keys_doc = Self::keys_to_bson_index_doc(keys, py)?;
                let name = self
                    .engine_col
                    .create_index(keys_doc.clone(), Some(opts))
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                if smongo_engine::index::is_2dsphere_keys(&keys_doc) {
                    self.index_mgr.bind(py).borrow_mut().register_2dsphere_planner_metadata(
                        py, &name, keys, kwargs,
                    )?;
                }
                name
            }
        };
        if !_internal {
            self.oplog_w
                .bind(py)
                .call_method1("log", ("index_create", &name, py.None()))?;
        }
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
            cursor.close().py()?;
        }
        let n_indexes = self.engine_col.list_indexes()
            .map(|v| v.len())
            .unwrap_or(0);
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
        let indexes = self.engine_col.list_indexes()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut rebuilt = 0i64;
        for spec in &indexes {
            let name = spec.name.clone();
            let keys = spec.keys.clone();
            let opts = spec.options.clone();
            // Drop and re-create to force a full rebuild
            let _ = self.engine_col.drop_index(&name);
            self.engine_col.create_index(keys, Some(opts))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            rebuilt += 1;
        }

        let rust_names: Vec<String> = self
            .index_mgr
            .bind(py)
            .borrow()
            .indexes()
            .keys()
            .cloned()
            .collect();
        if !rust_names.is_empty() {
            let all = self.get_all_unlocked_vec(py)?;
            let list = PyList::empty(py);
            for d in &all {
                list.append(d.bind(py))?;
            }
            let mut im = self.index_mgr.bind(py).borrow_mut();
            for name in rust_names {
                im.rebuild_index(py, &name, &list)?;
                rebuilt += 1;
            }
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
        // SAFETY: self.conn_ptr is valid for the lifetime of self
        let wt_session = unsafe { open_session_from_conn_ptr(self.conn_ptr) }.py()?;
        Ok(RustWtSession::from_safe(wt_session))
    }

    // --- Reads ---

    fn get_all(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.get_all_typed(py)
    }

    fn get_by_id(&self, py: Python<'_>, doc_id: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.acq_read(py);
        let result = (|| -> PyResult<Py<PyAny>> {
            let _guard = self.acq_lock(py);
            let id_bson = bson_helpers::py_to_bson(doc_id)?;
            let filter = bson::doc! { "_id": id_bson };
            let doc = self.engine_col.find_one(filter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            match doc {
                Some(d) => Ok(bson_helpers::doc_to_pydict(py, &d)?.unbind().into_any()),
                None => Ok(py.None()),
            }
        })();
        self.rel_read(py);
        result
    }

    fn get_by_ids(&self, py: Python<'_>, doc_ids: &Bound<'_, PyList>) -> PyResult<Py<PyList>> {
        self.acq_read(py);
        let result = (|| -> PyResult<Py<PyList>> {
            let _guard = self.acq_lock(py);
            let mut docs: Vec<Bound<'_, PyDict>> = Vec::with_capacity(doc_ids.len());
            for id_py in doc_ids.iter() {
                let id_bson = bson_helpers::py_to_bson(&id_py)?;
                let filter = bson::doc! { "_id": id_bson };
                if let Some(d) = self.engine_col.find_one(filter)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))? {
                    docs.push(bson_helpers::doc_to_pydict(py, &d)?);
                }
            }
            Ok(PyList::new(py, docs)?.unbind())
        })();
        self.rel_read(py);
        result
    }

    fn _get_by_id_unlocked(
        &self,
        py: Python<'_>,
        doc_id: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let id_bson = bson_helpers::py_to_bson(doc_id)?;
        let filter = bson::doc! { "_id": id_bson };
        let doc = self.engine_col.find_one(filter)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        match doc {
            Some(d) => Ok(bson_helpers::doc_to_pydict(py, &d)?.unbind().into_any()),
            None => Ok(py.None()),
        }
    }

    fn _get_all_unlocked(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let docs = self.engine_col.find(bson::Document::new())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let py_docs: Vec<Bound<'_, PyDict>> = docs.iter()
            .map(|d| bson_helpers::doc_to_pydict(py, d))
            .collect::<PyResult<_>>()?;
        Ok(PyList::new(py, py_docs)?.unbind())
    }

    // --- Find / Count ---

    #[pyo3(name = "find")]
    fn py_find(&self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<Py<PyList>> {
        self.find(py, query)
    }

    fn _find_matching_docs(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyList>> {
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
    fn py_find_streaming(
        slf: &Bound<'_, Self>,
        query: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        Self::find_streaming_typed(slf.clone().unbind(), py, query)
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

    fn scan_with_fields(&self, py: Python<'_>, fields: &Bound<'_, PyList>) -> PyResult<Py<PyList>> {
        let field_strs: Vec<String> = fields.extract()?;
        let results = PyList::empty(py);
        self.acq_read(py);
        let inner = (|| -> PyResult<()> {
            let _guard = self.acq_lock(py);
            let mut cursor = self.open_data_cursor(None)?;
            while cursor.next().is_ok() {
                let raw = cursor.get_value_raw().py()?;
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
                let tup =
                    pyo3::types::PyTuple::new(py, [doc_id.bind(py).clone(), vals.into_any()])?;
                results.append(tup)?;
            }
            cursor.close().py()?;
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
        let mut bson_docs: Vec<bson::Document> = Vec::with_capacity(docs.len());
        for item in docs.iter() {
            let d: Bound<'_, PyDict> = item.extract()?;
            let d = d.copy()?;
            if d.get_item("_id")?.is_none() {
                d.set_item("_id", oid_cls.call0()?)?;
            }
            self.validate_doc(py, &d)?;
            bson_docs.push(bson_helpers::pydict_to_doc(&d)?);
            prepared.push(d);
        }

        self.acq_write(py);
        let result = (|| -> PyResult<Py<PyAny>> {
            let guard = self.acq_lock(py);
            let engine_result = self.engine_col.insert_many(bson_docs)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            for doc in &prepared {
                self.index_mgr
                    .bind(py)
                    .borrow()
                    .add_doc(py, doc.as_any())?;
            }
            if !_internal {
                for (i, doc) in prepared.iter().enumerate() {
                    let id_str = format!("{}", engine_result.inserted_ids[i]);
                    let version = self.bump_version(&id_str);
                    self.log_oplog(
                        py,
                        "insert",
                        &doc.get_item("_id")?
                            .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                        doc.as_any(),
                        version,
                        Some(Self::sorted_keys(doc)),
                    )?;
                }
            }
            drop(guard);
            let id_items: Vec<Bound<'_, PyAny>> = engine_result
                .inserted_ids
                .iter()
                .map(|id| bson_helpers::bson_to_py(py, id))
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
        self.update(
            py,
            query,
            update_spec,
            multi,
            upsert,
            array_filters,
            _internal,
        )
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
        self.oplog_w
            .bind(py)
            .call_method1("truncate_count", (keep,))?;
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
        // SAFETY: self.conn_ptr is valid for the lifetime of self
        if let Ok(vs_session) = unsafe { open_session_from_conn_ptr(self.conn_ptr) } {
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
            let n = self.engine_col.count_documents(None)
                .map(|c| c as i64)
                .unwrap_or(0);
            let ni = self.engine_col.list_indexes()
                .map(|v| v.len())
                .unwrap_or(0);
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
    fn update_inner(&self, py: Python<'_>, opts: &UpdateOpts<'_, '_>) -> PyResult<Py<PyAny>> {
        let _guard = self.acq_lock(py);

        let filter = bson_helpers::pydict_to_doc(opts.query)?;
        let update_doc = if let Ok(ud) = opts.update_spec.cast::<PyDict>() {
            bson_helpers::pydict_to_doc(ud)?
        } else {
            return Err(PyRuntimeError::new_err("update spec must be a dict"));
        };

        // Upsert not supported by engine — use direct path
        if opts.upsert {
            let existing = self.engine_col.find_one(filter.clone())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            if existing.is_none() {
                return self.do_upsert(
                    py,
                    opts.query,
                    opts.update_spec,
                    opts.array_filters,
                    opts.internal,
                );
            }
        }

        let engine_result = if opts.multi {
            self.engine_col.update_many(filter, update_doc)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        } else {
            self.engine_col.update_one(filter, update_doc)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        };

        if !opts.internal && engine_result.modified_count > 0 {
            let changed = Self::changed_fields_from_update(opts.update_spec);
            // Log a single oplog entry for the update operation
            self.log_oplog(
                py,
                "update",
                py.None().bind(py),
                opts.update_spec,
                1,
                Some(changed),
            )?;
        }

        Ok(Py::new(
            py,
            UpdateResult::new(
                py,
                engine_result.matched_count as i64,
                engine_result.modified_count as i64,
                None,
            ),
        )?
        .into_any())
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

        let bson_doc = bson_helpers::pydict_to_doc(&doc)?;
        let engine_result = self.engine_col.insert_one(bson_doc)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        self.index_mgr
            .bind(py)
            .borrow()
            .add_doc(py, doc.as_any())?;

        if !_internal {
            let id_str = format!("{}", engine_result.inserted_id);
            let version = self.bump_version(&id_str);
            self.log_oplog(
                py,
                "insert",
                &doc.get_item("_id")?
                    .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?,
                doc.as_any(),
                version,
                Some(Self::sorted_keys(&doc)),
            )?;
        }

        let upserted_id = doc
            .get_item("_id")?
            .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?
            .unbind();
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
        let filter = bson_helpers::pydict_to_doc(query)?;

        let engine_result = if multi {
            self.engine_col.delete_many(filter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        } else {
            self.engine_col.delete_one(filter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        };

        if !_internal && engine_result.deleted_count > 0 {
            self.log_oplog(
                py,
                "delete",
                py.None().bind(py),
                py.None().bind(py),
                1,
                None,
            )?;
        }

        Ok(Py::new(py, DeleteResult::new(engine_result.deleted_count as i64))?.into_any())
    }
}
