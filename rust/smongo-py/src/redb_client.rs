//! Redb-backed local client wrapping smongo-engine `Database<RedbBackend>`.
//!
//! Oplog and sync helpers use the same logical table names as the legacy `table:` URI scheme.

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine_errors::{map_collection_error, map_database_error};
use bson::{Bson, Document};
use smongo_engine::collection::{DeleteOptions, FindOptions, InsertOptions, UpdateOptions};
use smongo_engine::database::{Database, TransactionSession};
use smongo_engine::oplog::{OplogReader as EngineOplogReader, OplogWriter as EngineOplogWriter};
use smongo_engine::{RedbBackend, RedbSession, StorageSession};

/// Shared slot for an active multi-collection wire/API transaction (one per `RedbLocalClient`).
pub(crate) type RedbTxnSlot = Arc<Mutex<Option<TransactionSession<RedbSession>>>>;

fn strip_table_prefix(uri: &str) -> &str {
    uri.strip_prefix("table:").unwrap_or(uri)
}

/// Oplog URI string passed to sync helpers (historical `table:` prefix; stripped before redb I/O).
fn sync_oplog_uri(db: &str, coll: &str) -> String {
    format!("table:__oplog_{db}_{coll}")
}

/// Redb-backed client owning a smongo-engine Database<RedbBackend>.
#[pyclass]
pub struct RedbLocalClient {
    db: Option<Arc<Database<RedbBackend>>>,
    oplog_hub: Arc<smongo_engine::oplog::OplogHub>,
    txn_slot: RedbTxnSlot,
}

#[pymethods]
impl RedbLocalClient {
    #[new]
    pub fn new(db_path: &str) -> PyResult<Self> {
        let db = Database::open(db_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to open redb: {}", e)))?;

        Ok(Self {
            db: Some(Arc::new(db)),
            oplog_hub: Arc::new(smongo_engine::oplog::OplogHub::new()),
            txn_slot: Arc::new(Mutex::new(None)),
        })
    }

    /// Used by `serverStatus` / diagnostics (embedded engine metadata).
    pub fn connection_stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        d.set_item("uri", "redb:")?;
        d.set_item("engine", "smongo-engine/redb")?;
        Ok(d.unbind())
    }

    /// Begin a multi-collection transaction (wire / `TransactionSession`).
    pub fn wire_txn_begin(&self) -> PyResult<()> {
        let db_arc = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let mut g = self.txn_slot.lock();
        if g.is_some() {
            return Err(PyRuntimeError::new_err("transaction already in progress"));
        }
        let ts = db_arc
            .start_session()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        ts.begin_transaction()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        *g = Some(ts);
        Ok(())
    }

    pub fn wire_txn_commit(&self) -> PyResult<()> {
        let mut g = self.txn_slot.lock();
        let Some(ts) = g.take() else {
            return Err(PyRuntimeError::new_err("no transaction in progress"));
        };
        ts.commit_transaction()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn wire_txn_abort(&self) -> PyResult<()> {
        let mut g = self.txn_slot.lock();
        let Some(ts) = g.take() else {
            return Err(PyRuntimeError::new_err("no transaction in progress"));
        };
        ts.rollback_transaction()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn get_db(&self, py: Python<'_>, name: &str) -> PyResult<Py<RedbLocalDB>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;

        Py::new(
            py,
            RedbLocalDB {
                db: db.clone(),
                db_name: name.to_string(),
                collections: Mutex::new(HashMap::new()),
                oplog_hub: self.oplog_hub.clone(),
                txn_slot: self.txn_slot.clone(),
            },
        )
    }

    /// Sync: read UTF-8 checkpoint / DLQ value (`table:` prefix stripped for redb).
    pub fn sync_kv_get(&self, table_uri: &str, key: &str) -> PyResult<Option<String>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let table = strip_table_prefix(table_uri);
        db.redb_kv_get(table, key)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    pub fn sync_kv_put(&self, table_uri: &str, key: &str, value: &str) -> PyResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let table = strip_table_prefix(table_uri);
        db.redb_kv_put(table, key, value)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    pub fn sync_kv_remove(&self, table_uri: &str, key: &str) -> PyResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let table = strip_table_prefix(table_uri);
        db.redb_kv_remove(table, key)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    pub fn sync_kv_scan(&self, table_uri: &str) -> PyResult<Vec<(String, String)>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let table = strip_table_prefix(table_uri);
        db.redb_kv_scan(table)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Atomic checkpoint + oplog truncation (sync checkpoint + oplog tail in one step).
    pub fn sync_atomic_checkpoint_truncate(
        &self,
        checkpoint_table_uri: &str,
        checkpoint_key: &str,
        checkpoint_val: &str,
        oplog_uri: &str,
        safe_key: &str,
    ) -> PyResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let ck = strip_table_prefix(checkpoint_table_uri);
        let ot = strip_table_prefix(oplog_uri);
        db.redb_atomic_checkpoint_truncate_oplog(ck, checkpoint_key, checkpoint_val, ot, safe_key)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    pub fn close(&mut self) -> PyResult<()> {
        self.db.take();
        Ok(())
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(
        &mut self,
        _py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_val: &Bound<'_, PyAny>,
        _exc_tb: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

/// Database handle for a specific database name.
#[pyclass]
pub struct RedbLocalDB {
    db: Arc<Database<RedbBackend>>,
    db_name: String,
    collections: Mutex<HashMap<String, Py<RedbLocalCollection>>>,
    oplog_hub: Arc<smongo_engine::oplog::OplogHub>,
    txn_slot: RedbTxnSlot,
}

#[pymethods]
impl RedbLocalDB {
    /// PyMongo-style alias for :meth:`collection`.
    #[pyo3(name = "get_collection")]
    pub fn get_collection(&self, py: Python<'_>, name: &str) -> PyResult<Py<RedbLocalCollection>> {
        self.collection(py, name)
    }

    pub fn collection(&self, py: Python<'_>, name: &str) -> PyResult<Py<RedbLocalCollection>> {
        let mut colls = self.collections.lock();
        if let Some(c) = colls.get(name) {
            return Ok(c.clone_ref(py));
        }

        let node_id = Arc::new(Mutex::new(None));
        let oplog_w = Py::new(
            py,
            RedbOplogWriterBridge {
                oplog_uri: sync_oplog_uri(&self.db_name, name),
                node_id: node_id.clone(),
            },
        )?;

        let col = Py::new(
            py,
            RedbLocalCollection {
                db: self.db.clone(),
                db_name: self.db_name.clone(),
                name: name.to_string(),
                oplog_hub: self.oplog_hub.clone(),
                oplog_node_id: node_id,
                oplog_writer: oplog_w.clone_ref(py),
                txn_slot: self.txn_slot.clone(),
            },
        )?;
        colls.insert(name.to_string(), col.clone_ref(py));
        Ok(col)
    }

    pub(crate) fn get_collection_typed(
        &self,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<Py<RedbLocalCollection>> {
        self.collection(py, name)
    }

    pub fn list_collection_names(&self) -> PyResult<Vec<String>> {
        self.db
            .list_collection_names()
            .map_err(|e| PyRuntimeError::new_err(format!("list_collection_names: {}", e)))
    }

    /// Drop the collection table, best-effort drop of its oplog table, and evict cached handles.
    pub fn drop_collection(&self, _py: Python<'_>, name: &str) -> PyResult<()> {
        {
            let mut colls = self.collections.lock();
            colls.remove(name);
        }
        self.db
            .drop_collection(name)
            .map_err(|e| PyRuntimeError::new_err(format!("drop_collection: {}", e)))?;
        let oplog = format!("__oplog_{}_{}", self.db_name, name);
        if let Ok(session) = self.db.open_storage_session() {
            let _ = session.drop_table(&oplog);
        }
        Ok(())
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.db_name
    }
}

/// Exposes `oplog_uri` and `node_id` for SyncManager (`_stamp_node_id`).
#[pyclass(name = "RedbOplogWriterBridge")]
pub struct RedbOplogWriterBridge {
    oplog_uri: String,
    node_id: Arc<Mutex<Option<String>>>,
}

#[pymethods]
impl RedbOplogWriterBridge {
    #[getter]
    pub fn oplog_uri(&self) -> String {
        self.oplog_uri.clone()
    }

    #[getter]
    pub fn node_id(&self) -> Option<String> {
        self.node_id.lock().clone()
    }

    #[setter]
    pub fn set_node_id(&self, value: Option<String>) {
        *self.node_id.lock() = value;
    }
}

#[pyclass(name = "RedbOplogReaderBridge")]
pub struct RedbOplogReaderBridge {
    db: Arc<Database<RedbBackend>>,
    db_name: String,
    coll_name: String,
}

#[pymethods]
impl RedbOplogReaderBridge {
    #[pyo3(signature = (checkpoint_key=None, *, skip_internal=true))]
    pub fn read_from(
        &self,
        py: Python<'_>,
        checkpoint_key: Option<String>,
        skip_internal: bool,
    ) -> PyResult<Vec<(String, Py<PyDict>)>> {
        let oplog_table = format!("__oplog_{}_{}", self.db_name, self.coll_name);
        let session = self
            .db
            .open_storage_session()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let reader = EngineOplogReader::new(session, &oplog_table);
        let rows = reader
            .read_from(checkpoint_key.as_deref(), skip_internal)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for (k, entry) in rows {
            let d = oplog_entry_to_pydict(py, &entry)?;
            out.push((k, d));
        }
        Ok(out)
    }
}

fn oplog_entry_to_pydict(
    py: Python<'_>,
    entry: &smongo_engine::oplog::OplogEntry,
) -> PyResult<Py<PyDict>> {
    let doc = bson::to_document(entry)
        .map_err(|e| PyRuntimeError::new_err(format!("oplog entry: {}", e)))?;
    Ok(crate::bson_helpers::doc_to_pydict(py, &doc)?.unbind())
}

/// Collection handle with engine oplog enabled.
#[pyclass]
pub struct RedbLocalCollection {
    db: Arc<Database<RedbBackend>>,
    db_name: String,
    name: String,
    oplog_hub: Arc<smongo_engine::oplog::OplogHub>,
    oplog_node_id: Arc<Mutex<Option<String>>>,
    oplog_writer: Py<RedbOplogWriterBridge>,
    txn_slot: RedbTxnSlot,
}

impl RedbLocalCollection {
    /// Expose the shared database handle for engine-direct aggregation.
    pub fn db_arc(&self) -> Arc<Database<RedbBackend>> {
        Arc::clone(&self.db)
    }

    pub fn engine_col(&self) -> PyResult<smongo_engine::collection::Collection<RedbSession>> {
        if self.txn_slot.lock().is_some() {
            return Err(PyRuntimeError::new_err(
                "invalid engine collection access while a multi-document transaction is active",
            ));
        }
        let nid = self.oplog_node_id.lock().clone();
        self.db
            .collection_with_oplog(&self.db_name, &self.name, Some(self.oplog_hub.clone()), nid)
            .map_err(|e| map_database_error(e, "collection_with_oplog"))
    }

    /// Wire / streaming: lazy find iterator (wire hot path).
    pub(crate) fn find_streaming_typed(
        coll: Py<Self>,
        py: Python<'_>,
        query: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let filter: Option<Bound<'_, PyDict>> = match query {
            Some(v) => Some(v.cast::<PyDict>()?.clone()),
            None => None,
        };
        let iter = coll
            .bind(py)
            .borrow()
            .find_iter(filter.as_ref())?;
        Ok(Py::new(py, iter)?.into_any())
    }

    /// Aggregate: full collection scan as a Python list (wire helper).
    pub(crate) fn get_all_typed(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let empty = PyDict::new(py);
        self.find(py, &empty, None)
    }

    pub(crate) fn count(&self, py: Python<'_>, query: Option<&Bound<'_, PyDict>>) -> PyResult<i64> {
        let empty = PyDict::new(py);
        let q = query.unwrap_or(&empty);
        self.count_documents(q).map(|n| n as i64)
    }

    pub(crate) fn count_fast(&self, py: Python<'_>) -> PyResult<i64> {
        let empty = PyDict::new(py);
        self.count_documents(&empty).map(|n| n as i64)
    }

    pub(crate) fn data_size_bytes(&self, py: Python<'_>) -> PyResult<i64> {
        let list = self.get_all_typed(py)?;
        let lb = list.bind(py);
        let mut size: i64 = 0;
        for item in lb.try_iter()? {
            let item = item?;
            let d = item.cast::<PyDict>()?;
            let doc = crate::bson_helpers::pydict_to_doc(d)?;
            size += bson::to_vec(&doc).map_or(0, |v| v.len()) as i64;
        }
        Ok(size)
    }

    fn py_dict_to_update_result(py: Python<'_>, d: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        use crate::results::UpdateResult;
        let m = d
            .get_item("matched_count")?
            .and_then(|x| x.extract::<i64>().ok())
            .unwrap_or(0);
        let mc = d
            .get_item("modified_count")?
            .and_then(|x| x.extract::<i64>().ok())
            .unwrap_or(0);
        let uid_item = d.get_item("upserted_id")?;
        let uid = match uid_item {
            Some(v) if !v.is_none() => Some(v.unbind()),
            _ => None,
        };
        Ok(Py::new(py, UpdateResult::new(py, m, mc, uid))?.into_any())
    }

    /// Wire `update` command: operator updates via engine (`update_one` / `update_many`).
    pub(crate) fn update(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        update_spec: &Bound<'_, PyAny>,
        multi: bool,
        upsert: bool,
        internal: bool,
    ) -> PyResult<Py<PyAny>> {
        let u = update_spec.cast::<PyDict>()?;
        let d = if multi {
            self.update_many(py, query, u, internal, upsert)?
        } else {
            self.update_one(py, query, u, internal, upsert)?
        };
        Self::py_dict_to_update_result(py, d.bind(py))
    }

    /// Wire `delete` command (returns ``DeleteResult`` like the Python storage layer).
    pub(crate) fn delete(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        multi: bool,
        internal: bool,
    ) -> PyResult<Py<PyAny>> {
        let d = if multi {
            self.delete_many(py, query, internal)?
        } else {
            self.delete_one(py, query, internal)?
        };
        let dc = d
            .bind(py)
            .get_item("deleted_count")?
            .and_then(|v| v.extract::<i64>().ok())
            .unwrap_or(0);
        Ok(Py::new(py, crate::results::DeleteResult::new(dc))?.into_any())
    }

    pub(crate) fn find_one_and_delete_core(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        internal: bool,
    ) -> PyResult<Py<PyAny>> {
        let before_opt = self.find_one(py, filter, None)?;
        let Some(before_py) = before_opt else {
            return Ok(py.None().into_bound(py).into_any().unbind());
        };
        let before_bound = before_py.bind(py);
        let out = crate::bson_helpers::shallow_copy_dict(py, before_bound.cast::<PyDict>()?)?
            .unbind()
            .into_bound(py)
            .into_any()
            .unbind();
        let id = before_bound
            .get_item("_id")?
            .ok_or_else(|| PyRuntimeError::new_err("missing _id"))?;
        let id_filter = PyDict::new(py);
        id_filter.set_item("_id", id)?;
        self.delete_one(py, &id_filter, internal)?;
        Ok(out)
    }

    pub(crate) fn find_one_and_update_core(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        update_spec: &Bound<'_, PyAny>,
        return_document: &str,
        internal: bool,
    ) -> PyResult<Py<PyAny>> {
        let before_opt = self.find_one(py, filter, None)?;
        let Some(before_py) = before_opt else {
            return Ok(py.None().into_bound(py).into_any().unbind());
        };
        let before_copy =
            crate::bson_helpers::shallow_copy_dict(py, before_py.bind(py).cast::<PyDict>()?)?
                .unbind();
        let update_dict = update_spec.cast::<PyDict>()?;
        self.update_one(py, filter, update_dict, internal, false)?;
        if return_document == "after" {
            let after = self
                .find_one(py, filter, None)?
                .unwrap_or_else(|| before_py.clone_ref(py));
            Ok(after.into_bound(py).into_any().unbind())
        } else {
            Ok(before_copy.into_bound(py).into_any().unbind())
        }
    }

    pub(crate) fn find_one_and_replace_core(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        replacement: &Bound<'_, PyDict>,
        upsert: bool,
        return_document: &str,
        internal: bool,
    ) -> PyResult<Py<PyAny>> {
        let Some(before_py) = self.find_one(py, filter, None)? else {
            if !upsert {
                return Ok(py.None().into_bound(py).into_any().unbind());
            }
            let rep = crate::bson_helpers::shallow_copy_dict(py, replacement)?;
            self.insert_one(py, &rep, internal)?;
            if return_document == "after" {
                return Ok(rep.clone().into_any().unbind());
            }
            return Ok(py.None().into_bound(py).into_any().unbind());
        };
        let before_bound = before_py.bind(py);
        let before_store =
            crate::bson_helpers::shallow_copy_dict(py, before_bound.cast::<PyDict>()?)?.unbind();
        let id = before_bound
            .get_item("_id")?
            .ok_or_else(|| PyRuntimeError::new_err("missing _id"))?;
        let id_filter = PyDict::new(py);
        id_filter.set_item("_id", &id)?;
        let rep = crate::bson_helpers::shallow_copy_dict(py, replacement)?;
        rep.set_item("_id", id)?;
        self.delete_one(py, &id_filter, internal)?;
        self.insert_one(py, &rep, internal)?;
        if return_document == "after" {
            Ok(rep.unbind().into_bound(py).into_any().unbind())
        } else {
            Ok(before_store.into_bound(py).into_any().unbind())
        }
    }
}

#[pymethods]
impl RedbLocalCollection {
    #[getter]
    pub fn _oplog_w(slf: PyRef<'_, Self>) -> Py<RedbOplogWriterBridge> {
        slf.oplog_writer.clone_ref(slf.py())
    }

    pub fn get_oplog_reader(&self, py: Python<'_>) -> PyResult<Py<RedbOplogReaderBridge>> {
        Py::new(
            py,
            RedbOplogReaderBridge {
                db: self.db.clone(),
                db_name: self.db_name.clone(),
                coll_name: self.name.clone(),
            },
        )
    }

    /// Drop oldest oplog rows so at most *keep* entries remain (matches Python :meth:`OplogWriter.truncate_count`).
    pub fn compact_oplog(&self, py: Python<'_>, keep: i64) -> PyResult<i64> {
        let _ = py;
        if self.txn_slot.lock().is_some() {
            return Err(PyRuntimeError::new_err(
                "compact_oplog is not allowed during an active multi-document transaction",
            ));
        }
        let oplog_table = format!("__oplog_{}_{}", self.db_name, self.name);
        let session = self
            .db
            .open_storage_session()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let ns = format!("{}.{}", self.db_name, self.name);
        let writer =
            EngineOplogWriter::new(session, &oplog_table, &ns, Some(self.oplog_hub.clone()));
        writer
            .truncate_count(keep)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    pub fn get_by_id(
        &self,
        py: Python<'_>,
        doc_id: Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let id = crate::bson_helpers::py_to_bson(&doc_id)?;
        let mut filter = Document::new();
        filter.insert("_id", id);
        let guard = self.txn_slot.lock();
        if let Some(ref ts) = *guard {
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            let doc = v
                .find_one(filter)
                .map_err(|e| map_collection_error(e, "find_one"))?;
            return match doc {
                Some(d) => Ok(Some(crate::bson_helpers::doc_to_pydict(py, &d)?.unbind())),
                None => Ok(None),
            };
        }
        drop(guard);
        let collection = self.engine_col()?;
        let doc = collection
            .find_one(filter)
            .map_err(|e| map_collection_error(e, "find_one"))?;
        match doc {
            Some(d) => Ok(Some(crate::bson_helpers::doc_to_pydict(py, &d)?.unbind())),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (document, *, internal=false))]
    pub fn insert_one(
        &self,
        py: Python<'_>,
        document: &Bound<'_, PyDict>,
        internal: bool,
    ) -> PyResult<Py<PyDict>> {
        let doc = crate::bson_helpers::pydict_to_doc(document)?;
        let guard = self.txn_slot.lock();
        let result = if let Some(ref ts) = *guard {
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            v.insert_one(doc)
                .map_err(|e| map_collection_error(e, "insert_one"))?
        } else {
            drop(guard);
            let collection = self.engine_col()?;
            collection
                .insert_one_with_options(doc, InsertOptions { internal })
                .map_err(|e| map_collection_error(e, "insert_one"))?
        };
        let result_dict = PyDict::new(py);
        result_dict.set_item(
            "inserted_id",
            crate::bson_helpers::bson_to_py(py, &result.inserted_id)?,
        )?;
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (documents, *, internal=false))]
    pub fn insert_many(
        &self,
        py: Python<'_>,
        documents: &Bound<'_, PyList>,
        internal: bool,
    ) -> PyResult<Py<PyDict>> {
        let docs: Vec<_> = documents
            .iter()
            .map(|d| {
                d.cast::<PyDict>()
                    .map_err(|_| PyRuntimeError::new_err("Expected dict"))
                    .and_then(|d| crate::bson_helpers::pydict_to_doc(d))
            })
            .collect::<PyResult<_>>()?;
        let guard = self.txn_slot.lock();
        if let Some(ref ts) = *guard {
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            let mut ids = Vec::new();
            for doc in docs {
                let r = v
                    .insert_one(doc)
                    .map_err(|e| map_collection_error(e, "insert_many"))?;
                ids.push(r.inserted_id);
            }
            let result_dict = PyDict::new(py);
            let id_list = PyList::empty(py);
            for id in ids {
                id_list.append(crate::bson_helpers::bson_to_py(py, &id)?)?;
            }
            result_dict.set_item("inserted_ids", id_list)?;
            let _ = internal;
            return Ok(result_dict.unbind());
        }
        drop(guard);
        let collection = self.engine_col()?;
        let result = collection
            .insert_many_with_options(docs, InsertOptions { internal })
            .map_err(|e| map_collection_error(e, "insert_many"))?;
        let result_dict = PyDict::new(py);
        let ids = PyList::empty(py);
        for id in result.inserted_ids {
            ids.append(crate::bson_helpers::bson_to_py(py, &id)?)?;
        }
        result_dict.set_item("inserted_ids", ids)?;
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (filter, projection=None))]
    pub fn find_one(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        projection: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let guard = self.txn_slot.lock();
        if let Some(ref ts) = *guard {
            if projection.is_some() {
                return Err(PyRuntimeError::new_err(
                    "projection not supported in multi-document transaction (redb)",
                ));
            }
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            let doc = v
                .find_one(query)
                .map_err(|e| map_collection_error(e, "find_one"))?;
            return match doc {
                Some(d) => Ok(Some(crate::bson_helpers::doc_to_pydict(py, &d)?.unbind())),
                None => Ok(None),
            };
        }
        drop(guard);
        let collection = self.engine_col()?;
        let doc = match projection {
            Some(p) => {
                let proj = crate::bson_helpers::pydict_to_doc(p)?;
                if proj.is_empty() {
                    collection
                        .find_one(query)
                        .map_err(|e| map_collection_error(e, "find_one"))?
                } else {
                    collection
                        .find_one_with_options(
                            query,
                            FindOptions {
                                projection: Some(proj),
                                ..Default::default()
                            },
                        )
                        .map_err(|e| map_collection_error(e, "find_one"))?
                }
            }
            None => collection
                .find_one(query)
                .map_err(|e| map_collection_error(e, "find_one"))?,
        };
        match doc {
            Some(d) => Ok(Some(crate::bson_helpers::doc_to_pydict(py, &d)?.unbind())),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (filter, projection=None))]
    pub fn find(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        projection: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyList>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let guard = self.txn_slot.lock();
        if let Some(ref ts) = *guard {
            if projection.is_some() {
                return Err(PyRuntimeError::new_err(
                    "projection not supported in multi-document transaction (redb)",
                ));
            }
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            let docs = v.find(query).map_err(|e| map_collection_error(e, "find"))?;
            let results = PyList::empty(py);
            for doc in docs {
                results.append(crate::bson_helpers::doc_to_pydict(py, &doc)?)?;
            }
            return Ok(results.unbind());
        }
        drop(guard);
        let collection = self.engine_col()?;
        let docs = match projection {
            Some(p) => {
                let proj = crate::bson_helpers::pydict_to_doc(p)?;
                if proj.is_empty() {
                    collection
                        .find(query)
                        .map_err(|e| map_collection_error(e, "find"))?
                } else {
                    collection
                        .find_with_options(
                            query,
                            FindOptions {
                                projection: Some(proj),
                                ..Default::default()
                            },
                        )
                        .map_err(|e| map_collection_error(e, "find"))?
                }
            }
            None => collection
                .find(query)
                .map_err(|e| map_collection_error(e, "find"))?,
        };
        let results = PyList::empty(py);
        for doc in docs {
            results.append(crate::bson_helpers::doc_to_pydict(py, &doc)?)?;
        }
        Ok(results.unbind())
    }

    pub fn get_all(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let empty = PyDict::new(py);
        self.find(py, &empty, None)
    }

    /// Return a lazy :class:`FindIterator` that yields documents one at a time.
    ///
    /// Unlike :meth:`find`, this does **not** materialise the full result set
    /// up front.  The engine's query planner still selects the optimal index
    /// strategy (PK lookup, index seek/scan, or collection scan).
    #[pyo3(signature = (filter=None))]
    pub fn find_iter(&self, filter: Option<&Bound<'_, PyDict>>) -> PyResult<FindIterator> {
        let query = match filter {
            Some(f) => crate::bson_helpers::pydict_to_doc(f)?,
            None => bson::Document::new(),
        };
        let collection = self.engine_col()?;
        let owned_iter = collection
            .find_into_iter(query)
            .map_err(|e| map_collection_error(e, "find_iter"))?;
        Ok(FindIterator {
            inner: parking_lot::Mutex::new(owned_iter),
        })
    }

    /// Run a full aggregation pipeline entirely in the Rust engine.
    ///
    /// Uses `DatabaseContext` for cross-collection resolution, vector search,
    /// geo queries, and write stages — zero FFI round-trips.
    ///
    /// Leading `$match` stages are automatically extracted and pushed into the
    /// storage-layer `find()` so the query planner can use indexes instead of
    /// scanning the entire collection.
    #[pyo3(signature = (pipeline, *, filter=None))]
    pub fn aggregate_engine(
        &self,
        py: Python<'_>,
        pipeline: &Bound<'_, PyList>,
        filter: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyList>> {
        let explicit_filter = match filter {
            Some(f) => crate::bson_helpers::pydict_to_doc(f)?,
            None => bson::Document::new(),
        };

        let bson_pipeline = crate::bson_helpers::pylist_to_pipeline(pipeline)?;

        let (leading_match, remaining_pipeline) =
            smongo_engine::aggregation::optimize_pipeline(&bson_pipeline);

        let merged_filter = match leading_match {
            Some(lm) if explicit_filter.is_empty() => lm,
            Some(lm) => {
                bson::doc! { "$and": [ explicit_filter, lm ] }
            }
            None => explicit_filter,
        };

        let collection = self.engine_col()?;
        let iter = collection
            .find_into_iter(merged_filter)
            .map_err(|e| map_collection_error(e, "aggregate_engine find_into_iter"))?;

        let ctx = smongo_engine::aggregation::DatabaseContext::new(&*self.db);
        let results = smongo_engine::aggregation::aggregate_with_db_collection_streaming(
            iter,
            &remaining_pipeline,
            &ctx,
            Some(&self.name),
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let out = PyList::empty(py);
        for doc in &results {
            out.append(crate::bson_helpers::doc_to_pydict(py, doc)?)?;
        }
        Ok(out.unbind())
    }

    #[pyo3(signature = (filter, update, *, return_document="before", internal=false))]
    pub fn find_one_and_update(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        update: &Bound<'_, PyAny>,
        return_document: &str,
        internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.find_one_and_update_core(py, filter, update, return_document, internal)
    }

    #[pyo3(signature = (filter, *, internal=false))]
    pub fn find_one_and_delete(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.find_one_and_delete_core(py, filter, internal)
    }

    #[pyo3(signature = (filter, replacement, *, upsert=false, return_document="before", internal=false))]
    pub fn find_one_and_replace(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        replacement: &Bound<'_, PyDict>,
        upsert: bool,
        return_document: &str,
        internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.find_one_and_replace_core(py, filter, replacement, upsert, return_document, internal)
    }

    pub fn count_documents(&self, filter: &Bound<'_, PyDict>) -> PyResult<u64> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let guard = self.txn_slot.lock();
        if let Some(ref ts) = *guard {
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            return v
                .count_documents(Some(query))
                .map_err(|e| map_collection_error(e, "count_documents"));
        }
        drop(guard);
        let collection = self.engine_col()?;
        collection
            .count_documents(Some(query))
            .map_err(|e| map_collection_error(e, "count_documents"))
    }

    #[pyo3(signature = (filter, *, execute=false))]
    pub fn explain(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        execute: bool,
    ) -> PyResult<Py<PyDict>> {
        let _ = execute;
        if self.txn_slot.lock().is_some() {
            let rd = PyDict::new(py);
            rd.set_item("plan", "collection_scan")?;
            rd.set_item("reason", "transaction_active")?;
            rd.set_item("indexUsed", py.None())?;
            return Ok(rd.unbind());
        }
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let collection = self.engine_col()?;
        let er = collection
            .explain_find(query)
            .map_err(|e| map_collection_error(e, "explain"))?;
        let doc = bson::to_document(&er)
            .map_err(|e| PyRuntimeError::new_err(format!("explain serialize: {}", e)))?;
        Ok(crate::bson_helpers::doc_to_pydict(py, &doc)?.unbind())
    }

    #[pyo3(signature = (filter, update, *, internal=false, upsert=false))]
    pub fn update_one(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        update: &Bound<'_, PyDict>,
        internal: bool,
        upsert: bool,
    ) -> PyResult<Py<PyDict>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let update_doc = crate::bson_helpers::pydict_to_doc(update)?;
        let guard = self.txn_slot.lock();
        if let Some(ref ts) = *guard {
            if upsert {
                return Err(PyRuntimeError::new_err(
                    "upsert not supported in multi-document transaction (redb)",
                ));
            }
            let _ = internal;
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            let result = v
                .update_one(query, update_doc)
                .map_err(|e| map_collection_error(e, "update_one"))?;
            let result_dict = PyDict::new(py);
            result_dict.set_item("matched_count", result.matched_count)?;
            result_dict.set_item("modified_count", result.modified_count)?;
            return Ok(result_dict.unbind());
        }
        drop(guard);
        let collection = self.engine_col()?;
        let result = collection
            .update_one_with_options(query, update_doc, UpdateOptions { upsert, internal })
            .map_err(|e| map_collection_error(e, "update_one"))?;
        let result_dict = PyDict::new(py);
        result_dict.set_item("matched_count", result.matched_count)?;
        result_dict.set_item("modified_count", result.modified_count)?;
        if let Some(ref uid) = result.upserted_id {
            result_dict.set_item("upserted_id", crate::bson_helpers::bson_to_py(py, uid)?)?;
        }
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (filter, update, *, internal=false, upsert=false))]
    pub fn update_many(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        update: &Bound<'_, PyDict>,
        internal: bool,
        upsert: bool,
    ) -> PyResult<Py<PyDict>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let update_doc = crate::bson_helpers::pydict_to_doc(update)?;
        let guard = self.txn_slot.lock();
        if let Some(ref ts) = *guard {
            if upsert {
                return Err(PyRuntimeError::new_err(
                    "upsert not supported in multi-document transaction (redb)",
                ));
            }
            let _ = internal;
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            let result = v
                .update_many(query, update_doc)
                .map_err(|e| map_collection_error(e, "update_many"))?;
            let result_dict = PyDict::new(py);
            result_dict.set_item("matched_count", result.matched_count)?;
            result_dict.set_item("modified_count", result.modified_count)?;
            return Ok(result_dict.unbind());
        }
        drop(guard);
        let collection = self.engine_col()?;
        let result = collection
            .update_many_with_options(query, update_doc, UpdateOptions { upsert, internal })
            .map_err(|e| map_collection_error(e, "update_many"))?;
        let result_dict = PyDict::new(py);
        result_dict.set_item("matched_count", result.matched_count)?;
        result_dict.set_item("modified_count", result.modified_count)?;
        if let Some(ref uid) = result.upserted_id {
            result_dict.set_item("upserted_id", crate::bson_helpers::bson_to_py(py, uid)?)?;
        }
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (filter, *, internal=false))]
    pub fn delete_one(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        internal: bool,
    ) -> PyResult<Py<PyDict>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let guard = self.txn_slot.lock();
        if let Some(ref ts) = *guard {
            let _ = internal;
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            let result = v
                .delete_one(query)
                .map_err(|e| map_collection_error(e, "delete_one"))?;
            let result_dict = PyDict::new(py);
            result_dict.set_item("deleted_count", result.deleted_count)?;
            return Ok(result_dict.unbind());
        }
        drop(guard);
        let collection = self.engine_col()?;
        let result = collection
            .delete_one_with_options(query, DeleteOptions { internal })
            .map_err(|e| map_collection_error(e, "delete_one"))?;
        let result_dict = PyDict::new(py);
        result_dict.set_item("deleted_count", result.deleted_count)?;
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (filter, *, internal=false))]
    pub fn delete_many(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        internal: bool,
    ) -> PyResult<Py<PyDict>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let guard = self.txn_slot.lock();
        if let Some(ref ts) = *guard {
            let _ = internal;
            let v = ts
                .collection(&self.name)
                .map_err(|e| map_database_error(e, "collection"))?;
            let result = v
                .delete_many(query)
                .map_err(|e| map_collection_error(e, "delete_many"))?;
            let result_dict = PyDict::new(py);
            result_dict.set_item("deleted_count", result.deleted_count)?;
            return Ok(result_dict.unbind());
        }
        drop(guard);
        let collection = self.engine_col()?;
        let result = collection
            .delete_many_with_options(query, DeleteOptions { internal })
            .map_err(|e| map_collection_error(e, "delete_many"))?;
        let result_dict = PyDict::new(py);
        result_dict.set_item("deleted_count", result.deleted_count)?;
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (keys, options=None))]
    pub fn create_index(
        &self,
        _py: Python<'_>,
        keys: &Bound<'_, PyDict>,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        if self.txn_slot.lock().is_some() {
            return Err(PyRuntimeError::new_err(
                "createIndex not supported during multi-document transaction",
            ));
        }
        let keys_doc = crate::bson_helpers::pydict_to_doc(keys)?;
        let opts = if let Some(opts_dict) = options {
            let mut opts_doc = crate::bson_helpers::pydict_to_doc(opts_dict)?;
            // Wire / PyMongo often omit false defaults; engine serde expects these keys.
            opts_doc
                .entry("unique".to_string())
                .or_insert(Bson::Boolean(false));
            opts_doc
                .entry("sparse".to_string())
                .or_insert(Bson::Boolean(false));
            opts_doc
                .entry("background".to_string())
                .or_insert(Bson::Boolean(false));
            if let Some(v) = opts_doc.remove("expireAfterSeconds") {
                opts_doc.insert("expire_after_seconds", v);
            }
            if let Some(v) = opts_doc.remove("partialFilterExpression") {
                opts_doc.insert("partial_filter_expression", v);
            }
            Some(
                bson::from_document::<smongo_engine::index::IndexOptions>(opts_doc).map_err(
                    |e| PyRuntimeError::new_err(format!("Invalid index options: {}", e)),
                )?,
            )
        } else {
            None
        };
        let collection = self.engine_col()?;
        collection
            .create_index(keys_doc, opts)
            .map_err(|e| map_collection_error(e, "create_index"))
    }

    pub fn drop_index(&self, name: &str) -> PyResult<()> {
        if self.txn_slot.lock().is_some() {
            return Err(PyRuntimeError::new_err(
                "dropIndex not supported during multi-document transaction",
            ));
        }
        let collection = self.engine_col()?;
        collection
            .drop_index(name)
            .map_err(|e| map_collection_error(e, "drop_index"))
    }

    pub fn list_indexes(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        if self.txn_slot.lock().is_some() {
            return Err(PyRuntimeError::new_err(
                "listIndexes not supported during multi-document transaction",
            ));
        }
        let collection = self.engine_col()?;
        let indexes = collection
            .list_indexes()
            .map_err(|e| map_collection_error(e, "list_indexes"))?;
        let result = PyList::empty(py);
        for idx in indexes {
            let doc = bson::to_document(&idx)
                .map_err(|e| PyRuntimeError::new_err(format!("serialize index: {}", e)))?;
            result.append(crate::bson_helpers::doc_to_pydict(py, &doc)?)?;
        }
        Ok(result.unbind())
    }

    pub fn rebuild_all_indexes(&self, _py: Python<'_>) -> PyResult<i64> {
        if self.txn_slot.lock().is_some() {
            return Err(PyRuntimeError::new_err(
                "reIndex not supported during multi-document transaction",
            ));
        }
        let collection = self.engine_col()?;
        collection
            .rebuild_all_indexes()
            .map_err(|e| map_collection_error(e, "rebuild_all_indexes"))
    }

    /// Delete documents expired per TTL indexes (engine `reap_expired`; caller-driven, no background thread).
    pub fn reap_expired(&self) -> PyResult<u64> {
        if self.txn_slot.lock().is_some() {
            return Err(PyRuntimeError::new_err(
                "reap_expired not supported during multi-document transaction",
            ));
        }
        let collection = self.engine_col()?;
        collection
            .reap_expired()
            .map_err(|e| map_collection_error(e, "reap_expired"))
    }

    pub fn storage_stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let collection = self.engine_col()?;
        let docs = collection
            .find(Document::new())
            .map_err(|e| map_collection_error(e, "find"))?;
        let mut doc_count = 0i64;
        let mut data_size = 0i64;
        for d in &docs {
            doc_count += 1;
            data_size += bson::to_vec(d).map(|v| v.len()).unwrap_or(0) as i64;
        }
        let n_indexes = collection.list_indexes().map(|v| v.len()).unwrap_or(0);
        let storage_size = data_size + (doc_count * 64);
        let storage_engine = PyDict::new(py);
        storage_engine.set_item("name", "redb")?;
        let result = PyDict::new(py);
        result.set_item("count", doc_count)?;
        result.set_item("dataSize", data_size)?;
        result.set_item("storageSize", storage_size)?;
        result.set_item("nindexes", n_indexes + 1)?;
        result.set_item("totalIndexSize", 0i64)?;
        let idx_sizes = PyDict::new(py);
        idx_sizes.set_item("_id_", 0i64)?;
        result.set_item("indexSizes", idx_sizes)?;
        result.set_item("storageEngine", storage_engine)?;
        Ok(result.unbind())
    }

    /// Subscribe to local oplog via engine ChangeStream (optional pipeline unused).
    pub fn watch(
        &self,
        py: Python<'_>,
        _pipeline: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<RedbChangeStream>> {
        let stream = Arc::new(smongo_engine::oplog::ChangeStream::new(
            Some(format!("{}.{}", self.db_name, self.name)),
            None,
        ));
        self.oplog_hub.register(stream.clone());
        Py::new(
            py,
            RedbChangeStream {
                stream,
                hub: self.oplog_hub.clone(),
            },
        )
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[pyclass(name = "RedbChangeStream")]
pub struct RedbChangeStream {
    stream: Arc<smongo_engine::oplog::ChangeStream>,
    hub: Arc<smongo_engine::oplog::OplogHub>,
}

#[pymethods]
impl RedbChangeStream {
    pub fn try_next(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let ev = self.stream.try_next();
        match ev {
            Some(e) => {
                let doc = bson::to_document(&e)
                    .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
                Ok(Some(crate::bson_helpers::doc_to_pydict(py, &doc)?.unbind()))
            }
            None => Ok(None),
        }
    }

    pub fn close(&mut self) {
        self.hub.unregister(self.stream.as_ref());
        self.stream.close();
    }
}

/// Lazy Python iterator backed by the engine's streaming `FindCursor`.
///
/// Documents are decoded one-at-a-time as Python calls `__next__`, avoiding
/// materialisation of the entire result set.  The iterator owns the engine
/// `Collection` so the storage session stays alive for the whole traversal.
#[pyclass(name = "FindIterator")]
pub struct FindIterator {
    inner: parking_lot::Mutex<smongo_engine::OwnedFindIter<RedbSession>>,
}

#[pymethods]
impl FindIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let mut guard = self.inner.lock();
        match guard.next() {
            Some(Ok(doc)) => Ok(Some(crate::bson_helpers::doc_to_pydict(py, &doc)?.unbind())),
            Some(Err(e)) => Err(PyRuntimeError::new_err(e.to_string())),
            None => Ok(None),
        }
    }
}
