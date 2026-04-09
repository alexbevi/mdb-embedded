//! Redb-backed local client wrapping smongo-engine Database<RedbBackend>.
//!
//! Parallel to storage_engine.rs (WT-backed), but uses smongo-engine exclusively.
//! Includes oplog + sync helper methods for Python SyncManager.

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;

use bson::Document;
use smongo_engine::collection::{DeleteOptions, FindOptions, InsertOptions, UpdateOptions};
use smongo_engine::database::Database;
use smongo_engine::oplog::OplogReader as EngineOplogReader;
use smongo_engine::{RedbBackend, RedbSession, StorageSession};

fn strip_table_prefix(uri: &str) -> &str {
    uri.strip_prefix("table:").unwrap_or(uri)
}

/// WiredTiger-style URI for sync (`table:__oplog_...`).
fn oplog_uri_wt(db: &str, coll: &str) -> String {
    format!("table:__oplog_{db}_{coll}")
}

/// Redb-backed client owning a smongo-engine Database<RedbBackend>.
#[pyclass]
pub struct RedbLocalClient {
    db: Option<Arc<Database<RedbBackend>>>,
    oplog_hub: Arc<smongo_engine::oplog::OplogHub>,
}

#[pymethods]
impl RedbLocalClient {
    #[new]
    fn new(db_path: &str) -> PyResult<Self> {
        let db = Database::open(db_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to open redb: {}", e)))?;

        Ok(Self {
            db: Some(Arc::new(db)),
            oplog_hub: Arc::new(smongo_engine::oplog::OplogHub::new()),
        })
    }

    fn get_db(&self, py: Python<'_>, name: &str) -> PyResult<Py<RedbLocalDB>> {
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
            },
        )
    }

    /// Sync: read UTF-8 checkpoint / DLQ value (`table:` prefix stripped for redb).
    fn sync_kv_get(&self, table_uri: &str, key: &str) -> PyResult<Option<String>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let table = strip_table_prefix(table_uri);
        db.redb_kv_get(table, key)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn sync_kv_put(&self, table_uri: &str, key: &str, value: &str) -> PyResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let table = strip_table_prefix(table_uri);
        db.redb_kv_put(table, key, value)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn sync_kv_remove(&self, table_uri: &str, key: &str) -> PyResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let table = strip_table_prefix(table_uri);
        db.redb_kv_remove(table, key)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn sync_kv_scan(&self, table_uri: &str) -> PyResult<Vec<(String, String)>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client closed"))?;
        let table = strip_table_prefix(table_uri);
        db.redb_kv_scan(table)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Atomic checkpoint + oplog truncation (matches WiredTiger sync semantics).
    fn sync_atomic_checkpoint_truncate(
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

    fn close(&mut self) -> PyResult<()> {
        self.db.take();
        Ok(())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
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
}

#[pymethods]
impl RedbLocalDB {
    fn collection(&self, py: Python<'_>, name: &str) -> PyResult<Py<RedbLocalCollection>> {
        let mut colls = self.collections.lock();
        if let Some(c) = colls.get(name) {
            return Ok(c.clone_ref(py));
        }

        let node_id = Arc::new(Mutex::new(None));
        let oplog_w = Py::new(
            py,
            RedbOplogWriterBridge {
                oplog_uri: oplog_uri_wt(&self.db_name, name),
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
            },
        )?;
        colls.insert(name.to_string(), col.clone_ref(py));
        Ok(col)
    }

    fn list_collection_names(&self) -> PyResult<Vec<String>> {
        self.db
            .list_collection_names()
            .map_err(|e| PyRuntimeError::new_err(format!("list_collection_names: {}", e)))
    }

    /// Drop the collection table, best-effort drop of its oplog table, and evict cached handles.
    fn drop_collection(&self, _py: Python<'_>, name: &str) -> PyResult<()> {
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
    fn name(&self) -> &str {
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
    fn oplog_uri(&self) -> String {
        self.oplog_uri.clone()
    }

    #[getter]
    fn node_id(&self) -> Option<String> {
        self.node_id.lock().clone()
    }

    #[setter]
    fn set_node_id(&self, value: Option<String>) {
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
    fn read_from(
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

fn oplog_entry_to_pydict(py: Python<'_>, entry: &smongo_engine::oplog::OplogEntry) -> PyResult<Py<PyDict>> {
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
}

impl RedbLocalCollection {
    fn engine_col(&self) -> PyResult<smongo_engine::collection::Collection<RedbSession>> {
        let nid = self.oplog_node_id.lock().clone();
        self.db
            .collection_with_oplog(
                &self.db_name,
                &self.name,
                Some(self.oplog_hub.clone()),
                nid,
            )
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

#[pymethods]
impl RedbLocalCollection {
    #[getter]
    fn _oplog_w(slf: PyRef<'_, Self>) -> Py<RedbOplogWriterBridge> {
        slf.oplog_writer.clone_ref(slf.py())
    }

    fn get_oplog_reader(&self, py: Python<'_>) -> PyResult<Py<RedbOplogReaderBridge>> {
        Py::new(
            py,
            RedbOplogReaderBridge {
                db: self.db.clone(),
                db_name: self.db_name.clone(),
                coll_name: self.name.clone(),
            },
        )
    }

    fn get_by_id(&self, py: Python<'_>, doc_id: Bound<'_, PyAny>) -> PyResult<Option<Py<PyDict>>> {
        let id = crate::bson_helpers::py_to_bson(&doc_id)?;
        let mut filter = Document::new();
        filter.insert("_id", id);
        let collection = self.engine_col()?;
        let doc = collection
            .find_one(filter)
            .map_err(|e| PyRuntimeError::new_err(format!("find_one: {}", e)))?;
        match doc {
            Some(d) => Ok(Some(crate::bson_helpers::doc_to_pydict(py, &d)?.unbind())),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (document, *, internal=false))]
    fn insert_one(
        &self,
        py: Python<'_>,
        document: &Bound<'_, PyDict>,
        internal: bool,
    ) -> PyResult<Py<PyDict>> {
        let doc = crate::bson_helpers::pydict_to_doc(document)?;
        let collection = self.engine_col()?;
        let result = collection
            .insert_one_with_options(doc, InsertOptions { internal })
            .map_err(|e| PyRuntimeError::new_err(format!("insert_one: {}", e)))?;
        let result_dict = PyDict::new(py);
        result_dict.set_item(
            "inserted_id",
            crate::bson_helpers::bson_to_py(py, &result.inserted_id)?,
        )?;
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (documents, *, internal=false))]
    fn insert_many(
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
                    .and_then(|d| crate::bson_helpers::pydict_to_doc(&d))
            })
            .collect::<PyResult<_>>()?;
        let collection = self.engine_col()?;
        let result = collection
            .insert_many_with_options(docs, InsertOptions { internal })
            .map_err(|e| PyRuntimeError::new_err(format!("insert_many: {}", e)))?;
        let result_dict = PyDict::new(py);
        let ids = PyList::empty(py);
        for id in result.inserted_ids {
            ids.append(crate::bson_helpers::bson_to_py(py, &id)?)?;
        }
        result_dict.set_item("inserted_ids", ids)?;
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (filter, projection=None))]
    fn find_one(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        projection: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let collection = self.engine_col()?;
        let doc = match projection {
            Some(p) => {
                let proj = crate::bson_helpers::pydict_to_doc(p)?;
                if proj.is_empty() {
                    collection
                        .find_one(query)
                        .map_err(|e| PyRuntimeError::new_err(format!("find_one: {}", e)))?
                } else {
                    collection
                        .find_one_with_options(
                            query,
                            FindOptions {
                                projection: Some(proj),
                                ..Default::default()
                            },
                        )
                        .map_err(|e| PyRuntimeError::new_err(format!("find_one: {}", e)))?
                }
            }
            None => collection
                .find_one(query)
                .map_err(|e| PyRuntimeError::new_err(format!("find_one: {}", e)))?,
        };
        match doc {
            Some(d) => Ok(Some(crate::bson_helpers::doc_to_pydict(py, &d)?.unbind())),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (filter, projection=None))]
    fn find(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        projection: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyList>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let collection = self.engine_col()?;
        let docs = match projection {
            Some(p) => {
                let proj = crate::bson_helpers::pydict_to_doc(p)?;
                if proj.is_empty() {
                    collection
                        .find(query)
                        .map_err(|e| PyRuntimeError::new_err(format!("find: {}", e)))?
                } else {
                    collection
                        .find_with_options(
                            query,
                            FindOptions {
                                projection: Some(proj),
                                ..Default::default()
                            },
                        )
                        .map_err(|e| PyRuntimeError::new_err(format!("find: {}", e)))?
                }
            }
            None => collection
                .find(query)
                .map_err(|e| PyRuntimeError::new_err(format!("find: {}", e)))?,
        };
        let results = PyList::empty(py);
        for doc in docs {
            results.append(crate::bson_helpers::doc_to_pydict(py, &doc)?)?;
        }
        Ok(results.unbind())
    }

    fn count_documents(&self, filter: &Bound<'_, PyDict>) -> PyResult<u64> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let collection = self.engine_col()?;
        collection
            .count_documents(Some(query))
            .map_err(|e| PyRuntimeError::new_err(format!("count_documents: {}", e)))
    }

    fn explain(&self, py: Python<'_>, filter: &Bound<'_, PyDict>) -> PyResult<Py<PyDict>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let collection = self.engine_col()?;
        let er = collection
            .explain_find(query)
            .map_err(|e| PyRuntimeError::new_err(format!("explain: {}", e)))?;
        let doc = bson::to_document(&er)
            .map_err(|e| PyRuntimeError::new_err(format!("explain serialize: {}", e)))?;
        Ok(crate::bson_helpers::doc_to_pydict(py, &doc)?.unbind())
    }

    #[pyo3(signature = (filter, update, *, internal=false, upsert=false))]
    fn update_one(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        update: &Bound<'_, PyDict>,
        internal: bool,
        upsert: bool,
    ) -> PyResult<Py<PyDict>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let update_doc = crate::bson_helpers::pydict_to_doc(update)?;
        let collection = self.engine_col()?;
        let result = collection
            .update_one_with_options(
                query,
                update_doc,
                UpdateOptions {
                    upsert,
                    internal,
                },
            )
            .map_err(|e| PyRuntimeError::new_err(format!("update_one: {}", e)))?;
        let result_dict = PyDict::new(py);
        result_dict.set_item("matched_count", result.matched_count)?;
        result_dict.set_item("modified_count", result.modified_count)?;
        if let Some(ref uid) = result.upserted_id {
            result_dict.set_item(
                "upserted_id",
                crate::bson_helpers::bson_to_py(py, uid)?,
            )?;
        }
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (filter, update, *, internal=false, upsert=false))]
    fn update_many(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        update: &Bound<'_, PyDict>,
        internal: bool,
        upsert: bool,
    ) -> PyResult<Py<PyDict>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let update_doc = crate::bson_helpers::pydict_to_doc(update)?;
        let collection = self.engine_col()?;
        let result = collection
            .update_many_with_options(
                query,
                update_doc,
                UpdateOptions {
                    upsert,
                    internal,
                },
            )
            .map_err(|e| PyRuntimeError::new_err(format!("update_many: {}", e)))?;
        let result_dict = PyDict::new(py);
        result_dict.set_item("matched_count", result.matched_count)?;
        result_dict.set_item("modified_count", result.modified_count)?;
        if let Some(ref uid) = result.upserted_id {
            result_dict.set_item(
                "upserted_id",
                crate::bson_helpers::bson_to_py(py, uid)?,
            )?;
        }
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (filter, *, internal=false))]
    fn delete_one(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        internal: bool,
    ) -> PyResult<Py<PyDict>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let collection = self.engine_col()?;
        let result = collection
            .delete_one_with_options(query, DeleteOptions { internal })
            .map_err(|e| PyRuntimeError::new_err(format!("delete_one: {}", e)))?;
        let result_dict = PyDict::new(py);
        result_dict.set_item("deleted_count", result.deleted_count)?;
        Ok(result_dict.unbind())
    }

    #[pyo3(signature = (filter, *, internal=false))]
    fn delete_many(
        &self,
        py: Python<'_>,
        filter: &Bound<'_, PyDict>,
        internal: bool,
    ) -> PyResult<Py<PyDict>> {
        let query = crate::bson_helpers::pydict_to_doc(filter)?;
        let collection = self.engine_col()?;
        let result = collection
            .delete_many_with_options(query, DeleteOptions { internal })
            .map_err(|e| PyRuntimeError::new_err(format!("delete_many: {}", e)))?;
        let result_dict = PyDict::new(py);
        result_dict.set_item("deleted_count", result.deleted_count)?;
        Ok(result_dict.unbind())
    }

    fn create_index(
        &self,
        _py: Python<'_>,
        keys: &Bound<'_, PyDict>,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let keys_doc = crate::bson_helpers::pydict_to_doc(keys)?;
        let opts = if let Some(opts_dict) = options {
            let opts_doc = crate::bson_helpers::pydict_to_doc(opts_dict)?;
            Some(
                bson::from_document::<smongo_engine::index::IndexOptions>(opts_doc)
                    .map_err(|e| PyRuntimeError::new_err(format!("Invalid index options: {}", e)))?,
            )
        } else {
            None
        };
        let collection = self.engine_col()?;
        collection
            .create_index(keys_doc, opts)
            .map_err(|e| PyRuntimeError::new_err(format!("create_index: {}", e)))
    }

    fn drop_index(&self, name: &str) -> PyResult<()> {
        let collection = self.engine_col()?;
        collection
            .drop_index(name)
            .map_err(|e| PyRuntimeError::new_err(format!("drop_index: {}", e)))
    }

    fn list_indexes(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let collection = self.engine_col()?;
        let indexes = collection
            .list_indexes()
            .map_err(|e| PyRuntimeError::new_err(format!("list_indexes: {}", e)))?;
        let result = PyList::empty(py);
        for idx in indexes {
            let doc = bson::to_document(&idx)
                .map_err(|e| PyRuntimeError::new_err(format!("serialize index: {}", e)))?;
            result.append(crate::bson_helpers::doc_to_pydict(py, &doc)?)?;
        }
        Ok(result.unbind())
    }

    /// Subscribe to local oplog via engine ChangeStream (optional pipeline unused).
    fn watch(&self, py: Python<'_>, _pipeline: Option<Bound<'_, PyAny>>) -> PyResult<Py<RedbChangeStream>> {
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
    fn name(&self) -> &str {
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
    fn try_next(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
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

    fn close(&mut self) {
        self.hub.unregister(self.stream.as_ref());
        self.stream.close();
    }
}
