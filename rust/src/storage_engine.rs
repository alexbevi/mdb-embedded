//! Rust port of `smongo.storage.engine.LocalClient` and `LocalDB`.
//!
//! `RustLocalClient` owns the WiredTiger connection via dlopen/`wt_safe`.
//! `RustLocalDB` manages collection namespaces with a Mutex-guarded cache.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use wiredtiger_sys::{WT_NOTFOUND, WtLibrary};

use crate::local_collection::RustLocalCollection;
use crate::wt_bridge::RustWtSession;
use crate::wt_safe::{WtConnection, WtResult, open_session_from_conn_ptr};

// ---------------------------------------------------------------------------
// RustLocalClient
// ---------------------------------------------------------------------------

/// Shared state for `RustLocalClient` accessible from `RustLocalDB`s.
struct ClientInner {
    conn: WtConnection,
    dbs: HashMap<String, Py<RustLocalDB>>,
    oplog_hub: Py<PyAny>,
}

/// WiredTiger connection manager -- opens, configures, and closes the WT engine.
#[pyclass]
pub struct RustLocalClient {
    inner: Option<Mutex<ClientInner>>,
    durable: bool,
}

// SAFETY: RustLocalClient wraps a Mutex<ClientInner> which handles internal
// synchronization.  All WT access goes through the mutex-guarded WtConnection.
// PyO3 requires Send+Sync for #[pyclass].
unsafe impl Send for RustLocalClient {}
unsafe impl Sync for RustLocalClient {}

impl RustLocalClient {
    /// Direct Rust accessor for getting a DB handle, bypassing Python dispatch.
    pub(crate) fn get_db_inner(
        &self,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<Py<RustLocalDB>> {
        let inner_mtx = self.inner.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client is closed"))?;
        let mut inner = inner_mtx.lock();

        if let Some(db) = inner.dbs.get(name) {
            return Ok(db.clone_ref(py));
        }

        let conn_ptr = inner.conn.raw_ptr();
        let oplog_hub = inner.oplog_hub.clone_ref(py);

        let db = Py::new(py, RustLocalDB {
            conn_ptr,
            db_name: name.to_string(),
            collections: Mutex::new(HashMap::new()),
            validators: Mutex::new(HashMap::new()),
            oplog_hub,
        })?;
        inner.dbs.insert(name.to_string(), db.clone_ref(py));
        Ok(db)
    }

    /// Open a WT session without Python dispatch, for use by Rust admin commands.
    pub(crate) fn open_session_typed(&self) -> PyResult<crate::wt_bridge::RustWtSession> {
        let inner_mtx = self.inner.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client is closed"))?;
        let inner = inner_mtx.lock();
        let session = inner.conn.open_session(None)?;
        Ok(crate::wt_bridge::RustWtSession::from_safe(session))
    }
}

#[pymethods]
impl RustLocalClient {
    #[new]
    #[pyo3(signature = (db_path, durable=true))]
    fn new(py: Python<'_>, db_path: &str, durable: bool) -> PyResult<Self> {
        std::fs::create_dir_all(db_path)
            .map_err(|e| PyRuntimeError::new_err(format!("mkdir {db_path}: {e}")))?;

        let mut config = "create,statistics=(fast)".to_string();
        if durable {
            let has_snappy = py
                .import("importlib")
                .and_then(|il| il.call_method1("import_module", ("snappy",)))
                .is_ok();
            if has_snappy {
                config.push_str(",log=(enabled=true,compressor=snappy)");
            } else {
                config.push_str(",log=(enabled=true)");
            }
        }

        let lib = Arc::new(
            WtLibrary::load_from_pip()
                .map_err(PyRuntimeError::new_err)?,
        );
        let conn = WtConnection::open(lib, db_path, Some(&config))?;

        let oplog_hub_cls = py
            .import("smongo._smongo_core")
            .and_then(|m| m.getattr("OplogHub"))?;
        let oplog_hub = oplog_hub_cls.call0()?.unbind();

        // Ensure table:__users exists and load persisted user credentials
        let user_store = py
            .import("smongo.wire.commands.users")?
            .getattr("_USER_STORE")?;
        let _ = load_persisted_users(py, &conn, &user_store);

        Ok(Self {
            inner: Some(Mutex::new(ClientInner {
                conn,
                dbs: HashMap::new(),
                oplog_hub,
            })),
            durable,
        })
    }

    fn get_db(slf: &Bound<'_, Self>, py: Python<'_>, name: &str) -> PyResult<Py<RustLocalDB>> {
        slf.borrow().get_db_inner(py, name)
    }

    fn checkpoint(&self) -> PyResult<()> {
        let inner_mtx = self.inner.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client is closed"))?;
        let inner = inner_mtx.lock();
        let session = inner.conn.open_session(None)?;
        session.checkpoint(None)?;
        Ok(())
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(inner_mtx) = self.inner.take() {
            let mut inner = inner_mtx.into_inner();
            // Close all DBs
            for (_, db) in inner.dbs.drain() {
                let mut db_ref = db.bind(py).borrow_mut();
                db_ref.close_collections(py)?;
            }
            inner.conn.close()?;
        }
        Ok(())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(&mut self, py: Python<'_>, _exc_type: &Bound<'_, PyAny>, _exc_val: &Bound<'_, PyAny>, _exc_tb: &Bound<'_, PyAny>) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }

    #[getter]
    fn durable(&self) -> bool {
        self.durable
    }

    #[getter]
    fn conn(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[getter]
    fn oplog_hub(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner_mtx = self.inner.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client is closed"))?;
        let inner = inner_mtx.lock();
        Ok(inner.oplog_hub.clone_ref(py))
    }

    /// Expose the raw connection as a RustWtSession factory for Python code
    /// that still needs direct session access.
    fn open_session(&self) -> PyResult<RustWtSession> {
        let inner_mtx = self.inner.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client is closed"))?;
        let inner = inner_mtx.lock();
        let session = inner.conn.open_session(None)?;
        Ok(RustWtSession::from_safe(session))
    }

    /// Return the loaded WiredTiger library version as a string (e.g. "11.3.1").
    #[getter]
    fn wiredtiger_version(&self) -> PyResult<String> {
        let inner_mtx = self.inner.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client is closed"))?;
        let inner = inner_mtx.lock();
        Ok(inner.conn.wt_version().to_string())
    }

    /// Read connection-level WiredTiger statistics using the correct "SSq"
    /// cursor format.  Returns a Python dict of {normalized_desc: int_value}.
    pub fn connection_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let result = PyDict::new(py);
        let inner_mtx = match self.inner.as_ref() {
            Some(m) => m,
            None => return Ok(result),
        };
        let inner = inner_mtx.lock();
        let mut session = match inner.conn.open_session(None) {
            Ok(s) => s,
            Err(_) => return Ok(result),
        };
        let cursor = match session.open_cursor("statistics:", Some("statistics=(fast)")) {
            Ok(c) => c,
            Err(_) => { let _ = session.close(); return Ok(result); }
        };
        let raw = cursor.raw_ptr();
        loop {
            let next_fn = match unsafe { (*raw).next } {
                Some(f) => f,
                None => break,
            };
            let rc = unsafe { next_fn(raw) };
            if rc != 0 { break; }

            let mut desc_ptr: *const std::os::raw::c_char = std::ptr::null();
            let mut _name_ptr: *const std::os::raw::c_char = std::ptr::null();
            let mut val: i64 = 0;
            let get_value_fn = unsafe { (*raw).get_value };
            let rc = unsafe {
                wiredtiger_sys::wt_shim_get_value_ssq(
                    get_value_fn, raw, &mut desc_ptr, &mut _name_ptr, &mut val,
                )
            };
            if rc != 0 { break; }
            let desc = unsafe { std::ffi::CStr::from_ptr(desc_ptr) }
                .to_string_lossy();
            let key = desc.to_lowercase().replace([' ', '-'], "_");
            let _ = result.set_item(key.as_str(), val);
        }
        drop(cursor);
        let _ = session.close();
        Ok(result)
    }
}

/// Load persisted user documents from `table:__users` into the given
/// `user_store` dict so that auth works across restarts.
fn load_persisted_users(
    py: Python<'_>,
    conn: &WtConnection,
    store: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let session = conn.open_session(None)?;
    let _ = session.create("table:__users", "key_format=S,value_format=S");

    let cursor = match session.open_cursor("table:__users", None) {
        Ok(c) => c,
        Err(_) => return Ok(()),
    };

    let json_util = crate::cached_modules::bson_json_util(py)?;

    loop {
        let rc = cursor.next_raw();
        if rc == WT_NOTFOUND || rc != 0 {
            break;
        }
        let key = match cursor.get_key_str() {
            Ok(k) => k,
            Err(_) => continue,
        };
        let value = match cursor.get_value_str() {
            Ok(v) => v,
            Err(_) => continue,
        };
        match json_util.call_method1("loads", (&value,)) {
            Ok(doc) => {
                let _ = store.set_item(&key, doc);
            }
            Err(_) => continue,
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// RustLocalDB
// ---------------------------------------------------------------------------

/// Database handle -- resolves collection names and manages namespace-level metadata.
#[pyclass]
pub struct RustLocalDB {
    conn_ptr: *mut wiredtiger_sys::WT_CONNECTION,
    db_name: crate::DbName,
    collections: Mutex<HashMap<crate::CollectionName, Py<RustLocalCollection>>>,
    validators: Mutex<HashMap<crate::CollectionName, Option<Py<PyAny>>>>,
    oplog_hub: Py<PyAny>,
}

// SAFETY: RustLocalDB fields are protected by their own Mutex instances.
// The conn_ptr is only used to open new sessions (an inherently thread-safe WT
// operation).  PyO3 requires Send+Sync for #[pyclass].
unsafe impl Send for RustLocalDB {}
unsafe impl Sync for RustLocalDB {}

impl RustLocalDB {
    fn open_session_raw(&self) -> WtResult<crate::wt_safe::WtSession> {
        open_session_from_conn_ptr(self.conn_ptr)
    }

    fn close_collections(&mut self, py: Python<'_>) -> PyResult<()> {
        let mut colls = self.collections.lock();
        for (_, coll) in colls.drain() {
            let mut coll_ref = coll.bind(py).borrow_mut();
            let _ = coll_ref.close(py);
        }
        Ok(())
    }
}

const INTERNAL_TABLE_PREFIXES: &[&str] = &["__oplog_", "__idx_", "__idxmeta_", "__sync_"];

impl RustLocalDB {
    /// Direct Rust accessor -- returns typed `Py<RustLocalCollection>` without
    /// going through Python method dispatch.
    pub(crate) fn get_collection_typed(
        &self,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<Py<RustLocalCollection>> {
        // Fast path: collection already exists.
        {
            let colls = self.collections.lock();
            if let Some(coll) = colls.get(name) {
                return Ok(coll.clone_ref(py));
            }
        }
        // Mutex released here -- creating a collection does Python calls that
        // may trigger a GIL switch, so we must not hold the Rust mutex while
        // the GIL could transfer to another thread (classic lock-inversion
        // deadlock: Rust-Mutex + GIL vs GIL + Rust-Mutex).

        let validator = {
            let validators = self.validators.lock();
            validators
                .get(name)
                .and_then(|v| v.as_ref().map(|v| v.clone_ref(py)))
        };

        let coll = RustLocalCollection::new_rust(
            py,
            self.conn_ptr,
            &self.db_name,
            name,
            None,
            validator,
            Some(self.oplog_hub.clone_ref(py)),
        )?;
        let coll_py = Py::new(py, coll)?;

        {
            let borrow = coll_py.bind(py).borrow();
            borrow._ttl_reaper.bind(py)
                .setattr("_collection", coll_py.bind(py))?;
        }

        // Re-acquire and insert (another thread may have raced us).
        let mut colls = self.collections.lock();
        if let Some(existing) = colls.get(name) {
            return Ok(existing.clone_ref(py));
        }
        colls.insert(name.to_string(), coll_py.clone_ref(py));
        Ok(coll_py)
    }
}

#[pymethods]
impl RustLocalDB {
    fn get_collection(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        Ok(self.get_collection_typed(py, name)?.into_any())
    }

    #[pyo3(signature = (name, validator=None, **kwargs))]
    fn create_collection(
        &self,
        py: Python<'_>,
        name: &str,
        validator: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = kwargs;
        let schema: Option<Py<PyAny>> = if let Some(v) = validator {
            if let Ok(json_schema) = v.get_item("$jsonSchema") {
                Some(json_schema.unbind())
            } else {
                Some(v.clone().unbind())
            }
        } else {
            None
        };

        {
            let mut validators = self.validators.lock();
            validators.insert(name.to_string(), schema.as_ref().map(|s| s.clone_ref(py)));
        }

        let coll = self.get_collection_typed(py, name)?;
        if let Some(schema) = schema {
            coll.bind(py).borrow_mut()._validator = Some(schema);
        }
        Ok(coll.into_any())
    }

    pub(crate) fn drop_collection(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let mut colls = self.collections.lock();
        if let Some(coll) = colls.remove(name) {
            let coll_ref = coll.bind(py).borrow();

            // Stop TTL reaper
            let _ = coll_ref._ttl_reaper.bind(py).call_method0("stop");

            // Gather URIs to drop -- direct Rust field access
            let idx_mgr = coll_ref.index_mgr.bind(py).borrow();
            let mut uris_to_drop = idx_mgr.all_index_uris();
            uris_to_drop.push(idx_mgr.meta_uri().to_string());
            drop(idx_mgr);

            uris_to_drop.push(coll_ref.oplog_uri.clone());
            uris_to_drop.push(coll_ref.table_uri.clone());

            // Close the collection session
            let _ = coll_ref.session_py.bind(py).call_method0("close");

            // Must drop borrow before dropping tables (which needs no borrow)
            drop(coll_ref);

            // Drop all tables
            let drop_session = self.open_session_raw()?;
            let _ = drop_session.checkpoint(None);
            for uri in &uris_to_drop {
                let _ = drop_session.drop_table(uri, Some("force"));
            }
        }

        let mut validators = self.validators.lock();
        validators.remove(name);
        Ok(())
    }

    pub(crate) fn list_collection_names(&self) -> PyResult<Vec<String>> {
        let mut names: std::collections::HashSet<String> = std::collections::HashSet::new();

        if let Ok(session) = self.open_session_raw() {
            let prefix = format!("table:{}_{}", self.db_name, ""); // "table:dbname_"
            if let Ok(cursor) = session.open_cursor("metadata:", None) {
                loop {
                    let rc = cursor.next_raw();
                    if rc == WT_NOTFOUND {
                        break;
                    }
                    if rc != 0 {
                        break;
                    }
                    if let Ok(uri) = cursor.get_key_str() {
                        if !uri.starts_with(&prefix) {
                            continue;
                        }
                        let is_internal = INTERNAL_TABLE_PREFIXES.iter().any(|tag| {
                            uri.starts_with(&format!("table:{}{}_", tag, self.db_name))
                        });
                        if is_internal {
                            continue;
                        }
                        let coll_name = &uri[prefix.len()..];
                        if !coll_name.is_empty() {
                            names.insert(coll_name.to_string());
                        }
                    }
                }
            }
        }

        // Also include collections from in-memory registry
        let colls = self.collections.lock();
        for k in colls.keys() {
            names.insert(k.clone());
        }

        let mut sorted: Vec<String> = names.into_iter().collect();
        sorted.sort();
        Ok(sorted)
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        self.close_collections(py)
    }

    #[getter]
    fn name(&self) -> &str {
        &self.db_name
    }

    /// Expose cached collections as a Python dict (mirrors Python LocalDB._collections).
    #[getter]
    fn _collections(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyDict>> {
        let colls = self.collections.lock();
        let d = pyo3::types::PyDict::new(py);
        for (k, v) in colls.iter() {
            d.set_item(k, v.bind(py).as_any())?;
        }
        Ok(d.unbind())
    }
}
