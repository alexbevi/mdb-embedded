//! Oplog reader, writer, and pub/sub hub for change notifications.
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Condvar, Mutex};

use std::ops::Deref;

use pyo3::exceptions::{PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use pyo3::wrap_pyfunction;

use crate::sync_utils;
use crate::wt_bridge::RustWtSession;

// ── OplogHub ─────────────────────────────────────────────────────────

/// Instance-scoped listener registry for oplog change notifications.
#[pyclass(module = "smongo._smongo_core")]
pub struct OplogHub {
    listeners: Mutex<Vec<Py<ChangeStream>>>,
}

#[pymethods]
impl OplogHub {
    #[new]
    fn new() -> Self {
        Self {
            listeners: Mutex::new(Vec::new()),
        }
    }

    fn register(&self, listener: Py<ChangeStream>) {
        self.listeners.lock().push(listener);
    }

    fn unregister(&self, py: Python<'_>, listener: &Bound<'_, PyAny>) {
        let mut listeners = self.listeners.lock();
        listeners.retain(|l: &Py<ChangeStream>| !l.bind(py).as_any().is(listener));
    }

    #[pyo3(signature = (entry, source_ns=None))]
    fn notify(&self, py: Python<'_>, entry: &Bound<'_, PyDict>, source_ns: Option<&str>) {
        let mut listeners = self.listeners.lock();
        let mut dead_indices = Vec::new();

        for (i, listener) in listeners.iter().enumerate() {
            let cs = listener.bind(py).borrow();

            if let Some(src_ns) = source_ns {
                if let Some(ref ns) = cs.namespace {
                    if !ns.is_empty() && ns != src_ns {
                        continue;
                    }
                }
            }

            if cs._enqueue(py, entry).is_err() {
                dead_indices.push(i);
            }
        }

        for &i in dead_indices.iter().rev() {
            listeners.remove(i);
        }
    }

    #[getter]
    fn _listeners<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let listeners = self.listeners.lock();
        let list = PyList::empty(py);
        for l in listeners.iter() {
            list.append(l.bind(py))?;
        }
        Ok(list)
    }
}

impl OplogHub {
    pub(crate) fn register_typed(&self, listener: Py<ChangeStream>) {
        self.listeners.lock().push(listener);
    }

    pub(crate) fn unregister_typed(&self, py: Python<'_>, target: &ChangeStream) {
        let mut listeners = self.listeners.lock();
        listeners.retain(|l| !std::ptr::eq(l.bind(py).borrow().deref(), target));
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn get_wt<'py>(session: &'py Bound<'py, PyAny>) -> PyResult<pyo3::PyRef<'py, RustWtSession>> {
    session.cast::<RustWtSession>().map(|s| s.borrow()).map_err(|_| {
        PyRuntimeError::new_err("expected RustWtSession")
    })
}

// ── OplogWriter ──────────────────────────────────────────────────────

/// Appends structured operations to a WiredTiger oplog table.
#[pyclass(module = "smongo._smongo_core")]
pub struct OplogWriter {
    #[pyo3(get)]
    session: Py<PyAny>,
    #[pyo3(get)]
    oplog_uri: String,
    #[pyo3(get)]
    namespace: String,
    hub: Option<Py<OplogHub>>,
}

#[pymethods]
impl OplogWriter {
    #[new]
    #[pyo3(signature = (session, oplog_uri, namespace, hub=None))]
    pub fn new(
        session: Py<PyAny>,
        oplog_uri: String,
        namespace: String,
        hub: Option<Py<OplogHub>>,
    ) -> Self {
        Self {
            session,
            oplog_uri,
            namespace,
            hub,
        }
    }

    #[getter]
    fn _hub(&self, py: Python<'_>) -> Option<Py<OplogHub>> {
        self.hub.as_ref().map(|h| h.clone_ref(py))
    }

    #[pyo3(signature = (op, doc_id, payload, *, version=None, internal=false, changed_fields=None))]
    #[allow(clippy::too_many_arguments)]
    fn log(
        &self,
        py: Python<'_>,
        op: &str,
        doc_id: &Bound<'_, PyAny>,
        payload: &Bound<'_, PyAny>,
        version: Option<&Bound<'_, PyAny>>,
        internal: bool,
        changed_fields: Option<&Bound<'_, PyList>>,
    ) -> PyResult<String> {
        let time_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let uuid_val = uuid::Uuid::new_v4();
        let oplog_key = format!("{:020}-{}", time_ns, uuid_val);

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        let checksum: Option<String> = if op != "delete" && !payload.is_none() {
            sync_utils::doc_checksum(py, payload)?
        } else {
            None
        };

        let log_entry = PyDict::new(py);
        log_entry.set_item("ts", ts)?;
        log_entry.set_item("ns", &self.namespace)?;
        log_entry.set_item("op", op)?;
        log_entry.set_item("doc_id", doc_id)?;
        log_entry.set_item("payload", payload)?;
        log_entry.set_item(
            "v",
            version
                .map(|v| v.clone().unbind())
                .unwrap_or_else(|| py.None()),
        )?;
        log_entry.set_item("checksum", checksum)?;
        log_entry.set_item("internal", internal)?;
        if let Some(cf) = changed_fields {
            log_entry.set_item("changed_fields", cf)?;
        }

        let json_mod = crate::cached_modules::json_mod(py)?;
        let ejson_default = wrap_pyfunction!(crate::sync_utils::ejson_default, py)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("default", ejson_default)?;
        let json_str = json_mod.call_method("dumps", (&log_entry,), Some(&kwargs))?;
        let json_str_val: String = json_str.extract()?;

        let session_bound = self.session.bind(py);
        let rs = get_wt(session_bound)?;
        let mut cursor = rs.open_cursor_typed(&self.oplog_uri, Some("overwrite=true"))?;
        cursor.set_key_str(&oplog_key)?;
        cursor.set_value_string(&json_str_val)?;
        cursor.insert_typed()?;
        cursor.close_typed()?;
        drop(rs);

        self.notify_listeners(py, &log_entry)?;
        Ok(oplog_key)
    }

    fn truncate_before(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        let session_bound = self.session.bind(py);
        let rs = get_wt(session_bound)?;
        let mut cursor = rs.open_cursor_typed(&self.oplog_uri, Some("overwrite=true"))?;
        let mut to_remove = Vec::new();
        loop {
            if cursor.next_rc()? != 0 { break; }
            let k = cursor.get_key_str()?;
            if k.as_str() >= key { break; }
            to_remove.push(k);
        }
        cursor.close_typed()?;

        if !to_remove.is_empty() {
            let mut cursor = rs.open_cursor_typed(&self.oplog_uri, Some("overwrite=true"))?;
            for k in &to_remove {
                cursor.set_key_str(k)?;
                let _ = cursor.remove_typed();
            }
            cursor.close_typed()?;
        }
        Ok(to_remove.len() as i64)
    }

    fn truncate_count(&self, py: Python<'_>, max_entries: i64) -> PyResult<i64> {
        let session_bound = self.session.bind(py);
        let rs = get_wt(session_bound)?;
        let mut cursor = rs.open_cursor_typed(&self.oplog_uri, None)?;
        let mut keys = Vec::new();
        loop {
            if cursor.next_rc()? != 0 { break; }
            keys.push(cursor.get_key_str()?);
        }
        cursor.close_typed()?;

        let excess = keys.len() as i64 - max_entries;
        if excess <= 0 {
            return Ok(0);
        }
        let mut cursor = rs.open_cursor_typed(&self.oplog_uri, Some("overwrite=true"))?;
        for k in &keys[..excess as usize] {
            cursor.set_key_str(k)?;
            let _ = cursor.remove_typed();
        }
        cursor.close_typed()?;
        Ok(excess)
    }
}

impl OplogWriter {
    fn notify_listeners(&self, py: Python<'_>, entry: &Bound<'_, PyDict>) -> PyResult<()> {
        if let Some(ref hub) = self.hub {
            let ns = entry
                .get_item("ns")?
                .and_then(|v| v.extract::<String>().ok());
            hub.borrow(py).notify(py, entry, ns.as_deref());
        }
        Ok(())
    }
}

// ── OplogReader ──────────────────────────────────────────────────────

/// Reads oplog entries, optionally from a checkpoint forward.
#[pyclass(module = "smongo._smongo_core")]
pub struct OplogReader {
    #[pyo3(get)]
    session: Py<PyAny>,
    #[pyo3(get)]
    oplog_uri: String,
}

#[pymethods]
impl OplogReader {
    #[new]
    pub fn new(session: Py<PyAny>, oplog_uri: String) -> Self {
        Self { session, oplog_uri }
    }

    fn read_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let json_mod = crate::cached_modules::json_mod(py)?;
        let object_hook = wrap_pyfunction!(crate::sync_utils::ejson_object_hook, py)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("object_hook", object_hook)?;

        let session_bound = self.session.bind(py);
        let rs = get_wt(session_bound)?;
        let mut cursor = rs.open_cursor_typed(&self.oplog_uri, None)?;
        let results = PyList::empty(py);
        loop {
            if cursor.next_rc()? != 0 { break; }
            let value = cursor.get_value_string()?;
            let entry = json_mod.call_method("loads", (&value,), Some(&kwargs))?;
            results.append(entry)?;
        }
        cursor.close_typed()?;
        Ok(results)
    }

    #[pyo3(signature = (checkpoint_key=None, *, skip_internal=true))]
    fn read_from<'py>(
        &self,
        py: Python<'py>,
        checkpoint_key: Option<&str>,
        skip_internal: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        let json_mod = crate::cached_modules::json_mod(py)?;
        let object_hook = wrap_pyfunction!(crate::sync_utils::ejson_object_hook, py)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("object_hook", object_hook)?;

        let session_bound = self.session.bind(py);
        let rs = get_wt(session_bound)?;
        let mut cursor = rs.open_cursor_typed(&self.oplog_uri, None)?;
        let results = PyList::empty(py);
        let mut past_checkpoint = checkpoint_key.is_none();

        loop {
            if cursor.next_rc()? != 0 { break; }
            let key = cursor.get_key_str()?;
            if !past_checkpoint {
                if checkpoint_key == Some(key.as_str()) {
                    past_checkpoint = true;
                }
                continue;
            }
            let value = cursor.get_value_string()?;
            let entry = json_mod.call_method("loads", (&value,), Some(&kwargs))?;
            if skip_internal {
                if let Ok(internal_val) = entry.get_item("internal") {
                    if internal_val.is_truthy()? {
                        continue;
                    }
                }
            }
            let tuple = PyTuple::new(py, &[
                PyString::new(py, &key).into_any(),
                entry.clone(),
            ])?;
            results.append(tuple)?;
        }
        cursor.close_typed()?;
        Ok(results)
    }

    fn latest_key(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let session_bound = self.session.bind(py);
        let rs = get_wt(session_bound)?;
        let mut cursor = rs.open_cursor_typed(&self.oplog_uri, None)?;
        let mut last_key: Option<String> = None;
        loop {
            if cursor.next_rc()? != 0 { break; }
            last_key = Some(cursor.get_key_str()?);
        }
        cursor.close_typed()?;
        Ok(last_key)
    }

    fn count(&self, py: Python<'_>) -> PyResult<i64> {
        let session_bound = self.session.bind(py);
        let rs = get_wt(session_bound)?;
        let mut cursor = rs.open_cursor_typed(&self.oplog_uri, None)?;
        let mut n: i64 = 0;
        loop {
            if cursor.next_rc()? != 0 { break; }
            n += 1;
        }
        cursor.close_typed()?;
        Ok(n)
    }

    fn oldest_key(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let session_bound = self.session.bind(py);
        let rs = get_wt(session_bound)?;
        let mut cursor = rs.open_cursor_typed(&self.oplog_uri, None)?;
        let result = if cursor.next_rc()? == 0 {
            Some(cursor.get_key_str()?)
        } else {
            None
        };
        cursor.close_typed()?;
        Ok(result)
    }
}

// ── ChangeStream ─────────────────────────────────────────────────────

struct ChangeStreamInner {
    queue: VecDeque<Py<PyAny>>,
    closed: bool,
}

/// Local change stream -- tails the oplog and yields MongoDB-format change events.
#[pyclass(module = "smongo._smongo_core")]
pub struct ChangeStream {
    #[pyo3(get)]
    namespace: Option<String>,
    filter: Option<Py<PyAny>>,
    state: Mutex<ChangeStreamInner>,
    cond: Condvar,
    hub: Option<Py<OplogHub>>,
}

fn op_to_change_type(op: &str) -> Option<&'static str> {
    match op {
        "insert" => Some("insert"),
        "update" => Some("update"),
        "delete" => Some("delete"),
        "replace" => Some("replace"),
        _ => None,
    }
}

#[pymethods]
impl ChangeStream {
    #[new]
    #[pyo3(signature = (namespace=None, pipeline=None, hub=None))]
    pub fn new(
        py: Python<'_>,
        namespace: Option<String>,
        pipeline: Option<&Bound<'_, PyList>>,
        hub: Option<Py<OplogHub>>,
    ) -> PyResult<Self> {
        let mut filter: Option<Py<PyAny>> = None;

        if let Some(pl) = pipeline {
            for stage in pl.iter() {
                if let Ok(spec) = stage.get_item("$match") {
                    let spec_dict: &Bound<'_, PyDict> = spec.cast()?;
                    let compiled = crate::query_compiler::compile_query(spec_dict)?;
                    filter = Some(Py::new(py, compiled)?.into_any());
                    break;
                }
            }
        }

        let hub_clone = hub.as_ref().map(|h| h.clone_ref(py));

        let cs = Self {
            namespace,
            filter,
            state: Mutex::new(ChangeStreamInner {
                queue: VecDeque::new(),
                closed: false,
            }),
            cond: Condvar::new(),
            hub: hub_clone,
        };

        Ok(cs)
    }

    fn _enqueue(&self, py: Python<'_>, oplog_entry: &Bound<'_, PyDict>) -> PyResult<()> {
        let op: String = oplog_entry
            .get_item("op")?
            .map(|v| v.extract::<String>().unwrap_or_default())
            .unwrap_or_default();

        let change_type = match op_to_change_type(&op) {
            Some(ct) => ct,
            None => return Ok(()),
        };

        let ns_str: String = oplog_entry
            .get_item("ns")?
            .map(|v| v.extract::<String>().unwrap_or_else(|_| ".".into()))
            .unwrap_or_else(|| ".".into());
        let ns_parts: Vec<&str> = ns_str.splitn(2, '.').collect();
        let db = ns_parts[0];
        let coll = if ns_parts.len() > 1 { ns_parts[1] } else { "" };

        let ns_dict = PyDict::new(py);
        ns_dict.set_item("db", db)?;
        ns_dict.set_item("coll", coll)?;

        let doc_key = PyDict::new(py);
        let doc_id = oplog_entry
            .get_item("doc_id")?
            .map(|v| v.unbind())
            .unwrap_or(py.None());
        doc_key.set_item("_id", &doc_id)?;

        let event = PyDict::new(py);
        event.set_item("operationType", change_type)?;
        event.set_item("ns", ns_dict)?;
        event.set_item("documentKey", doc_key)?;
        let ts = oplog_entry
            .get_item("ts")?
            .map(|v| v.unbind())
            .unwrap_or(py.None());
        event.set_item("_ts", &ts)?;

        if change_type == "insert" || change_type == "update" || change_type == "replace" {
            let payload = oplog_entry
                .get_item("payload")?
                .map(|v| v.unbind())
                .unwrap_or(py.None());
            event.set_item("fullDocument", &payload)?;
        }

        if let Some(ref filt) = self.filter {
            let result = filt.call1(py, (&event,))?;
            if !result.is_truthy(py)? {
                return Ok(());
            }
        }

        let mut inner = self.state.lock();
        inner.queue.push_back(event.into_any().unbind());
        self.cond.notify_all();
        Ok(())
    }

    #[getter]
    fn _closed(&self) -> bool {
        self.state.lock().closed
    }

    #[getter]
    fn _queue<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let inner = self.state.lock();
        let list = PyList::empty(py);
        for item in &inner.queue {
            list.append(item.bind(py))?;
        }
        Ok(list)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_val: &Bound<'_, PyAny>,
        _exc_tb: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.close(py)
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        loop {
            {
                let inner = self.state.lock();
                if inner.closed {
                    return Err(PyStopIteration::new_err(()));
                }
            }

            {
                let mut inner = self.state.lock();
                if let Some(item) = inner.queue.pop_front() {
                    return Ok(item);
                }
            }

            py.detach(|| {
                let mut inner = self.state.lock();
                self.cond
                    .wait_for(&mut inner, std::time::Duration::from_secs(1));
            });

            {
                let mut inner = self.state.lock();
                if let Some(item) = inner.queue.pop_front() {
                    return Ok(item);
                }
            }
        }
    }

    fn try_next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut inner = self.state.lock();
        match inner.queue.pop_front() {
            Some(item) => Ok(item),
            None => Ok(py.None()),
        }
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        {
            let mut inner = self.state.lock();
            inner.closed = true;
        }
        self.cond.notify_all();

        if let Some(ref h) = self.hub {
            h.borrow(py).unregister_typed(py, self);
        }
        Ok(())
    }
}
