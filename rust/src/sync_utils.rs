//! Synchronization primitives -- document checksums, change detection, and resume tokens.
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFloat, PyList, PySet};
use sha2::{Digest, Sha256};

use crate::objectid::ObjectId;

// ── doc_checksum ─────────────────────────────────────────────────────

fn py_sort_keys_json(py: Python<'_>, doc: &Bound<'_, PyDict>) -> PyResult<String> {
    let json_mod = crate::cached_modules::json_mod(py)?;
    let builtins = crate::cached_modules::builtins(py)?;
    let str_fn = builtins.getattr("str")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("sort_keys", true)?;
    kwargs.set_item("default", str_fn)?;
    let result = json_mod.call_method("dumps", (doc,), Some(&kwargs))?;
    result.extract::<String>()
}

/// Stable SHA-256 truncated hash of a document for integrity verification.
/// Returns None if the input is None.
#[pyfunction]
pub fn doc_checksum(py: Python<'_>, doc: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    if doc.is_none() {
        return Ok(None);
    }
    let dict = doc.cast::<PyDict>().map_err(|_| {
        PyValueError::new_err("doc_checksum expects a dict or None")
    })?;
    let raw = py_sort_keys_json(py, dict)?;
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let hash = hasher.finalize();
    let hex_str = hex::encode(hash);
    Ok(Some(hex_str[..16].to_string()))
}

// ── VectorClock ──────────────────────────────────────────────────────

/// Per-document vector clock for causal ordering across replicas.
#[pyclass(module = "smongo._smongo_core")]
pub struct VectorClock {
    clock: HashMap<String, i64>,
}

#[pymethods]
impl VectorClock {
    #[new]
    #[pyo3(signature = (state=None))]
    fn new(state: Option<HashMap<String, i64>>) -> Self {
        Self {
            clock: state.unwrap_or_default(),
        }
    }

    fn tick(&mut self, node_id: &str) -> PyResult<()> {
        let entry = self.clock.entry(node_id.to_string()).or_insert(0);
        *entry += 1;
        Ok(())
    }

    fn merge(&mut self, other: &VectorClock) -> PyResult<()> {
        for (nid, &ts) in &other.clock {
            let entry = self.clock.entry(nid.clone()).or_insert(0);
            *entry = (*entry).max(ts);
        }
        Ok(())
    }

    fn dominates(&self, other: &VectorClock) -> bool {
        if other.clock.is_empty() {
            return true;
        }
        for (nid, &ts) in &other.clock {
            if *self.clock.get(nid).unwrap_or(&0) < ts {
                return false;
            }
        }
        let all_keys: std::collections::HashSet<&String> =
            self.clock.keys().chain(other.clock.keys()).collect();
        all_keys.iter().any(|nid| {
            *self.clock.get(*nid).unwrap_or(&0) > *other.clock.get(*nid).unwrap_or(&0)
        })
    }

    fn concurrent_with(&self, other: &VectorClock) -> bool {
        !self.dominates(other) && !other.dominates(self)
    }

    fn to_dict(&self) -> HashMap<String, i64> {
        self.clock.clone()
    }

    #[classmethod]
    #[pyo3(signature = (d=None))]
    fn from_dict(_cls: &Bound<'_, pyo3::types::PyType>, d: Option<HashMap<String, i64>>) -> Self {
        Self {
            clock: d.unwrap_or_default(),
        }
    }
}

// ── TombstoneRegistry ────────────────────────────────────────────────

const DEFAULT_TOMBSTONE_TTL_SEC: f64 = 7.0 * 24.0 * 3600.0;

/// Track deleted document IDs with timestamps for tombstone expiry.
#[pyclass(module = "smongo._smongo_core")]
pub struct TombstoneRegistry {
    tombstones: Mutex<HashMap<String, f64>>,
    ttl: f64,
}

#[pymethods]
impl TombstoneRegistry {
    #[new]
    #[pyo3(signature = (ttl_sec=None))]
    fn new(ttl_sec: Option<f64>) -> Self {
        Self {
            tombstones: Mutex::new(HashMap::new()),
            ttl: ttl_sec.unwrap_or(DEFAULT_TOMBSTONE_TTL_SEC),
        }
    }

    fn mark_deleted(&self, doc_id: &Bound<'_, PyAny>) -> PyResult<()> {
        let key = doc_id.str()?.to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        self.tombstones.lock().insert(key, now);
        Ok(())
    }

    fn is_tombstoned(&self, doc_id: &Bound<'_, PyAny>) -> PyResult<bool> {
        let key = doc_id.str()?.to_string();
        Ok(self.tombstones.lock().contains_key(&key))
    }

    fn expire(&self) -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let mut ts = self.tombstones.lock();
        let expired: Vec<String> = ts
            .iter()
            .filter(|(_, &t)| now - t > self.ttl)
            .map(|(k, _)| k.clone())
            .collect();
        let count = expired.len() as i64;
        for k in expired {
            ts.remove(&k);
        }
        count
    }

    fn to_dict(&self) -> HashMap<String, f64> {
        self.tombstones.lock().clone()
    }

    fn load(&self, data: HashMap<String, f64>) {
        self.tombstones.lock().extend(data);
    }
}

// ── diff_fields ──────────────────────────────────────────────────────

/// Compute which fields actually differ between local and remote documents.
#[pyfunction]
pub fn diff_fields<'py>(
    py: Python<'py>,
    local: &Bound<'py, PyDict>,
    remote: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PySet>> {
    let result = PySet::empty(py)?;
    let local_keys: Vec<String> = local
        .keys()
        .iter()
        .filter_map(|k| k.extract::<String>().ok())
        .collect();
    let remote_keys: Vec<String> = remote
        .keys()
        .iter()
        .filter_map(|k| k.extract::<String>().ok())
        .collect();

    let mut all_keys: std::collections::HashSet<String> = local_keys.into_iter().collect();
    all_keys.extend(remote_keys);

    for key in &all_keys {
        if key == "_id" {
            continue;
        }
        let local_val = local.get_item(key)?;
        let remote_val = remote.get_item(key)?;
        match (local_val, remote_val) {
            (Some(lv), Some(rv)) => {
                if !lv.eq(&rv)? {
                    result.add(key)?;
                }
            }
            _ => {
                result.add(key)?;
            }
        }
    }
    Ok(result)
}

// ── Conflict resolvers ───────────────────────────────────────────────

fn get_last_modified(doc: &Bound<'_, PyDict>) -> f64 {
    doc.get_item("_lastModified")
        .ok()
        .flatten()
        .and_then(|v| v.extract::<f64>().ok())
        .unwrap_or(0.0)
}

/// Last-write-wins: compare _lastModified timestamps.
#[pyfunction]
pub fn lww<'py>(
    local: &Bound<'py, PyDict>,
    remote: &Bound<'py, PyDict>,
) -> Bound<'py, PyDict> {
    let local_ts = get_last_modified(local);
    let remote_ts = get_last_modified(remote);
    if remote_ts >= local_ts {
        remote.clone()
    } else {
        local.clone()
    }
}

/// Local-wins conflict resolution.
#[pyfunction]
pub fn local_wins<'py>(
    local: &Bound<'py, PyDict>,
    _remote: &Bound<'py, PyDict>,
) -> Bound<'py, PyDict> {
    local.clone()
}

/// Remote-wins conflict resolution.
#[pyfunction]
pub fn remote_wins<'py>(
    _local: &Bound<'py, PyDict>,
    remote: &Bound<'py, PyDict>,
) -> Bound<'py, PyDict> {
    remote.clone()
}

/// Field-level merge strategy.
#[pyfunction]
#[pyo3(signature = (local, remote, local_changed=None, remote_changed=None))]
pub fn field_merge<'py>(
    _py: Python<'py>,
    local: &Bound<'py, PyDict>,
    remote: &Bound<'py, PyDict>,
    local_changed: Option<&Bound<'py, PyAny>>,
    remote_changed: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyDict>> {
    let local_set: std::collections::HashSet<String> = match local_changed {
        Some(obj) => {
            let mut set = std::collections::HashSet::new();
            let iter = obj.try_iter()?;
            for item in iter {
                let item: Bound<'_, PyAny> = item?;
                set.insert(item.extract::<String>()?);
            }
            set
        }
        None => std::collections::HashSet::new(),
    };
    let remote_set: std::collections::HashSet<String> = match remote_changed {
        Some(obj) => {
            let mut set = std::collections::HashSet::new();
            let iter = obj.try_iter()?;
            for item in iter {
                let item: Bound<'_, PyAny> = item?;
                set.insert(item.extract::<String>()?);
            }
            set
        }
        None => std::collections::HashSet::new(),
    };

    let merged = local.copy()?;
    let local_ts = get_last_modified(local);
    let remote_ts = get_last_modified(remote);

    let local_keys: Vec<String> = local
        .keys()
        .iter()
        .filter_map(|k| k.extract::<String>().ok())
        .collect();
    let remote_keys: Vec<String> = remote
        .keys()
        .iter()
        .filter_map(|k| k.extract::<String>().ok())
        .collect();
    let mut all_fields: std::collections::HashSet<String> = local_keys.into_iter().collect();
    all_fields.extend(remote_keys);

    for field in &all_fields {
        if field == "_id" {
            let id_val = local
                .get_item("_id")?
                .or(remote.get_item("_id")?);
            if let Some(v) = id_val {
                merged.set_item("_id", v)?;
            }
            continue;
        }
        let in_local = local_set.contains(field);
        let in_remote = remote_set.contains(field);
        if in_local && !in_remote {
            if let Some(v) = local.get_item(field)? {
                merged.set_item(field, v)?;
            }
        } else if in_remote && !in_local {
            if let Some(v) = remote.get_item(field)? {
                merged.set_item(field, v)?;
            }
        } else if in_local && in_remote {
            if remote_ts >= local_ts {
                if let Some(v) = remote.get_item(field)? {
                    merged.set_item(field, v)?;
                }
            } else if let Some(v) = local.get_item(field)? {
                merged.set_item(field, v)?;
            }
        } else if let Some(v) = remote.get_item(field)? {
            merged.set_item(field, v)?;
        }
    }
    Ok(merged)
}

// ── CRDT helpers ─────────────────────────────────────────────────────

/// Merge two counter values (grow-only counter / PNCounter).
#[pyfunction]
pub fn crdt_counter_merge<'py>(
    py: Python<'py>,
    local_val: &Bound<'py, PyAny>,
    remote_val: &Bound<'py, PyAny>,
) -> PyResult<Py<PyAny>> {
    if let (Ok(lv), Ok(rv)) = (local_val.extract::<f64>(), remote_val.extract::<f64>()) {
        let max_val = lv.max(rv);
        if local_val.is_instance_of::<PyFloat>() || remote_val.is_instance_of::<PyFloat>() {
            return Ok(max_val.into_pyobject(py)?.into_any().unbind());
        }
        return Ok((max_val as i64).into_pyobject(py)?.into_any().unbind());
    }
    Ok(remote_val.clone().unbind())
}

/// Merge two sets (G-Set / OR-Set approximation): union of elements.
#[pyfunction]
pub fn crdt_set_merge<'py>(
    py: Python<'py>,
    local_val: &Bound<'py, PyAny>,
    remote_val: &Bound<'py, PyAny>,
) -> PyResult<Py<PyAny>> {
    let local_list = local_val.cast::<PyList>();
    let remote_list = remote_val.cast::<PyList>();
    if let (Ok(ll), Ok(rl)) = (local_list, remote_list) {
        let json_mod = crate::cached_modules::json_mod(py)?;
        let builtins = crate::cached_modules::builtins(py)?;
        let str_fn = builtins.getattr("str")?;
        let mut seen = std::collections::HashSet::<String>::new();
        let merged = PyList::empty(py);
        for source in [ll, rl] {
            for item in source.iter() {
                let key = if item.is_instance_of::<PyDict>() || item.is_instance_of::<PyList>() {
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("sort_keys", true)?;
                    kwargs.set_item("default", &str_fn)?;
                    json_mod
                        .call_method("dumps", (&item,), Some(&kwargs))?
                        .extract::<String>()?
                } else {
                    item.repr()?.to_string()
                };
                if seen.insert(key) {
                    merged.append(item)?;
                }
            }
        }
        return Ok(merged.into_any().unbind());
    }
    Ok(remote_val.clone().unbind())
}

/// Merge two documents using CRDT semantics for annotated fields.
#[pyfunction]
#[pyo3(signature = (local_doc, remote_doc, crdt_fields=None))]
pub fn crdt_merge_doc<'py>(
    py: Python<'py>,
    local_doc: &Bound<'py, PyDict>,
    remote_doc: &Bound<'py, PyDict>,
    crdt_fields: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    let crdt_map: HashMap<String, String> = match crdt_fields {
        Some(d) => {
            let mut map = HashMap::new();
            for (k, v) in d.iter() {
                map.insert(k.extract::<String>()?, v.extract::<String>()?);
            }
            map
        }
        None => HashMap::new(),
    };

    let merged = local_doc.copy()?;
    let local_ts = get_last_modified(local_doc);
    let remote_ts = get_last_modified(remote_doc);

    let local_keys: Vec<String> = local_doc
        .keys()
        .iter()
        .filter_map(|k| k.extract::<String>().ok())
        .collect();
    let remote_keys: Vec<String> = remote_doc
        .keys()
        .iter()
        .filter_map(|k| k.extract::<String>().ok())
        .collect();
    let mut all_fields: std::collections::HashSet<String> = local_keys.into_iter().collect();
    all_fields.extend(remote_keys);

    for field in &all_fields {
        if field == "_id" {
            continue;
        }
        if let Some(crdt_type) = crdt_map.get(field) {
            let lv = local_doc.get_item(field)?;
            let rv = remote_doc.get_item(field)?;
            match (lv, rv) {
                (Some(lv), Some(rv)) => {
                    if crdt_type == "counter" {
                        let val = crdt_counter_merge(py, &lv, &rv)?;
                        merged.set_item(field, val)?;
                    } else if crdt_type == "set" {
                        let val = crdt_set_merge(py, &lv, &rv)?;
                        merged.set_item(field, val)?;
                    } else if remote_ts >= local_ts {
                        merged.set_item(field, rv)?;
                    } else {
                        merged.set_item(field, lv)?;
                    }
                }
                (_, Some(rv)) => {
                    merged.set_item(field, rv)?;
                }
                (Some(lv), None) => {
                    merged.set_item(field, lv)?;
                }
                _ => {}
            }
        } else if let Some(rv) = remote_doc.get_item(field)? {
            if remote_ts >= local_ts {
                merged.set_item(field, rv)?;
            } else if let Some(lv) = local_doc.get_item(field)? {
                merged.set_item(field, lv)?;
            } else {
                merged.set_item(field, rv)?;
            }
        }
    }
    Ok(merged)
}

// ── EJSON helpers ────────────────────────────────────────────────────

/// JSON encoder default that preserves ObjectId via $oid (Extended JSON).
#[pyfunction]
pub fn ejson_default(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    if let Ok(oid) = obj.extract::<PyRef<'_, ObjectId>>() {
        let d = PyDict::new(py);
        d.set_item("$oid", oid.hex())?;
        return Ok(d.into_any().unbind());
    }
    Ok(obj.str()?.into_any().unbind())
}

/// JSON decoder object_hook that restores ObjectId from {"$oid": ...}.
#[pyfunction]
pub fn ejson_object_hook(py: Python<'_>, d: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
    if d.len() == 1 {
        if let Some(oid_val) = d.get_item("$oid")? {
            if let Ok(s) = oid_val.extract::<String>() {
                if let Ok(oid) = ObjectId::from_hex(py, &s) {
                    return Ok(Py::new(py, oid)?.into_any());
                }
            }
        }
    }
    Ok(d.clone().into_any().unbind())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn with_py<F>(f: F)
    where
        F: for<'py> FnOnce(Python<'py>),
    {
        INIT.call_once(|| {
            Python::initialize();
        });
        Python::attach(f);
    }

    #[test]
    fn test_vector_clock_tick_and_merge() {
        let mut a = VectorClock::new(None);
        a.tick("node_a").unwrap();
        a.tick("node_a").unwrap();
        assert_eq!(a.clock["node_a"], 2);

        let mut b = VectorClock::new(None);
        b.tick("node_b").unwrap();
        b.tick("node_b").unwrap();
        b.tick("node_b").unwrap();

        a.merge(&b).unwrap();
        assert_eq!(a.clock["node_a"], 2);
        assert_eq!(a.clock["node_b"], 3);
    }

    #[test]
    fn test_vector_clock_dominates() {
        let a = VectorClock::new(Some(
            [("x".into(), 5), ("y".into(), 3)].into(),
        ));
        let b = VectorClock::new(Some(
            [("x".into(), 3), ("y".into(), 2)].into(),
        ));
        assert!(a.dominates(&b));
        assert!(!b.dominates(&a));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let a = VectorClock::new(Some(
            [("x".into(), 5), ("y".into(), 1)].into(),
        ));
        let b = VectorClock::new(Some(
            [("x".into(), 3), ("y".into(), 4)].into(),
        ));
        assert!(a.concurrent_with(&b));
        assert!(b.concurrent_with(&a));
    }

    #[test]
    fn test_doc_checksum_deterministic() {
        with_py(|py| {
            let d1 = PyDict::new(py);
            d1.set_item("a", 1).unwrap();
            d1.set_item("b", 2).unwrap();
            let d2 = PyDict::new(py);
            d2.set_item("b", 2).unwrap();
            d2.set_item("a", 1).unwrap();
            let h1 = doc_checksum(py, d1.as_any()).unwrap();
            let h2 = doc_checksum(py, d2.as_any()).unwrap();
            assert_eq!(h1, h2);
        });
    }

    #[test]
    fn test_doc_checksum_none() {
        with_py(|py| {
            let result = doc_checksum(py, py.None().bind(py)).unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_doc_checksum_length() {
        with_py(|py| {
            let d = PyDict::new(py);
            d.set_item("x", 1).unwrap();
            let h = doc_checksum(py, d.as_any()).unwrap().unwrap();
            assert_eq!(h.len(), 16);
            u64::from_str_radix(&h, 16).unwrap();
        });
    }

    #[test]
    fn test_diff_fields_basic() {
        with_py(|py| {
            let local = PyDict::new(py);
            local.set_item("_id", "1").unwrap();
            local.set_item("a", 1).unwrap();
            local.set_item("b", 2).unwrap();
            local.set_item("c", 3).unwrap();

            let remote = PyDict::new(py);
            remote.set_item("_id", "1").unwrap();
            remote.set_item("a", 1).unwrap();
            remote.set_item("b", 99).unwrap();
            remote.set_item("d", 4).unwrap();

            let changed = diff_fields(py, &local, &remote).unwrap();
            assert!(!changed.contains("a").unwrap());
            assert!(changed.contains("b").unwrap());
            assert!(changed.contains("c").unwrap());
            assert!(changed.contains("d").unwrap());
            assert!(!changed.contains("_id").unwrap());
        });
    }

    #[test]
    fn test_lww_picks_newer() {
        with_py(|py| {
            let local = PyDict::new(py);
            local.set_item("_lastModified", 10).unwrap();
            local.set_item("x", "l").unwrap();

            let remote = PyDict::new(py);
            remote.set_item("_lastModified", 20).unwrap();
            remote.set_item("x", "r").unwrap();

            let result = lww(&local, &remote);
            let x: String = result.get_item("x").unwrap().unwrap().extract().unwrap();
            assert_eq!(x, "r");
        });
    }
}
