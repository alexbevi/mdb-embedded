//! Rust port of `smongo.index.IndexManager`.
//!
//! The hot-path methods (add_doc, remove_doc, update_doc) use WiredTiger
//! cursors directly via wt_safe, with key encoding from `index_encoding.rs`.
//! Index creation/deletion/metadata management is also handled in Rust.

use std::collections::HashMap;
use std::mem::ManuallyDrop;

use md5::{Digest, Md5};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::index_encoding;
use crate::paths;
use crate::query_compiler;
use crate::wt_bridge::RustWtSession;
use crate::wt_safe::WtSession;

pyo3::create_exception!(
    smongo._smongo_core,
    DuplicateKeyError,
    pyo3::exceptions::PyException
);

fn word_regex() -> Regex {
    Regex::new(r"\w+").unwrap_or_else(|e| {
        // \w+ is a compile-time constant -- this never fails.
        unreachable!("WORD_RE compile failed: {e}")
    })
}

static WORD_RE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(word_regex);

// ---------------------------------------------------------------------------
// IndexDef (crate-visible for query_planner)
// ---------------------------------------------------------------------------

pub(crate) struct IndexDef {
    pub(crate) name: String,
    pub(crate) keys: Vec<(String, IndexDir)>,
    pub(crate) unique: bool,
    pub(crate) sparse: bool,
    pub(crate) expire_after_seconds: Option<i64>,
    pub(crate) index_type: IndexType,
    pub(crate) partial_filter: Option<Py<PyAny>>,
    pub(crate) table_uri: Option<String>,
}

impl IndexDef {
    pub(crate) fn clone_ref(&self, py: Python<'_>) -> Self {
        Self {
            name: self.name.clone(),
            keys: self.keys.clone(),
            unique: self.unique,
            sparse: self.sparse,
            expire_after_seconds: self.expire_after_seconds,
            index_type: self.index_type,
            partial_filter: self.partial_filter.as_ref().map(|p| p.clone_ref(py)),
            table_uri: self.table_uri.clone(),
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum IndexType {
    Btree,
    Text,
    Hashed,
    Wildcard,
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum IndexDir {
    Asc,
    Desc,
}

impl IndexDir {
    pub(crate) fn as_i32(&self) -> i32 {
        match self {
            Self::Asc => 1,
            Self::Desc => -1,
        }
    }
}

impl IndexDef {
    pub(crate) fn fields(&self) -> Vec<&str> {
        self.keys.iter().map(|(f, _)| f.as_str()).collect()
    }

    pub(crate) fn directions(&self) -> Vec<i32> {
        self.keys.iter().map(|(_, d)| d.as_i32()).collect()
    }

    pub(crate) fn to_py_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        d.set_item("name", &self.name)?;
        let key_tuples: Vec<Bound<'_, pyo3::types::PyTuple>> = self
            .keys
            .iter()
            .map(|(f, dir)| {
                pyo3::types::PyTuple::new(
                    py,
                    [
                        f.into_pyobject(py)?.into_any(),
                        dir.as_i32().into_pyobject(py)?.into_any(),
                    ],
                )
            })
            .collect::<PyResult<_>>()?;
        let keys_list = PyList::new(py, key_tuples)?;
        d.set_item("keys", keys_list)?;
        d.set_item("unique", self.unique)?;
        d.set_item("sparse", self.sparse)?;
        d.set_item("expireAfterSeconds", self.expire_after_seconds)?;
        if self.index_type != IndexType::Btree {
            d.set_item(
                "type",
                match self.index_type {
                    IndexType::Text => "text",
                    IndexType::Hashed => "hashed",
                    IndexType::Wildcard => "wildcard",
                    IndexType::Btree => "btree",
                },
            )?;
        }
        if let Some(ref pf) = self.partial_filter {
            d.set_item("partialFilterExpression", pf.bind(py))?;
        }
        Ok(d.unbind())
    }
}

// ---------------------------------------------------------------------------
// RustIndexManager
// ---------------------------------------------------------------------------

/// Manages secondary indexes on a WiredTiger collection.
#[pyclass]
pub struct RustIndexManager {
    session_raw: Option<*mut wiredtiger_sys::WT_SESSION>,
    session_py: Py<RustWtSession>,
    db_name: crate::DbName,
    coll_name: crate::CollectionName,
    meta_uri: crate::TableUri,
    _indexes: HashMap<String, IndexDef>,
}

// SAFETY: RustIndexManager is a #[pyclass] requiring Send+Sync.  The
// session_raw pointer is only dereferenced while the owning collection's
// InlineRwLock is held (write lock for mutations, read lock for reads).
// This provides serialization independent of the GIL, safe under both
// GIL-enabled and free-threaded Python builds.
unsafe impl Send for RustIndexManager {}
unsafe impl Sync for RustIndexManager {}

// ---------------------------------------------------------------------------
// Pure-Rust helper functions (ported from Python `smongo.index`)
// ---------------------------------------------------------------------------

/// NFKD-normalise + regex-split into lowercase tokens (mirrors `_tokenize`).
#[pyfunction]
pub(crate) fn rs_tokenize(text: &str) -> Vec<String> {
    let normalized: String = text.nfkd().collect();
    WORD_RE
        .find_iter(&normalized)
        .map(|m| m.as_str().to_lowercase())
        .collect()
}

/// MD5-hash a Python value for hashed indexes (mirrors `_hash_value`).
/// Uses Python `json.dumps(value, sort_keys=True, default=str)` for serialization
/// to keep hash output byte-identical with the Python implementation.
#[pyfunction]
pub(crate) fn rs_hash_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    let json_mod = crate::cached_modules::json_mod(py)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("sort_keys", true)?;
    let str_cls = py.eval(c"str", None, None)?;
    kwargs.set_item("default", str_cls)?;
    let serialized: String = json_mod
        .call_method("dumps", (value,), Some(&kwargs))?
        .extract()?;
    let mut hasher = Md5::new();
    hasher.update(serialized.as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

/// Recursively flatten a dict into `(dotted_path, value)` pairs (mirrors `_flatten_doc`).
#[pyfunction]
pub(crate) fn rs_flatten_doc<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyAny>,
) -> PyResult<Vec<(String, Py<PyAny>)>> {
    let mut out = Vec::new();
    if let Ok(d) = doc.cast::<PyDict>() {
        flatten_inner(py, d, "", &mut out)?;
    }
    Ok(out)
}

fn flatten_inner(
    _py: Python<'_>,
    doc: &Bound<'_, PyDict>,
    prefix: &str,
    out: &mut Vec<(String, Py<PyAny>)>,
) -> PyResult<()> {
    for (k, v) in doc.iter() {
        let key: String = k.extract().unwrap_or_default();
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        if let Ok(d) = v.cast::<PyDict>() {
            flatten_inner(_py, d, &path, out)?;
        } else if let Ok(list) = v.cast::<PyList>() {
            for (i, item) in list.iter().enumerate() {
                if let Ok(d) = item.cast::<PyDict>() {
                    flatten_inner(_py, d, &format!("{path}.{i}"), out)?;
                } else {
                    out.push((path.clone(), item.unbind()));
                }
            }
        } else {
            out.push((path, v.unbind()));
        }
    }
    Ok(())
}

impl RustIndexManager {
    pub(crate) fn indexes(&self) -> &HashMap<String, IndexDef> {
        &self._indexes
    }

    fn borrow_session(&self) -> PyResult<ManuallyDrop<WtSession>> {
        crate::wt_bridge::borrow_wt_session(self.session_raw, "index manager")
    }

    fn passes_partial_filter(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        if let Some(ref pf) = idx.partial_filter {
            let pf_bound = pf.bind(py);
            if let Ok(pf_dict) = pf_bound.cast::<PyDict>() {
                return query_compiler::eval_query(doc.cast::<PyDict>()?, pf_dict);
            }
        }
        Ok(true)
    }

    fn insert_btree_entry(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let table_uri = match &idx.table_uri {
            Some(u) => u,
            None => return Ok(()),
        };

        let field_values = PyList::new(
            py,
            idx.fields()
                .iter()
                .map(|f| paths::get_value(doc, f).map(|v| v.into_bound(py)))
                .collect::<PyResult<Vec<_>>>()?,
        )?;
        let directions = PyList::new(py, idx.directions())?;

        if idx.sparse {
            let all_none = field_values.iter().all(|v| v.is_none());
            if all_none {
                return Ok(());
            }
        }

        let doc_id = doc.get_item("_id")?.str()?.to_string();
        let key = index_encoding::encode_index_key(&field_values, &doc_id, &directions)?;

        if idx.unique {
            let prefix = index_encoding::encode_index_key_prefix(&field_values, &directions)?;
            if self.has_duplicate(idx, &prefix, &doc_id)? {
                let msg = format!("E11000 duplicate key error index: {}", idx.name);
                return Err(DuplicateKeyError::new_err(msg));
            }
        }

        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(table_uri, Some("overwrite=true"))?;
        cursor.set_key_str(&key);
        cursor.set_value_str(&doc_id);
        cursor.update()?;
        cursor.close()?;
        Ok(())
    }

    fn delete_btree_entry(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let table_uri = match &idx.table_uri {
            Some(u) => u,
            None => return Ok(()),
        };

        let field_values = PyList::new(
            py,
            idx.fields()
                .iter()
                .map(|f| paths::get_value(doc, f).map(|v| v.into_bound(py)))
                .collect::<PyResult<Vec<_>>>()?,
        )?;
        let directions = PyList::new(py, idx.directions())?;

        if idx.sparse {
            let all_none = field_values.iter().all(|v| v.is_none());
            if all_none {
                return Ok(());
            }
        }

        let doc_id = doc.get_item("_id")?.str()?.to_string();
        let key = index_encoding::encode_index_key(&field_values, &doc_id, &directions)?;

        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(table_uri, Some("overwrite=true"))?;
        cursor.set_key_str(&key);
        let _ = cursor.remove(); // ignore if not found
        cursor.close()?;
        Ok(())
    }

    fn has_duplicate(&self, idx: &IndexDef, prefix: &str, doc_id: &str) -> PyResult<bool> {
        let table_uri = match &idx.table_uri {
            Some(u) => u,
            None => return Ok(false),
        };
        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(table_uri, None)?;
        cursor.set_key_str(prefix);
        let found = match cursor.search_near() {
            Ok(exact) => {
                if exact < 0 {
                    cursor.next().is_ok()
                } else {
                    true
                }
            }
            Err(_) => false,
        };
        if found {
            if let Ok(key) = cursor.get_key_str() {
                if key.starts_with(prefix) {
                    if let Ok(val) = cursor.get_value_str() {
                        if val != doc_id {
                            cursor.close()?;
                            return Ok(true);
                        }
                    }
                    // Check if there's another entry with same prefix but different doc_id
                    while cursor.next().is_ok() {
                        if let Ok(k) = cursor.get_key_str() {
                            if !k.starts_with(prefix) {
                                break;
                            }
                            if let Ok(v) = cursor.get_value_str() {
                                if v != doc_id {
                                    cursor.close()?;
                                    return Ok(true);
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        cursor.close()?;
        Ok(false)
    }

    fn insert_text_entry(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let table_uri = match &idx.table_uri {
            Some(u) => u,
            None => return Ok(()),
        };
        let doc_id = doc.get_item("_id")?.str()?.to_string();
        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(table_uri, Some("overwrite=true"))?;
        for (field, _) in &idx.keys {
            let val = paths::get_value(doc, field)?;
            if let Ok(s) = val.bind(py).extract::<String>() {
                let tokens = rs_tokenize(&s);
                for token in tokens {
                    let key = format!("{token}|{doc_id}");
                    cursor.set_key_str(&key);
                    cursor.set_value_str(&doc_id);
                    cursor.update()?;
                }
            }
        }
        cursor.close()?;
        Ok(())
    }

    fn delete_text_entry(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let table_uri = match &idx.table_uri {
            Some(u) => u,
            None => return Ok(()),
        };
        let doc_id = doc.get_item("_id")?.str()?.to_string();
        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(table_uri, Some("overwrite=true"))?;
        for (field, _) in &idx.keys {
            let val = paths::get_value(doc, field)?;
            if let Ok(s) = val.bind(py).extract::<String>() {
                let tokens = rs_tokenize(&s);
                for token in tokens {
                    let key = format!("{token}|{doc_id}");
                    cursor.set_key_str(&key);
                    let _ = cursor.remove();
                }
            }
        }
        cursor.close()?;
        Ok(())
    }

    fn insert_hashed_entry(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let table_uri = match &idx.table_uri {
            Some(u) => u,
            None => return Ok(()),
        };
        let doc_id = doc.get_item("_id")?.str()?.to_string();
        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(table_uri, Some("overwrite=true"))?;
        for (field, _) in &idx.keys {
            let val = paths::get_value(doc, field)?;
            let h = rs_hash_value(py, val.bind(py))?;
            let key = format!("{h}|{doc_id}");
            cursor.set_key_str(&key);
            cursor.set_value_str(&doc_id);
            cursor.update()?;
        }
        cursor.close()?;
        Ok(())
    }

    fn delete_hashed_entry(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let table_uri = match &idx.table_uri {
            Some(u) => u,
            None => return Ok(()),
        };
        let doc_id = doc.get_item("_id")?.str()?.to_string();
        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(table_uri, Some("overwrite=true"))?;
        for (field, _) in &idx.keys {
            let val = paths::get_value(doc, field)?;
            let h = rs_hash_value(py, val.bind(py))?;
            let key = format!("{h}|{doc_id}");
            cursor.set_key_str(&key);
            let _ = cursor.remove();
        }
        cursor.close()?;
        Ok(())
    }

    fn insert_wildcard_entry(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let table_uri = match &idx.table_uri {
            Some(u) => u,
            None => return Ok(()),
        };
        let doc_id = doc.get_item("_id")?.str()?.to_string();
        let pairs = rs_flatten_doc(py, doc)?;
        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(table_uri, Some("overwrite=true"))?;
        for (path, val) in &pairs {
            if path == "_id" {
                continue;
            }
            let encoded = index_encoding::sortable_encode(val.bind(py))?;
            let key = format!("{path}|{encoded}|{doc_id}");
            cursor.set_key_str(&key);
            cursor.set_value_str(&doc_id);
            cursor.update()?;
        }
        cursor.close()?;
        Ok(())
    }

    fn delete_wildcard_entry(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let table_uri = match &idx.table_uri {
            Some(u) => u,
            None => return Ok(()),
        };
        let doc_id = doc.get_item("_id")?.str()?.to_string();
        let pairs = rs_flatten_doc(py, doc)?;
        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(table_uri, Some("overwrite=true"))?;
        for (path, val) in &pairs {
            if path == "_id" {
                continue;
            }
            let encoded = index_encoding::sortable_encode(val.bind(py))?;
            let key = format!("{path}|{encoded}|{doc_id}");
            cursor.set_key_str(&key);
            let _ = cursor.remove();
        }
        cursor.close()?;
        Ok(())
    }

    fn insert_entry(&self, py: Python<'_>, idx: &IndexDef, doc: &Bound<'_, PyAny>) -> PyResult<()> {
        if !self.passes_partial_filter(py, idx, doc)? {
            return Ok(());
        }
        match idx.index_type {
            IndexType::Btree => self.insert_btree_entry(py, idx, doc),
            IndexType::Text => self.insert_text_entry(py, idx, doc),
            IndexType::Hashed => self.insert_hashed_entry(py, idx, doc),
            IndexType::Wildcard => self.insert_wildcard_entry(py, idx, doc),
        }
    }

    fn delete_entry(&self, py: Python<'_>, idx: &IndexDef, doc: &Bound<'_, PyAny>) -> PyResult<()> {
        if !self.passes_partial_filter(py, idx, doc)? {
            return Ok(());
        }
        match idx.index_type {
            IndexType::Btree => self.delete_btree_entry(py, idx, doc),
            IndexType::Text => self.delete_text_entry(py, idx, doc),
            IndexType::Hashed => self.delete_hashed_entry(py, idx, doc),
            IndexType::Wildcard => self.delete_wildcard_entry(py, idx, doc),
        }
    }

    // --- pub(crate) methods for direct Rust callers (no Python dispatch) ---

    pub(crate) fn add_doc(&self, py: Python<'_>, doc: &Bound<'_, PyAny>) -> PyResult<()> {
        let idx_snapshot: Vec<IndexDef> = self._indexes.values().map(|i| i.clone_ref(py)).collect();
        for idx in &idx_snapshot {
            self.insert_entry(py, idx, doc)?;
        }
        Ok(())
    }

    pub(crate) fn remove_doc(&self, py: Python<'_>, doc: &Bound<'_, PyAny>) -> PyResult<()> {
        let idx_snapshot: Vec<IndexDef> = self._indexes.values().map(|i| i.clone_ref(py)).collect();
        for idx in &idx_snapshot {
            self.delete_entry(py, idx, doc)?;
        }
        Ok(())
    }

    pub(crate) fn update_doc(
        &self,
        py: Python<'_>,
        old_doc: &Bound<'_, PyAny>,
        new_doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let idx_snapshot: Vec<IndexDef> = self._indexes.values().map(|i| i.clone_ref(py)).collect();
        for idx in &idx_snapshot {
            let needs_update = idx.fields().iter().any(|field| {
                let old_val = paths::get_value(old_doc, field).ok();
                let new_val = paths::get_value(new_doc, field).ok();
                match (old_val, new_val) {
                    (Some(a), Some(b)) => a.bind(py).ne(b.bind(py)).unwrap_or(true),
                    (None, None) => false,
                    _ => true,
                }
            });
            if needs_update {
                self.delete_entry(py, idx, old_doc)?;
                self.insert_entry(py, idx, new_doc)?;
            }
        }
        Ok(())
    }

    pub(crate) fn create_index(
        &mut self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let keys_list: Vec<(String, Py<PyAny>)> = if let Ok(s) = keys.extract::<String>() {
            vec![(s, 1i32.into_pyobject(py)?.unbind().into_any())]
        } else {
            keys.extract()?
        };

        let mut idx_type = IndexType::Btree;
        let mut parsed_keys: Vec<(String, IndexDir)> = Vec::new();
        for (f, d) in &keys_list {
            if let Ok(s) = d.bind(py).extract::<String>() {
                match s.as_str() {
                    "text" => idx_type = IndexType::Text,
                    "hashed" => idx_type = IndexType::Hashed,
                    "2dsphere" | "2d" => {
                        return Err(pyo3::exceptions::PyNotImplementedError::new_err(format!(
                            "{s} indexes are planned but not yet implemented; \
                                    $geoNear aggregation works without an index. \
                                    See WHATSNEXT.md for the geospatial roadmap."
                        )));
                    }
                    _ => {}
                }
                parsed_keys.push((f.clone(), IndexDir::Asc));
            } else if let Ok(i) = d.bind(py).extract::<i32>() {
                if i == -1 {
                    parsed_keys.push((f.clone(), IndexDir::Desc));
                } else {
                    parsed_keys.push((f.clone(), IndexDir::Asc));
                }
            } else {
                parsed_keys.push((f.clone(), IndexDir::Asc));
            }
            if f == "$**" {
                idx_type = IndexType::Wildcard;
            }
        }

        let empty_kw = PyDict::new(py);
        let kw = kwargs.unwrap_or(&empty_kw);
        let name: String = kw
            .get_item("name")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_else(|| {
                keys_list
                    .iter()
                    .map(|(f, d)| {
                        let ds = d
                            .bind(py)
                            .str()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|_| "1".into());
                        format!("{f}_{ds}")
                    })
                    .collect::<Vec<_>>()
                    .join("_")
            });

        if self._indexes.contains_key(&name) {
            return Ok(name);
        }

        let unique: bool = kw
            .get_item("unique")?
            .map(|v| v.extract().unwrap_or(false))
            .unwrap_or(false);
        let sparse: bool = kw
            .get_item("sparse")?
            .map(|v| v.extract().unwrap_or(false))
            .unwrap_or(false);
        let expire: Option<i64> = kw
            .get_item("expireAfterSeconds")?
            .and_then(|v| v.extract().ok());
        let partial_filter: Option<Py<PyAny>> =
            kw.get_item("partialFilterExpression")?.map(|v| v.unbind());

        let table_uri = format!("table:__idx_{}_{}_{name}", self.db_name, self.coll_name);

        let session = self.borrow_session()?;
        session.create(&table_uri, "key_format=S,value_format=S")?;

        // Persist metadata
        let defn = PyDict::new(py);
        let key_tuples: Vec<Bound<'_, pyo3::types::PyTuple>> = keys_list
            .iter()
            .map(|(f, d)| {
                pyo3::types::PyTuple::new(py, [PyString::new(py, f).into_any(), d.bind(py).clone()])
            })
            .collect::<PyResult<_>>()?;
        let keys_py = PyList::new(py, key_tuples)?;
        defn.set_item("keys", keys_py)?;
        defn.set_item("unique", unique)?;
        defn.set_item("sparse", sparse)?;
        defn.set_item("expireAfterSeconds", expire)?;
        let type_str = match idx_type {
            IndexType::Text => "text",
            IndexType::Hashed => "hashed",
            IndexType::Wildcard => "wildcard",
            IndexType::Btree => "btree",
        };
        defn.set_item("type", type_str)?;
        if let Some(ref pf) = partial_filter {
            defn.set_item("partialFilterExpression", pf.bind(py))?;
        }
        let json_str: String = py
            .import("json")?
            .call_method1("dumps", (defn,))?
            .extract()?;

        let mut meta_cursor = session.open_cursor(&self.meta_uri, Some("overwrite=true"))?;
        meta_cursor.set_key_str(&name);
        meta_cursor.set_value_str(&json_str);
        meta_cursor.update()?;
        meta_cursor.close()?;

        let idx = IndexDef {
            name: name.clone(),
            keys: parsed_keys,
            unique,
            sparse,
            expire_after_seconds: expire,
            index_type: idx_type,
            partial_filter,
            table_uri: Some(table_uri),
        };
        self._indexes.insert(name.clone(), idx);
        Ok(name)
    }

    pub(crate) fn drop_index(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        let idx = match self._indexes.remove(name) {
            Some(i) => i,
            None => return Ok(()),
        };
        if let Some(ref uri) = idx.table_uri {
            let session = self.borrow_session()?;
            let _ = session.drop_table(uri, Some("force"));

            let mut cursor = session.open_cursor(&self.meta_uri, Some("overwrite=true"))?;
            cursor.set_key_str(name);
            let _ = cursor.remove();
            cursor.close()?;
        }
        let _ = py;
        Ok(())
    }

    pub(crate) fn list_indexes(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for idx in self._indexes.values() {
            list.append(idx.to_py_dict(py)?)?;
        }
        Ok(list.unbind())
    }

    pub(crate) fn rebuild_index(
        &mut self,
        py: Python<'_>,
        name: &str,
        all_docs: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let idx = match self._indexes.get(name) {
            Some(i) => i.clone_ref(py),
            None => return Ok(()),
        };
        let table_uri = match &idx.table_uri {
            Some(u) => u.clone(),
            None => return Ok(()),
        };

        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(&table_uri, None)?;
        let mut keys_to_remove = Vec::new();
        while cursor.next().is_ok() {
            if let Ok(k) = cursor.get_key_str() {
                keys_to_remove.push(k);
            }
        }
        cursor.close()?;

        if !keys_to_remove.is_empty() {
            let mut cursor = session.open_cursor(&table_uri, Some("overwrite=true"))?;
            for key in &keys_to_remove {
                cursor.set_key_str(key);
                let _ = cursor.remove();
            }
            cursor.close()?;
        }

        for doc in all_docs.iter() {
            self.insert_entry(py, &idx, &doc)?;
        }
        Ok(())
    }

    pub(crate) fn meta_uri(&self) -> &str {
        &self.meta_uri
    }

    /// Returns all index table URIs (for dropping during collection removal).
    pub(crate) fn all_index_uris(&self) -> Vec<String> {
        self._indexes
            .values()
            .filter_map(|idx| idx.table_uri.clone())
            .collect()
    }

    fn load_metadata(&mut self, py: Python<'_>) -> PyResult<()> {
        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(&self.meta_uri, None)?;
        while cursor.next().is_ok() {
            let name = cursor.get_key_str()?;
            let defn_json = cursor.get_value_str()?;
            let json_mod = crate::cached_modules::json_mod(py)?;
            let defn: Bound<'_, PyDict> =
                json_mod.call_method1("loads", (&defn_json,))?.extract()?;

            let keys_obj = defn
                .get_item("keys")?
                .ok_or_else(|| PyRuntimeError::new_err("index metadata missing 'keys'"))?;
            let mut keys = Vec::new();
            let idx_type_str: String = defn
                .get_item("type")?
                .map(|v| v.extract().unwrap_or_else(|_| "btree".to_string()))
                .unwrap_or_else(|| "btree".to_string());
            let idx_type = match idx_type_str.as_str() {
                "text" => IndexType::Text,
                "hashed" => IndexType::Hashed,
                "wildcard" => IndexType::Wildcard,
                _ => IndexType::Btree,
            };
            for pair in keys_obj.try_iter()? {
                let pair = pair?;
                let field: String = pair.get_item(0)?.extract()?;
                let d = pair.get_item(1)?;
                let dir = if let Ok(i) = d.extract::<i32>() {
                    if i == -1 {
                        IndexDir::Desc
                    } else {
                        IndexDir::Asc
                    }
                } else {
                    IndexDir::Asc
                };
                keys.push((field, dir));
            }

            let unique: bool = defn
                .get_item("unique")?
                .map(|v| v.extract().unwrap_or(false))
                .unwrap_or(false);
            let sparse: bool = defn
                .get_item("sparse")?
                .map(|v| v.extract().unwrap_or(false))
                .unwrap_or(false);
            let expire: Option<i64> = defn
                .get_item("expireAfterSeconds")?
                .and_then(|v| v.extract().ok());
            let partial_filter = defn
                .get_item("partialFilterExpression")?
                .map(|v| v.unbind());

            let table_uri = format!("table:__idx_{}_{}_{name}", self.db_name, self.coll_name);
            self._indexes.insert(
                name.clone(),
                IndexDef {
                    name,
                    keys,
                    unique,
                    sparse,
                    expire_after_seconds: expire,
                    index_type: idx_type,
                    partial_filter,
                    table_uri: Some(table_uri),
                },
            );
        }
        cursor.close()?;
        Ok(())
    }
}

#[pymethods]
impl RustIndexManager {
    #[new]
    pub fn new(
        py: Python<'_>,
        session: Py<RustWtSession>,
        db_name: &str,
        coll_name: &str,
    ) -> PyResult<Self> {
        let session_raw = {
            let borrow = session.bind(py).borrow();
            let s = borrow.get()?;
            s.raw_ptr()
        };

        let meta_uri = format!("table:__idxmeta_{db_name}_{coll_name}");

        // Create the metadata table
        {
            // SAFETY: session_raw was obtained from a valid RustWtSession above.
            let s = ManuallyDrop::new(unsafe { WtSession::from_raw(session_raw) });
            s.create(&meta_uri, "key_format=S,value_format=S")?;
        }

        let mut mgr = Self {
            session_raw: Some(session_raw),
            session_py: session,
            db_name: db_name.to_string(),
            coll_name: coll_name.to_string(),
            meta_uri,
            _indexes: HashMap::new(),
        };
        mgr.load_metadata(py)?;
        Ok(mgr)
    }

    // --- Hot path: add_doc / remove_doc / update_doc ---

    #[pyo3(name = "add_doc")]
    fn py_add_doc(&self, py: Python<'_>, doc: &Bound<'_, PyAny>) -> PyResult<()> {
        self.add_doc(py, doc)
    }

    #[pyo3(name = "remove_doc")]
    fn py_remove_doc(&self, py: Python<'_>, doc: &Bound<'_, PyAny>) -> PyResult<()> {
        self.remove_doc(py, doc)
    }

    #[pyo3(name = "update_doc")]
    fn py_update_doc(
        &self,
        py: Python<'_>,
        old_doc: &Bound<'_, PyAny>,
        new_doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.update_doc(py, old_doc, new_doc)
    }

    // --- Index management ---

    #[pyo3(name = "create_index", signature = (keys, **kwargs))]
    fn py_create_index(
        &mut self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        self.create_index(py, keys, kwargs)
    }

    #[pyo3(name = "drop_index")]
    fn py_drop_index(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        self.drop_index(py, name)
    }

    #[pyo3(name = "list_indexes")]
    fn py_list_indexes(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.list_indexes(py)
    }

    #[pyo3(name = "rebuild_index")]
    fn py_rebuild_index(
        &mut self,
        py: Python<'_>,
        name: &str,
        all_docs: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        self.rebuild_index(py, name, all_docs)
    }

    fn get_indexes(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        for (name, idx) in &self._indexes {
            d.set_item(name, idx.to_py_dict(py)?)?;
        }
        Ok(d.unbind())
    }

    // --- Properties for Python compatibility ---

    #[getter(meta_uri)]
    fn py_meta_uri(&self) -> &str {
        self.meta_uri()
    }

    #[getter]
    fn session(&self, py: Python<'_>) -> Py<RustWtSession> {
        self.session_py.clone_ref(py)
    }

    #[getter]
    fn get_indexes_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let d = PyDict::new(py);
        for (name, idx) in &self._indexes {
            let obj = PyDict::new(py);
            obj.set_item("name", &idx.name)?;
            obj.set_item("table_uri", &idx.table_uri)?;
            let fields: Vec<&str> = idx.fields();
            obj.set_item("fields", fields)?;
            obj.set_item("directions", idx.directions())?;
            let key_tuples: Vec<Bound<'_, pyo3::types::PyTuple>> = idx
                .keys
                .iter()
                .map(|(f, d)| {
                    pyo3::types::PyTuple::new(
                        py,
                        [
                            f.into_pyobject(py)?.into_any(),
                            d.as_i32().into_pyobject(py)?.into_any(),
                        ],
                    )
                })
                .collect::<PyResult<_>>()?;
            let keys_py = PyList::new(py, key_tuples)?;
            obj.set_item("keys", keys_py)?;
            obj.set_item("unique", idx.unique)?;
            obj.set_item("sparse", idx.sparse)?;
            d.set_item(name, obj)?;
        }
        Ok(d.unbind().into_any())
    }

    /// Python-compatible `_indexes` property: dict[str, SimpleNamespace]
    /// with `table_uri`, `name`, `directions`, etc. Matches the Python
    /// IndexManager API that `LocalCollection` and `TTLReaper` depend on.
    #[getter(_indexes)]
    fn py_indexes(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let ns_cls = crate::cached_modules::types_mod(py)?.getattr("SimpleNamespace")?;
        let d = PyDict::new(py);
        for (name, idx) in &self._indexes {
            let kwargs = PyDict::new(py);
            kwargs.set_item("name", &idx.name)?;
            kwargs.set_item("table_uri", &idx.table_uri)?;
            kwargs.set_item("unique", idx.unique)?;
            kwargs.set_item("sparse", idx.sparse)?;
            kwargs.set_item("expire_after_seconds", idx.expire_after_seconds)?;
            kwargs.set_item("directions", idx.directions())?;
            let fields: Vec<&str> = idx.fields();
            kwargs.set_item("fields", fields)?;
            let ns = ns_cls.call((), Some(&kwargs))?;
            d.set_item(name, ns)?;
        }
        Ok(d.unbind())
    }
}
