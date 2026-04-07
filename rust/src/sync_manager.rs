//! Rust-accelerated sync helpers.
//!
//! The full SyncManager orchestration stays in Python (smongo/sync.py) because
//! it is deeply coupled to Python objects on both ends (local LocalCollection,
//! remote PyMongo collection, WT sessions for checkpoint persistence).
//!
//! This module ports the hot-path helpers that are called per-document during
//! push/pull cycles: ObjectId type bridging and the upsert-with-conflict-
//! resolution logic.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::objectid::ObjectId as RustObjectId;

/// Recursively convert engine ObjectId to bson.ObjectId (or str) for PyMongo.
///
/// Called on every document and sub-document during push. The recursive
/// tree-walk in Rust avoids Python per-element overhead.
#[pyfunction]
pub fn to_pymongo<'py>(py: Python<'py>, value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    if value.is_instance_of::<RustObjectId>() {
        let oid_str = value.str()?;
        let bson_mod = crate::cached_modules::bson_mod(py);
        return match bson_mod {
            Ok(bson) => {
                let bson_oid_cls = bson.getattr("ObjectId")?;
                Ok(bson_oid_cls.call1((oid_str,))?)
            }
            Err(_) => Ok(oid_str.into_any()),
        };
    }

    if let Ok(py_oid_cls) =
        crate::cached_modules::smongo_objectid(py).and_then(|m| m.getattr("_PyObjectId"))
    {
        if value.is_instance(&py_oid_cls)? {
            let oid_str = value.str()?;
            let bson_mod = crate::cached_modules::bson_mod(py);
            return match bson_mod {
                Ok(bson) => {
                    let bson_oid_cls = bson.getattr("ObjectId")?;
                    Ok(bson_oid_cls.call1((oid_str,))?)
                }
                Err(_) => Ok(oid_str.into_any()),
            };
        }
    }

    if let Ok(d) = value.cast::<PyDict>() {
        let out = PyDict::new(py);
        for (k, v) in d.iter() {
            out.set_item(k, to_pymongo(py, &v)?)?;
        }
        return Ok(out.into_any());
    }

    if let Ok(l) = value.cast::<PyList>() {
        let out = PyList::empty(py);
        for item in l.iter() {
            out.append(to_pymongo(py, &item)?)?;
        }
        return Ok(out.into_any());
    }

    Ok(value.clone())
}

/// Recursively convert bson.ObjectId to engine ObjectId after PyMongo read.
///
/// Called on every document during pull.
#[pyfunction]
pub fn from_pymongo<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(bson_mod) = crate::cached_modules::bson_mod(py) {
        if let Ok(bson_oid_cls) = bson_mod.getattr("ObjectId") {
            if value.is_instance(&bson_oid_cls)? {
                let oid_str: String = value.str()?.extract()?;
                let oid = crate::objectid::ObjectId::from_hex(py, &oid_str)?;
                return Ok(Py::new(py, oid)?.into_bound(py).into_any());
            }
        }
    }

    if let Ok(d) = value.cast::<PyDict>() {
        let out = PyDict::new(py);
        for (k, v) in d.iter() {
            out.set_item(k, from_pymongo(py, &v)?)?;
        }
        return Ok(out.into_any());
    }

    if let Ok(l) = value.cast::<PyList>() {
        let out = PyList::empty(py);
        for item in l.iter() {
            out.append(from_pymongo(py, &item)?)?;
        }
        return Ok(out.into_any());
    }

    Ok(value.clone())
}

/// Compute real-diff fields between two documents, excluding sync metadata.
///
/// Returns (changed_fields, has_real_diff) -- used by _upsert_remote_doc.
#[pyfunction]
pub fn sync_diff<'py>(
    py: Python<'py>,
    local_doc: &Bound<'py, PyDict>,
    remote_doc: &Bound<'py, PyDict>,
) -> PyResult<(Bound<'py, pyo3::types::PySet>, bool)> {
    let all_diff = crate::sync_utils::diff_fields(py, local_doc, remote_doc)?;

    let meta_field = "_lastModified";
    let _ = all_diff.discard(meta_field);
    let has_real_diff = !all_diff.is_empty();

    Ok((all_diff, has_real_diff))
}
