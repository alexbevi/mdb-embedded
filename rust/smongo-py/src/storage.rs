//! Rust-accelerated storage helpers.
//!
//! Provides optimized batch operations that keep BSON encode/decode, query
//! matching, and update application in compiled Rust code. Storage cursor
//! calls are delegated through PyO3 when the hot path uses Python-owned cursors.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use crate::bson_helpers;
use crate::query_compiler;
use crate::query_update;

// ---------------------------------------------------------------------------
// scan_and_filter: the hot inner loop of collection scan
// ---------------------------------------------------------------------------

/// Open a storage cursor, iterate with `next()`, decode BSON in Rust, match
/// against the compiled query predicate in Rust, and collect matching docs.
///
/// This replaces the Python `_iter_collection_scan` hot loop.
#[pyfunction]
pub fn scan_and_filter<'py>(
    py: Python<'py>,
    cursor: &Bound<'py, PyAny>,
    query: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let results = PyList::empty(py);
    let has_query = !query.is_empty();

    loop {
        let rc: i32 = cursor.call_method0("next")?.extract()?;
        if rc != 0 {
            break;
        }
        let raw_value = cursor.call_method0("get_value")?;
        let raw_bytes: &[u8] = raw_value.extract()?;
        let doc = bson_helpers::from_bson(py, raw_bytes)?;

        if !has_query || query_compiler::eval_query(&doc, query)? {
            results.append(&doc)?;
        }
    }

    Ok(results)
}

/// Like `scan_and_filter` but collects at most `batch_size` matching docs.
#[pyfunction]
#[pyo3(signature = (py_cursor, query, batch_size=100))]
pub fn scan_and_filter_batch<'py>(
    py: Python<'py>,
    py_cursor: &Bound<'py, PyAny>,
    query: &Bound<'py, PyDict>,
    batch_size: usize,
) -> PyResult<Bound<'py, PyList>> {
    let results = PyList::empty(py);
    let has_query = !query.is_empty();
    let mut count = 0usize;

    loop {
        if count >= batch_size {
            break;
        }
        let rc: i32 = py_cursor.call_method0("next")?.extract()?;
        if rc != 0 {
            break;
        }
        let raw_value = py_cursor.call_method0("get_value")?;
        let raw_bytes: &[u8] = raw_value.extract()?;
        let doc = bson_helpers::from_bson(py, raw_bytes)?;

        if !has_query || query_compiler::eval_query(&doc, query)? {
            results.append(&doc)?;
            count += 1;
        }
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// batch_insert: encode all docs to BSON in Rust, then insert via cursor
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn batch_insert<'py>(cursor: &Bound<'py, PyAny>, docs: &Bound<'py, PyList>) -> PyResult<()> {
    for doc_any in docs.iter() {
        let doc_dict = doc_any.cast::<PyDict>()?;
        let id_val = doc_dict
            .get_item("_id")?
            .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?;
        let id_str: String = id_val.str()?.extract()?;
        let bson_bytes = bson_helpers::to_bson(doc_dict)?;

        let py = doc_dict.py();
        let bson_bound = bson_bytes.bind(py);
        cursor.set_item(&id_str, bson_bound)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// batch_update: apply updates in Rust, re-encode BSON, write via cursor
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (cursor, docs, update_spec, *, array_filters=None, query=None))]
pub fn batch_update<'py>(
    cursor: &Bound<'py, PyAny>,
    docs: &Bound<'py, PyList>,
    update_spec: &Bound<'py, PyAny>,
    array_filters: Option<&Bound<'py, PyList>>,
    query: Option<&Bound<'py, PyDict>>,
) -> PyResult<u64> {
    let mut modified: u64 = 0;

    for doc_any in docs.iter() {
        let doc_dict = doc_any.cast::<PyDict>()?;

        query_update::apply_update(doc_dict, update_spec, array_filters, query)?;

        let id_val = doc_dict
            .get_item("_id")?
            .ok_or_else(|| PyRuntimeError::new_err("document missing _id"))?;
        let id_str: String = id_val.str()?.extract()?;
        let bson_bytes = bson_helpers::to_bson(doc_dict)?;

        let py = doc_dict.py();
        let bson_bound = bson_bytes.bind(py);
        cursor.set_item(&id_str, bson_bound)?;
        modified += 1;
    }

    Ok(modified)
}

// ---------------------------------------------------------------------------
// Single-doc helpers
// ---------------------------------------------------------------------------

/// Decode raw BSON bytes (from `cursor.get_value()`) into a Python dict.
#[pyfunction]
pub fn cursor_get_doc<'py>(
    py: Python<'py>,
    raw_value: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let raw_bytes: &[u8] = raw_value.extract()?;
    bson_helpers::from_bson(py, raw_bytes)
}

/// Encode a Python dict to BSON bytes for engine storage.
#[pyfunction]
pub fn doc_to_bson(doc: &Bound<'_, PyDict>) -> PyResult<Py<PyBytes>> {
    bson_helpers::to_bson(doc)
}

/// Match a single document against a query dict. Returns true if the doc matches.
#[pyfunction]
pub fn match_doc(doc: &Bound<'_, PyDict>, query: &Bound<'_, PyDict>) -> PyResult<bool> {
    if query.is_empty() {
        return Ok(true);
    }
    query_compiler::eval_query(doc, query)
}
