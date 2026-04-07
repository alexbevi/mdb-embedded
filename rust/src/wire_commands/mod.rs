//! Wire protocol command handlers ported from Python to Rust.
//!
//! Each handler receives `(py, ctx, cmd, seqs)` where `ctx` is a typed
//! `&Bound<'_, ConnectionContext>`, `cmd` is the command dict, and `seqs`
//! is the doc sequences dict.  Handlers call back into Python for storage I/O.

use std::collections::HashMap;
use std::sync::LazyLock;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::wire_context::ConnectionContext;

mod admin;
mod aggregate;
mod crud;
mod diagnostics;
mod handshake;
mod indexes;
mod sessions;

pub type HandlerFn = fn(
    Python<'_>,
    &Bound<'_, ConnectionContext>,
    &Bound<'_, PyDict>,
    &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>>;

pub static RUST_HANDLERS: LazyLock<HashMap<&'static str, HandlerFn>> = LazyLock::new(|| {
    let mut m: HashMap<&'static str, HandlerFn> = HashMap::new();
    handshake::register(&mut m);
    crud::register(&mut m);
    sessions::register(&mut m);
    indexes::register(&mut m);
    aggregate::register(&mut m);
    admin::register(&mut m);
    diagnostics::register(&mut m);
    m
});

// ── Helpers ─────────────────────────────────────────────────────────

pub(crate) fn ok_dict(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("ok", 1.0)?;
    Ok(d)
}

/// Wrap an i64 in `bson.Int64` so the BSON encoder emits BSON int64 (type 0x12).
pub(crate) fn bson_int64(py: Python<'_>, v: i64) -> PyResult<Bound<'_, PyAny>> {
    let cls = crate::cached_modules::bson_int64_cls(py)?;
    cls.call1((v,))
}

pub(crate) fn dict_get_str<'py>(
    dict: &Bound<'py, PyDict>,
    key: &str,
    default: &str,
) -> PyResult<String> {
    match dict.get_item(key)? {
        Some(v) => v.extract::<String>().or(Ok(default.to_string())),
        None => Ok(default.to_string()),
    }
}

pub(crate) fn dict_get_i64(dict: &Bound<'_, PyDict>, key: &str, default: i64) -> PyResult<i64> {
    match dict.get_item(key)? {
        Some(v) => v.extract::<i64>().or(Ok(default)),
        None => Ok(default),
    }
}

pub(crate) fn dict_get_bool(dict: &Bound<'_, PyDict>, key: &str, default: bool) -> PyResult<bool> {
    match dict.get_item(key)? {
        Some(v) => v.is_truthy().or(Ok(default)),
        None => Ok(default),
    }
}

pub(crate) fn dict_get_or_none<'py>(
    dict: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    dict.get_item(key)
}

/// Extract a required `PyDict` value from `dict[key]`, with a clear error message.
#[allow(dead_code)]
pub(crate) fn dict_get_dict<'py>(
    dict: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let v = dict.get_item(key)?.ok_or_else(|| {
        pyo3::exceptions::PyKeyError::new_err(format!("missing required key: {key}"))
    })?;
    Ok(v.cast_into::<PyDict>()?)
}

/// Extract a required `PyList` value from `dict[key]`, with a clear error message.
pub(crate) fn dict_get_list<'py>(
    dict: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<Bound<'py, PyList>> {
    let v = dict.get_item(key)?.ok_or_else(|| {
        pyo3::exceptions::PyKeyError::new_err(format!("missing required key: {key}"))
    })?;
    Ok(v.cast_into::<PyList>()?)
}

pub(crate) fn get_collection<'py>(
    ctx: &Bound<'py, ConnectionContext>,
    db_name: &str,
    coll_name: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let py = ctx.py();
    let ctx_ref = ctx.borrow();
    Ok(ctx_ref
        .get_collection_typed(py, db_name, coll_name)?
        .into_any()
        .into_bound(py))
}

/// Typed collection accessor -- returns a typed `Py<RustLocalCollection>`
/// without any Python dispatch.
pub(crate) fn get_collection_typed(
    ctx: &Bound<'_, ConnectionContext>,
    db_name: &str,
    coll_name: &str,
) -> PyResult<Py<crate::local_collection::RustLocalCollection>> {
    let py = ctx.py();
    let ctx_ref = ctx.borrow();
    ctx_ref.get_collection_typed(py, db_name, coll_name)
}

pub(crate) fn get_db<'py>(
    ctx: &Bound<'py, ConnectionContext>,
    db_name: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let py = ctx.py();
    let ctx_ref = ctx.borrow();
    Ok(ctx_ref.get_db_typed(py, db_name)?.into_any().into_bound(py))
}

pub(crate) fn classify_write_error(
    py: Python<'_>,
    err: &PyErr,
    _dup_key_err: &Bound<'_, PyAny>,
    validation_err: &Bound<'_, PyAny>,
) -> PyResult<(i32, String)> {
    let msg = err.value(py).str()?.to_string();
    if err.is_instance_of::<crate::index_manager::DuplicateKeyError>(py) {
        return Ok((11000, msg));
    }
    if err.is_instance(py, validation_err.cast()?) {
        return Ok((121, msg));
    }
    Ok((1, msg))
}

pub(crate) fn set_last_write(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    op: &str,
    n: i64,
    n_modified: i64,
    err: Option<&str>,
    write_errors: &Bound<'_, PyList>,
) -> PyResult<()> {
    let lwr = crate::wire_context::LastWriteResult {
        op: op.to_string(),
        n,
        n_modified,
        err: err.map(|s| s.to_string()),
        upserted_id: py.None(),
        write_errors: write_errors.clone().unbind(),
    };
    let lwr_py = Py::new(py, lwr)?;
    ctx.borrow_mut().last_write = lwr_py.into_any();
    Ok(())
}

pub(crate) fn apply_sort_py<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyAny>,
    sort_spec: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let docs_list: Vec<Bound<'py, PyAny>> = if let Ok(l) = docs.cast::<PyList>() {
        l.iter().collect()
    } else {
        docs.try_iter()?.collect::<PyResult<_>>()?
    };

    if docs_list.is_empty() {
        return Ok(PyList::empty(py).into_any());
    }

    let keys: Vec<(String, i64)> = if let Ok(d) = sort_spec.cast::<PyDict>() {
        d.iter()
            .map(|(k, v)| Ok((k.extract::<String>()?, v.extract::<i64>().unwrap_or(1))))
            .collect::<PyResult<_>>()?
    } else if let Ok(l) = sort_spec.cast::<PyList>() {
        l.iter()
            .map(|pair| {
                Ok((
                    pair.get_item(0)?.extract::<String>()?,
                    pair.get_item(1)?.extract::<i64>().unwrap_or(1),
                ))
            })
            .collect::<PyResult<_>>()?
    } else {
        return Ok(PyList::new(py, &docs_list)?.into_any());
    };

    let mut indices: Vec<usize> = (0..docs_list.len()).collect();
    for (key_name, direction) in keys.iter().rev() {
        indices.sort_by(|&a, &b| {
            let va = crate::paths::get_value(&docs_list[a], key_name)
                .ok()
                .map(|v| v.into_bound(py));
            let vb = crate::paths::get_value(&docs_list[b], key_name)
                .ok()
                .map(|v| v.into_bound(py));
            let a_none = va.as_ref().map(|v| v.is_none()).unwrap_or(true);
            let b_none = vb.as_ref().map(|v| v.is_none()).unwrap_or(true);
            let cmp = match (a_none, b_none) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => {
                    let a_str = va
                        .as_ref()
                        .and_then(|v| v.str().ok())
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    let b_str = vb
                        .as_ref()
                        .and_then(|v| v.str().ok())
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    a_str.cmp(&b_str)
                }
            };
            if *direction == -1 {
                cmp.reverse()
            } else {
                cmp
            }
        });
    }

    let result = PyList::empty(py);
    for &idx in &indices {
        result.append(&docs_list[idx])?;
    }
    Ok(result.into_any())
}

pub(crate) fn apply_projection_single<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyAny>,
    fields: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let fields_dict = match fields.cast::<PyDict>() {
        Ok(d) => d,
        Err(_) => return Ok(doc.clone()),
    };
    if fields_dict.is_empty() {
        return Ok(doc.clone());
    }

    let doc_dict = doc.cast::<PyDict>()?;
    let mut include: Vec<String> = Vec::new();
    let mut exclude: Vec<String> = Vec::new();

    for (k, v) in fields_dict.iter() {
        let key: String = k.extract()?;
        if v.is_truthy()? {
            include.push(key);
        } else {
            exclude.push(key);
        }
    }

    let result = PyDict::new(py);
    if !include.is_empty() {
        include.push("_id".to_string());
        for (k, v) in doc_dict.iter() {
            let key: String = k.extract()?;
            if include.contains(&key) {
                result.set_item(k, v)?;
            }
        }
    } else {
        for (k, v) in doc_dict.iter() {
            let key: String = k.extract()?;
            if !exclude.contains(&key) {
                result.set_item(k, v)?;
            }
        }
    }
    Ok(result.into_any())
}
