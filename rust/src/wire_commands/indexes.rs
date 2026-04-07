//! Wire protocol index management commands: `createIndexes`, `dropIndexes`, `listIndexes`.
use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::wire_context::ConnectionContext;
use crate::wire_cursors::CursorRegistry;
use crate::wire_errors::make_error;

use super::{bson_int64, dict_get_i64, dict_get_str, get_collection_typed, HandlerFn};

fn cmd_list_indexes(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("listIndexes")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'listIndexes'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let ns = format!("{db_name}.{coll_name}");

    let indexes_py = coll_py.bind(py).borrow().list_indexes(py)?;
    let indexes = indexes_py.bind(py);
    let formatted = PyList::empty(py);

    let id_spec = PyDict::new(py);
    id_spec.set_item("v", 2)?;
    let id_key = PyDict::new(py);
    id_key.set_item("_id", 1)?;
    id_spec.set_item("key", id_key)?;
    id_spec.set_item("name", "_id_")?;
    id_spec.set_item("ns", &ns)?;
    formatted.append(id_spec)?;

    for idx in indexes.try_iter()? {
        let idx = idx?;
        let keys = idx.get_item("keys")?;
        let key_dict = PyDict::new(py);
        if let Ok(keys_list) = keys.cast::<PyList>() {
            for pair in keys_list.iter() {
                let k: String = pair.get_item(0)?.extract()?;
                let d = pair.get_item(1)?;
                key_dict.set_item(k, d)?;
            }
        }
        let spec = PyDict::new(py);
        spec.set_item("v", 2)?;
        spec.set_item("key", key_dict)?;
        let name = idx.get_item("name")?;
        spec.set_item("name", name)?;
        spec.set_item("ns", &ns)?;
        if let Ok(unique) = idx.get_item("unique") {
            if unique.is_truthy()? {
                spec.set_item("unique", true)?;
            }
        }
        if let Ok(sparse) = idx.get_item("sparse") {
            if sparse.is_truthy()? {
                spec.set_item("sparse", true)?;
            }
        }
        if let Ok(eas) = idx.get_item("expireAfterSeconds") {
            if !eas.is_none() {
                spec.set_item("expireAfterSeconds", eas)?;
            }
        }
        formatted.append(spec)?;
    }

    let batch_size = {
        let cursor_opt = cmd.get_item("cursor")?;
        if let Some(c) = cursor_opt {
            if let Ok(d) = c.cast::<PyDict>() {
                dict_get_i64(d, "batchSize", 101)?
            } else {
                101
            }
        } else {
            101
        }
    };

    let cr = ctx.borrow().cursor_registry.clone_ref(py);
    let cr_reg = cr.bind(py).cast::<CursorRegistry>()?;
    let cmd_ns = format!("{db_name}.$cmd.listIndexes.{coll_name}");
    let (cursor_id, first_batch) =
        cr_reg
            .borrow()
            .create(py, &cmd_ns, &formatted, Some(batch_size as usize))?;

    let cursor_dict = PyDict::new(py);
    cursor_dict.set_item("id", bson_int64(py, cursor_id)?)?;
    cursor_dict.set_item("ns", &cmd_ns)?;
    cursor_dict.set_item("firstBatch", first_batch)?;
    let resp = PyDict::new(py);
    resp.set_item("cursor", cursor_dict)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_create_indexes(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("createIndexes")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'createIndexes'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;

    let before: i64 = coll_py.bind(py).borrow().list_indexes(py)?.bind(py).len()? as i64 + 1;

    let indexes = cmd
        .get_item("indexes")?
        .unwrap_or_else(|| PyList::empty(py).into_any());
    for idx_spec in indexes.try_iter()? {
        let idx_spec = idx_spec?;
        let key = idx_spec.get_item("key")?;
        let key_dict = key.cast::<PyDict>()?;
        let keys_list = PyList::empty(py);
        for (k, v) in key_dict.iter() {
            let pair = (k, v);
            keys_list.append(pair)?;
        }
        let kwargs = PyDict::new(py);
        if let Ok(name) = idx_spec.get_item("name") {
            kwargs.set_item("name", name)?;
        }
        if let Ok(unique) = idx_spec.get_item("unique") {
            if unique.is_truthy()? {
                kwargs.set_item("unique", true)?;
            }
        }
        if let Ok(sparse) = idx_spec.get_item("sparse") {
            if sparse.is_truthy()? {
                kwargs.set_item("sparse", true)?;
            }
        }
        if let Ok(eas) = idx_spec.get_item("expireAfterSeconds") {
            kwargs.set_item("expireAfterSeconds", eas)?;
        }
        coll_py
            .bind(py)
            .borrow()
            .create_index(py, keys_list.as_any(), false, Some(&kwargs))?;
    }

    let after: i64 = coll_py.bind(py).borrow().list_indexes(py)?.bind(py).len()? as i64 + 1;

    let resp = PyDict::new(py);
    resp.set_item("numIndexesBefore", before)?;
    resp.set_item("numIndexesAfter", after)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_drop_indexes(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("dropIndexes")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'dropIndexes'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;

    let n_before: i64 = coll_py.bind(py).borrow().list_indexes(py)?.bind(py).len()? as i64 + 1;
    let index = cmd.get_item("index")?;

    if let Some(ref idx) = index {
        if let Ok(s) = idx.extract::<String>() {
            if s == "*" {
                let indexes_py = coll_py.bind(py).borrow().list_indexes(py)?;
                let indexes = indexes_py.bind(py);
                let index_list: Vec<Bound<'_, PyAny>> =
                    indexes.try_iter()?.collect::<PyResult<_>>()?;
                for entry in index_list {
                    let name: String = entry.get_item("name")?.extract()?;
                    let _ = coll_py.bind(py).borrow().drop_index(py, &name, false);
                }
            } else {
                coll_py.bind(py).borrow().drop_index(py, &s, false)?;
            }
        } else if let Ok(list) = idx.cast::<PyList>() {
            for item in list.iter() {
                if let Ok(name) = item.extract::<String>() {
                    coll_py.bind(py).borrow().drop_index(py, &name, false)?;
                }
            }
        } else if let Ok(dict) = idx.cast::<PyDict>() {
            let target_keys: Vec<String> = dict
                .keys()
                .iter()
                .map(|k| k.extract().unwrap_or_default())
                .collect();
            let indexes_py = coll_py.bind(py).borrow().list_indexes(py)?;
            let indexes = indexes_py.bind(py);
            for entry in indexes.try_iter()? {
                let entry = entry?;
                let idx_keys_raw = entry.get_item("keys")?;
                if let Ok(keys_list) = idx_keys_raw.cast::<PyList>() {
                    let idx_keys: Vec<String> = keys_list
                        .iter()
                        .filter_map(|pair| pair.get_item(0).ok().and_then(|k| k.extract().ok()))
                        .collect();
                    let mut target_sorted = target_keys.clone();
                    target_sorted.sort();
                    let mut idx_sorted = idx_keys;
                    idx_sorted.sort();
                    if target_sorted == idx_sorted {
                        let name: String = entry.get_item("name")?.extract()?;
                        coll_py.bind(py).borrow().drop_index(py, &name, false)?;
                        break;
                    }
                }
            }
        } else if idx.is_none() {
            let r = make_error(
                py,
                "InvalidOptions",
                "dropIndexes requires an 'index' parameter",
            )?;
            return Ok(r.into_any().unbind());
        }
    } else {
        let r = make_error(
            py,
            "InvalidOptions",
            "dropIndexes requires an 'index' parameter",
        )?;
        return Ok(r.into_any().unbind());
    }

    let resp = PyDict::new(py);
    resp.set_item("nIndexesWas", n_before)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_reindex(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("reIndex")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'reIndex'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let n: i64 = coll_py.bind(py).borrow().rebuild_all_indexes(py)?;
    let resp = PyDict::new(py);
    resp.set_item("nIndexesWas", n + 1)?;
    resp.set_item("nIndexes", n + 1)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

pub(crate) fn register(m: &mut HashMap<&'static str, HandlerFn>) {
    m.insert("listIndexes", cmd_list_indexes);
    m.insert("createIndexes", cmd_create_indexes);
    m.insert("dropIndexes", cmd_drop_indexes);
    m.insert("reIndex", cmd_reindex);
}
