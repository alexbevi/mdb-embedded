//! Wire protocol index management commands: `createIndexes`, `dropIndexes`,
//! `listIndexes`, `createSearchIndex`, `createSearchIndexes`, `listSearchIndexes`.
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
        if let Ok(opts) = idx.get_item("options") {
            if let Ok(opts_dict) = opts.cast::<PyDict>() {
                if let Ok(Some(pfe)) = opts_dict.get_item("partial_filter_expression") {
                    if !pfe.is_none() {
                        spec.set_item("partialFilterExpression", pfe)?;
                    }
                }
                if let Ok(Some(coll)) = opts_dict.get_item("collation") {
                    if !coll.is_none() {
                        spec.set_item("collation", coll)?;
                    }
                }
                if let Ok(Some(idx_type)) = opts_dict.get_item("index_type") {
                    if !idx_type.is_none() {
                        spec.set_item("type", idx_type)?;
                    }
                }
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

    let before: i64 = coll_py.bind(py).borrow().list_indexes(py)?.bind(py).len() as i64 + 1;

    let indexes = cmd
        .get_item("indexes")?
        .unwrap_or_else(|| PyList::empty(py).into_any());
    for idx_spec in indexes.try_iter()? {
        let idx_spec = idx_spec?;
        let key = idx_spec.get_item("key")?;
        let key_dict = key.cast::<PyDict>()?;
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
        if let Ok(pfe) = idx_spec.get_item("partialFilterExpression") {
            kwargs.set_item("partialFilterExpression", pfe)?;
        }
        if let Ok(coll) = idx_spec.get_item("collation") {
            kwargs.set_item("collation", coll)?;
        }
        if let Ok(vs_opts) = idx_spec.get_item("vectorSearchOptions") {
            kwargs.set_item("vectorSearchOptions", vs_opts)?;
        }
        if let Ok(idx_type) = idx_spec.get_item("type") {
            kwargs.set_item("type", idx_type)?;
        }
        if let Ok(weights) = idx_spec.get_item("weights") {
            kwargs.set_item("weights", weights)?;
        }
        if let Ok(prefix_len) = idx_spec.get_item("prefixLength") {
            kwargs.set_item("prefixLength", prefix_len)?;
        }
        coll_py
            .bind(py)
            .borrow()
            .create_index(py, key_dict.as_any(), Some(&kwargs), None)?;
    }

    let after: i64 = coll_py.bind(py).borrow().list_indexes(py)?.bind(py).len() as i64 + 1;

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

    let n_before: i64 = coll_py.bind(py).borrow().list_indexes(py)?.bind(py).len() as i64 + 1;
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
                    let _ = coll_py.bind(py).borrow().drop_index(&name);
                }
            } else {
                coll_py.bind(py).borrow().drop_index(&s)?;
            }
        } else if let Ok(list) = idx.cast::<PyList>() {
            for item in list.iter() {
                if let Ok(name) = item.extract::<String>() {
                    coll_py.bind(py).borrow().drop_index(&name)?;
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
                        coll_py.bind(py).borrow().drop_index(&name)?;
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

// ---------------------------------------------------------------------------
// createSearchIndex / createSearchIndexes — Atlas-style vector search index
// creation.  PyMongo's `Collection.create_search_index()` sends these.
//
// Atlas index definition:
// {
//   "name": "my_index",
//   "type": "vectorSearch",
//   "definition": {
//     "fields": [
//       {"type": "vector", "path": "embedding", "numDimensions": 768, "similarity": "cosine"},
//       {"type": "filter", "path": "tenant_id"}
//     ]
//   }
// }
//
// We translate this into our `createIndexes` format so the engine stores it
// as a regular vector index.
// ---------------------------------------------------------------------------

fn atlas_search_index_to_create_indexes(
    py: Python<'_>,
    coll_py: &Py<crate::redb_client::RedbLocalCollection>,
    index: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let name: String = index
        .get_item("name")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_else(|| "default".to_string());

    let definition = index
        .get_item("definition")?
        .ok_or_else(|| PyValueError::new_err("search index requires 'definition'"))?;
    let definition = definition.cast::<PyDict>()?;

    let fields = definition
        .get_item("fields")?
        .ok_or_else(|| PyValueError::new_err("search index definition requires 'fields'"))?;

    let mut vector_path: Option<String> = None;
    let mut num_dimensions: i64 = 0;
    let mut similarity = "cosine".to_string();
    let mut indexing_method = "hnsw".to_string();

    for field_any in fields.try_iter()? {
        let field_any = field_any?;
        let field = field_any.cast::<PyDict>()?;
        let field_type: String = field
            .get_item("type")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default();

        if field_type == "vector" {
            vector_path = field
                .get_item("path")?
                .and_then(|v| v.extract::<String>().ok());
            if let Some(nd) = field.get_item("numDimensions")? {
                num_dimensions = nd.extract::<i64>()?;
            }
            if let Some(sim) = field.get_item("similarity")? {
                similarity = sim.extract::<String>()?;
            }
            if let Some(im) = field.get_item("indexingMethod")? {
                indexing_method = im.extract::<String>()?;
            }
        }
    }

    let path = vector_path
        .ok_or_else(|| PyValueError::new_err("no vector field found in search index definition"))?;

    if num_dimensions <= 0 {
        return Err(PyValueError::new_err(
            "numDimensions must be a positive integer",
        ));
    }

    let key_dict = PyDict::new(py);
    key_dict.set_item(&path, "vectorSearch")?;

    let opts = PyDict::new(py);
    opts.set_item("name", &name)?;
    opts.set_item("unique", false)?;
    opts.set_item("sparse", false)?;
    opts.set_item("background", false)?;

    let vs_opts = PyDict::new(py);
    vs_opts.set_item("dimensions", num_dimensions)?;
    vs_opts.set_item("metric", &similarity)?;
    vs_opts.set_item("indexing_method", &indexing_method)?;
    opts.set_item("vectorSearchOptions", vs_opts)?;

    let type_str = "vectorSearch";
    opts.set_item("type", type_str)?;

    coll_py
        .bind(py)
        .borrow()
        .create_index(py, key_dict.as_any(), Some(&opts.as_borrowed()), None)?;

    Ok(name)
}

fn cmd_create_search_index(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("createSearchIndex")?
        .ok_or_else(|| PyValueError::new_err("missing 'createSearchIndex'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;

    let index = cmd
        .get_item("index")?
        .ok_or_else(|| PyValueError::new_err("missing 'index'"))?;
    let index = index.cast::<PyDict>()?;
    let name = atlas_search_index_to_create_indexes(py, &coll_py, index)?;

    let resp = PyDict::new(py);
    resp.set_item("indexName", name)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_create_search_indexes(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("createSearchIndexes")?
        .ok_or_else(|| PyValueError::new_err("missing 'createSearchIndexes'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;

    let indexes = cmd
        .get_item("indexes")?
        .ok_or_else(|| PyValueError::new_err("missing 'indexes'"))?;

    let names = PyList::empty(py);
    for idx_any in indexes.try_iter()? {
        let idx_any = idx_any?;
        let idx = idx_any.cast::<PyDict>()?;
        let name = atlas_search_index_to_create_indexes(py, &coll_py, idx)?;
        let entry = PyDict::new(py);
        entry.set_item("name", &name)?;
        names.append(entry)?;
    }

    let resp = PyDict::new(py);
    resp.set_item("indexesCreated", names)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_list_search_indexes(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("listSearchIndexes")?
        .ok_or_else(|| PyValueError::new_err("missing 'listSearchIndexes'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let ns = format!("{db_name}.{coll_name}");

    let indexes_py = coll_py.bind(py).borrow().list_indexes(py)?;
    let indexes = indexes_py.bind(py);
    let results = PyList::empty(py);

    for idx_any in indexes.try_iter()? {
        let idx_any = idx_any?;
        let idx_dict = idx_any.cast::<PyDict>()?;

        let idx_type: String = idx_dict
            .get_item("type")?
            .map(|v| v.extract::<String>().unwrap_or_default())
            .unwrap_or_default();
        if idx_type != "vectorSearch" {
            continue;
        }
        let entry = PyDict::new(py);
        if let Some(name) = idx_dict.get_item("name")? {
            entry.set_item("id", &name)?;
            entry.set_item("name", &name)?;
        }
        entry.set_item("type", "vectorSearch")?;
        entry.set_item("status", "READY")?;
        entry.set_item("queryable", true)?;
        if let Some(vs_opts) = idx_dict.get_item("vectorSearchOptions")? {
            let defn = PyDict::new(py);
            let fields = PyList::empty(py);
            let field = PyDict::new(py);
            field.set_item("type", "vector")?;
            if let Some(keys) = idx_dict.get_item("key")? {
                if let Ok(keys_dict) = keys.cast::<PyDict>() {
                    if let Some((k, _)) = keys_dict.iter().next() {
                        field.set_item("path", k)?;
                    }
                }
            }
            if let Ok(d) = vs_opts.get_item("dimensions") {
                field.set_item("numDimensions", d)?;
            }
            if let Ok(m) = vs_opts.get_item("metric") {
                field.set_item("similarity", m)?;
            }
            fields.append(field)?;
            defn.set_item("fields", fields)?;
            entry.set_item("latestDefinition", defn)?;
        }
        results.append(entry)?;
    }

    let cursor_dict = PyDict::new(py);
    cursor_dict.set_item("firstBatch", results)?;
    cursor_dict.set_item("id", bson_int64(py, 0)?)?;
    cursor_dict.set_item("ns", ns)?;

    let resp = PyDict::new(py);
    resp.set_item("cursor", cursor_dict)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

pub(crate) fn register(m: &mut HashMap<&'static str, HandlerFn>) {
    m.insert("listIndexes", cmd_list_indexes);
    m.insert("createIndexes", cmd_create_indexes);
    m.insert("dropIndexes", cmd_drop_indexes);
    m.insert("reIndex", cmd_reindex);
    m.insert("createSearchIndex", cmd_create_search_index);
    m.insert("createSearchIndexes", cmd_create_search_indexes);
    m.insert("listSearchIndexes", cmd_list_search_indexes);
}
