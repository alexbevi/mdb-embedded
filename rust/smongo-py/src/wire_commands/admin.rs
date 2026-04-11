//! Administrative wire commands: `drop`, `create`, `listCollections`, `renameCollection`, etc.
use std::collections::HashMap;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::redb_client::RedbLocalDB;
use crate::wire_context::ConnectionContext;
use crate::wire_cursors::CursorRegistry;
use crate::wire_errors::{error_response, make_error};

use super::{
    bson_int64, dict_get_bool, dict_get_i64, dict_get_str, get_collection, get_collection_typed,
    get_db, ok_dict, HandlerFn,
};

fn resolve_user_store(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    let ctx_ref = ctx.borrow();
    let cached = ctx_ref.cached_imports()?;
    Ok((
        cached.user_store.clone_ref(py),
        cached.user_store_lock.clone_ref(py),
    ))
}

fn cmd_get_log(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let log_type = dict_get_str(cmd, "getLog", "global")?;
    if log_type == "global" || log_type == "*" {
        let log_buffer = ctx.borrow().log_buffer.clone_ref(py);
        let log_buffer = log_buffer.into_bound(py);
        let result = log_buffer.call_method0("get_lines")?;
        let lines = result.get_item(0)?;
        let total = result.get_item(1)?;
        let resp = PyDict::new(py);
        resp.set_item("totalLinesWritten", total)?;
        resp.set_item("log", lines)?;
        resp.set_item("ok", 1.0)?;
        return Ok(resp.into_any().unbind());
    }
    if log_type == "startupWarnings" {
        let resp = PyDict::new(py);
        resp.set_item("totalLinesWritten", 0)?;
        resp.set_item("log", PyList::empty(py))?;
        resp.set_item("ok", 1.0)?;
        return Ok(resp.into_any().unbind());
    }
    let r = make_error(
        py,
        "InvalidOptions",
        &format!("unknown getLog type: {log_type}"),
    )?;
    Ok(r.into_any().unbind())
}

fn cmd_free_monitoring(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let fm = ctx.borrow().free_monitoring.clone_ref(py);
    let state = fm.bind(py).getattr("state")?;
    let resp = PyDict::new(py);
    resp.set_item("state", state)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_cmdline_opts(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let sys_mod = crate::cached_modules::sys_mod(py)?;
    let argv = sys_mod.getattr("argv")?;
    let resp = PyDict::new(py);
    resp.set_item("argv", argv)?;
    resp.set_item("parsed", PyDict::new(py))?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_list_databases(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    use std::collections::BTreeSet;

    let seen_list = ctx.call_method0("list_known_dbs")?;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for item in seen_list.try_iter()? {
        if let Ok(s) = item?.extract::<String>() {
            seen.insert(s);
        }
    }

    if seen.is_empty() {
        seen.insert("test".to_string());
    }

    let name_only = dict_get_bool(cmd, "nameOnly", false)?;
    let sorted_dbs = PyList::new(py, seen.iter())?;

    let mut total_size: i64 = 0;
    let databases = PyList::empty(py);

    for db_item in sorted_dbs.try_iter()? {
        let db_name: String = db_item?.extract()?;
        let entry = PyDict::new(py);
        entry.set_item("name", &db_name)?;

        if !name_only {
            let mut db_size: i64 = 0;
            let result: PyResult<()> = (|| {
                let db = get_db(ctx, &db_name)?;
                let db_ref = db.cast::<RedbLocalDB>()?;
                let coll_names = db_ref.borrow().list_collection_names()?;
                for cn in &coll_names {
                    let result: PyResult<()> = (|| {
                        let coll_py = db_ref.borrow().get_collection_typed(py, cn)?;
                        let stats_py = coll_py.bind(py).borrow().storage_stats(py)?;
                        let stats = stats_py.bind(py);
                        let storage: i64 = stats
                            .get_item("storageSize")?
                            .map(|v| v.extract::<i64>().unwrap_or(0))
                            .unwrap_or(0);
                        let data: i64 = stats
                            .get_item("dataSize")?
                            .map(|v| v.extract::<i64>().unwrap_or(0))
                            .unwrap_or(0);
                        db_size += storage + data;
                        Ok(())
                    })();
                    let _ = result;
                }
                Ok(())
            })();
            let _ = result;
            total_size += db_size;
            entry.set_item("sizeOnDisk", db_size)?;
            entry.set_item("empty", db_size == 0)?;
        }
        databases.append(entry)?;
    }

    let resp = PyDict::new(py);
    resp.set_item("databases", databases)?;
    if !name_only {
        resp.set_item("totalSize", total_size)?;
    }
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_list_collections(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let db = get_db(ctx, &db_name)?;
    let db_ref = db.cast::<RedbLocalDB>()?;
    let coll_names = db_ref.borrow().list_collection_names()?;
    let name_only = dict_get_bool(cmd, "nameOnly", false)?;
    let filter_doc = cmd.get_item("filter")?;

    let uuid_mod = crate::cached_modules::uuid_mod(py)?;
    let uuid5 = uuid_mod.getattr("uuid5")?;
    let ns_dns = uuid_mod.getattr("NAMESPACE_DNS")?;
    let binary_cls = crate::cached_modules::bson_mod(py)?.getattr("Binary")?;

    let result = PyList::empty(py);
    for name in &coll_names {
        let entry = PyDict::new(py);
        entry.set_item("name", name)?;
        entry.set_item("type", "collection")?;

        if !name_only {
            entry.set_item("options", PyDict::new(py))?;
            let info = PyDict::new(py);
            info.set_item("readOnly", false)?;
            let ns_str = format!("{db_name}.{name}");
            let uid = uuid5.call1((&ns_dns, &ns_str))?;
            let uid_bytes = uid.getattr("bytes")?;
            let binary = binary_cls.call1((uid_bytes, 4))?;
            info.set_item("uuid", binary)?;
            entry.set_item("info", info)?;
        }

        if let Some(ref fd) = filter_doc {
            if fd.is_truthy()? {
                let fd_dict = fd.cast::<PyDict>()?;
                let mut matches = true;
                for (fk, fv) in fd_dict.iter() {
                    let ev = entry.get_item(fk.extract::<String>()?)?;
                    if let Some(ev) = ev {
                        if !ev.eq(&fv)? {
                            matches = false;
                            break;
                        }
                    } else {
                        matches = false;
                        break;
                    }
                }
                if !matches {
                    continue;
                }
            }
        }
        result.append(entry)?;
    }

    let ns = format!("{db_name}.$cmd.listCollections");
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
    let (cursor_id, first_batch) =
        cr_reg
            .borrow()
            .create(py, &ns, &result, Some(batch_size as usize))?;

    let cursor_dict = PyDict::new(py);
    cursor_dict.set_item("id", bson_int64(py, cursor_id)?)?;
    cursor_dict.set_item("ns", &ns)?;
    cursor_dict.set_item("firstBatch", first_batch)?;
    let resp = PyDict::new(py);
    resp.set_item("cursor", cursor_dict)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_create_collection(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("create")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'create'"))?
        .extract()?;
    let validator = cmd.get_item("validator")?;
    let db = get_db(ctx, &db_name)?;
    let kwargs = PyDict::new(py);
    if let Some(v) = validator {
        kwargs.set_item("validator", v)?;
    }
    db.call_method("create_collection", (&coll_name,), Some(&kwargs))?;
    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_drop(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("drop")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'drop'"))?
        .extract()?;
    let mut n_indexes_was: i64 = 1;

    let _: PyResult<()> = (|| {
        let db = get_db(ctx, &db_name)?;
        let db_ref = db.cast::<RedbLocalDB>()?;
        let coll_py = db_ref
            .borrow()
            .get_collection_typed(py, coll_name.as_str())?;
        let list_py = coll_py.bind(py).borrow().list_indexes(py)?;
        let list_bound = list_py.bind(py);
        n_indexes_was = list_bound.len() as i64 + 1;
        let indexes: Vec<Bound<'_, PyAny>> = list_bound.try_iter()?.collect::<PyResult<_>>()?;
        for idx in indexes {
            let name: String = idx.get_item("name")?.extract()?;
            let _ = coll_py.bind(py).borrow().drop_index(&name);
        }
        let empty = PyDict::new(py);
        coll_py.bind(py).borrow().delete_many(py, &empty, false)?;
        db.call_method1("drop_collection", (&coll_name,))?;
        Ok(())
    })();

    let resp = PyDict::new(py);
    resp.set_item("ns", format!("{db_name}.{coll_name}"))?;
    resp.set_item("nIndexesWas", n_indexes_was)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_drop_database(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;

    let db = get_db(ctx, &db_name)?;
    let db_ref = db.cast::<RedbLocalDB>()?;
    let names = db_ref.borrow().list_collection_names()?;
    for coll_name in names {
        db_ref.borrow().drop_collection(py, &coll_name)?;
    }

    let resp = PyDict::new(py);
    resp.set_item("dropped", &db_name)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_coll_mod(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("collMod")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'collMod'"))?
        .extract()?;
    let coll = get_collection(ctx, &db_name, &coll_name)?;

    if let Some(validator) = cmd.get_item("validator")? {
        let schema = if let Ok(vd) = validator.cast::<PyDict>() {
            if let Some(js) = vd.get_item("$jsonSchema")? {
                js
            } else {
                validator.clone()
            }
        } else {
            validator.clone()
        };
        coll.setattr("validator", schema)?;
    }

    if let Some(level) = cmd.get_item("validationLevel")? {
        if let Ok(level_str) = level.extract::<String>() {
            if level_str == "off" {
                coll.setattr("validator", py.None())?;
            }
        }
    }

    if let Some(idx_spec) = cmd.get_item("index")? {
        if let Ok(idx_dict) = idx_spec.cast::<PyDict>() {
            let has_key_pattern = idx_dict.get_item("keyPattern")?.is_some();
            let has_name = idx_dict.get_item("name")?.is_some();
            if has_key_pattern || has_name {
                let target_name = if has_name {
                    idx_dict
                        .get_item("name")?
                        .and_then(|v| v.extract::<String>().ok())
                } else {
                    None
                };
                let target_name = if target_name.is_none() && has_key_pattern {
                    let kp = idx_dict.get_item("keyPattern")?.ok_or_else(|| {
                        PyValueError::new_err("missing required field 'keyPattern'")
                    })?;
                    let indexes = coll.call_method0("list_indexes")?;
                    let mut found_name = None;
                    for idx in indexes.try_iter()? {
                        let idx = idx?;
                        let idx_keys = PyDict::new(py);
                        let keys_raw = idx.get_item("keys")?;
                        if let Ok(kl) = keys_raw.cast::<PyList>() {
                            for pair in kl.iter() {
                                idx_keys.set_item(pair.get_item(0)?, pair.get_item(1)?)?;
                            }
                        }
                        if idx_keys.eq(&kp)? {
                            found_name = Some(idx.get_item("name")?.extract::<String>()?);
                            break;
                        }
                    }
                    found_name
                } else {
                    target_name
                };

                if let Some(name) = target_name {
                    if let Some(eas) = idx_dict.get_item("expireAfterSeconds")? {
                        let idx_mgr = coll.getattr("index_mgr")?;
                        let indexes = idx_mgr.getattr("_indexes")?;
                        if let Ok(Some(idx_def)) = indexes.call_method1("get", (&name,)).map(|v| {
                            if v.is_none() {
                                None
                            } else {
                                Some(v)
                            }
                        }) {
                            idx_def.setattr("expire_after_seconds", eas)?;
                        }
                    }
                }
            }
        }
    }

    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_rename_collection(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let src_ns: String = cmd
        .get_item("renameCollection")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'renameCollection'"))?
        .extract()?;
    let dst_ns = dict_get_str(cmd, "to", "")?;
    let drop_target = dict_get_bool(cmd, "dropTarget", false)?;

    if !src_ns.contains('.') || !dst_ns.contains('.') {
        let r = make_error(py, "InvalidNamespace", "invalid namespace for rename")?;
        return Ok(r.into_any().unbind());
    }

    let (src_db, src_coll) = src_ns
        .split_once('.')
        .ok_or_else(|| PyValueError::new_err("invalid source namespace for renameCollection"))?;
    let (dst_db, dst_coll) = dst_ns.split_once('.').ok_or_else(|| {
        PyValueError::new_err("invalid destination namespace for renameCollection")
    })?;

    let src_collection = get_collection(ctx, src_db, src_coll)?;
    let docs = src_collection.call_method0("get_all")?;

    let dst_collection = get_collection(ctx, dst_db, dst_coll)?;
    let existing = dst_collection.call_method0("get_all")?;
    if existing.is_truthy()? && !drop_target {
        let r = make_error(
            py,
            "NamespaceExists",
            &format!("target namespace {dst_ns} already exists"),
        )?;
        return Ok(r.into_any().unbind());
    }
    if existing.is_truthy()? && drop_target {
        let empty = PyDict::new(py);
        dst_collection.call_method1("delete_many", (empty,))?;
    }

    for doc in docs.try_iter()? {
        let doc = doc?;
        dst_collection.call_method1("insert_one", (&doc,))?;
    }

    let empty = PyDict::new(py);
    src_collection.call_method1("delete_many", (empty,))?;
    let src_dbobj = get_db(ctx, src_db)?;
    src_dbobj.call_method1("drop_collection", (src_coll,))?;

    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_compact(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("compact")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'compact'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let before: i64 = {
        let stats = coll_py.bind(py).borrow().storage_stats(py)?;
        stats
            .bind(py)
            .get_item("storageSize")?
            .map(|v| v.extract().unwrap_or(0))
            .unwrap_or(0)
    };
    coll_py.bind(py).as_any().call_method0("compact")?;
    let after: i64 = {
        let stats = coll_py.bind(py).borrow().storage_stats(py)?;
        stats
            .bind(py)
            .get_item("storageSize")?
            .map(|v| v.extract().unwrap_or(0))
            .unwrap_or(0)
    };
    let freed = (before - after).max(0);
    let resp = PyDict::new(py);
    resp.set_item("bytesFreed", freed)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_coll_stats(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("collStats")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'collStats'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let stats = coll_py.bind(py).borrow().storage_stats(py)?;
    let stats = stats.bind(py);
    let count: i64 = stats
        .get_item("count")?
        .map(|v| v.extract().unwrap_or(0))
        .unwrap_or(0);
    let data_size: i64 = stats
        .get_item("dataSize")?
        .map(|v| v.extract().unwrap_or(0))
        .unwrap_or(0);
    let resp = PyDict::new(py);
    resp.set_item("ns", format!("{db_name}.{coll_name}"))?;
    resp.set_item("count", count)?;
    resp.set_item("size", data_size)?;
    resp.set_item("avgObjSize", if count > 0 { data_size / count } else { 0 })?;
    resp.set_item("storageSize", stats.get_item("storageSize")?)?;
    resp.set_item("nindexes", stats.get_item("nindexes")?)?;
    resp.set_item("totalIndexSize", stats.get_item("totalIndexSize")?)?;
    resp.set_item("indexSizes", stats.get_item("indexSizes")?)?;
    let se = stats.get_item("storageEngine")?.unwrap_or_else(|| {
        let d = PyDict::new(py);
        let _ = d.set_item("name", "redb");
        d.into_any()
    });
    resp.set_item("storageEngine", se)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_db_stats(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let db = get_db(ctx, &db_name)?;

    let mut total_objects: i64 = 0;
    let mut total_data_size: i64 = 0;
    let mut total_storage_size: i64 = 0;
    let mut total_indexes: i64 = 0;
    let mut total_index_size: i64 = 0;
    let mut coll_count: i64 = 0;

    let db_ref = db.cast::<RedbLocalDB>()?;
    let coll_names = db_ref.borrow().list_collection_names()?;
    for cn in &coll_names {
        let result: PyResult<()> = (|| {
            let coll_py = db_ref.borrow().get_collection_typed(py, cn)?;
            let stats_py = coll_py.bind(py).borrow().storage_stats(py)?;
            let stats = stats_py.bind(py);
            total_objects += stats
                .get_item("count")?
                .map(|v| v.extract::<i64>())
                .transpose()?
                .unwrap_or(0);
            total_data_size += stats
                .get_item("dataSize")?
                .map(|v| v.extract::<i64>())
                .transpose()?
                .unwrap_or(0);
            total_storage_size += stats
                .get_item("storageSize")?
                .map(|v| v.extract::<i64>())
                .transpose()?
                .unwrap_or(0);
            total_indexes += stats
                .get_item("nindexes")?
                .map(|v| v.extract::<i64>())
                .transpose()?
                .unwrap_or(0);
            total_index_size += stats
                .get_item("totalIndexSize")?
                .map(|v| v.extract::<i64>())
                .transpose()?
                .unwrap_or(0);
            coll_count += 1;
            Ok(())
        })();
        let _ = result;
    }

    let resp = PyDict::new(py);
    resp.set_item("db", &db_name)?;
    resp.set_item("collections", coll_count)?;
    resp.set_item("objects", total_objects)?;
    resp.set_item(
        "avgObjSize",
        if total_objects > 0 {
            total_data_size / total_objects
        } else {
            0
        },
    )?;
    resp.set_item("dataSize", total_data_size)?;
    resp.set_item("storageSize", total_storage_size)?;
    resp.set_item("indexes", total_indexes)?;
    resp.set_item("indexSize", total_index_size)?;
    resp.set_item("scaleFactor", 1)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_validate(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("validate")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'validate'"))?
        .extract()?;
    let coll = get_collection(ctx, &db_name, &coll_name)?;
    let result = coll.call_method0("verify")?;
    let resp = PyDict::new(py);
    resp.set_item("ns", format!("{db_name}.{coll_name}"))?;
    resp.set_item("nrecords", result.get_item("nrecords")?)?;
    resp.set_item("nIndexes", result.get_item("nIndexes")?)?;
    resp.set_item("valid", result.get_item("valid")?)?;
    resp.set_item("errors", result.get_item("errors")?)?;
    resp.set_item("warnings", result.get_item("warnings")?)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_server_status(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let time_mod = crate::cached_modules::time_mod(py)?;
    let server_start: f64 = ctx.borrow().cached_imports()?.server_start;
    let now: f64 = time_mod.call_method0("time")?.extract()?;
    let uptime = now - server_start;

    let counters = crate::wire_dispatch::get_opcounters(py)?;

    let si = crate::cached_modules::system_info(py)?;
    let resource_mod = crate::cached_modules::resource_mod(py)?;
    let utc = crate::cached_modules::datetime_tz_utc(py)?;
    let local_time =
        crate::cached_modules::datetime_datetime_cls(py)?.call_method1("now", (&utc,))?;

    let rss_mb: f64 = (|| -> PyResult<f64> {
        let rusage =
            resource_mod.call_method1("getrusage", (resource_mod.getattr("RUSAGE_SELF")?,))?;
        let rss: f64 = rusage.getattr("ru_maxrss")?.extract()?;
        if si.system == "Darwin" {
            Ok(rss / (1024.0 * 1024.0))
        } else {
            Ok(rss / 1024.0)
        }
    })()
    .unwrap_or(0.0);

    let conn_meta: Bound<'_, PyDict> = (|| -> PyResult<Bound<'_, PyDict>> {
        let lc = ctx.borrow().local_client_typed(py)?;
        Ok(lc.bind(py).borrow().connection_stats(py)?.into_bound(py))
    })()
    .unwrap_or_else(|_| PyDict::new(py));

    let ctx_mod = crate::cached_modules::smongo_wire_context(py)?;
    let virt_mb = ctx_mod.getattr("get_virtual_memory_mb")?.call0()?;

    let address = ctx.borrow().address.clone_ref(py);
    let address = address.into_bound(py);
    let host = if !si.node.is_empty() {
        si.node.clone()
    } else {
        address.get_item(0)?.extract().unwrap_or_default()
    };

    let conn_counter = ctx.borrow().conn_counter.clone_ref(py);
    let conn_snap = conn_counter.bind(py).call_method0("snapshot")?;
    let sr = ctx.borrow().session_registry.clone_ref(py);
    let session_count = sr.bind(py).getattr("count")?;

    let resp = PyDict::new(py);
    resp.set_item("host", host)?;
    resp.set_item("version", "7.0.0-smongo")?;
    resp.set_item("process", "smongo")?;
    resp.set_item("pid", crate::cached_modules::cached_pid(py)?)?;
    resp.set_item("uptime", uptime)?;
    resp.set_item("uptimeMillis", (uptime * 1000.0) as i64)?;
    resp.set_item("uptimeEstimate", uptime as i64)?;
    resp.set_item("localTime", local_time)?;
    resp.set_item("connections", conn_snap)?;
    resp.set_item("opcounters", counters)?;

    let mem = PyDict::new(py);
    mem.set_item("bits", 64)?;
    mem.set_item("resident", rss_mb as i64)?;
    mem.set_item("virtual", virt_mb)?;
    mem.set_item("supported", true)?;
    mem.set_item("note", "virtual memory reported via OS process stats")?;
    resp.set_item("mem", mem)?;

    let lsrc = PyDict::new(py);
    lsrc.set_item("activeSessionsCount", session_count)?;
    resp.set_item("logicalSessionRecordCache", lsrc)?;

    let storage_engine = PyDict::new(py);
    storage_engine.set_item("name", "redb")?;
    if let Ok(uri) = conn_meta.get_item("uri") {
        storage_engine.set_item("uri", uri)?;
    }
    if let Ok(eng) = conn_meta.get_item("engine") {
        storage_engine.set_item("engine", eng)?;
    }
    resp.set_item("storageEngine", storage_engine)?;
    resp.set_item("ok", 1.0)?;

    let sync_mgr = ctx.borrow().sync_mgr.clone_ref(py);
    let sync_mgr = sync_mgr.into_bound(py);
    if !sync_mgr.is_none() {
        let status_result: PyResult<()> = (|| {
            let status = sync_mgr.call_method0("status")?;
            resp.set_item("sync", status)?;
            Ok(())
        })();
        let _ = status_result;
    }

    Ok(resp.into_any().unbind())
}

fn cmd_fsync(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let lock = dict_get_bool(cmd, "lock", false)?;
    let is_async = dict_get_bool(cmd, "async", false)?;

    let tables_flushed: i64 = 1;

    let resp = PyDict::new(py);
    resp.set_item("numFiles", tables_flushed)?;
    resp.set_item("ok", 1.0)?;
    if lock {
        resp.set_item("lockCount", 1)?;
        resp.set_item("info", "fsync with lock is advisory only in embedded mode")?;
    }
    if is_async {
        resp.set_item("async", true)?;
    }
    Ok(resp.into_any().unbind())
}

fn cmd_getnonce(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let secrets_mod = crate::cached_modules::secrets_mod(py)?;
    let nonce = secrets_mod.call_method1("token_hex", (8,))?;
    let resp = PyDict::new(py);
    resp.set_item("nonce", nonce)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_get_parameter(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let param = cmd.get_item("getParameter")?;
    let all_params = dict_get_bool(cmd, "allParameters", false)?;

    let ps = ctx.borrow().param_store.clone_ref(py);
    let ps = ps.into_bound(py);

    if all_params {
        let all = ps.call_method0("get_all")?;
        let resp = all.cast::<PyDict>()?.copy()?;
        resp.set_item("ok", 1.0)?;
        return Ok(resp.into_any().unbind());
    }

    if let Some(ref p) = param {
        if let Ok(s) = p.extract::<String>() {
            if s == "*" {
                let all = ps.call_method0("get_all")?;
                let resp = all.cast::<PyDict>()?.copy()?;
                resp.set_item("ok", 1.0)?;
                return Ok(resp.into_any().unbind());
            }
            let val = ps.call_method1("get", (&s,))?;
            if !val.is_none() {
                let resp = PyDict::new(py);
                resp.set_item(&s, val)?;
                resp.set_item("ok", 1.0)?;
                return Ok(resp.into_any().unbind());
            }
            let r = make_error(py, "InvalidOptions", &format!("no such parameter: {s:?}"))?;
            return Ok(r.into_any().unbind());
        }
    }

    let r = make_error(
        py,
        "InvalidOptions",
        "getParameter requires a string parameter name or '*'",
    )?;
    Ok(r.into_any().unbind())
}

fn cmd_set_parameter(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let ps = ctx.borrow().param_store.clone_ref(py);
    let ps = ps.into_bound(py);
    let skip_keys = ["setParameter", "$db", "lsid", "txnNumber", "$clusterTime"];
    let changed = PyDict::new(py);
    for (key, val) in cmd.iter() {
        let key_str: String = key.extract()?;
        if skip_keys.contains(&key_str.as_str()) {
            continue;
        }
        let old = ps.call_method1("get", (&key_str,))?;
        ps.call_method1("set", (&key_str, &val))?;
        let entry = PyDict::new(py);
        entry.set_item("was", old)?;
        entry.set_item("now", val)?;
        changed.set_item(&key_str, entry)?;
    }
    let resp = PyDict::new(py);
    resp.set_item("was", changed)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_conn_pool_stats(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let cc = ctx.borrow().conn_counter.clone_ref(py);
    let snap = cc.bind(py).call_method0("snapshot")?;
    let current: i64 = snap.get_item("current")?.extract()?;
    let available: i64 = snap.get_item("available")?.extract()?;
    let total_created: i64 = snap.get_item("totalCreated")?.extract()?;
    let resp = PyDict::new(py);
    resp.set_item("numClientConnections", current)?;
    resp.set_item("numAScopedConnections", 0)?;
    resp.set_item("totalInUse", current)?;
    resp.set_item("totalAvailable", available)?;
    resp.set_item("totalCreated", total_created)?;
    resp.set_item("totalRefreshing", 0)?;
    resp.set_item("pools", PyDict::new(py))?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_features(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let pid = crate::cached_modules::cached_pid(py)?;
    let resp = PyDict::new(py);
    resp.set_item("oidMachine", pid)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_log_rotate(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let logging = crate::cached_modules::logging_mod(py)?;
    let root = logging.getattr("root")?;
    let handlers = root.getattr("handlers")?;
    for handler in handlers.try_iter()? {
        let handler = handler?;
        if handler.hasattr("doRollover")? {
            handler.call_method0("doRollover")?;
        }
    }
    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_sharding_state(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let resp = PyDict::new(py);
    resp.set_item("enabled", false)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_repl_get_config(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let r = make_error(
        py,
        "NotPrimaryOrSecondary",
        "smongo is standalone, not a replica set member",
    )?;
    Ok(r.into_any().unbind())
}

fn cmd_repl_status(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let sync_mgr = ctx.borrow().sync_mgr.clone_ref(py);
    let sync_mgr = sync_mgr.into_bound(py);
    if !sync_mgr.is_none() {
        let result: PyResult<Py<PyAny>> = (|| {
            let status = sync_mgr.call_method0("status")?;
            let member = PyDict::new(py);
            member.set_item("_id", 0)?;
            member.set_item("name", "localhost")?;
            member.set_item("state", 1)?;
            member.set_item("stateStr", "PRIMARY")?;
            let members = PyList::new(py, [member])?;
            let resp = PyDict::new(py);
            resp.set_item("set", "smongo")?;
            resp.set_item("members", members)?;
            resp.set_item("sync", status)?;
            resp.set_item("ok", 1.0)?;
            Ok(resp.into_any().unbind())
        })();
        if let Ok(r) = result {
            return Ok(r);
        }
    }
    let r = make_error(
        py,
        "NotPrimaryOrSecondary",
        "replSetGetStatus requires sync to be configured",
    )?;
    Ok(r.into_any().unbind())
}

fn cmd_set_free_monitoring(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let action = dict_get_str(cmd, "action", "")?;
    if action != "enable" && action != "disable" {
        let r = make_error(py, "InvalidOptions", &format!("invalid action: {action:?}"))?;
        return Ok(r.into_any().unbind());
    }
    let fm = ctx.borrow().free_monitoring.clone_ref(py);
    fm.bind(py).call_method1("set", (&action,))?;
    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_lock_info(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let tracker = ctx.borrow().op_tracker.clone_ref(py);
    let ops = tracker.bind(py).call_method0("active_ops")?;
    let lock_entries = PyList::empty(py);
    for o in ops.cast::<PyList>()?.iter() {
        let ns = o.get_item("ns")?;
        let op_str: String = o.get_item("op")?.extract()?;
        let mode = if op_str == "query" || op_str == "getmore" {
            "IS"
        } else {
            "IX"
        };
        let granted = PyList::empty(py);
        let gd = PyDict::new(py);
        gd.set_item("mode", mode)?;
        granted.append(gd)?;
        let entry = PyDict::new(py);
        entry.set_item("resourceId", ns)?;
        entry.set_item("granted", granted)?;
        entry.set_item("pending", PyList::empty(py))?;
        lock_entries.append(entry)?;
    }
    let resp = PyDict::new(py);
    resp.set_item("lockInfo", lock_entries)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_list_commands(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (help_dict, handlers) = {
        let ctx_ref = ctx.borrow();
        let cached = ctx_ref.cached_imports()?;
        (
            cached.help_dict.clone_ref(py),
            cached.handlers.clone_ref(py),
        )
    };
    let help_dict = help_dict.into_bound(py);
    let handlers = handlers.into_bound(py);

    let admin_commands: Vec<&str> = vec![
        "fsync",
        "serverStatus",
        "hostInfo",
        "top",
        "logRotate",
        "getParameter",
        "setParameter",
        "currentOp",
        "killOp",
        "listDatabases",
        "replSetGetStatus",
        "replSetGetConfig",
        "shardingState",
        "connPoolStats",
        "getCmdLineOpts",
    ];

    let commands = PyDict::new(py);
    let handlers_dict = handlers.cast::<PyDict>()?;
    for (name, _handler) in handlers_dict.iter() {
        let name_str: String = name.extract()?;
        let help_text = help_dict
            .get_item(&name_str)
            .ok()
            .and_then(|v| {
                if v.is_none() {
                    None
                } else {
                    v.extract::<String>().ok()
                }
            })
            .unwrap_or_default();
        let entry = PyDict::new(py);
        entry.set_item("help", help_text)?;
        entry.set_item("adminOnly", admin_commands.contains(&name_str.as_str()))?;
        entry.set_item("slaveOk", true)?;
        commands.set_item(name, entry)?;
    }

    let resp = PyDict::new(py);
    resp.set_item("commands", commands)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_client_sync(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let sync_mgr = ctx.borrow().sync_mgr.clone_ref(py);
    let sync_mgr = sync_mgr.into_bound(py);
    if !sync_mgr.is_none() {
        let result: PyResult<Py<PyAny>> = (|| {
            let status = sync_mgr.call_method0("status")?;
            let resp = PyDict::new(py);
            resp.set_item("sync", status)?;
            resp.set_item("ok", 1.0)?;
            Ok(resp.into_any().unbind())
        })();
        match result {
            Ok(r) => return Ok(r),
            Err(e) => {
                let msg = e.value(py).str()?.to_string();
                let r = error_response(py, 1, "InternalError", &msg)?;
                return Ok(r.into_any().unbind());
            }
        }
    }
    let resp = PyDict::new(py);
    resp.set_item("sync", py.None())?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_users_info(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (store_py, lock_py) = resolve_user_store(py, ctx)?;
    let store = store_py.bind(py);
    let lock = lock_py.bind(py);

    let db_name = dict_get_str(cmd, "$db", "test")?;
    let target = cmd.get_item("usersInfo")?;

    let _guard = lock.call_method1("__enter__", ())?;
    let result = (|| -> PyResult<Py<PyAny>> {
        if let Some(ref t) = target {
            if let Ok(s) = t.extract::<String>() {
                let key = format!("{db_name}.{s}");
                let user = store.call_method1("get", (&key,))?;
                let users = if user.is_none() {
                    PyList::empty(py)
                } else {
                    PyList::new(py, [user])?
                };
                let resp = PyDict::new(py);
                resp.set_item("users", users)?;
                resp.set_item("ok", 1.0)?;
                return Ok(resp.into_any().unbind());
            }
            if let Ok(d) = t.cast::<PyDict>() {
                let u = d
                    .get_item("user")?
                    .map(|v| v.extract::<String>().unwrap_or_default())
                    .unwrap_or_default();
                let d_name = d
                    .get_item("db")?
                    .map(|v| v.extract::<String>().unwrap_or(db_name.clone()))
                    .unwrap_or(db_name.clone());
                let key = format!("{d_name}.{u}");
                let user = store.call_method1("get", (&key,))?;
                let users = if user.is_none() {
                    PyList::empty(py)
                } else {
                    PyList::new(py, [user])?
                };
                let resp = PyDict::new(py);
                resp.set_item("users", users)?;
                resp.set_item("ok", 1.0)?;
                return Ok(resp.into_any().unbind());
            }
            if t.extract::<i64>().map(|v| v == 1).unwrap_or(false) || t.is_truthy()? {
                let prefix = format!("{db_name}.");
                let users = PyList::empty(py);
                let store_items = store.call_method0("items")?;
                for item in store_items.try_iter()? {
                    let item = item?;
                    let k: String = item.get_item(0)?.extract()?;
                    if k.starts_with(&prefix) {
                        users.append(item.get_item(1)?)?;
                    }
                }
                let resp = PyDict::new(py);
                resp.set_item("users", users)?;
                resp.set_item("ok", 1.0)?;
                return Ok(resp.into_any().unbind());
            }
        }
        let values = store.call_method0("values")?;
        let list_fn = crate::cached_modules::builtins(py)?.getattr("list")?;
        let users = list_fn.call1((values,))?;
        let resp = PyDict::new(py);
        resp.set_item("users", users)?;
        resp.set_item("ok", 1.0)?;
        Ok(resp.into_any().unbind())
    })();
    lock.call_method1("__exit__", (py.None(), py.None(), py.None()))?;
    result
}

fn cmd_roles_info(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let builtin_roles = PyList::empty(py);
    for (role_name, db) in [
        ("read", "admin"),
        ("readWrite", "admin"),
        ("dbAdmin", "admin"),
        ("dbOwner", "admin"),
        ("root", "admin"),
    ] {
        let r = PyDict::new(py);
        r.set_item("role", role_name)?;
        r.set_item("db", db)?;
        r.set_item("isBuiltin", true)?;
        r.set_item("roles", PyList::empty(py))?;
        r.set_item("inheritedRoles", PyList::empty(py))?;
        builtin_roles.append(r)?;
    }

    let mut show_builtin = false;
    if let Some(ri) = cmd.get_item("rolesInfo")? {
        if let Ok(d) = ri.cast::<PyDict>() {
            show_builtin = d
                .get_item("showBuiltinRoles")?
                .map(|v| v.is_truthy().unwrap_or(false))
                .unwrap_or(false);
        } else if ri.extract::<i64>().map(|v| v == 1).unwrap_or(false) {
            show_builtin = dict_get_bool(cmd, "showBuiltinRoles", false)?;
        }
    }

    let resp = PyDict::new(py);
    if show_builtin {
        resp.set_item("roles", builtin_roles)?;
    } else {
        resp.set_item("roles", PyList::empty(py))?;
    }
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_create_user(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (store_py, lock_py) = resolve_user_store(py, ctx)?;
    let store = store_py.bind(py);
    let lock = lock_py.bind(py);

    let db_name = dict_get_str(cmd, "$db", "test")?;
    let user = dict_get_str(cmd, "createUser", "")?;
    if user.is_empty() {
        let r = make_error(py, "InvalidOptions", "createUser requires a username")?;
        return Ok(r.into_any().unbind());
    }
    let key = format!("{db_name}.{user}");
    let roles = cmd
        .get_item("roles")?
        .unwrap_or_else(|| PyList::empty(py).into_any());

    let _guard = lock.call_method1("__enter__", ())?;
    let result = (|| -> PyResult<Py<PyAny>> {
        let existing = store.call_method1("get", (&key,))?;
        if !existing.is_none() {
            let r = make_error(
                py,
                "DuplicateKey",
                &format!("User \"{user}@{db_name}\" already exists"),
            )?;
            return Ok(r.into_any().unbind());
        }
        let bson_oid = crate::cached_modules::bson_objectid_cls(py)?.call0()?;
        let entry = PyDict::new(py);
        entry.set_item("_id", format!("{db_name}.{user}"))?;
        entry.set_item("userId", bson_oid)?;
        entry.set_item("user", &user)?;
        entry.set_item("db", &db_name)?;
        entry.set_item("roles", &roles)?;
        let mechs = PyList::new(py, ["SCRAM-SHA-256"])?;
        entry.set_item("mechanisms", mechs)?;

        if let Some(pwd_obj) = cmd.get_item("pwd")? {
            let pwd: String = pwd_obj.extract()?;
            build_scram_credentials(py, &entry, &pwd)?;
        }

        store.set_item(&key, &entry)?;
        persist_user_to_redb(py, ctx, &key, &entry)?;
        Ok(ok_dict(py)?.into_any().unbind())
    })();
    lock.call_method1("__exit__", (py.None(), py.None(), py.None()))?;
    result
}

fn build_scram_credentials(
    py: Python<'_>,
    entry: &Bound<'_, PyDict>,
    password: &str,
) -> PyResult<()> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;

    let salt = crate::scram::generate_salt();
    let iterations = crate::scram::default_iterations();
    let cred = crate::scram::hash_password(password, &salt, iterations);

    let scram_dict = PyDict::new(py);
    scram_dict.set_item("salt", b64.encode(&cred.salt))?;
    scram_dict.set_item("storedKey", b64.encode(cred.stored_key))?;
    scram_dict.set_item("serverKey", b64.encode(cred.server_key))?;
    scram_dict.set_item("iterationCount", cred.iteration_count)?;

    let creds_dict = PyDict::new(py);
    creds_dict.set_item("SCRAM-SHA-256", scram_dict)?;
    entry.set_item("credentials", creds_dict)?;
    Ok(())
}

fn persist_user_to_redb(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    key: &str,
    user_doc: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let lc = ctx.borrow().local_client_typed(py)?;
    let json_util = crate::cached_modules::bson_json_util(py)?;
    let value: String = json_util.call_method1("dumps", (user_doc,))?.extract()?;
    lc.bind(py).borrow().sync_kv_put("table:__users", key, &value)
}

fn delete_user_from_redb(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    key: &str,
) -> PyResult<()> {
    let lc = ctx.borrow().local_client_typed(py)?;
    lc.bind(py).borrow().sync_kv_remove("table:__users", key)
}

fn cmd_drop_user(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (store_py, lock_py) = resolve_user_store(py, ctx)?;
    let store = store_py.bind(py);
    let lock = lock_py.bind(py);

    let db_name = dict_get_str(cmd, "$db", "test")?;
    let user = dict_get_str(cmd, "dropUser", "")?;
    let key = format!("{db_name}.{user}");

    let _guard = lock.call_method1("__enter__", ())?;
    let result = (|| -> PyResult<Py<PyAny>> {
        let existing = store.call_method1("get", (&key,))?;
        if existing.is_none() {
            let r = make_error(
                py,
                "UserNotFound",
                &format!("User \"{user}@{db_name}\" not found"),
            )?;
            return Ok(r.into_any().unbind());
        }
        store.call_method1("__delitem__", (&key,))?;
        let _ = delete_user_from_redb(py, ctx, &key);
        Ok(ok_dict(py)?.into_any().unbind())
    })();
    lock.call_method1("__exit__", (py.None(), py.None(), py.None()))?;
    result
}

fn cmd_update_user(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (store_py, lock_py) = resolve_user_store(py, ctx)?;
    let store = store_py.bind(py);
    let lock = lock_py.bind(py);

    let db_name = dict_get_str(cmd, "$db", "test")?;
    let user = dict_get_str(cmd, "updateUser", "")?;
    let key = format!("{db_name}.{user}");

    let _guard = lock.call_method1("__enter__", ())?;
    let result = (|| -> PyResult<Py<PyAny>> {
        let existing = store.call_method1("get", (&key,))?;
        if existing.is_none() {
            let r = make_error(
                py,
                "UserNotFound",
                &format!("User \"{user}@{db_name}\" not found"),
            )?;
            return Ok(r.into_any().unbind());
        }
        let entry = store.get_item(&key)?;
        if let Some(roles) = cmd.get_item("roles")? {
            entry.set_item("roles", roles)?;
        }
        if let Some(mechs) = cmd.get_item("mechanisms")? {
            entry.set_item("mechanisms", mechs)?;
        }
        if let Some(pwd_obj) = cmd.get_item("pwd")? {
            let pwd: String = pwd_obj.extract()?;
            let entry_dict = entry.cast::<PyDict>()?;
            build_scram_credentials(py, entry_dict, &pwd)?;
        }
        let _ = persist_user_to_redb(py, ctx, &key, &entry);
        Ok(ok_dict(py)?.into_any().unbind())
    })();
    lock.call_method1("__exit__", (py.None(), py.None(), py.None()))?;
    result
}

fn cmd_grant_roles_to_user(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (store_py, lock_py) = resolve_user_store(py, ctx)?;
    let store = store_py.bind(py);
    let lock = lock_py.bind(py);

    let db_name = dict_get_str(cmd, "$db", "test")?;
    let user = dict_get_str(cmd, "grantRolesToUser", "")?;
    let key = format!("{db_name}.{user}");

    let new_roles = cmd
        .get_item("roles")?
        .ok_or_else(|| PyRuntimeError::new_err("grantRolesToUser requires 'roles' array"))?;

    let _guard = lock.call_method1("__enter__", ())?;
    let result = (|| -> PyResult<Py<PyAny>> {
        let existing = store.call_method1("get", (&key,))?;
        if existing.is_none() {
            let r = make_error(
                py,
                "UserNotFound",
                &format!("User \"{user}@{db_name}\" not found"),
            )?;
            return Ok(r.into_any().unbind());
        }
        let entry = store.get_item(&key)?;
        let current_roles = entry.call_method1("get", ("roles", PyList::empty(py)))?;
        let current_list = current_roles.cast::<PyList>()?;

        let new_list = new_roles.cast::<PyList>()?;
        for item in new_list.iter() {
            let already = current_list.contains(&item)?;
            if !already {
                current_list.append(item)?;
            }
        }
        entry.set_item("roles", current_list)?;
        let _ = persist_user_to_redb(py, ctx, &key, &entry);
        Ok(ok_dict(py)?.into_any().unbind())
    })();
    lock.call_method1("__exit__", (py.None(), py.None(), py.None()))?;
    result
}

fn cmd_revoke_roles_from_user(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (store_py, lock_py) = resolve_user_store(py, ctx)?;
    let store = store_py.bind(py);
    let lock = lock_py.bind(py);

    let db_name = dict_get_str(cmd, "$db", "test")?;
    let user = dict_get_str(cmd, "revokeRolesFromUser", "")?;
    let key = format!("{db_name}.{user}");

    let revoke_roles = cmd
        .get_item("roles")?
        .ok_or_else(|| PyRuntimeError::new_err("revokeRolesFromUser requires 'roles' array"))?;

    let _guard = lock.call_method1("__enter__", ())?;
    let result = (|| -> PyResult<Py<PyAny>> {
        let existing = store.call_method1("get", (&key,))?;
        if existing.is_none() {
            let r = make_error(
                py,
                "UserNotFound",
                &format!("User \"{user}@{db_name}\" not found"),
            )?;
            return Ok(r.into_any().unbind());
        }
        let entry = store.get_item(&key)?;
        let current_roles = entry.call_method1("get", ("roles", PyList::empty(py)))?;
        let current_list = current_roles.cast::<PyList>()?;

        let revoke_list = revoke_roles.cast::<PyList>()?;
        let mut keep = Vec::new();
        for item in current_list.iter() {
            let should_remove = revoke_list.contains(&item)?;
            if !should_remove {
                keep.push(item);
            }
        }
        let new_roles = PyList::new(py, &keep)?;
        entry.set_item("roles", new_roles)?;
        let _ = persist_user_to_redb(py, ctx, &key, &entry);
        Ok(ok_dict(py)?.into_any().unbind())
    })();
    lock.call_method1("__exit__", (py.None(), py.None(), py.None()))?;
    result
}

pub(crate) fn register(m: &mut HashMap<&'static str, HandlerFn>) {
    m.insert("getLog", cmd_get_log);
    m.insert("getFreeMonitoringStatus", cmd_free_monitoring);
    m.insert("getCmdLineOpts", cmd_cmdline_opts);
    m.insert("listDatabases", cmd_list_databases);
    m.insert("listCollections", cmd_list_collections);
    m.insert("create", cmd_create_collection);
    m.insert("drop", cmd_drop);
    m.insert("dropDatabase", cmd_drop_database);
    m.insert("collMod", cmd_coll_mod);
    m.insert("renameCollection", cmd_rename_collection);
    m.insert("compact", cmd_compact);
    m.insert("collStats", cmd_coll_stats);
    m.insert("dbStats", cmd_db_stats);
    m.insert("validate", cmd_validate);
    m.insert("serverStatus", cmd_server_status);
    m.insert("fsync", cmd_fsync);
    m.insert("getnonce", cmd_getnonce);
    m.insert("getParameter", cmd_get_parameter);
    m.insert("setParameter", cmd_set_parameter);
    m.insert("connPoolStats", cmd_conn_pool_stats);
    m.insert("features", cmd_features);
    m.insert("logRotate", cmd_log_rotate);
    m.insert("shardingState", cmd_sharding_state);
    m.insert("replSetGetConfig", cmd_repl_get_config);
    m.insert("replSetGetStatus", cmd_repl_status);
    m.insert("setFreeMonitoring", cmd_set_free_monitoring);
    m.insert("lockInfo", cmd_lock_info);
    m.insert("listCommands", cmd_list_commands);
    m.insert("client.sync", cmd_client_sync);
    m.insert("usersInfo", cmd_users_info);
    m.insert("rolesInfo", cmd_roles_info);
    m.insert("createUser", cmd_create_user);
    m.insert("dropUser", cmd_drop_user);
    m.insert("updateUser", cmd_update_user);
    m.insert("grantRolesToUser", cmd_grant_roles_to_user);
    m.insert("revokeRolesFromUser", cmd_revoke_roles_from_user);
}
