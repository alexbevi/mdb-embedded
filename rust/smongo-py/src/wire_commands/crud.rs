//! Wire protocol CRUD command handlers: `find`, `insert`, `update`, `delete`, `getMore`, `count`, `distinct`.
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use std::collections::HashMap;

use crate::redb_client::RedbLocalCollection;
use crate::wire_context::ConnectionContext;
use crate::wire_cursors::CursorRegistry;
use crate::wire_dispatch::inc_counter;
use crate::wire_errors::make_error;

use super::{
    apply_projection_single, apply_sort_py, bson_int64, classify_write_error, dict_get_bool,
    dict_get_i64, dict_get_list, dict_get_or_none, dict_get_str, get_collection_typed,
    set_last_write, HandlerFn,
};

fn resolve_validation_err<'py>(
    py: Python<'py>,
    _ctx: &Bound<'py, ConnectionContext>,
) -> PyResult<Bound<'py, PyAny>> {
    Ok(py.get_type::<crate::schema::ValidationError>().into_any())
}

fn cmd_find(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("query");
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("find")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'find'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;

    let raw_filter = cmd
        .get_item("filter")?
        .unwrap_or_else(|| PyDict::new(py).into_any());
    let filter_dict = raw_filter.cast::<PyDict>()?.clone();

    let projection = cmd.get_item("projection")?;
    let sort_spec = cmd.get_item("sort")?;
    let skip_val = dict_get_i64(cmd, "skip", 0)?;
    let limit_val = dict_get_i64(cmd, "limit", 0)?;
    let batch_size = dict_get_i64(cmd, "batchSize", 101)?;
    let single_batch = dict_get_bool(cmd, "singleBatch", false)?;
    let plan = coll_py
        .bind(py)
        .borrow()
        .explain(py, &filter_dict, false)?;
    let plan_bound = plan.bind(py);
    let plan_str = plan_bound
        .get_item("plan")?
        .map(|v| v.extract::<String>().unwrap_or_default())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "COLLSCAN".to_string());
    let plan_summary = if let Ok(Some(index_val)) = plan_bound.get_item("index") {
        if !index_val.is_none() {
            let idx_str: String = index_val.extract().unwrap_or_default();
            format!("IXSCAN {{ {idx_str} }}")
        } else {
            plan_str
        }
    } else {
        plan_str
    };
    ctx.borrow_mut().last_plan_summary = plan_summary;

    let docs = RedbLocalCollection::find_streaming_typed(
        coll_py.clone_ref(py),
        py,
        Some(filter_dict.as_any()),
    )?;

    let sorted: Bound<'_, PyAny> = if let Some(ref sort) = sort_spec {
        if !sort.is_none() {
            apply_sort_py(py, docs.bind(py).as_any(), sort)?
        } else {
            docs.into_bound(py).into_any()
        }
    } else {
        docs.into_bound(py).into_any()
    };

    let materialized_list;
    let sorted_list: &Bound<'_, PyList> = match sorted.cast::<PyList>() {
        Ok(lst) => lst,
        Err(_) => {
            let items: Vec<Bound<'_, PyAny>> = sorted.try_iter()?.collect::<PyResult<Vec<_>>>()?;
            materialized_list = PyList::new(py, items)?;
            &materialized_list
        }
    };
    let total = sorted_list.len();
    let skip = skip_val as usize;
    let sliced = if skip > 0 || limit_val > 0 {
        let start = skip.min(total);
        let end = if limit_val > 0 {
            (start + limit_val as usize).min(total)
        } else {
            total
        };
        sorted_list.get_slice(start, end)
    } else {
        sorted_list.clone()
    };

    let projected = if let Some(ref proj) = projection {
        if !proj.is_none() {
            let out = PyList::empty(py);
            for item in sliced.iter() {
                out.append(apply_projection_single(py, &item, proj)?)?;
            }
            out
        } else {
            sliced
        }
    } else {
        sliced
    };

    let result_bound = &projected;

    let ns = format!("{db_name}.{coll_name}");

    if single_batch || batch_size <= 0 {
        let cursor_dict = PyDict::new(py);
        cursor_dict.set_item("id", bson_int64(py, 0)?)?;
        cursor_dict.set_item("ns", &ns)?;
        cursor_dict.set_item("firstBatch", result_bound)?;
        let resp = PyDict::new(py);
        resp.set_item("cursor", cursor_dict)?;
        resp.set_item("ok", 1.0)?;
        return Ok(resp.into_any().unbind());
    }

    let cr = ctx.borrow().cursor_registry.clone_ref(py);
    let cr_reg = cr.bind(py).cast::<CursorRegistry>()?;
    let (cursor_id, first_batch) =
        cr_reg
            .borrow()
            .create(py, &ns, result_bound, Some(batch_size as usize))?;

    let cursor_dict = PyDict::new(py);
    cursor_dict.set_item("id", bson_int64(py, cursor_id)?)?;
    cursor_dict.set_item("ns", &ns)?;
    cursor_dict.set_item("firstBatch", first_batch.bind(py))?;
    let resp = PyDict::new(py);
    resp.set_item("cursor", cursor_dict)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_insert(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("insert");
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("insert")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'insert'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let ordered = dict_get_bool(cmd, "ordered", true)?;

    let documents = seqs
        .call_method1("get", ("documents",))?
        .extract::<Option<Bound<'_, PyAny>>>()?;
    let documents = match documents {
        Some(d) if !d.is_none() => d,
        _ => cmd
            .get_item("documents")?
            .unwrap_or_else(|| PyList::empty(py).into_any()),
    };
    let doc_list = documents.cast_into::<PyList>()?;

    if doc_list.len() > 100_000 {
        let r = make_error(
            py,
            "InvalidLength",
            &format!("batch size {} exceeds limit 100000", doc_list.len()),
        )?;
        return Ok(r.into_any().unbind());
    }

    let dup_key_err = py.None().into_bound(py);
    let validation_err = resolve_validation_err(py, ctx)?;

    let mut inserted: i64 = 0;
    let write_errors = PyList::empty(py);

    for (i, doc) in doc_list.iter().enumerate() {
        let doc_dict = doc.cast::<PyDict>()?;
        match coll_py.bind(py).borrow().insert_one(py, doc_dict, false) {
            Ok(_) => inserted += 1,
            Err(err) => {
                let (code, msg) = classify_write_error(py, &err, &dup_key_err, &validation_err)?;
                let entry = PyDict::new(py);
                entry.set_item("index", i)?;
                entry.set_item("code", code)?;
                entry.set_item("errmsg", &msg)?;
                write_errors.append(entry)?;
                if ordered {
                    break;
                }
            }
        }
    }

    let err_msg: Option<String> = if !write_errors.is_empty() {
        let first = write_errors.get_item(0)?;
        Some(first.get_item("errmsg")?.extract()?)
    } else {
        None
    };

    set_last_write(
        py,
        ctx,
        "insert",
        inserted,
        0,
        err_msg.as_deref(),
        &write_errors,
    )?;

    let resp = PyDict::new(py);
    resp.set_item("n", inserted)?;
    resp.set_item("ok", 1.0)?;
    if !write_errors.is_empty() {
        resp.set_item("writeErrors", &write_errors)?;
    }
    Ok(resp.into_any().unbind())
}

fn cmd_update(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("update");
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("update")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'update'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let ordered = dict_get_bool(cmd, "ordered", true)?;

    let updates = seqs
        .call_method1("get", ("updates",))?
        .extract::<Option<Bound<'_, PyAny>>>()?;
    let updates = match updates {
        Some(u) if !u.is_none() => u,
        _ => cmd
            .get_item("updates")?
            .unwrap_or_else(|| PyList::empty(py).into_any()),
    };
    let update_list = updates.cast_into::<PyList>()?;

    if update_list.len() > 100_000 {
        let r = make_error(
            py,
            "InvalidLength",
            &format!("batch size {} exceeds limit 100000", update_list.len()),
        )?;
        return Ok(r.into_any().unbind());
    }

    let dup_key_err = py.None().into_bound(py);
    let validation_err = resolve_validation_err(py, ctx)?;
    let objectid_cls = py.get_type::<crate::objectid::ObjectId>().into_any();

    let mut n: i64 = 0;
    let mut n_modified: i64 = 0;
    let upserted_list = PyList::empty(py);
    let write_errors = PyList::empty(py);

    for (i, spec) in update_list.iter().enumerate() {
        let result: PyResult<()> = (|| {
            let spec_dict = spec.cast::<PyDict>()?;
            let q_raw = spec_dict
                .get_item("q")?
                .unwrap_or_else(|| PyDict::new(py).into_any());
            let u_raw = spec_dict
                .get_item("u")?
                .unwrap_or_else(|| PyDict::new(py).into_any());
            let q_dict = q_raw.cast::<PyDict>()?.clone();
            let multi = spec_dict
                .get_item("multi")?
                .map(|v| v.is_truthy().unwrap_or(false))
                .unwrap_or(false);
            let upsert = spec_dict
                .get_item("upsert")?
                .map(|v| v.is_truthy().unwrap_or(false))
                .unwrap_or(false);

            let u_bound = &u_raw;
            let has_operators = if let Ok(d) = u_bound.cast::<PyDict>() {
                d.keys().iter().any(|k| {
                    k.extract::<String>()
                        .map(|s| s.starts_with('$'))
                        .unwrap_or(false)
                })
            } else {
                false
            };

            let coll_b = coll_py.bind(py).borrow();
            let result_py: Py<PyAny> = if has_operators {
                coll_py
                    .bind(py)
                    .borrow()
                    .update(py, &q_dict, u_bound, multi, false, false)?
            } else {
                match coll_b.find_one(py, &q_dict, None)? {
                    Some(target_py) => {
                        let target = target_py.bind(py);
                        let target_id = target.get_item("_id")?;
                        let id_filter = PyDict::new(py);
                        id_filter.set_item("_id", target_id)?;
                        coll_b.find_one_and_replace_core(
                            py,
                            &id_filter,
                            u_bound.cast::<PyDict>()?,
                            false,
                            "before",
                            false,
                        )?;
                        Py::new(py, crate::results::UpdateResult::new(py, 1, 1, None))?.into_any()
                    }
                    None => Py::new(py, crate::results::UpdateResult::new(py, 0, 0, None))?
                        .into_any(),
                }
            };
            let result = result_py.bind(py);

            let mod_count: i64 = result.getattr("modified_count")?.extract()?;
            let match_count: i64 = result.getattr("matched_count")?.extract()?;

            if mod_count > 0 {
                n += mod_count;
                n_modified += mod_count;
            } else if upsert && match_count == 0 {
                let new_doc = if has_operators {
                    let new_doc = PyDict::new(py);
                    for (k, v) in q_dict.iter() {
                        if let Ok(vd) = v.cast::<PyDict>() {
                            let has_op = vd.keys().iter().any(|kk: Bound<'_, PyAny>| {
                                kk.extract::<String>()
                                    .map(|s| s.starts_with('$'))
                                    .unwrap_or(false)
                            });
                            if has_op {
                                continue;
                            }
                        }
                        new_doc.set_item(k, v)?;
                    }
                    crate::query_update::apply_update(&new_doc, u_bound, None, None)?;
                    new_doc.into_any()
                } else {
                    let d = PyDict::new(py);
                    if let Ok(ud) = u_bound.cast::<PyDict>() {
                        for (k, v) in ud.iter() {
                            d.set_item(k, v)?;
                        }
                    }
                    d.into_any()
                };

                if new_doc.get_item("_id").is_err() || new_doc.get_item("_id")?.is_none() {
                    let oid = objectid_cls.call0()?;
                    new_doc.set_item("_id", &oid)?;
                }
                coll_py
                    .bind(py)
                    .borrow()
                    .insert_one(py, new_doc.cast::<PyDict>()?, false)?;
                let entry = PyDict::new(py);
                entry.set_item("index", i)?;
                entry.set_item("_id", new_doc.get_item("_id")?)?;
                upserted_list.append(entry)?;
                n += 1;
            } else {
                n += match_count;
            }
            Ok(())
        })();

        if let Err(err) = result {
            let (code, msg) = classify_write_error(py, &err, &dup_key_err, &validation_err)?;
            let entry = PyDict::new(py);
            entry.set_item("index", i)?;
            entry.set_item("code", code)?;
            entry.set_item("errmsg", &msg)?;
            write_errors.append(entry)?;
            if ordered {
                break;
            }
        }
    }

    let err_msg: Option<String> = if !write_errors.is_empty() {
        let first = write_errors.get_item(0)?;
        Some(first.get_item("errmsg")?.extract()?)
    } else {
        None
    };

    set_last_write(
        py,
        ctx,
        "update",
        n,
        n_modified,
        err_msg.as_deref(),
        &write_errors,
    )?;

    let resp = PyDict::new(py);
    resp.set_item("n", n)?;
    resp.set_item("nModified", n_modified)?;
    resp.set_item("ok", 1.0)?;
    if !upserted_list.is_empty() {
        resp.set_item("upserted", upserted_list)?;
    }
    if !write_errors.is_empty() {
        resp.set_item("writeErrors", &write_errors)?;
    }
    Ok(resp.into_any().unbind())
}

fn cmd_delete(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("delete");
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("delete")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'delete'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let ordered = dict_get_bool(cmd, "ordered", true)?;

    let deletes = seqs
        .call_method1("get", ("deletes",))?
        .extract::<Option<Bound<'_, PyAny>>>()?;
    let deletes = match deletes {
        Some(d) if !d.is_none() => d,
        _ => cmd
            .get_item("deletes")?
            .unwrap_or_else(|| PyList::empty(py).into_any()),
    };
    let delete_list = deletes.cast_into::<PyList>()?;

    if delete_list.len() > 100_000 {
        let r = make_error(
            py,
            "InvalidLength",
            &format!("batch size {} exceeds limit 100000", delete_list.len()),
        )?;
        return Ok(r.into_any().unbind());
    }

    let mut n_total: i64 = 0;
    let write_errors = PyList::empty(py);

    for (i, spec) in delete_list.iter().enumerate() {
        let result: PyResult<()> = (|| {
            let spec_dict = spec.cast::<PyDict>()?;
            let q_raw = spec_dict
                .get_item("q")?
                .unwrap_or_else(|| PyDict::new(py).into_any());
            let q_dict = q_raw.cast::<PyDict>()?.clone();
            let limit_val = spec_dict
                .get_item("limit")?
                .and_then(|v| v.extract::<i64>().ok())
                .unwrap_or(0);
            let multi = limit_val == 0;

            let result = coll_py
                .bind(py)
                .borrow()
                .delete(py, &q_dict, multi, false)?;
            let deleted: i64 = result.bind(py).getattr("deleted_count")?.extract()?;
            n_total += deleted;
            Ok(())
        })();

        if let Err(err) = result {
            let msg = err.value(py).str()?.to_string();
            let entry = PyDict::new(py);
            entry.set_item("index", i)?;
            entry.set_item("code", 1)?;
            entry.set_item("errmsg", &msg)?;
            write_errors.append(entry)?;
            if ordered {
                break;
            }
        }
    }

    let err_msg: Option<String> = if !write_errors.is_empty() {
        let first = write_errors.get_item(0)?;
        Some(first.get_item("errmsg")?.extract()?)
    } else {
        None
    };

    set_last_write(
        py,
        ctx,
        "delete",
        n_total,
        0,
        err_msg.as_deref(),
        &write_errors,
    )?;

    let resp = PyDict::new(py);
    resp.set_item("n", n_total)?;
    resp.set_item("ok", 1.0)?;
    if !write_errors.is_empty() {
        resp.set_item("writeErrors", &write_errors)?;
    }
    Ok(resp.into_any().unbind())
}

fn cmd_count(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("query");
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("count")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'count'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let raw_query = cmd
        .get_item("query")?
        .unwrap_or_else(|| PyDict::new(py).into_any());
    let query_dict = raw_query.cast::<PyDict>()?.clone();
    let count = coll_py.bind(py).borrow().count(py, Some(&query_dict))?;
    let resp = PyDict::new(py);
    resp.set_item("n", count)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_distinct(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("query");
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("distinct")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'distinct'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let key = dict_get_str(cmd, "key", "")?;
    let raw_query = cmd
        .get_item("query")?
        .unwrap_or_else(|| PyDict::new(py).into_any());
    let docs = RedbLocalCollection::find_streaming_typed(
        coll_py.clone_ref(py),
        py,
        Some(&raw_query),
    )?;

    let seen = PyList::empty(py);
    let iter = docs.bind(py).try_iter()?;
    for item in iter {
        let doc = item?;
        let v = crate::paths::get_value(&doc, &key)?;
        if !seen.contains(v.bind(py))? {
            seen.append(v)?;
        }
    }

    let resp = PyDict::new(py);
    resp.set_item("values", seen)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_get_more(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("getmore");
    let cursor_id_any = cmd
        .get_item("getMore")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'getMore'"))?;
    let cid: i64 = cursor_id_any.extract()?;
    let coll_name = dict_get_str(cmd, "collection", "")?;
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let batch_size = dict_get_or_none(cmd, "batchSize")?;
    let bs_opt: Option<usize> = match batch_size {
        None => None,
        Some(ref b) if b.is_none() => None,
        Some(b) => Some(b.extract::<i64>()? as usize),
    };

    let cr = ctx.borrow().cursor_registry.clone_ref(py);
    let cr_reg = cr.bind(py).cast::<CursorRegistry>()?;

    if cr_reg.borrow().is_tailable(cid) {
        let max_await = dict_get_i64(cmd, "maxTimeMS", 1000)? as u64;
        let (new_id_opt, batch_opt) = cr_reg
            .borrow()
            .get_more_change_stream(py, cid, bs_opt, max_await)?;
        let Some(batch_py) = batch_opt else {
            let r = make_error(py, "CursorNotFound", &format!("cursor id {cid} not found"))?;
            return Ok(r.into_any().unbind());
        };
        let ns = format!("{db_name}.{coll_name}");
        let batch_bound = batch_py.bind(py);
        let out_batch: Bound<'_, PyAny> = if batch_bound.is_truthy()? {
            batch_bound.clone().into_any()
        } else {
            PyList::empty(py).into_any()
        };
        let cursor_dict = PyDict::new(py);
        cursor_dict.set_item("id", bson_int64(py, new_id_opt.unwrap_or(0))?)?;
        cursor_dict.set_item("ns", &ns)?;
        cursor_dict.set_item("nextBatch", out_batch)?;
        let resp = PyDict::new(py);
        resp.set_item("cursor", cursor_dict)?;
        resp.set_item("ok", 1.0)?;
        return Ok(resp.into_any().unbind());
    }

    let (new_id_opt, batch_opt) = cr_reg.borrow().get_more(py, cid, bs_opt)?;
    let Some(batch_py) = batch_opt else {
        let r = make_error(py, "CursorNotFound", &format!("cursor id {cid} not found"))?;
        return Ok(r.into_any().unbind());
    };
    let ns = format!("{db_name}.{coll_name}");
    let cursor_dict = PyDict::new(py);
    cursor_dict.set_item("id", bson_int64(py, new_id_opt.unwrap_or(0))?)?;
    cursor_dict.set_item("ns", &ns)?;
    cursor_dict.set_item("nextBatch", batch_py.bind(py))?;
    let resp = PyDict::new(py);
    resp.set_item("cursor", cursor_dict)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_kill_cursors(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let cursor_ids = cmd
        .get_item("cursors")?
        .unwrap_or_else(|| PyList::empty(py).into_any());
    let cr = ctx.borrow().cursor_registry.clone_ref(py);
    let cr_reg = cr.bind(py).cast::<CursorRegistry>()?;
    let mut ids: Vec<i64> = Vec::new();
    if let Ok(ids_list) = cursor_ids.cast::<PyList>() {
        for cid_bound in ids_list.iter() {
            ids.push(cid_bound.extract::<i64>()?);
        }
    }
    let killed_vec = cr_reg.borrow().kill(ids);

    let killed = PyList::empty(py);
    for k in &killed_vec {
        killed.append(*k)?;
    }

    let not_found = PyList::empty(py);
    if let Ok(ids_list) = cursor_ids.cast::<PyList>() {
        for cid_bound in ids_list.iter() {
            let cid: i64 = cid_bound.extract()?;
            if !killed_vec.contains(&cid) {
                not_found.append(cid_bound)?;
            }
        }
    }

    let resp = PyDict::new(py);
    resp.set_item("cursorsKilled", killed)?;
    resp.set_item("cursorsNotFound", not_found)?;
    resp.set_item("cursorsAlive", PyList::empty(py))?;
    resp.set_item("cursorsUnknown", PyList::empty(py))?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_find_and_modify(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name = if let Some(v) = cmd.get_item("findAndModify")? {
        v.extract::<String>()?
    } else {
        cmd.get_item("findandmodify")?
            .ok_or_else(|| PyValueError::new_err("missing required field 'findandmodify'"))?
            .extract::<String>()?
    };
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;

    let raw_query = cmd
        .get_item("query")?
        .unwrap_or_else(|| PyDict::new(py).into_any());
    let qd = raw_query.cast::<PyDict>()?.clone();
    let sort_spec = cmd.get_item("sort")?;
    let remove = dict_get_bool(cmd, "remove", false)?;
    let update_spec = cmd.get_item("update")?;
    let new_flag = dict_get_bool(cmd, "new", false)?;
    let upsert = dict_get_bool(cmd, "upsert", false)?;
    let fields = cmd.get_item("fields")?;
    let return_doc = if new_flag { "after" } else { "before" };

    let doc: Bound<'_, PyAny>;

    if remove {
        if let Some(ref sort) = sort_spec {
            if sort.is_truthy()? {
                let matching_py = coll_py.bind(py).borrow().find(py, &qd, None)?;
                let matching = apply_sort_py(py, matching_py.bind(py).as_any(), sort)?;
                if matching.is_truthy()? && matching.len()? > 0 {
                    let first = matching.get_item(0)?;
                    let id_filter = PyDict::new(py);
                    id_filter.set_item("_id", first.get_item("_id")?)?;
                    doc = coll_py
                        .bind(py)
                        .borrow()
                        .find_one_and_delete_core(py, &id_filter, false)?
                        .bind(py)
                        .clone();
                } else {
                    doc = py.None().into_bound(py);
                }
            } else {
                doc = coll_py
                    .bind(py)
                    .borrow()
                    .find_one_and_delete_core(py, &qd, false)?
                    .bind(py)
                    .clone();
            }
        } else {
            doc = coll_py
                .bind(py)
                .borrow()
                .find_one_and_delete_core(py, &qd, false)?
                .bind(py)
                .clone();
        }
    } else if let Some(ref us) = update_spec {
        if !us.is_none() {
            let us_bound = us;

            let has_operators = if let Ok(d) = us_bound.cast::<PyDict>() {
                d.keys().iter().any(|k| {
                    k.extract::<String>()
                        .map(|s| s.starts_with('$'))
                        .unwrap_or(false)
                })
            } else {
                false
            };

            let matching = if let Some(ref sort) = sort_spec {
                if sort.is_truthy()? {
                    let m = coll_py.bind(py).borrow().find(py, &qd, None)?;
                    apply_sort_py(py, m.bind(py).as_any(), sort)?
                } else {
                    match coll_py.bind(py).borrow().find_one(py, &qd, None)? {
                        None => PyList::empty(py).into_any(),
                        Some(d) => {
                            let b = d.bind(py);
                            PyList::new(py, [b])?.into_any()
                        }
                    }
                }
            } else {
                match coll_py.bind(py).borrow().find_one(py, &qd, None)? {
                    None => PyList::empty(py).into_any(),
                    Some(d) => {
                        let b = d.bind(py);
                        PyList::new(py, [b])?.into_any()
                    }
                }
            };

            if matching.is_truthy()? && matching.len()? > 0 {
                let first = matching.get_item(0)?;
                let id_filter = PyDict::new(py);
                id_filter.set_item("_id", first.get_item("_id")?)?;
                if has_operators {
                    doc = coll_py
                        .bind(py)
                        .borrow()
                        .find_one_and_update_core(py, &id_filter, us_bound, return_doc, false)?
                        .bind(py)
                        .clone();
                } else {
                    doc = coll_py
                        .bind(py)
                        .borrow()
                        .find_one_and_replace_core(
                            py,
                            &id_filter,
                            us_bound.cast::<PyDict>()?,
                            false,
                            return_doc,
                            false,
                        )?
                        .bind(py)
                        .clone();
                }
            } else if upsert {
                let objectid_cls = py.get_type::<crate::objectid::ObjectId>().into_any();

                let new_doc = if has_operators {
                    let nd = PyDict::new(py);
                    for (k, v) in qd.iter() {
                        if let Ok(vd) = v.cast::<PyDict>() {
                            let has_op = vd.keys().iter().any(|kk: Bound<'_, PyAny>| {
                                kk.extract::<String>()
                                    .map(|s| s.starts_with('$'))
                                    .unwrap_or(false)
                            });
                            if has_op {
                                continue;
                            }
                        }
                        nd.set_item(k, v)?;
                    }
                    crate::query_update::apply_update(&nd, us_bound, None, None)?;
                    nd.into_any()
                } else {
                    let nd = PyDict::new(py);
                    if let Ok(ud) = us_bound.cast::<PyDict>() {
                        for (k, v) in ud.iter() {
                            nd.set_item(k, v)?;
                        }
                    }
                    nd.into_any()
                };

                if new_doc.get_item("_id").is_err() || new_doc.get_item("_id")?.is_none() {
                    let oid = objectid_cls.call0()?;
                    new_doc.set_item("_id", oid)?;
                }
                coll_py
                    .bind(py)
                    .borrow()
                    .insert_one(py, new_doc.cast::<PyDict>()?, false)?;
                doc = if new_flag {
                    new_doc
                } else {
                    py.None().into_bound(py)
                };
            } else {
                doc = py.None().into_bound(py);
            }
        } else {
            let r = make_error(
                py,
                "InvalidOptions",
                "findAndModify requires 'remove' or 'update'",
            )?;
            return Ok(r.into_any().unbind());
        }
    } else {
        let r = make_error(
            py,
            "InvalidOptions",
            "findAndModify requires 'remove' or 'update'",
        )?;
        return Ok(r.into_any().unbind());
    }

    let resp = PyDict::new(py);
    resp.set_item("ok", 1.0)?;
    if !doc.is_none() {
        if let Some(ref f) = fields {
            if f.is_truthy()? {
                let projected = apply_projection_single(py, &doc, f)?;
                resp.set_item("value", projected)?;
            } else {
                resp.set_item("value", &doc)?;
            }
        } else {
            resp.set_item("value", &doc)?;
        }
    } else {
        resp.set_item("value", py.None())?;
    }
    let last_err = PyDict::new(py);
    last_err.set_item("n", if !doc.is_none() { 1 } else { 0 })?;
    last_err.set_item("updatedExisting", !doc.is_none() && !upsert)?;
    resp.set_item("lastErrorObject", last_err)?;
    Ok(resp.into_any().unbind())
}

fn cmd_bulk_write(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("command");
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let ordered = dict_get_bool(cmd, "ordered", true)?;

    let ops_list = dict_get_list(cmd, "ops").unwrap_or_else(|_| PyList::empty(py).clone());
    let ns_info_list = dict_get_list(cmd, "nsInfo").unwrap_or_else(|_| PyList::empty(py).clone());

    let dup_key_err = py.None().into_bound(py);
    let validation_err = resolve_validation_err(py, ctx)?;

    let mut n_inserted: i64 = 0;
    let mut n_matched: i64 = 0;
    let mut n_modified: i64 = 0;
    let mut n_deleted: i64 = 0;
    let mut n_upserted: i64 = 0;
    let write_errors = PyList::empty(py);

    let resolve_ns = |ns_idx: usize| -> PyResult<(String, String)> {
        if ns_idx < ns_info_list.len() {
            let entry = ns_info_list.get_item(ns_idx)?;
            let ns_str: String = entry.get_item("ns")?.extract()?;
            if let Some((db, coll)) = ns_str.split_once('.') {
                Ok((db.to_string(), coll.to_string()))
            } else {
                Ok((db_name.clone(), ns_str))
            }
        } else {
            Ok((db_name.clone(), "unknown".to_string()))
        }
    };

    for (i, op) in ops_list.iter().enumerate() {
        let op_dict = op.cast::<PyDict>()?;
        let result: PyResult<()> = (|| {
            if let Some(insert_idx) = op_dict.get_item("insert")? {
                let ns_idx: usize = insert_idx.extract()?;
                let (coll_db, coll_name) = resolve_ns(ns_idx)?;
                let coll_py = get_collection_typed(ctx, &coll_db, &coll_name)?;
                let doc_raw = op_dict
                    .get_item("document")?
                    .unwrap_or_else(|| PyDict::new(py).into_any());
                let doc = doc_raw.cast::<PyDict>()?;
                coll_py.bind(py).borrow().insert_one(py, doc, false)?;
                n_inserted += 1;
            } else if let Some(update_idx) = op_dict.get_item("update")? {
                let ns_idx: usize = update_idx.extract()?;
                let (coll_db, coll_name) = resolve_ns(ns_idx)?;
                let coll_py = get_collection_typed(ctx, &coll_db, &coll_name)?;
                let q_raw = op_dict
                    .get_item("filter")?
                    .unwrap_or_else(|| PyDict::new(py).into_any());
                let u_raw = op_dict
                    .get_item("updateMods")?
                    .unwrap_or_else(|| PyDict::new(py).into_any());
                let q_dict = q_raw.cast::<PyDict>()?.clone();
                let multi = op_dict
                    .get_item("multi")?
                    .map(|v| v.is_truthy().unwrap_or(false))
                    .unwrap_or(false);
                let upsert = op_dict
                    .get_item("upsert")?
                    .map(|v| v.is_truthy().unwrap_or(false))
                    .unwrap_or(false);
                let upd_result = coll_py
                    .bind(py)
                    .borrow()
                    .update(py, &q_dict, &u_raw, multi, upsert, false)?;
                let upserted_id = upd_result.bind(py).getattr("upserted_id")?;
                if !upserted_id.is_none() {
                    n_upserted += 1;
                } else {
                    n_matched += upd_result
                        .bind(py)
                        .getattr("matched_count")?
                        .extract::<i64>()?;
                    n_modified += upd_result
                        .bind(py)
                        .getattr("modified_count")?
                        .extract::<i64>()?;
                }
            } else if let Some(delete_idx) = op_dict.get_item("delete")? {
                let ns_idx: usize = delete_idx.extract()?;
                let (coll_db, coll_name) = resolve_ns(ns_idx)?;
                let coll_py = get_collection_typed(ctx, &coll_db, &coll_name)?;
                let q_raw = op_dict
                    .get_item("filter")?
                    .unwrap_or_else(|| PyDict::new(py).into_any());
                let q_dict = q_raw.cast::<PyDict>()?.clone();
                let multi = op_dict
                    .get_item("multi")?
                    .map(|v| v.is_truthy().unwrap_or(true))
                    .unwrap_or(true);
                let del_result = coll_py
                    .bind(py)
                    .borrow()
                    .delete(py, &q_dict, multi, false)?;
                n_deleted += del_result
                    .bind(py)
                    .getattr("deleted_count")?
                    .extract::<i64>()?;
            }
            Ok(())
        })();

        if let Err(err) = result {
            let (code, msg) = classify_write_error(py, &err, &dup_key_err, &validation_err)?;
            let entry = PyDict::new(py);
            entry.set_item("index", i)?;
            entry.set_item("code", code)?;
            entry.set_item("errmsg", &msg)?;
            write_errors.append(entry)?;
            if ordered {
                break;
            }
        }
    }

    set_last_write(
        py,
        ctx,
        "bulkWrite",
        n_inserted + n_modified + n_deleted,
        n_modified,
        if !write_errors.is_empty() {
            let first = write_errors.get_item(0)?;
            Some(first.get_item("errmsg")?.extract::<String>()?)
        } else {
            None
        }
        .as_deref(),
        &write_errors,
    )?;

    let resp = PyDict::new(py);
    resp.set_item("ok", 1.0)?;
    resp.set_item("nInserted", n_inserted)?;
    resp.set_item("nMatched", n_matched)?;
    resp.set_item("nModified", n_modified)?;
    resp.set_item("nDeleted", n_deleted)?;
    resp.set_item("nUpserted", n_upserted)?;
    if !write_errors.is_empty() {
        resp.set_item("writeErrors", &write_errors)?;
    }
    Ok(resp.into_any().unbind())
}

fn cmd_get_last_error(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (lw, conn_id) = {
        let c = ctx.borrow();
        (c.last_write.clone_ref(py), c.connection_id)
    };
    let lw = lw.into_bound(py);
    if lw.is_none() {
        let resp = PyDict::new(py);
        resp.set_item("ok", 1.0)?;
        resp.set_item("err", py.None())?;
        resp.set_item("n", 0)?;
        return Ok(resp.into_any().unbind());
    }
    let resp = PyDict::new(py);
    resp.set_item("ok", 1.0)?;
    resp.set_item("err", lw.getattr("err")?)?;
    resp.set_item("n", lw.getattr("n")?)?;
    resp.set_item("nModified", lw.getattr("n_modified")?)?;
    resp.set_item("connectionId", conn_id)?;
    let upserted_id = lw.getattr("upserted_id")?;
    if !upserted_id.is_none() {
        resp.set_item("upserted", upserted_id)?;
    }
    let write_errors = lw.getattr("write_errors")?;
    if write_errors.is_truthy()? {
        resp.set_item("writeErrors", write_errors)?;
    }
    Ok(resp.into_any().unbind())
}

fn cmd_estimated_doc_count(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("query");
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name: String = cmd
        .get_item("estimatedDocumentCount")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'estimatedDocumentCount'"))?
        .extract()?;
    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let count = coll_py.bind(py).borrow().count_fast(py)?;
    let resp = PyDict::new(py);
    resp.set_item("n", count)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_data_size(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let key_pattern = cmd.get_item("keyPattern")?;
    let min_key = cmd.get_item("min")?;
    let max_key = cmd.get_item("max")?;
    let ns_raw = dict_get_str(cmd, "dataSize", "")?;

    let (db_name, coll_name) = if ns_raw.contains('.') {
        let parts: Vec<&str> = ns_raw.splitn(2, '.').collect();
        (parts[0].to_string(), parts[1].to_string())
    } else {
        let db = dict_get_str(cmd, "$db", "test")?;
        (db, ns_raw)
    };

    if coll_name.is_empty() {
        let r = make_error(
            py,
            "InvalidNamespace",
            "dataSize requires a valid namespace",
        )?;
        return Ok(r.into_any().unbind());
    }

    let coll_py = get_collection_typed(ctx, &db_name, &coll_name)?;
    let time_mod = crate::cached_modules::time_mod(py)?;
    let t0 = time_mod.call_method0("monotonic")?;

    let kp_truthy = key_pattern
        .as_ref()
        .map(|k| k.is_truthy().unwrap_or(false))
        .unwrap_or(false);
    let (size, n_objects, _estimate) = if kp_truthy && (min_key.is_some() || max_key.is_some()) {
        let query = PyDict::new(py);
        let kp = key_pattern
            .ok_or_else(|| PyValueError::new_err("missing required field 'keyPattern'"))?;
        if let Ok(kp_dict) = kp.cast::<PyDict>() {
            for field in kp_dict.keys().iter() {
                let field_str: String = field.extract()?;
                let bounds = PyDict::new(py);
                if let Some(ref mk) = min_key {
                    if mk.is_truthy()? {
                        if let Ok(v) = mk.get_item(&field_str) {
                            bounds.set_item("$gte", v)?;
                        }
                    }
                }
                if let Some(ref xk) = max_key {
                    if xk.is_truthy()? {
                        if let Ok(v) = xk.get_item(&field_str) {
                            bounds.set_item("$lt", v)?;
                        }
                    }
                }
                if !bounds.is_empty() {
                    query.set_item(&field_str, bounds)?;
                }
            }
        }

        let doc_iter = if !query.is_empty() {
            RedbLocalCollection::find_streaming_typed(
                coll_py.clone_ref(py),
                py,
                Some(query.as_any()),
            )?
        } else {
            RedbLocalCollection::find_streaming_typed(coll_py.clone_ref(py), py, None)?
        };
        let mut size: i64 = 0;
        let mut count: i64 = 0;
        for item in doc_iter.bind(py).try_iter()? {
            let d = item?;
            count += 1;
            let doc_dict = d.cast::<PyDict>()?;
            let encoded = crate::raw_bson::raw_encode_document(py, doc_dict)?;
            size += encoded.len() as i64;
        }
        (size, count, false)
    } else {
        let size: i64 = coll_py.bind(py).borrow().data_size_bytes(py)?;
        let n: i64 = coll_py.bind(py).borrow().count_fast(py)?;
        (size, n, false)
    };

    let t1 = time_mod.call_method0("monotonic")?;
    let elapsed: f64 = t1.extract::<f64>()? - t0.extract::<f64>()?;
    let millis = (elapsed * 1000.0) as i64;

    let resp = PyDict::new(py);
    resp.set_item("size", size)?;
    resp.set_item("numObjects", n_objects)?;
    resp.set_item("millis", millis)?;
    resp.set_item("estimate", false)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

pub(crate) fn register(m: &mut HashMap<&'static str, HandlerFn>) {
    m.insert("find", cmd_find);
    m.insert("insert", cmd_insert);
    m.insert("update", cmd_update);
    m.insert("delete", cmd_delete);
    m.insert("count", cmd_count);
    m.insert("distinct", cmd_distinct);
    m.insert("getMore", cmd_get_more);
    m.insert("killCursors", cmd_kill_cursors);
    m.insert("findAndModify", cmd_find_and_modify);
    m.insert("findandmodify", cmd_find_and_modify);
    m.insert("bulkWrite", cmd_bulk_write);
    m.insert("getLastError", cmd_get_last_error);
    m.insert("getlasterror", cmd_get_last_error);
    m.insert("estimatedDocumentCount", cmd_estimated_doc_count);
    m.insert("dataSize", cmd_data_size);
}
