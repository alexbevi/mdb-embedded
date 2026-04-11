//! Wire protocol `aggregate` command handler.
use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::wire_context::ConnectionContext;
use crate::wire_dispatch::inc_counter;
use crate::wire_errors::make_error;

use super::bson_int64;

use super::{dict_get_i64, dict_get_str, get_collection, get_collection_typed, HandlerFn};

fn cmd_aggregate(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    inc_counter("query");
    let db_name = dict_get_str(cmd, "$db", "test")?;
    let coll_name_val = cmd
        .get_item("aggregate")?
        .ok_or_else(|| PyValueError::new_err("missing required field 'aggregate'"))?;

    let is_db_agg = coll_name_val
        .extract::<i64>()
        .map(|v| v == 1)
        .unwrap_or(false)
        || coll_name_val
            .extract::<String>()
            .map(|v| v == "1")
            .unwrap_or(false);

    let raw_pipeline = cmd
        .get_item("pipeline")?
        .unwrap_or_else(|| PyList::empty(py).into_any());
    let pipeline_list = raw_pipeline.cast::<PyList>()?;

    let pipeline = pipeline_list;

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

    if is_db_agg {
        if !pipeline.is_empty() {
            let first = pipeline.get_item(0)?;
            if let Ok(d) = first.cast::<PyDict>() {
                if d.get_item("$currentOp")?.is_some() {
                    let ns = format!("{db_name}.$cmd.aggregate");
                    let cursor_dict = PyDict::new(py);
                    cursor_dict.set_item("id", bson_int64(py, 0)?)?;
                    cursor_dict.set_item("ns", &ns)?;
                    cursor_dict.set_item("firstBatch", PyList::empty(py))?;
                    let resp = PyDict::new(py);
                    resp.set_item("cursor", cursor_dict)?;
                    resp.set_item("ok", 1.0)?;
                    return Ok(resp.into_any().unbind());
                }
            }
        }
        let ns = format!("{db_name}.$cmd.aggregate");
        let cursor_dict = PyDict::new(py);
        cursor_dict.set_item("id", bson_int64(py, 0)?)?;
        cursor_dict.set_item("ns", &ns)?;
        cursor_dict.set_item("firstBatch", PyList::empty(py))?;
        let resp = PyDict::new(py);
        resp.set_item("cursor", cursor_dict)?;
        resp.set_item("ok", 1.0)?;
        return Ok(resp.into_any().unbind());
    }

    let coll_name: String = coll_name_val.extract()?;
    let coll = get_collection(ctx, &db_name, &coll_name)?;
    let ns = format!("{db_name}.{coll_name}");

    if !pipeline.is_empty() {
        let first = pipeline.get_item(0)?;
        if let Ok(d) = first.cast::<PyDict>() {
            if let Some(coll_stats_spec) = d.get_item("$collStats")? {
                let stats = coll.call_method0("storage_stats")?;
                let stats_dict = stats.cast::<PyDict>()?;
                let count: i64 = stats_dict
                    .get_item("count")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or(0);
                let data_size: i64 = stats_dict
                    .get_item("dataSize")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or(0);
                let storage_size: i64 = stats_dict
                    .get_item("storageSize")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or(0);
                let nindexes: i64 = stats_dict
                    .get_item("nindexes")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or(0);
                let total_idx_size: i64 = stats_dict
                    .get_item("totalIndexSize")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or(0);
                let idx_sizes = stats_dict
                    .get_item("indexSizes")?
                    .unwrap_or_else(|| PyDict::new(py).into_any());
                let se = stats_dict.get_item("storageEngine")?.unwrap_or_else(|| {
                    let d = PyDict::new(py);
                    let _ = d.set_item("name", "redb");
                    d.into_any()
                });

                let doc = PyDict::new(py);
                doc.set_item("ns", &ns)?;

                let spec_dict = coll_stats_spec.cast::<PyDict>().ok();

                if spec_dict
                    .as_ref()
                    .is_some_and(|s| s.get_item("storageStats").ok().flatten().is_some())
                {
                    let ss = PyDict::new(py);
                    ss.set_item("count", count)?;
                    ss.set_item("size", data_size)?;
                    ss.set_item("avgObjSize", if count > 0 { data_size / count } else { 0 })?;
                    ss.set_item("storageSize", storage_size)?;
                    ss.set_item("freeStorageSize", 0)?;
                    ss.set_item("nindexes", nindexes)?;
                    ss.set_item("totalIndexSize", total_idx_size)?;
                    ss.set_item("totalSize", storage_size + total_idx_size)?;
                    ss.set_item("indexSizes", idx_sizes)?;
                    ss.set_item("scaleFactor", 1)?;
                    ss.set_item("storageEngine", se)?;
                    doc.set_item("storageStats", ss)?;
                }
                if spec_dict
                    .as_ref()
                    .is_some_and(|s| s.get_item("count").ok().flatten().is_some())
                {
                    doc.set_item("count", count)?;
                }

                let first_batch = PyList::new(py, [doc.as_any()])?;
                let cursor_dict = PyDict::new(py);
                cursor_dict.set_item("id", bson_int64(py, 0)?)?;
                cursor_dict.set_item("ns", &ns)?;
                cursor_dict.set_item("firstBatch", first_batch)?;
                let resp = PyDict::new(py);
                resp.set_item("cursor", cursor_dict)?;
                resp.set_item("ok", 1.0)?;
                return Ok(resp.into_any().unbind());
            }
            if d.get_item("$changeStream")?.is_some() {
                let change_pipeline = if pipeline.len() > 1 {
                    let rest = PyList::empty(py);
                    for i in 1..pipeline.len() {
                        rest.append(pipeline.get_item(i)?)?;
                    }
                    rest.into_any()
                } else {
                    py.None().into_bound(py)
                };
                let stream = if change_pipeline.is_none() {
                    coll.call_method1("watch", (py.None(),))?
                } else {
                    coll.call_method1("watch", (&change_pipeline,))?
                };
                let cr = ctx.borrow().cursor_registry.clone_ref(py);
                let cr_reg = cr.bind(py).cast::<crate::wire_cursors::CursorRegistry>()?;
                let cursor_id = cr_reg.borrow().create_change_stream(
                    py,
                    &ns,
                    stream.unbind(),
                    Some(batch_size as usize),
                )?;
                let cursor_dict = PyDict::new(py);
                cursor_dict.set_item("id", bson_int64(py, cursor_id)?)?;
                cursor_dict.set_item("ns", &ns)?;
                cursor_dict.set_item("firstBatch", PyList::empty(py))?;
                let resp = PyDict::new(py);
                resp.set_item("cursor", cursor_dict)?;
                resp.set_item("ok", 1.0)?;
                return Ok(resp.into_any().unbind());
            }
        }
    }

    let coll_typed = get_collection_typed(ctx, &db_name, &coll_name)?;
    let result = coll_typed
        .bind(py)
        .borrow()
        .aggregate_engine(py, pipeline, None)?;
    let result_bound = result.bind(py);

    let cr = ctx.borrow().cursor_registry.clone_ref(py);
    let cr_reg = cr.bind(py).cast::<crate::wire_cursors::CursorRegistry>()?;
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

fn cmd_map_reduce(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let r = make_error(
        py,
        "CommandNotSupported",
        "mapReduce is deprecated and not supported by smongo. Use aggregation instead.",
    )?;
    Ok(r.into_any().unbind())
}

pub(crate) fn register(m: &mut HashMap<&'static str, HandlerFn>) {
    m.insert("aggregate", cmd_aggregate);
    m.insert("mapReduce", cmd_map_reduce);
    m.insert("mapreduce", cmd_map_reduce);
}
