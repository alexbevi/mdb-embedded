//! MongoDB update operator application (`$set`, `$unset`, `$inc`, `$push`, etc.).
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::paths;
use crate::query_compiler;
use crate::query_expressions;

/// Get a value at `path`, returning `Some(list)` if it's a `PyList`, `None` otherwise.
fn get_path_as_list<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyAny>,
    path: &str,
) -> PyResult<Option<Bound<'py, PyList>>> {
    let val = paths::get_value(doc, path)?;
    let val_bound = val.into_bound(py);
    match val_bound.cast::<PyList>() {
        Ok(l) => Ok(Some(l.clone())),
        Err(_) => Ok(None),
    }
}

/// Get a value at `path`, coercing to a list (None -> empty, non-list -> wrap).
fn get_path_coerce_array<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyDict>,
    path: &str,
) -> PyResult<Bound<'py, PyList>> {
    let val = paths::get_value(doc.as_any(), path)?;
    let val_bound = val.into_bound(py);
    if val_bound.is_none() {
        Ok(PyList::empty(py))
    } else if let Ok(l) = val_bound.cast::<PyList>() {
        Ok(l.clone())
    } else {
        PyList::new(py, [&val_bound])
    }
}

#[pyfunction]
#[pyo3(signature = (doc, update, *, array_filters=None, query=None))]
pub fn apply_update<'py>(
    doc: &Bound<'py, PyDict>,
    update: &Bound<'py, PyAny>,
    array_filters: Option<&Bound<'py, PyList>>,
    query: Option<&Bound<'py, PyDict>>,
) -> PyResult<()> {
    let py = doc.py();

    if let Ok(pipeline) = update.cast::<PyList>() {
        return apply_pipeline_update(py, doc, pipeline);
    }

    let update_dict = update.cast::<PyDict>()?;

    let mut filter_map: Vec<(String, Bound<'py, PyDict>)> = Vec::new();
    if let Some(afs) = array_filters {
        for af_any in afs.iter() {
            let af = af_any.cast::<PyDict>()?;
            for key in af.keys() {
                let k: String = key.extract()?;
                let ident = k.split('.').next().unwrap_or(&k).to_string();
                filter_map.push((ident, af.clone()));
            }
        }
    }

    for (op_obj, fields_obj) in update_dict.iter() {
        let op: String = op_obj.extract()?;
        let fields = fields_obj.cast::<PyDict>()?;

        match op.as_str() {
            "$set" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    set_with_positional(py, doc, &k, v.unbind(), query, &filter_map)?;
                }
            }
            "$inc" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    let cur = get_positional(py, doc, &k, query, &filter_map)?;
                    let cur_bound = cur.into_bound(py);
                    let new_val = if cur_bound.is_none() {
                        v.clone()
                    } else {
                        let builtins = crate::cached_modules::operator_mod(py)?;
                        builtins.getattr("add")?.call1((&cur_bound, &v))?
                    };
                    set_with_positional(py, doc, &k, new_val.unbind(), query, &filter_map)?;
                }
            }
            "$push" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    apply_push(py, doc, &k, &v)?;
                }
            }
            "$unset" => {
                for key in fields.keys() {
                    let k: String = key.extract()?;
                    paths::unset_value(doc.as_any(), &k)?;
                }
            }
            "$addToSet" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    apply_add_to_set(py, doc, &k, &v)?;
                }
            }
            "$pull" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    apply_pull(py, doc, &k, &v)?;
                }
            }
            "$pop" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    if let Some(arr) = get_path_as_list(py, doc.as_any(), &k)? {
                        if !arr.is_empty() {
                            let direction: i64 = v.extract()?;
                            if direction == -1 {
                                arr.as_any().call_method1("pop", (0,))?;
                            } else {
                                arr.as_any().call_method0("pop")?;
                            }
                            paths::set_value(doc.as_any(), &k, arr.as_any().clone().unbind())?;
                        }
                    }
                }
            }
            "$min" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    let cur = paths::get_value(doc.as_any(), &k)?;
                    let cur_bound = cur.into_bound(py);
                    if cur_bound.is_none() || v.lt(&cur_bound)? {
                        paths::set_value(doc.as_any(), &k, v.unbind())?;
                    }
                }
            }
            "$max" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    let cur = paths::get_value(doc.as_any(), &k)?;
                    let cur_bound = cur.into_bound(py);
                    if cur_bound.is_none() || v.gt(&cur_bound)? {
                        paths::set_value(doc.as_any(), &k, v.unbind())?;
                    }
                }
            }
            "$mul" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    let cur = paths::get_value(doc.as_any(), &k)?;
                    let cur_bound = cur.into_bound(py);
                    let base = if cur_bound.is_none() {
                        0i64.into_pyobject(py)?.into_any()
                    } else {
                        cur_bound
                    };
                    let result = py.import("operator")?.getattr("mul")?.call1((&base, &v))?;
                    paths::set_value(doc.as_any(), &k, result.unbind())?;
                }
            }
            "$rename" => {
                for (old_obj, new_obj) in fields.iter() {
                    let old_name: String = old_obj.extract()?;
                    let new_name: String = new_obj.extract()?;
                    let val = paths::get_value(doc.as_any(), &old_name)?;
                    if !val.bind(py).is_none() {
                        paths::unset_value(doc.as_any(), &old_name)?;
                        paths::set_value(doc.as_any(), &new_name, val)?;
                    }
                }
            }
            "$currentDate" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    if let Ok(d) = v.cast::<PyDict>() {
                        if let Some(t) = d.get_item("$type")? {
                            if t.extract::<String>()? == "timestamp" {
                                let time_mod = crate::cached_modules::time_mod(py)?;
                                let ts = time_mod.call_method0("time")?;
                                paths::set_value(doc.as_any(), &k, ts.unbind())?;
                                continue;
                            }
                        }
                    }
                    let dt_mod = crate::cached_modules::datetime(py)?;
                    let dt = dt_mod.getattr("datetime")?;
                    let utc = dt_mod.getattr("UTC")?;
                    let now = dt.call_method1("now", (utc,))?;
                    let iso = now.call_method0("isoformat")?;
                    paths::set_value(doc.as_any(), &k, iso.unbind())?;
                }
            }
            "$bit" => {
                for (k_obj, v) in fields.iter() {
                    let k: String = k_obj.extract()?;
                    let cur = paths::get_value(doc.as_any(), &k)?;
                    let cur_bound = cur.into_bound(py);
                    let mut val: i64 = if cur_bound.is_none() {
                        0
                    } else {
                        cur_bound.extract()?
                    };
                    if let Ok(ops) = v.cast::<PyDict>() {
                        if let Some(and_v) = ops.get_item("and")? {
                            val &= and_v.extract::<i64>()?;
                        }
                        if let Some(or_v) = ops.get_item("or")? {
                            val |= or_v.extract::<i64>()?;
                        }
                        if let Some(xor_v) = ops.get_item("xor")? {
                            val ^= xor_v.extract::<i64>()?;
                        }
                    }
                    paths::set_value(doc.as_any(), &k, val.into_pyobject(py)?.into_any().unbind())?;
                }
            }
            _ => {
                return Err(PyNotImplementedError::new_err(format!(
                    "Update operator {op} not supported"
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pipeline updates
// ---------------------------------------------------------------------------

fn apply_pipeline_update<'py>(
    _py: Python<'py>,
    doc: &Bound<'py, PyDict>,
    pipeline: &Bound<'py, PyList>,
) -> PyResult<()> {
    for stage_any in pipeline.iter() {
        let stage = stage_any.cast::<PyDict>()?;
        let (op_obj, spec) = stage.iter().next().ok_or_else(|| {
            PyRuntimeError::new_err("pipeline update stage must be a non-empty dict")
        })?;
        let op: String = op_obj.extract()?;

        match op.as_str() {
            "$set" | "$addFields" => {
                let spec_dict = spec.cast::<PyDict>()?;
                for (field_obj, expr) in spec_dict.iter() {
                    let field: String = field_obj.extract()?;
                    let resolved = query_expressions::resolve_expr(doc.as_any(), &expr)?;
                    paths::set_value(doc.as_any(), &field, resolved.unbind())?;
                }
            }
            "$unset" => {
                if let Ok(s) = spec.extract::<String>() {
                    paths::unset_value(doc.as_any(), &s)?;
                } else if let Ok(list) = spec.cast::<PyList>() {
                    for f in list.iter() {
                        let field: String = f.extract()?;
                        paths::unset_value(doc.as_any(), &field)?;
                    }
                }
            }
            "$replaceRoot" => {
                let spec_dict = spec.cast::<PyDict>()?;
                if let Some(nr) = spec_dict.get_item("newRoot")? {
                    let new_root = query_expressions::resolve_expr(doc.as_any(), &nr)?;
                    if let Ok(new_dict) = new_root.cast::<PyDict>() {
                        doc.call_method0("clear")?;
                        doc.call_method1("update", (new_dict,))?;
                    }
                }
            }
            "$replaceWith" => {
                let new_root = query_expressions::resolve_expr(doc.as_any(), &spec)?;
                if let Ok(new_dict) = new_root.cast::<PyDict>() {
                    doc.call_method0("clear")?;
                    doc.call_method1("update", (new_dict,))?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $push
// ---------------------------------------------------------------------------

fn apply_push<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyDict>,
    key: &str,
    value: &Bound<'py, PyAny>,
) -> PyResult<()> {
    let arr = get_path_coerce_array(py, doc, key)?;

    if let Ok(d) = value.cast::<PyDict>() {
        if let Some(each_val) = d.get_item("$each")? {
            let items = each_val.cast::<PyList>()?;
            let pos = d.get_item("$position")?;
            if let Some(pos_val) = pos {
                if let Ok(p) = pos_val.extract::<usize>() {
                    for (i, item) in items.iter().enumerate() {
                        arr.as_any().call_method1("insert", (p + i, &item))?;
                    }
                } else {
                    arr.as_any().call_method1("extend", (items,))?;
                }
            } else {
                arr.as_any().call_method1("extend", (items,))?;
            }

            if let Some(sort_spec) = d.get_item("$sort")? {
                apply_push_sort(py, &arr, &sort_spec)?;
            }

            if let Some(slice_val) = d.get_item("$slice")? {
                let sl: isize = slice_val.extract()?;
                let new_arr = if sl >= 0 {
                    arr.get_slice(0, sl as usize)
                } else {
                    let start = (arr.len() as isize + sl).max(0) as usize;
                    arr.get_slice(start, arr.len())
                };
                paths::set_value(doc.as_any(), key, new_arr.into_any().unbind())?;
                return Ok(());
            }
        } else {
            arr.append(value)?;
        }
    } else {
        arr.append(value)?;
    }

    paths::set_value(doc.as_any(), key, arr.into_any().unbind())
}

fn apply_push_sort<'py>(
    py: Python<'py>,
    arr: &Bound<'py, PyList>,
    sort_spec: &Bound<'py, PyAny>,
) -> PyResult<()> {
    if let Ok(direction) = sort_spec.extract::<i64>() {
        let kwargs = PyDict::new(py);
        kwargs.set_item("reverse", direction == -1)?;
        arr.as_any().call_method("sort", (), Some(&kwargs))?;
    } else if let Ok(spec_dict) = sort_spec.cast::<PyDict>() {
        let items: Vec<(String, i64)> = spec_dict
            .iter()
            .map(|(k, v)| Ok((k.extract::<String>()?, v.extract::<i64>()?)))
            .collect::<PyResult<_>>()?;

        for (sf, sd) in items.iter().rev() {
            let locals = PyDict::new(py);
            let gv = pyo3::wrap_pyfunction!(crate::paths::get_value, py)?;
            locals.set_item("_gv", &gv)?;
            locals.set_item("_f", sf)?;
            let key_fn = py.eval(
                c"(lambda gv, f: lambda item: (gv(item, f) if isinstance(item, dict) else item) or 0)(_gv, _f)",
                None,
                Some(&locals),
            )?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("key", &key_fn)?;
            kwargs.set_item("reverse", *sd == -1)?;
            arr.as_any().call_method("sort", (), Some(&kwargs))?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $addToSet
// ---------------------------------------------------------------------------

fn apply_add_to_set<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyDict>,
    key: &str,
    value: &Bound<'py, PyAny>,
) -> PyResult<()> {
    let arr = get_path_coerce_array(py, doc, key)?;

    if let Ok(d) = value.cast::<PyDict>() {
        if let Some(each_val) = d.get_item("$each")? {
            let items = each_val.cast::<PyList>()?;
            for item in items.iter() {
                if !arr.contains(&item)? {
                    arr.append(&item)?;
                }
            }
            paths::set_value(doc.as_any(), key, arr.into_any().unbind())?;
            return Ok(());
        }
    }

    if !arr.contains(value)? {
        arr.append(value)?;
    }
    paths::set_value(doc.as_any(), key, arr.into_any().unbind())
}

// ---------------------------------------------------------------------------
// $pull
// ---------------------------------------------------------------------------

fn apply_pull<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyDict>,
    key: &str,
    value: &Bound<'py, PyAny>,
) -> PyResult<()> {
    let arr = match get_path_as_list(py, doc.as_any(), key)? {
        Some(l) => l,
        None => return Ok(()),
    };

    let new_arr = PyList::empty(py);
    if let Ok(d) = value.cast::<PyDict>() {
        for item in arr.iter() {
            if item.is_instance_of::<PyDict>() {
                if !query_compiler::eval_query(item.cast::<PyDict>()?, d)? {
                    new_arr.append(&item)?;
                }
            } else {
                new_arr.append(&item)?;
            }
        }
    } else {
        for item in arr.iter() {
            if item.ne(value)? {
                new_arr.append(&item)?;
            }
        }
    }
    paths::set_value(doc.as_any(), key, new_arr.into_any().unbind())
}

// ---------------------------------------------------------------------------
// Positional operators
// ---------------------------------------------------------------------------

fn find_positional_index<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyDict>,
    array_path: &str,
    query: Option<&Bound<'py, PyDict>>,
) -> PyResult<Option<usize>> {
    let query = match query {
        Some(q) => q,
        None => return Ok(None),
    };
    let arr = match get_path_as_list(py, doc.as_any(), array_path)? {
        Some(l) => l,
        None => return Ok(None),
    };

    let prefix = format!("{array_path}.");
    for (qk_obj, qv) in query.iter() {
        let qk: String = qk_obj.extract()?;
        if qk.starts_with(&prefix) {
            let sub_field = &qk[prefix.len()..];
            for (i, elem) in arr.iter().enumerate() {
                if elem.is_instance_of::<PyDict>() {
                    let elem_val = paths::get_value(&elem, sub_field)?;
                    if elem_val.bind(py).eq(&qv)? {
                        return Ok(Some(i));
                    }
                }
            }
        } else if qk == array_path {
            if let Ok(qv_dict) = qv.cast::<PyDict>() {
                for (i, elem) in arr.iter().enumerate() {
                    let test_doc: Bound<'_, PyDict> = if let Ok(d) = elem.cast::<PyDict>() {
                        d.clone()
                    } else {
                        let d = PyDict::new(py);
                        d.set_item(&qk, &elem)?;
                        d
                    };
                    if query_compiler::eval_query(&test_doc, qv_dict)? {
                        return Ok(Some(i));
                    }
                }
            } else {
                for (i, elem) in arr.iter().enumerate() {
                    if elem.eq(&qv)? {
                        return Ok(Some(i));
                    }
                }
            }
        }
    }

    if arr.is_empty() {
        Ok(None)
    } else {
        Ok(Some(0))
    }
}

fn set_with_positional<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyDict>,
    path: &str,
    value: Py<PyAny>,
    query: Option<&Bound<'py, PyDict>>,
    filter_map: &[(String, Bound<'py, PyDict>)],
) -> PyResult<()> {
    if path.contains(".$.") || path.ends_with(".$") {
        let parts: Vec<&str> = path.splitn(2, ".$").collect();
        let array_path = parts[0];
        if let Some(idx) = find_positional_index(py, doc, array_path, query)? {
            let remainder = parts.get(1).unwrap_or(&"").trim_start_matches('.');
            let actual_path = if remainder.is_empty() {
                format!("{array_path}.{idx}")
            } else {
                format!("{array_path}.{idx}.{remainder}")
            };
            paths::set_value(doc.as_any(), &actual_path, value)?;
        }
        return Ok(());
    }

    if path.contains(".$[]") {
        let parts: Vec<&str> = path.splitn(2, ".$[]").collect();
        let array_path = parts[0];
        let remainder = parts.get(1).unwrap_or(&"").trim_start_matches('.');
        if let Some(arr) = get_path_as_list(py, doc.as_any(), array_path)? {
            for i in 0..arr.len() {
                let actual_path = if remainder.is_empty() {
                    format!("{array_path}.{i}")
                } else {
                    format!("{array_path}.{i}.{remainder}")
                };
                paths::set_value(doc.as_any(), &actual_path, value.clone_ref(py))?;
            }
        }
        return Ok(());
    }

    let re_mod = crate::cached_modules::re_mod(py)?;
    let m = re_mod.call_method1("search", (r"\.\$\[(\w+)\]", path))?;
    if !m.is_none() {
        let ident: String = m.call_method1("group", (1,))?.extract()?;
        let start: usize = m.call_method0("start")?.extract()?;
        let end: usize = m.call_method0("end")?.extract()?;
        let array_path = &path[..start];
        let remainder = path[end..].trim_start_matches('.');

        let af = filter_map
            .iter()
            .find(|(id, _)| id == &ident)
            .map(|(_, d)| d);

        if let (Some(arr), Some(af_dict)) = (get_path_as_list(py, doc.as_any(), array_path)?, af) {
            let stripped = PyDict::new(py);
            for (k_obj, v) in af_dict.iter() {
                let k: String = k_obj.extract()?;
                let new_key = if k.contains('.') {
                    k.split('.').nth(1).unwrap_or(&k).to_string()
                } else {
                    k
                };
                stripped.set_item(new_key, &v)?;
            }
            for (i, elem) in arr.iter().enumerate() {
                let test_doc: Bound<'_, PyDict> = if let Ok(d) = elem.cast::<PyDict>() {
                    d.clone()
                } else {
                    let d = PyDict::new(py);
                    d.set_item("", &elem)?;
                    d
                };
                if query_compiler::eval_query(&test_doc, &stripped)? {
                    let actual_path = if remainder.is_empty() {
                        format!("{array_path}.{i}")
                    } else {
                        format!("{array_path}.{i}.{remainder}")
                    };
                    paths::set_value(doc.as_any(), &actual_path, value.clone_ref(py))?;
                }
            }
        }
        return Ok(());
    }

    paths::set_value(doc.as_any(), path, value)
}

fn get_positional<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyDict>,
    path: &str,
    query: Option<&Bound<'py, PyDict>>,
    _filter_map: &[(String, Bound<'py, PyDict>)],
) -> PyResult<Py<PyAny>> {
    if path.contains(".$.") || path.ends_with(".$") {
        let parts: Vec<&str> = path.splitn(2, ".$").collect();
        let array_path = parts[0];
        if let Some(idx) = find_positional_index(py, doc, array_path, query)? {
            let remainder = parts.get(1).unwrap_or(&"").trim_start_matches('.');
            let actual_path = if remainder.is_empty() {
                format!("{array_path}.{idx}")
            } else {
                format!("{array_path}.{idx}.{remainder}")
            };
            return paths::get_value(doc.as_any(), &actual_path);
        }
        return Ok(py.None());
    }
    paths::get_value(doc.as_any(), path)
}
