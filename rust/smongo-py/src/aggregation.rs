//! Aggregation pipeline stages ported to Rust.
//!
//! Each stage function receives a `PyList[PyDict]` of documents and a stage
//! spec, and returns a new `PyList[PyDict]`.  All heavy computation (expression
//! resolution, path traversal, query matching) uses the already-ported Rust
//! modules; only collection I/O crosses back to Python.

use std::collections::HashMap;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyList, PyNone};

use crate::paths;
use crate::query_compiler;
use crate::query_expressions;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn py_none(py: Python<'_>) -> Bound<'_, PyAny> {
    PyNone::get(py).to_owned().into_any()
}

fn py_bool(py: Python<'_>, val: bool) -> Bound<'_, PyAny> {
    PyBool::new(py, val).to_owned().into_any()
}

/// Serialize a Python value to a hashable string key for grouping.
fn to_group_key(py: Python<'_>, val: &Bound<'_, PyAny>) -> PyResult<String> {
    if val.is_none() {
        return Ok("__null__".to_string());
    }
    if let Ok(s) = val.extract::<String>() {
        return Ok(format!("s:{s}"));
    }
    if let Ok(b) = val.extract::<bool>() {
        return Ok(format!("b:{b}"));
    }
    if let Ok(i) = val.extract::<i64>() {
        return Ok(format!("i:{i}"));
    }
    if let Ok(f) = val.extract::<f64>() {
        return Ok(format!("f:{f}"));
    }
    let json_mod = crate::cached_modules::json_mod(py)?;
    let dumped: String = json_mod
        .call_method1("dumps", (val,))
        .and_then(|r| r.extract())?;
    Ok(format!("j:{dumped}"))
}

/// Extract (operator_name, stage_dict, spec_value) from a pipeline stage.
fn stage_parts<'py>(
    stage: &Bound<'py, PyAny>,
) -> PyResult<(String, Bound<'py, PyDict>, Bound<'py, PyAny>)> {
    let dict = stage.cast::<PyDict>()?.clone();
    let (op_obj, spec) = dict
        .iter()
        .next()
        .ok_or_else(|| PyValueError::new_err("empty pipeline stage"))?;
    Ok((op_obj.extract()?, dict, spec))
}

// ---------------------------------------------------------------------------
// $group
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (docs, spec, *, allow_disk_use=false))]
pub fn group_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
    allow_disk_use: bool,
) -> PyResult<Bound<'py, PyList>> {
    if allow_disk_use {
        let stages_mod = crate::cached_modules::smongo_agg_stages(py)?;
        let py_fn = stages_mod.getattr("_py_group_stage")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("allow_disk_use", true)?;
        let result = py_fn.call((docs, spec), Some(&kwargs))?;
        return Ok(result.cast::<PyList>()?.clone());
    }

    let id_expr = spec
        .get_item("_id")?
        .ok_or_else(|| PyValueError::new_err("$group requires '_id'"))?;

    let mut groups: Vec<(String, Py<PyAny>, Vec<Py<PyAny>>)> = Vec::new();
    let mut key_index: HashMap<String, usize> = HashMap::new();

    for doc in docs.iter() {
        let key_val = query_expressions::resolve_expr(doc.as_any(), &id_expr)?;
        let key_str = to_group_key(py, &key_val)?;

        if let Some(&idx) = key_index.get(&key_str) {
            groups[idx].2.push(doc.unbind());
        } else {
            let idx = groups.len();
            key_index.insert(key_str.clone(), idx);
            groups.push((key_str, key_val.unbind(), vec![doc.unbind()]));
        }
    }

    let results = PyList::empty(py);
    for (_key_str, key_val, group_docs) in &groups {
        let out = PyDict::new(py);
        out.set_item("_id", key_val.bind(py))?;

        for (field_obj, expr) in spec.iter() {
            let field: String = field_obj.extract()?;
            if field == "_id" {
                continue;
            }
            if !expr.is_instance_of::<PyDict>() {
                continue;
            }
            let group_list = PyList::new(py, group_docs.iter().map(|d| d.bind(py)))?;
            let val = eval_accumulator(py, expr.cast::<PyDict>()?, &group_list)?;
            out.set_item(field, val)?;
        }
        results.append(out)?;
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// Accumulator evaluation
// ---------------------------------------------------------------------------

fn eval_accumulator<'py>(
    py: Python<'py>,
    accum: &Bound<'py, PyDict>,
    group_docs: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyAny>> {
    let (op_obj, val) = accum
        .iter()
        .next()
        .ok_or_else(|| PyValueError::new_err("empty accumulator"))?;
    let op: String = op_obj.extract()?;

    match op.as_str() {
        "$sum" => accum_sum(py, &val, group_docs),
        "$avg" => accum_avg(py, &val, group_docs),
        "$min" => accum_min_max(py, &val, group_docs, false),
        "$max" => accum_min_max(py, &val, group_docs, true),
        "$first" => {
            if group_docs.is_empty() {
                Ok(py_none(py))
            } else {
                query_expressions::resolve_expr(group_docs.get_item(0)?.as_any(), &val)
            }
        }
        "$last" => {
            if group_docs.is_empty() {
                Ok(py_none(py))
            } else {
                let last = group_docs.get_item(group_docs.len() - 1)?;
                query_expressions::resolve_expr(last.as_any(), &val)
            }
        }
        "$push" => {
            let result = PyList::empty(py);
            for doc in group_docs.iter() {
                let v = query_expressions::resolve_expr(doc.as_any(), &val)?;
                result.append(v)?;
            }
            Ok(result.into_any())
        }
        "$addToSet" => {
            let result = PyList::empty(py);
            for doc in group_docs.iter() {
                let v = query_expressions::resolve_expr(doc.as_any(), &val)?;
                if !result.contains(&v)? {
                    result.append(v)?;
                }
            }
            Ok(result.into_any())
        }
        "$stdDevPop" => accum_stddev(py, &val, group_docs, false),
        "$stdDevSamp" => accum_stddev(py, &val, group_docs, true),
        "$mergeObjects" => {
            let result = PyDict::new(py);
            for doc in group_docs.iter() {
                let v = query_expressions::resolve_expr(doc.as_any(), &val)?;
                if let Ok(d) = v.cast::<PyDict>() {
                    result.update(d.as_mapping())?;
                }
            }
            Ok(result.into_any())
        }
        "$top" | "$bottom" => accum_top_bottom(py, &val, group_docs, &op, 1, true),
        "$topN" | "$bottomN" => {
            let n: usize = if let Ok(d) = val.cast::<PyDict>() {
                d.get_item("n")?.map_or(1, |v| v.extract().unwrap_or(1))
            } else {
                1
            };
            accum_top_bottom(py, &val, group_docs, &op, n, false)
        }
        "$firstN" => {
            let (n, input_expr) = extract_n_input(py, &val)?;
            let result = PyList::empty(py);
            for (i, doc) in group_docs.iter().enumerate() {
                if i >= n {
                    break;
                }
                let v = query_expressions::resolve_expr(doc.as_any(), &input_expr)?;
                result.append(v)?;
            }
            Ok(result.into_any())
        }
        "$lastN" => {
            let (n, input_expr) = extract_n_input(py, &val)?;
            let start = group_docs.len().saturating_sub(n);
            let result = PyList::empty(py);
            for i in start..group_docs.len() {
                let doc = group_docs.get_item(i)?;
                let v = query_expressions::resolve_expr(doc.as_any(), &input_expr)?;
                result.append(v)?;
            }
            Ok(result.into_any())
        }
        _ => Ok(py_none(py)),
    }
}

fn accum_sum<'py>(
    py: Python<'py>,
    val: &Bound<'py, PyAny>,
    group_docs: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(1i64) = val.extract::<i64>() {
        let len = group_docs.len() as i64;
        return Ok(len.into_pyobject(py)?.into_any());
    }
    let mut sum_f: f64 = 0.0;
    let mut all_int = true;
    let mut sum_i: i64 = 0;
    for doc in group_docs.iter() {
        let v = query_expressions::resolve_expr(doc.as_any(), val)?;
        if v.is_none() {
            continue;
        }
        if let Ok(i) = v.extract::<i64>() {
            sum_i = sum_i.wrapping_add(i);
            sum_f += i as f64;
        } else if let Ok(f) = v.extract::<f64>() {
            sum_f += f;
            all_int = false;
        }
    }
    if all_int {
        Ok(sum_i.into_pyobject(py)?.into_any())
    } else {
        Ok(PyFloat::new(py, sum_f).into_any())
    }
}

fn accum_avg<'py>(
    py: Python<'py>,
    val: &Bound<'py, PyAny>,
    group_docs: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut sum: f64 = 0.0;
    let mut count: usize = 0;
    for doc in group_docs.iter() {
        let v = query_expressions::resolve_expr(doc.as_any(), val)?;
        if v.is_none() {
            continue;
        }
        if let Ok(f) = v.extract::<f64>() {
            sum += f;
            count += 1;
        } else if let Ok(i) = v.extract::<i64>() {
            sum += i as f64;
            count += 1;
        }
    }
    if count == 0 {
        Ok(0i64.into_pyobject(py)?.into_any())
    } else {
        Ok(PyFloat::new(py, sum / count as f64).into_any())
    }
}

fn accum_min_max<'py>(
    py: Python<'py>,
    val: &Bound<'py, PyAny>,
    group_docs: &Bound<'py, PyList>,
    is_max: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let mut best: Option<Bound<'py, PyAny>> = None;
    for doc in group_docs.iter() {
        let v = query_expressions::resolve_expr(doc.as_any(), val)?;
        if v.is_none() {
            continue;
        }
        best = Some(match best {
            None => v,
            Some(ref cur) => {
                let cmp: bool = if is_max { v.gt(cur)? } else { v.lt(cur)? };
                if cmp {
                    v
                } else {
                    cur.clone()
                }
            }
        });
    }
    Ok(best.unwrap_or_else(|| py_none(py)))
}

fn accum_stddev<'py>(
    py: Python<'py>,
    val: &Bound<'py, PyAny>,
    group_docs: &Bound<'py, PyList>,
    is_sample: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let mut vals: Vec<f64> = Vec::new();
    for doc in group_docs.iter() {
        let v = query_expressions::resolve_expr(doc.as_any(), val)?;
        if v.is_none() {
            continue;
        }
        if let Ok(f) = v.extract::<f64>() {
            vals.push(f);
        } else if let Ok(i) = v.extract::<i64>() {
            vals.push(i as f64);
        }
    }
    if vals.is_empty() || (is_sample && vals.len() < 2) {
        return Ok(py_none(py));
    }
    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
    let variance: f64 = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>();
    let divisor = if is_sample {
        (vals.len() - 1) as f64
    } else {
        vals.len() as f64
    };
    Ok(PyFloat::new(py, (variance / divisor).sqrt()).into_any())
}

fn accum_top_bottom<'py>(
    py: Python<'py>,
    val: &Bound<'py, PyAny>,
    group_docs: &Bound<'py, PyList>,
    op: &str,
    n: usize,
    single: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let (sort_by, output_expr) = if let Ok(d) = val.cast::<PyDict>() {
        let sb = d
            .get_item("sortBy")?
            .unwrap_or_else(|| PyDict::new(py).into_any());
        let out = d.get_item("output")?.unwrap_or_else(|| val.clone());
        (sb, out)
    } else {
        (PyDict::new(py).into_any(), val.clone())
    };

    let sorted = if let Ok(sb_dict) = sort_by.cast::<PyDict>() {
        if !sb_dict.is_empty() {
            sort_stage(py, group_docs, sb_dict, false)?
        } else {
            group_docs.clone()
        }
    } else {
        group_docs.clone()
    };

    let is_bottom = op == "$bottom" || op == "$bottomN";

    if single {
        let idx = if is_bottom {
            sorted.len().saturating_sub(1)
        } else {
            0
        };
        if sorted.is_empty() {
            return Ok(py_none(py));
        }
        return query_expressions::resolve_expr(sorted.get_item(idx)?.as_any(), &output_expr);
    }

    let result = PyList::empty(py);
    if is_bottom {
        let start = sorted.len().saturating_sub(n);
        for i in (start..sorted.len()).rev() {
            let doc = sorted.get_item(i)?;
            let v = query_expressions::resolve_expr(doc.as_any(), &output_expr)?;
            result.append(v)?;
        }
    } else {
        for i in 0..n.min(sorted.len()) {
            let doc = sorted.get_item(i)?;
            let v = query_expressions::resolve_expr(doc.as_any(), &output_expr)?;
            result.append(v)?;
        }
    }
    Ok(result.into_any())
}

fn extract_n_input<'py>(
    _py: Python<'py>,
    val: &Bound<'py, PyAny>,
) -> PyResult<(usize, Bound<'py, PyAny>)> {
    if let Ok(d) = val.cast::<PyDict>() {
        let n: usize = d.get_item("n")?.map_or(1, |v| v.extract().unwrap_or(1));
        let input = d.get_item("input")?.unwrap_or_else(|| val.clone());
        Ok((n, input))
    } else {
        Ok((1, val.clone()))
    }
}

// ---------------------------------------------------------------------------
// $sort
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (docs, spec, *, allow_disk_use=false))]
pub fn sort_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
    allow_disk_use: bool,
) -> PyResult<Bound<'py, PyList>> {
    if allow_disk_use {
        let stages_mod = crate::cached_modules::smongo_agg_stages(py)?;
        let py_fn = stages_mod.getattr("_py_sort_stage")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("allow_disk_use", true)?;
        let result = py_fn.call((docs, spec), Some(&kwargs))?;
        return Ok(result.cast::<PyList>()?.clone());
    }

    let mut result: Vec<Py<PyAny>> = docs.iter().map(|d| d.unbind()).collect();
    let spec_items: Vec<(String, i32)> = spec
        .iter()
        .map(|(k, v)| Ok((k.extract::<String>()?, v.extract::<i32>()?)))
        .collect::<PyResult<_>>()?;

    for (field, direction) in spec_items.iter().rev() {
        let field = field.clone();
        let reverse = *direction == -1;

        let key_fn = pyo3::types::PyCFunction::new_closure(
            py,
            None,
            None,
            move |args: &Bound<'_, pyo3::types::PyTuple>,
                  _kwargs: Option<&Bound<'_, PyDict>>|
                  -> PyResult<Py<PyAny>> {
                let py = args.py();
                let doc = args.get_item(0)?;
                let value = paths::get_value(&doc, &field)?;
                let is_some = !value.bind(py).is_none();
                let tup =
                    pyo3::types::PyTuple::new(py, &[py_bool(py, is_some), value.into_bound(py)])?;
                Ok(tup.unbind().into())
            },
        )?;

        let builtins = crate::cached_modules::builtins(py)?;
        let sorted_fn = builtins.getattr("sorted")?;
        let items_list = PyList::new(py, result.iter().map(|d| d.bind(py)))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("key", key_fn)?;
        kwargs.set_item("reverse", reverse)?;
        let sorted_result = sorted_fn.call((items_list,), Some(&kwargs))?;
        let sorted_list = sorted_result.cast::<PyList>()?;
        result = sorted_list.iter().map(|d| d.unbind()).collect();
    }

    PyList::new(py, result.iter().map(|d| d.bind(py)))
}

// ---------------------------------------------------------------------------
// $unwind
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn unwind_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let (path, preserve_null) = if let Ok(s) = spec.extract::<String>() {
        let p = if let Some(stripped) = s.strip_prefix('$') {
            stripped.to_string()
        } else {
            s
        };
        (p, false)
    } else if let Ok(d) = spec.cast::<PyDict>() {
        let mut path_str: String = d
            .get_item("path")?
            .map_or_else(|| Ok(String::new()), |v| v.extract())?;
        if let Some(stripped) = path_str.strip_prefix('$') {
            path_str = stripped.to_string();
        }
        let preserve: bool = d
            .get_item("preserveNullAndEmptyArrays")?
            .is_some_and(|v| v.extract().unwrap_or(false));
        (path_str, preserve)
    } else {
        return Err(PyTypeError::new_err(
            "$unwind spec must be a string or dict",
        ));
    };

    let copy_mod = crate::cached_modules::copy_mod(py)?;
    let results = PyList::empty(py);

    for doc in docs.iter() {
        let val = paths::get_value(&doc, &path)?;
        let val_bound = val.bind(py);

        if let Ok(arr) = val_bound.cast::<PyList>() {
            if arr.is_empty() && preserve_null {
                let new_doc = copy_mod.call_method1("deepcopy", (&doc,))?;
                paths::set_value(&new_doc, &path, py_none(py).unbind())?;
                results.append(new_doc)?;
            } else {
                for item in arr.iter() {
                    let new_doc = copy_mod.call_method1("deepcopy", (&doc,))?;
                    paths::set_value(&new_doc, &path, item.unbind())?;
                    results.append(new_doc)?;
                }
            }
        } else if !val_bound.is_none() || preserve_null {
            results.append(&doc)?;
        }
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// $project
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn project_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let results = PyList::empty(py);

    // Determine if this is an exclusion projection (all numeric/bool values are 0/false).
    // MongoDB rule: a $project is exclusion-mode when every simple field spec is 0/false
    // (with the exception of _id which can be 0 in either mode).
    let mut has_exclusion = false;
    let mut has_inclusion = false;
    for (_field_obj, expr) in spec.iter() {
        let field: String = _field_obj.extract()?;
        if field == "_id" {
            continue; // _id: 0 is allowed in both modes
        }
        if let Ok(i) = expr.extract::<i64>() {
            if i == 0 {
                has_exclusion = true;
            } else {
                has_inclusion = true;
            }
        } else if let Ok(b) = expr.extract::<bool>() {
            if !b {
                has_exclusion = true;
            } else {
                has_inclusion = true;
            }
        } else {
            has_inclusion = true; // expression => inclusion mode
        }
    }
    let exclusion_mode = has_exclusion && !has_inclusion;

    for doc in docs.iter() {
        if exclusion_mode {
            // Exclusion: copy the entire doc, then remove excluded fields
            let copy_mod = crate::cached_modules::copy_mod(py)?;
            let new_doc = copy_mod.call_method1("deepcopy", (&doc,))?;
            let new_dict = new_doc.cast::<PyDict>()?;
            for (field_obj, expr) in spec.iter() {
                let field: String = field_obj.extract()?;
                let exclude = if let Ok(i) = expr.extract::<i64>() {
                    i == 0
                } else if let Ok(b) = expr.extract::<bool>() {
                    !b
                } else {
                    false
                };
                if exclude {
                    let _ = new_dict.del_item(&field);
                }
            }
            results.append(new_dict)?;
        } else {
            // Inclusion: build a new doc with only the specified fields
            let new_doc = PyDict::new(py);
            for (field_obj, expr) in spec.iter() {
                let field: String = field_obj.extract()?;
                if let Ok(i) = expr.extract::<i64>() {
                    if i == 1 {
                        let val = paths::get_value(&doc, &field)?;
                        new_doc.set_item(&field, val.bind(py))?;
                    }
                } else if let Ok(b) = expr.extract::<bool>() {
                    if b {
                        let val = paths::get_value(&doc, &field)?;
                        new_doc.set_item(&field, val.bind(py))?;
                    }
                } else {
                    let val = query_expressions::resolve_expr(doc.as_any(), &expr)?;
                    new_doc.set_item(&field, val)?;
                }
            }
            results.append(new_doc)?;
        }
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// $addFields / $set
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn add_fields_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let copy_mod = crate::cached_modules::copy_mod(py)?;
    let results = PyList::empty(py);

    for doc in docs.iter() {
        let new_doc = copy_mod.call_method1("deepcopy", (&doc,))?;
        for (field_obj, expr) in spec.iter() {
            let field: String = field_obj.extract()?;
            let val = query_expressions::resolve_expr(doc.as_any(), &expr)?;
            paths::set_value(&new_doc, &field, val.unbind())?;
        }
        results.append(new_doc)?;
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// $match
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn match_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let results = PyList::empty(py);
    for doc in docs.iter() {
        if query_compiler::eval_query(doc.cast::<PyDict>()?, spec)? {
            results.append(&doc)?;
        }
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// $limit
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn limit_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: usize,
) -> PyResult<Bound<'py, PyList>> {
    let results = PyList::empty(py);
    for (i, doc) in docs.iter().enumerate() {
        if i >= spec {
            break;
        }
        results.append(doc)?;
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// $skip
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn skip_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: usize,
) -> PyResult<Bound<'py, PyList>> {
    let results = PyList::empty(py);
    for (i, doc) in docs.iter().enumerate() {
        if i >= spec {
            results.append(doc)?;
        }
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// $count
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn count_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &str,
) -> PyResult<Bound<'py, PyList>> {
    let result = PyDict::new(py);
    let len = docs.len() as i64;
    result.set_item(spec, len)?;
    let out = PyList::empty(py);
    out.append(result)?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// $sample
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn sample_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let size: usize = spec
        .get_item("size")?
        .map_or(docs.len(), |v| v.extract().unwrap_or(docs.len()));
    let actual_size = size.min(docs.len());
    let random_mod = crate::cached_modules::random_mod(py)?;
    let result = random_mod.call_method1("sample", (docs, actual_size))?;
    Ok(result.cast::<PyList>()?.clone())
}

// ---------------------------------------------------------------------------
// $replaceRoot / $replaceWith
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn replace_root_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let new_root_expr = spec
        .get_item("newRoot")?
        .ok_or_else(|| PyValueError::new_err("$replaceRoot requires 'newRoot' expression"))?;
    let results = PyList::empty(py);
    for doc in docs.iter() {
        let new_root = query_expressions::resolve_expr(doc.as_any(), &new_root_expr)?;
        if !new_root.is_instance_of::<PyDict>() {
            return Err(PyTypeError::new_err(format!(
                "$replaceRoot requires 'newRoot' to evaluate to an object, got {}",
                new_root.get_type().name()?
            )));
        }
        results.append(new_root)?;
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// $unset
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn unset_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let copy_mod = crate::cached_modules::copy_mod(py)?;
    let fields: Vec<String> = if let Ok(s) = spec.extract::<String>() {
        vec![s]
    } else {
        spec.extract()?
    };
    let results = PyList::empty(py);
    for doc in docs.iter() {
        let new_doc = copy_mod.call_method1("deepcopy", (&doc,))?;
        for f in &fields {
            paths::unset_value(&new_doc, f)?;
        }
        results.append(new_doc)?;
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// $redact
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn redact_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let results = PyList::empty(py);
    for doc in docs.iter() {
        if let Some(result) = redact_doc(py, &doc, spec)? {
            results.append(result)?;
        }
    }
    Ok(results)
}

fn redact_doc<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyAny>,
    expr: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let val = query_expressions::resolve_expr(doc, expr)?;
    if let Ok(s) = val.extract::<String>() {
        match s.as_str() {
            "$$KEEP" => return Ok(Some(doc.clone())),
            "$$PRUNE" => return Ok(None),
            "$$DESCEND" => {
                let dict = doc.cast::<PyDict>()?;
                let new_doc = PyDict::new(py);
                for (k, v) in dict.iter() {
                    if v.is_instance_of::<PyDict>() {
                        if let Some(child) = redact_doc(py, &v, expr)? {
                            new_doc.set_item(&k, child)?;
                        }
                    } else if let Ok(arr) = v.cast::<PyList>() {
                        let new_arr = PyList::empty(py);
                        for elem in arr.iter() {
                            if elem.is_instance_of::<PyDict>() {
                                if let Some(child) = redact_doc(py, &elem, expr)? {
                                    new_arr.append(child)?;
                                }
                            } else {
                                new_arr.append(&elem)?;
                            }
                        }
                        new_doc.set_item(&k, new_arr)?;
                    } else {
                        new_doc.set_item(&k, &v)?;
                    }
                }
                return Ok(Some(new_doc.into_any()));
            }
            _ => {}
        }
    }
    Ok(Some(doc.clone()))
}

// ---------------------------------------------------------------------------
// $sortByCount
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn sort_by_count_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let mut counts: Vec<(String, Py<PyAny>, i64)> = Vec::new();
    let mut key_index: HashMap<String, usize> = HashMap::new();

    for doc in docs.iter() {
        let val = query_expressions::resolve_expr(doc.as_any(), spec)?;
        let key_str = to_group_key(py, &val)?;

        if let Some(&idx) = key_index.get(&key_str) {
            counts[idx].2 += 1;
        } else {
            let idx = counts.len();
            key_index.insert(key_str.clone(), idx);
            counts.push((key_str, val.unbind(), 1));
        }
    }

    counts.sort_by(|a, b| b.2.cmp(&a.2));

    let results = PyList::empty(py);
    for (_key_str, val, count) in &counts {
        let out = PyDict::new(py);
        out.set_item("_id", val.bind(py))?;
        out.set_item("count", *count)?;
        results.append(out)?;
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// $bucket
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn bucket_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let group_by = spec
        .get_item("groupBy")?
        .ok_or_else(|| PyValueError::new_err("$bucket requires 'groupBy'"))?;
    let boundaries_obj = spec
        .get_item("boundaries")?
        .ok_or_else(|| PyValueError::new_err("$bucket requires 'boundaries'"))?;
    let boundaries = boundaries_obj.cast::<PyList>()?;
    let default = spec.get_item("default")?;
    let output = spec.get_item("output")?;

    if boundaries.len() < 2 {
        return Err(PyValueError::new_err(
            "$bucket requires at least 2 boundaries",
        ));
    }

    let num_buckets = boundaries.len() - 1;
    let mut buckets: Vec<Vec<Py<PyAny>>> = (0..num_buckets).map(|_| Vec::new()).collect();
    let mut default_bucket: Vec<Py<PyAny>> = Vec::new();

    for doc in docs.iter() {
        let val = query_expressions::resolve_expr(doc.as_any(), &group_by)?;
        let mut placed = false;

        #[allow(clippy::needless_range_loop)]
        for i in 0..num_buckets {
            let lo = boundaries.get_item(i)?;
            let hi = boundaries.get_item(i + 1)?;
            if val.ge(&lo)? && val.lt(&hi)? {
                buckets[i].push(doc.clone().unbind());
                placed = true;
                break;
            }
        }
        if !placed && default.is_some() {
            default_bucket.push(doc.unbind());
        }
    }

    let results = PyList::empty(py);
    #[allow(clippy::needless_range_loop)]
    for i in 0..num_buckets {
        let out = PyDict::new(py);
        let bucket_id = boundaries.get_item(i)?;
        out.set_item("_id", &bucket_id)?;
        if let Some(ref output_spec) = output {
            let output_dict = output_spec.cast::<PyDict>()?;
            let group_list = PyList::new(py, buckets[i].iter().map(|d| d.bind(py)))?;
            for (field, accum) in output_dict.iter() {
                let val = eval_accumulator(py, accum.cast::<PyDict>()?, &group_list)?;
                out.set_item(field, val)?;
            }
        } else {
            out.set_item("count", buckets[i].len())?;
        }
        results.append(out)?;
    }

    if let Some(ref def_val) = default {
        let out = PyDict::new(py);
        out.set_item("_id", def_val)?;
        if let Some(ref output_spec) = output {
            let output_dict = output_spec.cast::<PyDict>()?;
            let group_list = PyList::new(py, default_bucket.iter().map(|d| d.bind(py)))?;
            for (field, accum) in output_dict.iter() {
                let val = eval_accumulator(py, accum.cast::<PyDict>()?, &group_list)?;
                out.set_item(field, val)?;
            }
        } else {
            out.set_item("count", default_bucket.len())?;
        }
        results.append(out)?;
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// $bucketAuto
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn bucket_auto_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let group_by = spec
        .get_item("groupBy")?
        .ok_or_else(|| PyValueError::new_err("$bucketAuto requires 'groupBy'"))?;
    let granularity: usize = spec
        .get_item("buckets")?
        .map_or(5, |v| v.extract().unwrap_or(5));
    let output = spec.get_item("output")?;

    let mut vals: Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>)> = Vec::with_capacity(docs.len());
    for doc in docs.iter() {
        let v = query_expressions::resolve_expr(doc.as_any(), &group_by)?;
        vals.push((v, doc));
    }

    vals.sort_by(|a, b| {
        let a_some = !a.0.is_none();
        let b_some = !b.0.is_none();
        a_some
            .cmp(&b_some)
            .then_with(|| a.0.lt(&b.0).unwrap_or(false).cmp(&true).reverse())
    });

    let n = granularity.max(1);
    let chunk_size = vals.len().div_ceil(n).max(1);
    let results = PyList::empty(py);

    for chunk in vals.chunks(chunk_size) {
        if chunk.is_empty() {
            continue;
        }
        let lo = &chunk[0].0;
        let hi = &chunk[chunk.len() - 1].0;
        let group_docs = PyList::new(py, chunk.iter().map(|(_, d)| d))?;
        let out = PyDict::new(py);
        let id_dict = PyDict::new(py);
        id_dict.set_item("min", lo)?;
        id_dict.set_item("max", hi)?;
        out.set_item("_id", id_dict)?;
        out.set_item("count", chunk.len())?;
        if let Some(ref output_spec) = output {
            let output_dict = output_spec.cast::<PyDict>()?;
            for (field, accum) in output_dict.iter() {
                let val = eval_accumulator(py, accum.cast::<PyDict>()?, &group_docs)?;
                out.set_item(field, val)?;
            }
        }
        results.append(out)?;
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// $setWindowFields
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn set_window_fields_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let partition_by = spec.get_item("partitionBy")?;
    let sort_by = spec.get_item("sortBy")?;
    let output_spec = spec
        .get_item("output")?
        .unwrap_or_else(|| PyDict::new(py).into_any());

    let working_docs = if let Some(ref sb) = sort_by {
        if let Ok(sb_dict) = sb.cast::<PyDict>() {
            if !sb_dict.is_empty() {
                sort_stage(py, docs, sb_dict, false)?
            } else {
                docs.clone()
            }
        } else {
            docs.clone()
        }
    } else {
        docs.clone()
    };

    let mut partitions: Vec<(String, Vec<Py<PyAny>>)> = Vec::new();
    let mut part_index: HashMap<String, usize> = HashMap::new();

    for doc in working_docs.iter() {
        let key = if let Some(ref pb) = partition_by {
            let k = query_expressions::resolve_expr(doc.as_any(), pb)?;
            to_group_key(py, &k)?
        } else {
            "__none__".to_string()
        };
        if let Some(&idx) = part_index.get(&key) {
            partitions[idx].1.push(doc.unbind());
        } else {
            let idx = partitions.len();
            part_index.insert(key.clone(), idx);
            partitions.push((key, vec![doc.unbind()]));
        }
    }

    let copy_mod = crate::cached_modules::copy_mod(py)?;
    let results = PyList::empty(py);
    let output_dict = output_spec.cast::<PyDict>()?;

    for (_pk, part_docs) in &partitions {
        let part_len = part_docs.len();
        for (i, doc_py) in part_docs.iter().enumerate() {
            let doc = doc_py.bind(py);
            let new_doc = copy_mod.call_method1("deepcopy", (doc,))?;

            for (field_obj, window_spec_obj) in output_dict.iter() {
                let field: String = field_obj.extract()?;
                if !window_spec_obj.is_instance_of::<PyDict>() {
                    continue;
                }
                let window_spec = window_spec_obj.cast::<PyDict>()?;

                let (accum_op, accum_val) = window_spec
                    .iter()
                    .next()
                    .ok_or_else(|| PyValueError::new_err("empty window spec"))?;
                let op_str: String = accum_op.extract()?;
                if op_str == "$window" {
                    continue;
                }

                let window = window_spec.get_item("window")?;
                let (lo, hi) = if let Some(ref w) = window {
                    let w_dict = w.cast::<PyDict>()?;
                    if let Some(docs_window) = w_dict.get_item("documents")? {
                        let dw = docs_window.cast::<PyList>()?;
                        if dw.len() == 2 {
                            let lo_bound: i64 =
                                dw.get_item(0)?.extract().unwrap_or(-(part_len as i64));
                            let hi_bound: i64 =
                                dw.get_item(1)?.extract().unwrap_or(part_len as i64);
                            let lo = (i as i64 + lo_bound).max(0) as usize;
                            let hi =
                                ((i as i64 + hi_bound + 1).min(part_len as i64)).max(0) as usize;
                            (lo, hi)
                        } else {
                            (0, part_len)
                        }
                    } else {
                        (0, part_len)
                    }
                } else {
                    (0, part_len)
                };

                let window_docs = PyList::new(py, part_docs[lo..hi].iter().map(|d| d.bind(py)))?;
                let accum_dict = PyDict::new(py);
                accum_dict.set_item(&op_str, &accum_val)?;
                let val = eval_accumulator(py, &accum_dict, &window_docs)?;
                paths::set_value(&new_doc, &field, val.unbind())?;
            }
            results.append(new_doc)?;
        }
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// Engine fast path — delegates to smongo_engine::aggregation when possible
// ---------------------------------------------------------------------------

const PYTHON_REQUIRED_STAGES: &[&str] = &[
    "$lookup",
    "$graphLookup",
    "$facet",
    "$out",
    "$merge",
    "$unionWith",
    "$vectorSearch",
    "$geoNear",
];

fn pipeline_is_engine_compatible(pipeline: &Bound<'_, PyList>) -> PyResult<bool> {
    for stage_obj in pipeline.iter() {
        let (op, _, _) = stage_parts(&stage_obj)?;
        if PYTHON_REQUIRED_STAGES.contains(&op.as_str()) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn run_engine_pipeline<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    pipeline: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyList>> {
    let bson_docs: Vec<bson::Document> = docs
        .iter()
        .map(|d| crate::bson_helpers::pydict_to_doc(d.cast::<PyDict>()?))
        .collect::<PyResult<_>>()?;

    let bson_pipeline: Vec<bson::Document> = pipeline
        .iter()
        .map(|s| crate::bson_helpers::pydict_to_doc(s.cast::<PyDict>()?))
        .collect::<PyResult<_>>()?;

    let results = smongo_engine::aggregation::aggregate(bson_docs, &bson_pipeline)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    let out = PyList::empty(py);
    for doc in &results {
        out.append(crate::bson_helpers::doc_to_pydict(py, doc)?)?;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// aggregate_pipeline -- full pipeline dispatch in Rust
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (docs, pipeline, *, collection_getter=None, max_pipeline_docs=100_000, allow_disk_use=false, memory_limit_bytes=104_857_600))]
pub fn aggregate_pipeline<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    pipeline: &Bound<'py, PyList>,
    collection_getter: Option<&Bound<'py, PyAny>>,
    max_pipeline_docs: usize,
    allow_disk_use: bool,
    memory_limit_bytes: usize,
) -> PyResult<Bound<'py, PyList>> {
    if !allow_disk_use
        && collection_getter.is_none()
        && pipeline_is_engine_compatible(pipeline)?
    {
        return run_engine_pipeline(py, docs, pipeline);
    }

    let constants_mod = crate::cached_modules::smongo_agg_constants(py)?;
    let estimate_fn = constants_mod.getattr("_estimate_docs_bytes")?;
    let doc_limit_exc = constants_mod.getattr("DocumentLimitExceeded")?;
    let mem_limit_exc = constants_mod.getattr("MemoryLimitExceeded")?;

    let optimized = optimize_pipeline(py, pipeline)?;
    let mut current = docs.clone();

    for stage_obj in optimized.iter() {
        let (op, _, spec) = stage_parts(&stage_obj)?;

        let sd = match op.as_str() {
            "$match" | "$group" | "$project" | "$sort" | "$addFields" | "$set" | "$sample"
            | "$bucket" | "$bucketAuto" | "$setWindowFields" | "$replaceRoot" | "$lookup"
            | "$graphLookup" | "$facet" => Some(spec.cast::<PyDict>()?),
            _ => None,
        };
        let stage_dict = || {
            pyo3::exceptions::PyValueError::new_err(format!(
                "stage {op} requires a document argument"
            ))
        };

        current = match op.as_str() {
            "$match" => match_stage(py, &current, sd.ok_or_else(&stage_dict)?)?,
            "$group" => {
                check_memory(
                    py,
                    &current,
                    "$group",
                    memory_limit_bytes,
                    allow_disk_use,
                    &estimate_fn,
                    &mem_limit_exc,
                )?;
                group_stage(py, &current, sd.ok_or_else(&stage_dict)?, allow_disk_use)?
            }
            "$project" => project_stage(py, &current, sd.ok_or_else(&stage_dict)?)?,
            "$sort" => {
                check_memory(
                    py,
                    &current,
                    "$sort",
                    memory_limit_bytes,
                    allow_disk_use,
                    &estimate_fn,
                    &mem_limit_exc,
                )?;
                sort_stage(py, &current, sd.ok_or_else(&stage_dict)?, allow_disk_use)?
            }
            "$limit" => {
                let n: usize = spec.extract()?;
                limit_stage(py, &current, n)?
            }
            "$skip" => {
                let n: usize = spec.extract()?;
                skip_stage(py, &current, n)?
            }
            "$unwind" => unwind_stage(py, &current, &spec)?,
            "$addFields" | "$set" => add_fields_stage(py, &current, sd.ok_or_else(&stage_dict)?)?,
            "$count" => {
                let name: String = spec.extract()?;
                count_stage(py, &current, &name)?
            }
            "$replaceRoot" => replace_root_stage(py, &current, sd.ok_or_else(&stage_dict)?)?,
            "$replaceWith" => {
                let wrapper = PyDict::new(py);
                wrapper.set_item("newRoot", &spec)?;
                replace_root_stage(py, &current, &wrapper)?
            }
            "$sample" => sample_stage(py, &current, sd.ok_or_else(&stage_dict)?)?,
            "$bucket" => bucket_stage(py, &current, sd.ok_or_else(&stage_dict)?)?,
            "$bucketAuto" => bucket_auto_stage(py, &current, sd.ok_or_else(&stage_dict)?)?,
            "$unset" => unset_stage(py, &current, &spec)?,
            "$redact" => redact_stage(py, &current, &spec)?,
            "$sortByCount" => sort_by_count_stage(py, &current, &spec)?,
            "$setWindowFields" => {
                set_window_fields_stage(py, &current, sd.ok_or_else(&stage_dict)?)?
            }
            "$lookup" => {
                let lookup_dict = sd.ok_or_else(&stage_dict)?;
                if lookup_dict.contains("pipeline")? && !lookup_dict.contains("localField")? {
                    crate::aggregation_joins::pipeline_lookup_stage(
                        py,
                        &current,
                        lookup_dict,
                        collection_getter,
                        max_pipeline_docs,
                    )?
                } else {
                    crate::aggregation_joins::lookup_stage(
                        py,
                        &current,
                        lookup_dict,
                        collection_getter,
                    )?
                }
            }
            "$graphLookup" => crate::aggregation_joins::graph_lookup_stage(
                py,
                &current,
                sd.ok_or_else(&stage_dict)?,
                collection_getter,
            )?,
            "$facet" => crate::aggregation_joins::facet_stage(
                py,
                &current,
                sd.ok_or_else(&stage_dict)?,
                collection_getter,
                max_pipeline_docs,
            )?,
            "$out" | "$merge" | "$unionWith" | "$vectorSearch" | "$geoNear" => {
                dispatch_python_stage(
                    py,
                    &current,
                    &stage_obj,
                    &op,
                    &spec,
                    collection_getter,
                    max_pipeline_docs,
                )?
            }
            _ => {
                return Err(pyo3::exceptions::PyNotImplementedError::new_err(format!(
                    "Aggregation stage {op} not supported"
                )));
            }
        };

        check_doc_limit(py, &current, max_pipeline_docs, &doc_limit_exc)?;
    }

    Ok(current)
}

/// Dispatch I/O-dominated stages to their Python implementations.
fn dispatch_python_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    _stage_obj: &Bound<'py, PyAny>,
    op: &str,
    spec: &Bound<'py, PyAny>,
    collection_getter: Option<&Bound<'py, PyAny>>,
    max_pipeline_docs: usize,
) -> PyResult<Bound<'py, PyList>> {
    let spec_dict = spec.cast::<PyDict>();
    match op {
        "$out" => {
            let output_mod = crate::cached_modules::smongo_agg_output(py)?;
            let func = output_mod.getattr("out_stage")?;
            let result = func.call1((docs, spec, collection_getter))?;
            Ok(result.cast::<PyList>()?.clone())
        }
        "$merge" => {
            let output_mod = crate::cached_modules::smongo_agg_output(py)?;
            let func = output_mod.getattr("merge_stage")?;
            let result = func.call1((docs, spec_dict?, collection_getter))?;
            Ok(result.cast::<PyList>()?.clone())
        }
        "$unionWith" => {
            let joins_mod = crate::cached_modules::smongo_agg_joins(py)?;
            let func = joins_mod.getattr("union_with_stage")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("max_pipeline_docs", max_pipeline_docs)?;
            let result = func.call((docs, spec_dict?, collection_getter), Some(&kwargs))?;
            Ok(result.cast::<PyList>()?.clone())
        }
        "$vectorSearch" => {
            let vector_mod = crate::cached_modules::smongo_agg_vector(py)?;
            let func = vector_mod.getattr("vector_search_stage")?;
            let result = func.call1((docs, spec_dict?))?;
            Ok(result.cast::<PyList>()?.clone())
        }
        "$geoNear" => {
            let geo_mod = crate::cached_modules::smongo_agg_geo(py)?;
            let func = geo_mod.getattr("geo_near_stage")?;
            let result = func.call1((docs, spec_dict?))?;
            Ok(result.cast::<PyList>()?.clone())
        }
        _ => Err(pyo3::exceptions::PyNotImplementedError::new_err(format!(
            "Stage {op} not supported in Rust dispatch"
        ))),
    }
}

fn check_doc_limit<'py>(
    _py: Python<'py>,
    docs: &Bound<'py, PyList>,
    limit: usize,
    exc_cls: &Bound<'py, PyAny>,
) -> PyResult<()> {
    if docs.len() > limit {
        let msg = format!(
            "Aggregation produced {} documents, exceeding the limit of {}. \
             Add earlier $match / $limit stages or raise max_pipeline_docs.",
            docs.len(),
            limit
        );
        let err = exc_cls.call1((msg,))?;
        Err(PyErr::from_value(err))
    } else {
        Ok(())
    }
}

fn check_memory<'py>(
    _py: Python<'py>,
    docs: &Bound<'py, PyList>,
    stage_name: &str,
    memory_limit_bytes: usize,
    allow_disk_use: bool,
    estimate_fn: &Bound<'py, PyAny>,
    exc_cls: &Bound<'py, PyAny>,
) -> PyResult<()> {
    let est: usize = estimate_fn.call1((docs,))?.extract()?;
    if est > memory_limit_bytes && !allow_disk_use {
        let mb = est as f64 / (1024.0 * 1024.0);
        let limit_mb = memory_limit_bytes as f64 / (1024.0 * 1024.0);
        let msg = format!(
            "{stage_name} requires ~{mb:.0} MB, exceeding the \
             {limit_mb:.0} MB limit. Pass allowDiskUse=True to \
             enable spill-to-disk for memory-intensive stages."
        );
        let err = exc_cls.call1((msg,))?;
        Err(PyErr::from_value(err))
    } else {
        Ok(())
    }
}

fn optimize_pipeline<'py>(
    py: Python<'py>,
    pipeline: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyList>> {
    let len = pipeline.len();
    if len < 2 {
        return Ok(pipeline.clone());
    }

    let out = PyList::empty(py);
    let mut i = 0;
    while i < len {
        let stage = pipeline.get_item(i)?;
        let (op, stage_dict, _) = stage_parts(&stage)?;

        if op == "$match" && i + 1 < len {
            let next_stage = pipeline.get_item(i + 1)?;
            let (next_op, next_dict, _) = stage_parts(&next_stage)?;
            if next_op == "$match" {
                let merged = PyDict::new(py);
                let and_list = PyList::new(
                    py,
                    [
                        stage_dict.get_item("$match")?.ok_or_else(|| {
                            PyValueError::new_err("$match stage missing predicate")
                        })?,
                        next_dict.get_item("$match")?.ok_or_else(|| {
                            PyValueError::new_err("$match stage missing predicate")
                        })?,
                    ],
                )?;
                let match_dict = PyDict::new(py);
                match_dict.set_item("$and", and_list)?;
                merged.set_item("$match", match_dict)?;
                out.append(merged)?;
                i += 2;
                continue;
            }
        }

        if op == "$limit" && i + 1 < len {
            let next_stage = pipeline.get_item(i + 1)?;
            let (next_op, _, _) = stage_parts(&next_stage)?;
            if next_op == "$match" {
                out.append(&next_stage)?;
                out.append(&stage)?;
                i += 2;
                continue;
            }
        }

        out.append(&stage)?;
        i += 1;
    }

    Ok(out)
}
