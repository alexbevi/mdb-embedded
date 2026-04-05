//! Aggregation join stages ported to Rust.
//!
//! `$lookup` (equality), `$graphLookup`, and `$facet` have their
//! compute-heavy inner loops in Rust while delegating collection I/O
//! to Python.

use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::aggregation;
use crate::paths;
use crate::query_compiler;
use crate::query_expressions;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn hashable_key(py: Python<'_>, val: &Bound<'_, PyAny>) -> PyResult<String> {
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

// ---------------------------------------------------------------------------
// $lookup (equality join)
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn lookup_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
    collection_getter: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyList>> {
    let from_coll: Option<String> = spec.get_item("from")?.and_then(|v| v.extract().ok());
    let local_field: Option<String> = spec.get_item("localField")?.and_then(|v| v.extract().ok());
    let foreign_field: Option<String> = spec.get_item("foreignField")?.and_then(|v| v.extract().ok());
    let as_field: Option<String> = spec.get_item("as")?.and_then(|v| v.extract().ok());

    let (from_coll, local_field, foreign_field, as_field) = match (
        from_coll,
        local_field,
        foreign_field,
        as_field,
    ) {
        (Some(fc), Some(lf), Some(ff), Some(af)) => (fc, lf, ff, af),
        (fc, lf, ff, af) => {
            let mut missing = Vec::new();
            if fc.is_none() {
                missing.push("from");
            }
            if lf.is_none() {
                missing.push("localField");
            }
            if ff.is_none() {
                missing.push("foreignField");
            }
            if af.is_none() {
                missing.push("as");
            }
            return Err(PyValueError::new_err(format!(
                "$lookup missing required fields: {}",
                missing.join(", ")
            )));
        }
    };

    let foreign_coll = get_foreign_collection(py, collection_getter, &from_coll)?;

    if let Some(ref fc) = foreign_coll {
        if foreign_has_index(fc, &foreign_field)? {
            return lookup_indexed(py, docs, fc, &local_field, &foreign_field, &as_field);
        }
    }

    let foreign_docs = get_all_docs(py, foreign_coll.as_ref())?;

    let mut foreign_index: HashMap<String, Vec<Py<PyAny>>> = HashMap::new();
    for fd in foreign_docs.iter() {
        let fv = paths::get_value(&fd, &foreign_field)?;
        let key = hashable_key(py, fv.bind(py))?;
        foreign_index.entry(key).or_default().push(fd.unbind());
    }

    let copy_mod = crate::cached_modules::copy_mod(py)?;
    let results = PyList::empty(py);
    for doc in docs.iter() {
        let new_doc = copy_mod.call_method1("deepcopy", (&doc,))?;
        let local_val = paths::get_value(&doc, &local_field)?;
        let key = hashable_key(py, local_val.bind(py))?;
        let matches = if let Some(v) = foreign_index.get(&key) {
            PyList::new(py, v.iter().map(|d| d.bind(py)))?
        } else {
            PyList::empty(py)
        };
        paths::set_value(&new_doc, &as_field, matches.into_any().unbind())?;
        results.append(new_doc)?;
    }
    Ok(results)
}

fn lookup_indexed<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    foreign_coll: &Bound<'py, PyAny>,
    local_field: &str,
    foreign_field: &str,
    as_field: &str,
) -> PyResult<Bound<'py, PyList>> {
    let copy_mod = crate::cached_modules::copy_mod(py)?;
    let results = PyList::empty(py);
    for doc in docs.iter() {
        let new_doc = copy_mod.call_method1("deepcopy", (&doc,))?;
        let local_val = paths::get_value(&doc, local_field)?;
        let matches = if !local_val.bind(py).is_none() {
            let query = PyDict::new(py);
            query.set_item(foreign_field, local_val.bind(py))?;
            let result = foreign_coll.call_method1("find", (query,))?;
            let items: Vec<Bound<'py, PyAny>> = result.try_iter()?.collect::<PyResult<_>>()?;
            PyList::new(py, items)?
        } else {
            PyList::empty(py)
        };
        paths::set_value(&new_doc, as_field, matches.into_any().unbind())?;
        results.append(new_doc)?;
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// $graphLookup (BFS)
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn graph_lookup_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
    collection_getter: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyList>> {
    let from_coll_name: String = spec
        .get_item("from")?
        .ok_or_else(|| PyValueError::new_err("$graphLookup missing 'from'"))?
        .extract()?;
    let start_with = spec
        .get_item("startWith")?
        .ok_or_else(|| PyValueError::new_err("$graphLookup missing 'startWith'"))?;
    let connect_from: String = spec
        .get_item("connectFromField")?
        .ok_or_else(|| PyValueError::new_err("$graphLookup missing 'connectFromField'"))?
        .extract()?;
    let connect_to: String = spec
        .get_item("connectToField")?
        .ok_or_else(|| PyValueError::new_err("$graphLookup missing 'connectToField'"))?
        .extract()?;
    let as_field: String = spec
        .get_item("as")?
        .ok_or_else(|| PyValueError::new_err("$graphLookup missing 'as'"))?
        .extract()?;
    let max_depth: Option<usize> = spec
        .get_item("maxDepth")?
        .and_then(|v| v.extract().ok());
    let depth_field: Option<String> = spec
        .get_item("depthField")?
        .and_then(|v| v.extract().ok());
    let restrict_search = spec.get_item("restrictSearchWithMatch")?;

    let foreign_coll = get_foreign_collection(py, collection_getter, &from_coll_name)?;
    let foreign_docs = get_all_docs(py, foreign_coll.as_ref())?;

    let mut foreign_index: HashMap<String, Vec<Py<PyAny>>> = HashMap::new();
    for fd in foreign_docs.iter() {
        let fv = paths::get_value(&fd, &connect_to)?;
        let key = hashable_key(py, fv.bind(py))?;
        foreign_index.entry(key).or_default().push(fd.unbind());
    }

    let restrict_query = restrict_search.as_ref().filter(|v| {
        if !v.is_instance_of::<PyDict>() {
            return false;
        }
        match v.cast::<PyDict>() {
            Ok(d) => !d.is_empty(),
            Err(_) => false,
        }
    });

    let copy_mod = crate::cached_modules::copy_mod(py)?;
    let results = PyList::empty(py);

    for doc in docs.iter() {
        let start_val = query_expressions::resolve_expr(doc.as_any(), &start_with)?;

        let mut frontier: Vec<Py<PyAny>> = if let Ok(list) = start_val.cast::<PyList>() {
            list.iter().map(|v| v.unbind()).collect()
        } else {
            vec![start_val.unbind()]
        };

        let mut visited = std::collections::HashSet::<String>::new();
        let matched_results = PyList::empty(py);
        let mut depth: usize = 0;

        while !frontier.is_empty() {
            if let Some(md) = max_depth {
                if depth > md {
                    break;
                }
            }
            let mut next_frontier: Vec<Py<PyAny>> = Vec::new();
            for val_py in &frontier {
                let val = val_py.bind(py);
                let key = hashable_key(py, val)?;
                if visited.contains(&key) {
                    continue;
                }
                visited.insert(key.clone());

                let lookup_key = hashable_key(py, val)?;
                if let Some(matches) = foreign_index.get(&lookup_key) {
                    for matched_py in matches {
                        let matched = matched_py.bind(py);
                        if let Some(rq) = restrict_query {
                            let rq_dict = rq.cast::<PyDict>()?;
                            if !query_compiler::eval_query(matched.cast::<PyDict>()?, rq_dict)? {
                                continue;
                            }
                        }
                        let r = copy_mod.call_method1("deepcopy", (matched,))?;
                        if let Some(ref df) = depth_field {
                            r.cast::<PyDict>()?.set_item(df, depth)?;
                        }
                        matched_results.append(&r)?;

                        let next_val = paths::get_value(matched, &connect_from)?;
                        if !next_val.bind(py).is_none() {
                            if let Ok(list) = next_val.bind(py).cast::<PyList>() {
                                for item in list.iter() {
                                    next_frontier.push(item.unbind());
                                }
                            } else {
                                next_frontier.push(next_val);
                            }
                        }
                    }
                }
            }
            frontier = next_frontier;
            depth += 1;
        }

        let new_doc = copy_mod.call_method1("deepcopy", (&doc,))?;
        paths::set_value(&new_doc, &as_field, matched_results.into_any().unbind())?;
        results.append(new_doc)?;
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// $facet (Rust sub-pipeline dispatch)
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (docs, spec, collection_getter=None, *, max_pipeline_docs=100_000))]
pub fn facet_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
    collection_getter: Option<&Bound<'py, PyAny>>,
    max_pipeline_docs: usize,
) -> PyResult<Bound<'py, PyList>> {
    let copy_mod = crate::cached_modules::copy_mod(py)?;
    let result = PyDict::new(py);

    for (facet_name, pipeline_obj) in spec.iter() {
        let docs_copy = copy_mod.call_method1("deepcopy", (docs,))?;
        let docs_list = docs_copy.cast::<PyList>()?;
        let pipeline = pipeline_obj.cast::<PyList>()?;

        let sub_result = aggregation::aggregate_pipeline(
            py,
            docs_list,
            pipeline,
            collection_getter,
            max_pipeline_docs,
            false,
            100 * 1024 * 1024,
        )?;
        result.set_item(facet_name, sub_result)?;
    }

    let out = PyList::empty(py);
    out.append(result)?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// $lookup with sub-pipeline
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (docs, spec, collection_getter=None, *, max_pipeline_docs=100_000))]
pub fn pipeline_lookup_stage<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
    spec: &Bound<'py, PyDict>,
    collection_getter: Option<&Bound<'py, PyAny>>,
    max_pipeline_docs: usize,
) -> PyResult<Bound<'py, PyList>> {
    let from_coll_name: String = spec
        .get_item("from")?
        .ok_or_else(|| PyValueError::new_err("$lookup with pipeline requires 'from'"))?
        .extract()?;
    let as_field: String = spec
        .get_item("as")?
        .ok_or_else(|| PyValueError::new_err("$lookup with pipeline requires 'as'"))?
        .extract()?;
    let let_vars = spec.get_item("let")?.unwrap_or_else(|| PyDict::new(py).into_any());
    let pipeline = spec.get_item("pipeline")?.unwrap_or_else(|| PyList::empty(py).into_any());

    let foreign_coll = get_foreign_collection(py, collection_getter, &from_coll_name)?
        .ok_or_else(|| PyValueError::new_err("$lookup with pipeline requires a collection getter"))?;

    let copy_mod = crate::cached_modules::copy_mod(py)?;
    let results = PyList::empty(py);

    for doc in docs.iter() {
        let mut scope: Vec<(String, Py<PyAny>)> = Vec::new();
        if let Ok(let_dict) = let_vars.cast::<PyDict>() {
            for (var_name, var_expr) in let_dict.iter() {
                let name: String = var_name.extract()?;
                let val = query_expressions::resolve_expr(doc.as_any(), &var_expr)?;
                scope.push((name, val.unbind()));
            }
        }

        let bound_pipeline = bind_pipeline_vars(py, &pipeline, &scope)?;

        let foreign_docs = get_all_docs(py, Some(&foreign_coll))?;
        let sub_result = aggregation::aggregate_pipeline(
            py,
            &foreign_docs,
            &bound_pipeline,
            collection_getter,
            max_pipeline_docs,
            false,
            100 * 1024 * 1024,
        )?;

        let new_doc = copy_mod.call_method1("deepcopy", (&doc,))?;
        paths::set_value(&new_doc, &as_field, sub_result.into_any().unbind())?;
        results.append(new_doc)?;
    }

    Ok(results)
}

fn bind_pipeline_vars<'py>(
    py: Python<'py>,
    pipeline: &Bound<'py, PyAny>,
    scope: &[(String, Py<PyAny>)],
) -> PyResult<Bound<'py, PyList>> {
    let joins_mod = crate::cached_modules::smongo_agg_joins(py)?;
    let bind_fn = joins_mod.getattr("_bind_pipeline_vars")?;
    let scope_dict = PyDict::new(py);
    for (name, val) in scope {
        scope_dict.set_item(name, val.bind(py))?;
    }
    let result = bind_fn.call1((pipeline, scope_dict))?;
    Ok(result.cast::<PyList>()?.clone())
}

// ---------------------------------------------------------------------------
// Collection helper utilities
// ---------------------------------------------------------------------------

fn get_foreign_collection<'py>(
    _py: Python<'py>,
    collection_getter: Option<&Bound<'py, PyAny>>,
    name: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if let Some(getter) = collection_getter {
        match getter.call1((name,)) {
            Ok(coll) => Ok(Some(coll)),
            Err(_) => Ok(None),
        }
    } else {
        Ok(None)
    }
}

fn get_all_docs<'py>(
    py: Python<'py>,
    coll: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyList>> {
    if let Some(c) = coll {
        if c.hasattr("get_all")? {
            let result = c.call_method0("get_all")?;
            if let Ok(list) = result.cast::<PyList>() {
                return Ok(list.clone());
            }
            let items: Vec<Bound<'py, PyAny>> = result.try_iter()?.collect::<PyResult<_>>()?;
            return PyList::new(py, items);
        }
        let result = c.call_method1("find", (PyDict::new(py),))?;
        let items: Vec<Bound<'py, PyAny>> = result.try_iter()?.collect::<PyResult<_>>()?;
        Ok(PyList::new(py, items)?)
    } else {
        Ok(PyList::empty(py))
    }
}

fn foreign_has_index(coll: &Bound<'_, PyAny>, field: &str) -> PyResult<bool> {
    let indexes = match coll.call_method0("list_indexes") {
        Ok(r) => r,
        Err(_) => return Ok(false),
    };
    let idx_items: Vec<Bound<'_, PyAny>> = match indexes.try_iter() {
        Ok(it) => it.collect::<PyResult<_>>()?,
        Err(_) => return Ok(false),
    };
    for idx in idx_items {
        if let Ok(d) = idx.cast::<PyDict>() {
            if let Some(keys) = d.get_item("keys")? {
                if let Ok(keys_list) = keys.cast::<PyList>() {
                    if !keys_list.is_empty() {
                        let first = keys_list.get_item(0)?;
                        if let Ok(tup) = first.cast::<pyo3::types::PyTuple>() {
                            if tup.len() >= 1 {
                                let key_name: String = tup.get_item(0)?.extract()?;
                                if key_name == field {
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(false)
}
