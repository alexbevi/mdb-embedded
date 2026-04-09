//! Aggregation expression evaluator (`$add`, `$concat`, `$cond`, etc.).
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};

use crate::paths;

#[pyfunction]
pub fn resolve_expr<'py>(
    doc: &Bound<'py, PyAny>,
    expr: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    resolve_inner(doc, expr)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn py_none(py: Python<'_>) -> Bound<'_, PyAny> {
    py.None().into_bound(py)
}

fn py_bool(py: Python<'_>, val: bool) -> Bound<'_, PyAny> {
    PyBool::new(py, val).to_owned().into_any()
}

fn dict_get<'py>(
    d: &Bound<'py, PyDict>,
    key: &str,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    Ok(d.get_item(key)?.unwrap_or_else(|| py_none(py)))
}

fn any_none(vals: &[Bound<'_, PyAny>]) -> bool {
    vals.iter().any(|v| v.is_none())
}

fn has_float(vals: &[Bound<'_, PyAny>]) -> bool {
    vals.iter().any(|v| v.is_instance_of::<PyFloat>())
}

fn copy_dict<'py>(d: &Bound<'py, PyDict>) -> PyResult<Bound<'py, PyDict>> {
    let copy = d.call_method0("copy")?;
    Ok(copy.cast_into::<PyDict>()?)
}

fn py_mod_i64(a: i64, b: i64) -> i64 {
    let r = a % b;
    if r != 0 && ((r ^ b) < 0) {
        r + b
    } else {
        r
    }
}

fn py_mod_f64(a: f64, b: f64) -> f64 {
    let r = a % b;
    if r != 0.0 && r.is_sign_negative() != b.is_sign_negative() {
        r + b
    } else {
        r
    }
}

// ---------------------------------------------------------------------------
// Arg pre-classification -- cast arg once, reuse everywhere
// ---------------------------------------------------------------------------

enum ExprArg<'py> {
    Dict(Bound<'py, PyDict>),
    List(Bound<'py, PyList>),
    Scalar,
}

impl<'py> ExprArg<'py> {
    fn as_list(&self) -> Option<&Bound<'py, PyList>> {
        if let ExprArg::List(l) = self {
            Some(l)
        } else {
            None
        }
    }
    fn as_dict(&self) -> Option<&Bound<'py, PyDict>> {
        if let ExprArg::Dict(d) = self {
            Some(d)
        } else {
            None
        }
    }
}

fn classify_arg<'py>(arg: &Bound<'py, PyAny>) -> ExprArg<'py> {
    if let Ok(d) = arg.cast::<PyDict>() {
        ExprArg::Dict(d.clone())
    } else if let Ok(l) = arg.cast::<PyList>() {
        ExprArg::List(l.clone())
    } else {
        ExprArg::Scalar
    }
}

fn resolve_pylist<'py>(
    doc: &Bound<'py, PyAny>,
    list: &Bound<'py, PyList>,
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let mut result = Vec::with_capacity(list.len());
    for item in list.iter() {
        result.push(resolve_inner(doc, &item)?);
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Core resolution -- mirrors Python resolve_expr
// ---------------------------------------------------------------------------

fn resolve_inner<'py>(
    doc: &Bound<'py, PyAny>,
    expr: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let py = doc.py();

    if expr.is_none() {
        return Ok(py_none(py));
    }

    if let Ok(s) = expr.extract::<String>() {
        if let Some(rest) = s.strip_prefix("$$") {
            return match rest {
                "ROOT" | "CURRENT" => Ok(doc.clone()),
                _ => {
                    let doc_dict = doc.cast::<PyDict>()?;
                    match doc_dict.get_item(rest)? {
                        Some(v) => Ok(v),
                        None => Err(PyValueError::new_err(format!(
                            "Unsupported system variable: {s}"
                        ))),
                    }
                }
            };
        }
        if let Some(field) = s.strip_prefix('$') {
            return Ok(paths::get_value(doc, field)?.into_bound(py));
        }
        return Ok(expr.clone());
    }

    let expr_dict = match expr.cast::<PyDict>() {
        Ok(d) => d,
        Err(_) => return Ok(expr.clone()),
    };

    if expr_dict.len() == 1 {
        let (op_obj, arg) = expr_dict
            .iter()
            .next()
            .ok_or_else(|| PyValueError::new_err("expected single-key expression dict"))?;
        let op: String = op_obj.extract()?;
        if op.starts_with('$') {
            return eval_expr_op(py, &op, &arg, doc);
        }
    }

    let result = PyDict::new(py);
    for (k, v) in expr_dict.iter() {
        result.set_item(&k, resolve_inner(doc, &v)?)?;
    }
    Ok(result.into_any())
}

// ---------------------------------------------------------------------------
// Operator dispatch
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn eval_expr_op<'py>(
    py: Python<'py>,
    op: &str,
    arg: &Bound<'py, PyAny>,
    doc: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let ea = classify_arg(arg);

    match op {
        // ── Conditional ─────────────────────────────────────────────
        "$cond" => match &ea {
            ExprArg::Dict(d) => {
                let cond = resolve_inner(doc, &dict_get(d, "if", py)?)?;
                if cond.is_truthy()? {
                    resolve_inner(doc, &dict_get(d, "then", py)?)
                } else {
                    resolve_inner(doc, &dict_get(d, "else", py)?)
                }
            }
            ExprArg::List(l) if l.len() == 3 => {
                let cond = resolve_inner(doc, &l.get_item(0)?)?;
                if cond.is_truthy()? {
                    resolve_inner(doc, &l.get_item(1)?)
                } else {
                    resolve_inner(doc, &l.get_item(2)?)
                }
            }
            _ => Ok(py_none(py)),
        },

        "$ifNull" => {
            if let Some(l) = ea.as_list() {
                if l.len() >= 2 {
                    let val = resolve_inner(doc, &l.get_item(0)?)?;
                    return if val.is_none() {
                        resolve_inner(doc, &l.get_item(1)?)
                    } else {
                        Ok(val)
                    };
                }
            }
            Ok(py_none(py))
        }

        "$switch" => {
            let d = ea
                .as_dict()
                .ok_or_else(|| PyValueError::new_err("$switch requires a document"))?;
            let branches = dict_get(d, "branches", py)?;
            if let Ok(br_list) = branches.cast::<PyList>() {
                for branch in br_list.iter() {
                    let br = branch.cast::<PyDict>()?;
                    let case_val = resolve_inner(doc, &dict_get(br, "case", py)?)?;
                    if case_val.is_truthy()? {
                        return resolve_inner(doc, &dict_get(br, "then", py)?);
                    }
                }
            }
            resolve_inner(doc, &dict_get(d, "default", py)?)
        }

        // ── String ──────────────────────────────────────────────────
        "$concat" => {
            let l = ea
                .as_list()
                .ok_or_else(|| PyValueError::new_err("$concat requires an array"))?;
            let vals = resolve_pylist(doc, l)?;
            if any_none(&vals) {
                return Ok(py_none(py));
            }
            let mut result = String::new();
            for v in &vals {
                result.push_str(&v.str()?.extract::<String>()?);
            }
            Ok(result.into_pyobject(py)?.into_any())
        }

        "$toUpper" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_instance_of::<PyString>() {
                let s: String = val.extract()?;
                Ok(s.to_uppercase().into_pyobject(py)?.into_any())
            } else {
                Ok(py_none(py))
            }
        }

        "$toLower" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_instance_of::<PyString>() {
                let s: String = val.extract()?;
                Ok(s.to_lowercase().into_pyobject(py)?.into_any())
            } else {
                Ok(py_none(py))
            }
        }

        "$substr" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 3 {
                    let s = resolve_inner(doc, &l.get_item(0)?)?;
                    let start = resolve_inner(doc, &l.get_item(1)?)?;
                    let length = resolve_inner(doc, &l.get_item(2)?)?;
                    if let Ok(s_str) = s.extract::<String>() {
                        let start_i: usize = start.extract()?;
                        let len_i: usize = length.extract()?;
                        let chars: Vec<char> = s_str.chars().collect();
                        let end_i = (start_i + len_i).min(chars.len());
                        let start_clamped = start_i.min(chars.len());
                        let result: String = chars[start_clamped..end_i].iter().collect();
                        return Ok(result.into_pyobject(py)?.into_any());
                    }
                }
            }
            Ok(py_none(py))
        }

        "$strLenCP" => {
            let val = resolve_inner(doc, arg)?;
            if let Ok(s) = val.extract::<String>() {
                Ok((s.len() as i64).into_pyobject(py)?.into_any())
            } else {
                Ok(py_none(py))
            }
        }

        // ── Array ───────────────────────────────────────────────────
        "$arrayElemAt" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let arr = resolve_inner(doc, &l.get_item(0)?)?;
                    let idx = resolve_inner(doc, &l.get_item(1)?)?;
                    if let Ok(arr_list) = arr.cast::<PyList>() {
                        if let Ok(i) = idx.extract::<isize>() {
                            let len = arr_list.len() as isize;
                            if -len <= i && i < len {
                                let actual = if i < 0 {
                                    (len + i) as usize
                                } else {
                                    i as usize
                                };
                                return arr_list.get_item(actual);
                            }
                        }
                    }
                }
            }
            Ok(py_none(py))
        }

        "$size" => {
            let val = resolve_inner(doc, arg)?;
            if let Ok(l) = val.cast::<PyList>() {
                Ok((l.len() as i64).into_pyobject(py)?.into_any())
            } else {
                Ok(py_none(py))
            }
        }

        "$filter" => {
            let d = ea
                .as_dict()
                .ok_or_else(|| PyValueError::new_err("$filter requires a document"))?;
            let input_arr = resolve_inner(doc, &dict_get(d, "input", py)?)?;
            let as_name: String = d
                .get_item("as")?
                .map(|v| v.extract::<String>())
                .transpose()?
                .unwrap_or_else(|| "this".to_string());
            let cond_expr = dict_get(d, "cond", py)?;

            let input_list = match input_arr.cast::<PyList>() {
                Ok(l) => l,
                Err(_) => return Ok(py_none(py)),
            };

            let result = PyList::empty(py);
            let doc_dict = doc.cast::<PyDict>()?;
            for item in input_list.iter() {
                let scoped = copy_dict(doc_dict)?;
                scoped.set_item(&as_name, &item)?;
                if resolve_inner(scoped.as_any(), &cond_expr)?.is_truthy()? {
                    result.append(&item)?;
                }
            }
            Ok(result.into_any())
        }

        "$concatArrays" => {
            let list = ea
                .as_list()
                .ok_or_else(|| PyValueError::new_err("$concatArrays requires an array"))?;
            let result = PyList::empty(py);
            for a in list.iter() {
                let val = resolve_inner(doc, &a)?;
                match val.cast::<PyList>() {
                    Ok(l) => {
                        for item in l.iter() {
                            result.append(&item)?;
                        }
                    }
                    Err(_) => return Ok(py_none(py)),
                }
            }
            Ok(result.into_any())
        }

        "$in" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let val = resolve_inner(doc, &l.get_item(0)?)?;
                    let arr = resolve_inner(doc, &l.get_item(1)?)?;
                    if let Ok(arr_list) = arr.cast::<PyList>() {
                        return Ok(py_bool(py, arr_list.contains(&val)?));
                    }
                    return Ok(py_bool(py, false));
                }
            }
            Ok(py_bool(py, false))
        }

        // ── Arithmetic ──────────────────────────────────────────────
        "$add" => {
            let l = ea
                .as_list()
                .ok_or_else(|| PyValueError::new_err("$add requires an array"))?;
            let vals = resolve_pylist(doc, l)?;
            if any_none(&vals) {
                return Ok(py_none(py));
            }
            if has_float(&vals) {
                let mut total: f64 = 0.0;
                for v in &vals {
                    total += v.extract::<f64>()?;
                }
                Ok(total.into_pyobject(py)?.into_any())
            } else {
                let mut total: i64 = 0;
                for v in &vals {
                    total = total.wrapping_add(v.extract::<i64>()?);
                }
                Ok(total.into_pyobject(py)?.into_any())
            }
        }

        "$subtract" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let a = resolve_inner(doc, &l.get_item(0)?)?;
                    let b = resolve_inner(doc, &l.get_item(1)?)?;
                    if !a.is_none() && !b.is_none() {
                        if a.is_instance_of::<PyFloat>() || b.is_instance_of::<PyFloat>() {
                            let af: f64 = a.extract()?;
                            let bf: f64 = b.extract()?;
                            return Ok((af - bf).into_pyobject(py)?.into_any());
                        }
                        let ai: i64 = a.extract()?;
                        let bi: i64 = b.extract()?;
                        return Ok(ai.wrapping_sub(bi).into_pyobject(py)?.into_any());
                    }
                }
            }
            Ok(py_none(py))
        }

        "$multiply" => {
            let l = ea
                .as_list()
                .ok_or_else(|| PyValueError::new_err("$multiply requires an array"))?;
            let vals = resolve_pylist(doc, l)?;
            if any_none(&vals) {
                return Ok(py_none(py));
            }
            if has_float(&vals) {
                let mut product: f64 = 1.0;
                for v in &vals {
                    product *= v.extract::<f64>()?;
                }
                Ok(product.into_pyobject(py)?.into_any())
            } else {
                let mut product: i64 = 1;
                for v in &vals {
                    product = product.wrapping_mul(v.extract::<i64>()?);
                }
                Ok(product.into_pyobject(py)?.into_any())
            }
        }

        "$divide" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let a = resolve_inner(doc, &l.get_item(0)?)?;
                    let b = resolve_inner(doc, &l.get_item(1)?)?;
                    if !a.is_none() && !b.is_none() {
                        let bf: f64 = b.extract()?;
                        if bf != 0.0 {
                            let af: f64 = a.extract()?;
                            return Ok((af / bf).into_pyobject(py)?.into_any());
                        }
                    }
                }
            }
            Ok(py_none(py))
        }

        "$mod" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let a = resolve_inner(doc, &l.get_item(0)?)?;
                    let b = resolve_inner(doc, &l.get_item(1)?)?;
                    if !a.is_none() && !b.is_none() {
                        if a.is_instance_of::<PyFloat>() || b.is_instance_of::<PyFloat>() {
                            let af: f64 = a.extract()?;
                            let bf: f64 = b.extract()?;
                            if bf != 0.0 {
                                return Ok(py_mod_f64(af, bf).into_pyobject(py)?.into_any());
                            }
                        } else {
                            let ai: i64 = a.extract()?;
                            let bi: i64 = b.extract()?;
                            if bi != 0 {
                                return Ok(py_mod_i64(ai, bi).into_pyobject(py)?.into_any());
                            }
                        }
                    }
                }
            }
            Ok(py_none(py))
        }

        "$abs" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_none() {
                return Ok(py_none(py));
            }
            if val.is_instance_of::<PyFloat>() {
                let v: f64 = val.extract()?;
                Ok(v.abs().into_pyobject(py)?.into_any())
            } else {
                let v: i64 = val.extract()?;
                Ok(v.abs().into_pyobject(py)?.into_any())
            }
        }

        "$ceil" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_none() {
                return Ok(py_none(py));
            }
            let v: f64 = val.extract()?;
            Ok((v.ceil() as i64).into_pyobject(py)?.into_any())
        }

        "$floor" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_none() {
                return Ok(py_none(py));
            }
            let v: f64 = val.extract()?;
            Ok((v.floor() as i64).into_pyobject(py)?.into_any())
        }

        "$round" => {
            if let Some(l) = ea.as_list() {
                let val = resolve_inner(doc, &l.get_item(0)?)?;
                if val.is_none() {
                    return Ok(py_none(py));
                }
                let places: i32 = if l.len() > 1 {
                    resolve_inner(doc, &l.get_item(1)?)?.extract().unwrap_or(0)
                } else {
                    0
                };
                crate::cached_modules::builtins_round(py)?.call1((&val, places))
            } else {
                Ok(py_none(py))
            }
        }

        // ── Comparison (expression form) ────────────────────────────
        "$eq" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let a = resolve_inner(doc, &l.get_item(0)?)?;
                    let b = resolve_inner(doc, &l.get_item(1)?)?;
                    return Ok(py_bool(py, a.eq(&b)?));
                }
            }
            Ok(py_bool(py, false))
        }

        "$ne" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let a = resolve_inner(doc, &l.get_item(0)?)?;
                    let b = resolve_inner(doc, &l.get_item(1)?)?;
                    return Ok(py_bool(py, a.ne(&b)?));
                }
            }
            Ok(py_bool(py, true))
        }

        "$gt" => cmp_op(py, &ea, doc, |a, b| a.gt(b)),
        "$lt" => cmp_op(py, &ea, doc, |a, b| a.lt(b)),
        "$gte" => cmp_op(py, &ea, doc, |a, b| a.ge(b)),
        "$lte" => cmp_op(py, &ea, doc, |a, b| a.le(b)),

        // ── Boolean ─────────────────────────────────────────────────
        "$and" => {
            let list = ea
                .as_list()
                .ok_or_else(|| PyValueError::new_err("$and requires an array"))?;
            for item in list.iter() {
                if !resolve_inner(doc, &item)?.is_truthy()? {
                    return Ok(py_bool(py, false));
                }
            }
            Ok(py_bool(py, true))
        }

        "$or" => {
            let list = ea
                .as_list()
                .ok_or_else(|| PyValueError::new_err("$or requires an array"))?;
            for item in list.iter() {
                if resolve_inner(doc, &item)?.is_truthy()? {
                    return Ok(py_bool(py, true));
                }
            }
            Ok(py_bool(py, false))
        }

        "$not" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 1 {
                    let val = resolve_inner(doc, &l.get_item(0)?)?;
                    return Ok(py_bool(py, !val.is_truthy()?));
                }
            }
            let val = resolve_inner(doc, arg)?;
            Ok(py_bool(py, !val.is_truthy()?))
        }

        // ── Type ────────────────────────────────────────────────────
        "$type" => {
            let val = resolve_inner(doc, arg)?;
            let type_str = if val.is_none() {
                "null"
            } else if val.is_instance_of::<PyBool>() {
                "bool"
            } else if val.is_instance_of::<PyInt>() {
                "int"
            } else if val.is_instance_of::<PyFloat>() {
                "double"
            } else if val.is_instance_of::<PyString>() {
                "string"
            } else if val.is_instance_of::<PyList>() {
                "array"
            } else if val.is_instance_of::<PyDict>() {
                "object"
            } else {
                "unknown"
            };
            Ok(type_str.into_pyobject(py)?.into_any())
        }

        "$literal" => Ok(arg.clone()),

        // ── Date ────────────────────────────────────────────────────
        "$dateFromString" => {
            let d = match ea.as_dict() {
                Some(d) => d,
                None => return Ok(py_none(py)),
            };
            let date_str = resolve_inner(doc, &dict_get(d, "dateString", py)?)?;
            if date_str.is_instance_of::<PyString>() {
                let s: String = date_str.extract()?;
                let normalized = s.replace("Z", "+00:00");
                let dt_class = crate::cached_modules::datetime_datetime_cls(py)?;
                match dt_class.call_method1("fromisoformat", (normalized,)) {
                    Ok(dt) => dt.call_method0("isoformat"),
                    Err(_) => {
                        if let Some(on_err) = d.get_item("onError")? {
                            resolve_inner(doc, &on_err)
                        } else {
                            Ok(py_none(py))
                        }
                    }
                }
            } else {
                Ok(py_none(py))
            }
        }

        "$toDate" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_instance_of::<PyString>() {
                let s: String = val.extract()?;
                let normalized = s.replace("Z", "+00:00");
                let dt_class = crate::cached_modules::datetime_datetime_cls(py)?;
                match dt_class.call_method1("fromisoformat", (normalized,)) {
                    Ok(dt) => dt.call_method0("isoformat"),
                    Err(_) => Ok(py_none(py)),
                }
            } else if val.is_instance_of::<PyInt>() || val.is_instance_of::<PyFloat>() {
                let millis: f64 = val.extract()?;
                let dt_class = crate::cached_modules::datetime_datetime_cls(py)?;
                let utc = crate::cached_modules::datetime_tz_utc(py)?;
                let dt = dt_class.call_method1("fromtimestamp", (millis / 1000.0, utc))?;
                dt.call_method0("isoformat")
            } else {
                Ok(py_none(py))
            }
        }

        "$dateToString" => {
            let d = match ea.as_dict() {
                Some(d) => d,
                None => return Ok(py_none(py)),
            };
            let date_val = resolve_inner(doc, &dict_get(d, "date", py)?)?;
            let fmt: String = d
                .get_item("format")?
                .map(|v| v.extract::<String>())
                .transpose()?
                .unwrap_or_else(|| "%Y-%m-%dT%H:%M:%S.%fZ".to_string());

            if date_val.is_instance_of::<PyString>() {
                let s: String = date_val.extract()?;
                let normalized = s.replace("Z", "+00:00");
                let dt_class = crate::cached_modules::datetime_datetime_cls(py)?;
                match dt_class.call_method1("fromisoformat", (normalized,)) {
                    Ok(dt) => dt.call_method1("strftime", (fmt,)),
                    Err(_) => Ok(py_none(py)),
                }
            } else {
                Ok(py_none(py))
            }
        }

        "$convert" => match ea.as_dict() {
            Some(d) => eval_convert(py, d, doc),
            None => Ok(py_none(py)),
        },

        // ── Variable binding ────────────────────────────────────────
        "$let" => {
            let d = match ea.as_dict() {
                Some(d) => d,
                None => return Ok(py_none(py)),
            };
            let vars_spec = d
                .get_item("vars")?
                .unwrap_or_else(|| PyDict::new(py).into_any());
            let in_expr = dict_get(d, "in", py)?;
            let doc_dict = doc.cast::<PyDict>()?;
            let scoped = copy_dict(doc_dict)?;
            if let Ok(vars_dict) = vars_spec.cast::<PyDict>() {
                for (k, v) in vars_dict.iter() {
                    scoped.set_item(&k, resolve_inner(doc, &v)?)?;
                }
            }
            resolve_inner(scoped.as_any(), &in_expr)
        }

        // ── Array higher-order ──────────────────────────────────────
        "$map" => {
            let d = match ea.as_dict() {
                Some(d) => d,
                None => return Ok(py_none(py)),
            };
            let input_arr = resolve_inner(doc, &dict_get(d, "input", py)?)?;
            let as_name: String = d
                .get_item("as")?
                .map(|v| v.extract::<String>())
                .transpose()?
                .unwrap_or_else(|| "this".to_string());
            let in_expr = dict_get(d, "in", py)?;
            let input_list = match input_arr.cast::<PyList>() {
                Ok(l) => l,
                Err(_) => return Ok(py_none(py)),
            };
            let result = PyList::empty(py);
            let doc_dict = doc.cast::<PyDict>()?;
            for item in input_list.iter() {
                let scoped = copy_dict(doc_dict)?;
                scoped.set_item(&as_name, &item)?;
                result.append(resolve_inner(scoped.as_any(), &in_expr)?)?;
            }
            Ok(result.into_any())
        }

        "$reduce" => {
            let d = match ea.as_dict() {
                Some(d) => d,
                None => return Ok(py_none(py)),
            };
            let input_arr = resolve_inner(doc, &dict_get(d, "input", py)?)?;
            let initial = resolve_inner(doc, &dict_get(d, "initialValue", py)?)?;
            let in_expr = dict_get(d, "in", py)?;
            let input_list = match input_arr.cast::<PyList>() {
                Ok(l) => l,
                Err(_) => return Ok(py_none(py)),
            };
            let doc_dict = doc.cast::<PyDict>()?;
            let mut accum = initial;
            for item in input_list.iter() {
                let scoped = copy_dict(doc_dict)?;
                scoped.set_item("value", &accum)?;
                scoped.set_item("this", &item)?;
                accum = resolve_inner(scoped.as_any(), &in_expr)?;
            }
            Ok(accum)
        }

        "$range" => {
            if let Some(l) = ea.as_list() {
                if l.len() >= 2 {
                    let start = resolve_inner(doc, &l.get_item(0)?)?;
                    let end = resolve_inner(doc, &l.get_item(1)?)?;
                    let step = if l.len() > 2 {
                        resolve_inner(doc, &l.get_item(2)?)?
                    } else {
                        1i64.into_pyobject(py)?.into_any()
                    };
                    let start_i: i64 = start.extract()?;
                    let end_i: i64 = end.extract()?;
                    let step_i: i64 = step.extract().unwrap_or(1);
                    let step_val = if step_i == 0 { 1 } else { step_i };
                    let mut result_vec: Vec<i64> = Vec::new();
                    let mut i = start_i;
                    if step_val > 0 {
                        while i < end_i {
                            result_vec.push(i);
                            i += step_val;
                        }
                    } else {
                        while i > end_i {
                            result_vec.push(i);
                            i += step_val;
                        }
                    }
                    return Ok(PyList::new(py, &result_vec)?.into_any());
                }
            }
            Ok(py_none(py))
        }

        "$zip" => match ea.as_dict() {
            Some(d) => eval_zip(py, d, doc),
            None => Ok(py_none(py)),
        },

        "$reverseArray" => {
            let val = resolve_inner(doc, arg)?;
            if let Ok(l) = val.cast::<PyList>() {
                let items: Vec<Bound<'py, PyAny>> = l.iter().rev().collect();
                Ok(PyList::new(py, &items)?.into_any())
            } else {
                Ok(py_none(py))
            }
        }

        "$slice" => {
            if let Some(l) = ea.as_list() {
                if l.len() >= 2 {
                    let arr = resolve_inner(doc, &l.get_item(0)?)?;
                    let arr_list = match arr.cast::<PyList>() {
                        Ok(a) => a,
                        Err(_) => return Ok(py_none(py)),
                    };
                    if l.len() == 2 {
                        let n: isize = resolve_inner(doc, &l.get_item(1)?)?.extract()?;
                        let result = if n >= 0 {
                            arr_list.get_slice(0, n as usize)
                        } else {
                            let start = (arr_list.len() as isize + n).max(0) as usize;
                            arr_list.get_slice(start, arr_list.len())
                        };
                        return Ok(result.into_any());
                    } else if l.len() == 3 {
                        let pos: usize = resolve_inner(doc, &l.get_item(1)?)?.extract()?;
                        let n: usize = resolve_inner(doc, &l.get_item(2)?)?.extract()?;
                        let end = (pos + n).min(arr_list.len());
                        return Ok(arr_list.get_slice(pos, end).into_any());
                    }
                }
            }
            Ok(py_none(py))
        }

        "$isArray" => {
            let val = resolve_inner(doc, arg)?;
            Ok(py_bool(py, val.is_instance_of::<PyList>()))
        }

        "$indexOfArray" => {
            if let Some(l) = ea.as_list() {
                if l.len() >= 2 {
                    let arr = resolve_inner(doc, &l.get_item(0)?)?;
                    let search = resolve_inner(doc, &l.get_item(1)?)?;
                    let arr_list = match arr.cast::<PyList>() {
                        Ok(a) => a,
                        Err(_) => return Ok((-1i64).into_pyobject(py)?.into_any()),
                    };
                    let start: i64 = if l.len() > 2 {
                        resolve_inner(doc, &l.get_item(2)?)?.extract().unwrap_or(0)
                    } else {
                        0
                    };
                    let end: i64 = if l.len() > 3 {
                        resolve_inner(doc, &l.get_item(3)?)?
                            .extract()
                            .unwrap_or(arr_list.len() as i64)
                    } else {
                        arr_list.len() as i64
                    };
                    match arr_list
                        .as_any()
                        .call_method1("index", (&search, start, end))
                    {
                        Ok(idx) => return Ok(idx),
                        Err(_) => return Ok((-1i64).into_pyobject(py)?.into_any()),
                    }
                }
            }
            Ok((-1i64).into_pyobject(py)?.into_any())
        }

        // ── Object ──────────────────────────────────────────────────
        "$objectToArray" => {
            let val = resolve_inner(doc, arg)?;
            if let Ok(d) = val.cast::<PyDict>() {
                let result = PyList::empty(py);
                for (k, v) in d.iter() {
                    let entry = PyDict::new(py);
                    entry.set_item("k", &k)?;
                    entry.set_item("v", &v)?;
                    result.append(entry)?;
                }
                Ok(result.into_any())
            } else {
                Ok(py_none(py))
            }
        }

        "$arrayToObject" => {
            let val = resolve_inner(doc, arg)?;
            if let Ok(l) = val.cast::<PyList>() {
                let result = PyDict::new(py);
                for item in l.iter() {
                    if let Ok(d) = item.cast::<PyDict>() {
                        if let (Some(k), Some(v)) = (d.get_item("k")?, d.get_item("v")?) {
                            result.set_item(&k, &v)?;
                        }
                    } else if let Ok(pair) = item.cast::<PyList>() {
                        if pair.len() == 2 {
                            let k = pair.get_item(0)?.str()?;
                            result.set_item(k, pair.get_item(1)?)?;
                        }
                    }
                }
                Ok(result.into_any())
            } else {
                Ok(py_none(py))
            }
        }

        "$mergeObjects" => {
            if let Some(l) = ea.as_list() {
                let merged = PyDict::new(py);
                for a in l.iter() {
                    let val = resolve_inner(doc, &a)?;
                    if let Ok(d) = val.cast::<PyDict>() {
                        merged.call_method1("update", (d,))?;
                    }
                }
                Ok(merged.into_any())
            } else {
                let val = resolve_inner(doc, arg)?;
                if val.is_instance_of::<PyDict>() {
                    Ok(val)
                } else {
                    Ok(PyDict::new(py).into_any())
                }
            }
        }

        "$getField" => {
            let (field_name, input_obj) = if let Some(d) = ea.as_dict() {
                let fname = resolve_inner(doc, &dict_get(d, "field", py)?)?;
                let inp = match d.get_item("input")? {
                    Some(v) => resolve_inner(doc, &v)?,
                    None => doc.clone(),
                };
                (fname, inp)
            } else if let Ok(s) = arg.extract::<String>() {
                (s.into_pyobject(py)?.into_any(), doc.clone())
            } else {
                return Ok(py_none(py));
            };
            if let Ok(obj) = input_obj.cast::<PyDict>() {
                if let Ok(name) = field_name.extract::<String>() {
                    return Ok(obj.get_item(&name)?.unwrap_or_else(|| py_none(py)));
                }
            }
            Ok(py_none(py))
        }

        "$setField" => {
            let d = match ea.as_dict() {
                Some(d) => d,
                None => return Ok(py_none(py)),
            };
            let field_name = resolve_inner(doc, &dict_get(d, "field", py)?)?;
            let input_obj = match d.get_item("input")? {
                Some(v) => resolve_inner(doc, &v)?,
                None => {
                    let doc_dict = doc.cast::<PyDict>()?;
                    copy_dict(doc_dict)?.into_any()
                }
            };
            let value = resolve_inner(doc, &dict_get(d, "value", py)?)?;
            if let Ok(obj) = input_obj.cast::<PyDict>() {
                if let Ok(name) = field_name.extract::<String>() {
                    let result = copy_dict(obj)?;
                    result.set_item(&name, &value)?;
                    return Ok(result.into_any());
                }
            }
            Ok(py_none(py))
        }

        // ── Regex ───────────────────────────────────────────────────
        "$regexMatch" => match ea.as_dict() {
            Some(d) => eval_regex_match(py, d, doc),
            None => Ok(py_bool(py, false)),
        },
        "$regexFind" => match ea.as_dict() {
            Some(d) => eval_regex_find(py, d, doc, false),
            None => Ok(py_none(py)),
        },
        "$regexFindAll" => match ea.as_dict() {
            Some(d) => eval_regex_find(py, d, doc, true),
            None => Ok(PyList::empty(py).into_any()),
        },

        // ── String extras ───────────────────────────────────────────
        "$toString" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_none() {
                Ok(py_none(py))
            } else {
                Ok(val.str()?.into_any())
            }
        }

        "$toInt" => {
            let val = resolve_inner(doc, arg)?;
            match crate::cached_modules::builtins_int(py)?.call1((&val,)) {
                Ok(v) => Ok(v),
                Err(_) => Ok(py_none(py)),
            }
        }

        "$toDouble" => {
            let val = resolve_inner(doc, arg)?;
            match crate::cached_modules::builtins_float(py)?.call1((&val,)) {
                Ok(v) => Ok(v),
                Err(_) => Ok(py_none(py)),
            }
        }

        "$toBool" => {
            let val = resolve_inner(doc, arg)?;
            Ok(py_bool(py, val.is_truthy()?))
        }

        "$trim" => match ea.as_dict() {
            Some(d) => str_trim(py, d, doc, TrimMode::Both),
            None => Ok(py_none(py)),
        },
        "$ltrim" => match ea.as_dict() {
            Some(d) => str_trim(py, d, doc, TrimMode::Left),
            None => Ok(py_none(py)),
        },
        "$rtrim" => match ea.as_dict() {
            Some(d) => str_trim(py, d, doc, TrimMode::Right),
            None => Ok(py_none(py)),
        },

        "$split" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let s = resolve_inner(doc, &l.get_item(0)?)?;
                    let delim = resolve_inner(doc, &l.get_item(1)?)?;
                    if s.is_instance_of::<PyString>() && delim.is_instance_of::<PyString>() {
                        return s.call_method1("split", (&delim,));
                    }
                }
            }
            Ok(py_none(py))
        }

        "$indexOfCP" => {
            if let Some(l) = ea.as_list() {
                if l.len() >= 2 {
                    let s = resolve_inner(doc, &l.get_item(0)?)?;
                    let sub = resolve_inner(doc, &l.get_item(1)?)?;
                    if s.is_instance_of::<PyString>() && sub.is_instance_of::<PyString>() {
                        let start: i64 = if l.len() > 2 {
                            resolve_inner(doc, &l.get_item(2)?)?.extract().unwrap_or(0)
                        } else {
                            0
                        };
                        let s_str: String = s.extract()?;
                        let end: i64 = if l.len() > 3 {
                            resolve_inner(doc, &l.get_item(3)?)?
                                .extract()
                                .unwrap_or(s_str.len() as i64)
                        } else {
                            s_str.len() as i64
                        };
                        return s.call_method1("find", (&sub, start, end));
                    }
                }
            }
            Ok((-1i64).into_pyobject(py)?.into_any())
        }

        "$replaceOne" => match ea.as_dict() {
            Some(d) => str_replace(py, d, doc, false),
            None => Ok(py_none(py)),
        },
        "$replaceAll" => match ea.as_dict() {
            Some(d) => str_replace(py, d, doc, true),
            None => Ok(py_none(py)),
        },

        // ── Math extras ─────────────────────────────────────────────
        "$sqrt" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_instance_of::<PyInt>() || val.is_instance_of::<PyFloat>() {
                let v: f64 = val.extract()?;
                if v >= 0.0 {
                    return Ok(v.sqrt().into_pyobject(py)?.into_any());
                }
            }
            Ok(py_none(py))
        }

        "$pow" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let base = resolve_inner(doc, &l.get_item(0)?)?;
                    let exp = resolve_inner(doc, &l.get_item(1)?)?;
                    if (base.is_instance_of::<PyInt>() || base.is_instance_of::<PyFloat>())
                        && (exp.is_instance_of::<PyInt>() || exp.is_instance_of::<PyFloat>())
                    {
                        let b: f64 = base.extract()?;
                        let e: f64 = exp.extract()?;
                        let result = b.powf(e);
                        if result == result.floor()
                            && result.is_finite()
                            && !base.is_instance_of::<PyFloat>()
                            && !exp.is_instance_of::<PyFloat>()
                            && e >= 0.0
                        {
                            return Ok((result as i64).into_pyobject(py)?.into_any());
                        }
                        return Ok(result.into_pyobject(py)?.into_any());
                    }
                }
            }
            Ok(py_none(py))
        }

        "$log" => {
            if let Some(l) = ea.as_list() {
                if l.len() == 2 {
                    let val = resolve_inner(doc, &l.get_item(0)?)?;
                    let base = resolve_inner(doc, &l.get_item(1)?)?;
                    let v: f64 = val.extract().unwrap_or(0.0);
                    let b: f64 = base.extract().unwrap_or(0.0);
                    if v > 0.0 && b > 0.0 {
                        return Ok(v.log(b).into_pyobject(py)?.into_any());
                    }
                }
            }
            Ok(py_none(py))
        }

        "$log10" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_instance_of::<PyInt>() || val.is_instance_of::<PyFloat>() {
                let v: f64 = val.extract()?;
                if v > 0.0 {
                    return Ok(v.log10().into_pyobject(py)?.into_any());
                }
            }
            Ok(py_none(py))
        }

        "$ln" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_instance_of::<PyInt>() || val.is_instance_of::<PyFloat>() {
                let v: f64 = val.extract()?;
                if v > 0.0 {
                    return Ok(v.ln().into_pyobject(py)?.into_any());
                }
            }
            Ok(py_none(py))
        }

        "$exp" => {
            let val = resolve_inner(doc, arg)?;
            if val.is_instance_of::<PyInt>() || val.is_instance_of::<PyFloat>() {
                let v: f64 = val.extract()?;
                return Ok(v.exp().into_pyobject(py)?.into_any());
            }
            Ok(py_none(py))
        }

        "$trunc" => {
            if let Some(l) = ea.as_list() {
                let val = resolve_inner(doc, &l.get_item(0)?)?;
                let places: i32 = if l.len() > 1 {
                    resolve_inner(doc, &l.get_item(1)?)?.extract().unwrap_or(0)
                } else {
                    0
                };
                if val.is_instance_of::<PyInt>() || val.is_instance_of::<PyFloat>() {
                    let v: f64 = val.extract()?;
                    let factor = 10f64.powi(places);
                    let truncated = (v * factor) as i64;
                    return Ok((truncated as f64 / factor).into_pyobject(py)?.into_any());
                }
            } else {
                let val = resolve_inner(doc, arg)?;
                if val.is_instance_of::<PyInt>() || val.is_instance_of::<PyFloat>() {
                    let v: f64 = val.extract()?;
                    return Ok((v as i64).into_pyobject(py)?.into_any());
                }
            }
            Ok(py_none(py))
        }

        // ── $meta -- Atlas compatibility ────────────────────────────
        "$meta" => {
            if let Ok(meta_kind) = arg.extract::<String>() {
                let field = match meta_kind.as_str() {
                    "vectorSearchScore" => "_vectorScore",
                    "textScore" => "_textScore",
                    "searchScore" => "_searchScore",
                    "indexKey" => "_indexKey",
                    other => {
                        return Err(PyValueError::new_err(format!(
                            "Unsupported $meta keyword: {other}"
                        )));
                    }
                };
                return Ok(paths::get_value(doc, field)?.into_bound(py));
            }
            Err(PyValueError::new_err("$meta requires a string argument"))
        }

        _ => Err(PyValueError::new_err(format!(
            "Unsupported expression operator: {op}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Sub-dispatchers to keep eval_expr_op manageable
// ---------------------------------------------------------------------------

fn cmp_op<'py>(
    py: Python<'py>,
    ea: &ExprArg<'py>,
    doc: &Bound<'py, PyAny>,
    f: fn(&Bound<'py, PyAny>, &Bound<'py, PyAny>) -> PyResult<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    if let Some(l) = ea.as_list() {
        if l.len() == 2 {
            let a = resolve_inner(doc, &l.get_item(0)?)?;
            let b = resolve_inner(doc, &l.get_item(1)?)?;
            if !a.is_none() && !b.is_none() {
                return Ok(py_bool(py, f(&a, &b)?));
            }
        }
    }
    Ok(py_bool(py, false))
}

fn eval_convert<'py>(
    py: Python<'py>,
    d: &Bound<'py, PyDict>,
    doc: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let input_val = resolve_inner(doc, &dict_get(d, "input", py)?)?;
    let to_type: Option<String> = d.get_item("to")?.and_then(|v| v.extract::<String>().ok());
    let on_error_raw = d.get_item("onError")?;

    let convert = || -> PyResult<Bound<'py, PyAny>> {
        match to_type.as_deref() {
            Some("string") => {
                if input_val.is_none() {
                    Ok(py_none(py))
                } else {
                    Ok(input_val.str()?.into_any())
                }
            }
            Some("int") => crate::cached_modules::builtins_int(py)?.call1((&input_val,)),
            Some("double" | "decimal") => {
                crate::cached_modules::builtins_float(py)?.call1((&input_val,))
            }
            Some("bool") => Ok(py_bool(py, input_val.is_truthy()?)),
            Some("date") => {
                if input_val.is_instance_of::<PyString>() {
                    let s: String = input_val.extract()?;
                    let normalized = s.replace("Z", "+00:00");
                    let dt_class = crate::cached_modules::datetime_datetime_cls(py)?;
                    let dt = dt_class.call_method1("fromisoformat", (normalized,))?;
                    dt.call_method0("isoformat")
                } else if input_val.is_instance_of::<PyInt>()
                    || input_val.is_instance_of::<PyFloat>()
                {
                    let millis: f64 = input_val.extract()?;
                    let dt_class = crate::cached_modules::datetime_datetime_cls(py)?;
                    let utc = crate::cached_modules::datetime_tz_utc(py)?;
                    let dt = dt_class.call_method1("fromtimestamp", (millis / 1000.0, utc))?;
                    dt.call_method0("isoformat")
                } else {
                    Ok(py_none(py))
                }
            }
            _ => Ok(py_none(py)),
        }
    };

    match convert() {
        Ok(v) => Ok(v),
        Err(_) => match on_error_raw {
            Some(on_err) => resolve_inner(doc, &on_err),
            None => Ok(py_none(py)),
        },
    }
}

fn eval_zip<'py>(
    py: Python<'py>,
    d: &Bound<'py, PyDict>,
    doc: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let inputs_raw = d
        .get_item("inputs")?
        .unwrap_or_else(|| PyList::empty(py).into_any());
    let use_longest: bool = d
        .get_item("useLongestLength")?
        .map(|v| v.is_truthy())
        .transpose()?
        .unwrap_or(false);
    let defaults_raw = d
        .get_item("defaults")?
        .unwrap_or_else(|| PyList::empty(py).into_any());

    let inputs_list = inputs_raw.cast::<PyList>()?;
    let mut resolved: Vec<Bound<'py, PyList>> = Vec::new();
    for inp in inputs_list.iter() {
        let val = resolve_inner(doc, &inp)?;
        match val.cast_into::<PyList>() {
            Ok(l) => resolved.push(l),
            Err(_) => return Ok(py_none(py)),
        }
    }

    let defaults_list = defaults_raw.cast::<PyList>().ok();

    let result = PyList::empty(py);
    if use_longest {
        let max_len = resolved.iter().map(|r| r.len()).max().unwrap_or(0);
        for i in 0..max_len {
            let row = PyList::empty(py);
            for (j, arr) in resolved.iter().enumerate() {
                if i < arr.len() {
                    row.append(arr.get_item(i)?)?;
                } else if let Some(defs) = &defaults_list {
                    if j < defs.len() {
                        row.append(defs.get_item(j)?)?;
                    } else {
                        row.append(py.None())?;
                    }
                } else {
                    row.append(py.None())?;
                }
            }
            result.append(row)?;
        }
    } else {
        let min_len = resolved.iter().map(|r| r.len()).min().unwrap_or(0);
        for i in 0..min_len {
            let row = PyList::empty(py);
            for arr in &resolved {
                row.append(arr.get_item(i)?)?;
            }
            result.append(row)?;
        }
    }
    Ok(result.into_any())
}

fn eval_regex_match<'py>(
    py: Python<'py>,
    d: &Bound<'py, PyDict>,
    doc: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let input_val = resolve_inner(doc, &dict_get(d, "input", py)?)?;
    if !input_val.is_instance_of::<PyString>() {
        return Ok(py_bool(py, false));
    }
    let regex_str: String = d
        .get_item("regex")?
        .map(|v| v.extract::<String>())
        .transpose()?
        .unwrap_or_default();
    let opts: String = d
        .get_item("options")?
        .map(|v| v.extract::<String>())
        .transpose()?
        .unwrap_or_default();

    let flags = crate::query_compiler::build_regex_flags(&opts);
    let text: String = input_val.extract()?;
    match crate::query_compiler::safe_regex_search_pub(py, &regex_str, flags, &text) {
        Ok(matched) => Ok(py_bool(py, matched)),
        Err(_) => Ok(py_bool(py, false)),
    }
}

fn eval_regex_find<'py>(
    py: Python<'py>,
    d: &Bound<'py, PyDict>,
    doc: &Bound<'py, PyAny>,
    find_all: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let input_val = resolve_inner(doc, &dict_get(d, "input", py)?)?;
    if !input_val.is_instance_of::<PyString>() {
        return if find_all {
            Ok(PyList::empty(py).into_any())
        } else {
            Ok(py_none(py))
        };
    }
    let regex_str: String = d
        .get_item("regex")?
        .map(|v| v.extract::<String>())
        .transpose()?
        .unwrap_or_default();
    let opts: String = d
        .get_item("options")?
        .map(|v| v.extract::<String>())
        .transpose()?
        .unwrap_or_default();

    let flags = crate::query_compiler::build_regex_flags(&opts);
    let text: String = input_val.extract()?;

    let compiled = {
        let mut rust_pattern = String::new();
        if flags & 2 != 0 {
            rust_pattern.push_str("(?i)");
        }
        if flags & 8 != 0 {
            rust_pattern.push_str("(?m)");
        }
        if flags & 16 != 0 {
            rust_pattern.push_str("(?s)");
        }
        if flags & 64 != 0 {
            rust_pattern.push_str("(?x)");
        }
        rust_pattern.push_str(&regex_str);

        match regex::Regex::new(&rust_pattern) {
            Ok(re) => re,
            Err(_) => {
                let re_mod = crate::cached_modules::re_mod(py)?;
                let compiled = re_mod.call_method1("compile", (&regex_str, flags))?;
                if find_all {
                    let matches = compiled.call_method1("findall", (&text,))?;
                    return Ok(matches);
                } else {
                    let m = compiled.call_method1("search", (&text,))?;
                    return if m.is_none() {
                        Ok(py_none(py))
                    } else {
                        let d = PyDict::new(py);
                        d.set_item("match", m.call_method0("group")?)?;
                        d.set_item("idx", m.call_method0("start")?)?;
                        let captures = m.call_method0("groups")?;
                        d.set_item("captures", captures)?;
                        Ok(d.into_any())
                    };
                }
            }
        }
    };

    if find_all {
        let results = PyList::empty(py);
        for m in compiled.captures_iter(&text) {
            results.append(rust_match_to_dict(py, &m)?)?;
        }
        Ok(results.into_any())
    } else {
        match compiled.captures(&text) {
            None => Ok(py_none(py)),
            Some(m) => Ok(rust_match_to_dict(py, &m)?.into_any()),
        }
    }
}

fn rust_match_to_dict<'py>(
    py: Python<'py>,
    m: &regex::Captures<'_>,
) -> PyResult<Bound<'py, PyDict>> {
    let entry = PyDict::new(py);
    let full = m.get(0).map(|m| m.as_str()).unwrap_or("");
    let start = m.get(0).map(|m| m.start()).unwrap_or(0);
    entry.set_item("match", full)?;
    entry.set_item("idx", start)?;
    let caps = PyList::empty(py);
    for i in 1..m.len() {
        match m.get(i) {
            Some(c) => caps.append(c.as_str())?,
            None => caps.append(py.None())?,
        }
    }
    entry.set_item("captures", caps)?;
    Ok(entry)
}

enum TrimMode {
    Both,
    Left,
    Right,
}

fn str_trim<'py>(
    py: Python<'py>,
    d: &Bound<'py, PyDict>,
    doc: &Bound<'py, PyAny>,
    mode: TrimMode,
) -> PyResult<Bound<'py, PyAny>> {
    let input_val = resolve_inner(doc, &dict_get(d, "input", py)?)?;
    let chars = d.get_item("chars")?;

    if !input_val.is_instance_of::<PyString>() {
        return Ok(py_none(py));
    }

    let method = match mode {
        TrimMode::Both => "strip",
        TrimMode::Left => "lstrip",
        TrimMode::Right => "rstrip",
    };

    match chars {
        Some(c) => input_val.call_method1(method, (&c,)),
        None => input_val.call_method0(method),
    }
}

fn str_replace<'py>(
    py: Python<'py>,
    d: &Bound<'py, PyDict>,
    doc: &Bound<'py, PyAny>,
    replace_all: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let input_val = resolve_inner(doc, &dict_get(d, "input", py)?)?;
    let find_val = resolve_inner(doc, &dict_get(d, "find", py)?)?;
    let replacement = resolve_inner(doc, &dict_get(d, "replacement", py)?)?;

    if input_val.is_instance_of::<PyString>()
        && find_val.is_instance_of::<PyString>()
        && replacement.is_instance_of::<PyString>()
    {
        if replace_all {
            input_val.call_method1("replace", (&find_val, &replacement))
        } else {
            input_val.call_method1("replace", (&find_val, &replacement, 1i64))
        }
    } else {
        Ok(py_none(py))
    }
}
