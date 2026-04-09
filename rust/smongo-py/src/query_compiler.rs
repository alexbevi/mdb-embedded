//! MongoDB query predicate compiler -- transforms query documents into callable Rust predicates.
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};

use crate::geo_query::{
    parse_field_geo_intersects_geometry, parse_field_geo_within_center_sphere,
    parse_field_geo_within_geometry, parse_geo_intersects_inner_geometry,
    parse_geo_within_inner_center_sphere, parse_geo_within_inner_geometry,
};
use crate::paths;

pub(crate) const MAX_REGEX_PATTERN_LEN: usize = 1024;

const RE_IGNORECASE: u32 = 2;
const RE_MULTILINE: u32 = 8;
const RE_DOTALL: u32 = 16;
const RE_VERBOSE: u32 = 64;

/// A compiled MongoDB query predicate backed by Rust evaluation logic.
///
/// Created by `compile_query`; callable as `predicate(doc) -> bool`.
#[pyclass(module = "smongo._smongo_core")]
pub struct CompiledQuery {
    query: Py<PyDict>,
}

#[pymethods]
impl CompiledQuery {
    fn __call__(&self, doc: &Bound<'_, PyDict>) -> PyResult<bool> {
        eval_query(doc, self.query.bind(doc.py()))
    }

    fn __repr__(&self) -> String {
        "CompiledQuery(...)".to_string()
    }
}

#[pyfunction]
pub fn compile_query(query: &Bound<'_, PyDict>) -> PyResult<CompiledQuery> {
    Ok(CompiledQuery {
        query: query.clone().unbind(),
    })
}

/// Cast a `PyAny` to a list of dicts, consolidating the per-element cast.
fn as_dict_list<'py>(val: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let list = val.cast::<PyList>()?;
    list.iter()
        .map(|item| Ok(item.cast::<PyDict>()?.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// Core evaluation -- mirrors Python's compile_query inner `match` function
// ---------------------------------------------------------------------------

pub(crate) fn eval_query(doc: &Bound<'_, PyDict>, query: &Bound<'_, PyDict>) -> PyResult<bool> {
    let py = doc.py();

    for (key_obj, condition) in query.iter() {
        let key: String = key_obj.extract()?;

        match key.as_str() {
            "$or" => {
                let subs = as_dict_list(&condition)?;
                let mut matched = false;
                for sub in &subs {
                    if eval_query(doc, sub)? {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return Ok(false);
                }
            }
            "$and" => {
                for sub in &as_dict_list(&condition)? {
                    if !eval_query(doc, sub)? {
                        return Ok(false);
                    }
                }
            }
            "$nor" => {
                for sub in &as_dict_list(&condition)? {
                    if eval_query(doc, sub)? {
                        return Ok(false);
                    }
                }
            }
            "$expr" => {
                let result = crate::query_expressions::resolve_expr(doc, &condition)?;
                if !result.is_truthy()? {
                    return Ok(false);
                }
            }
            "$comment" => continue,
            "$text" => {
                let search_str = if let Ok(d) = condition.cast::<PyDict>() {
                    match d.get_item("$search")? {
                        Some(v) => v.extract::<String>().unwrap_or_default(),
                        None => String::new(),
                    }
                } else {
                    condition.str()?.extract::<String>()?
                };
                if !text_match(doc, &search_str)? {
                    return Ok(false);
                }
            }
            _ => {
                let value = paths::get_value(doc, &key)?;
                let value_ref = value.bind(py);

                if let Ok(cond_dict) = condition.cast::<PyDict>() {
                    let mut regex_flags: u32 = 0;
                    if let Some(opts) = cond_dict.get_item("$options")? {
                        let opts_str: String = opts.extract()?;
                        if opts_str.contains('i') {
                            regex_flags |= RE_IGNORECASE;
                        }
                        if opts_str.contains('m') {
                            regex_flags |= RE_MULTILINE;
                        }
                        if opts_str.contains('s') {
                            regex_flags |= RE_DOTALL;
                        }
                    }
                    if let Some((clon, clat, rrad)) =
                        parse_field_geo_within_center_sphere(&cond_dict)?
                    {
                        let max_m = rrad * crate::geo_s2::EARTH_RADIUS_METERS;
                        let Some((dlon, dlat)) = crate::geo_s2::extract_lon_lat_py(&value_ref)?
                        else {
                            return Ok(false);
                        };
                        let dist = crate::geo_s2::haversine_meters(clon, clat, dlon, dlat);
                        if dist > max_m {
                            return Ok(false);
                        }
                        for (op_obj, cond_val) in cond_dict.iter() {
                            let op: String = op_obj.extract()?;
                            if op == "$geoWithin" {
                                continue;
                            }
                            if !eval_op(py, &op, value_ref, &cond_val, doc, &key, regex_flags)? {
                                return Ok(false);
                            }
                        }
                    } else if let Some(shape) = parse_field_geo_within_geometry(&cond_dict)? {
                        let Some((dlon, dlat)) = crate::geo_s2::extract_lon_lat_py(&value_ref)?
                        else {
                            return Ok(false);
                        };
                        if !shape.contains_point_lonlat(dlon, dlat) {
                            return Ok(false);
                        }
                        for (op_obj, cond_val) in cond_dict.iter() {
                            let op: String = op_obj.extract()?;
                            if op == "$geoWithin" {
                                continue;
                            }
                            if !eval_op(py, &op, value_ref, &cond_val, doc, &key, regex_flags)? {
                                return Ok(false);
                            }
                        }
                    } else if let Some(shape) = parse_field_geo_intersects_geometry(&cond_dict)? {
                        let Some((dlon, dlat)) = crate::geo_s2::extract_lon_lat_py(&value_ref)?
                        else {
                            return Ok(false);
                        };
                        if !shape.intersects_point_lonlat(dlon, dlat) {
                            return Ok(false);
                        }
                        for (op_obj, cond_val) in cond_dict.iter() {
                            let op: String = op_obj.extract()?;
                            if op == "$geoIntersects" {
                                continue;
                            }
                            if !eval_op(py, &op, value_ref, &cond_val, doc, &key, regex_flags)? {
                                return Ok(false);
                            }
                        }
                    } else if let Some((clon, clat, max_m, min_m)) =
                        crate::geo_query::parse_field_near_spec(&cond_dict)?
                    {
                        let max_m = max_m.or(Some(crate::geo_s2::DEFAULT_NEAR_MAX_DISTANCE_M));
                        let Some((dlon, dlat)) = crate::geo_s2::extract_lon_lat_py(&value_ref)?
                        else {
                            return Ok(false);
                        };
                        let dist = crate::geo_s2::haversine_meters(clon, clat, dlon, dlat);
                        if let Some(m) = max_m {
                            if dist > m {
                                return Ok(false);
                            }
                        }
                        if let Some(m) = min_m {
                            if dist < m {
                                return Ok(false);
                            }
                        }
                        for (op_obj, cond_val) in cond_dict.iter() {
                            let op: String = op_obj.extract()?;
                            if matches!(
                                op.as_str(),
                                "$near" | "$nearSphere" | "$maxDistance" | "$minDistance"
                            ) {
                                continue;
                            }
                            if !eval_op(py, &op, value_ref, &cond_val, doc, &key, regex_flags)? {
                                return Ok(false);
                            }
                        }
                    } else {
                        for (op_obj, cond_val) in cond_dict.iter() {
                            let op: String = op_obj.extract()?;
                            if !eval_op(py, &op, value_ref, &cond_val, doc, &key, regex_flags)? {
                                return Ok(false);
                            }
                        }
                    }
                } else {
                    if !value_ref.eq(&condition)? {
                        return Ok(false);
                    }
                }
            }
        }
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// Single-operator evaluation -- mirrors Python's _eval_op
// ---------------------------------------------------------------------------

fn eval_op<'py>(
    py: Python<'py>,
    op: &str,
    value: &Bound<'py, PyAny>,
    cond_val: &Bound<'py, PyAny>,
    doc: &Bound<'py, PyAny>,
    key: &str,
    regex_flags: u32,
) -> PyResult<bool> {
    match op {
        "$gt" => {
            if value.is_none() {
                return Ok(false);
            }
            value.gt(cond_val)
        }
        "$lt" => {
            if value.is_none() {
                return Ok(false);
            }
            value.lt(cond_val)
        }
        "$gte" => {
            if value.is_none() {
                return Ok(false);
            }
            value.ge(cond_val)
        }
        "$lte" => {
            if value.is_none() {
                return Ok(false);
            }
            value.le(cond_val)
        }
        "$eq" => value.eq(cond_val),
        "$ne" => value.ne(cond_val),
        "$in" => {
            if let Ok(val_list) = value.cast::<PyList>() {
                for item in val_list.iter() {
                    if cond_val.contains(&item)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            } else {
                cond_val.contains(value)
            }
        }
        "$nin" => {
            if let Ok(val_list) = value.cast::<PyList>() {
                for item in val_list.iter() {
                    if cond_val.contains(&item)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            } else {
                Ok(!cond_val.contains(value)?)
            }
        }
        "$exists" => {
            let present = paths::field_exists(doc, key)?;
            let want = cond_val.is_truthy()?;
            Ok(if want { present } else { !present })
        }
        "$regex" => {
            if value.is_none() || value.cast::<PyString>().is_err() {
                return Ok(false);
            }
            let pattern: String = cond_val.extract()?;
            let text: String = value.extract()?;
            safe_regex_search_pub(py, &pattern, regex_flags, &text)
        }
        "$options" => Ok(true),
        "$not" => {
            if let Ok(cond_dict) = cond_val.cast::<PyDict>() {
                for (k, v) in cond_dict.iter() {
                    let sub_op: String = k.extract()?;
                    if !eval_op(py, &sub_op, value, &v, doc, key, 0)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            } else {
                Ok(false)
            }
        }
        "$all" => {
            if value.cast::<PyList>().is_err() {
                return Ok(false);
            }
            let items = cond_val.cast::<PyList>()?;
            for item in items.iter() {
                if !value.contains(&item)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        "$elemMatch" => {
            let val_list = match value.cast::<PyList>() {
                Ok(l) => l,
                Err(_) => return Ok(false),
            };
            let cond_dict = cond_val.cast::<PyDict>()?;
            for elem in val_list.iter() {
                let test_doc: Bound<'_, PyDict> = if let Ok(d) = elem.cast::<PyDict>() {
                    d.clone()
                } else {
                    let d = PyDict::new(py);
                    d.set_item("value", &elem)?;
                    d
                };
                if eval_query(&test_doc, cond_dict)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        "$size" => match value.cast::<PyList>() {
            Ok(l) => {
                let expected: usize = cond_val.extract()?;
                Ok(l.len() == expected)
            }
            Err(_) => Ok(false),
        },
        "$type" => {
            let type_names: Vec<String> = if let Ok(s) = cond_val.extract::<String>() {
                vec![s]
            } else {
                cond_val.extract()?
            };
            for tn in &type_names {
                if check_bson_type(value, tn) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        "$mod" => {
            let args = match cond_val.cast::<PyList>() {
                Ok(l) if l.len() == 2 => l,
                _ => return Ok(false),
            };
            if !(value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>()) {
                return Ok(false);
            }
            let divisor: i64 = args.get_item(0)?.extract()?;
            if divisor == 0 {
                return Ok(false);
            }
            let remainder: i64 = args.get_item(1)?.extract()?;
            let val: i64 = value.extract()?;
            Ok(val % divisor == remainder)
        }
        "$bitsAllSet" => bits_check(value, cond_val, BitsMode::AllSet),
        "$bitsAnySet" => bits_check(value, cond_val, BitsMode::AnySet),
        "$bitsAllClear" => bits_check(value, cond_val, BitsMode::AllClear),
        "$bitsAnyClear" => bits_check(value, cond_val, BitsMode::AnyClear),
        "$near" | "$nearSphere" => Ok(true),
        "$maxDistance" | "$minDistance" => Ok(false),
        "$geoWithin" => {
            let inner = cond_val.cast::<PyDict>()?;
            if let Some((clon, clat, rrad)) = parse_geo_within_inner_center_sphere(&inner)? {
                let max_m = rrad * crate::geo_s2::EARTH_RADIUS_METERS;
                let Some((dlon, dlat)) = crate::geo_s2::extract_lon_lat_py(value)? else {
                    return Ok(false);
                };
                let dist = crate::geo_s2::haversine_meters(clon, clat, dlon, dlat);
                Ok(dist <= max_m)
            } else if let Some(shape) = parse_geo_within_inner_geometry(&inner)? {
                let Some((dlon, dlat)) = crate::geo_s2::extract_lon_lat_py(value)? else {
                    return Ok(false);
                };
                Ok(shape.contains_point_lonlat(dlon, dlat))
            } else {
                Err(PyValueError::new_err(
                    "$geoWithin requires $centerSphere or $geometry Polygon/MultiPolygon",
                ))
            }
        },
        "$geoIntersects" => {
            let inner = cond_val.cast::<PyDict>()?;
            if let Some(shape) = parse_geo_intersects_inner_geometry(&inner)? {
                let Some((dlon, dlat)) = crate::geo_s2::extract_lon_lat_py(value)? else {
                    return Ok(false);
                };
                Ok(shape.intersects_point_lonlat(dlon, dlat))
            } else {
                Err(PyValueError::new_err(
                    "$geoIntersects requires $geometry Polygon or MultiPolygon",
                ))
            }
        },
        _ => Err(PyValueError::new_err(format!(
            "unknown query operator: {op}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn check_bson_type(value: &Bound<'_, PyAny>, type_name: &str) -> bool {
    match type_name {
        "double" => value.is_instance_of::<PyFloat>(),
        "string" => value.is_instance_of::<PyString>(),
        "object" => value.is_instance_of::<PyDict>(),
        "array" => value.is_instance_of::<PyList>(),
        "bool" => value.is_instance_of::<PyBool>(),
        "int" | "long" => value.is_instance_of::<PyInt>(),
        "null" => value.is_none(),
        "number" => value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>(),
        _ => false,
    }
}

enum BitsMode {
    AllSet,
    AnySet,
    AllClear,
    AnyClear,
}

fn bits_check(
    value: &Bound<'_, PyAny>,
    bitmask: &Bound<'_, PyAny>,
    mode: BitsMode,
) -> PyResult<bool> {
    if !(value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>()) {
        return Ok(false);
    }
    let val: i64 = value.extract()?;

    let mask: i64 = if let Ok(m) = bitmask.extract::<i64>() {
        m
    } else if let Ok(positions) = bitmask.cast::<PyList>() {
        let mut m: i64 = 0;
        for pos in positions.iter() {
            let p: i64 = pos.extract()?;
            m |= 1i64 << p;
        }
        m
    } else {
        return Ok(false);
    };

    match mode {
        BitsMode::AllSet => Ok((val & mask) == mask),
        BitsMode::AnySet => Ok((val & mask) != 0),
        BitsMode::AllClear => Ok((val & mask) == 0),
        BitsMode::AnyClear => Ok((val & mask) != mask),
    }
}

/// Check for nested quantifiers (ReDoS pattern): `[+*]\s*)\s*[+*?{]`
pub(crate) fn has_nested_quantifiers(pattern: &str) -> bool {
    let bytes = pattern.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        if bytes[i] == b'+' || bytes[i] == b'*' {
            let mut j = i + 1;
            while j < len && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < len && bytes[j] == b')' {
                j += 1;
                while j < len && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j < len && matches!(bytes[j], b'+' | b'*' | b'?' | b'{') {
                    return true;
                }
            }
        }
        i += 1;
    }
    false
}

/// Build regex flags integer from a MongoDB `$options` string (e.g. "ims").
pub(crate) fn build_regex_flags(opts: &str) -> u32 {
    let mut flags = 0u32;
    for ch in opts.chars() {
        match ch {
            'i' => flags |= RE_IGNORECASE,
            'm' => flags |= RE_MULTILINE,
            's' => flags |= RE_DOTALL,
            'x' => flags |= RE_VERBOSE,
            _ => {}
        }
    }
    flags
}

pub(crate) fn safe_regex_search_pub(
    py: Python,
    pattern: &str,
    flags: u32,
    text: &str,
) -> PyResult<bool> {
    if pattern.len() > MAX_REGEX_PATTERN_LEN {
        return Err(PyValueError::new_err(format!(
            "regex pattern length {} exceeds limit {}",
            pattern.len(),
            MAX_REGEX_PATTERN_LEN
        )));
    }
    if has_nested_quantifiers(pattern) {
        return Err(PyValueError::new_err(
            "regex pattern rejected: nested quantifiers are not allowed",
        ));
    }

    let mut rust_pattern = String::new();
    if flags & RE_IGNORECASE != 0 {
        rust_pattern.push_str("(?i)");
    }
    if flags & RE_MULTILINE != 0 {
        rust_pattern.push_str("(?m)");
    }
    if flags & RE_DOTALL != 0 {
        rust_pattern.push_str("(?s)");
    }
    if flags & RE_VERBOSE != 0 {
        rust_pattern.push_str("(?x)");
    }
    rust_pattern.push_str(pattern);

    match regex::Regex::new(&rust_pattern) {
        Ok(re) => Ok(re.is_match(text)),
        Err(_) => {
            let re_mod = crate::cached_modules::re_mod(py)?;
            let compiled = re_mod.call_method1("compile", (pattern, flags))?;
            let result = compiled.call_method1("search", (text,))?;
            Ok(!result.is_none())
        }
    }
}

fn text_match(doc: &Bound<'_, PyAny>, search_str: &str) -> PyResult<bool> {
    let tokens: Vec<String> = search_str
        .to_lowercase()
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    if tokens.is_empty() {
        return Ok(true);
    }

    let mut strings = Vec::new();
    extract_strings(doc, &mut strings)?;
    let all_text = strings.join(" ").to_lowercase();

    Ok(tokens.iter().all(|t| all_text.contains(t.as_str())))
}

fn extract_strings(v: &Bound<'_, PyAny>, out: &mut Vec<String>) -> PyResult<()> {
    if let Ok(s) = v.extract::<String>() {
        out.push(s);
    } else if let Ok(d) = v.cast::<PyDict>() {
        for (_, val) in d.iter() {
            extract_strings(&val, out)?;
        }
    } else if let Ok(l) = v.cast::<PyList>() {
        for item in l.iter() {
            extract_strings(&item, out)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn with_py<F>(f: F)
    where
        F: for<'py> FnOnce(Python<'py>),
    {
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(Python::initialize);
        Python::attach(f);
    }

    #[test]
    fn test_nested_quantifier_detection() {
        assert!(has_nested_quantifiers("(a+)+"));
        assert!(has_nested_quantifiers("(a*)*"));
        assert!(has_nested_quantifiers("(a+)?"));
        assert!(has_nested_quantifiers("(a*){2}"));
        assert!(!has_nested_quantifiers("a+b*"));
        assert!(!has_nested_quantifiers("(a+)b"));
    }

    #[test]
    fn test_empty_query_matches_all() {
        with_py(|py| {
            let query = PyDict::new(py);
            let doc = PyDict::new(py);
            doc.set_item("x", 1).unwrap();
            assert!(eval_query(&doc, &query).unwrap());
        });
    }

    #[test]
    fn test_equality_match() {
        with_py(|py| {
            let query = PyDict::new(py);
            query.set_item("x", 1).unwrap();
            let doc = PyDict::new(py);
            doc.set_item("x", 1).unwrap();
            assert!(eval_query(&doc, &query).unwrap());
        });
    }

    #[test]
    fn test_equality_mismatch() {
        with_py(|py| {
            let query = PyDict::new(py);
            query.set_item("x", 1).unwrap();
            let doc = PyDict::new(py);
            doc.set_item("x", 2).unwrap();
            assert!(!eval_query(&doc, &query).unwrap());
        });
    }

    #[test]
    fn test_gt_operator() {
        with_py(|py| {
            let ops = PyDict::new(py);
            ops.set_item("$gt", 5).unwrap();
            let query = PyDict::new(py);
            query.set_item("x", ops).unwrap();

            let doc = PyDict::new(py);
            doc.set_item("x", 10).unwrap();
            assert!(eval_query(&doc, &query).unwrap());

            let doc2 = PyDict::new(py);
            doc2.set_item("x", 3).unwrap();
            assert!(!eval_query(&doc2, &query).unwrap());
        });
    }

    #[test]
    fn test_in_operator() {
        with_py(|py| {
            let vals = PyList::new(py, [1, 2, 3]).unwrap();
            let ops = PyDict::new(py);
            ops.set_item("$in", vals).unwrap();
            let query = PyDict::new(py);
            query.set_item("x", ops).unwrap();

            let doc = PyDict::new(py);
            doc.set_item("x", 2).unwrap();
            assert!(eval_query(&doc, &query).unwrap());

            let doc2 = PyDict::new(py);
            doc2.set_item("x", 5).unwrap();
            assert!(!eval_query(&doc2, &query).unwrap());
        });
    }

    #[test]
    fn test_or_combinator() {
        with_py(|py| {
            let sub1 = PyDict::new(py);
            sub1.set_item("x", 1).unwrap();
            let sub2 = PyDict::new(py);
            sub2.set_item("x", 2).unwrap();
            let or_list = PyList::new(py, [sub1.as_any(), sub2.as_any()]).unwrap();
            let query = PyDict::new(py);
            query.set_item("$or", or_list).unwrap();

            let doc = PyDict::new(py);
            doc.set_item("x", 2).unwrap();
            assert!(eval_query(&doc, &query).unwrap());

            let doc2 = PyDict::new(py);
            doc2.set_item("x", 3).unwrap();
            assert!(!eval_query(&doc2, &query).unwrap());
        });
    }

    #[test]
    fn test_exists_operator() {
        with_py(|py| {
            let ops = PyDict::new(py);
            ops.set_item("$exists", true).unwrap();
            let query = PyDict::new(py);
            query.set_item("x", ops).unwrap();

            let doc = PyDict::new(py);
            doc.set_item("x", 1).unwrap();
            assert!(eval_query(&doc, &query).unwrap());

            let doc2 = PyDict::new(py);
            doc2.set_item("y", 1).unwrap();
            assert!(!eval_query(&doc2, &query).unwrap());
        });
    }

    #[test]
    fn test_bits_all_set() {
        with_py(|py| {
            let value = 7i64.into_pyobject(py).unwrap().into_any();
            let mask = 3i64.into_pyobject(py).unwrap().into_any();
            assert!(bits_check(&value, &mask, BitsMode::AllSet).unwrap());
        });
    }
}
