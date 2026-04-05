//! Rust-native `$jsonSchema` document validation.
//!
//! Replaces the Python `smongo.schema` module on the hot path so that every
//! insert/update on a validated collection stays in Rust.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyNone, PyString};

use crate::query_compiler::{has_nested_quantifiers, MAX_REGEX_PATTERN_LEN};

pyo3::create_exception!(smongo._smongo_core, ValidationError, pyo3::exceptions::PyException);

const MAX_NESTING_DEPTH: usize = 100;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validate a document against a `$jsonSchema` spec (Rust-internal entry point).
///
/// Called directly from `RustLocalCollection::validate_doc` -- zero Python
/// method dispatch.
pub(crate) fn validate_document(
    doc: &Bound<'_, PyDict>,
    schema: &Bound<'_, PyAny>,
) -> PyResult<()> {
    if schema.is_none() || !schema.is_truthy()? {
        return Ok(());
    }
    let schema_dict = schema.cast::<PyDict>().map_err(|_| {
        ValidationError::new_err("schema must be a dict")
    })?;
    if schema_dict.is_empty() {
        return Ok(());
    }
    validate_object(doc.as_any(), schema_dict, "", 0)
}

/// Python-callable wrapper so `smongo.schema` can delegate here.
#[pyfunction]
#[pyo3(name = "validate_document")]
pub fn py_validate_document(doc: &Bound<'_, PyDict>, schema: &Bound<'_, PyAny>) -> PyResult<()> {
    validate_document(doc, schema)
}

// ---------------------------------------------------------------------------
// Type resolution
// ---------------------------------------------------------------------------

/// Returns `true` when `value` satisfies the given MongoDB/JSON-Schema type name.
fn check_bson_type(value: &Bound<'_, PyAny>, type_name: &str) -> PyResult<bool> {
    match type_name {
        "string" => Ok(value.is_instance_of::<PyString>()),
        "int" | "long" => {
            Ok(value.is_instance_of::<PyInt>() && !value.is_instance_of::<PyBool>())
        }
        "double" => Ok(value.is_instance_of::<PyFloat>()),
        "number" => {
            if value.is_instance_of::<PyBool>() {
                return Ok(false);
            }
            Ok(value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>())
        }
        "bool" | "boolean" => Ok(value.is_instance_of::<PyBool>()),
        "object" => Ok(value.is_instance_of::<PyDict>()),
        "array" => Ok(value.is_instance_of::<PyList>()),
        "null" => Ok(value.is_none()),
        _ => Ok(false),
    }
}

/// Check whether `value` matches any of the type names given in a `bsonType` /
/// `type` field (which can be a single string or a list of strings).
fn check_type_spec(value: &Bound<'_, PyAny>, type_spec: &Bound<'_, PyAny>) -> PyResult<bool> {
    if let Ok(s) = type_spec.extract::<String>() {
        return check_bson_type(value, &s);
    }
    if let Ok(list) = type_spec.cast::<PyList>() {
        for item in list.iter() {
            if let Ok(s) = item.extract::<String>() {
                if check_bson_type(value, &s)? {
                    return Ok(true);
                }
            }
        }
        return Ok(false);
    }
    Ok(true)
}

fn type_spec_label(type_spec: &Bound<'_, PyAny>) -> String {
    type_spec.str().map(|s| s.to_string()).unwrap_or_else(|_| "unknown".to_string())
}

// ---------------------------------------------------------------------------
// Path helper
// ---------------------------------------------------------------------------

fn join_path(base: &str, field: &str) -> String {
    if base.is_empty() {
        field.to_string()
    } else {
        format!("{base}.{field}")
    }
}

// ---------------------------------------------------------------------------
// Validators
// ---------------------------------------------------------------------------

fn validate_object(
    value: &Bound<'_, PyAny>,
    schema: &Bound<'_, PyDict>,
    path: &str,
    depth: usize,
) -> PyResult<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(ValidationError::new_err(format!(
            "Document exceeds maximum nesting depth of {MAX_NESTING_DEPTH}"
        )));
    }

    // bsonType / type check on the value itself
    if let Some(type_spec) = schema.get_item("bsonType")?.or(schema.get_item("type")?) {
        if !check_type_spec(value, &type_spec)? {
            let label = type_spec_label(&type_spec);
            let actual = value.get_type().name()?.to_string();
            return Err(ValidationError::new_err(format!(
                "Document failed validation at '{path}': expected type {label}, got {actual}"
            )));
        }
    }

    let dict = match value.cast::<PyDict>() {
        Ok(d) => d,
        Err(_) => {
            validate_scalar(value, schema, path)?;
            return Ok(());
        }
    };

    // required
    if let Some(req_obj) = schema.get_item("required")? {
        if let Ok(req_list) = req_obj.cast::<PyList>() {
            for field_obj in req_list.iter() {
                let field: String = field_obj.extract()?;
                if !dict.contains(&field)? {
                    return Err(ValidationError::new_err(format!(
                        "Document failed validation: missing required field '{}'",
                        join_path(path, &field)
                    )));
                }
            }
        }
    }

    // properties
    if let Some(props_obj) = schema.get_item("properties")? {
        if let Ok(props) = props_obj.cast::<PyDict>() {
            for (field_obj, field_schema) in props.iter() {
                let field: String = field_obj.extract()?;
                if let Some(val) = dict.get_item(&field)? {
                    let sub = field_schema.cast::<PyDict>().map_err(|_| {
                        ValidationError::new_err("property schema must be a dict")
                    })?;
                    validate_value(&val, sub, &join_path(path, &field), depth + 1)?;
                }
            }
        }
    }

    // additionalProperties
    if let Some(ap) = schema.get_item("additionalProperties")? {
        if ap.is_instance_of::<PyBool>() && !ap.is_truthy()? {
            let allowed: Vec<String> = if let Some(props_obj) = schema.get_item("properties")? {
                if let Ok(props) = props_obj.cast::<PyDict>() {
                    props.keys().iter().map(|k| k.extract::<String>()).collect::<PyResult<Vec<_>>>()?
                } else {
                    vec![]
                }
            } else {
                vec![]
            };
            for key_obj in dict.keys().iter() {
                let key: String = key_obj.extract()?;
                if key != "_id" && !allowed.contains(&key) {
                    return Err(ValidationError::new_err(format!(
                        "Document failed validation: additional property '{}' not allowed",
                        join_path(path, &key)
                    )));
                }
            }
        }
    }

    // minProperties / maxProperties
    if let Some(min_p) = schema.get_item("minProperties")? {
        let min: usize = min_p.extract()?;
        if dict.len() < min {
            return Err(ValidationError::new_err(format!(
                "Document failed validation at '{path}': too few properties"
            )));
        }
    }
    if let Some(max_p) = schema.get_item("maxProperties")? {
        let max: usize = max_p.extract()?;
        if dict.len() > max {
            return Err(ValidationError::new_err(format!(
                "Document failed validation at '{path}': too many properties"
            )));
        }
    }

    Ok(())
}

fn validate_value(
    value: &Bound<'_, PyAny>,
    schema: &Bound<'_, PyDict>,
    path: &str,
    depth: usize,
) -> PyResult<()> {
    // null handling
    if value.is_none() || value.is_instance_of::<PyNone>() {
        if let Some(type_spec) = schema.get_item("bsonType")?.or(schema.get_item("type")?) {
            let allows_null = if let Ok(s) = type_spec.extract::<String>() {
                s == "null"
            } else if let Ok(list) = type_spec.cast::<PyList>() {
                let mut found = false;
                for item in list.iter() {
                    if let Ok(s) = item.extract::<String>() {
                        if s == "null" {
                            found = true;
                            break;
                        }
                    }
                }
                found
            } else {
                true
            };
            if !allows_null {
                return Err(ValidationError::new_err(format!(
                    "Document failed validation at '{path}': null not allowed"
                )));
            }
        }
        return Ok(());
    }

    // type check
    if let Some(type_spec) = schema.get_item("bsonType")?.or(schema.get_item("type")?) {
        if !check_type_spec(value, &type_spec)? {
            let label = type_spec_label(&type_spec);
            let actual = value.get_type().name()?.to_string();
            return Err(ValidationError::new_err(format!(
                "Document failed validation at '{path}': expected type {label}, got {actual}"
            )));
        }
    }

    if value.is_instance_of::<PyDict>() {
        let d = value.cast::<PyDict>().unwrap();
        validate_object(d.as_any(), schema, path, depth)?;
    } else if value.is_instance_of::<PyList>() {
        let l = value.cast::<PyList>().unwrap();
        validate_array(l, schema, path, depth)?;
    } else {
        validate_scalar(value, schema, path)?;
    }

    Ok(())
}

fn validate_scalar(
    value: &Bound<'_, PyAny>,
    schema: &Bound<'_, PyDict>,
    path: &str,
) -> PyResult<()> {
    // Numeric constraints (exclude booleans)
    let is_numeric = (value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>())
        && !value.is_instance_of::<PyBool>();

    if is_numeric {
        let v: f64 = value.extract()?;
        if let Some(min_obj) = schema.get_item("minimum")? {
            let min: f64 = min_obj.extract()?;
            if v < min {
                return Err(ValidationError::new_err(format!(
                    "Document failed validation at '{path}': value {v} < minimum {min}"
                )));
            }
        }
        if let Some(max_obj) = schema.get_item("maximum")? {
            let max: f64 = max_obj.extract()?;
            if v > max {
                return Err(ValidationError::new_err(format!(
                    "Document failed validation at '{path}': value {v} > maximum {max}"
                )));
            }
        }
        if let Some(emin_obj) = schema.get_item("exclusiveMinimum")? {
            let emin: f64 = emin_obj.extract()?;
            if v <= emin {
                return Err(ValidationError::new_err(format!(
                    "Document failed validation at '{path}': value {v} <= exclusiveMinimum"
                )));
            }
        }
        if let Some(emax_obj) = schema.get_item("exclusiveMaximum")? {
            let emax: f64 = emax_obj.extract()?;
            if v >= emax {
                return Err(ValidationError::new_err(format!(
                    "Document failed validation at '{path}': value {v} >= exclusiveMaximum"
                )));
            }
        }
    }

    // String constraints
    if value.is_instance_of::<PyString>() {
        let s: String = value.extract()?;
        if let Some(ml) = schema.get_item("minLength")? {
            let min: usize = ml.extract()?;
            if s.len() < min {
                return Err(ValidationError::new_err(format!(
                    "Document failed validation at '{path}': string too short"
                )));
            }
        }
        if let Some(ml) = schema.get_item("maxLength")? {
            let max: usize = ml.extract()?;
            if s.len() > max {
                return Err(ValidationError::new_err(format!(
                    "Document failed validation at '{path}': string too long"
                )));
            }
        }
        if let Some(pat_obj) = schema.get_item("pattern")? {
            let pattern: String = pat_obj.extract()?;
            if !safe_regex_search(&pattern, &s)? {
                return Err(ValidationError::new_err(format!(
                    "Document failed validation at '{path}': pattern mismatch"
                )));
            }
        }
    }

    // enum
    if let Some(enum_obj) = schema.get_item("enum")? {
        if let Ok(enum_list) = enum_obj.cast::<PyList>() {
            let mut found = false;
            for item in enum_list.iter() {
                if value.eq(&item)? {
                    found = true;
                    break;
                }
            }
            if !found {
                let repr = enum_list.repr()?;
                return Err(ValidationError::new_err(format!(
                    "Document failed validation at '{path}': value not in enum {repr}"
                )));
            }
        }
    }

    Ok(())
}

fn validate_array(
    value: &Bound<'_, PyList>,
    schema: &Bound<'_, PyDict>,
    path: &str,
    depth: usize,
) -> PyResult<()> {
    let len = value.len();

    if let Some(mi) = schema.get_item("minItems")? {
        let min: usize = mi.extract()?;
        if len < min {
            return Err(ValidationError::new_err(format!(
                "Document failed validation at '{path}': too few items"
            )));
        }
    }
    if let Some(mi) = schema.get_item("maxItems")? {
        let max: usize = mi.extract()?;
        if len > max {
            return Err(ValidationError::new_err(format!(
                "Document failed validation at '{path}': too many items"
            )));
        }
    }

    if let Some(ui) = schema.get_item("uniqueItems")? {
        if ui.is_truthy()? {
            let items: Vec<Bound<'_, PyAny>> = value.iter().collect();
            for i in 0..items.len() {
                for j in (i + 1)..items.len() {
                    if items[i].eq(&items[j])? {
                        return Err(ValidationError::new_err(format!(
                            "Document failed validation at '{path}': duplicate items"
                        )));
                    }
                }
            }
        }
    }

    if let Some(items_obj) = schema.get_item("items")? {
        if let Ok(items_schema) = items_obj.cast::<PyDict>() {
            for (i, item) in value.iter().enumerate() {
                validate_value(&item, items_schema, &format!("{path}[{i}]"), depth + 1)?;
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Regex helper (simplified -- schema patterns have no MongoDB $options)
// ---------------------------------------------------------------------------

fn safe_regex_search(pattern: &str, text: &str) -> PyResult<bool> {
    if pattern.len() > MAX_REGEX_PATTERN_LEN {
        return Err(PyValueError::new_err(format!(
            "regex pattern length {} exceeds limit {MAX_REGEX_PATTERN_LEN}",
            pattern.len()
        )));
    }
    if has_nested_quantifiers(pattern) {
        return Err(PyValueError::new_err(
            "regex pattern rejected: nested quantifiers are not allowed",
        ));
    }
    match regex::Regex::new(pattern) {
        Ok(re) => Ok(re.is_match(text)),
        Err(e) => Err(PyValueError::new_err(format!("invalid regex pattern: {e}"))),
    }
}
