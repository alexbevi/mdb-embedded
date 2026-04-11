//! BSON Boundary Adapter -- isolates all BSON <-> engine type conversion
//! at the wire edge.
//!
//! Inbound:  wire BSON types  -> engine-friendly Python dicts.
//! Outbound: engine dicts     -> BSON-encodable dicts.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use pyo3::Py;

const MAX_NESTING_DEPTH: usize = 100;

// ---------------------------------------------------------------------------
// Inbound: wire BSON types -> engine types
// ---------------------------------------------------------------------------

fn convert_inbound<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
    depth: usize,
    bson_oid_cls: &Bound<'py, PyAny>,
    decimal128_cls: &Bound<'py, PyAny>,
    regex_cls: &Bound<'py, PyAny>,
    engine_oid_cls: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if depth > MAX_NESTING_DEPTH {
        return Err(PyValueError::new_err(format!(
            "document exceeds maximum nesting depth of {MAX_NESTING_DEPTH}"
        )));
    }

    if value.is_instance(bson_oid_cls)? {
        let hex_str: String = value.str()?.extract()?;
        return engine_oid_cls.call1((hex_str,));
    }

    if value.is_instance(decimal128_cls)? {
        let decimal = value.call_method0("to_decimal")?;
        let float_cls = crate::cached_modules::builtins_float(py)?;
        let f = float_cls.call1((decimal,))?;
        return Ok(f);
    }

    if value.is_instance(regex_cls)? {
        let pattern: Bound<'py, PyAny> = value.getattr("pattern")?;
        let flags_attr: Bound<'py, PyAny> = value.getattr("flags")?;
        let flags_str: Bound<'py, PyAny> = if flags_attr.is_truthy()? {
            flags_attr.str()?.into_any()
        } else {
            PyString::new(py, "").into_any()
        };
        let dict = PyDict::new(py);
        dict.set_item("$regex", pattern)?;
        dict.set_item("$options", flags_str)?;
        return Ok(dict.into_any());
    }

    if let Ok(d) = value.cast::<PyDict>() {
        let result = PyDict::new(py);
        for (k, v) in d.iter() {
            let converted = convert_inbound(
                py,
                &v,
                depth + 1,
                bson_oid_cls,
                decimal128_cls,
                regex_cls,
                engine_oid_cls,
            )?;
            result.set_item(k, converted)?;
        }
        return Ok(result.into_any());
    }

    if let Ok(l) = value.cast::<PyList>() {
        let result = PyList::empty(py);
        for item in l.iter() {
            let converted = convert_inbound(
                py,
                &item,
                depth + 1,
                bson_oid_cls,
                decimal128_cls,
                regex_cls,
                engine_oid_cls,
            )?;
            result.append(converted)?;
        }
        return Ok(result.into_any());
    }

    Ok(value.clone())
}

#[pyfunction]
pub fn normalize_inbound<'py>(py: Python<'py>, doc: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    if doc.is_none() {
        return Ok(py.None().into_any());
    }
    let d = match doc.cast::<PyDict>() {
        Ok(d) => d,
        Err(_) => return Ok(doc.clone().unbind()),
    };

    let bson_oid_cls = crate::cached_modules::bson_objectid_cls(py)?;
    let decimal128_cls = crate::cached_modules::bson_decimal128_cls(py)?;
    let regex_cls = crate::cached_modules::bson_regex_cls(py)?;
    let engine_oid_cls = py.get_type::<crate::objectid::ObjectId>().into_any();

    let result = PyDict::new(py);
    for (k, v) in d.iter() {
        let converted = convert_inbound(
            py,
            &v,
            1,
            &bson_oid_cls,
            &decimal128_cls,
            &regex_cls,
            &engine_oid_cls,
        )?;
        result.set_item(k, converted)?;
    }
    Ok(result.into_any().unbind())
}

// ---------------------------------------------------------------------------
// Outbound: engine types -> wire BSON types
// ---------------------------------------------------------------------------

pub(crate) fn is_objectid_hex(s: &str) -> bool {
    s.len() == 24 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn convert_outbound<'py>(
    py: Python<'py>,
    key: Option<&str>,
    value: &Bound<'py, PyAny>,
    depth: usize,
    engine_oid_cls: &Bound<'py, PyAny>,
    bson_oid_cls: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if depth > MAX_NESTING_DEPTH {
        return Err(PyValueError::new_err(format!(
            "document exceeds maximum nesting depth of {MAX_NESTING_DEPTH}"
        )));
    }

    if value.is_instance(engine_oid_cls)? {
        let hex_str: String = value.str()?.extract()?;
        return bson_oid_cls.call1((hex_str,));
    }

    if key == Some("_id") {
        if let Ok(s) = value.extract::<String>() {
            if is_objectid_hex(&s) {
                return bson_oid_cls.call1((s,));
            }
        }
    }

    if let Ok(d) = value.cast::<PyDict>() {
        let result = PyDict::new(py);
        for (k, v) in d.iter() {
            let key_str: String = k.extract()?;
            let converted = convert_outbound(
                py,
                Some(&key_str),
                &v,
                depth + 1,
                engine_oid_cls,
                bson_oid_cls,
            )?;
            result.set_item(k, converted)?;
        }
        return Ok(result.into_any());
    }

    if let Ok(l) = value.cast::<PyList>() {
        let result = PyList::empty(py);
        for item in l.iter() {
            let converted =
                convert_outbound(py, None, &item, depth + 1, engine_oid_cls, bson_oid_cls)?;
            result.append(converted)?;
        }
        return Ok(result.into_any());
    }

    Ok(value.clone())
}

#[pyfunction]
pub fn normalize_outbound<'py>(py: Python<'py>, doc: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    if doc.is_none() {
        return Ok(py.None().into_any());
    }
    let d = match doc.cast::<PyDict>() {
        Ok(d) => d,
        Err(_) => return Ok(doc.clone().unbind()),
    };

    let engine_oid_cls = py.get_type::<crate::objectid::ObjectId>().into_any();
    let bson_oid_cls = crate::cached_modules::bson_objectid_cls(py)?;

    let result = PyDict::new(py);
    for (k, v) in d.iter() {
        let key_str: String = k.extract()?;
        let converted =
            convert_outbound(py, Some(&key_str), &v, 1, &engine_oid_cls, &bson_oid_cls)?;
        result.set_item(k, converted)?;
    }
    Ok(result.into_any().unbind())
}

#[pyfunction]
pub fn normalize_outbound_docs<'py>(
    py: Python<'py>,
    docs: &Bound<'py, PyList>,
) -> PyResult<Py<PyList>> {
    let engine_oid_cls = py.get_type::<crate::objectid::ObjectId>().into_any();
    let bson_oid_cls = crate::cached_modules::bson_objectid_cls(py)?;

    let result = PyList::empty(py);
    for item in docs.iter() {
        let d = match item.cast::<PyDict>() {
            Ok(d) => d,
            Err(_) => {
                result.append(item)?;
                continue;
            }
        };
        let out = PyDict::new(py);
        for (k, v) in d.iter() {
            let key_str: String = k.extract()?;
            let converted =
                convert_outbound(py, Some(&key_str), &v, 1, &engine_oid_cls, &bson_oid_cls)?;
            out.set_item(k, converted)?;
        }
        result.append(out)?;
    }
    Ok(result.unbind())
}
