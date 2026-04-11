//! Bidirectional conversion between Python objects and BSON documents.
//!
//! Hot-path encode/decode (`to_bson` / `from_bson`) now delegate to the
//! single-pass raw BSON codec in [`crate::raw_bson`].  The intermediate
//! `bson::Document` helpers below are retained for non-wire callers
//! (storage layer, tests) that still need them.
use bson::oid::ObjectId as BsonOid;
use bson::spec::BinarySubtype;
use bson::{Bson, Document};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};

use crate::objectid::ObjectId as RustObjectId;

/// Shallow-copy a Python dict entirely in Rust, bypassing `dict.copy()`
/// Python method dispatch (~600ns saved per call).
pub(crate) fn shallow_copy_dict<'py>(
    py: Python<'py>,
    src: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyDict>> {
    let new = PyDict::new(py);
    for (k, v) in src.iter() {
        new.set_item(k, v)?;
    }
    Ok(new)
}

/// Convert a Python value into a BSON value.
///
/// Handles the same types as the Python `_normalize` function:
/// dict, list, smongo ObjectId, bson.ObjectId, str, int, float, bool,
/// None, bytes, and falls back to `str(v)` for anything else.
///
/// Retained for non-wire callers (storage layer) that still need
/// `bson::Document` intermediates.
pub(crate) fn py_to_bson(val: &Bound<'_, PyAny>) -> PyResult<Bson> {
    let py = val.py();

    if val.is_none() {
        return Ok(Bson::Null);
    }

    // bool must come before int (Python bool is a subclass of int)
    if let Ok(b) = val.cast::<PyBool>() {
        return Ok(Bson::Boolean(b.is_true()));
    }

    if let Ok(i) = val.cast::<PyInt>() {
        let v: i64 = i.extract()?;
        if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
            return Ok(Bson::Int32(v as i32));
        }
        return Ok(Bson::Int64(v));
    }

    if let Ok(f) = val.cast::<PyFloat>() {
        return Ok(Bson::Double(f.extract()?));
    }

    if let Ok(s) = val.cast::<PyString>() {
        let s_str = s.to_str()?;
        // MinKey/MaxKey sentinel strings
        if s_str == "$MinKey" {
            return Ok(Bson::MinKey);
        }
        if s_str == "$MaxKey" {
            return Ok(Bson::MaxKey);
        }
        return Ok(Bson::String(s_str.to_owned()));
    }

    // smongo.ObjectId (our Rust-backed ObjectId)
    if let Ok(oid) = val.extract::<PyRef<'_, RustObjectId>>() {
        let raw = oid.raw_bytes();
        return Ok(Bson::ObjectId(BsonOid::from_bytes(raw)));
    }

    // bson.Decimal128 → lossless via .bid raw bytes
    if let Ok(d128_cls) = crate::cached_modules::bson_decimal128_cls(py) {
        if val.is_instance(&d128_cls)? {
            let bid = val.getattr("bid")?;
            let raw: Vec<u8> = bid.extract()?;
            if raw.len() == 16 {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&raw);
                return Ok(Bson::Decimal128(bson::Decimal128::from_bytes(bytes)));
            }
        }
    }

    // bson.Regex → BSON RegularExpression
    if let Ok(regex_cls) = crate::cached_modules::bson_regex_cls(py) {
        if val.is_instance(&regex_cls)? {
            let pattern: String = val.getattr("pattern")?.extract()?;
            let flags: String = val
                .getattr("flags")?
                .extract::<String>()
                .unwrap_or_default();
            return Ok(Bson::RegularExpression(bson::Regex {
                pattern,
                options: flags,
            }));
        }
    }

    // uuid.UUID → BSON Binary subtype 4
    if let Ok(uuid_cls) = crate::cached_modules::uuid_uuid_cls(py) {
        if val.is_instance(&uuid_cls)? {
            let uuid_bytes: Vec<u8> = val.getattr("bytes")?.extract()?;
            return Ok(Bson::Binary(bson::Binary {
                subtype: BinarySubtype::Uuid,
                bytes: uuid_bytes,
            }));
        }
    }

    // bson.Binary before plain bytes (Binary subclasses bytes, needs subtype preserved)
    if let Ok(bin_cls) = crate::cached_modules::bson_binary_cls(py) {
        if val.is_instance(&bin_cls)? {
            let bytes: Vec<u8> = val.extract()?;
            let subtype_int: u8 = val.getattr("subtype")?.extract()?;
            let subtype = BinarySubtype::from(subtype_int);
            return Ok(Bson::Binary(bson::Binary {
                subtype,
                bytes,
            }));
        }
    }

    if let Ok(b) = val.cast::<PyBytes>() {
        return Ok(Bson::Binary(bson::Binary {
            subtype: BinarySubtype::Generic,
            bytes: b.as_bytes().to_vec(),
        }));
    }

    // re.Pattern (compiled regex) → BSON RegularExpression
    let type_name = val.get_type().qualname()?.to_string();
    if type_name == "Pattern" {
        let pattern: String = val.getattr("pattern")?.extract()?;
        let flags_int: u32 = val.getattr("flags")?.extract()?;
        let mut opts = String::new();
        if flags_int & 2 != 0 {
            opts.push('i');
        }
        if flags_int & 8 != 0 {
            opts.push('m');
        }
        if flags_int & 16 != 0 {
            opts.push('s');
        }
        if flags_int & 64 != 0 {
            opts.push('x');
        }
        return Ok(Bson::RegularExpression(bson::Regex {
            pattern,
            options: opts,
        }));
    }

    if let Ok(d) = val.cast::<PyDict>() {
        return Ok(Bson::Document(pydict_to_doc(d)?));
    }

    if let Ok(l) = val.cast::<PyList>() {
        return Ok(Bson::Array(pylist_to_array(l)?));
    }

    // Python tuples → BSON arrays (same as lists)
    if let Ok(t) = val.cast::<pyo3::types::PyTuple>() {
        let mut arr = Vec::with_capacity(t.len());
        for item in t.iter() {
            arr.push(py_to_bson(&item)?);
        }
        return Ok(Bson::Array(arr));
    }

    // datetime.datetime -> BSON DateTime
    let datetime_cls = crate::cached_modules::datetime_datetime_cls(py)?;
    if val.is_instance(&datetime_cls)? {
        let ts: f64 = val.call_method0("timestamp")?.extract()?;
        let millis = (ts * 1000.0) as i64;
        return Ok(Bson::DateTime(bson::DateTime::from_millis(millis)));
    }

    // PyMongo bson.ObjectId — extract via str() -> hex -> BsonOid
    if type_name == "ObjectId" {
        let hex_str: String = val.str()?.extract()?;
        if hex_str.len() == 24 {
            if let Ok(oid) = BsonOid::parse_str(&hex_str) {
                return Ok(Bson::ObjectId(oid));
            }
        }
    }

    // Fallback: str(v) with warning
    let warnings = py.import("warnings")?;
    let msg = format!(
        "smongo BSON encoder (storage): unknown type '{}', encoding as string",
        type_name
    );
    warnings.call_method1("warn", (msg,))?;

    let s: String = val.str()?.extract()?;
    Ok(Bson::String(s))
}

/// Convert a Python dict to a BSON Document.
pub(crate) fn pydict_to_doc(dict: &Bound<'_, PyDict>) -> PyResult<Document> {
    let mut doc = Document::new();
    for (k, v) in dict.iter() {
        let key: String = k.extract()?;
        doc.insert(key, py_to_bson(&v)?);
    }
    Ok(doc)
}

/// Convert any dict-like Python object to a BSON Document.
///
/// Uses Python's `.items()` protocol instead of the C-level `PyDict_Next`,
/// making it safe for dict subclasses (`bson.SON`, `OrderedDict`, etc.)
/// that may not iterate correctly via the C API.
pub(crate) fn pyany_to_doc(obj: &Bound<'_, PyAny>) -> PyResult<Document> {
    if let Ok(dict) = obj.cast::<PyDict>() {
        return pydict_to_doc(dict);
    }
    let mut doc = Document::new();
    let type_name = obj
        .get_type()
        .qualname()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    let items = obj.call_method0("items").map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "Expected a dict-like object with .items(), got {type_name}"
        ))
    })?;
    for pair in items.try_iter()? {
        let pair = pair?;
        let key: String = pair.get_item(0)?.extract()?;
        let val = pair.get_item(1)?;
        doc.insert(key, py_to_bson(&val)?);
    }
    Ok(doc)
}

/// Convert a Python list of dict-like objects to a BSON pipeline,
/// validating that each stage has a single `$`-prefixed operator key.
pub(crate) fn pylist_to_pipeline(list: &Bound<'_, PyList>) -> PyResult<Vec<Document>> {
    let mut pipeline = Vec::with_capacity(list.len());
    for (i, item) in list.iter().enumerate() {
        let doc = pyany_to_doc(&item)?;
        if let Some((first_key, _)) = doc.iter().next() {
            if first_key.is_empty() || !first_key.starts_with('$') {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Pipeline stage {} has invalid operator key {:?} \
                     (expected a $-prefixed operator like $match, $group, etc.). \
                     All keys: {:?}",
                    i,
                    first_key,
                    doc.keys().collect::<Vec<_>>()
                )));
            }
        } else {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Pipeline stage {} is an empty document",
                i
            )));
        }
        pipeline.push(doc);
    }
    Ok(pipeline)
}

/// Convert a Python list to a BSON Array.
fn pylist_to_array(list: &Bound<'_, PyList>) -> PyResult<Vec<Bson>> {
    let mut arr = Vec::with_capacity(list.len());
    for item in list.iter() {
        arr.push(py_to_bson(&item)?);
    }
    Ok(arr)
}

/// Convert a BSON value back to a Python object.
///
/// Handles the denormalization inline: `bson::oid::ObjectId` is converted
/// back to the smongo `ObjectId` class.
pub(crate) fn bson_to_py<'py>(py: Python<'py>, val: &Bson) -> PyResult<Bound<'py, PyAny>> {
    match val {
        Bson::Null => Ok(py.None().into_bound(py)),
        Bson::Boolean(b) => Ok(b.into_pyobject(py)?.to_owned().into_any()),
        Bson::Int32(i) => Ok(i.into_pyobject(py)?.to_owned().into_any()),
        Bson::Int64(i) => Ok(i.into_pyobject(py)?.to_owned().into_any()),
        Bson::Double(f) => Ok(f.into_pyobject(py)?.to_owned().into_any()),
        Bson::String(s) => Ok(s.into_pyobject(py)?.to_owned().into_any()),
        Bson::Binary(bin) => Ok(PyBytes::new(py, &bin.bytes).into_any()),
        Bson::ObjectId(oid) => {
            let hex_str = oid.to_hex();
            let obj = Py::new(py, crate::objectid::ObjectId::from_hex(py, &hex_str)?)?;
            Ok(obj.into_bound(py).into_any())
        }
        Bson::DateTime(dt) => {
            let millis = dt.timestamp_millis();
            let secs = millis / 1000;
            let micros = ((millis % 1000) * 1000) as u32;
            let utc = crate::cached_modules::datetime_tz_utc(py)?;
            let dt_obj = crate::cached_modules::datetime_datetime_cls(py)?.call_method1(
                "fromtimestamp",
                (secs as f64 + micros as f64 / 1_000_000.0, utc),
            )?;
            Ok(dt_obj)
        }
        Bson::Document(d) => {
            let dict = doc_to_pydict(py, d)?;
            Ok(dict.into_any())
        }
        Bson::Array(arr) => {
            let list = array_to_pylist(py, arr)?;
            Ok(list.into_any())
        }
        Bson::RegularExpression(regex) => {
            let re_mod = crate::cached_modules::re_mod(py)?;
            let pattern = &regex.pattern;
            let mut flags = 0u32;
            for c in regex.options.chars() {
                match c {
                    'i' => flags |= 2,  // re.IGNORECASE
                    'm' => flags |= 8,  // re.MULTILINE
                    's' => flags |= 16, // re.DOTALL
                    'x' => flags |= 64, // re.VERBOSE
                    _ => {}
                }
            }
            let compiled = re_mod.call_method1("compile", (pattern, flags))?;
            Ok(compiled)
        }
        Bson::Timestamp(ts) => {
            let dict = PyDict::new(py);
            dict.set_item("t", ts.time)?;
            dict.set_item("i", ts.increment)?;
            Ok(dict.into_any())
        }
        // Decimal128, Symbol, etc. -- return as string representation
        other => {
            let s = format!("{other}");
            Ok(s.into_pyobject(py)?.to_owned().into_any())
        }
    }
}

/// Convert a BSON Document to a Python dict.
pub(crate) fn doc_to_pydict<'py>(py: Python<'py>, doc: &Document) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (k, v) in doc.iter() {
        dict.set_item(k, bson_to_py(py, v)?)?;
    }
    Ok(dict)
}

/// Convert a BSON Array to a Python list.
pub(crate) fn array_to_pylist<'py>(py: Python<'py>, arr: &[Bson]) -> PyResult<Bound<'py, PyList>> {
    let items: Vec<Bound<'py, PyAny>> = arr
        .iter()
        .map(|v| bson_to_py(py, v))
        .collect::<PyResult<_>>()?;
    PyList::new(py, items)
}

/// Encode a Python dict to BSON bytes.
///
/// Single-pass raw encoder: converts `smongo.ObjectId`, `datetime`,
/// `_id` hex strings, and all standard Python types directly to BSON
/// wire bytes without intermediate `bson::Document` allocation.
#[pyfunction]
pub fn to_bson(doc: &Bound<'_, PyDict>) -> PyResult<Py<PyBytes>> {
    let py = doc.py();
    let bytes = crate::raw_bson::raw_encode_document(py, doc)?;
    Ok(PyBytes::new(py, &bytes).unbind())
}

/// Decode BSON bytes to a Python dict.
///
/// Single-pass raw decoder: parses BSON wire bytes directly into
/// engine-ready Python dicts (`smongo.ObjectId`, Python `datetime`,
/// `float` for Decimal128, regex dicts, etc.) without intermediate
/// `bson::Document` allocation.
#[pyfunction]
pub fn from_bson<'py>(py: Python<'py>, raw: &[u8]) -> PyResult<Bound<'py, PyDict>> {
    crate::raw_bson::raw_decode_slice(py, raw)
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
    fn test_roundtrip_basic_types() {
        with_py(|py| {
            let dict = PyDict::new(py);
            dict.set_item("str", "hello").unwrap();
            dict.set_item("int", 42).unwrap();
            dict.set_item("float", 1.234).unwrap();
            dict.set_item("bool", true).unwrap();
            dict.set_item("none", py.None()).unwrap();

            let encoded = to_bson(&dict).unwrap();
            let decoded = from_bson(py, encoded.as_bytes(py)).unwrap();

            assert_eq!(
                decoded
                    .get_item("str")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "hello"
            );
            assert_eq!(
                decoded
                    .get_item("int")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                42
            );
            let float_val: f64 = decoded
                .get_item("float")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert!((float_val - 1.234).abs() < 1e-10);
            assert!(decoded
                .get_item("bool")
                .unwrap()
                .unwrap()
                .extract::<bool>()
                .unwrap());
            assert!(decoded.get_item("none").unwrap().unwrap().is_none());
        });
    }

    #[test]
    fn test_roundtrip_nested() {
        with_py(|py| {
            let inner = PyDict::new(py);
            inner.set_item("x", 1).unwrap();
            let outer = PyDict::new(py);
            outer.set_item("nested", inner).unwrap();
            let list = PyList::new(py, [1, 2, 3]).unwrap();
            outer.set_item("list", list).unwrap();

            let encoded = to_bson(&outer).unwrap();
            let decoded = from_bson(py, encoded.as_bytes(py)).unwrap();

            let nested_val = decoded.get_item("nested").unwrap().unwrap();
            let nested = nested_val.cast::<PyDict>().unwrap();
            assert_eq!(
                nested
                    .get_item("x")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                1
            );

            let list_val = decoded.get_item("list").unwrap().unwrap();
            let list = list_val.cast::<PyList>().unwrap();
            assert_eq!(list.len(), 3);
        });
    }

    #[test]
    fn test_roundtrip_bytes() {
        with_py(|py| {
            let dict = PyDict::new(py);
            let data = PyBytes::new(py, b"\x01\x02\x03");
            dict.set_item("bin", data).unwrap();

            let encoded = to_bson(&dict).unwrap();
            let decoded = from_bson(py, encoded.as_bytes(py)).unwrap();

            let result = decoded.get_item("bin").unwrap().unwrap();
            let result_bytes = result.cast::<PyBytes>().unwrap();
            assert_eq!(result_bytes.as_bytes(), b"\x01\x02\x03");
        });
    }

    #[test]
    fn test_int32_vs_int64() {
        with_py(|py| {
            let dict = PyDict::new(py);
            dict.set_item("small", 42).unwrap();
            dict.set_item("big", 3_000_000_000i64).unwrap();

            let encoded = to_bson(&dict).unwrap();
            let decoded = from_bson(py, encoded.as_bytes(py)).unwrap();

            assert_eq!(
                decoded
                    .get_item("small")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                42
            );
            assert_eq!(
                decoded
                    .get_item("big")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                3_000_000_000i64
            );
        });
    }
}
