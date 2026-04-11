//! BSON codec for the wire protocol.
//!
//! Both encoding and decoding delegate to the `bson` Rust crate
//! (maintained by the MongoDB team).  This guarantees spec-compliant
//! BSON that is byte-compatible with every MongoDB driver and tool
//! (Compass, mongosh, Node.js driver, PyMongo, etc.).
//!
//! **Encoder**: Python dict → `bson::Document` (via `pydict_to_doc`) → `bson::to_vec`
//! **Decoder**: `bson::from_slice` → `bson::Document` → Python dict (via `doc_to_pydict`)

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

// ---------------------------------------------------------------------------
// Decoder: raw BSON bytes  →  Python dict
// ---------------------------------------------------------------------------

/// Decode a BSON document starting at `data[*offset]`.
///
/// Reads the 4-byte length prefix, parses the document via the `bson`
/// crate, converts it to a Python dict, and advances `*offset` past
/// the entire document.
pub(crate) fn raw_decode_document<'py>(
    py: Python<'py>,
    data: &[u8],
    offset: &mut usize,
    _depth: usize,
) -> PyResult<Bound<'py, PyDict>> {
    if *offset + 4 > data.len() {
        return Err(PyValueError::new_err(format!(
            "BSON truncated: need 4 bytes at offset {}, have {}",
            offset,
            data.len()
        )));
    }
    let doc_len =
        i32::from_le_bytes([data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]])
            as usize;
    if doc_len < 5 {
        return Err(PyValueError::new_err(format!(
            "BSON document length {doc_len} is too small"
        )));
    }
    if *offset + doc_len > data.len() {
        return Err(PyValueError::new_err(format!(
            "BSON truncated: document says {doc_len} bytes at offset {}, have {}",
            offset,
            data.len()
        )));
    }

    let slice = &data[*offset..*offset + doc_len];
    let doc: bson::Document = bson::from_slice(slice)
        .map_err(|e| PyValueError::new_err(format!("BSON decode error: {e}")))?;
    *offset += doc_len;

    crate::bson_helpers::doc_to_pydict(py, &doc)
}

/// Convenience wrapper: decode a complete BSON document from a byte slice.
pub(crate) fn raw_decode_slice<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyDict>> {
    let mut offset = 0;
    raw_decode_document(py, data, &mut offset, 0)
}

// ---------------------------------------------------------------------------
// Encoder: Python dict  →  raw BSON bytes
// ---------------------------------------------------------------------------

/// Encode a Python dict as a raw BSON document byte vector.
///
/// Delegates to the `bson` Rust crate via `pydict_to_doc` → `bson::to_vec`.
pub(crate) fn raw_encode_document(_py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<Vec<u8>> {
    let doc = crate::bson_helpers::pydict_to_doc(dict)?;
    bson::to_vec(&doc).map_err(|e| PyValueError::new_err(format!("BSON encode error: {e}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pyo3::types::{PyBytes, PyList};

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
            dict.set_item("int32", 42).unwrap();
            dict.set_item("int64", 3_000_000_000i64).unwrap();
            dict.set_item("float", 1.234).unwrap();
            dict.set_item("bool_t", true).unwrap();
            dict.set_item("bool_f", false).unwrap();
            dict.set_item("none", py.None()).unwrap();

            let encoded = raw_encode_document(py, &dict).unwrap();
            let decoded = raw_decode_slice(py, &encoded).unwrap();

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
                    .get_item("int32")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                42
            );
            assert_eq!(
                decoded
                    .get_item("int64")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                3_000_000_000i64
            );
            let fv: f64 = decoded
                .get_item("float")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert!((fv - 1.234).abs() < 1e-10);
            assert!(decoded
                .get_item("bool_t")
                .unwrap()
                .unwrap()
                .extract::<bool>()
                .unwrap());
            assert!(!decoded
                .get_item("bool_f")
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

            let encoded = raw_encode_document(py, &outer).unwrap();
            let decoded = raw_decode_slice(py, &encoded).unwrap();

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

            let encoded = raw_encode_document(py, &dict).unwrap();
            let decoded = raw_decode_slice(py, &encoded).unwrap();

            let result = decoded.get_item("bin").unwrap().unwrap();
            let result_bytes = result.cast::<PyBytes>().unwrap();
            assert_eq!(result_bytes.as_bytes(), b"\x01\x02\x03");
        });
    }

    #[test]
    fn test_roundtrip_objectid() {
        with_py(|py| {
            use crate::objectid::ObjectId as RustObjectId;

            let oid = RustObjectId::generate(py);
            let raw_bytes = oid.raw_bytes();
            let oid_py = Py::new(py, oid).unwrap();

            let dict = PyDict::new(py);
            dict.set_item("_id", oid_py.bind(py)).unwrap();

            let encoded = raw_encode_document(py, &dict).unwrap();
            let decoded = raw_decode_slice(py, &encoded).unwrap();

            let result = decoded.get_item("_id").unwrap().unwrap();
            assert!(result.is_instance_of::<RustObjectId>());
            let result_oid: PyRef<'_, RustObjectId> = result.extract().unwrap();
            assert_eq!(result_oid.raw_bytes(), raw_bytes);
        });
    }

    #[test]
    fn test_empty_document() {
        with_py(|py| {
            let dict = PyDict::new(py);
            let encoded = raw_encode_document(py, &dict).unwrap();
            assert_eq!(encoded.len(), 5);
            assert_eq!(encoded, vec![5, 0, 0, 0, 0]);

            let decoded = raw_decode_slice(py, &encoded).unwrap();
            assert_eq!(decoded.len(), 0);
        });
    }

    #[test]
    fn test_decode_truncated_errors() {
        with_py(|py| {
            assert!(raw_decode_slice(py, &[]).is_err());
            assert!(raw_decode_slice(py, &[5, 0, 0]).is_err());
            assert!(raw_decode_slice(py, &[10, 0, 0, 0, 0]).is_err());
        });
    }

    #[test]
    fn test_compat_with_bson_crate() {
        with_py(|py| {
            let dict = PyDict::new(py);
            dict.set_item("a", 42).unwrap();
            dict.set_item("b", "hello").unwrap();
            dict.set_item("c", 3.14).unwrap();

            let raw_bytes = raw_encode_document(py, &dict).unwrap();
            let bson_doc: bson::Document = bson::from_slice(&raw_bytes).unwrap();
            assert_eq!(bson_doc.get_i32("a").unwrap(), 42);
            assert_eq!(bson_doc.get_str("b").unwrap(), "hello");
            assert!((bson_doc.get_f64("c").unwrap() - 3.14).abs() < 1e-10);

            let mut doc = bson::Document::new();
            doc.insert("x", bson::Bson::Int32(99));
            doc.insert("y", bson::Bson::String("world".to_string()));
            let bson_bytes = bson::to_vec(&doc).unwrap();
            let decoded = raw_decode_slice(py, &bson_bytes).unwrap();
            assert_eq!(
                decoded
                    .get_item("x")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                99
            );
            assert_eq!(
                decoded
                    .get_item("y")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "world"
            );
        });
    }

    #[test]
    fn test_tuple_encoded_as_array() {
        with_py(|py| {
            let dict = PyDict::new(py);
            let tup = pyo3::types::PyTuple::new(py, [1, 2, 3]).unwrap();
            dict.set_item("arr", tup).unwrap();

            let encoded = raw_encode_document(py, &dict).unwrap();
            let bson_doc: bson::Document = bson::from_slice(&encoded).unwrap();
            let arr = bson_doc.get_array("arr").unwrap();
            assert_eq!(arr.len(), 3);
        });
    }
}
