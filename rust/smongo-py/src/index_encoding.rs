//! Index-key encoding helpers for WiredTiger sort-order inversion.
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList};

use crate::objectid::ObjectId as RustObjectId;

const HEX_INVERT: [u8; 256] = {
    let mut table = [0u8; 256];
    let mut i = 0usize;
    while i < 256 {
        table[i] = i as u8;
        i += 1;
    }
    table[b'0' as usize] = b'f';
    table[b'1' as usize] = b'e';
    table[b'2' as usize] = b'd';
    table[b'3' as usize] = b'c';
    table[b'4' as usize] = b'b';
    table[b'5' as usize] = b'a';
    table[b'6' as usize] = b'9';
    table[b'7' as usize] = b'8';
    table[b'8' as usize] = b'7';
    table[b'9' as usize] = b'6';
    table[b'a' as usize] = b'5';
    table[b'b' as usize] = b'4';
    table[b'c' as usize] = b'3';
    table[b'd' as usize] = b'2';
    table[b'e' as usize] = b'1';
    table[b'f' as usize] = b'0';
    table
};

pub(crate) fn sortable_encode_impl(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    if value.is_none() {
        return Ok("00".to_string());
    }

    if let Ok(oid) = value.extract::<PyRef<'_, RustObjectId>>() {
        return Ok(format!("15{}", oid.hex()));
    }

    if let Ok(b) = value.extract::<bool>() {
        return Ok(if b { "31" } else { "30" }.to_string());
    }

    if let Ok(v) = value.extract::<f64>() {
        let bytes = v.to_be_bytes();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);
        if buf[0] & 0x80 != 0 {
            for b in &mut buf {
                *b = !*b;
            }
        } else {
            buf[0] ^= 0x80;
        }
        return Ok(format!("1{}", hex::encode(buf)));
    }

    if let Ok(v) = value.extract::<i64>() {
        let f = v as f64;
        let bytes = f.to_be_bytes();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);
        if buf[0] & 0x80 != 0 {
            for b in &mut buf {
                *b = !*b;
            }
        } else {
            buf[0] ^= 0x80;
        }
        return Ok(format!("1{}", hex::encode(buf)));
    }

    if let Ok(s) = value.extract::<String>() {
        return Ok(format!("2{}", hex::encode(s.as_bytes())));
    }

    let json_mod = crate::cached_modules::json_mod(py)?;
    let dumped: String = json_mod
        .call_method(
            "dumps",
            (value,),
            Some(&[("sort_keys", true)].into_py_dict(py)?),
        )
        .and_then(|r| r.extract())?;
    Ok(format!("2{}", hex::encode(dumped.as_bytes())))
}

pub(crate) fn invert_encoded_impl(s: &str) -> String {
    let mut out = Vec::with_capacity(s.len());
    for &b in s.as_bytes() {
        out.push(HEX_INVERT[b as usize]);
    }
    unsafe { String::from_utf8_unchecked(out) }
}

#[pyfunction]
pub fn sortable_encode(value: &Bound<'_, PyAny>) -> PyResult<String> {
    sortable_encode_impl(value.py(), value)
}

#[pyfunction]
pub fn invert_encoded(s: &str) -> String {
    invert_encoded_impl(s)
}

#[pyfunction]
pub fn encode_index_key(
    field_values: &Bound<'_, PyList>,
    doc_id: &str,
    directions: &Bound<'_, PyList>,
) -> PyResult<String> {
    let py = field_values.py();
    let mut parts: Vec<String> = Vec::with_capacity(field_values.len() + 1);
    for (val, dir_obj) in field_values.iter().zip(directions.iter()) {
        let dir: i32 = dir_obj.extract()?;
        let mut encoded = sortable_encode_impl(py, &val)?;
        if dir == -1 {
            encoded = invert_encoded_impl(&encoded);
        }
        parts.push(encoded);
    }
    parts.push(doc_id.to_string());
    Ok(parts.join("|"))
}

#[pyfunction]
pub fn encode_index_key_prefix(
    field_values: &Bound<'_, PyList>,
    directions: &Bound<'_, PyList>,
) -> PyResult<String> {
    let py = field_values.py();
    let mut parts: Vec<String> = Vec::with_capacity(field_values.len());
    for (val, dir_obj) in field_values.iter().zip(directions.iter()) {
        let dir: i32 = dir_obj.extract()?;
        let mut encoded = sortable_encode_impl(py, &val)?;
        if dir == -1 {
            encoded = invert_encoded_impl(&encoded);
        }
        parts.push(encoded);
    }
    let mut key = parts.join("|");
    key.push('|');
    Ok(key)
}
