//! Single-pass BSON byte-level decoder and encoder.
//!
//! Converts directly between raw BSON bytes and engine-ready Python dicts
//! without intermediate `bson::Document` allocation.  The decoder produces
//! engine types inline (smongo `ObjectId`, Python `datetime`, `bson.Binary`
//! with subtype, regex dict, etc.) so the wire path no longer needs a second
//! `normalize_inbound` walk.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};

use crate::objectid::ObjectId as RustObjectId;

const MAX_DEPTH: usize = 100;

// BSON element type tags (bsonspec.org)
const BSON_DOUBLE: u8 = 0x01;
const BSON_STRING: u8 = 0x02;
const BSON_DOCUMENT: u8 = 0x03;
const BSON_ARRAY: u8 = 0x04;
const BSON_BINARY: u8 = 0x05;
const BSON_UNDEFINED: u8 = 0x06;
const BSON_OBJECTID: u8 = 0x07;
const BSON_BOOLEAN: u8 = 0x08;
const BSON_DATETIME: u8 = 0x09;
const BSON_NULL: u8 = 0x0A;
const BSON_REGEX: u8 = 0x0B;
const BSON_DBPOINTER: u8 = 0x0C;
const BSON_JAVASCRIPT: u8 = 0x0D;
const BSON_SYMBOL: u8 = 0x0E;
const BSON_CODE_W_SCOPE: u8 = 0x0F;
const BSON_INT32: u8 = 0x10;
const BSON_TIMESTAMP: u8 = 0x11;
const BSON_INT64: u8 = 0x12;
const BSON_DECIMAL128: u8 = 0x13;
const BSON_MAX_KEY: u8 = 0x7F;
const BSON_MIN_KEY: u8 = 0xFF;

// ---------------------------------------------------------------------------
// Array index string cache: avoid allocating "0", "1", ... per element.
// ---------------------------------------------------------------------------

const IDX_CACHE_SIZE: usize = 1000;

/// Pre-computed string representations of 0..999.
fn cached_idx_str(i: usize) -> String {
    static CACHE: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(|| (0..IDX_CACHE_SIZE).map(|n| n.to_string()).collect());
    if i < IDX_CACHE_SIZE {
        cache[i].clone()
    } else {
        i.to_string()
    }
}

// ---------------------------------------------------------------------------
// Little-endian read helpers
// ---------------------------------------------------------------------------

#[inline]
fn read_i32_le(data: &[u8], off: usize) -> i32 {
    i32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
}

#[inline]
fn read_u32_le(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
}

#[inline]
fn read_i64_le(data: &[u8], off: usize) -> i64 {
    i64::from_le_bytes([
        data[off],
        data[off + 1],
        data[off + 2],
        data[off + 3],
        data[off + 4],
        data[off + 5],
        data[off + 6],
        data[off + 7],
    ])
}

#[inline]
fn read_u64_le(data: &[u8], off: usize) -> u64 {
    u64::from_le_bytes([
        data[off],
        data[off + 1],
        data[off + 2],
        data[off + 3],
        data[off + 4],
        data[off + 5],
        data[off + 6],
        data[off + 7],
    ])
}

#[inline]
fn read_f64_le(data: &[u8], off: usize) -> f64 {
    f64::from_le_bytes([
        data[off],
        data[off + 1],
        data[off + 2],
        data[off + 3],
        data[off + 4],
        data[off + 5],
        data[off + 6],
        data[off + 7],
    ])
}

/// Bounds check: ensure `need` bytes are available at `offset`.
#[inline]
fn ensure(data: &[u8], offset: usize, need: usize) -> PyResult<()> {
    if offset + need > data.len() {
        return Err(PyValueError::new_err(format!(
            "BSON truncated: need {need} bytes at offset {offset}, have {}",
            data.len()
        )));
    }
    Ok(())
}

/// Read a null-terminated C string.  Advances `offset` past the null byte.
fn read_cstring<'a>(data: &'a [u8], offset: &mut usize) -> PyResult<&'a str> {
    let start = *offset;
    let rest = data.get(start..).ok_or_else(|| {
        PyValueError::new_err(format!("BSON: cstring read past end at offset {start}"))
    })?;
    let null_pos = rest
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| PyValueError::new_err("BSON: unterminated cstring"))?
        + start;
    let s = std::str::from_utf8(&data[start..null_pos])
        .map_err(|e| PyValueError::new_err(format!("BSON: invalid UTF-8 in cstring: {e}")))?;
    *offset = null_pos + 1;
    Ok(s)
}

// ---------------------------------------------------------------------------
// Decoder: raw BSON bytes  →  engine-ready PyDict
// ---------------------------------------------------------------------------

/// Decode a BSON document starting at `data[*offset]`.
///
/// Reads the 4-byte length prefix, parses all elements, and advances
/// `*offset` past the entire document (including the trailing `0x00`).
/// All BSON types are converted to engine types inline:
///   ObjectId → `smongo.ObjectId`, DateTime → Python datetime,
///   Decimal128 → `bson.Decimal128`, Binary → `bson.Binary` (with subtype),
///   Regex → `{$regex, $options}` dict, etc.
pub(crate) fn raw_decode_document<'py>(
    py: Python<'py>,
    data: &[u8],
    offset: &mut usize,
    depth: usize,
) -> PyResult<Bound<'py, PyDict>> {
    if depth > MAX_DEPTH {
        return Err(PyValueError::new_err(format!(
            "BSON document exceeds maximum nesting depth of {MAX_DEPTH}"
        )));
    }

    ensure(data, *offset, 5)?;
    let doc_len = read_i32_le(data, *offset) as usize;
    if doc_len < 5 {
        return Err(PyValueError::new_err(format!(
            "BSON document length {doc_len} is too small"
        )));
    }
    ensure(data, *offset, doc_len)?;
    let doc_end = *offset + doc_len;
    *offset += 4;

    let dict = PyDict::new(py);

    while *offset < doc_end - 1 {
        let type_tag = data[*offset];
        *offset += 1;
        if type_tag == 0 {
            break;
        }
        let key = read_cstring(data, offset)?;
        let value = decode_value(py, data, offset, type_tag, depth)?;
        dict.set_item(key, value)?;
    }

    *offset = doc_end;
    Ok(dict)
}

/// Convenience wrapper: decode a complete BSON document from a byte slice.
pub(crate) fn raw_decode_slice<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyDict>> {
    let mut offset = 0;
    raw_decode_document(py, data, &mut offset, 0)
}

/// Decode a BSON array into a Python list.
fn decode_array<'py>(
    py: Python<'py>,
    data: &[u8],
    offset: &mut usize,
    depth: usize,
) -> PyResult<Bound<'py, PyList>> {
    if depth > MAX_DEPTH {
        return Err(PyValueError::new_err(format!(
            "BSON array exceeds maximum nesting depth of {MAX_DEPTH}"
        )));
    }

    ensure(data, *offset, 5)?;
    let doc_len = read_i32_le(data, *offset) as usize;
    if doc_len < 5 {
        return Err(PyValueError::new_err("BSON array length too small"));
    }
    ensure(data, *offset, doc_len)?;
    let doc_end = *offset + doc_len;
    *offset += 4;

    let list = PyList::empty(py);

    while *offset < doc_end - 1 {
        let type_tag = data[*offset];
        *offset += 1;
        if type_tag == 0 {
            break;
        }
        // Skip the array index key ("0", "1", ...)
        let _ = read_cstring(data, offset)?;
        let value = decode_value(py, data, offset, type_tag, depth)?;
        list.append(value)?;
    }

    *offset = doc_end;
    Ok(list)
}

/// Decode a single BSON value based on its type tag.
fn decode_value<'py>(
    py: Python<'py>,
    data: &[u8],
    offset: &mut usize,
    type_tag: u8,
    depth: usize,
) -> PyResult<Bound<'py, PyAny>> {
    match type_tag {
        BSON_DOUBLE => {
            ensure(data, *offset, 8)?;
            let v = read_f64_le(data, *offset);
            *offset += 8;
            Ok(v.into_pyobject(py)?.to_owned().into_any())
        }

        BSON_STRING | BSON_JAVASCRIPT | BSON_SYMBOL => {
            ensure(data, *offset, 4)?;
            let str_len = read_i32_le(data, *offset) as usize;
            *offset += 4;
            if str_len < 1 {
                return Err(PyValueError::new_err("BSON string length < 1"));
            }
            ensure(data, *offset, str_len)?;
            let s = std::str::from_utf8(&data[*offset..*offset + str_len - 1])
                .map_err(|e| PyValueError::new_err(format!("BSON: invalid UTF-8: {e}")))?;
            *offset += str_len;
            Ok(PyString::new(py, s).into_any())
        }

        BSON_DOCUMENT => {
            let dict = raw_decode_document(py, data, offset, depth + 1)?;
            Ok(dict.into_any())
        }

        BSON_ARRAY => {
            let list = decode_array(py, data, offset, depth + 1)?;
            Ok(list.into_any())
        }

        BSON_BINARY => {
            ensure(data, *offset, 5)?;
            let bin_len = read_i32_le(data, *offset) as usize;
            *offset += 4;
            let subtype = data[*offset];
            *offset += 1;
            ensure(data, *offset, bin_len)?;
            let bytes = &data[*offset..*offset + bin_len];
            *offset += bin_len;

            if subtype == 0x00 {
                return Ok(PyBytes::new(py, bytes).into_any());
            }
            // subtype 4 = UUID — construct via keyword arg (first positional is hex, not bytes)
            if subtype == 0x04 && bin_len == 16 {
                let uuid_cls = crate::cached_modules::uuid_uuid_cls(py)?;
                let py_bytes = PyBytes::new(py, bytes);
                let kwargs = PyDict::new(py);
                kwargs.set_item("bytes", py_bytes)?;
                return Ok(uuid_cls.call((), Some(&kwargs))?.into_any());
            }
            // Non-zero subtype: return bson.Binary to preserve the subtype
            if let Ok(bin_cls) = crate::cached_modules::bson_binary_cls(py) {
                let py_bytes = PyBytes::new(py, bytes);
                return Ok(bin_cls.call1((py_bytes, subtype as i32))?.into_any());
            }
            Ok(PyBytes::new(py, bytes).into_any())
        }

        BSON_UNDEFINED | BSON_NULL => Ok(py.None().into_bound(py)),

        BSON_OBJECTID => {
            ensure(data, *offset, 12)?;
            let mut raw = [0u8; 12];
            raw.copy_from_slice(&data[*offset..*offset + 12]);
            *offset += 12;
            let obj = Py::new(py, RustObjectId::from_raw(py, raw))?;
            Ok(obj.into_bound(py).into_any())
        }

        BSON_BOOLEAN => {
            ensure(data, *offset, 1)?;
            let v = data[*offset] != 0;
            *offset += 1;
            Ok(v.into_pyobject(py)?.to_owned().into_any())
        }

        BSON_DATETIME => {
            ensure(data, *offset, 8)?;
            let millis = read_i64_le(data, *offset);
            *offset += 8;
            let secs = millis / 1000;
            let micros = (millis % 1000) * 1000;
            let ts = secs as f64 + micros as f64 / 1_000_000.0;
            let utc = crate::cached_modules::datetime_tz_utc(py)?;
            let dt_cls = crate::cached_modules::datetime_datetime_cls(py)?;
            Ok(dt_cls.call_method1("fromtimestamp", (ts, utc))?)
        }

        BSON_REGEX => {
            let pattern = read_cstring(data, offset)?;
            let options = read_cstring(data, offset)?;
            let dict = PyDict::new(py);
            dict.set_item("$regex", pattern)?;
            dict.set_item("$options", options)?;
            Ok(dict.into_any())
        }

        BSON_DBPOINTER => {
            // Deprecated: BSON string (namespace) + 12-byte ObjectId
            ensure(data, *offset, 4)?;
            let str_len = read_i32_le(data, *offset) as usize;
            *offset += 4;
            if str_len < 1 {
                return Err(PyValueError::new_err("BSON DBPointer string length < 1"));
            }
            ensure(data, *offset, str_len + 12)?;
            let ns = std::str::from_utf8(&data[*offset..*offset + str_len - 1]).unwrap_or("");
            *offset += str_len + 12;
            Ok(PyString::new(py, &format!("DBPointer({ns})")).into_any())
        }

        BSON_CODE_W_SCOPE => {
            ensure(data, *offset, 4)?;
            let total = read_i32_le(data, *offset) as usize;
            if total < 14 {
                return Err(PyValueError::new_err("BSON CodeWScope too small"));
            }
            ensure(data, *offset, total)?;
            let scope_end = *offset + total;
            *offset += 4;
            ensure(data, *offset, 4)?;
            let str_len = read_i32_le(data, *offset) as usize;
            *offset += 4;
            if str_len < 1 {
                return Err(PyValueError::new_err("BSON CodeWScope string length < 1"));
            }
            ensure(data, *offset, str_len)?;
            let code = std::str::from_utf8(&data[*offset..*offset + str_len - 1]).unwrap_or("");
            *offset = scope_end;
            Ok(PyString::new(py, code).into_any())
        }

        BSON_INT32 => {
            ensure(data, *offset, 4)?;
            let v = read_i32_le(data, *offset);
            *offset += 4;
            Ok(v.into_pyobject(py)?.to_owned().into_any())
        }

        BSON_TIMESTAMP => {
            ensure(data, *offset, 8)?;
            let increment = read_u32_le(data, *offset);
            let time = read_u32_le(data, *offset + 4);
            *offset += 8;
            let dict = PyDict::new(py);
            dict.set_item("t", time)?;
            dict.set_item("i", increment)?;
            Ok(dict.into_any())
        }

        BSON_INT64 => {
            ensure(data, *offset, 8)?;
            let v = read_i64_le(data, *offset);
            *offset += 8;
            Ok(v.into_pyobject(py)?.to_owned().into_any())
        }

        BSON_DECIMAL128 => {
            ensure(data, *offset, 16)?;
            let raw_bytes = &data[*offset..*offset + 16];
            *offset += 16;
            // Return lossless bson.Decimal128 from raw BID bytes
            if let Ok(d128_cls) = crate::cached_modules::bson_decimal128_cls(py) {
                let py_bytes = PyBytes::new(py, raw_bytes);
                // bson.Decimal128 can be constructed from raw BID bytes via
                // Decimal128.from_bid(bytes).  If not available, fall back to f64.
                if let Ok(obj) = d128_cls.call_method1("from_bid", (py_bytes,)) {
                    return Ok(obj);
                }
            }
            let v = decimal128_to_f64(raw_bytes, 0);
            Ok(v.into_pyobject(py)?.to_owned().into_any())
        }

        BSON_MIN_KEY => Ok(PyString::new(py, "$MinKey").into_any()),
        BSON_MAX_KEY => Ok(PyString::new(py, "$MaxKey").into_any()),

        _ => Err(PyValueError::new_err(format!(
            "BSON: unknown type tag 0x{type_tag:02X}"
        ))),
    }
}

/// Convert IEEE 754 decimal128 (BID encoding, little-endian) to f64.
/// Kept as fallback when the `bson` Python package is not available.
fn decimal128_to_f64(data: &[u8], off: usize) -> f64 {
    let low = read_u64_le(data, off);
    let high = read_u64_le(data, off + 8);

    let negative = (high >> 63) & 1 == 1;
    let combo = (high >> 58) & 0x1F;

    if combo >= 0x1E {
        return if combo == 0x1E {
            if negative {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            }
        } else {
            f64::NAN
        };
    }

    let (exponent, coeff_high);
    if (high >> 61) & 0x3 != 0x3 {
        exponent = ((high >> 49) & 0x3FFF) as i32 - 6176;
        coeff_high = high & 0x0001_FFFF_FFFF_FFFF;
    } else {
        exponent = ((high >> 47) & 0x3FFF) as i32 - 6176;
        coeff_high = (high & 0x7FFF_FFFF_FFFF) | (1u64 << 49);
    }

    let coefficient = ((coeff_high as u128) << 64) | (low as u128);
    if coefficient == 0 {
        return if negative { -0.0 } else { 0.0 };
    }

    let value = coefficient as f64 * 10f64.powi(exponent);
    if negative {
        -value
    } else {
        value
    }
}

// ---------------------------------------------------------------------------
// EncodeContext: all Python class references resolved once per encode call
// ---------------------------------------------------------------------------

struct EncodeContext<'py> {
    int64_cls: Option<Bound<'py, PyAny>>,
    binary_cls: Option<Bound<'py, PyAny>>,
    timestamp_cls: Option<Bound<'py, PyAny>>,
    datetime_cls: Bound<'py, PyAny>,
    decimal128_cls: Option<Bound<'py, PyAny>>,
    regex_cls: Option<Bound<'py, PyAny>>,
    uuid_cls: Option<Bound<'py, PyAny>>,
}

impl<'py> EncodeContext<'py> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        Ok(Self {
            int64_cls: crate::cached_modules::bson_int64_cls(py).ok(),
            binary_cls: crate::cached_modules::bson_binary_cls(py).ok(),
            timestamp_cls: crate::cached_modules::bson_timestamp_cls(py).ok(),
            datetime_cls: crate::cached_modules::datetime_datetime_cls(py)?,
            decimal128_cls: crate::cached_modules::bson_decimal128_cls(py).ok(),
            regex_cls: crate::cached_modules::bson_regex_cls(py).ok(),
            uuid_cls: crate::cached_modules::uuid_uuid_cls(py).ok(),
        })
    }
}

// ---------------------------------------------------------------------------
// Encoder: Python dict  →  raw BSON bytes
// ---------------------------------------------------------------------------

/// Encode a Python dict as a raw BSON document byte vector.
pub(crate) fn raw_encode_document(py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<Vec<u8>> {
    let ctx = EncodeContext::new(py)?;
    let mut buf = Vec::with_capacity(256);
    encode_doc_into(py, &mut buf, dict, 0, &ctx)?;
    Ok(buf)
}

fn encode_doc_into(
    py: Python<'_>,
    buf: &mut Vec<u8>,
    dict: &Bound<'_, PyDict>,
    depth: usize,
    ctx: &EncodeContext<'_>,
) -> PyResult<()> {
    if depth > MAX_DEPTH {
        return Err(PyValueError::new_err(format!(
            "document exceeds maximum nesting depth of {MAX_DEPTH}"
        )));
    }

    let start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // length placeholder

    for (k, v) in dict.iter() {
        let key: String = k.extract()?;
        encode_element(py, buf, &key, &v, depth, ctx)?;
    }

    buf.push(0x00); // document terminator
    let len = (buf.len() - start) as i32;
    buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

fn encode_array_into(
    py: Python<'_>,
    buf: &mut Vec<u8>,
    list: &Bound<'_, PyList>,
    depth: usize,
    ctx: &EncodeContext<'_>,
) -> PyResult<()> {
    if depth > MAX_DEPTH {
        return Err(PyValueError::new_err(format!(
            "array exceeds maximum nesting depth of {MAX_DEPTH}"
        )));
    }

    let start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // length placeholder

    for (i, v) in list.iter().enumerate() {
        let idx_str = cached_idx_str(i);
        encode_element(py, buf, &idx_str, &v, depth, ctx)?;
    }

    buf.push(0x00);
    let len = (buf.len() - start) as i32;
    buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

fn encode_element(
    py: Python<'_>,
    buf: &mut Vec<u8>,
    key: &str,
    value: &Bound<'_, PyAny>,
    depth: usize,
    ctx: &EncodeContext<'_>,
) -> PyResult<()> {
    if value.is_none() {
        buf.push(BSON_NULL);
        write_cstring(buf, key);
        return Ok(());
    }

    // bool before int (Python bool subclasses int)
    if let Ok(b) = value.cast::<PyBool>() {
        buf.push(BSON_BOOLEAN);
        write_cstring(buf, key);
        buf.push(if b.is_true() { 0x01 } else { 0x00 });
        return Ok(());
    }

    // bson.Int64 before generic int (Int64 subclasses int, must stay BSON int64)
    if let Some(ref int64_cls) = ctx.int64_cls {
        if value.is_instance(int64_cls)? {
            buf.push(BSON_INT64);
            write_cstring(buf, key);
            let v: i64 = value.extract()?;
            buf.extend_from_slice(&v.to_le_bytes());
            return Ok(());
        }
    }

    if let Ok(i) = value.cast::<PyInt>() {
        let v: i64 = i.extract()?;
        if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
            buf.push(BSON_INT32);
            write_cstring(buf, key);
            buf.extend_from_slice(&(v as i32).to_le_bytes());
        } else {
            buf.push(BSON_INT64);
            write_cstring(buf, key);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        return Ok(());
    }

    if let Ok(f) = value.cast::<PyFloat>() {
        buf.push(BSON_DOUBLE);
        write_cstring(buf, key);
        let v: f64 = f.extract()?;
        buf.extend_from_slice(&v.to_le_bytes());
        return Ok(());
    }

    // smongo.ObjectId (engine type) → 12-byte BSON ObjectId
    if let Ok(oid) = value.extract::<PyRef<'_, RustObjectId>>() {
        buf.push(BSON_OBJECTID);
        write_cstring(buf, key);
        buf.extend_from_slice(&oid.raw_bytes());
        return Ok(());
    }

    if let Ok(s) = value.cast::<PyString>() {
        let s_str = s.to_str()?;

        // MinKey/MaxKey sentinel strings → proper BSON type tags
        if s_str == "$MinKey" {
            buf.push(BSON_MIN_KEY);
            write_cstring(buf, key);
            return Ok(());
        }
        if s_str == "$MaxKey" {
            buf.push(BSON_MAX_KEY);
            write_cstring(buf, key);
            return Ok(());
        }

        // Outbound normalization inline: _id 24-char hex → BSON ObjectId
        if key == "_id" && is_objectid_hex(s_str) {
            if let Ok(bytes) = hex::decode(s_str) {
                buf.push(BSON_OBJECTID);
                write_cstring(buf, key);
                buf.extend_from_slice(&bytes);
                return Ok(());
            }
        }

        buf.push(BSON_STRING);
        write_cstring(buf, key);
        write_bson_string(buf, s_str);
        return Ok(());
    }

    // bson.Decimal128 → BSON Decimal128 (0x13), lossless via .bid raw bytes
    if let Some(ref d128_cls) = ctx.decimal128_cls {
        if value.is_instance(d128_cls)? {
            buf.push(BSON_DECIMAL128);
            write_cstring(buf, key);
            let bid = value.getattr("bid")?;
            let raw: &[u8] = bid.extract()?;
            if raw.len() == 16 {
                buf.extend_from_slice(raw);
            } else {
                buf.extend_from_slice(&[0u8; 16]);
            }
            return Ok(());
        }
    }

    // bson.Regex → BSON Regex (0x0B)
    if let Some(ref regex_cls) = ctx.regex_cls {
        if value.is_instance(regex_cls)? {
            buf.push(BSON_REGEX);
            write_cstring(buf, key);
            let pattern: String = value.getattr("pattern")?.extract()?;
            let flags: String = value
                .getattr("flags")?
                .extract::<String>()
                .unwrap_or_default();
            write_cstring(buf, &pattern);
            write_cstring(buf, &flags);
            return Ok(());
        }
    }

    // uuid.UUID → BSON Binary subtype 4
    if let Some(ref uuid_cls) = ctx.uuid_cls {
        if value.is_instance(uuid_cls)? {
            buf.push(BSON_BINARY);
            write_cstring(buf, key);
            let bytes_attr = value.getattr("bytes")?;
            let uuid_bytes: &[u8] = bytes_attr.extract()?;
            buf.extend_from_slice(&(uuid_bytes.len() as i32).to_le_bytes());
            buf.push(0x04); // UUID subtype
            buf.extend_from_slice(uuid_bytes);
            return Ok(());
        }
    }

    // bson.Binary before plain bytes (Binary subclasses bytes, needs subtype preserved)
    if let Some(ref bin_cls) = ctx.binary_cls {
        if value.is_instance(bin_cls)? {
            buf.push(BSON_BINARY);
            write_cstring(buf, key);
            let bytes: &[u8] = value.extract()?;
            let subtype: u8 = value.getattr("subtype")?.extract()?;
            buf.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
            buf.push(subtype);
            buf.extend_from_slice(bytes);
            return Ok(());
        }
    }

    if let Ok(b) = value.cast::<PyBytes>() {
        buf.push(BSON_BINARY);
        write_cstring(buf, key);
        let bytes = b.as_bytes();
        buf.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
        buf.push(0x00); // generic subtype
        buf.extend_from_slice(bytes);
        return Ok(());
    }

    // re.Pattern (compiled regex) → BSON Regex (0x0B)
    let type_name = value.get_type().qualname()?.to_string();
    if type_name == "Pattern" {
        buf.push(BSON_REGEX);
        write_cstring(buf, key);
        let pattern: String = value.getattr("pattern")?.extract()?;
        let flags_int: u32 = value.getattr("flags")?.extract()?;
        let mut opts = String::new();
        if flags_int & 2 != 0 {
            opts.push('i');
        } // re.IGNORECASE
        if flags_int & 8 != 0 {
            opts.push('m');
        } // re.MULTILINE
        if flags_int & 16 != 0 {
            opts.push('s');
        } // re.DOTALL
        if flags_int & 64 != 0 {
            opts.push('x');
        } // re.VERBOSE
        write_cstring(buf, &pattern);
        write_cstring(buf, &opts);
        return Ok(());
    }

    if let Ok(d) = value.cast::<PyDict>() {
        // Check for regex dict {$regex: ..., $options: ...}
        if let Ok(Some(regex_val)) = d.get_item("$regex") {
            if let Ok(Some(opts_val)) = d.get_item("$options") {
                let pattern: String = regex_val.extract()?;
                let opts: String = opts_val.extract().unwrap_or_default();
                buf.push(BSON_REGEX);
                write_cstring(buf, key);
                write_cstring(buf, &pattern);
                write_cstring(buf, &opts);
                return Ok(());
            }
        }
        buf.push(BSON_DOCUMENT);
        write_cstring(buf, key);
        encode_doc_into(py, buf, d, depth + 1, ctx)?;
        return Ok(());
    }

    if let Ok(l) = value.cast::<PyList>() {
        buf.push(BSON_ARRAY);
        write_cstring(buf, key);
        encode_array_into(py, buf, l, depth + 1, ctx)?;
        return Ok(());
    }

    // Python tuples → BSON arrays (same as lists)
    if let Ok(t) = value.cast::<pyo3::types::PyTuple>() {
        buf.push(BSON_ARRAY);
        write_cstring(buf, key);
        let start = buf.len();
        buf.extend_from_slice(&[0u8; 4]);
        for (i, v) in t.iter().enumerate() {
            let idx_str = cached_idx_str(i);
            encode_element(py, buf, &idx_str, &v, depth + 1, ctx)?;
        }
        buf.push(0x00);
        let len = (buf.len() - start) as i32;
        buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
        return Ok(());
    }

    // bson.Timestamp → BSON Timestamp (0x11)
    if let Some(ref ts_cls) = ctx.timestamp_cls {
        if value.is_instance(ts_cls)? {
            buf.push(BSON_TIMESTAMP);
            write_cstring(buf, key);
            let time_val: u32 = value.getattr("time")?.extract()?;
            let inc_val: u32 = value.getattr("inc")?.extract()?;
            buf.extend_from_slice(&inc_val.to_le_bytes());
            buf.extend_from_slice(&time_val.to_le_bytes());
            return Ok(());
        }
    }

    // datetime.datetime → BSON DateTime
    if value.is_instance(&ctx.datetime_cls)? {
        buf.push(BSON_DATETIME);
        write_cstring(buf, key);
        let ts: f64 = value.call_method0("timestamp")?.extract()?;
        let millis = (ts * 1000.0) as i64;
        buf.extend_from_slice(&millis.to_le_bytes());
        return Ok(());
    }

    // PyMongo bson.ObjectId (not our engine type) — check by qualname
    if type_name == "ObjectId" {
        let hex_str: String = value.str()?.extract()?;
        if hex_str.len() == 24 {
            if let Ok(bytes) = hex::decode(&hex_str) {
                buf.push(BSON_OBJECTID);
                write_cstring(buf, key);
                buf.extend_from_slice(&bytes);
                return Ok(());
            }
        }
    }

    // Fallback: str(value) as BSON string, with a warning so surprises are visible
    let warnings = py.import("warnings")?;
    let msg = format!(
        "smongo BSON encoder: unknown type '{}' for key '{}', encoding as string",
        type_name, key
    );
    warnings.call_method1("warn", (msg,))?;

    let s: String = value.str()?.extract()?;
    buf.push(BSON_STRING);
    write_cstring(buf, key);
    write_bson_string(buf, &s);
    Ok(())
}

#[inline]
fn write_cstring(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0x00);
}

#[inline]
fn write_bson_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = (bytes.len() + 1) as i32; // +1 for null terminator
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    buf.push(0x00);
}

fn is_objectid_hex(s: &str) -> bool {
    s.len() == 24 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
    fn test_id_hex_string_encoded_as_objectid() {
        with_py(|py| {
            let hex = "507f1f77bcf86cd799439011";
            let dict = PyDict::new(py);
            dict.set_item("_id", hex).unwrap();

            let encoded = raw_encode_document(py, &dict).unwrap();
            assert_eq!(encoded[4], BSON_OBJECTID);

            let decoded = raw_decode_slice(py, &encoded).unwrap();
            let result = decoded.get_item("_id").unwrap().unwrap();
            assert!(result.is_instance_of::<RustObjectId>());
            let result_oid: PyRef<'_, RustObjectId> = result.extract().unwrap();
            assert_eq!(result_oid.hex(), hex);
        });
    }

    #[test]
    fn test_empty_document() {
        with_py(|py| {
            let dict = PyDict::new(py);
            let encoded = raw_encode_document(py, &dict).unwrap();
            assert_eq!(encoded.len(), 5); // 4-byte length + terminator
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
    fn test_decimal128_conversion() {
        let low: u64 = 10;
        let high: u64 = (6175u64) << 49;
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&low.to_le_bytes());
        bytes[8..16].copy_from_slice(&high.to_le_bytes());

        let result = decimal128_to_f64(&bytes, 0);
        assert!((result - 1.0).abs() < 1e-10, "expected 1.0, got {result}");
    }

    #[test]
    fn test_decimal128_zero() {
        let bytes = [0u8; 16];
        let result = decimal128_to_f64(&bytes, 0);
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_decimal128_negative() {
        let low: u64 = 10;
        let high: u64 = (1u64 << 63) | ((6175u64) << 49);
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&low.to_le_bytes());
        bytes[8..16].copy_from_slice(&high.to_le_bytes());

        let result = decimal128_to_f64(&bytes, 0);
        assert!((result + 1.0).abs() < 1e-10, "expected -1.0, got {result}");
    }

    #[test]
    fn test_decimal128_infinity() {
        let high: u64 = 0x1Eu64 << 58;
        let mut bytes = [0u8; 16];
        bytes[8..16].copy_from_slice(&high.to_le_bytes());
        assert!(decimal128_to_f64(&bytes, 0).is_infinite());
        assert!(decimal128_to_f64(&bytes, 0) > 0.0);

        let high_neg: u64 = (1u64 << 63) | (0x1Eu64 << 58);
        bytes[8..16].copy_from_slice(&high_neg.to_le_bytes());
        assert!(decimal128_to_f64(&bytes, 0).is_infinite());
        assert!(decimal128_to_f64(&bytes, 0) < 0.0);
    }

    #[test]
    fn test_decimal128_nan() {
        let high: u64 = 0x1Fu64 << 58;
        let mut bytes = [0u8; 16];
        bytes[8..16].copy_from_slice(&high.to_le_bytes());
        assert!(decimal128_to_f64(&bytes, 0).is_nan());
    }

    /// Cross-validate: encode with raw encoder, decode with bson crate (and vice versa).
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
    fn test_binary_subtype_preserved_on_decode() {
        with_py(|py| {
            // Encode a BSON document with Binary subtype 5 (MD5) using the bson crate
            let mut doc = bson::Document::new();
            doc.insert(
                "hash",
                bson::Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Md5,
                    bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
                }),
            );
            let bson_bytes = bson::to_vec(&doc).unwrap();
            let decoded = raw_decode_slice(py, &bson_bytes).unwrap();

            let val = decoded.get_item("hash").unwrap().unwrap();
            // Should NOT be plain bytes -- should be bson.Binary with subtype
            let subtype: i32 = val.getattr("subtype").unwrap().extract().unwrap();
            assert_eq!(subtype, 5); // MD5
        });
    }

    #[test]
    fn test_binary_subtype_zero_stays_pybytes() {
        with_py(|py| {
            let mut doc = bson::Document::new();
            doc.insert(
                "data",
                bson::Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: vec![1, 2, 3],
                }),
            );
            let bson_bytes = bson::to_vec(&doc).unwrap();
            let decoded = raw_decode_slice(py, &bson_bytes).unwrap();

            let val = decoded.get_item("data").unwrap().unwrap();
            assert!(val.cast::<PyBytes>().is_ok());
        });
    }

    #[test]
    fn test_minkey_maxkey_roundtrip() {
        with_py(|py| {
            let dict = PyDict::new(py);
            dict.set_item("lo", "$MinKey").unwrap();
            dict.set_item("hi", "$MaxKey").unwrap();

            let encoded = raw_encode_document(py, &dict).unwrap();
            let bson_doc: bson::Document = bson::from_slice(&encoded).unwrap();
            assert!(matches!(bson_doc.get("lo").unwrap(), bson::Bson::MinKey));
            assert!(matches!(bson_doc.get("hi").unwrap(), bson::Bson::MaxKey));

            let decoded = raw_decode_slice(py, &encoded).unwrap();
            assert_eq!(
                decoded
                    .get_item("lo")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "$MinKey"
            );
            assert_eq!(
                decoded
                    .get_item("hi")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "$MaxKey"
            );
        });
    }

    #[test]
    fn test_regex_dict_encoded_as_bson_regex() {
        with_py(|py| {
            let regex_dict = PyDict::new(py);
            regex_dict.set_item("$regex", "^hello").unwrap();
            regex_dict.set_item("$options", "i").unwrap();

            let dict = PyDict::new(py);
            dict.set_item("pat", regex_dict).unwrap();

            let encoded = raw_encode_document(py, &dict).unwrap();
            let bson_doc: bson::Document = bson::from_slice(&encoded).unwrap();
            match bson_doc.get("pat").unwrap() {
                bson::Bson::RegularExpression(r) => {
                    assert_eq!(r.pattern, "^hello");
                    assert_eq!(r.options, "i");
                }
                other => panic!("expected Regex, got {other:?}"),
            }
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
