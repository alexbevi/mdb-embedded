//! MongoDB Wire Protocol -- OP_MSG (2013), OP_COMPRESSED (2012),
//! and legacy OP_QUERY / OP_REPLY.
//!
//! Pure binary framing: header parsing, section decode/encode,
//! CRC-32C checksum validation, and transparent compression.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::LazyLock;

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::Py;

use crate::raw_bson::{raw_decode_document, raw_encode_document};

pub(crate) static COMPRESSOR_IDS: LazyLock<HashMap<&'static str, i32>> = LazyLock::new(|| {
    HashMap::from([("noop", 0), ("snappy", 1), ("zlib", 2), ("zstd", 3)])
});

pub const OP_REPLY: i32 = 1;
const _OP_QUERY: i32 = 2004;
pub const OP_COMPRESSED: i32 = 2012;
pub const OP_MSG: i32 = 2013;
pub const HEADER_SIZE: usize = 16;
const MAX_MSG_SIZE: usize = 48 * 1024 * 1024;

const COMPRESSOR_NOOP: u8 = 0;
const COMPRESSOR_SNAPPY: u8 = 1;
const COMPRESSOR_ZLIB: u8 = 2;
const COMPRESSOR_ZSTD: u8 = 3;

pyo3::create_exception!(_smongo_core, ProtocolError, pyo3::exceptions::PyException);
pyo3::create_exception!(_smongo_core, ChecksumMismatch, ProtocolError);

// ---------------------------------------------------------------------------
// MsgHeader
// ---------------------------------------------------------------------------

/// MongoDB wire message header: total length, request/response ids, and opcode.
#[pyclass(module = "smongo._smongo_core", skip_from_py_object)]
#[derive(Clone)]
pub struct MsgHeader {
    #[pyo3(get)]
    pub length: i32,
    #[pyo3(get)]
    pub request_id: i32,
    #[pyo3(get)]
    pub response_to: i32,
    #[pyo3(get)]
    pub op_code: i32,
}

// ---------------------------------------------------------------------------
// Little-endian read helpers
// ---------------------------------------------------------------------------

fn read_i32(data: &[u8], off: usize) -> i32 {
    i32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
}

fn read_u32(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
}

fn parse_header(data: &[u8]) -> PyResult<MsgHeader> {
    if data.len() < HEADER_SIZE {
        return Err(ProtocolError::new_err("message too short for header"));
    }
    Ok(MsgHeader {
        length: read_i32(data, 0),
        request_id: read_i32(data, 4),
        response_to: read_i32(data, 8),
        op_code: read_i32(data, 12),
    })
}

// ---------------------------------------------------------------------------
// CRC-32C
// ---------------------------------------------------------------------------

fn validate_checksum(data: &[u8], msg_length: usize) -> PyResult<()> {
    if msg_length < 4 || msg_length > data.len() {
        return Err(ProtocolError::new_err("invalid message length for checksum"));
    }
    let msg_body = &data[..msg_length - 4];
    let expected = read_u32(data, msg_length - 4);
    let actual = crc32c::crc32c(msg_body);
    if actual != expected {
        return Err(ChecksumMismatch::new_err(format!(
            "CRC-32C mismatch: expected {expected:#010x}, got {actual:#010x}"
        )));
    }
    Ok(())
}

fn compute_checksum(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

// ---------------------------------------------------------------------------
// Compression helpers
// ---------------------------------------------------------------------------

fn decompress(cid: u8, data: &[u8], expected: usize) -> PyResult<Vec<u8>> {
    if expected > MAX_MSG_SIZE {
        return Err(ProtocolError::new_err(format!(
            "declared uncompressed size {expected} exceeds limit {MAX_MSG_SIZE}"
        )));
    }
    match cid {
        COMPRESSOR_NOOP => Ok(data.to_vec()),
        COMPRESSOR_SNAPPY => {
            let out = snap::raw::Decoder::new()
                .decompress_vec(data)
                .map_err(|e| ProtocolError::new_err(format!("snappy decompression error: {e}")))?;
            if out.len() != expected {
                return Err(ProtocolError::new_err(format!(
                    "snappy decompressed size {} != declared {expected}",
                    out.len()
                )));
            }
            Ok(out)
        }
        COMPRESSOR_ZLIB => {
            let mut decoder = ZlibDecoder::new(data);
            let mut out = Vec::with_capacity(expected);
            decoder
                .read_to_end(&mut out)
                .map_err(|e| ProtocolError::new_err(format!("zlib decompression error: {e}")))?;
            if out.len() != expected {
                return Err(ProtocolError::new_err(format!(
                    "zlib decompressed size {} != declared {expected}",
                    out.len()
                )));
            }
            Ok(out)
        }
        COMPRESSOR_ZSTD => {
            let out = zstd::decode_all(std::io::Cursor::new(data))
                .map_err(|e| ProtocolError::new_err(format!("zstd decompression error: {e}")))?;
            if out.len() != expected {
                return Err(ProtocolError::new_err(format!(
                    "zstd decompressed size {} != declared {expected}",
                    out.len()
                )));
            }
            Ok(out)
        }
        _ => Err(ProtocolError::new_err(format!(
            "unknown compressor id: {cid}"
        ))),
    }
}

fn compress(cid: u8, data: &[u8]) -> PyResult<Vec<u8>> {
    match cid {
        COMPRESSOR_NOOP => Ok(data.to_vec()),
        COMPRESSOR_SNAPPY => snap::raw::Encoder::new()
            .compress_vec(data)
            .map_err(|e| ProtocolError::new_err(format!("snappy compression error: {e}"))),
        COMPRESSOR_ZLIB => {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(data)
                .map_err(|e| ProtocolError::new_err(format!("zlib compression error: {e}")))?;
            encoder
                .finish()
                .map_err(|e| ProtocolError::new_err(format!("zlib compression error: {e}")))
        }
        COMPRESSOR_ZSTD => zstd::encode_all(std::io::Cursor::new(data), 0)
            .map_err(|e| ProtocolError::new_err(format!("zstd compression error: {e}"))),
        _ => Err(ProtocolError::new_err(format!(
            "unknown compressor id: {cid}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Public PyO3 functions
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn decode_header(data: &[u8]) -> PyResult<MsgHeader> {
    parse_header(data)
}

#[pyfunction]
#[pyo3(signature = (data,))]
pub fn decode_msg(py: Python<'_>, data: &[u8]) -> PyResult<(MsgHeader, u32, Py<PyAny>, Py<PyAny>)> {
    let header = parse_header(data)?;
    if data.len() < HEADER_SIZE + 4 {
        return Err(ProtocolError::new_err("message too short for OP_MSG flags"));
    }

    let flags = read_u32(data, HEADER_SIZE);
    let has_checksum = (flags & 0x01) != 0;
    let end = header.length as usize - if has_checksum { 4 } else { 0 };
    let mut offset = HEADER_SIZE + 4;

    if has_checksum {
        validate_checksum(data, header.length as usize)?;
    }

    let mut body_doc: Py<PyAny> = py.None().into_any();
    let doc_sequences = PyDict::new(py);

    while offset < end {
        if offset >= data.len() {
            return Err(ProtocolError::new_err("truncated OP_MSG sections"));
        }
        let kind = data[offset];
        offset += 1;

        if kind == 0 {
            let dict = raw_decode_document(py, data, &mut offset, 0)
                .map_err(|e| ProtocolError::new_err(format!("BSON decode error: {e}")))?;
            body_doc = dict.into_any().unbind();
        } else if kind == 1 {
            if offset + 4 > data.len() {
                return Err(ProtocolError::new_err("truncated Kind 1 section header"));
            }
            let section_size = read_i32(data, offset) as usize;
            let section_end = offset + section_size;
            offset += 4;

            let null_pos = data[offset..section_end]
                .iter()
                .position(|&b| b == 0)
                .ok_or_else(|| {
                    ProtocolError::new_err("missing null terminator in Kind 1 identifier")
                })?
                + offset;
            let identifier = std::str::from_utf8(&data[offset..null_pos])
                .map_err(|e| ProtocolError::new_err(format!("invalid UTF-8: {e}")))?;
            offset = null_pos + 1;

            let docs = PyList::empty(py);
            while offset < section_end {
                let dict = raw_decode_document(py, data, &mut offset, 0)
                    .map_err(|e| ProtocolError::new_err(format!("BSON decode error: {e}")))?;
                docs.append(dict)?;
            }
            doc_sequences.set_item(identifier, docs)?;
        } else {
            return Err(ProtocolError::new_err(format!(
                "unknown OP_MSG section kind: {kind}"
            )));
        }
    }

    Ok((
        header,
        flags,
        body_doc,
        doc_sequences.into_any().unbind(),
    ))
}

#[pyfunction]
#[pyo3(signature = (request_id, response_to, doc, include_checksum=false))]
pub fn encode_msg(
    py: Python<'_>,
    request_id: i32,
    response_to: i32,
    doc: &Bound<'_, PyDict>,
    include_checksum: bool,
) -> PyResult<Py<PyBytes>> {
    let body_bson = raw_encode_document(py, doc)
        .map_err(|e| ProtocolError::new_err(format!("BSON encode error: {e}")))?;

    let flag_bits: u32 = if include_checksum { 0x01 } else { 0x00 };

    // payload = flags(4) + kind_0_byte(1) + body_bson
    let payload_len = 4 + 1 + body_bson.len();

    if include_checksum {
        let length = (HEADER_SIZE + payload_len + 4) as i32;
        let mut msg = Vec::with_capacity(length as usize);
        msg.extend_from_slice(&length.to_le_bytes());
        msg.extend_from_slice(&request_id.to_le_bytes());
        msg.extend_from_slice(&response_to.to_le_bytes());
        msg.extend_from_slice(&OP_MSG.to_le_bytes());
        msg.extend_from_slice(&flag_bits.to_le_bytes());
        msg.push(0u8); // Kind 0
        msg.extend_from_slice(&body_bson);
        let crc = compute_checksum(&msg);
        msg.extend_from_slice(&crc.to_le_bytes());
        return Ok(PyBytes::new(py, &msg).unbind());
    }

    let length = (HEADER_SIZE + payload_len) as i32;
    let mut msg = Vec::with_capacity(length as usize);
    msg.extend_from_slice(&length.to_le_bytes());
    msg.extend_from_slice(&request_id.to_le_bytes());
    msg.extend_from_slice(&response_to.to_le_bytes());
    msg.extend_from_slice(&OP_MSG.to_le_bytes());
    msg.extend_from_slice(&flag_bits.to_le_bytes());
    msg.push(0u8); // Kind 0
    msg.extend_from_slice(&body_bson);

    Ok(PyBytes::new(py, &msg).unbind())
}

#[pyfunction]
pub fn decode_query(
    py: Python<'_>,
    data: &[u8],
) -> PyResult<(MsgHeader, i32, String, i32, i32, Py<PyAny>)> {
    let header = parse_header(data)?;
    let mut offset = HEADER_SIZE;

    if offset + 4 > data.len() {
        return Err(ProtocolError::new_err("truncated OP_QUERY flags"));
    }
    let flags = read_i32(data, offset);
    offset += 4;

    let null_pos = data[offset..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| ProtocolError::new_err("missing null in collection name"))?
        + offset;
    let full_coll_name = std::str::from_utf8(&data[offset..null_pos])
        .map_err(|e| ProtocolError::new_err(format!("invalid UTF-8: {e}")))?
        .to_string();
    offset = null_pos + 1;

    if offset + 8 > data.len() {
        return Err(ProtocolError::new_err("truncated OP_QUERY skip/limit"));
    }
    let skip = read_i32(data, offset);
    offset += 4;
    let limit = read_i32(data, offset);
    offset += 4;

    let query_dict = raw_decode_document(py, data, &mut offset, 0)
        .map_err(|e| ProtocolError::new_err(format!("BSON decode error: {e}")))?;
    let query_doc = query_dict.into_any().unbind();

    Ok((header, flags, full_coll_name, skip, limit, query_doc))
}

#[pyfunction]
#[pyo3(signature = (request_id, response_to, docs, cursor_id=0, starting_from=0, response_flags=0))]
pub fn encode_reply(
    py: Python<'_>,
    request_id: i32,
    response_to: i32,
    docs: &Bound<'_, PyList>,
    cursor_id: i64,
    starting_from: i32,
    response_flags: i32,
) -> PyResult<Py<PyBytes>> {
    let mut docs_bson = Vec::new();
    for item in docs.iter() {
        let dict = item.cast::<PyDict>().map_err(|_| {
            ProtocolError::new_err("encode_reply: each doc must be a dict")
        })?;
        let raw = raw_encode_document(py, dict)
            .map_err(|e| ProtocolError::new_err(format!("BSON encode error: {e}")))?;
        docs_bson.extend_from_slice(&raw);
    }

    let num_returned = docs.len() as i32;
    // payload: responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + docs
    let payload_len = 4 + 8 + 4 + 4 + docs_bson.len();
    let length = (HEADER_SIZE + payload_len) as i32;

    let mut msg = Vec::with_capacity(length as usize);
    msg.extend_from_slice(&length.to_le_bytes());
    msg.extend_from_slice(&request_id.to_le_bytes());
    msg.extend_from_slice(&response_to.to_le_bytes());
    msg.extend_from_slice(&OP_REPLY.to_le_bytes());
    msg.extend_from_slice(&response_flags.to_le_bytes());
    msg.extend_from_slice(&cursor_id.to_le_bytes());
    msg.extend_from_slice(&starting_from.to_le_bytes());
    msg.extend_from_slice(&num_returned.to_le_bytes());
    msg.extend_from_slice(&docs_bson);

    Ok(PyBytes::new(py, &msg).unbind())
}

// -- OP_COMPRESSED --------------------------------------------------------

#[pyfunction]
pub fn decode_compressed(py: Python<'_>, data: &[u8]) -> PyResult<Py<PyBytes>> {
    let header = parse_header(data)?;
    let mut offset = HEADER_SIZE;

    if offset + 9 > data.len() {
        return Err(ProtocolError::new_err("truncated OP_COMPRESSED header"));
    }
    let original_opcode = read_i32(data, offset);
    offset += 4;
    let uncompressed_size = read_i32(data, offset) as usize;
    offset += 4;
    let compressor_id = data[offset];
    offset += 1;
    let compressed_data = &data[offset..header.length as usize];

    let decompressed = decompress(compressor_id, compressed_data, uncompressed_size)?;

    let inner_length = (HEADER_SIZE + decompressed.len()) as i32;
    let mut result = Vec::with_capacity(inner_length as usize);
    result.extend_from_slice(&inner_length.to_le_bytes());
    result.extend_from_slice(&header.request_id.to_le_bytes());
    result.extend_from_slice(&header.response_to.to_le_bytes());
    result.extend_from_slice(&original_opcode.to_le_bytes());
    result.extend_from_slice(&decompressed);

    Ok(PyBytes::new(py, &result).unbind())
}

#[pyfunction]
pub fn encode_compressed(py: Python<'_>, data: &[u8], compressor_id: i32) -> PyResult<Py<PyBytes>> {
    let header = parse_header(data)?;
    let original_opcode = header.op_code;
    let inner_payload = &data[HEADER_SIZE..];
    let uncompressed_size = inner_payload.len() as i32;

    let compressed = compress(compressor_id as u8, inner_payload)?;

    // payload: originalOpcode(4) + uncompressedSize(4) + compressorId(1) + compressed
    let payload_len = 4 + 4 + 1 + compressed.len();
    let length = (HEADER_SIZE + payload_len) as i32;

    let mut msg = Vec::with_capacity(length as usize);
    msg.extend_from_slice(&length.to_le_bytes());
    msg.extend_from_slice(&header.request_id.to_le_bytes());
    msg.extend_from_slice(&header.response_to.to_le_bytes());
    msg.extend_from_slice(&OP_COMPRESSED.to_le_bytes());
    msg.extend_from_slice(&original_opcode.to_le_bytes());
    msg.extend_from_slice(&uncompressed_size.to_le_bytes());
    msg.push(compressor_id as u8);
    msg.extend_from_slice(&compressed);

    Ok(PyBytes::new(py, &msg).unbind())
}

#[pyfunction]
pub fn available_compressors() -> Vec<String> {
    vec![
        "snappy".to_string(),
        "zlib".to_string(),
        "zstd".to_string(),
    ]
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
    fn test_header_roundtrip() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100i32.to_le_bytes());
        buf.extend_from_slice(&1i32.to_le_bytes());
        buf.extend_from_slice(&0i32.to_le_bytes());
        buf.extend_from_slice(&OP_MSG.to_le_bytes());

        let hdr = parse_header(&buf).unwrap();
        assert_eq!(hdr.length, 100);
        assert_eq!(hdr.request_id, 1);
        assert_eq!(hdr.response_to, 0);
        assert_eq!(hdr.op_code, OP_MSG);
    }

    #[test]
    fn test_encode_decode_msg_roundtrip() {
        with_py(|py| {
            let doc = PyDict::new(py);
            doc.set_item("hello", 1).unwrap();
            doc.set_item("ok", 1.0).unwrap();

            let encoded = encode_msg(py, 42, 0, &doc, false).unwrap();
            let bytes = encoded.as_bytes(py);

            let (hdr, flags, body, seqs) = decode_msg(py, bytes).unwrap();
            assert_eq!(hdr.op_code, OP_MSG);
            assert_eq!(hdr.request_id, 42);
            assert_eq!(flags, 0);

            let body = body.bind(py).cast::<PyDict>().unwrap().clone();
            let val: i32 = body
                .get_item("hello")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(val, 1);

            let seqs = seqs.bind(py).cast::<PyDict>().unwrap().clone();
            assert_eq!(seqs.len(), 0);
        });
    }

    #[test]
    fn test_crc32c_roundtrip() {
        with_py(|py| {
            let doc = PyDict::new(py);
            doc.set_item("ping", 1).unwrap();

            let encoded = encode_msg(py, 1, 0, &doc, true).unwrap();
            let bytes = encoded.as_bytes(py);

            let flags_raw = read_u32(bytes, HEADER_SIZE);
            assert_eq!(flags_raw & 0x01, 0x01);

            let (hdr, flags, body, _seqs) = decode_msg(py, bytes).unwrap();
            assert_eq!(hdr.op_code, OP_MSG);
            assert_eq!(flags & 0x01, 0x01);

            let body = body.bind(py).cast::<PyDict>().unwrap().clone();
            let val: i32 = body
                .get_item("ping")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(val, 1);
        });
    }

    #[test]
    fn test_zlib_compressed_roundtrip() {
        with_py(|py| {
            let doc = PyDict::new(py);
            doc.set_item("ping", 1).unwrap();

            let original = encode_msg(py, 42, 0, &doc, false).unwrap();
            let original_bytes = original.as_bytes(py);

            let compressed = encode_compressed(py, original_bytes, COMPRESSOR_ZLIB as i32).unwrap();
            let compressed_bytes = compressed.as_bytes(py);
            let hdr = parse_header(compressed_bytes).unwrap();
            assert_eq!(hdr.op_code, OP_COMPRESSED);

            let decompressed = decode_compressed(py, compressed_bytes).unwrap();
            let decompressed_bytes = decompressed.as_bytes(py);
            let inner_hdr = parse_header(decompressed_bytes).unwrap();
            assert_eq!(inner_hdr.op_code, OP_MSG);

            let (_h, _f, body, _s) = decode_msg(py, decompressed_bytes).unwrap();
            let body = body.bind(py).cast::<PyDict>().unwrap().clone();
            let val: i32 = body
                .get_item("ping")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(val, 1);
        });
    }
}
