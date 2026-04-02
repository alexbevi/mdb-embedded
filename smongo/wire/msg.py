"""
MongoDB Wire Protocol -- OP_MSG (opcode 2013), OP_COMPRESSED (opcode 2012),
and legacy OP_QUERY/OP_REPLY.

Handles the binary framing: 16-byte message header, flag bits, Section Kind 0
(body document) and Kind 1 (document sequences), CRC-32C checksum validation,
and transparent compression/decompression.
"""

import struct
import zlib
from typing import Any

from bson import decode as _bson_decode
from bson import encode as _bson_encode

from ._types import DocSequences

OP_REPLY = 1
OP_QUERY = 2004
OP_COMPRESSED = 2012
OP_MSG = 2013
HEADER_SIZE = 16

MAX_MSG_SIZE = 48 * 1024 * 1024

COMPRESSOR_NOOP = 0
COMPRESSOR_SNAPPY = 1
COMPRESSOR_ZLIB = 2
COMPRESSOR_ZSTD = 3

_COMPRESSOR_NAMES: dict[int, str] = {
    COMPRESSOR_NOOP: "noop",
    COMPRESSOR_SNAPPY: "snappy",
    COMPRESSOR_ZLIB: "zlib",
    COMPRESSOR_ZSTD: "zstd",
}
_COMPRESSOR_IDS: dict[str, int] = {v: k for k, v in _COMPRESSOR_NAMES.items()}

try:
    import crc32c as _crc32c_mod

    def _crc32c(data: bytes) -> int:
        return _crc32c_mod.crc32c(data)  # type: ignore[no-any-return]

except ImportError:
    _crc32c = None  # type: ignore[assignment]

try:
    import snappy as _snappy
except ImportError:
    _snappy = None

try:
    import zstandard as _zstd
except ImportError:
    _zstd = None


class ProtocolError(Exception):
    """Raised when the wire message violates the protocol framing rules."""


class ChecksumMismatch(ProtocolError):
    """Raised when the CRC-32C checksum in an OP_MSG does not match."""


class MsgHeader:
    __slots__ = ("length", "op_code", "request_id", "response_to")

    def __init__(self, length: int, request_id: int, response_to: int, op_code: int) -> None:
        self.length = length
        self.request_id = request_id
        self.response_to = response_to
        self.op_code = op_code


def decode_header(data: bytes | bytearray) -> MsgHeader:
    """Parse the 16-byte standard message header."""
    length, req_id, resp_to, op_code = struct.unpack_from("<iiii", data, 0)
    return MsgHeader(length, req_id, resp_to, op_code)


def decode_msg(data: bytes | bytearray) -> tuple[MsgHeader, int, Any, DocSequences]:
    """Decode a full OP_MSG message (header + body).

    Returns (header, flags, body_doc, doc_sequences) where doc_sequences is
    a dict mapping section identifier strings to lists of BSON documents.

    Raises ProtocolError on unknown section kinds and ChecksumMismatch when
    the trailing CRC-32C does not match.
    """
    header = decode_header(data)
    flags = struct.unpack_from("<I", data, HEADER_SIZE)[0]

    has_checksum = bool(flags & 0x01)
    end = header.length - (4 if has_checksum else 0)
    offset = HEADER_SIZE + 4

    if has_checksum:
        _validate_checksum(data, header.length)

    body_doc: Any = None
    doc_sequences: DocSequences = {}

    while offset < end:
        kind = data[offset]
        offset += 1

        if kind == 0:
            doc_size = struct.unpack_from("<i", data, offset)[0]
            body_doc = _bson_decode(bytes(data[offset : offset + doc_size]))
            offset += doc_size
        elif kind == 1:
            section_size = struct.unpack_from("<i", data, offset)[0]
            section_end = offset + section_size
            offset += 4
            null_pos = data.index(b"\x00", offset)
            identifier = data[offset:null_pos].decode("utf-8")
            offset = null_pos + 1
            docs: list[Any] = []
            while offset < section_end:
                doc_size = struct.unpack_from("<i", data, offset)[0]
                docs.append(_bson_decode(bytes(data[offset : offset + doc_size])))
                offset += doc_size
            doc_sequences[identifier] = docs
        else:
            raise ProtocolError(f"unknown OP_MSG section kind: {kind}")

    return header, flags, body_doc, doc_sequences


def encode_msg(
    request_id: int, response_to: int, doc: dict[str, Any], include_checksum: bool = False
) -> bytes:
    """Encode a response document as an OP_MSG with a single Kind 0 section."""
    body_bson = _bson_encode(doc)
    sections = b"\x00" + body_bson
    flag_bits = 0x01 if include_checksum else 0x00
    flags = struct.pack("<I", flag_bits)
    payload = flags + sections

    if include_checksum:
        length = HEADER_SIZE + len(payload) + 4
        header = struct.pack("<iiii", length, request_id, response_to, OP_MSG)
        msg_without_crc = header + payload
        crc = _compute_checksum(msg_without_crc)
        return msg_without_crc + struct.pack("<I", crc)

    length = HEADER_SIZE + len(payload)
    header = struct.pack("<iiii", length, request_id, response_to, OP_MSG)
    return header + payload


def decode_query(data: bytes | bytearray) -> tuple[MsgHeader, int, str, int, int, Any]:
    """Decode a legacy OP_QUERY message.

    Returns (header, flags, collection_name, skip, limit, query_doc).
    """
    header = decode_header(data)
    offset = HEADER_SIZE
    flags = struct.unpack_from("<i", data, offset)[0]
    offset += 4
    null_pos = data.index(b"\x00", offset)
    full_coll_name = data[offset:null_pos].decode("utf-8")
    offset = null_pos + 1
    skip, limit = struct.unpack_from("<ii", data, offset)
    offset += 8
    doc_size = struct.unpack_from("<i", data, offset)[0]
    query_doc = _bson_decode(bytes(data[offset : offset + doc_size]))
    return header, flags, full_coll_name, skip, limit, query_doc


def encode_reply(
    request_id: int,
    response_to: int,
    docs: list[dict[str, Any]],
    cursor_id: int = 0,
    starting_from: int = 0,
    response_flags: int = 0,
) -> bytes:
    """Encode a legacy OP_REPLY message."""
    docs_bson = b"".join(_bson_encode(d) for d in docs)
    payload = struct.pack("<iqii", response_flags, cursor_id, starting_from, len(docs))
    payload += docs_bson
    length = HEADER_SIZE + len(payload)
    header = struct.pack("<iiii", length, request_id, response_to, OP_REPLY)
    return header + payload


# -- OP_COMPRESSED (opcode 2012) -------------------------------------------


def decode_compressed(data: bytes | bytearray) -> bytes:
    """Decode an OP_COMPRESSED message and return the decompressed inner message.

    Returns the full decompressed message bytes (header rewritten to the
    original opcode) ready to be fed to decode_msg / decode_query.
    """
    header = decode_header(data)
    offset = HEADER_SIZE
    original_opcode = struct.unpack_from("<i", data, offset)[0]
    offset += 4
    uncompressed_size = struct.unpack_from("<i", data, offset)[0]
    offset += 4
    compressor_id = data[offset]
    offset += 1
    compressed_data = data[offset : header.length]

    decompressed = _decompress(compressor_id, compressed_data, uncompressed_size)

    inner_length = HEADER_SIZE + len(decompressed)
    inner_header = struct.pack(
        "<iiii", inner_length, header.request_id, header.response_to, original_opcode
    )
    return inner_header + decompressed


def encode_compressed(data: bytes, compressor_id: int) -> bytes:
    """Wrap an already-encoded OP_MSG or OP_REPLY in OP_COMPRESSED framing."""
    header = decode_header(data)
    original_opcode = header.op_code
    inner_payload = data[HEADER_SIZE:]
    uncompressed_size = len(inner_payload)

    compressed = _compress(compressor_id, inner_payload)

    payload = struct.pack("<ii", original_opcode, uncompressed_size)
    payload += struct.pack("B", compressor_id)
    payload += compressed

    length = HEADER_SIZE + len(payload)
    out_header = struct.pack("<iiii", length, header.request_id, header.response_to, OP_COMPRESSED)
    return out_header + payload


# -- compression helpers ---------------------------------------------------


def _decompress(compressor_id: int, data: bytes | bytearray, expected_size: int) -> bytes:
    if expected_size < 0 or expected_size > MAX_MSG_SIZE:
        raise ProtocolError(
            f"declared uncompressed size {expected_size} exceeds limit {MAX_MSG_SIZE}"
        )
    if compressor_id == COMPRESSOR_NOOP:
        return bytes(data)
    if compressor_id == COMPRESSOR_SNAPPY:
        if _snappy is None:
            raise ProtocolError("snappy compression not available (install python-snappy)")
        result = _snappy.decompress(data)
        if len(result) != expected_size:
            raise ProtocolError(
                f"snappy decompressed size {len(result)} != declared {expected_size}"
            )
        return result  # type: ignore[no-any-return]
    if compressor_id == COMPRESSOR_ZLIB:
        result = zlib.decompress(data, zlib.MAX_WBITS, expected_size)
        if len(result) != expected_size:
            raise ProtocolError(f"zlib decompressed size {len(result)} != declared {expected_size}")
        return bytes(result)
    if compressor_id == COMPRESSOR_ZSTD:
        if _zstd is None:
            raise ProtocolError("zstd compression not available (install zstandard)")
        reader = _zstd.ZstdDecompressor()
        return bytes(reader.decompress(data, max_output_size=expected_size))
    raise ProtocolError(f"unknown compressor id: {compressor_id}")


def _compress(compressor_id: int, data: bytes) -> bytes:
    if compressor_id == COMPRESSOR_NOOP:
        return data
    if compressor_id == COMPRESSOR_SNAPPY:
        if _snappy is None:
            raise ProtocolError("snappy compression not available")
        return _snappy.compress(data)  # type: ignore[no-any-return]
    if compressor_id == COMPRESSOR_ZLIB:
        return zlib.compress(data)
    if compressor_id == COMPRESSOR_ZSTD:
        if _zstd is None:
            raise ProtocolError("zstd compression not available")
        compressor = _zstd.ZstdCompressor()
        return bytes(compressor.compress(data))
    raise ProtocolError(f"unknown compressor id: {compressor_id}")


def available_compressors() -> list[str]:
    """Return list of compressor names this server can handle."""
    avail = ["zlib"]
    if _snappy is not None:
        avail.append("snappy")
    if _zstd is not None:
        avail.append("zstd")
    return avail


# -- CRC-32C helpers -------------------------------------------------------


def _validate_checksum(data: bytes | bytearray, msg_length: int) -> None:
    """Validate the trailing CRC-32C checksum of an OP_MSG.

    If no CRC-32C library is installed, validation is silently skipped.
    """
    if _crc32c is None:
        return
    msg_body = data[: msg_length - 4]
    expected = struct.unpack_from("<I", data, msg_length - 4)[0]
    actual = _crc32c(bytes(msg_body))
    if actual != expected:
        raise ChecksumMismatch(f"CRC-32C mismatch: expected {expected:#010x}, got {actual:#010x}")


def _compute_checksum(data: bytes | bytearray) -> int:
    """Compute CRC-32C over the given bytes. Falls back to zlib.crc32 if needed."""
    if _crc32c is not None:
        return _crc32c(bytes(data))
    return zlib.crc32(bytes(data)) & 0xFFFFFFFF
