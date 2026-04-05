"""
MongoDB Wire Protocol -- OP_MSG (opcode 2013), OP_COMPRESSED (opcode 2012),
and legacy OP_QUERY/OP_REPLY.

Core binary framing (header parsing, section decode/encode, CRC-32C,
compression) lives in Rust (_smongo_core).  This module re-exports the Rust
implementations and keeps protocol constants + Python compression helpers
that tests depend on.
"""

import zlib

from smongo._smongo_core import (
    ChecksumMismatch,
    MsgHeader,
    ProtocolError,
    available_compressors,
    decode_compressed,
    decode_header,
    decode_msg,
    decode_query,
    encode_compressed,
    encode_msg,
    encode_reply,
)

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
    _zstd = None  # type: ignore[assignment]


# -- compression helpers (kept for test_security.py) -----------------------


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


__all__ = [
    "COMPRESSOR_NOOP",
    "COMPRESSOR_SNAPPY",
    "COMPRESSOR_ZLIB",
    "COMPRESSOR_ZSTD",
    "HEADER_SIZE",
    "MAX_MSG_SIZE",
    "OP_COMPRESSED",
    "OP_MSG",
    "OP_QUERY",
    "OP_REPLY",
    "_COMPRESSOR_IDS",
    "_COMPRESSOR_NAMES",
    "ChecksumMismatch",
    "MsgHeader",
    "ProtocolError",
    "_compress",
    "_decompress",
    "available_compressors",
    "decode_compressed",
    "decode_header",
    "decode_msg",
    "decode_query",
    "encode_compressed",
    "encode_msg",
    "encode_reply",
]
