"""
ObjectId -- MongoDB-compatible document identifier.

Follows the MongoDB ObjectId spec:
    4-byte timestamp (seconds since epoch)
    5-byte random value (per-process, generated once)
    3-byte incrementing counter (per-process)

The 12-byte value is represented as a 24-character hex string.
"""

from __future__ import annotations

import os
import struct
import threading
import time
from datetime import UTC, datetime

_RANDOM_BYTES = os.urandom(5)
_COUNTER_LOCK = threading.Lock()
_COUNTER = int.from_bytes(os.urandom(3), "big")


class ObjectId:
    __slots__ = ("_bytes",)

    def __init__(self, oid: str | bytes | ObjectId | None = None) -> None:
        if oid is None:
            self._bytes = self._generate()
        elif isinstance(oid, bytes) and len(oid) == 12:
            self._bytes = oid
        elif isinstance(oid, str) and len(oid) == 24:
            self._bytes = bytes.fromhex(oid)
        elif isinstance(oid, ObjectId):
            self._bytes = oid._bytes
        else:
            raise ValueError(f"Invalid ObjectId: {oid!r}")

    @staticmethod
    def _generate() -> bytes:
        global _COUNTER
        ts = struct.pack(">I", int(time.time()))
        with _COUNTER_LOCK:
            _COUNTER = (_COUNTER + 1) & 0xFFFFFF
            counter = _COUNTER
        cnt = struct.pack(">I", counter)[1:]  # 3 bytes
        return ts + _RANDOM_BYTES + cnt

    @property
    def generation_time(self) -> datetime:
        ts = struct.unpack(">I", self._bytes[:4])[0]
        return datetime.fromtimestamp(ts, tz=UTC)

    @property
    def binary(self) -> bytes:
        return self._bytes

    def __str__(self) -> str:
        return self._bytes.hex()

    def __repr__(self) -> str:
        return f"ObjectId('{self}')"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ObjectId):
            return self._bytes == other._bytes
        if isinstance(other, str):
            return str(self) == other
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    def __hash__(self) -> int:
        return hash(self._bytes)

    def __lt__(self, other: object) -> bool:
        if isinstance(other, ObjectId):
            return self._bytes < other._bytes
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, ObjectId):
            return self._bytes <= other._bytes
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, ObjectId):
            return self._bytes > other._bytes
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, ObjectId):
            return self._bytes >= other._bytes
        return NotImplemented

    @staticmethod
    def is_valid(oid: str | ObjectId) -> bool:
        if isinstance(oid, ObjectId):
            return True
        if isinstance(oid, str) and len(oid) == 24:
            try:
                bytes.fromhex(oid)
                return True
            except ValueError:
                return False
        return False
