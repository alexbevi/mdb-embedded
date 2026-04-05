"""Tests for smongo.index -- key encoding and IndexDef.

The Python IndexManager / QueryPlanner classes were removed after the full
Rust port.  This file tests only the encoding utilities and IndexDef that
remain.  IndexManager / QueryPlanner behaviour is exercised through the
RustLocalCollection integration tests in test_storage.py and test_streaming.py.
"""

import pytest

from smongo.index import (
    DuplicateKeyError,
    IndexDef,
    _invert_encoded,
    _sortable_encode,
    encode_index_key,
    encode_index_key_prefix,
)
from smongo.objectid import ObjectId

# ── Key encoding ─────────────────────────────────────────────────────


class TestSortableEncode:
    def test_none(self):
        assert _sortable_encode(None) == "00"

    def test_bool_false(self):
        assert _sortable_encode(False) == "30"

    def test_bool_true(self):
        assert _sortable_encode(True) == "31"

    def test_positive_int(self):
        enc = _sortable_encode(42)
        assert enc.startswith("1")

    def test_negative_int(self):
        enc = _sortable_encode(-10)
        assert enc.startswith("1")

    def test_float(self):
        enc = _sortable_encode(3.14)
        assert enc.startswith("1")

    def test_string(self):
        enc = _sortable_encode("hello")
        assert enc.startswith("2")

    def test_objectid(self):
        oid = ObjectId()
        enc = _sortable_encode(oid)
        assert enc.startswith("15")

    def test_dict_fallback(self):
        enc = _sortable_encode({"x": 1})
        assert enc.startswith("2")

    def test_numeric_ordering(self):
        values = [-100, -1, 0, 1, 42, 100, 999]
        encoded = [_sortable_encode(v) for v in values]
        assert encoded == sorted(encoded)

    def test_string_ordering(self):
        values = ["apple", "banana", "cherry"]
        encoded = [_sortable_encode(v) for v in values]
        assert encoded == sorted(encoded)


class TestInvertEncoded:
    def test_invert_roundtrip(self):
        enc = _sortable_encode(42)
        inv = _invert_encoded(enc)
        assert _invert_encoded(inv) == enc

    def test_invert_reverses_ordering(self):
        vals = [1, 2, 3]
        encoded = [_sortable_encode(v) for v in vals]
        inverted = [_invert_encoded(e) for e in encoded]
        assert inverted == sorted(inverted, reverse=True)


class TestEncodeIndexKey:
    def test_single_field_asc(self):
        key = encode_index_key([42], "doc1", [1])
        assert "|" in key
        assert key.endswith("doc1")

    def test_multi_field(self):
        key = encode_index_key(["NYC", 30], "doc1", [1, -1])
        parts = key.split("|")
        assert len(parts) == 3  # 2 fields + doc_id

    def test_prefix(self):
        prefix = encode_index_key_prefix([42], [1])
        assert prefix.endswith("|")


# ── IndexDef ─────────────────────────────────────────────────────────


class TestIndexDef:
    def test_fields(self):
        idx = IndexDef("test", [("age", 1), ("name", -1)])
        assert idx.fields == ["age", "name"]

    def test_directions(self):
        idx = IndexDef("test", [("age", 1), ("name", -1)])
        assert idx.directions == [1, -1]

    def test_to_dict(self):
        idx = IndexDef("test", [("age", 1)], unique=True, expire_after_seconds=60)
        d = idx.to_dict()
        assert d["name"] == "test"
        assert d["unique"] is True
        assert d["expireAfterSeconds"] == 60
