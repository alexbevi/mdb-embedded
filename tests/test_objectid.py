"""Tests for smongo.objectid.ObjectId."""

import threading
import time
from datetime import UTC, datetime

import pytest

from smongo.objectid import ObjectId


class TestObjectIdConstruction:
    def test_auto_generate(self):
        oid = ObjectId()
        assert len(oid.binary) == 12
        assert len(str(oid)) == 24

    def test_from_hex_string(self):
        hex_str = "507f1f77bcf86cd799439011"
        oid = ObjectId(hex_str)
        assert str(oid) == hex_str

    def test_from_bytes(self):
        raw = b"\x50\x7f\x1f\x77\xbc\xf8\x6c\xd7\x99\x43\x90\x11"
        oid = ObjectId(raw)
        assert oid.binary == raw

    def test_from_objectid(self):
        original = ObjectId()
        copy = ObjectId(original)
        assert copy == original
        assert copy._bytes is original._bytes

    def test_invalid_short_string(self):
        with pytest.raises(ValueError, match="Invalid ObjectId"):
            ObjectId("abc")

    def test_invalid_non_hex_string(self):
        with pytest.raises(ValueError):
            ObjectId("zzzzzzzzzzzzzzzzzzzzzzzz")

    def test_invalid_type_int(self):
        with pytest.raises(ValueError, match="Invalid ObjectId"):
            ObjectId(12345)

    def test_invalid_type_empty_bytes(self):
        with pytest.raises(ValueError, match="Invalid ObjectId"):
            ObjectId(b"short")

    def test_invalid_none_like(self):
        oid = ObjectId(None)
        assert len(oid.binary) == 12


class TestObjectIdProperties:
    def test_generation_time_close_to_now(self):
        oid = ObjectId()
        gen_time = oid.generation_time
        assert isinstance(gen_time, datetime)
        assert gen_time.tzinfo == UTC
        assert abs(gen_time.timestamp() - time.time()) < 2

    def test_binary_length(self):
        assert len(ObjectId().binary) == 12

    def test_str_is_24_hex(self):
        s = str(ObjectId())
        assert len(s) == 24
        int(s, 16)  # should not raise

    def test_repr(self):
        oid = ObjectId()
        assert repr(oid) == f"ObjectId('{oid}')"


class TestObjectIdComparison:
    def test_eq_same(self):
        oid = ObjectId()
        assert oid == ObjectId(oid)

    def test_eq_string(self):
        oid = ObjectId()
        assert oid == str(oid)

    def test_ne_different(self):
        assert ObjectId() != ObjectId()

    def test_ne_string_mismatch(self):
        oid = ObjectId()
        assert oid != "000000000000000000000000"

    def test_eq_not_implemented_for_int(self):
        assert ObjectId().__eq__(42) is NotImplemented

    def test_ne_not_implemented_for_int(self):
        assert ObjectId().__ne__(42) is NotImplemented

    def test_ordering(self):
        a = ObjectId()
        time.sleep(0.01)
        b = ObjectId()
        assert a < b
        assert a <= b
        assert b > a
        assert b >= a

    def test_ordering_not_implemented_for_str(self):
        assert ObjectId().__lt__("x") is NotImplemented
        assert ObjectId().__le__("x") is NotImplemented
        assert ObjectId().__gt__("x") is NotImplemented
        assert ObjectId().__ge__("x") is NotImplemented

    def test_hash_consistent(self):
        oid = ObjectId()
        copy = ObjectId(str(oid))
        assert hash(oid) == hash(copy)

    def test_hash_differs(self):
        assert hash(ObjectId()) != hash(ObjectId())

    def test_usable_in_set(self):
        oid = ObjectId()
        s = {oid, ObjectId(str(oid))}
        assert len(s) == 1


class TestObjectIdIsValid:
    def test_valid_objectid_instance(self):
        assert ObjectId.is_valid(ObjectId()) is True

    def test_valid_hex_string(self):
        assert ObjectId.is_valid("507f1f77bcf86cd799439011") is True

    def test_invalid_hex_string(self):
        assert ObjectId.is_valid("zzzzzzzzzzzzzzzzzzzzzzzz") is False

    def test_wrong_length_string(self):
        assert ObjectId.is_valid("507f1f") is False

    def test_non_string_type(self):
        assert ObjectId.is_valid(42) is False
        assert ObjectId.is_valid(None) is False


class TestObjectIdCounter:
    def test_sequential_ids_unique(self):
        ids = [ObjectId() for _ in range(100)]
        assert len(set(str(o) for o in ids)) == 100

    def test_thread_safety(self):
        results = []
        def gen_ids():
            results.extend([str(ObjectId()) for _ in range(200)])

        threads = [threading.Thread(target=gen_ids) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(set(results)) == 1000
