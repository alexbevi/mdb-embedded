"""Tests for TTL reaper batching and scan_with_fields."""

import time

from smongo.storage import _TTL_DELETE_BATCH_SIZE, TTLReaper


class TestScanWithFields:
    def test_returns_ids_and_field_values(self, local_collection):
        local_collection.insert_many([
            {"name": "a", "ts": 100},
            {"name": "b", "ts": 200},
            {"name": "c", "ts": 300},
        ])
        results = local_collection.scan_with_fields(["ts"])
        assert len(results) == 3
        ts_values = sorted(r[1]["ts"] for r in results)
        assert ts_values == [100, 200, 300]

    def test_returns_none_for_missing_field(self, local_collection):
        local_collection.insert_one({"name": "x"})
        results = local_collection.scan_with_fields(["nonexistent"])
        assert len(results) == 1
        assert results[0][1]["nonexistent"] is None

    def test_multiple_fields(self, local_collection):
        local_collection.insert_one({"a": 1, "b": 2, "c": 3})
        results = local_collection.scan_with_fields(["a", "c"])
        assert len(results) == 1
        assert results[0][1] == {"a": 1, "c": 3}

    def test_empty_collection(self, local_collection):
        results = local_collection.scan_with_fields(["x"])
        assert results == []


class TestTTLReaperBatching:
    def test_batch_size_constant(self):
        assert _TTL_DELETE_BATCH_SIZE == 500

    def test_reaper_deletes_expired_docs(self, local_collection):
        now = time.time()
        local_collection.insert_many([
            {"ts": now - 200, "name": "expired_1"},
            {"ts": now - 200, "name": "expired_2"},
            {"ts": now + 3600, "name": "fresh"},
        ])
        local_collection.create_index([("ts", 1)], expireAfterSeconds=60)
        reaper = TTLReaper(local_collection, interval_sec=1, batch_size=10)
        reaper._reap_once()
        remaining = local_collection.get_all()
        names = [d["name"] for d in remaining]
        assert "fresh" in names
        assert "expired_1" not in names
        assert "expired_2" not in names

    def test_reaper_no_ttl_indexes_skips(self, local_collection):
        local_collection.insert_one({"name": "x"})
        reaper = TTLReaper(local_collection)
        reaper._reap_once()
        assert len(local_collection.get_all()) == 1

    def test_reaper_batch_size_param(self, local_collection):
        reaper = TTLReaper(local_collection, batch_size=42)
        assert reaper._batch_size == 42
