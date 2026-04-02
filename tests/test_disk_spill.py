"""Tests for the real disk-spill implementation in DiskSpillSorter and DiskSpillGrouper."""

from __future__ import annotations

import glob
import os
import random
import tempfile

import pytest

from smongo.aggregation.constants import (
    DEFAULT_MEMORY_LIMIT_BYTES,
    DiskSpillGrouper,
    DiskSpillSorter,
    MemoryLimitExceeded,
    _estimate_docs_bytes,
    _write_chunk_to_file,
    _iter_jsonl_file,
)
from smongo.aggregation.cursor import Cursor


# ── DiskSpillSorter ──────────────────────────────────────────────────


class TestDiskSpillSorter:
    def _key_fn(self, d):
        return (d.get("x") is not None, d.get("x"))

    def test_sort_small_no_spill(self):
        """Lists smaller than CHUNK_SIZE are sorted in-memory (no files)."""
        docs = [{"x": i} for i in range(50)]
        random.shuffle(docs)
        sorter = DiskSpillSorter(self._key_fn)
        result = sorter.sort(docs)
        assert [d["x"] for d in result] == list(range(50))
        assert sorter._tmpfiles == []

    def test_sort_large_creates_and_cleans_temp_files(self, tmp_path):
        """Sorting more docs than CHUNK_SIZE creates temp files, then cleans up."""
        docs = [{"x": i} for i in range(500)]
        random.shuffle(docs)

        sorter = DiskSpillSorter(self._key_fn)
        sorter._CHUNK_SIZE = 100

        result = sorter.sort(docs)
        assert len(result) == 500
        assert [d["x"] for d in result] == list(range(500))
        assert sorter._tmpfiles == []

    def test_sort_correctness_ascending(self):
        docs = [{"x": random.randint(0, 999)} for _ in range(2000)]
        sorter = DiskSpillSorter(self._key_fn, reverse=False)
        sorter._CHUNK_SIZE = 300
        result = sorter.sort(docs)
        vals = [d["x"] for d in result]
        assert vals == sorted(vals)

    def test_sort_correctness_descending(self):
        docs = [{"x": random.randint(0, 999)} for _ in range(2000)]
        sorter = DiskSpillSorter(self._key_fn, reverse=True)
        sorter._CHUNK_SIZE = 300
        result = sorter.sort(docs)
        vals = [d["x"] for d in result]
        assert vals == sorted(vals, reverse=True)

    def test_sort_empty_input(self):
        sorter = DiskSpillSorter(self._key_fn)
        assert sorter.sort([]) == []

    def test_sort_single_doc(self):
        sorter = DiskSpillSorter(self._key_fn)
        result = sorter.sort([{"x": 42}])
        assert result == [{"x": 42}]

    def test_sort_single_chunk(self):
        """Exactly one chunk = no merge needed."""
        docs = [{"x": i} for i in range(100)]
        random.shuffle(docs)
        sorter = DiskSpillSorter(self._key_fn)
        sorter._CHUNK_SIZE = 100
        result = sorter.sort(docs)
        assert [d["x"] for d in result] == list(range(100))

    def test_sort_with_none_values(self):
        docs = [{"x": None}, {"x": 3}, {"x": 1}, {"x": None}, {"x": 2}]
        sorter = DiskSpillSorter(self._key_fn)
        sorter._CHUNK_SIZE = 2
        result = sorter.sort(docs)
        vals = [d["x"] for d in result]
        assert vals[:2] == [None, None]
        assert vals[2:] == [1, 2, 3]

    def test_compound_sort_keys(self):
        docs = [
            {"city": "NYC", "age": 30},
            {"city": "NYC", "age": 20},
            {"city": "SF", "age": 25},
            {"city": "SF", "age": 35},
            {"city": "LA", "age": 28},
        ]
        random.shuffle(docs)
        key_fn = lambda d: (d["city"] is not None, d["city"])
        sorter = DiskSpillSorter(key_fn)
        sorter._CHUNK_SIZE = 2
        result = sorter.sort(docs)
        cities = [d["city"] for d in result]
        assert cities == sorted(cities)


# ── DiskSpillGrouper ─────────────────────────────────────────────────


class TestDiskSpillGrouper:
    def test_basic_grouping(self):
        grouper = DiskSpillGrouper()
        grouper._FLUSH_THRESHOLD = 3
        for i in range(9):
            grouper.add(i % 3, {"v": i})

        groups = {}
        for key, it in grouper.iter_groups():
            groups[key] = list(it)
        grouper.cleanup()

        assert len(groups) == 3
        assert len(groups[0]) == 3
        assert len(groups[1]) == 3
        assert len(groups[2]) == 3

    def test_grouper_cleanup(self):
        grouper = DiskSpillGrouper()
        grouper._FLUSH_THRESHOLD = 2
        for i in range(10):
            grouper.add(i % 2, {"v": i})
        grouper._flush()

        assert len(grouper._bucket_files) > 0
        all_paths = [p for paths in grouper._bucket_files.values() for p in paths]
        for p in all_paths:
            assert os.path.exists(p)

        grouper.cleanup()
        for p in all_paths:
            assert not os.path.exists(p)

    def test_grouper_empty(self):
        grouper = DiskSpillGrouper()
        groups = list(grouper.iter_groups())
        assert groups == []
        grouper.cleanup()

    def test_grouper_single_key(self):
        grouper = DiskSpillGrouper()
        grouper._FLUSH_THRESHOLD = 5
        for i in range(20):
            grouper.add("only", {"v": i})

        groups = {}
        for key, it in grouper.iter_groups():
            groups[key] = list(it)
        grouper.cleanup()
        assert len(groups) == 1
        assert len(groups["only"]) == 20

    def test_grouper_dict_keys(self):
        grouper = DiskSpillGrouper()
        grouper._FLUSH_THRESHOLD = 2
        grouper.add({"dept": "eng"}, {"v": 1})
        grouper.add({"dept": "eng"}, {"v": 2})
        grouper.add({"dept": "mgmt"}, {"v": 3})

        groups = {}
        for key, it in grouper.iter_groups():
            groups[str(key)] = list(it)
        grouper.cleanup()
        assert len(groups) == 2


# ── Integration via Cursor.aggregate ─────────────────────────────────


class TestDiskSpillAggregation:
    def test_sort_via_aggregate_with_disk_use(self):
        docs = [{"x": random.randint(0, 9999)} for _ in range(200)]
        result = Cursor(docs).aggregate(
            [{"$sort": {"x": 1}}],
            allowDiskUse=True,
            memory_limit_bytes=1,
        )
        vals = [d["x"] for d in result]
        assert vals == sorted(vals)

    def test_group_via_aggregate_with_disk_use(self):
        docs = [{"city": f"city_{i % 5}", "val": i} for i in range(100)]
        result = Cursor(docs).aggregate(
            [{"$group": {"_id": "$city", "total": {"$sum": "$val"}}}],
            allowDiskUse=True,
            memory_limit_bytes=1,
        )
        assert len(result) == 5
        totals = {d["_id"]: d["total"] for d in result}
        for c in range(5):
            expected = sum(i for i in range(100) if i % 5 == c)
            assert totals[f"city_{c}"] == expected

    def test_memory_limit_exceeded_without_disk_use(self):
        docs = [{"x": i, "payload": "a" * 500} for i in range(500)]
        with pytest.raises(MemoryLimitExceeded):
            Cursor(docs).aggregate(
                [{"$sort": {"x": 1}}],
                allowDiskUse=False,
                memory_limit_bytes=1,
            )

    def test_group_memory_limit_exceeded_without_disk_use(self):
        docs = [{"x": i, "payload": "b" * 500} for i in range(500)]
        with pytest.raises(MemoryLimitExceeded):
            Cursor(docs).aggregate(
                [{"$group": {"_id": "$x", "c": {"$sum": 1}}}],
                allowDiskUse=False,
                memory_limit_bytes=1,
            )

    def test_descending_sort_via_aggregate(self):
        docs = [{"x": random.randint(0, 999)} for _ in range(200)]
        result = Cursor(docs).aggregate(
            [{"$sort": {"x": -1}}],
            allowDiskUse=True,
            memory_limit_bytes=1,
        )
        vals = [d["x"] for d in result]
        assert vals == sorted(vals, reverse=True)


# ── Helper functions ─────────────────────────────────────────────────


class TestHelpers:
    def test_write_and_read_jsonl(self):
        docs = [{"a": 1, "b": "hello"}, {"a": 2, "b": "world"}]
        path = _write_chunk_to_file(docs)
        try:
            result = list(_iter_jsonl_file(path))
            assert result == docs
        finally:
            os.unlink(path)

    def test_estimate_docs_bytes(self):
        docs = [{"x": i, "data": "a" * 100} for i in range(100)]
        est = _estimate_docs_bytes(docs)
        assert est > 0

    def test_estimate_empty(self):
        assert _estimate_docs_bytes([]) == 0
