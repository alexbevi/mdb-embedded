"""Tests for disk-spill aggregation stages ($group, $sort with allow_disk_use).

These test the Python fallback paths (_py_group_stage, _py_sort_stage) that
Rust delegates to when allow_disk_use=True and memory limits are exceeded.
"""

import pytest

from smongo.aggregation import Cursor
from smongo.aggregation.stages import (
    _eval_accumulator,
    _py_group_stage,
    _py_sort_stage,
)


class FakeCollection:
    """Mock collection for testing."""

    def __init__(self, data=None):
        self.data = list(data or [])

    def find(self, q):
        return [d for d in self.data if all(d.get(k) == v for k, v in q.items())]

    def get_all(self):
        return list(self.data)


# ── _py_group_stage ─────────────────────────────────────────────────


class TestPyGroupStage:
    def test_group_stage_in_memory(self):
        """_py_group_stage groups docs in memory when allow_disk_use=False."""
        docs = [
            {"_id": "1", "dept": "eng", "salary": 100},
            {"_id": "2", "dept": "eng", "salary": 120},
            {"_id": "3", "dept": "sales", "salary": 90},
        ]
        spec = {"_id": "$dept", "total": {"$sum": "$salary"}}
        result = _py_group_stage(docs, spec, allow_disk_use=False)
        assert len(result) == 2
        by_dept = {r["_id"]: r for r in result}
        assert by_dept["eng"]["total"] == 220
        assert by_dept["sales"]["total"] == 90

    def test_group_stage_disk_spill(self):
        """_py_group_stage uses disk-spill grouper when allow_disk_use=True."""
        # Create enough docs to trigger disk spill
        docs = [{"_id": str(i), "dept": f"dept_{i % 10}", "value": i} for i in range(1000)]
        spec = {"_id": "$dept", "count": {"$sum": 1}, "total": {"$sum": "$value"}}
        result = _py_group_stage(docs, spec, allow_disk_use=True)
        # Should produce 10 groups (dept_0 through dept_9)
        assert len(result) == 10
        # Verify grouping worked
        total_count = sum(r["count"] for r in result)
        assert total_count == 1000

    def test_group_stage_complex_id(self):
        """_py_group_stage handles dict/list _id values by JSON serialization."""
        docs = [
            {"_id": "1", "city": "NYC", "dept": "eng", "count": 5},
            {"_id": "2", "city": "NYC", "dept": "sales", "count": 3},
            {"_id": "3", "city": "NYC", "dept": "eng", "count": 7},
        ]
        spec = {
            "_id": {"city": "$city", "dept": "$dept"},
            "total": {"$sum": "$count"},
        }
        result = _py_group_stage(docs, spec, allow_disk_use=False)
        assert len(result) == 2
        # Find the eng group
        eng_group = next(r for r in result if r["_id"]["dept"] == "eng")
        assert eng_group["total"] == 12


# ── _py_sort_stage ──────────────────────────────────────────────────


class TestPySortStage:
    def test_sort_stage_in_memory(self):
        """_py_sort_stage sorts in memory when data is small."""
        docs = [
            {"_id": "1", "age": 30},
            {"_id": "2", "age": 25},
            {"_id": "3", "age": 35},
        ]
        spec = {"age": 1}
        result = _py_sort_stage(docs, spec, allow_disk_use=False)
        ages = [d["age"] for d in result]
        assert ages == [25, 30, 35]

    def test_sort_stage_descending(self):
        """_py_sort_stage handles descending sort."""
        docs = [
            {"_id": "1", "age": 30},
            {"_id": "2", "age": 25},
            {"_id": "3", "age": 35},
        ]
        spec = {"age": -1}
        result = _py_sort_stage(docs, spec, allow_disk_use=False)
        ages = [d["age"] for d in result]
        assert ages == [35, 30, 25]

    def test_sort_stage_disk_spill(self):
        """_py_sort_stage uses disk spill for large datasets."""
        # Create large dataset to trigger disk spill
        docs = [{"_id": str(i), "value": 1000 - i} for i in range(5000)]
        spec = {"value": 1}
        result = _py_sort_stage(docs, spec, allow_disk_use=True)
        # Should be sorted ascending (1000-4999 = -3999 to 1000-0 = 1000)
        assert result[0]["value"] == -3999
        assert result[-1]["value"] == 1000
        # Verify full sort
        values = [d["value"] for d in result]
        assert values == sorted(values)

    def test_sort_stage_null_handling(self):
        """_py_sort_stage sorts nulls before non-nulls."""
        docs = [
            {"_id": "1", "age": 30},
            {"_id": "2"},  # missing age
            {"_id": "3", "age": 25},
        ]
        spec = {"age": 1}
        result = _py_sort_stage(docs, spec, allow_disk_use=False)
        # Null should come first
        assert "_id" in result[0] and result[0]["_id"] == "2"
        assert result[1]["age"] == 25
        assert result[2]["age"] == 30

    def test_sort_stage_multi_field(self):
        """_py_sort_stage handles multi-field sorts (reversed iteration)."""
        docs = [
            {"_id": "1", "city": "NYC", "age": 30},
            {"_id": "2", "city": "SF", "age": 25},
            {"_id": "3", "city": "NYC", "age": 25},
        ]
        # Sort by city asc, then age desc
        spec = {"city": 1, "age": -1}
        result = _py_sort_stage(docs, spec, allow_disk_use=False)
        # NYC docs first (age 30 before 25), then SF
        assert result[0]["_id"] == "1"  # NYC, 30
        assert result[1]["_id"] == "3"  # NYC, 25
        assert result[2]["_id"] == "2"  # SF, 25


# ── _eval_accumulator ───────────────────────────────────────────────


class TestEvalAccumulator:
    def test_sum_accumulator(self):
        """$sum accumulator totals values."""
        docs = [{"value": 10}, {"value": 20}, {"value": 30}]
        result = _eval_accumulator({"$sum": "$value"}, docs)
        assert result == 60

    def test_avg_accumulator(self):
        """$avg accumulator computes average."""
        docs = [{"value": 10}, {"value": 20}, {"value": 30}]
        result = _eval_accumulator({"$avg": "$value"}, docs)
        assert result == 20

    def test_min_accumulator(self):
        """$min accumulator finds minimum."""
        docs = [{"value": 30}, {"value": 10}, {"value": 20}]
        result = _eval_accumulator({"$min": "$value"}, docs)
        assert result == 10

    def test_max_accumulator(self):
        """$max accumulator finds maximum."""
        docs = [{"value": 30}, {"value": 10}, {"value": 20}]
        result = _eval_accumulator({"$max": "$value"}, docs)
        assert result == 30

    def test_first_accumulator(self):
        """$first accumulator takes first value."""
        docs = [{"value": "a"}, {"value": "b"}, {"value": "c"}]
        result = _eval_accumulator({"$first": "$value"}, docs)
        assert result == "a"

    def test_last_accumulator(self):
        """$last accumulator takes last value."""
        docs = [{"value": "a"}, {"value": "b"}, {"value": "c"}]
        result = _eval_accumulator({"$last": "$value"}, docs)
        assert result == "c"

    def test_push_accumulator(self):
        """$push accumulator creates array of values."""
        docs = [{"value": 1}, {"value": 2}, {"value": 3}]
        result = _eval_accumulator({"$push": "$value"}, docs)
        assert result == [1, 2, 3]

    def test_add_to_set_accumulator(self):
        """$addToSet accumulator creates set (unique values)."""
        docs = [{"value": 1}, {"value": 2}, {"value": 1}, {"value": 3}]
        result = _eval_accumulator({"$addToSet": "$value"}, docs)
        assert set(result) == {1, 2, 3}
        assert len(result) == 3

    def test_std_dev_pop_accumulator(self):
        """$stdDevPop accumulator computes population std dev."""
        docs = [{"value": 10}, {"value": 20}, {"value": 30}]
        result = _eval_accumulator({"$stdDevPop": "$value"}, docs)
        # Std dev of [10, 20, 30] with population formula
        import math

        expected = math.sqrt(((10 - 20) ** 2 + (20 - 20) ** 2 + (30 - 20) ** 2) / 3)
        assert abs(result - expected) < 0.01

    def test_std_dev_samp_accumulator(self):
        """$stdDevSamp accumulator computes sample std dev."""
        docs = [{"value": 10}, {"value": 20}, {"value": 30}]
        result = _eval_accumulator({"$stdDevSamp": "$value"}, docs)
        # Sample std dev (n-1 denominator)
        import math

        expected = math.sqrt(((10 - 20) ** 2 + (20 - 20) ** 2 + (30 - 20) ** 2) / 2)
        assert abs(result - expected) < 0.01

    def test_accumulator_with_literal(self):
        """Accumulator with literal value (e.g., $sum: 1)."""
        docs = [{"x": 1}, {"x": 2}, {"x": 3}]
        result = _eval_accumulator({"$sum": 1}, docs)
        assert result == 3  # Count


# ── Integration with Cursor.aggregate ──────────────────────────────


class TestCursorDiskSpillIntegration:
    def test_aggregate_with_disk_spill_group(self):
        """Cursor.aggregate with allowDiskUse triggers disk-spill $group."""
        docs = [{"_id": str(i), "category": i % 5, "value": i} for i in range(100)]

        def getter(name):
            return FakeCollection(docs)

        result = Cursor(docs, collection_getter=getter).aggregate(
            [{"$group": {"_id": "$category", "total": {"$sum": "$value"}}}],
            allowDiskUse=True,
        )
        assert len(result) == 5

    def test_aggregate_with_disk_spill_sort(self):
        """Cursor.aggregate with allowDiskUse triggers disk-spill $sort."""
        docs = [{"_id": str(i), "value": 100 - i} for i in range(200)]
        result = Cursor(docs).aggregate([{"$sort": {"value": 1}}], allowDiskUse=True)
        values = [d["value"] for d in result]
        # Should be sorted
        assert values[0] < values[-1]
