"""Additional tests for aggregation output stages to boost coverage.

Covers:
- $out error conditions
- $merge all whenMatched/whenNotMatched combinations
- $merge error conditions
- $unionWith with and without pipelines
"""

import pytest

from smongo.aggregation import Cursor


class FakeCollection:
    """Mock collection for testing output stages."""

    def __init__(self, data=None):
        self.data = list(data or [])

    def delete(self, q, multi=True):
        self.data.clear()

    def insert_many(self, docs):
        self.data.extend(docs)

    def insert_one(self, doc):
        self.data.append(doc)

    def find(self, q):
        return [d for d in self.data if all(d.get(k) == v for k, v in q.items())]

    def update(self, q, update_spec, multi=False):
        for d in self.data:
            if d["_id"] == q["_id"]:
                d.update(update_spec.get("$set", {}))

    def get_all(self):
        return list(self.data)


# ── $out error conditions ───────────────────────────────────────────


class TestOutErrorConditions:
    def test_out_without_collection_getter_raises(self):
        """$out requires a collection_getter to be provided."""
        data = [{"_id": "1", "x": 1}]
        with pytest.raises(RuntimeError, match="requires a collection getter"):
            Cursor(data).aggregate([{"$out": "target"}])

    def test_out_with_dict_spec(self):
        """$out can accept a dict spec with 'coll' or 'db' key."""
        target = FakeCollection()

        def getter(name):
            return target

        data = [{"_id": "1", "x": 1}]
        # Test dict with 'coll' key
        result = Cursor(data, collection_getter=getter).aggregate([{"$out": {"coll": "target"}}])
        assert len(result) == 1
        assert len(target.data) == 1

    def test_out_large_batch(self):
        """$out handles large datasets by batching inserts."""
        from smongo.aggregation.output import _OUT_BATCH_SIZE

        target = FakeCollection()

        def getter(name):
            return target

        # Create more docs than batch size
        data = [{"_id": str(i), "x": i} for i in range(_OUT_BATCH_SIZE + 500)]
        result = Cursor(data, collection_getter=getter).aggregate([{"$out": "target"}])
        assert len(result) == _OUT_BATCH_SIZE + 500
        assert len(target.data) == _OUT_BATCH_SIZE + 500


# ── $merge all branches ─────────────────────────────────────────────


class TestMergeAllBranches:
    def test_merge_without_collection_getter_raises(self):
        """$merge requires a collection_getter."""
        data = [{"_id": "1", "x": 1}]
        with pytest.raises(RuntimeError, match="requires a collection getter"):
            Cursor(data).aggregate([{"$merge": {"into": "target"}}])

    def test_merge_with_dict_into(self):
        """$merge can accept a dict for 'into' with 'coll' key."""
        target = FakeCollection([{"_id": "1", "x": "old"}])

        def getter(name):
            return target

        data = [{"_id": "1", "x": "new"}]
        result = Cursor(data, collection_getter=getter).aggregate(
            [{"$merge": {"into": {"coll": "target"}, "on": "_id"}}]
        )
        assert len(result) == 1
        assert target.data[0]["x"] == "new"

    def test_merge_when_matched_merge(self):
        """$merge with whenMatched: 'merge' merges fields."""
        target = FakeCollection([{"_id": "1", "x": "old", "y": "keep"}])

        def getter(name):
            return target

        data = [{"_id": "1", "x": "new", "z": "added"}]
        Cursor(data, collection_getter=getter).aggregate(
            [
                {
                    "$merge": {
                        "into": "target",
                        "on": "_id",
                        "whenMatched": "merge",
                    }
                }
            ]
        )
        assert target.data[0]["x"] == "new"
        assert target.data[0]["y"] == "keep"
        assert target.data[0]["z"] == "added"

    def test_merge_when_matched_keep_existing(self):
        """$merge with whenMatched: 'keepExisting' keeps old doc."""
        target = FakeCollection([{"_id": "1", "x": "old"}])

        def getter(name):
            return target

        data = [{"_id": "1", "x": "new"}]
        Cursor(data, collection_getter=getter).aggregate(
            [
                {
                    "$merge": {
                        "into": "target",
                        "on": "_id",
                        "whenMatched": "keepExisting",
                    }
                }
            ]
        )
        # keepExisting does nothing, doc unchanged
        assert target.data[0]["x"] == "old"

    def test_merge_when_matched_fail(self):
        """$merge with whenMatched: 'fail' raises on match."""
        target = FakeCollection([{"_id": "1", "x": "old"}])

        def getter(name):
            return target

        data = [{"_id": "1", "x": "new"}]
        with pytest.raises(ValueError, match="document already exists"):
            Cursor(data, collection_getter=getter).aggregate(
                [
                    {
                        "$merge": {
                            "into": "target",
                            "on": "_id",
                            "whenMatched": "fail",
                        }
                    }
                ]
            )

    def test_merge_when_not_matched_discard(self):
        """$merge with whenNotMatched: 'discard' skips inserts."""
        target = FakeCollection([{"_id": "1", "x": "old"}])

        def getter(name):
            return target

        data = [{"_id": "2", "x": "new"}]
        Cursor(data, collection_getter=getter).aggregate(
            [
                {
                    "$merge": {
                        "into": "target",
                        "on": "_id",
                        "whenNotMatched": "discard",
                    }
                }
            ]
        )
        # Doc not inserted
        assert len(target.data) == 1
        assert target.data[0]["_id"] == "1"

    def test_merge_when_not_matched_fail(self):
        """$merge with whenNotMatched: 'fail' raises on no match."""
        target = FakeCollection([{"_id": "1", "x": "old"}])

        def getter(name):
            return target

        data = [{"_id": "2", "x": "new"}]
        with pytest.raises(ValueError, match="no matching document found"):
            Cursor(data, collection_getter=getter).aggregate(
                [
                    {
                        "$merge": {
                            "into": "target",
                            "on": "_id",
                            "whenNotMatched": "fail",
                        }
                    }
                ]
            )

    def test_merge_unsupported_when_matched(self):
        """$merge with unsupported whenMatched value raises."""
        target = FakeCollection()

        def getter(name):
            return target

        data = [{"_id": "1", "x": 1}]
        with pytest.raises(ValueError, match="unsupported whenMatched"):
            Cursor(data, collection_getter=getter).aggregate(
                [{"$merge": {"into": "target", "whenMatched": "invalid"}}]
            )

    def test_merge_unsupported_when_not_matched(self):
        """$merge with unsupported whenNotMatched value raises."""
        target = FakeCollection()

        def getter(name):
            return target

        data = [{"_id": "1", "x": 1}]
        with pytest.raises(ValueError, match="unsupported whenNotMatched"):
            Cursor(data, collection_getter=getter).aggregate(
                [{"$merge": {"into": "target", "whenNotMatched": "invalid"}}]
            )

    def test_merge_composite_on_key(self):
        """$merge supports composite 'on' keys as a list."""
        target = FakeCollection([{"_id": "1", "dept": "eng", "city": "NYC", "count": 5}])

        def getter(name):
            return target

        data = [{"_id": "2", "dept": "eng", "city": "NYC", "count": 10}]
        Cursor(data, collection_getter=getter).aggregate(
            [
                {
                    "$merge": {
                        "into": "target",
                        "on": ["dept", "city"],
                        "whenMatched": "replace",
                    }
                }
            ]
        )
        # Should match on dept+city and replace
        assert len(target.data) == 1
        assert target.data[0]["count"] == 10


# ── $unionWith ──────────────────────────────────────────────────────


class TestUnionWith:
    def test_union_with_basic(self):
        """$unionWith combines docs from another collection."""
        source = FakeCollection([{"_id": "1", "x": 1}, {"_id": "2", "x": 2}])
        other = FakeCollection([{"_id": "3", "x": 3}, {"_id": "4", "x": 4}])

        def getter(name):
            if name == "other":
                return other
            return source

        data = source.get_all()
        result = Cursor(data, collection_getter=getter).aggregate(
            [{"$unionWith": {"coll": "other"}}]
        )
        assert len(result) == 4
        ids = {d["_id"] for d in result}
        assert ids == {"1", "2", "3", "4"}

    def test_union_with_pipeline(self):
        """$unionWith can apply a pipeline to the foreign collection."""
        source = FakeCollection([{"_id": "1", "x": 1}])
        other = FakeCollection([{"_id": "2", "x": 5}, {"_id": "3", "x": 10}])

        def getter(name):
            if name == "other":
                return other
            return source

        data = source.get_all()
        result = Cursor(data, collection_getter=getter).aggregate(
            [{"$unionWith": {"coll": "other", "pipeline": [{"$match": {"x": {"$gt": 6}}}]}}]
        )
        # Source: 1 doc, Other after filter: 1 doc (x=10)
        assert len(result) == 2
        ids = {d["_id"] for d in result}
        assert ids == {"1", "3"}

    def test_union_with_without_coll_raises(self):
        """$unionWith requires 'coll' key."""
        data = [{"_id": "1", "x": 1}]

        def getter(name):
            return FakeCollection()

        with pytest.raises(ValueError, match="requires 'coll'"):
            Cursor(data, collection_getter=getter).aggregate([{"$unionWith": {}}])

    def test_union_with_without_collection_getter_raises(self):
        """$unionWith requires collection_getter."""
        data = [{"_id": "1", "x": 1}]
        with pytest.raises(RuntimeError, match="requires a collection getter"):
            Cursor(data).aggregate([{"$unionWith": {"coll": "other"}}])
