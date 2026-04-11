"""Parity tests: verify RedbLocalCollection and RedbCollection behave identically.

These tests run the same CRUD and query operations against both the Rust-native
``RedbLocalCollection`` and the Python ``RedbCollection`` wrapper, catching any
behavioral drift between the two code paths.

Each test is automatically parameterized via the ``engine_collection`` fixture
which yields both implementations in turn.

Note: result object *types* differ (Rust returns dicts; wrapper returns
``InsertResult`` / ``UpdateResult`` / ``DeleteResult``).  These tests focus on
**storage behavior** — what gets stored, retrieved, counted — not result shapes.
"""

from __future__ import annotations

import pytest

from smongo._smongo_core import DuplicateKeyError, RedbLocalClient
from smongo.storage.redb_engine import RedbClient


@pytest.fixture(params=["native", "wrapper"], ids=["native", "wrapper"])
def engine_collection(request, tmp_path):
    """Yield a collection from either the Rust-native or Python-wrapper path."""
    db_dir = str(tmp_path / "redb_data")
    if request.param == "native":
        client = RedbLocalClient(db_dir)
        db = client.get_db("testdb")
        coll = db.get_collection("testcoll")
        yield coll
        client.close()
    else:
        client = RedbClient(db_dir)
        db = client.get_db("testdb")
        coll = db.get_collection("testcoll")
        yield coll
        client.close()


class TestInsertParity:
    def test_insert_one_persists(self, engine_collection):
        engine_collection.insert_one({"x": 1})
        assert engine_collection.count_documents({}) == 1

    def test_insert_many_persists(self, engine_collection):
        engine_collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        assert engine_collection.count_documents({}) == 3

    def test_insert_and_find_roundtrip(self, engine_collection):
        engine_collection.insert_one({"key": "value", "num": 42})
        docs = engine_collection.find({"key": "value"})
        assert len(docs) == 1
        assert docs[0]["num"] == 42


class TestFindParity:
    def test_find_empty(self, engine_collection):
        assert engine_collection.find({}) == []

    def test_find_one_missing(self, engine_collection):
        assert engine_collection.find_one({"nope": 1}) is None

    def test_find_with_filter(self, engine_collection):
        engine_collection.insert_many([{"x": 1}, {"x": 2}, {"x": 3}])
        results = engine_collection.find({"x": {"$gt": 1}})
        assert len(results) == 2

    def test_find_with_projection(self, engine_collection):
        engine_collection.insert_one({"a": 1, "b": 2, "c": 3})
        docs = engine_collection.find({}, {"a": 1, "_id": 0})
        assert len(docs) == 1
        assert "a" in docs[0]
        assert "b" not in docs[0]

    def test_find_one_with_projection(self, engine_collection):
        engine_collection.insert_one({"a": 10, "b": 20})
        doc = engine_collection.find_one({}, {"a": 1, "_id": 0})
        assert doc is not None
        assert "a" in doc
        assert "b" not in doc


class TestUpdateParity:
    def test_update_one(self, engine_collection):
        engine_collection.insert_one({"x": 1})
        engine_collection.update_one({"x": 1}, {"$set": {"x": 2}})
        assert engine_collection.find_one({"x": 2}) is not None
        assert engine_collection.find_one({"x": 1}) is None

    def test_update_many(self, engine_collection):
        engine_collection.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
        engine_collection.update_many({"x": 1}, {"$set": {"x": 99}})
        assert len(engine_collection.find({"x": 99})) == 2

    def test_upsert_creates_document(self, engine_collection):
        engine_collection.update_one(
            {"x": "missing"}, {"$set": {"x": "created"}}, upsert=True
        )
        assert engine_collection.find_one({"x": "created"}) is not None


class TestDeleteParity:
    def test_delete_one(self, engine_collection):
        engine_collection.insert_many([{"x": 1}, {"x": 1}])
        engine_collection.delete_one({"x": 1})
        assert len(engine_collection.find({"x": 1})) == 1

    def test_delete_many(self, engine_collection):
        engine_collection.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
        engine_collection.delete_many({"x": 1})
        assert engine_collection.find({"x": 1}) == []
        assert len(engine_collection.find({"x": 2})) == 1


class TestCountParity:
    def test_count_documents_empty(self, engine_collection):
        assert engine_collection.count_documents({}) == 0

    def test_count_documents_with_filter(self, engine_collection):
        engine_collection.insert_many([{"x": 1}, {"x": 2}, {"x": 1}])
        assert engine_collection.count_documents({"x": 1}) == 2


class TestIndexParity:
    def test_create_and_list_index(self, engine_collection):
        name = engine_collection.create_index({"field": 1})
        assert isinstance(name, str)
        indexes = engine_collection.list_indexes()
        field_names = [idx.get("name", "") for idx in indexes]
        assert name in field_names

    def test_create_unique_index_enforced(self, engine_collection):
        engine_collection.create_index({"uid": 1}, unique=True)
        engine_collection.insert_one({"uid": "a"})
        with pytest.raises(DuplicateKeyError):
            engine_collection.insert_one({"uid": "a"})


class TestVerifyParity:
    def test_verify_empty(self, engine_collection):
        result = engine_collection.verify()
        assert result["valid"] is True
        assert result["nrecords"] == 0

    def test_verify_after_inserts(self, engine_collection):
        engine_collection.insert_many([{"x": i} for i in range(5)])
        result = engine_collection.verify()
        assert result["valid"] is True
        assert result["nrecords"] == 5


class TestAggregateEngineParity:
    def test_basic_match(self, engine_collection):
        engine_collection.insert_many([{"x": 1}, {"x": 2}, {"x": 3}])
        result = list(engine_collection.aggregate_engine([{"$match": {"x": {"$gt": 1}}}]))
        assert len(result) == 2

    def test_group(self, engine_collection):
        engine_collection.insert_many([
            {"dept": "eng", "salary": 100},
            {"dept": "eng", "salary": 200},
            {"dept": "sales", "salary": 150},
        ])
        result = list(engine_collection.aggregate_engine([
            {"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}}
        ]))
        by_dept = {r["_id"]: r["total"] for r in result}
        assert by_dept["eng"] == 300
        assert by_dept["sales"] == 150


class TestFindAndModifyParity:
    def test_find_one_and_update(self, engine_collection):
        engine_collection.insert_one({"x": 1, "y": "old"})
        before = engine_collection.find_one_and_update(
            {"x": 1}, {"$set": {"y": "new"}}, return_document="before"
        )
        assert before is not None
        assert before["y"] == "old"
        assert engine_collection.find_one({"x": 1})["y"] == "new"

    def test_find_one_and_delete(self, engine_collection):
        engine_collection.insert_one({"x": 1})
        deleted = engine_collection.find_one_and_delete({"x": 1})
        assert deleted is not None
        assert engine_collection.count_documents({}) == 0


class TestStorageStatsParity:
    def test_storage_stats_shape(self, engine_collection):
        engine_collection.insert_many([{"x": i} for i in range(3)])
        stats = engine_collection.storage_stats()
        assert "count" in stats
        assert stats["count"] == 3
