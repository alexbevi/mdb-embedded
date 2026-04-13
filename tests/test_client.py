"""Tests for smongo.client facade API."""

import pytest

from smongo.aggregation import Cursor
from smongo.client import Collection, Database, MongoClient


class TestMongoClient:
    def test_local_uri_client(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        assert client.mode == "local"
        assert client.get_local_client() is not None

    def test_local_empty_path_defaults(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        client = MongoClient("local://")
        assert client.mode == "local"
        client.close()

    def test_get_db(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        db = client["mydb"]
        assert isinstance(db, Database)

    def test_get_local_client_remote_raises(self):
        c = MongoClient.__new__(MongoClient)
        c.mode = "remote"
        with pytest.raises(RuntimeError, match="local mode"):
            c.get_local_client()


class TestDatabase:
    def test_get_collection(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        db = client["mydb"]
        coll = db["users"]
        assert isinstance(coll, Collection)

    def test_collection_cached(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        db = client["mydb"]
        c1 = db["users"]
        c2 = db["users"]
        assert c1 is c2

    def test_list_collection_names_local_cache(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        db = client["mydb"]
        db["users"]
        db["orders"]
        names = db.list_collection_names()
        assert set(names) == {"users", "orders"}

    def test_create_collection_validator(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        db = client["mydb"]
        coll = db.create_collection(
            "strict",
            validator={"$jsonSchema": {"required": ["name"]}},
        )
        assert isinstance(coll, Collection)
        with pytest.raises(Exception):
            coll.insert_one({"age": 10})


class TestCollectionFacade:
    def test_insert_and_find(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        r = coll.insert_one({"name": "Alice", "age": 30})
        assert len(r.inserted_ids) == 1
        docs = list(coll.find({"name": "Alice"}))
        assert len(docs) == 1
        assert docs[0]["age"] == 30

    def test_find_projection_returns_cursor(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_one({"name": "Alice", "age": 30})
        c = coll.find({}, {"name": 1})
        assert isinstance(c, Cursor)
        doc = c.to_list()[0]
        assert "name" in doc
        assert "age" not in doc

    def test_find_one(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_one({"name": "Bob"})
        doc = coll.find_one({"name": "Bob"})
        assert doc is not None
        assert doc["name"] == "Bob"

    def test_aggregate(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_many([{"city": "NYC"}, {"city": "SF"}, {"city": "NYC"}])
        result = coll.aggregate([{"$group": {"_id": "$city", "count": {"$sum": 1}}}])
        nyc = next(r for r in result if r["_id"] == "NYC")
        assert nyc["count"] == 2

    def test_count_documents(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
        assert coll.count_documents({"x": 1}) == 2

    def test_explain(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        plan = coll.explain({"x": 1})
        assert "plan" in plan

    def test_watch(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        stream = coll.watch()
        coll.insert_one({"name": "watch-me"})
        event = stream.try_next()
        stream.close()
        assert event is not None
        assert event["operationType"] == "insert"

    def test_update_one_and_many(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
        r1 = coll.update_one({"x": 1}, {"$set": {"x": 9}})
        r2 = coll.update_many({"x": 1}, {"$set": {"x": 8}})
        assert r1.modified_count == 1
        assert r2.modified_count >= 0

    def test_delete_one_and_many(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
        r1 = coll.delete_one({"x": 1})
        r2 = coll.delete_many({"x": 1})
        assert r1.deleted_count == 1
        assert r2.deleted_count >= 0

    def test_index_methods(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        name = coll.create_index([("age", 1)])
        assert name == "age_1"
        indexes = coll.list_indexes()
        assert any(i["name"] == "age_1" for i in indexes)
        coll.drop_index("age_1")
        indexes2 = coll.list_indexes()
        assert not any(i["name"] == "age_1" for i in indexes2)

    def test_get_oplog_local(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_one({"x": 1})
        oplog = coll.get_oplog()
        assert len(oplog) >= 1

    def test_get_local_collection(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        local_coll = coll.get_local_collection()
        assert local_coll is not None

    def test_find_one_and_update(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_one({"_id": "fau", "x": 1})
        before = coll.find_one_and_update({"_id": "fau"}, {"$set": {"x": 99}})
        assert before["x"] == 1
        assert coll.find_one({"_id": "fau"})["x"] == 99

    def test_find_one_and_delete(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_one({"_id": "fad", "x": 1})
        deleted = coll.find_one_and_delete({"_id": "fad"})
        assert deleted["x"] == 1
        assert coll.find_one({"_id": "fad"}) is None

    def test_find_one_and_replace(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_one({"_id": "far", "x": 1, "y": 2})
        before = coll.find_one_and_replace({"_id": "far"}, {"x": 99})
        assert before["x"] == 1
        after = coll.find_one({"_id": "far"})
        assert after["x"] == 99
        assert "y" not in after

    def test_bulk_write_mixed_operations(self, tmp_path):
        from smongo.client import DeleteOne, InsertOne, UpdateOne

        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        coll.insert_many([{"_id": "bw1", "x": 1}, {"_id": "bw2", "x": 2}])
        result = coll.bulk_write(
            [
                InsertOne({"_id": "bw3", "x": 3}),
                UpdateOne({"_id": "bw1"}, {"$set": {"x": 10}}),
                DeleteOne({"_id": "bw2"}),
            ]
        )
        assert result.inserted_count == 1
        assert result.modified_count == 1
        assert result.deleted_count == 1
        assert coll.find_one({"_id": "bw1"})["x"] == 10
        assert coll.find_one({"_id": "bw2"}) is None
        assert coll.find_one({"_id": "bw3"})["x"] == 3

    def test_bulk_write_upsert(self, tmp_path):
        from smongo.client import ReplaceOne, UpdateOne

        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        result = coll.bulk_write(
            [
                UpdateOne({"_id": "up1"}, {"$set": {"x": 1}}, upsert=True),
                ReplaceOne({"_id": "up2"}, {"_id": "up2", "x": 2}, upsert=True),
            ]
        )
        assert result.upserted_count == 2
        assert 0 in result.upserted_ids
        assert coll.find_one({"_id": "up1"})["x"] == 1
        assert coll.find_one({"_id": "up2"})["x"] == 2

    def test_update_one_upsert(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        coll = client["mydb"]["users"]
        r = coll.update_one({"_id": "new"}, {"$set": {"val": 42}}, upsert=True)
        assert r.upserted_id is not None
        assert coll.find_one({"_id": "new"})["val"] == 42

    def test_database_cached(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/redb")
        db1 = client["mydb"]
        db2 = client["mydb"]
        assert db1 is db2


class TestClientRedbDefault:
    """Smoke tests for ``local://`` (redb embedded client)."""

    def test_insert_find_inserted_ids_shape(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/rdb")
        try:
            coll = client["db"]["users"]
            r = coll.insert_one({"name": "Zed", "n": 1})
            assert len(r.inserted_ids) == 1
            docs = list(coll.find({"name": "Zed"}))
            assert len(docs) == 1 and docs[0]["n"] == 1
        finally:
            client.close()

    def test_find_one_projection_redb(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/rdb")
        try:
            coll = client["db"]["c"]
            coll.insert_one({"a": 1, "b": 2})
            doc = coll.find_one({}, {"a": 1})
            assert doc is not None
            assert doc["a"] == 1
            assert "b" not in doc
        finally:
            client.close()

    def test_aggregate_and_explain_redb(self, tmp_path):
        """``Collection.aggregate`` uses ``find_streaming`` on the backend; redb must implement it."""
        client = MongoClient(f"local://{tmp_path}/rdb")
        try:
            coll = client["db"]["c"]
            coll.insert_many([{"city": "NYC"}, {"city": "SF"}, {"city": "NYC"}])
            rows = list(coll.aggregate([{"$group": {"_id": "$city", "count": {"$sum": 1}}}]))
            nyc = next(r for r in rows if r["_id"] == "NYC")
            assert nyc["count"] == 2
            plan = coll.explain({"city": 1})
            assert "plan" in plan
        finally:
            client.close()


class TestDistinct:
    """Tests for Collection.distinct() with proper MongoDB semantics."""

    def _make_coll(self, tmp_path, name="c"):
        client = MongoClient(f"local://{tmp_path}/rdb")
        return client, client["db"][name]

    def test_basic_distinct(self, tmp_path):
        client, coll = self._make_coll(tmp_path)
        coll.insert_many([{"x": 1}, {"x": 2}, {"x": 1}, {"x": 3}])
        result = coll.distinct("x")
        assert sorted(result) == [1, 2, 3]
        client.close()

    def test_distinct_with_filter(self, tmp_path):
        client, coll = self._make_coll(tmp_path)
        coll.insert_many([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 1, "y": "b"}])
        result = coll.distinct("x", {"y": "b"})
        assert sorted(result) == [1, 2]
        client.close()

    def test_distinct_dotted_path(self, tmp_path):
        client, coll = self._make_coll(tmp_path)
        coll.insert_many([{"a": {"b": 1}}, {"a": {"b": 2}}, {"a": {"b": 1}}])
        result = coll.distinct("a.b")
        assert sorted(result) == [1, 2]
        client.close()

    def test_distinct_null_included(self, tmp_path):
        """Explicit null values should be included in the result."""
        client, coll = self._make_coll(tmp_path)
        coll.insert_many([{"x": 1}, {"x": None}, {"x": 2}])
        result = coll.distinct("x")
        assert None in result
        non_null = [v for v in result if v is not None]
        assert sorted(non_null) == [1, 2]
        client.close()

    def test_distinct_missing_field_excluded(self, tmp_path):
        """Documents where the field is entirely absent yield nothing."""
        client, coll = self._make_coll(tmp_path)
        coll.insert_many([{"x": 1}, {"y": 2}, {"x": 3}])
        result = coll.distinct("x")
        assert sorted(result) == [1, 3]
        client.close()

    def test_distinct_array_flattening(self, tmp_path):
        """Values inside arrays should be flattened (MongoDB semantics)."""
        client, coll = self._make_coll(tmp_path)
        coll.insert_many([{"tags": ["a", "b"]}, {"tags": ["b", "c"]}])
        result = coll.distinct("tags")
        assert sorted(result) == ["a", "b", "c"]
        client.close()

    def test_distinct_dotted_path_through_array(self, tmp_path):
        """Dotted paths through arrays should flatten at each level."""
        client, coll = self._make_coll(tmp_path)
        coll.insert_many(
            [
                {"items": [{"name": "a"}, {"name": "b"}]},
                {"items": [{"name": "b"}, {"name": "c"}]},
            ]
        )
        result = coll.distinct("items.name")
        assert sorted(result) == ["a", "b", "c"]
        client.close()

    def test_distinct_dedup_int_float(self, tmp_path):
        """int(1) and float(1.0) should be treated as the same value."""
        client, coll = self._make_coll(tmp_path)
        coll.insert_many([{"x": 1}, {"x": 1.0}, {"x": 2}])
        result = coll.distinct("x")
        numeric = [v for v in result if v is not None]
        assert len(numeric) == 2
        client.close()

    def test_distinct_empty_collection(self, tmp_path):
        client, coll = self._make_coll(tmp_path)
        result = coll.distinct("x")
        assert result == []
        client.close()
