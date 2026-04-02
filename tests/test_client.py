"""Tests for smongo.client facade API."""

import pytest

import smongo.client as client_mod
from smongo.aggregation import Cursor
from smongo.client import Collection, Database, MongoClient


class TestMongoClient:
    def test_local_uri_client(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        assert client.mode == "local"
        assert client.get_local_client() is not None

    def test_local_empty_path_defaults(self, monkeypatch):
        captured = {}

        class FakeLocalClient:
            def __init__(self, db_path, **kwargs):
                captured["db_path"] = db_path

        monkeypatch.setattr(client_mod, "LocalClient", FakeLocalClient)
        client = MongoClient("local://")
        assert client.mode == "local"
        assert captured["db_path"] == "local_wt_data"

    def test_get_db(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        db = client["mydb"]
        assert isinstance(db, Database)

    def test_get_local_client_remote_raises(self):
        c = MongoClient.__new__(MongoClient)
        c.mode = "remote"
        with pytest.raises(RuntimeError, match="local mode"):
            c.get_local_client()


class TestDatabase:
    def test_get_collection(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        db = client["mydb"]
        coll = db["users"]
        assert isinstance(coll, Collection)

    def test_collection_cached(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        db = client["mydb"]
        c1 = db["users"]
        c2 = db["users"]
        assert c1 is c2

    def test_list_collection_names_local_cache(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        db = client["mydb"]
        _ = db["users"]
        _ = db["orders"]
        names = db.list_collection_names()
        assert set(names) == {"users", "orders"}

    def test_create_collection_validator(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
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
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        r = coll.insert_one({"name": "Alice", "age": 30})
        assert len(r.inserted_ids) == 1
        docs = list(coll.find({"name": "Alice"}))
        assert len(docs) == 1
        assert docs[0]["age"] == 30

    def test_find_projection_returns_cursor(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_one({"name": "Alice", "age": 30})
        c = coll.find({}, {"name": 1})
        assert isinstance(c, Cursor)
        doc = c.to_list()[0]
        assert "name" in doc
        assert "age" not in doc

    def test_find_one(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_one({"name": "Bob"})
        doc = coll.find_one({"name": "Bob"})
        assert doc is not None
        assert doc["name"] == "Bob"

    def test_aggregate(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_many([{"city": "NYC"}, {"city": "SF"}, {"city": "NYC"}])
        result = coll.aggregate(
            [{"$group": {"_id": "$city", "count": {"$sum": 1}}}]
        )
        nyc = next(r for r in result if r["_id"] == "NYC")
        assert nyc["count"] == 2

    def test_count_documents(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
        assert coll.count_documents({"x": 1}) == 2

    def test_explain(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        plan = coll.explain({"x": 1})
        assert "plan" in plan

    def test_watch(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        stream = coll.watch()
        coll.insert_one({"name": "watch-me"})
        event = stream.try_next()
        stream.close()
        assert event is not None
        assert event["operationType"] == "insert"

    def test_update_one_and_many(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
        r1 = coll.update_one({"x": 1}, {"$set": {"x": 9}})
        r2 = coll.update_many({"x": 1}, {"$set": {"x": 8}})
        assert r1.modified_count == 1
        assert r2.modified_count >= 0

    def test_delete_one_and_many(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
        r1 = coll.delete_one({"x": 1})
        r2 = coll.delete_many({"x": 1})
        assert r1.deleted_count == 1
        assert r2.deleted_count >= 0

    def test_index_methods(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        name = coll.create_index([("age", 1)])
        assert name == "age_1"
        indexes = coll.list_indexes()
        assert any(i["name"] == "age_1" for i in indexes)
        coll.drop_index("age_1")
        indexes2 = coll.list_indexes()
        assert not any(i["name"] == "age_1" for i in indexes2)

    def test_get_oplog_local(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_one({"x": 1})
        oplog = coll.get_oplog()
        assert len(oplog) >= 1

    def test_get_local_collection(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        local_coll = coll.get_local_collection()
        assert local_coll is not None

    def test_find_one_and_update(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_one({"_id": "fau", "x": 1})
        before = coll.find_one_and_update({"_id": "fau"}, {"$set": {"x": 99}})
        assert before["x"] == 1
        assert coll.find_one({"_id": "fau"})["x"] == 99

    def test_find_one_and_delete(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_one({"_id": "fad", "x": 1})
        deleted = coll.find_one_and_delete({"_id": "fad"})
        assert deleted["x"] == 1
        assert coll.find_one({"_id": "fad"}) is None

    def test_find_one_and_replace(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_one({"_id": "far", "x": 1, "y": 2})
        before = coll.find_one_and_replace({"_id": "far"}, {"x": 99})
        assert before["x"] == 1
        after = coll.find_one({"_id": "far"})
        assert after["x"] == 99
        assert "y" not in after

    def test_bulk_write_mixed_operations(self, tmp_path):
        from smongo.client import DeleteOne, InsertOne, UpdateOne
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        coll.insert_many([{"_id": "bw1", "x": 1}, {"_id": "bw2", "x": 2}])
        result = coll.bulk_write([
            InsertOne({"_id": "bw3", "x": 3}),
            UpdateOne({"_id": "bw1"}, {"$set": {"x": 10}}),
            DeleteOne({"_id": "bw2"}),
        ])
        assert result.inserted_count == 1
        assert result.modified_count == 1
        assert result.deleted_count == 1
        assert coll.find_one({"_id": "bw1"})["x"] == 10
        assert coll.find_one({"_id": "bw2"}) is None
        assert coll.find_one({"_id": "bw3"})["x"] == 3

    def test_bulk_write_upsert(self, tmp_path):
        from smongo.client import ReplaceOne, UpdateOne
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        result = coll.bulk_write([
            UpdateOne({"_id": "up1"}, {"$set": {"x": 1}}, upsert=True),
            ReplaceOne({"_id": "up2"}, {"_id": "up2", "x": 2}, upsert=True),
        ])
        assert result.upserted_count == 2
        assert 0 in result.upserted_ids
        assert coll.find_one({"_id": "up1"})["x"] == 1
        assert coll.find_one({"_id": "up2"})["x"] == 2

    def test_update_one_upsert(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["mydb"]["users"]
        r = coll.update_one({"_id": "new"}, {"$set": {"val": 42}}, upsert=True)
        assert r.upserted_id is not None
        assert coll.find_one({"_id": "new"})["val"] == 42

    def test_database_cached(self, tmp_path):
        client = MongoClient(f"local://{tmp_path}/wt")
        db1 = client["mydb"]
        db2 = client["mydb"]
        assert db1 is db2
