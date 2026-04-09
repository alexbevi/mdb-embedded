"""Remote-mode branch coverage for smongo.client."""

import types

import pytest

import smongo.client as client_mod
from smongo.client import Collection, MongoClient


class FakeRemoteCollection:
    def __init__(self):
        self.docs = [{"_id": "1", "x": 1}]

    def find(self, query=None, projection=None):
        return list(self.docs)

    def find_one(self, query=None, projection=None):
        return self.docs[0] if self.docs else None

    def aggregate(self, pipeline):
        return iter([{"ok": True}])

    def count_documents(self, query=None):
        return len(self.docs)

    def watch(self, pipeline=None):
        return iter([])

    def insert_one(self, doc):
        self.docs.append(doc)
        return types.SimpleNamespace(inserted_id=doc.get("_id"))

    def insert_many(self, docs):
        self.docs.extend(docs)
        return types.SimpleNamespace(inserted_ids=[d.get("_id") for d in docs])

    def update_one(self, query, update, upsert=False):
        return types.SimpleNamespace(modified_count=1, upserted_id=None)

    def update_many(self, query, update, upsert=False):
        return types.SimpleNamespace(modified_count=1, upserted_id=None)

    def delete_one(self, query):
        return types.SimpleNamespace(deleted_count=1)

    def delete_many(self, query):
        return types.SimpleNamespace(deleted_count=1)

    def create_index(self, keys, **kwargs):
        return "idx_1"

    def drop_index(self, name):
        return None

    def list_indexes(self):
        return [{"name": "_id_"}]


class FakeRemoteDB:
    def __init__(self):
        self._colls = {"users": FakeRemoteCollection()}

    def __getitem__(self, name):
        if name not in self._colls:
            self._colls[name] = FakeRemoteCollection()
        return self._colls[name]

    def create_collection(self, name, **kwargs):
        self._colls[name] = FakeRemoteCollection()

    def list_collection_names(self):
        return list(self._colls.keys())


class FakePyMongoClient:
    def __init__(self, uri):
        self.uri = uri
        self._dbs = {}

    def __getitem__(self, name):
        if name not in self._dbs:
            self._dbs[name] = FakeRemoteDB()
        return self._dbs[name]


def test_remote_mongo_client_and_db(monkeypatch):
    monkeypatch.setattr(client_mod, "_PyMongoClient", FakePyMongoClient)
    client = MongoClient("mongodb://example")
    assert client.mode == "remote"
    db = client["appdb"]
    assert db.list_collection_names() == ["users"]


def test_remote_import_error(monkeypatch):
    monkeypatch.setattr(client_mod, "_PyMongoClient", None)
    with pytest.raises(ImportError, match="pymongo required"):
        MongoClient("mongodb://example")


def test_remote_collection_delegates(monkeypatch):
    monkeypatch.setattr(client_mod, "_PyMongoClient", FakePyMongoClient)
    coll = MongoClient("mongodb://example")["appdb"]["users"]
    assert isinstance(coll, Collection)
    assert coll.find({}) == [{"_id": "1", "x": 1}]
    assert coll.find_one({}) == {"_id": "1", "x": 1}
    assert coll.aggregate([]) == [{"ok": True}]
    assert coll.count_documents({}) == 1
    assert coll.explain({}) == {"plan": "remote"}
    assert list(coll.watch([])) == []
    assert coll.insert_one({"_id": "2", "x": 2}).inserted_id == "2"
    assert coll.insert_many([{"_id": "3"}]).inserted_ids == ["3"]
    assert coll.update_one({}, {"$set": {"x": 3}}).modified_count == 1
    assert coll.update_many({}, {"$set": {"x": 3}}).modified_count == 1
    assert coll.delete_one({}).deleted_count == 1
    assert coll.delete_many({}).deleted_count == 1
    assert coll.create_index([("x", 1)]) == "idx_1"
    coll.drop_index("idx_1")
    assert coll.list_indexes() == [{"name": "_id_"}]
    assert coll.get_oplog() == []


def test_remote_get_local_collection_raises(monkeypatch):
    monkeypatch.setattr(client_mod, "_PyMongoClient", FakePyMongoClient)
    coll = MongoClient("mongodb://example")["appdb"]["users"]
    with pytest.raises(RuntimeError):
        coll.get_local_collection()


def test_remote_create_collection(monkeypatch):
    monkeypatch.setattr(client_mod, "_PyMongoClient", FakePyMongoClient)
    db = MongoClient("mongodb://example")["appdb"]
    coll = db.create_collection("orders")
    assert coll is not None
