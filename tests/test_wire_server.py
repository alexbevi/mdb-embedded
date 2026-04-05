"""Integration test -- start a real WireServer and connect with pymongo."""

import socket
import threading
import time

import pytest
from pymongo import MongoClient as PyMongoClient
from pymongo.errors import (
    ConnectionFailure,
    DuplicateKeyError,
    OperationFailure,
    ServerSelectionTimeoutError,
)

from smongo.wire.server import WireServer


def _find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"Port {port} not open after {timeout}s")


@pytest.fixture(scope="class")
def wire_env(tmp_path_factory):
    """Start a fresh WireServer for each test class."""
    db_path = str(tmp_path_factory.mktemp("wire_wt"))
    port = _find_free_port()
    server = WireServer(db_path, host="127.0.0.1", port=port)
    server.start()
    _wait_for_port("127.0.0.1", port)
    yield server, port
    server.stop()
    time.sleep(0.3)


_coll_counter = 0


@pytest.fixture
def client(wire_env):
    server, port = wire_env
    uri = f"mongodb://127.0.0.1:{port}/?directConnection=true"
    c = PyMongoClient(uri, serverSelectionTimeoutMS=5000)
    yield c
    c.close()


@pytest.fixture
def db(client):
    return client["wiretest"]


@pytest.fixture
def coll(db):
    global _coll_counter
    _coll_counter += 1
    return db[f"items_{_coll_counter}"]


class TestPymongoCRUD:
    def test_insert_and_find(self, coll):
        coll.insert_one({"name": "Alice", "age": 30})
        docs = list(coll.find({}))
        assert len(docs) == 1
        assert docs[0]["name"] == "Alice"

    def test_insert_many_and_count(self, coll):
        coll.insert_many([{"v": i} for i in range(5)])
        assert coll.count_documents({}) == 5

    def test_find_with_filter(self, coll):
        coll.insert_many([{"x": 1}, {"x": 2}, {"x": 3}])
        docs = list(coll.find({"x": {"$gt": 1}}))
        assert len(docs) == 2

    def test_update_one(self, coll):
        coll.insert_one({"name": "Bob", "score": 10})
        result = coll.update_one({"name": "Bob"}, {"$set": {"score": 99}})
        assert result.modified_count == 1
        doc = coll.find_one({"name": "Bob"})
        assert doc["score"] == 99

    def test_delete_one(self, coll):
        coll.insert_many([{"a": 1}, {"a": 2}])
        result = coll.delete_one({"a": 1})
        assert result.deleted_count == 1
        assert coll.count_documents({}) == 1

    def test_delete_many(self, coll):
        coll.insert_many([{"a": 1}, {"a": 1}, {"a": 2}])
        result = coll.delete_many({"a": 1})
        assert result.deleted_count == 2

    def test_replace_one(self, coll):
        coll.insert_one({"name": "orig", "val": 1})
        result = coll.replace_one({"name": "orig"}, {"name": "replaced", "val": 99})
        assert result.modified_count == 1
        doc = coll.find_one({"name": "replaced"})
        assert doc["val"] == 99

    def test_update_many(self, coll):
        coll.insert_many([{"g": "a", "v": 1}, {"g": "a", "v": 2}, {"g": "b", "v": 3}])
        result = coll.update_many({"g": "a"}, {"$inc": {"v": 10}})
        assert result.modified_count == 2


class TestPymongoFindAdvanced:
    def test_find_with_sort(self, coll):
        coll.insert_many([{"v": 3}, {"v": 1}, {"v": 2}])
        docs = list(coll.find({}).sort("v", 1))
        assert [d["v"] for d in docs] == [1, 2, 3]

    def test_find_with_skip_and_limit(self, coll):
        coll.insert_many([{"v": i} for i in range(10)])
        docs = list(coll.find({}).sort("v", 1).skip(3).limit(2))
        assert len(docs) == 2
        assert docs[0]["v"] == 3
        assert docs[1]["v"] == 4

    def test_find_with_projection(self, coll):
        coll.insert_one({"a": 1, "b": 2, "c": 3})
        doc = coll.find_one({}, {"a": 1, "_id": 0})
        assert "a" in doc
        assert "b" not in doc

    def test_distinct(self, coll):
        coll.insert_many([{"c": "x"}, {"c": "y"}, {"c": "x"}])
        vals = coll.distinct("c")
        assert sorted(vals) == ["x", "y"]

    def test_large_cursor_iteration(self, coll):
        coll.insert_many([{"i": n} for n in range(200)])
        docs = list(coll.find({}))
        assert len(docs) == 200


class TestPymongoFindAndModify:
    def test_find_one_and_update(self, coll):
        coll.insert_one({"name": "test", "v": 1})
        doc = coll.find_one_and_update({"name": "test"}, {"$set": {"v": 42}}, return_document=True)
        assert doc["v"] == 42

    def test_find_one_and_replace(self, coll):
        coll.insert_one({"name": "old", "val": 1})
        doc = coll.find_one_and_replace(
            {"name": "old"}, {"name": "new", "val": 99}, return_document=True
        )
        assert doc["name"] == "new"
        assert doc["val"] == 99

    def test_find_one_and_delete(self, coll):
        coll.insert_one({"x": 1})
        doc = coll.find_one_and_delete({"x": 1})
        assert doc["x"] == 1
        assert coll.count_documents({}) == 0


class TestPymongoAggregation:
    def test_aggregate_pipeline(self, coll):
        coll.insert_many(
            [
                {"dept": "eng", "sal": 100},
                {"dept": "eng", "sal": 200},
                {"dept": "hr", "sal": 150},
            ]
        )
        result = list(
            coll.aggregate(
                [
                    {"$match": {"dept": "eng"}},
                    {"$group": {"_id": "$dept", "total": {"$sum": "$sal"}}},
                ]
            )
        )
        assert len(result) == 1
        assert result[0]["total"] == 300


class TestPymongoIndexes:
    def test_create_and_list_indexes(self, coll):
        coll.create_index([("name", 1)], name="name_1")
        indexes = list(coll.list_indexes())
        names = [idx["name"] for idx in indexes]
        assert "_id_" in names
        assert "name_1" in names

    def test_drop_index(self, coll):
        coll.create_index([("x", 1)], name="x_1")
        coll.drop_index("x_1")
        names = [idx["name"] for idx in coll.list_indexes()]
        assert "x_1" not in names


class TestPymongoAdmin:
    def test_ping(self, client):
        resp = client.admin.command("ping")
        assert resp.get("ok") == 1.0

    def test_list_databases(self, client):
        client["somedb"]["somecoll"].insert_one({"x": 1})
        dbs = client.list_database_names()
        assert isinstance(dbs, list)

    def test_drop_database(self, client):
        db = client["todrop"]
        db["c1"].insert_one({"x": 1})
        client.drop_database("todrop")

    def test_drop_collection(self, db):
        db["dropcol"].insert_one({"y": 1})
        db.drop_collection("dropcol")

    def test_build_info(self, client):
        resp = client.admin.command("buildInfo")
        assert resp["ok"] == 1.0
        assert "version" in resp

    def test_server_status(self, client):
        resp = client.admin.command("serverStatus")
        assert resp["ok"] == 1.0
        assert "uptime" in resp


class TestPymongoErrors:
    def test_duplicate_key_error(self, coll):
        coll.create_index([("uid", 1)], unique=True, name="uid_uniq")
        coll.insert_one({"uid": "abc"})
        with pytest.raises(DuplicateKeyError):
            coll.insert_one({"uid": "abc"})


class TestPymongoConcurrent:
    def test_concurrent_connections(self, wire_env):
        """Multiple pymongo clients can connect and operate simultaneously."""
        server, port = wire_env
        uri = f"mongodb://127.0.0.1:{port}/?directConnection=true"
        results = []
        errors = []

        def worker(n):
            try:
                c = PyMongoClient(uri, serverSelectionTimeoutMS=5000)
                db = c["conctest"]
                coll = db[f"coll_{n}"]
                coll.insert_one({"worker": n})
                count = coll.count_documents({})
                results.append(count)
                c.close()
            except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError, OSError) as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent errors: {errors}"
        assert len(results) == 5
        assert all(r >= 1 for r in results)
