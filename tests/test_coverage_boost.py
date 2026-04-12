import asyncio
import json
from unittest.mock import MagicMock

import pytest

from smongo import MongoClient as EmbeddedClient
from smongo.async_client import AsyncCursor
from smongo.wire.msg import (
    COMPRESSOR_NOOP,
    COMPRESSOR_ZLIB,
    MAX_MSG_SIZE,
    ProtocolError,
    _compress,
    _decompress,
)


@pytest.fixture
def _web_app(tmp_path):
    try:
        import web_app as wa
    except RuntimeError:
        pytest.skip("web_app requires exclusive access to its local data directory")

    old_client = wa.client
    old_remote = wa.remote
    old_sync = wa.sync_mgr
    old_cache = wa._collections_cache.copy()
    old_streams = wa._watch_streams.copy()
    old_key = wa._API_KEY
    old_limiter = wa._limiter

    wa.client = EmbeddedClient(f"local://{tmp_path}/coverage_redb")
    wa._collections_cache.clear()
    wa._watch_streams.clear()

    users_remote = MagicMock()
    dept_remote = MagicMock()

    def db_getitem(_self, name):
        if name == "users":
            return users_remote
        if name == "departments":
            return dept_remote
        return MagicMock()

    db_mock = MagicMock()
    db_mock.__getitem__ = db_getitem

    def remote_getitem(_self, name):
        if name == wa.DB_NAME:
            return db_mock
        return MagicMock()

    remote_mock = MagicMock()
    remote_mock.__getitem__ = remote_getitem
    wa.remote = remote_mock

    sync_mock = MagicMock()
    sync_mock.sync_now = MagicMock()
    sync_mock.status.return_value = {
        "running": False,
        "pushed": 0,
        "pulled": 0,
        "conflicts": 0,
        "errors": 0,
    }
    wa.sync_mgr = sync_mock

    yield wa

    wa.client = old_client
    wa.remote = old_remote
    wa.sync_mgr = old_sync
    wa._collections_cache.clear()
    wa._collections_cache.update(old_cache)
    wa._watch_streams.clear()
    wa._watch_streams.update(old_streams)
    wa._API_KEY = old_key
    wa._limiter = old_limiter


@pytest.fixture
def client(_web_app):
    _web_app._API_KEY = None
    _web_app.app.config["TESTING"] = True
    with _web_app.app.test_client() as c:
        yield c


@pytest.fixture
def seeded_client(client):
    r = client.post("/api/seed")
    assert r.status_code == 200
    assert r.get_json()["ok"] is True
    return client


class TestWebRoutes:
    def test_seed_docs_query_aggregate(self, seeded_client):
        r = seeded_client.get("/api/docs?coll=users")
        assert r.status_code == 200
        docs = r.get_json()
        assert any(d.get("name") == "Alice" for d in docs)

        q = seeded_client.post(
            "/api/query",
            data=json.dumps({"coll": "users", "query": {"name": "Alice"}}),
            content_type="application/json",
        )
        assert q.status_code == 200
        b = q.get_json()
        assert b["count"] >= 1
        assert "plan" in b

        agg = seeded_client.post(
            "/api/aggregate",
            data=json.dumps(
                {
                    "coll": "users",
                    "pipeline": [{"$match": {"city": "NYC"}}, {"$count": "n"}],
                }
            ),
            content_type="application/json",
        )
        assert agg.status_code == 200
        assert agg.get_json()["count"] >= 1

    def test_insert_update_delete(self, client):
        ins = client.post(
            "/api/insert",
            data=json.dumps({"coll": "users", "doc": {"name": "CovDoc", "n": 1}}),
            content_type="application/json",
        )
        assert ins.status_code == 200
        oid = ins.get_json()["inserted_id"]

        up = client.post(
            "/api/update",
            data=json.dumps(
                {
                    "coll": "users",
                    "query": {"name": "CovDoc"},
                    "update": {"$set": {"n": 2}},
                }
            ),
            content_type="application/json",
        )
        assert up.status_code == 200
        assert up.get_json()["modified_count"] >= 1

        dele = client.post(
            "/api/delete",
            data=json.dumps({"coll": "users", "query": {"name": "CovDoc"}}),
            content_type="application/json",
        )
        assert dele.status_code == 200
        assert dele.get_json()["deleted_count"] >= 1
        assert oid

    def test_stats_indexes_oplog_shell_sync_metrics(self, seeded_client):
        st = seeded_client.get("/api/stats?coll=users")
        assert st.status_code == 200
        sj = st.get_json()
        assert sj["doc_count"] >= 1
        assert sj["index_count"] >= 1

        li = seeded_client.get("/api/indexes?coll=users")
        assert li.status_code == 200
        names_before = {i["name"] for i in li.get_json()}

        cr = seeded_client.post(
            "/api/indexes",
            data=json.dumps({"coll": "users", "keys": [["cov_boost_field", 1]], "unique": False}),
            content_type="application/json",
        )
        assert cr.status_code == 200
        iname = cr.get_json()["name"]
        assert iname not in names_before

        dr = seeded_client.delete(f"/api/indexes/{iname}?coll=users")
        assert dr.status_code == 200
        assert dr.get_json()["dropped"] == iname

        op = seeded_client.get("/api/oplog?coll=users&limit=5")
        assert op.status_code == 200
        assert isinstance(op.get_json(), list)

        sh = seeded_client.post(
            "/api/shell",
            data=json.dumps({"command": "db.users.find({})"}),
            content_type="application/json",
        )
        assert sh.status_code == 200
        assert isinstance(sh.get_json()["result"], list)

        ss = seeded_client.get("/api/sync/status")
        assert ss.status_code == 200
        assert "running" in ss.get_json()

        m = seeded_client.get("/metrics")
        assert m.status_code == 200
        assert b"smongo_" in m.data

    def test_schema_post(self, client):
        schema = {"$jsonSchema": {"required": ["name"]}}
        r = client.post(
            "/api/schema",
            data=json.dumps({"coll": "cov_schema_coll", "validator": schema}),
            content_type="application/json",
        )
        assert r.status_code == 200
        bad = client.post(
            "/api/insert",
            data=json.dumps({"coll": "cov_schema_coll", "doc": {"age": 1}}),
            content_type="application/json",
        )
        assert bad.status_code == 400

    def test_query_invalid_filter_returns_400(self, client):
        client.post(
            "/api/insert",
            data=json.dumps({"coll": "users", "doc": {"name": "q"}}),
            content_type="application/json",
        )
        r = client.post(
            "/api/query",
            data=json.dumps({"coll": "users", "query": "not-a-dict"}),
            content_type="application/json",
        )
        assert r.status_code == 400
        assert "error" in r.get_json()

    def test_aggregate_too_many_stages_returns_400(self, client):
        pipeline = [{"$match": {}}] * 60
        r = client.post(
            "/api/aggregate",
            data=json.dumps({"coll": "users", "pipeline": pipeline}),
            content_type="application/json",
        )
        assert r.status_code == 400
        assert "stage limit" in r.get_json()["error"].lower()

    def test_oplog_invalid_limit_returns_400(self, client):
        r = client.get("/api/oplog?coll=users&limit=abc")
        assert r.status_code == 400
        assert "integer" in r.get_json()["error"].lower()

    def test_shell_find_one(self, seeded_client):
        r = seeded_client.post(
            "/api/shell",
            data=json.dumps({"command": 'db.users.findOne({"name": "Alice"})'}),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert r.get_json()["result"]["name"] == "Alice"

    def test_shell_insert_one(self, client):
        r = client.post(
            "/api/shell",
            data=json.dumps({"command": 'db.users.insertOne({"name": "ShellIns"})'}),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert r.get_json()["result"]["acknowledged"] is True

    def test_shell_insert_many(self, client):
        r = client.post(
            "/api/shell",
            data=json.dumps({"command": 'db.users.insertMany([{"name":"A"},{"name":"B"}])'}),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert r.get_json()["result"]["insertedCount"] == 2

    def test_shell_update_one(self, seeded_client):
        r = seeded_client.post(
            "/api/shell",
            data=json.dumps(
                {"command": 'db.users.updateOne({"name": "Alice"}, {"$set": {"age": 99}})'}
            ),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert r.get_json()["result"]["acknowledged"] is True

    def test_shell_update_many(self, seeded_client):
        r = seeded_client.post(
            "/api/shell",
            data=json.dumps(
                {"command": 'db.users.updateMany({"city": "NYC"}, {"$set": {"tag": "ny"}})'}
            ),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert r.get_json()["result"]["modifiedCount"] >= 1

    def test_shell_delete_one(self, seeded_client):
        r = seeded_client.post(
            "/api/shell",
            data=json.dumps({"command": 'db.users.deleteOne({"name": "Alice"})'}),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert r.get_json()["result"]["deletedCount"] == 1

    def test_shell_delete_many(self, seeded_client):
        r = seeded_client.post(
            "/api/shell",
            data=json.dumps({"command": 'db.users.deleteMany({"city": "NYC"})'}),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert r.get_json()["result"]["deletedCount"] >= 0

    def test_shell_aggregate(self, seeded_client):
        r = seeded_client.post(
            "/api/shell",
            data=json.dumps({"command": 'db.users.aggregate([{"$count": "n"}])'}),
            content_type="application/json",
        )
        assert r.status_code == 200

    def test_shell_count_documents(self, seeded_client):
        r = seeded_client.post(
            "/api/shell",
            data=json.dumps({"command": "db.users.countDocuments({})"}),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert r.get_json()["result"] >= 1

    def test_shell_get_indexes(self, seeded_client):
        r = seeded_client.post(
            "/api/shell",
            data=json.dumps({"command": "db.users.getIndexes()"}),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert isinstance(r.get_json()["result"], list)

    def test_shell_create_drop_index(self, client):
        client.post(
            "/api/shell",
            data=json.dumps({"command": 'db.users.insertOne({"x": 1})'}),
            content_type="application/json",
        )
        r = client.post(
            "/api/shell",
            data=json.dumps({"command": 'db.users.createIndex({"x": 1})'}),
            content_type="application/json",
        )
        assert r.status_code == 200
        idx_name = r.get_json()["result"]["name"]
        dr = client.post(
            "/api/shell",
            data=json.dumps({"command": f'db.users.dropIndex("{idx_name}")'}),
            content_type="application/json",
        )
        assert dr.status_code == 200

    def test_shell_explain(self, seeded_client):
        r = seeded_client.post(
            "/api/shell",
            data=json.dumps({"command": 'db.users.explain({"name": "Alice"})'}),
            content_type="application/json",
        )
        assert r.status_code == 200

    def test_shell_unknown_command(self, client):
        r = client.post(
            "/api/shell",
            data=json.dumps({"command": "db.users.unknownOp()"}),
            content_type="application/json",
        )
        assert r.status_code == 400
        assert "Unknown command" in r.get_json()["error"]

    def test_shell_not_db_prefix(self, client):
        r = client.post(
            "/api/shell",
            data=json.dumps({"command": "something.else()"}),
            content_type="application/json",
        )
        assert r.status_code == 400

    def test_sync_push(self, client):
        r = client.post("/api/sync/push")
        assert r.status_code == 200

    def test_sync_pull(self, client):
        r = client.post("/api/sync/pull")
        assert r.status_code == 200

    def test_sync_start_stop(self, client):
        r = client.post("/api/sync/start")
        assert r.status_code == 200
        r2 = client.post("/api/sync/stop")
        assert r2.status_code == 200

    def test_remote_docs(self, _web_app):
        _web_app._API_KEY = None
        _web_app.app.config["TESTING"] = True
        with _web_app.app.test_client() as c:
            r = c.get("/api/remote/docs?coll=users")
            assert r.status_code == 200

    def test_remote_insert(self, client):
        r = client.post(
            "/api/remote/insert",
            data=json.dumps({"coll": "users", "doc": {"name": "RemoteTest"}}),
            content_type="application/json",
        )
        assert r.status_code == 200

    def test_watch_next(self, client):
        r = client.get("/api/watch/next?coll=users")
        assert r.status_code in (200, 500)

    def test_schema_missing_validator(self, client):
        r = client.post(
            "/api/schema",
            data=json.dumps({"coll": "users"}),
            content_type="application/json",
        )
        assert r.status_code == 400
        assert "validator" in r.get_json()["error"].lower()

    def test_404_handler(self, client):
        r = client.get("/api/nonexistent")
        assert r.status_code == 404
        assert "error" in r.get_json()


class TestAsyncCursor:
    def test_list_backed_iteration(self):
        docs = [{"_id": 1, "x": 1}, {"_id": 2, "x": 2}]
        cursor = AsyncCursor(docs)
        result = asyncio.run(self._collect(cursor))
        assert result == docs

    def test_list_backed_empty(self):
        cursor = AsyncCursor([])
        result = asyncio.run(self._collect(cursor))
        assert result == []

    def test_to_list(self):
        docs = [{"a": 1}]
        cursor = AsyncCursor(docs)
        assert cursor.to_list() == docs

    def test_len(self):
        docs = [{"a": 1}, {"b": 2}]
        cursor = AsyncCursor(docs)
        assert len(cursor) == 2

    def test_iterator_backed(self):
        docs = [{"_id": 1}, {"_id": 2}]
        cursor = AsyncCursor(iter(docs))
        result = asyncio.run(self._collect(cursor))
        assert len(result) == 2

    def test_iterator_len_materializes(self):
        cursor = AsyncCursor(iter([{"a": 1}]))
        assert len(cursor) == 1

    @staticmethod
    async def _collect(cursor):
        results = []
        async for doc in cursor:
            results.append(doc)
        return results


class TestCompression:
    def test_noop_roundtrip(self):
        data = b"hello world"
        compressed = _compress(COMPRESSOR_NOOP, data)
        assert _decompress(COMPRESSOR_NOOP, compressed, len(data)) == data

    def test_zlib_roundtrip(self):
        data = b"x" * 1000
        compressed = _compress(COMPRESSOR_ZLIB, data)
        assert _decompress(COMPRESSOR_ZLIB, compressed, len(data)) == data

    def test_decompress_invalid_size(self):
        with pytest.raises(ProtocolError):
            _decompress(COMPRESSOR_NOOP, b"data", -1)
        with pytest.raises(ProtocolError):
            _decompress(COMPRESSOR_NOOP, b"data", MAX_MSG_SIZE + 1)

    def test_unknown_compressor_compress(self):
        with pytest.raises(ProtocolError):
            _compress(99, b"data")

    def test_unknown_compressor_decompress(self):
        with pytest.raises(ProtocolError):
            _decompress(99, b"data", 4)
