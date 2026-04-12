"""Tests for wire command Python fallback handlers: crud, admin, aggregation.

Calls handler functions DIRECTLY (bypassing Rust rs_dispatch) with monkeypatch
on ConnectionContext class to inject mock collections.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from smongo._smongo_core import ConnectionContext, RedbLocalClient
from smongo.storage.results import DeleteResult, InsertResult, UpdateResult
from smongo.wire.commands.admin import (
    _cmd_coll_mod,
    _cmd_coll_stats,
    _cmd_compact,
    _cmd_create_collection,
    _cmd_db_stats,
    _cmd_drop,
    _cmd_drop_database,
    _cmd_explain,
    _cmd_fsync,
    _cmd_get_parameter,
    _cmd_getnonce,
    _cmd_list_collections,
    _cmd_list_databases,
    _cmd_rename_collection,
    _cmd_server_status,
    _cmd_set_parameter,
    _cmd_validate,
)
from smongo.wire.commands.aggregation import _cmd_aggregate, _cmd_map_reduce
from smongo.wire.commands.crud import (
    _cmd_bulk_write,
    _cmd_count,
    _cmd_data_size,
    _cmd_delete,
    _cmd_distinct,
    _cmd_estimated_doc_count,
    _cmd_find,
    _cmd_find_and_modify,
    _cmd_get_last_error,
    _cmd_get_more,
    _cmd_insert,
    _cmd_kill_cursors,
    _cmd_update,
)
from smongo.wire.cursors import CursorRegistry


def _make_mock_coll(docs=None):
    store = list(docs or [])
    coll = MagicMock()

    def _find_streaming(filt=None):
        if not filt:
            return iter(list(store))
        return iter(
            [
                d
                for d in store
                if all(d.get(k) == v for k, v in filt.items() if not isinstance(v, dict))
            ]
        )

    def _find(filt=None):
        return list(_find_streaming(filt))

    def _find_one(filt=None, projection=None):
        for d in _find(filt):
            return d
        return None

    def _insert_one(doc):
        from bson import ObjectId

        if "_id" not in doc:
            doc["_id"] = ObjectId()
        store.append(doc)
        return InsertResult([doc["_id"]])

    def _update(q, u, multi=False, upsert=False):
        matched = modified = 0
        for d in store:
            if all(d.get(k) == v for k, v in q.items() if not isinstance(v, dict)):
                matched += 1
                if isinstance(u, dict) and "$set" in u:
                    d.update(u["$set"])
                    modified += 1
                if not multi:
                    break
        return UpdateResult(matched, modified)

    def _delete(q, multi=False):
        to_remove = []
        for d in store:
            if all(d.get(k) == v for k, v in q.items() if not isinstance(v, dict)):
                to_remove.append(d)
                if not multi:
                    break
        for d in to_remove:
            store.remove(d)
        return DeleteResult(len(to_remove))

    coll.find_streaming = MagicMock(side_effect=_find_streaming)
    coll.find = MagicMock(side_effect=_find)
    coll.find_one = MagicMock(side_effect=_find_one)
    coll.insert_one = MagicMock(side_effect=_insert_one)
    coll.update = MagicMock(side_effect=_update)
    coll.delete = MagicMock(side_effect=_delete)
    coll.count = MagicMock(return_value=len(store))
    coll.count_fast = MagicMock(return_value=len(store))
    coll.data_size_bytes = MagicMock(return_value=100)
    coll.explain = MagicMock(return_value={"plan": "COLLSCAN", "index": None})
    coll.find_one_and_delete = MagicMock(return_value=store[0] if store else None)
    coll.find_one_and_update = MagicMock(return_value=store[0] if store else None)
    coll.find_one_and_replace = MagicMock(return_value=store[0] if store else None)
    coll.compact = MagicMock()
    coll.storage_stats = MagicMock(
        return_value={
            "count": len(store),
            "dataSize": 200,
            "storageSize": 400,
            "nindexes": 1,
            "totalIndexSize": 50,
            "indexSizes": {"_id_": 50},
            "storageEngine": {"name": "redb"},
        }
    )
    coll.verify = MagicMock(
        return_value={
            "nrecords": len(store),
            "nIndexes": 1,
            "valid": True,
            "errors": [],
            "warnings": [],
        }
    )
    coll.list_indexes = MagicMock(return_value=[])
    coll.create_index = MagicMock(return_value="idx_1")
    coll.drop_index = MagicMock()
    coll.rebuild_all_indexes = MagicMock(return_value=0)
    coll.get_all = MagicMock(return_value=list(store))
    coll.aggregate_engine = MagicMock(return_value=list(store))
    coll.watch = MagicMock(return_value=MagicMock())
    coll._validator = None
    coll.index_mgr = MagicMock()
    coll.index_mgr._indexes = {}
    return coll


@pytest.fixture
def local_client(tmp_path):
    return RedbLocalClient(str(tmp_path / "wire_crud_redb"))


@pytest.fixture
def cursor_registry():
    return CursorRegistry(default_batch_size=101)


@pytest.fixture
def ctx(local_client, cursor_registry):
    return ConnectionContext(
        local_client,
        connection_id=1,
        address=("127.0.0.1", 50000),
        cursor_registry=cursor_registry,
    )


SEQ: dict = {}
MOCK_COLL = None
MOCK_DB = None


def _ok(resp):
    assert resp.get("ok") == 1.0, resp
    return resp


def _err(resp):
    assert resp.get("ok") == 0, resp
    return resp


class TestCrudFind:
    def test_find_empty(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_find(ctx, {"find": "x", "$db": "t"}, SEQ))
        assert r["cursor"]["firstBatch"] == []

    def test_find_docs(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "x": 1}, {"_id": "2", "x": 2}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_find(ctx, {"find": "x", "$db": "t"}, SEQ))
        assert len(r["cursor"]["firstBatch"]) == 2

    def test_find_sorted(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "v": 3}, {"_id": "2", "v": 1}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: MagicMock())
        r = _ok(
            _cmd_find(ctx, {"find": "x", "$db": "t", "sort": {"v": 1}, "singleBatch": True}, SEQ)
        )
        vals = [d["v"] for d in r["cursor"]["firstBatch"]]
        assert vals == [1, 3]

    def test_find_skip_limit(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": str(i)} for i in range(10)])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_find(ctx, {"find": "x", "$db": "t", "skip": 2, "limit": 3}, SEQ))
        assert len(r["cursor"]["firstBatch"]) == 3

    def test_find_projection(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "a": 1, "b": 2}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_find(ctx, {"find": "x", "$db": "t", "projection": {"a": 1, "_id": 0}}, SEQ))
        assert "a" in r["cursor"]["firstBatch"][0]

    def test_find_single_batch(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1"}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: MagicMock())
        r = _ok(_cmd_find(ctx, {"find": "x", "$db": "t", "singleBatch": True}, SEQ))
        assert r["cursor"]["id"] == 0


class TestCrudInsert:
    def test_insert(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_insert(ctx, {"insert": "x", "$db": "t", "documents": [{"n": 1}, {"n": 2}]}, SEQ)
        )
        assert r["n"] == 2

    def test_insert_seqs(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_insert(ctx, {"insert": "x", "$db": "t"}, {"documents": [{"a": 1}]}))
        assert r["n"] == 1

    def test_insert_over_limit(self, ctx):
        r = _err(_cmd_insert(ctx, {"insert": "x", "$db": "t", "documents": [{}] * 100001}, SEQ))


class TestCrudUpdate:
    def test_update_operator(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "x": 1}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_update(
                ctx,
                {"update": "x", "$db": "t", "updates": [{"q": {"x": 1}, "u": {"$set": {"x": 9}}}]},
                SEQ,
            )
        )
        assert r["nModified"] >= 1

    def test_update_replacement(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "x": 1}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_update(
                ctx,
                {"update": "x", "$db": "t", "updates": [{"q": {"x": 1}, "u": {"x": 1, "z": 3}}]},
                SEQ,
            )
        )
        assert r["ok"] == 1.0

    def test_update_upsert_operator(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_update(
                ctx,
                {
                    "update": "x",
                    "$db": "t",
                    "updates": [{"q": {"x": 1}, "u": {"$set": {"v": 1}}, "upsert": True}],
                },
                SEQ,
            )
        )
        assert r.get("upserted") or r["n"] >= 1

    def test_update_upsert_replacement(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_update(
                ctx,
                {
                    "update": "x",
                    "$db": "t",
                    "updates": [{"q": {"x": 1}, "u": {"x": 1, "v": 1}, "upsert": True}],
                },
                SEQ,
            )
        )
        assert r.get("upserted") or r["n"] >= 1

    def test_update_multi(self, ctx, monkeypatch):
        mc = _make_mock_coll(
            [{"_id": "1", "g": "a"}, {"_id": "2", "g": "a"}, {"_id": "3", "g": "b"}]
        )
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_update(
                ctx,
                {
                    "update": "x",
                    "$db": "t",
                    "updates": [{"q": {"g": "a"}, "u": {"$set": {"d": 1}}, "multi": True}],
                },
                SEQ,
            )
        )
        assert r["nModified"] == 2


class TestCrudDelete:
    def test_delete_single(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "k": 1}, {"_id": "2", "k": 2}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_delete(
                ctx, {"delete": "x", "$db": "t", "deletes": [{"q": {"k": 1}, "limit": 1}]}, SEQ
            )
        )
        assert r["n"] == 1

    def test_delete_multi(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "k": 1}, {"_id": "2", "k": 1}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_delete(
                ctx, {"delete": "x", "$db": "t", "deletes": [{"q": {"k": 1}, "limit": 0}]}, SEQ
            )
        )
        assert r["n"] == 2


class TestCrudCountDistinct:
    def test_count(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1"}, {"_id": "2"}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_count(ctx, {"count": "x", "$db": "t"}, SEQ))
        assert r["n"] == 2

    def test_distinct(self, ctx, monkeypatch):
        mc = _make_mock_coll(
            [{"_id": "1", "c": "a"}, {"_id": "2", "c": "b"}, {"_id": "3", "c": "a"}]
        )
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_distinct(ctx, {"distinct": "x", "$db": "t", "key": "c"}, SEQ))
        assert sorted(r["values"]) == ["a", "b"]


class TestCrudGetMore:
    def test_get_more(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": str(i)} for i in range(200)])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_find(ctx, {"find": "x", "$db": "t", "batchSize": 5}, SEQ))
        cid = r["cursor"]["id"]
        r2 = _ok(_cmd_get_more(ctx, {"getMore": cid, "collection": "x", "$db": "t"}, SEQ))
        assert len(r2["cursor"]["nextBatch"]) > 0

    def test_get_more_not_found(self, ctx):
        r = _err(_cmd_get_more(ctx, {"getMore": 99999, "collection": "x", "$db": "t"}, SEQ))


class TestCrudKillCursors:
    def test_kill(self, ctx):
        r = _ok(_cmd_kill_cursors(ctx, {"killCursors": "x", "$db": "t", "cursors": [99999]}, SEQ))
        assert 99999 in r["cursorsNotFound"]


class TestCrudFindAndModify:
    def test_update_new(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "x": 1}])
        mc.find_one_and_update.return_value = {"_id": "1", "x": 1, "y": 99}
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_find_and_modify(
                ctx,
                {
                    "findAndModify": "x",
                    "$db": "t",
                    "query": {"x": 1},
                    "update": {"$set": {"y": 99}},
                    "new": True,
                },
                SEQ,
            )
        )
        assert r["value"]["y"] == 99

    def test_remove(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "z": 1}])
        mc.find_one_and_delete.return_value = {"_id": "1", "z": 1}
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_find_and_modify(
                ctx, {"findAndModify": "x", "$db": "t", "query": {"z": 1}, "remove": True}, SEQ
            )
        )
        assert r["value"] is not None

    def test_upsert_operator(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_find_and_modify(
                ctx,
                {
                    "findAndModify": "x",
                    "$db": "t",
                    "query": {"u": 1},
                    "update": {"$set": {"v": 1}},
                    "upsert": True,
                    "new": True,
                },
                SEQ,
            )
        )
        assert r["value"] is not None

    def test_upsert_replacement(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_find_and_modify(
                ctx,
                {
                    "findandmodify": "x",
                    "$db": "t",
                    "query": {},
                    "update": {"v": 1},
                    "upsert": True,
                    "new": True,
                },
                SEQ,
            )
        )
        assert r["value"] is not None

    def test_no_op(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        _err(_cmd_find_and_modify(ctx, {"findAndModify": "x", "$db": "t", "query": {}}, SEQ))

    def test_sort_update(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "s": 3}, {"_id": "2", "s": 1}])
        mc.find_one_and_update.return_value = {"_id": "2", "s": 1, "p": True}
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_find_and_modify(
                ctx,
                {
                    "findAndModify": "x",
                    "$db": "t",
                    "query": {},
                    "update": {"$set": {"p": True}},
                    "sort": {"s": 1},
                    "new": True,
                },
                SEQ,
            )
        )
        assert r["value"]["s"] == 1

    def test_sort_remove(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "s": 3}, {"_id": "2", "s": 1}])
        mc.find_one_and_delete.return_value = {"_id": "1", "s": 3}
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_find_and_modify(
                ctx,
                {"findAndModify": "x", "$db": "t", "query": {}, "remove": True, "sort": {"s": -1}},
                SEQ,
            )
        )
        assert r["value"]["s"] == 3

    def test_no_match(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_find_and_modify(
                ctx,
                {
                    "findAndModify": "x",
                    "$db": "t",
                    "query": {"x": 9999},
                    "update": {"$set": {"v": 1}},
                },
                SEQ,
            )
        )
        assert r["value"] is None


class TestCrudBulkWrite:
    def test_bulk_insert(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_bulk_write(
                ctx,
                {
                    "bulkWrite": 1,
                    "$db": "t",
                    "ops": [
                        {"insert": 0, "document": {"b": 1}},
                        {"insert": 0, "document": {"b": 2}},
                    ],
                    "nsInfo": [{"ns": "t.c"}],
                },
                SEQ,
            )
        )
        assert r["nInserted"] == 2

    def test_bulk_update_delete(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "k": 1}, {"_id": "2", "k": 2}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_bulk_write(
                ctx,
                {
                    "bulkWrite": 1,
                    "$db": "t",
                    "ops": [
                        {"update": 0, "filter": {"k": 1}, "updateMods": {"$set": {"d": 1}}},
                        {"delete": 0, "filter": {"k": 2}},
                    ],
                    "nsInfo": [{"ns": "t.c"}],
                },
                SEQ,
            )
        )
        assert r["ok"] == 1.0


class TestCrudMisc:
    def test_get_last_error_none(self, ctx):
        r = _ok(_cmd_get_last_error(ctx, {"getLastError": 1, "$db": "t"}, SEQ))
        assert r["n"] == 0

    def test_estimated_count(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        mc.count_fast.return_value = 5
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_estimated_doc_count(ctx, {"estimatedDocumentCount": "x", "$db": "t"}, SEQ))
        assert r["n"] == 5

    def test_data_size(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_data_size(ctx, {"dataSize": "t.x", "$db": "t"}, SEQ))
        assert r["size"] >= 0

    def test_data_size_empty(self, ctx):
        _err(_cmd_data_size(ctx, {"dataSize": "", "$db": "t"}, SEQ))

    def test_data_size_range(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "age": 5}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_data_size(
                ctx,
                {
                    "dataSize": "t.x",
                    "$db": "t",
                    "keyPattern": {"age": 1},
                    "min": {"age": 0},
                    "max": {"age": 10},
                },
                SEQ,
            )
        )
        assert r["ok"] == 1.0


class TestAdminCommands:
    def test_list_databases(self, ctx):
        r = _ok(_cmd_list_databases(ctx, {"listDatabases": 1, "$db": "admin"}, SEQ))
        assert isinstance(r["databases"], list)

    def test_list_databases_name_only(self, ctx):
        r = _ok(
            _cmd_list_databases(ctx, {"listDatabases": 1, "$db": "admin", "nameOnly": True}, SEQ)
        )
        for d in r["databases"]:
            assert "sizeOnDisk" not in d

    def test_list_collections(self, ctx, monkeypatch):
        mdb = MagicMock()
        mdb.list_collection_names.return_value = ["c1"]
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: mdb)
        r = _ok(_cmd_list_collections(ctx, {"listCollections": 1, "$db": "t"}, SEQ))
        assert "c1" in [e["name"] for e in r["cursor"]["firstBatch"]]

    def test_list_collections_name_only(self, ctx, monkeypatch):
        mdb = MagicMock()
        mdb.list_collection_names.return_value = ["c"]
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: mdb)
        r = _ok(
            _cmd_list_collections(ctx, {"listCollections": 1, "$db": "t", "nameOnly": True}, SEQ)
        )
        for e in r["cursor"]["firstBatch"]:
            assert "info" not in e

    def test_list_collections_filter(self, ctx, monkeypatch):
        mdb = MagicMock()
        mdb.list_collection_names.return_value = ["a", "b"]
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: mdb)
        r = _ok(
            _cmd_list_collections(
                ctx, {"listCollections": 1, "$db": "t", "filter": {"name": "a"}}, SEQ
            )
        )
        assert [e["name"] for e in r["cursor"]["firstBatch"]] == ["a"]

    def test_create(self, ctx, monkeypatch):
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: MagicMock())
        _ok(_cmd_create_collection(ctx, {"create": "c", "$db": "t"}, SEQ))

    def test_drop(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        mdb = MagicMock()
        mdb.get_collection.return_value = mc
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: mdb)
        _ok(_cmd_drop(ctx, {"drop": "c", "$db": "t"}, SEQ))

    def test_drop_database(self, ctx, monkeypatch):
        mdb = MagicMock()
        mdb._collections = {"c": MagicMock()}
        mdb.get_collection.return_value = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: mdb)
        _ok(_cmd_drop_database(ctx, {"dropDatabase": 1, "$db": "t"}, SEQ))

    def test_explain(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_explain(ctx, {"explain": {"find": "x", "filter": {}}, "$db": "t"}, SEQ))
        assert "queryPlanner" in r

    def test_explain_unknown(self, ctx):
        r = _ok(_cmd_explain(ctx, {"explain": "blah", "$db": "t"}, SEQ))
        assert "queryPlanner" in r

    def test_collmod(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        _ok(
            _cmd_coll_mod(
                ctx,
                {"collMod": "x", "$db": "t", "validator": {"$jsonSchema": {"required": ["x"]}}},
                SEQ,
            )
        )

    def test_collmod_off(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        _ok(_cmd_coll_mod(ctx, {"collMod": "x", "$db": "t", "validationLevel": "off"}, SEQ))

    def test_collmod_index(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        mc.list_indexes.return_value = [{"name": "x_1", "keys": {"x": 1}}]
        mi = MagicMock()
        mc.index_mgr._indexes = {"x_1": mi}
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        _ok(
            _cmd_coll_mod(
                ctx,
                {
                    "collMod": "x",
                    "$db": "t",
                    "index": {"keyPattern": {"x": 1}, "expireAfterSeconds": 60},
                },
                SEQ,
            )
        )

    def test_rename(self, ctx, monkeypatch):
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: MagicMock())
        _ok(
            _cmd_rename_collection(
                ctx, {"renameCollection": "t.a", "to": "t.b", "$db": "admin"}, SEQ
            )
        )

    def test_rename_invalid(self, ctx):
        _err(
            _cmd_rename_collection(
                ctx, {"renameCollection": "bad", "to": "bad", "$db": "admin"}, SEQ
            )
        )

    def test_rename_cross_db(self, ctx):
        _err(
            _cmd_rename_collection(
                ctx, {"renameCollection": "a.c", "to": "b.c", "$db": "admin"}, SEQ
            )
        )

    def test_compact(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_compact(ctx, {"compact": "x", "$db": "t"}, SEQ))
        assert "bytesFreed" in r

    def test_validate(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_validate(ctx, {"validate": "x", "$db": "t"}, SEQ))
        assert r["valid"] is True

    def test_coll_stats(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(_cmd_coll_stats(ctx, {"collStats": "x", "$db": "t"}, SEQ))
        assert "count" in r

    def test_db_stats(self, ctx, monkeypatch):
        mdb = MagicMock()
        mdb.list_collection_names.return_value = ["c1"]
        mdb.get_collection.return_value = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: mdb)
        r = _ok(_cmd_db_stats(ctx, {"dbStats": 1, "$db": "t"}, SEQ))
        assert r["collections"] >= 1

    def test_server_status(self, ctx):
        r = _ok(_cmd_server_status(ctx, {"serverStatus": 1, "$db": "admin"}, SEQ))
        assert "uptime" in r

    def test_fsync(self, ctx, monkeypatch):
        monkeypatch.setattr(type(ctx.local_client), "checkpoint", lambda s: None, raising=False)
        r = _ok(_cmd_fsync(ctx, {"fsync": 1, "$db": "admin"}, SEQ))
        assert r["numFiles"] >= 1

    def test_fsync_lock(self, ctx, monkeypatch):
        monkeypatch.setattr(type(ctx.local_client), "checkpoint", lambda s: None, raising=False)
        r = _ok(_cmd_fsync(ctx, {"fsync": 1, "$db": "admin", "lock": True}, SEQ))
        assert r["lockCount"] == 1

    def test_fsync_async(self, ctx, monkeypatch):
        monkeypatch.setattr(type(ctx.local_client), "checkpoint", lambda s: None, raising=False)
        r = _ok(_cmd_fsync(ctx, {"fsync": 1, "$db": "admin", "async": True}, SEQ))
        assert r.get("async") is True

    def test_getnonce(self, ctx):
        r = _ok(_cmd_getnonce(ctx, {"getnonce": 1, "$db": "admin"}, SEQ))
        assert len(r["nonce"]) == 16

    def test_set_get_param(self, ctx):
        _ok(_cmd_set_parameter(ctx, {"setParameter": 1, "$db": "admin", "p": 42}, SEQ))
        r = _ok(_cmd_get_parameter(ctx, {"getParameter": "p", "$db": "admin"}, SEQ))
        assert r["p"] == 42

    def test_get_all_params(self, ctx):
        _ok(_cmd_set_parameter(ctx, {"setParameter": 1, "$db": "admin", "q": 1}, SEQ))
        r = _ok(_cmd_get_parameter(ctx, {"getParameter": "*", "$db": "admin"}, SEQ))
        assert "q" in r

    def test_get_unknown_param(self, ctx):
        _err(_cmd_get_parameter(ctx, {"getParameter": "nope_xyz", "$db": "admin"}, SEQ))

    def test_get_invalid_param(self, ctx):
        _err(_cmd_get_parameter(ctx, {"getParameter": 123, "$db": "admin"}, SEQ))


class TestAggregation:
    def test_match(self, ctx, monkeypatch):
        mc = _make_mock_coll([{"_id": "1", "v": 1}, {"_id": "2", "v": 2}])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: MagicMock())
        r = _ok(
            _cmd_aggregate(
                ctx, {"aggregate": "x", "$db": "t", "pipeline": [{"$match": {}}], "cursor": {}}, SEQ
            )
        )
        assert len(r["cursor"]["firstBatch"]) >= 1

    def test_db_level(self, ctx):
        r = _ok(
            _cmd_aggregate(ctx, {"aggregate": 1, "$db": "t", "pipeline": [], "cursor": {}}, SEQ)
        )
        assert r["cursor"]["firstBatch"] == []

    def test_current_op(self, ctx):
        r = _ok(
            _cmd_aggregate(
                ctx,
                {"aggregate": 1, "$db": "admin", "pipeline": [{"$currentOp": {}}], "cursor": {}},
                SEQ,
            )
        )
        assert r["ok"] == 1.0

    def test_list_search_indexes(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_aggregate(
                ctx,
                {
                    "aggregate": "x",
                    "$db": "t",
                    "pipeline": [{"$listSearchIndexes": {}}],
                    "cursor": {},
                },
                SEQ,
            )
        )
        assert r["cursor"]["firstBatch"] == []

    def test_index_stats(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: MagicMock())
        r = _ok(
            _cmd_aggregate(
                ctx,
                {"aggregate": "x", "$db": "t", "pipeline": [{"$indexStats": {}}], "cursor": {}},
                SEQ,
            )
        )
        assert r["ok"] == 1.0

    def test_coll_stats(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        monkeypatch.setattr(ConnectionContext, "get_db", lambda s, d: MagicMock())
        r = _ok(
            _cmd_aggregate(
                ctx,
                {
                    "aggregate": "x",
                    "$db": "t",
                    "pipeline": [{"$collStats": {"storageStats": {}, "count": {}}}],
                    "cursor": {},
                },
                SEQ,
            )
        )
        assert len(r["cursor"]["firstBatch"]) == 1

    def test_change_stream(self, ctx, monkeypatch):
        mc = _make_mock_coll([])
        monkeypatch.setattr(ConnectionContext, "get_collection", lambda s, d, c: mc)
        r = _ok(
            _cmd_aggregate(
                ctx,
                {"aggregate": "x", "$db": "t", "pipeline": [{"$changeStream": {}}], "cursor": {}},
                SEQ,
            )
        )
        assert r["cursor"]["id"] != 0

    def test_map_reduce(self, ctx):
        _err(_cmd_map_reduce(ctx, {"mapReduce": "x", "$db": "t"}, SEQ))
