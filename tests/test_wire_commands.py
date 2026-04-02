"""Unit tests for wire/commands.py -- command handlers against a real engine."""

import pytest
from bson import ObjectId as BsonObjectId

from smongo.wire.commands import dispatch
from smongo.wire.context import ConnectionContext
from smongo.wire.cursors import CursorRegistry


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


# ── Handshake / admin ───────────────────────────────────────────────


class TestHandshake:
    def test_hello(self, ctx):
        resp = dispatch(ctx, {"hello": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["ismaster"] is True
        assert resp["maxWireVersion"] >= 17
        assert resp["connectionId"] == 1

    def test_ismaster(self, ctx):
        resp = dispatch(ctx, {"isMaster": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["ismaster"] is True

    def test_hello_ok_flag(self, ctx):
        resp = dispatch(ctx, {"hello": 1, "helloOk": True, "$db": "admin"})
        assert resp.get("helloOk") is True

    def test_ping(self, ctx):
        resp = dispatch(ctx, {"ping": 1, "$db": "admin"})
        assert resp == {"ok": 1.0}

    def test_build_info(self, ctx):
        resp = dispatch(ctx, {"buildInfo": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "version" in resp
        assert "embedded" in resp["modules"]

    def test_whatsmyuri(self, ctx):
        resp = dispatch(ctx, {"whatsmyuri": 1, "$db": "admin"})
        assert resp["you"] == "127.0.0.1:50000"

    def test_server_status(self, ctx):
        resp = dispatch(ctx, {"serverStatus": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "uptime" in resp


class TestUnknownCommand:
    def test_returns_command_not_found(self, ctx):
        resp = dispatch(ctx, {"nonExistentCommand": 1, "$db": "test"})
        assert resp["ok"] == 0
        assert resp["code"] == 59


# ── CRUD ─────────────────────────────────────────────────────────────


class TestInsert:
    def test_insert_single(self, ctx):
        resp = dispatch(ctx, {
            "insert": "things", "documents": [{"name": "alpha"}], "$db": "testdb"
        })
        assert resp["ok"] == 1.0
        assert resp["n"] == 1

    def test_insert_multiple(self, ctx):
        resp = dispatch(ctx, {
            "insert": "things",
            "documents": [{"x": 1}, {"x": 2}, {"x": 3}],
            "$db": "testdb",
        })
        assert resp["n"] == 3

    def test_insert_via_doc_sequence(self, ctx):
        resp = dispatch(
            ctx,
            {"insert": "things", "$db": "testdb"},
            doc_sequences={"documents": [{"y": 10}, {"y": 20}]},
        )
        assert resp["n"] == 2


class TestFind:
    def test_find_empty(self, ctx):
        resp = dispatch(ctx, {"find": "empty", "filter": {}, "$db": "testdb"})
        assert resp["ok"] == 1.0
        assert resp["cursor"]["firstBatch"] == []
        assert resp["cursor"]["id"] == 0

    def test_find_returns_inserted_docs(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"val": 1}, {"val": 2}, {"val": 3}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {"find": "items", "filter": {}, "$db": "testdb"})
        assert resp["ok"] == 1.0
        batch = resp["cursor"]["firstBatch"]
        assert len(batch) == 3

    def test_find_with_filter(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"val": 1}, {"val": 2}, {"val": 3}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "find": "items", "filter": {"val": {"$gt": 1}}, "$db": "testdb"
        })
        batch = resp["cursor"]["firstBatch"]
        assert len(batch) == 2

    def test_find_with_sort_and_limit(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"v": 3}, {"v": 1}, {"v": 2}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "find": "items",
            "filter": {},
            "sort": {"v": 1},
            "limit": 2,
            "$db": "testdb",
        })
        batch = resp["cursor"]["firstBatch"]
        assert len(batch) == 2
        assert batch[0]["v"] == 1
        assert batch[1]["v"] == 2

    def test_find_with_projection(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"a": 1, "b": 2}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "find": "items", "filter": {}, "projection": {"a": 1}, "$db": "testdb"
        })
        doc = resp["cursor"]["firstBatch"][0]
        assert "a" in doc
        assert "b" not in doc

    def test_find_single_batch(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"v": i} for i in range(10)],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "find": "items", "filter": {}, "singleBatch": True, "$db": "testdb"
        })
        assert resp["cursor"]["id"] == 0
        assert len(resp["cursor"]["firstBatch"]) == 10

    def test_find_objectid_converted(self, ctx):
        dispatch(ctx, {
            "insert": "items", "documents": [{"x": 1}], "$db": "testdb"
        })
        resp = dispatch(ctx, {"find": "items", "filter": {}, "$db": "testdb"})
        doc = resp["cursor"]["firstBatch"][0]
        assert isinstance(doc["_id"], BsonObjectId)


class TestUpdate:
    def test_update_one(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"name": "a", "v": 1}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "update": "items",
            "updates": [{"q": {"name": "a"}, "u": {"$set": {"v": 99}}}],
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0
        assert resp["nModified"] == 1

        found = dispatch(ctx, {"find": "items", "filter": {"name": "a"}, "$db": "testdb"})
        assert found["cursor"]["firstBatch"][0]["v"] == 99

    def test_update_multi(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"t": "x", "v": 1}, {"t": "x", "v": 2}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "update": "items",
            "updates": [{"q": {"t": "x"}, "u": {"$inc": {"v": 10}}, "multi": True}],
            "$db": "testdb",
        })
        assert resp["nModified"] == 2


class TestDelete:
    def test_delete_one(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"a": 1}, {"a": 2}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "delete": "items",
            "deletes": [{"q": {"a": 1}, "limit": 1}],
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0
        assert resp["n"] == 1

    def test_delete_many(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"a": 1}, {"a": 1}, {"a": 2}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "delete": "items",
            "deletes": [{"q": {"a": 1}, "limit": 0}],
            "$db": "testdb",
        })
        assert resp["n"] == 2


class TestCount:
    def test_count_all(self, ctx):
        dispatch(ctx, {
            "insert": "items", "documents": [{"x": 1}, {"x": 2}], "$db": "testdb"
        })
        resp = dispatch(ctx, {"count": "items", "$db": "testdb"})
        assert resp["n"] == 2

    def test_count_with_query(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"x": 1}, {"x": 2}, {"x": 3}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {"count": "items", "query": {"x": {"$gte": 2}}, "$db": "testdb"})
        assert resp["n"] == 2


class TestDistinct:
    def test_distinct_values(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"c": "a"}, {"c": "b"}, {"c": "a"}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {"distinct": "items", "key": "c", "$db": "testdb"})
        assert resp["ok"] == 1.0
        assert sorted(resp["values"]) == ["a", "b"]


# ── Cursor commands ──────────────────────────────────────────────────


class TestCursors:
    def test_batched_find_and_get_more(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"i": n} for n in range(10)],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "find": "items", "filter": {}, "batchSize": 3, "$db": "testdb"
        })
        cursor_id = resp["cursor"]["id"]
        assert cursor_id != 0
        assert len(resp["cursor"]["firstBatch"]) == 3

        resp2 = dispatch(ctx, {
            "getMore": cursor_id, "collection": "items", "$db": "testdb"
        })
        assert resp2["ok"] == 1.0
        assert len(resp2["cursor"]["nextBatch"]) > 0

    def test_get_more_unknown_cursor(self, ctx):
        resp = dispatch(ctx, {"getMore": 999999, "collection": "x", "$db": "testdb"})
        assert resp["ok"] == 0
        assert resp["code"] == 43

    def test_kill_cursors(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [{"i": n} for n in range(10)],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "find": "items", "filter": {}, "batchSize": 2, "$db": "testdb"
        })
        cursor_id = resp["cursor"]["id"]

        kill_resp = dispatch(ctx, {
            "killCursors": "items", "cursors": [cursor_id], "$db": "testdb"
        })
        assert cursor_id in kill_resp["cursorsKilled"]


# ── Index commands ───────────────────────────────────────────────────


class TestIndexCommands:
    def test_create_and_list_indexes(self, ctx):
        dispatch(ctx, {
            "createIndexes": "items",
            "indexes": [{"key": {"name": 1}, "name": "name_1"}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {"listIndexes": "items", "$db": "testdb"})
        assert resp["ok"] == 1.0
        names = [idx["name"] for idx in resp["cursor"]["firstBatch"]]
        assert "_id_" in names
        assert "name_1" in names

    def test_drop_index(self, ctx):
        dispatch(ctx, {
            "createIndexes": "items",
            "indexes": [{"key": {"x": 1}, "name": "x_1"}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "dropIndexes": "items", "index": "x_1", "$db": "testdb"
        })
        assert resp["ok"] == 1.0


# ── Aggregation ──────────────────────────────────────────────────────


class TestAggregate:
    def test_simple_pipeline(self, ctx):
        dispatch(ctx, {
            "insert": "items",
            "documents": [
                {"dept": "eng", "v": 10},
                {"dept": "eng", "v": 20},
                {"dept": "sales", "v": 5},
            ],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "aggregate": "items",
            "pipeline": [
                {"$match": {"dept": "eng"}},
                {"$group": {"_id": "$dept", "total": {"$sum": "$v"}}},
            ],
            "cursor": {},
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0
        batch = resp["cursor"]["firstBatch"]
        assert len(batch) == 1
        assert batch[0]["total"] == 30


# ── Database / collection admin ──────────────────────────────────────


class TestAdmin:
    def test_list_databases(self, ctx):
        ctx.get_db("mydb")
        resp = dispatch(ctx, {"listDatabases": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        names = [d["name"] for d in resp["databases"]]
        assert "mydb" in names

    def test_list_collections(self, ctx):
        dispatch(ctx, {
            "insert": "stuff", "documents": [{"x": 1}], "$db": "testdb"
        })
        resp = dispatch(ctx, {"listCollections": 1, "$db": "testdb"})
        assert resp["ok"] == 1.0

    def test_create_collection(self, ctx):
        resp = dispatch(ctx, {"create": "newcoll", "$db": "testdb"})
        assert resp["ok"] == 1.0

    def test_drop_collection(self, ctx):
        dispatch(ctx, {
            "insert": "todrop", "documents": [{"a": 1}], "$db": "testdb"
        })
        resp = dispatch(ctx, {"drop": "todrop", "$db": "testdb"})
        assert resp["ok"] == 1.0

    def test_coll_stats(self, ctx):
        dispatch(ctx, {
            "insert": "items", "documents": [{"x": 1}], "$db": "testdb"
        })
        resp = dispatch(ctx, {"collStats": "items", "$db": "testdb"})
        assert resp["ok"] == 1.0
        assert resp["count"] == 1


# ── Session / auth stubs ─────────────────────────────────────────────


class TestStubs:
    def test_end_sessions(self, ctx):
        resp = dispatch(ctx, {"endSessions": [], "$db": "admin"})
        assert resp["ok"] == 1.0

    def test_sasl_start_rejected(self, ctx):
        resp = dispatch(ctx, {"saslStart": 1, "$db": "admin"})
        assert resp["ok"] == 0
        assert resp["code"] == 18

    def test_connection_status(self, ctx):
        resp = dispatch(ctx, {"connectionStatus": 1, "$db": "admin"})
        assert resp["ok"] == 1.0

    def test_get_log(self, ctx):
        resp = dispatch(ctx, {"getLog": "global", "$db": "admin"})
        assert resp["ok"] == 1.0

    def test_explain(self, ctx):
        resp = dispatch(ctx, {
            "explain": {"find": "items", "filter": {}}, "$db": "testdb"
        })
        assert resp["ok"] == 1.0
        assert "queryPlanner" in resp


# ── findAndModify ────────────────────────────────────────────────────


class TestFindAndModify:
    def test_find_and_delete(self, ctx):
        dispatch(ctx, {
            "insert": "fam", "documents": [{"x": 1}, {"x": 2}], "$db": "testdb"
        })
        resp = dispatch(ctx, {
            "findAndModify": "fam", "query": {"x": 1}, "remove": True, "$db": "testdb"
        })
        assert resp["ok"] == 1.0
        assert resp["value"]["x"] == 1
        found = dispatch(ctx, {"find": "fam", "filter": {}, "$db": "testdb"})
        assert len(found["cursor"]["firstBatch"]) == 1

    def test_find_and_update(self, ctx):
        dispatch(ctx, {
            "insert": "fam", "documents": [{"x": 1, "y": 10}], "$db": "testdb"
        })
        resp = dispatch(ctx, {
            "findAndModify": "fam",
            "query": {"x": 1},
            "update": {"$set": {"y": 99}},
            "new": True,
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0
        assert resp["value"]["y"] == 99

    def test_find_and_replace(self, ctx):
        dispatch(ctx, {
            "insert": "fam", "documents": [{"x": 1, "y": 10}], "$db": "testdb"
        })
        resp = dispatch(ctx, {
            "findAndModify": "fam",
            "query": {"x": 1},
            "update": {"x": 1, "y": 999, "replaced": True},
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0

    def test_find_and_modify_with_sort(self, ctx):
        dispatch(ctx, {
            "insert": "fam",
            "documents": [{"g": "a", "v": 3}, {"g": "a", "v": 1}, {"g": "a", "v": 2}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "findAndModify": "fam",
            "query": {"g": "a"},
            "sort": {"v": 1},
            "update": {"$set": {"picked": True}},
            "new": True,
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0
        assert resp["value"]["v"] == 1
        assert resp["value"]["picked"] is True

    def test_find_and_modify_upsert_insert(self, ctx):
        resp = dispatch(ctx, {
            "findAndModify": "fam",
            "query": {"key": "missing"},
            "update": {"$set": {"val": 42}},
            "upsert": True,
            "new": True,
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0
        assert resp["value"]["val"] == 42
        assert resp["value"]["key"] == "missing"

    def test_find_and_modify_fields_projection(self, ctx):
        dispatch(ctx, {
            "insert": "fam", "documents": [{"a": 1, "b": 2, "c": 3}], "$db": "testdb"
        })
        resp = dispatch(ctx, {
            "findAndModify": "fam",
            "query": {"a": 1},
            "update": {"$set": {"b": 99}},
            "new": True,
            "fields": {"a": 1, "b": 1},
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0
        assert "a" in resp["value"]
        assert "b" in resp["value"]
        assert "c" not in resp["value"]

    def test_find_and_modify_no_match_returns_null(self, ctx):
        resp = dispatch(ctx, {
            "findAndModify": "fam",
            "query": {"missing": True},
            "update": {"$set": {"x": 1}},
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0
        assert resp["value"] is None

    def test_find_and_modify_requires_update_or_remove(self, ctx):
        resp = dispatch(ctx, {
            "findAndModify": "fam", "query": {}, "$db": "testdb"
        })
        assert resp["ok"] == 0
        assert resp["code"] == 72


# ── Update with upsert ──────────────────────────────────────────────


class TestUpdateUpsert:
    def test_upsert_creates_document(self, ctx):
        resp = dispatch(ctx, {
            "update": "upcoll",
            "updates": [{"q": {"key": "new"}, "u": {"$set": {"val": 1}}, "upsert": True}],
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0
        assert resp["n"] == 1
        assert len(resp.get("upserted", [])) == 1

        found = dispatch(ctx, {"find": "upcoll", "filter": {"key": "new"}, "$db": "testdb"})
        assert len(found["cursor"]["firstBatch"]) == 1
        assert found["cursor"]["firstBatch"][0]["val"] == 1

    def test_upsert_updates_existing(self, ctx):
        dispatch(ctx, {
            "insert": "upcoll", "documents": [{"key": "exist", "val": 0}], "$db": "testdb"
        })
        resp = dispatch(ctx, {
            "update": "upcoll",
            "updates": [{"q": {"key": "exist"}, "u": {"$set": {"val": 99}}, "upsert": True}],
            "$db": "testdb",
        })
        assert resp["nModified"] == 1
        assert "upserted" not in resp


# ── dropDatabase ─────────────────────────────────────────────────────


class TestDropDatabase:
    def test_drop_database_clears_collections(self, ctx):
        dispatch(ctx, {
            "insert": "c1", "documents": [{"x": 1}], "$db": "dropme"
        })
        dispatch(ctx, {
            "insert": "c2", "documents": [{"y": 2}], "$db": "dropme"
        })
        resp = dispatch(ctx, {"dropDatabase": 1, "$db": "dropme"})
        assert resp["ok"] == 1.0
        assert resp["dropped"] == "dropme"

        found = dispatch(ctx, {"find": "c1", "filter": {}, "$db": "dropme"})
        assert len(found["cursor"]["firstBatch"]) == 0


# ── drop collection with indexes ─────────────────────────────────────


class TestDropCollectionComplete:
    def test_drop_removes_indexes(self, ctx):
        dispatch(ctx, {
            "insert": "dropcoll", "documents": [{"x": 1}], "$db": "testdb"
        })
        dispatch(ctx, {
            "createIndexes": "dropcoll",
            "indexes": [{"key": {"x": 1}, "name": "x_1"}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {"drop": "dropcoll", "$db": "testdb"})
        assert resp["ok"] == 1.0


# ── dbStats ──────────────────────────────────────────────────────────


class TestDbStats:
    def test_db_stats_returns_counts(self, ctx):
        dispatch(ctx, {
            "insert": "statcoll", "documents": [{"x": 1}, {"x": 2}], "$db": "testdb"
        })
        resp = dispatch(ctx, {"dbStats": 1, "$db": "testdb"})
        assert resp["ok"] == 1.0
        assert resp["db"] == "testdb"
        assert "collections" in resp
        assert "objects" in resp
        assert "indexes" in resp


# ── serverStatus enhanced ────────────────────────────────────────────


class TestServerStatusEnhanced:
    def test_server_status_has_opcounters(self, ctx):
        resp = dispatch(ctx, {"serverStatus": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "opcounters" in resp
        assert "version" in resp
        assert "process" in resp
        assert "pid" in resp
        assert "mem" in resp

    def test_server_status_has_connections(self, ctx):
        resp = dispatch(ctx, {"serverStatus": 1, "$db": "admin"})
        assert "connections" in resp
        assert "current" in resp["connections"]


# ── hostInfo enhanced ────────────────────────────────────────────────


class TestHostInfoEnhanced:
    def test_host_info_has_real_data(self, ctx):
        resp = dispatch(ctx, {"hostInfo": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "hostname" in resp["system"]
        assert "type" in resp["os"]
        assert "name" in resp["os"]


# ── getParameter ─────────────────────────────────────────────────────


class TestGetParameter:
    def test_get_known_parameter(self, ctx):
        resp = dispatch(ctx, {"getParameter": "featureCompatibilityVersion", "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "featureCompatibilityVersion" in resp

    def test_get_all_parameters(self, ctx):
        resp = dispatch(ctx, {"getParameter": "*", "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "featureCompatibilityVersion" in resp

    def test_get_unknown_parameter(self, ctx):
        resp = dispatch(ctx, {"getParameter": "nonexistent", "$db": "admin"})
        assert resp["ok"] == 0, "unknown parameter should return an error"


# ── collMod ──────────────────────────────────────────────────────────


class TestCollMod:
    def test_coll_mod_with_validator(self, ctx):
        dispatch(ctx, {"create": "modcoll", "$db": "testdb"})
        resp = dispatch(ctx, {
            "collMod": "modcoll",
            "validator": {"$jsonSchema": {"required": ["name"]}},
            "$db": "testdb",
        })
        assert resp["ok"] == 1.0


# ── renameCollection ─────────────────────────────────────────────────


class TestRenameCollection:
    def test_rename_collection(self, ctx):
        dispatch(ctx, {
            "insert": "src", "documents": [{"x": 1}, {"x": 2}], "$db": "testdb"
        })
        resp = dispatch(ctx, {
            "renameCollection": "testdb.src",
            "to": "testdb.dst",
            "$db": "admin",
        })
        assert resp["ok"] == 1.0

        found = dispatch(ctx, {"find": "dst", "filter": {}, "$db": "testdb"})
        assert len(found["cursor"]["firstBatch"]) == 2

    def test_rename_to_existing_fails(self, ctx):
        dispatch(ctx, {
            "insert": "s1", "documents": [{"x": 1}], "$db": "testdb"
        })
        dispatch(ctx, {
            "insert": "s2", "documents": [{"y": 2}], "$db": "testdb"
        })
        resp = dispatch(ctx, {
            "renameCollection": "testdb.s1",
            "to": "testdb.s2",
            "$db": "admin",
        })
        assert resp["ok"] == 0
        assert resp["code"] == 48


# ── Diagnostic commands ──────────────────────────────────────────────


class TestDiagnostics:
    def test_list_commands(self, ctx):
        resp = dispatch(ctx, {"listCommands": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "commands" in resp
        assert "find" in resp["commands"]
        assert "insert" in resp["commands"]
        assert "ping" in resp["commands"]

    def test_current_op(self, ctx):
        resp = dispatch(ctx, {"currentOp": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "inprog" in resp

    def test_kill_op(self, ctx):
        resp = dispatch(ctx, {"killOp": 1, "op": 123, "$db": "admin"})
        assert resp["ok"] == 1.0

    def test_validate(self, ctx):
        dispatch(ctx, {
            "insert": "valcoll", "documents": [{"x": 1}], "$db": "testdb"
        })
        resp = dispatch(ctx, {"validate": "valcoll", "$db": "testdb"})
        assert resp["ok"] == 1.0
        assert resp["valid"] is True
        assert resp["nrecords"] == 1

    def test_logout(self, ctx):
        resp = dispatch(ctx, {"logout": 1, "$db": "admin"})
        assert resp["ok"] == 1.0

    def test_map_reduce_rejected(self, ctx):
        resp = dispatch(ctx, {
            "mapReduce": "coll", "map": "", "reduce": "", "$db": "testdb"
        })
        assert resp["ok"] == 0
        assert resp["code"] == 115


# ── Admin aggregate with $currentOp ─────────────────────────────────


class TestAdminAggregate:
    def test_aggregate_current_op(self, ctx):
        resp = dispatch(ctx, {
            "aggregate": 1,
            "pipeline": [{"$currentOp": {}}],
            "cursor": {},
            "$db": "admin",
        })
        assert resp["ok"] == 1.0
        assert resp["cursor"]["firstBatch"] == []


# ── Default $db injection ────────────────────────────────────────────


class TestDefaultDb:
    def test_missing_db_defaults_to_test(self, ctx):
        resp = dispatch(ctx, {"ping": 1})
        assert resp["ok"] == 1.0


# ── Error paths ──────────────────────────────────────────────────────


class TestErrorPaths:
    def test_duplicate_key_error_via_wire(self, ctx):
        dispatch(ctx, {
            "insert": "errcoll", "documents": [{"_id": "dup", "x": 1}], "$db": "testdb"
        })
        dispatch(ctx, {
            "createIndexes": "errcoll",
            "indexes": [{"key": {"x": 1}, "name": "x_1", "unique": True}],
            "$db": "testdb",
        })
        resp = dispatch(ctx, {
            "insert": "errcoll", "documents": [{"_id": "dup2", "x": 1}], "$db": "testdb"
        })
        assert resp.get("writeErrors")
        assert resp["writeErrors"][0]["code"] == 11000

    def test_hello_with_sasl_supported_mechs(self, ctx):
        resp = dispatch(ctx, {
            "hello": 1,
            "saslSupportedMechs": "testdb.user",
            "$db": "admin",
        })
        assert resp["ok"] == 1.0
        assert resp["saslSupportedMechs"] == ["SCRAM-SHA-1", "SCRAM-SHA-256"]

    def test_hello_compression_negotiation(self, ctx):
        resp = dispatch(ctx, {
            "hello": 1,
            "compression": ["zlib"],
            "$db": "admin",
        })
        assert resp["ok"] == 1.0
        assert "compression" in resp
        assert "zlib" in resp["compression"]
