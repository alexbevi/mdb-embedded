"""Tests for real wire protocol command implementations (no stubs)."""

import uuid

import pytest
from bson import Binary

from smongo.wire.commands import dispatch
from smongo.wire.context import ConnectionContext
from smongo.wire.cursors import CursorRegistry


@pytest.fixture
def cursor_registry():
    return CursorRegistry(default_batch_size=101)


@pytest.fixture
def ctx(local_client, cursor_registry):
    c = ConnectionContext(
        local_client,
        connection_id=1,
        address=("127.0.0.1", 50000),
        cursor_registry=cursor_registry,
    )
    yield c
    from smongo.storage.transaction import _txn_state

    _txn_state.session = None


def _make_lsid() -> dict:
    """Generate a valid lsid for session-based commands."""
    return {"id": Binary(uuid.uuid4().bytes, subtype=4)}


# =====================================================================
# getLastError -- real per-connection write tracking
# =====================================================================


class TestGetLastError:
    def test_no_prior_write(self, ctx):
        resp = dispatch(ctx, {"getLastError": 1, "$db": "test"})
        assert resp["ok"] == 1.0
        assert resp["err"] is None
        assert resp["n"] == 0

    def test_tracks_insert(self, ctx):
        dispatch(ctx, {"insert": "gle", "documents": [{"x": 1}], "$db": "test"})
        resp = dispatch(ctx, {"getLastError": 1, "$db": "test"})
        assert resp["ok"] == 1.0
        assert resp["n"] == 1
        assert resp["err"] is None

    def test_tracks_update(self, ctx):
        dispatch(ctx, {"insert": "gle_u", "documents": [{"x": 1}], "$db": "test"})
        dispatch(
            ctx,
            {
                "update": "gle_u",
                "updates": [{"q": {"x": 1}, "u": {"$set": {"x": 2}}}],
                "$db": "test",
            },
        )
        resp = dispatch(ctx, {"getLastError": 1, "$db": "test"})
        assert resp["ok"] == 1.0
        assert resp["n"] >= 1
        assert resp["nModified"] >= 1

    def test_tracks_delete(self, ctx):
        dispatch(ctx, {"insert": "gle_d", "documents": [{"x": 1}], "$db": "test"})
        dispatch(
            ctx,
            {
                "delete": "gle_d",
                "deletes": [{"q": {"x": 1}, "limit": 1}],
                "$db": "test",
            },
        )
        resp = dispatch(ctx, {"getLastError": 1, "$db": "test"})
        assert resp["ok"] == 1.0
        assert resp["n"] >= 1

    def test_alias_lowercase(self, ctx):
        resp = dispatch(ctx, {"getlasterror": 1, "$db": "test"})
        assert resp["ok"] == 1.0


# =====================================================================
# startTransaction / commitTransaction / abortTransaction
# =====================================================================


class TestTransactions:
    def test_start_requires_lsid(self, ctx):
        resp = dispatch(ctx, {"startTransaction": 1, "$db": "test"})
        assert resp["ok"] == 0

    def test_start_commit(self, ctx):
        lsid = _make_lsid()
        resp = dispatch(ctx, {"startTransaction": 1, "$db": "test", "lsid": lsid})
        assert resp["ok"] == 1.0

        dispatch(
            ctx,
            {
                "insert": "txn_coll",
                "documents": [{"k": "v"}],
                "$db": "test",
                "lsid": lsid,
            },
        )

        resp = dispatch(ctx, {"commitTransaction": 1, "$db": "test", "lsid": lsid})
        assert resp["ok"] == 1.0

    def test_abort_rolls_back_insert(self, ctx):
        lsid = _make_lsid()
        dispatch(ctx, {"startTransaction": 1, "$db": "test", "lsid": lsid})

        dispatch(
            ctx,
            {
                "insert": "txn_abort",
                "documents": [{"rollme": True}],
                "$db": "test",
                "lsid": lsid,
            },
        )

        coll = ctx.get_collection("test", "txn_abort")
        assert len(coll.find({"rollme": True})) == 1

        resp = dispatch(ctx, {"abortTransaction": 1, "$db": "test", "lsid": lsid})
        assert resp["ok"] == 1.0
        assert len(coll.find({"rollme": True})) == 0

    def test_abort_rolls_back_delete(self, ctx):
        coll = ctx.get_collection("test", "txn_del")
        coll.insert_one({"keep": True})

        lsid = _make_lsid()
        dispatch(ctx, {"startTransaction": 1, "$db": "test", "lsid": lsid})
        dispatch(
            ctx,
            {
                "delete": "txn_del",
                "deletes": [{"q": {"keep": True}, "limit": 1}],
                "$db": "test",
                "lsid": lsid,
            },
        )
        assert len(coll.find({"keep": True})) == 0

        dispatch(ctx, {"abortTransaction": 1, "$db": "test", "lsid": lsid})
        assert len(coll.find({"keep": True})) == 1

    def test_abort_rolls_back_update(self, ctx):
        coll = ctx.get_collection("test", "txn_upd")
        coll.insert_one({"val": 1})

        lsid = _make_lsid()
        dispatch(ctx, {"startTransaction": 1, "$db": "test", "lsid": lsid})
        dispatch(
            ctx,
            {
                "update": "txn_upd",
                "updates": [{"q": {"val": 1}, "u": {"$set": {"val": 999}}}],
                "$db": "test",
                "lsid": lsid,
            },
        )
        assert coll.find({"val": 999})

        dispatch(ctx, {"abortTransaction": 1, "$db": "test", "lsid": lsid})
        assert coll.find({"val": 1})
        assert not coll.find({"val": 999})

    def test_commit_without_start_fails(self, ctx):
        lsid = _make_lsid()
        resp = dispatch(ctx, {"commitTransaction": 1, "$db": "test", "lsid": lsid})
        assert resp["ok"] == 0

    def test_double_start_fails(self, ctx):
        lsid = _make_lsid()
        dispatch(ctx, {"startTransaction": 1, "$db": "test", "lsid": lsid})
        resp = dispatch(ctx, {"startTransaction": 1, "$db": "test", "lsid": lsid})
        assert resp["ok"] == 0


# =====================================================================
# getnonce -- real cryptographic nonce
# =====================================================================


class TestGetNonce:
    def test_returns_hex_nonce(self, ctx):
        resp = dispatch(ctx, {"getnonce": 1, "$db": "test"})
        assert resp["ok"] == 1.0
        nonce = resp["nonce"]
        assert isinstance(nonce, str)
        assert len(nonce) == 16
        int(nonce, 16)  # valid hex

    def test_nonces_are_unique(self, ctx):
        r1 = dispatch(ctx, {"getnonce": 1, "$db": "test"})
        r2 = dispatch(ctx, {"getnonce": 1, "$db": "test"})
        assert r1["nonce"] != r2["nonce"]


# =====================================================================
# fsync -- real WiredTiger checkpoint
# =====================================================================


class TestFsync:
    def test_checkpoint(self, ctx):
        resp = dispatch(ctx, {"fsync": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert isinstance(resp["numFiles"], int)

    def test_with_lock(self, ctx):
        resp = dispatch(ctx, {"fsync": 1, "lock": True, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "lockCount" in resp


# =====================================================================
# usersInfo / rolesInfo
# =====================================================================


class TestUsersInfo:
    def test_returns_users_list(self, ctx):
        from smongo.wire.commands.users import _USER_STORE, _USER_STORE_LOCK
        with _USER_STORE_LOCK:
            _USER_STORE.clear()
        resp = dispatch(ctx, {"usersInfo": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["users"] == []


class TestRolesInfo:
    def test_no_builtin_by_default(self, ctx):
        resp = dispatch(ctx, {"rolesInfo": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["roles"] == []

    def test_show_builtin_roles(self, ctx):
        resp = dispatch(
            ctx,
            {
                "rolesInfo": 1,
                "showBuiltinRoles": True,
                "$db": "admin",
            },
        )
        assert resp["ok"] == 1.0
        assert len(resp["roles"]) > 0
        assert any(r["role"] == "root" for r in resp["roles"])


# =====================================================================
# saslStart
# =====================================================================


class TestSaslStartMessage:
    def test_suggests_no_credentials(self, ctx):
        resp = dispatch(ctx, {"saslStart": 1, "mechanism": "SCRAM-SHA-256", "$db": "admin"})
        assert resp["ok"] == 0
        assert "without credentials" in resp["errmsg"]
        assert "remove username/password" in resp["errmsg"]


# =====================================================================
# bulkWrite
# =====================================================================


class TestBulkWrite:
    def test_insert_via_bulk(self, ctx):
        resp = dispatch(
            ctx,
            {
                "bulkWrite": 1,
                "$db": "testdb",
                "ops": [
                    {"insert": 0, "document": {"name": "alpha"}},
                    {"insert": 0, "document": {"name": "beta"}},
                ],
                "nsInfo": [{"ns": "testdb.things"}],
            },
        )
        assert resp["ok"] == 1.0
        assert resp["nInserted"] == 2

    def test_delete_via_bulk(self, ctx):
        dispatch(
            ctx, {"insert": "bw_del", "documents": [{"name": "x"}, {"name": "y"}], "$db": "testdb"}
        )
        resp = dispatch(
            ctx,
            {
                "bulkWrite": 1,
                "$db": "testdb",
                "ops": [{"delete": 0, "filter": {}}],
                "nsInfo": [{"ns": "testdb.bw_del"}],
            },
        )
        assert resp["ok"] == 1.0
        assert resp["nDeleted"] >= 1

    def test_update_via_bulk(self, ctx):
        dispatch(ctx, {"insert": "bw_upd", "documents": [{"name": "x", "v": 1}], "$db": "testdb"})
        resp = dispatch(
            ctx,
            {
                "bulkWrite": 1,
                "$db": "testdb",
                "ops": [{"update": 0, "filter": {"name": "x"}, "updateMods": {"$set": {"v": 2}}}],
                "nsInfo": [{"ns": "testdb.bw_upd"}],
            },
        )
        assert resp["ok"] == 1.0
        assert resp["nModified"] >= 1

    def test_bulk_write_error(self, ctx):
        coll = ctx.get_collection("testdb", "bw_err")
        coll.create_index("name", unique=True)
        coll.insert_one({"name": "dup"})
        resp = dispatch(
            ctx,
            {
                "bulkWrite": 1,
                "$db": "testdb",
                "ops": [{"insert": 0, "document": {"name": "dup"}}],
                "nsInfo": [{"ns": "testdb.bw_err"}],
            },
        )
        assert resp["ok"] == 1.0
        assert "writeErrors" in resp


# =====================================================================
# setParameter / getParameter with mutable store
# =====================================================================


class TestParameterStore:
    def test_set_and_get(self, ctx):
        dispatch(ctx, {"setParameter": 1, "logLevel": 2, "$db": "admin"})
        resp = dispatch(ctx, {"getParameter": "logLevel", "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["logLevel"] == 2

    def test_get_all(self, ctx):
        resp = dispatch(ctx, {"getParameter": "*", "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "featureCompatibilityVersion" in resp

    def test_set_returns_old_value(self, ctx):
        resp = dispatch(ctx, {"setParameter": 1, "quiet": True, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "quiet" in resp["was"]


# =====================================================================
# estimatedDocumentCount / dataSize / compact / reIndex
# =====================================================================


class TestEstimatedDocumentCount:
    def test_count(self, ctx):
        dispatch(
            ctx,
            {
                "insert": "edc",
                "documents": [{"a": 1}, {"a": 2}, {"a": 3}],
                "$db": "test",
            },
        )
        resp = dispatch(ctx, {"estimatedDocumentCount": "edc", "$db": "test"})
        assert resp["ok"] == 1.0
        assert resp["n"] == 3


class TestCompact:
    def test_compact_ok(self, ctx):
        dispatch(ctx, {"insert": "cmp", "documents": [{"a": 1}], "$db": "test"})
        resp = dispatch(ctx, {"compact": "cmp", "$db": "test"})
        assert resp["ok"] == 1.0


class TestReIndex:
    def test_reindex_ok(self, ctx):
        coll = ctx.get_collection("test", "ridx")
        coll.insert_one({"x": 1})
        coll.create_index("x")
        resp = dispatch(ctx, {"reIndex": "ridx", "$db": "test"})
        assert resp["ok"] == 1.0
        assert resp["nIndexes"] >= 2


# =====================================================================
# Enhanced collStats / dbStats / validate / serverStatus
# =====================================================================


class TestEnhancedCollStats:
    def test_has_sizes(self, ctx):
        dispatch(ctx, {"insert": "cs", "documents": [{"v": "x" * 100}], "$db": "test"})
        resp = dispatch(ctx, {"collStats": "cs", "$db": "test"})
        assert resp["ok"] == 1.0
        assert resp["count"] >= 1
        assert "size" in resp
        assert "storageSize" in resp
        assert "nindexes" in resp
        assert "wiredTiger" in resp


class TestEnhancedDbStats:
    def test_has_sizes(self, ctx):
        dispatch(ctx, {"insert": "ds", "documents": [{"v": 1}], "$db": "statdb"})
        resp = dispatch(ctx, {"dbStats": 1, "$db": "statdb"})
        assert resp["ok"] == 1.0
        assert resp["objects"] >= 1
        assert "dataSize" in resp
        assert "indexSize" in resp


class TestEnhancedValidate:
    def test_returns_integrity_info(self, ctx):
        dispatch(ctx, {"insert": "val", "documents": [{"x": 1}], "$db": "test"})
        resp = dispatch(ctx, {"validate": "val", "$db": "test"})
        assert resp["ok"] == 1.0
        assert resp["valid"] is True
        assert resp["nrecords"] == 1
        assert "errors" in resp
        assert "warnings" in resp


class TestEnhancedServerStatus:
    def test_has_wt_stats(self, ctx):
        resp = dispatch(ctx, {"serverStatus": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "wiredTiger" in resp
        assert "logicalSessionRecordCache" in resp
        assert resp["mem"]["bits"] == 64


# =====================================================================
# currentOp / killOp
# =====================================================================


class TestCurrentOp:
    def test_returns_inprog(self, ctx):
        resp = dispatch(ctx, {"currentOp": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert isinstance(resp["inprog"], list)


class TestKillOp:
    def test_not_found(self, ctx):
        resp = dispatch(ctx, {"killOp": 1, "op": 999999, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "not found" in resp["info"]


# =====================================================================
# top / profile / connPoolStats / features
# =====================================================================


class TestTop:
    def test_returns_totals(self, ctx):
        dispatch(ctx, {"insert": "topcol", "documents": [{"x": 1}], "$db": "test"})
        resp = dispatch(ctx, {"top": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert isinstance(resp["totals"], dict)


class TestProfile:
    def test_set_level(self, ctx):
        resp = dispatch(ctx, {"profile": 2, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "was" in resp

    def test_set_slowms(self, ctx):
        resp = dispatch(ctx, {"profile": 1, "slowms": 50, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["slowms"] == 100 or resp["slowms"] == 50  # was the old value


class TestConnPoolStats:
    def test_returns_ok(self, ctx):
        resp = dispatch(ctx, {"connPoolStats": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "totalCreated" in resp


class TestFeatures:
    def test_returns_ok(self, ctx):
        resp = dispatch(ctx, {"features": 1, "$db": "admin"})
        assert resp["ok"] == 1.0


class TestLogRotate:
    def test_returns_ok(self, ctx):
        resp = dispatch(ctx, {"logRotate": 1, "$db": "admin"})
        assert resp["ok"] == 1.0


class TestShardingState:
    def test_not_sharded(self, ctx):
        resp = dispatch(ctx, {"shardingState": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["enabled"] is False


class TestLockInfo:
    def test_returns_ok(self, ctx):
        resp = dispatch(ctx, {"lockInfo": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
