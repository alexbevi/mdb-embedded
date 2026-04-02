"""Tests for enterprise-grade fixes: real data everywhere, no stubs."""

import platform
import sys
import uuid

import pytest
from bson import Binary

from smongo.wire.commands import dispatch
from smongo.wire.commands.users import _USER_STORE, _USER_STORE_LOCK
from smongo.wire.context import (
    ConnectionContext,
    ConnectionCounter,
    FreeMonitoringState,
    LogBuffer,
    get_total_memory_mb,
    get_virtual_memory_mb,
)
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


def _make_lsid() -> dict:
    return {"id": Binary(uuid.uuid4().bytes, subtype=4)}


# ── LogBuffer ────────────────────────────────────────────────────────


class TestLogBuffer:
    def test_captures_log_lines(self):
        import logging

        buf = LogBuffer(max_lines=10)
        logger = logging.getLogger("test.logbuffer")
        logger.addHandler(buf)
        logger.setLevel(logging.DEBUG)
        logger.info("hello from test")
        lines, total = buf.get_lines()
        assert total >= 1
        assert any("hello from test" in ln for ln in lines)
        logger.removeHandler(buf)

    def test_max_lines_cap(self):
        import logging

        buf = LogBuffer(max_lines=3)
        logger = logging.getLogger("test.logbuf_cap")
        logger.addHandler(buf)
        logger.setLevel(logging.DEBUG)
        for i in range(10):
            logger.info("msg %d", i)
        lines, total = buf.get_lines()
        assert total == 10
        assert len(lines) == 3
        assert "msg 9" in lines[-1]
        logger.removeHandler(buf)


# ── ConnectionCounter ────────────────────────────────────────────────


class TestConnectionCounter:
    def test_connect_disconnect(self):
        cc = ConnectionCounter(max_connections=10)
        snap = cc.snapshot()
        assert snap["current"] == 0
        assert snap["available"] == 10
        assert snap["totalCreated"] == 0

        cc.connect()
        cc.connect()
        snap = cc.snapshot()
        assert snap["current"] == 2
        assert snap["available"] == 8
        assert snap["totalCreated"] == 2

        cc.disconnect()
        snap = cc.snapshot()
        assert snap["current"] == 1
        assert snap["available"] == 9
        assert snap["totalCreated"] == 2

    def test_disconnect_clamps_at_zero(self):
        cc = ConnectionCounter()
        cc.disconnect()
        assert cc.snapshot()["current"] == 0


# ── FreeMonitoringState ──────────────────────────────────────────────


class TestFreeMonitoringState:
    def test_default_disabled(self):
        fm = FreeMonitoringState()
        assert fm.state == "disabled"

    def test_enable_disable(self):
        fm = FreeMonitoringState()
        fm.set("enable")
        assert fm.state == "enabled"
        fm.set("disable")
        assert fm.state == "disabled"


# ── System helpers ───────────────────────────────────────────────────


class TestSystemHelpers:
    def test_total_memory_mb_positive(self):
        mem = get_total_memory_mb()
        assert mem > 0, "should detect physical memory"

    def test_virtual_memory_mb_non_negative(self):
        vmem = get_virtual_memory_mb()
        assert vmem >= 0


# ── getLog command ───────────────────────────────────────────────────


class TestGetLog:
    def test_returns_real_log_lines(self, ctx):
        import logging

        logger = logging.getLogger("smongo.wire.commands")
        logger.info("enterprise test log line")
        resp = dispatch(ctx, {"getLog": "global", "$db": "admin"})
        assert resp["ok"] == 1.0
        assert isinstance(resp["log"], list)
        assert resp["totalLinesWritten"] >= 0

    def test_startup_warnings(self, ctx):
        resp = dispatch(ctx, {"getLog": "startupWarnings", "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["log"] == []

    def test_unknown_type(self, ctx):
        resp = dispatch(ctx, {"getLog": "bogus", "$db": "admin"})
        assert resp["ok"] == 0


# ── buildInfo ────────────────────────────────────────────────────────


class TestBuildInfo:
    def test_gitversion_not_zeros(self, ctx):
        resp = dispatch(ctx, {"buildInfo": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["gitVersion"] != "0000000000000000000000000000000000000000"
        assert len(resp["gitVersion"]) > 0

    def test_has_build_environment(self, ctx):
        resp = dispatch(ctx, {"buildInfo": 1, "$db": "admin"})
        assert "buildEnvironment" in resp
        assert resp["buildEnvironment"]["target_os"] == platform.system().lower()

    def test_has_sys_info(self, ctx):
        resp = dispatch(ctx, {"buildInfo": 1, "$db": "admin"})
        assert "sysInfo" in resp
        assert platform.system() in resp["sysInfo"]


# ── hostInfo ─────────────────────────────────────────────────────────


class TestHostInfo:
    def test_real_memory(self, ctx):
        resp = dispatch(ctx, {"hostInfo": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["system"]["memSizeMB"] > 0

    def test_has_current_time(self, ctx):
        resp = dispatch(ctx, {"hostInfo": 1, "$db": "admin"})
        assert "currentTime" in resp["system"]

    def test_cpu_cores(self, ctx):
        resp = dispatch(ctx, {"hostInfo": 1, "$db": "admin"})
        assert resp["system"]["numCores"] >= 1


# ── getCmdLineOpts ───────────────────────────────────────────────────


class TestGetCmdLineOpts:
    def test_has_real_argv(self, ctx):
        resp = dispatch(ctx, {"getCmdLineOpts": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert isinstance(resp["argv"], list)
        assert len(resp["argv"]) > 0
        assert resp["argv"] == sys.argv


# ── serverStatus ─────────────────────────────────────────────────────


class TestServerStatusEnterprise:
    def test_real_connections(self, ctx):
        resp = dispatch(ctx, {"serverStatus": 1, "$db": "admin"})
        conns = resp["connections"]
        assert "current" in conns
        assert "available" in conns
        assert "totalCreated" in conns
        assert isinstance(conns["current"], int)
        assert isinstance(conns["available"], int)

    def test_virtual_memory(self, ctx):
        resp = dispatch(ctx, {"serverStatus": 1, "$db": "admin"})
        assert "virtual" in resp["mem"]


# ── connPoolStats ────────────────────────────────────────────────────


class TestConnPoolStatsEnterprise:
    def test_uses_real_counter(self, ctx):
        resp = dispatch(ctx, {"connPoolStats": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "totalCreated" in resp
        assert "totalAvailable" in resp
        assert isinstance(resp["totalCreated"], int)


# ── listDatabases ────────────────────────────────────────────────────


class TestListDatabasesEnterprise:
    def test_real_size_on_disk(self, ctx):
        dispatch(
            ctx,
            {
                "insert": "sized_coll",
                "$db": "sizedb",
                "documents": [{"x": i, "data": "A" * 200} for i in range(50)],
            },
        )
        resp = dispatch(ctx, {"listDatabases": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        found = [d for d in resp["databases"] if d["name"] == "sizedb"]
        assert len(found) == 1
        assert found[0]["sizeOnDisk"] > 0
        assert resp["totalSize"] > 0

    def test_name_only(self, ctx):
        dispatch(ctx, {"insert": "x", "$db": "nodb", "documents": [{"a": 1}]})
        resp = dispatch(ctx, {"listDatabases": 1, "nameOnly": True, "$db": "admin"})
        assert resp["ok"] == 1.0
        for db in resp["databases"]:
            assert "name" in db
            assert "sizeOnDisk" not in db


# ── listCollections ──────────────────────────────────────────────────


class TestListCollectionsEnterprise:
    def test_filter(self, ctx):
        dispatch(ctx, {"create": "alpha", "$db": "filterdb"})
        dispatch(ctx, {"create": "beta", "$db": "filterdb"})
        resp = dispatch(
            ctx,
            {
                "listCollections": 1,
                "filter": {"name": "alpha"},
                "$db": "filterdb",
            },
        )
        assert resp["ok"] == 1.0
        names = [e["name"] for e in resp["cursor"]["firstBatch"]]
        assert "alpha" in names
        assert "beta" not in names

    def test_name_only(self, ctx):
        dispatch(ctx, {"create": "coll1", "$db": "noinfodb"})
        resp = dispatch(
            ctx,
            {
                "listCollections": 1,
                "nameOnly": True,
                "$db": "noinfodb",
            },
        )
        for entry in resp["cursor"]["firstBatch"]:
            assert "name" in entry
            assert "info" not in entry


# ── dropIndexes ──────────────────────────────────────────────────────


class TestDropIndexesEnterprise:
    def test_drop_by_list(self, ctx):
        dispatch(ctx, {"create": "idxdrop", "$db": "idxdb"})
        dispatch(
            ctx,
            {
                "createIndexes": "idxdrop",
                "$db": "idxdb",
                "indexes": [
                    {"key": {"a": 1}, "name": "a_1"},
                    {"key": {"b": 1}, "name": "b_1"},
                ],
            },
        )
        resp = dispatch(
            ctx,
            {
                "dropIndexes": "idxdrop",
                "index": ["a_1"],
                "$db": "idxdb",
            },
        )
        assert resp["ok"] == 1.0
        assert resp["nIndexesWas"] >= 3

        idx_resp = dispatch(ctx, {"listIndexes": "idxdrop", "$db": "idxdb"})
        idx_names = [e["name"] for e in idx_resp["cursor"]["firstBatch"]]
        assert "a_1" not in idx_names
        assert "b_1" in idx_names

    def test_drop_missing_param(self, ctx):
        dispatch(ctx, {"create": "idxnone", "$db": "idxdb2"})
        resp = dispatch(ctx, {"dropIndexes": "idxnone", "$db": "idxdb2"})
        assert resp["ok"] == 0


# ── killAllSessions ──────────────────────────────────────────────────


class TestKillAllSessions:
    def test_clears_sessions(self, ctx):
        dispatch(ctx, {"startSession": 1, "$db": "admin"})
        dispatch(ctx, {"startSession": 1, "$db": "admin"})
        assert ctx.session_registry.count > 0
        resp = dispatch(ctx, {"killAllSessions": [], "$db": "admin"})
        assert resp["ok"] == 1.0
        assert ctx.session_registry.count == 0


# ── bulkWrite upsert ─────────────────────────────────────────────────


class TestBulkWriteUpsert:
    def test_upsert_tracks_count(self, ctx):
        resp = dispatch(
            ctx,
            {
                "bulkWrite": 1,
                "$db": "bulkdb",
                "nsInfo": [{"ns": "bulkdb.ups"}],
                "ops": [
                    {
                        "update": 0,
                        "filter": {"_id": "upserted_doc"},
                        "updateMods": {"$set": {"val": 42}},
                        "upsert": True,
                    },
                ],
            },
        )
        assert resp["ok"] == 1.0
        assert resp["nUpserted"] == 1


# ── compact ──────────────────────────────────────────────────────────


class TestCompactEnterprise:
    def test_bytes_freed_measured(self, ctx):
        dispatch(
            ctx,
            {
                "insert": "compacted",
                "$db": "cdb",
                "documents": [{"data": "X" * 500} for _ in range(100)],
            },
        )
        resp = dispatch(ctx, {"compact": "compacted", "$db": "cdb"})
        assert resp["ok"] == 1.0
        assert "bytesFreed" in resp
        assert isinstance(resp["bytesFreed"], int)


# ── dataSize ─────────────────────────────────────────────────────────


class TestDataSizeEnterprise:
    def test_millis_measured(self, ctx):
        dispatch(
            ctx, {"insert": "timed", "$db": "dsdb", "documents": [{"x": i} for i in range(50)]}
        )
        resp = dispatch(ctx, {"dataSize": "dsdb.timed", "$db": "admin"})
        assert resp["ok"] == 1.0
        assert isinstance(resp["millis"], int)
        assert resp["numObjects"] == 50

    def test_key_pattern_scoped(self, ctx):
        dispatch(ctx, {"insert": "kp", "$db": "dsdb", "documents": [{"x": i} for i in range(100)]})
        resp = dispatch(
            ctx,
            {
                "dataSize": "dsdb.kp",
                "$db": "admin",
                "keyPattern": {"x": 1},
                "min": {"x": 10},
                "max": {"x": 50},
            },
        )
        assert resp["ok"] == 1.0
        assert resp["numObjects"] == 40


# ── dbStats ──────────────────────────────────────────────────────────


class TestDbStatsEnterprise:
    def test_finds_all_collections(self, ctx):
        dispatch(ctx, {"insert": "colA", "$db": "dbsdb", "documents": [{"a": 1}]})
        dispatch(ctx, {"insert": "colB", "$db": "dbsdb", "documents": [{"b": 2}]})
        resp = dispatch(ctx, {"dbStats": 1, "$db": "dbsdb"})
        assert resp["ok"] == 1.0
        assert resp["collections"] >= 2
        assert resp["objects"] >= 2
        assert "scaleFactor" in resp


# ── collMod ──────────────────────────────────────────────────────────


class TestCollModEnterprise:
    def test_validation_level_off(self, ctx):
        dispatch(
            ctx,
            {
                "create": "validated",
                "$db": "cmdb",
            },
        )
        dispatch(
            ctx,
            {
                "collMod": "validated",
                "$db": "cmdb",
                "validator": {"$jsonSchema": {"required": ["name"]}},
            },
        )
        # with validator, insert without "name" should fail
        resp = dispatch(
            ctx,
            {
                "insert": "validated",
                "$db": "cmdb",
                "documents": [{"other": "data"}],
            },
        )
        assert resp.get("writeErrors") or resp["ok"] == 0 or resp.get("n", 1) == 0

        # now turn off validation
        dispatch(
            ctx,
            {
                "collMod": "validated",
                "$db": "cmdb",
                "validationLevel": "off",
            },
        )
        resp = dispatch(
            ctx,
            {
                "insert": "validated",
                "$db": "cmdb",
                "documents": [{"other": "data"}],
            },
        )
        assert resp["ok"] == 1.0
        assert resp["n"] >= 1


# ── listCommands ─────────────────────────────────────────────────────


class TestListCommandsEnterprise:
    def test_has_real_help_text(self, ctx):
        resp = dispatch(ctx, {"listCommands": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        cmds = resp["commands"]
        assert "find" in cmds
        assert cmds["find"]["help"] != ""
        assert cmds["insert"]["help"] != ""
        assert cmds["aggregate"]["help"] != ""

    def test_admin_only_marked(self, ctx):
        resp = dispatch(ctx, {"listCommands": 1, "$db": "admin"})
        assert resp["commands"]["serverStatus"]["adminOnly"] is True
        assert resp["commands"]["find"]["adminOnly"] is False


# ── setFreeMonitoring / getFreeMonitoringStatus ──────────────────────


class TestFreeMonitoring:
    def test_enable_disable_cycle(self, ctx):
        resp = dispatch(ctx, {"getFreeMonitoringStatus": 1, "$db": "admin"})
        assert resp["state"] == "disabled"

        resp = dispatch(ctx, {"setFreeMonitoring": 1, "action": "enable", "$db": "admin"})
        assert resp["ok"] == 1.0

        resp = dispatch(ctx, {"getFreeMonitoringStatus": 1, "$db": "admin"})
        assert resp["state"] == "enabled"

        dispatch(ctx, {"setFreeMonitoring": 1, "action": "disable", "$db": "admin"})
        resp = dispatch(ctx, {"getFreeMonitoringStatus": 1, "$db": "admin"})
        assert resp["state"] == "disabled"

    def test_invalid_action(self, ctx):
        resp = dispatch(ctx, {"setFreeMonitoring": 1, "action": "bogus", "$db": "admin"})
        assert resp["ok"] == 0


# ── User management ─────────────────────────────────────────────────


class TestUserManagement:
    @pytest.fixture(autouse=True)
    def _cleanup_users(self):
        yield
        with _USER_STORE_LOCK:
            _USER_STORE.clear()

    def test_create_and_query_user(self, ctx):
        resp = dispatch(
            ctx,
            {
                "createUser": "testuser",
                "pwd": "secret",
                "roles": [{"role": "readWrite", "db": "mydb"}],
                "$db": "mydb",
            },
        )
        assert resp["ok"] == 1.0

        resp = dispatch(ctx, {"usersInfo": "testuser", "$db": "mydb"})
        assert resp["ok"] == 1.0
        assert len(resp["users"]) == 1
        assert resp["users"][0]["user"] == "testuser"
        assert resp["users"][0]["db"] == "mydb"

    def test_duplicate_user_fails(self, ctx):
        dispatch(ctx, {"createUser": "dup", "roles": [], "$db": "test"})
        resp = dispatch(ctx, {"createUser": "dup", "roles": [], "$db": "test"})
        assert resp["ok"] == 0

    def test_drop_user(self, ctx):
        dispatch(ctx, {"createUser": "droppable", "roles": [], "$db": "test"})
        resp = dispatch(ctx, {"dropUser": "droppable", "$db": "test"})
        assert resp["ok"] == 1.0

        resp = dispatch(ctx, {"usersInfo": "droppable", "$db": "test"})
        assert len(resp["users"]) == 0

    def test_drop_nonexistent_user(self, ctx):
        resp = dispatch(ctx, {"dropUser": "ghost", "$db": "test"})
        assert resp["ok"] == 0

    def test_update_user_roles(self, ctx):
        dispatch(
            ctx,
            {
                "createUser": "updatable",
                "roles": [{"role": "read", "db": "test"}],
                "$db": "test",
            },
        )
        resp = dispatch(
            ctx,
            {
                "updateUser": "updatable",
                "roles": [{"role": "readWrite", "db": "test"}],
                "$db": "test",
            },
        )
        assert resp["ok"] == 1.0

        resp = dispatch(ctx, {"usersInfo": "updatable", "$db": "test"})
        assert resp["users"][0]["roles"] == [{"role": "readWrite", "db": "test"}]

    def test_update_nonexistent_user(self, ctx):
        resp = dispatch(ctx, {"updateUser": "nobody", "roles": [], "$db": "test"})
        assert resp["ok"] == 0

    def test_users_info_all(self, ctx):
        dispatch(ctx, {"createUser": "a", "roles": [], "$db": "test"})
        dispatch(ctx, {"createUser": "b", "roles": [], "$db": "test"})
        resp = dispatch(ctx, {"usersInfo": 1, "$db": "test"})
        assert len(resp["users"]) == 2


# ── lockInfo ─────────────────────────────────────────────────────────


class TestLockInfoEnterprise:
    def test_returns_list(self, ctx):
        resp = dispatch(ctx, {"lockInfo": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert isinstance(resp["lockInfo"], list)


# ── getParameter error for unknown ───────────────────────────────────


class TestGetParameterEnterprise:
    def test_known_param_returns_value(self, ctx):
        resp = dispatch(ctx, {"getParameter": "logLevel", "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "logLevel" in resp

    def test_unknown_param_returns_error(self, ctx):
        resp = dispatch(ctx, {"getParameter": "totallyBogus", "$db": "admin"})
        assert resp["ok"] == 0

    def test_all_params(self, ctx):
        resp = dispatch(ctx, {"getParameter": "*", "allParameters": True, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "logLevel" in resp


# ── plan_summary in profiler ─────────────────────────────────────────


class TestPlanSummary:
    def test_find_populates_plan_summary(self, ctx):
        dispatch(
            ctx, {"insert": "plancoll", "$db": "plandb", "documents": [{"x": i} for i in range(5)]}
        )
        dispatch(
            ctx,
            {
                "profile": 2,
                "$db": "plandb",
            },
        )
        dispatch(
            ctx,
            {
                "find": "plancoll",
                "filter": {"x": 3},
                "$db": "plandb",
            },
        )
        resp = dispatch(ctx, {"system.profile": 1, "$db": "plandb"})
        entries = resp["cursor"]["firstBatch"]
        plan_entries = [e for e in entries if e.get("planSummary")]
        assert len(plan_entries) > 0, "profiler should capture plan summary"


# ── saslSupportedMechs ───────────────────────────────────────────────


class TestSaslMechs:
    def test_hello_returns_mechanisms(self, ctx):
        resp = dispatch(ctx, {"hello": 1, "saslSupportedMechs": "test.user", "$db": "admin"})
        assert "SCRAM-SHA-256" in resp["saslSupportedMechs"]
        assert "SCRAM-SHA-1" in resp["saslSupportedMechs"]


# ── whatsmyuri ───────────────────────────────────────────────────────


class TestWhatsMyUri:
    def test_returns_address(self, ctx):
        resp = dispatch(ctx, {"whatsmyuri": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert resp["you"] == "127.0.0.1:50000"
