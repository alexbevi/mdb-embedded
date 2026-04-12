import logging
import uuid
from unittest.mock import MagicMock

import pytest
from bson import Binary, Int64

import smongo.wire.commands  # noqa: F401
from smongo.wire.commands.diagnostic import (
    _cmd_client_sync,
    _cmd_conn_pool_stats,
    _cmd_current_op,
    _cmd_features,
    _cmd_kill_op,
    _cmd_list_commands,
    _cmd_lock_info,
    _cmd_log_rotate,
    _cmd_profile,
    _cmd_read_profile,
    _cmd_repl_get_config,
    _cmd_repl_status,
    _cmd_set_free_monitoring,
    _cmd_set_profiling,
    _cmd_sharding_state,
    _cmd_top,
)
from smongo.wire.commands.handshake import (
    _cmd_build_info,
    _cmd_cmdline_opts,
    _cmd_conn_status,
    _cmd_free_monitoring,
    _cmd_get_log,
    _cmd_hello,
    _cmd_host_info,
    _cmd_logout,
    _cmd_ping,
    _cmd_sasl_continue,
    _cmd_sasl_start,
    _cmd_whatsmyuri,
)
from smongo.wire.commands.indexes import (
    _cmd_create_indexes,
    _cmd_drop_indexes,
    _cmd_list_indexes,
    _cmd_reindex,
)
from smongo.wire.commands.sessions import (
    _cmd_abort_txn,
    _cmd_commit_txn,
    _cmd_end_sessions,
    _cmd_kill_all_sessions,
    _cmd_kill_sessions,
    _cmd_refresh_sessions,
    _cmd_start_session,
    _cmd_start_txn,
)
from smongo.wire.commands.users import (
    _USER_STORE,
    _USER_STORE_LOCK,
    _cmd_create_user,
    _cmd_drop_user,
    _cmd_roles_info,
    _cmd_update_user,
    _cmd_users_info,
)
from smongo.wire.context import ConnectionContext
from smongo.wire.cursors import CursorRegistry

SEQ: dict = {}


@pytest.fixture(autouse=True)
def clear_user_store():
    with _USER_STORE_LOCK:
        _USER_STORE.clear()
    yield
    with _USER_STORE_LOCK:
        _USER_STORE.clear()


@pytest.fixture
def cursor_registry():
    return CursorRegistry(default_batch_size=101)


@pytest.fixture
def wire_ctx(local_client, cursor_registry):
    return ConnectionContext(
        local_client,
        connection_id=1,
        address=("127.0.0.1", 50000),
        cursor_registry=cursor_registry,
    )


@pytest.fixture
def diag_ctx(local_client, cursor_registry):
    ot = MagicMock()
    ot.active_ops.return_value = [
        {"connectionId": 1, "ns": "db.c1", "op": "query"},
        {"connectionId": 2, "ns": "db.c2", "op": "insert"},
        {"connectionId": 1, "ns": "db.c3", "op": "getmore"},
    ]
    ot.kill_op.return_value = True
    cc = MagicMock()
    cc.snapshot.return_value = {"current": 3, "available": 9, "totalCreated": 12}
    prof = MagicMock()
    prof.level = 0
    prof.slow_ms = 100
    prof.get_entries.return_value = [{"op": "find"}]
    top = MagicMock()
    top.snapshot.return_value = {"ns": {"time": 1}}
    lb = MagicMock()
    lb.get_lines.return_value = (["log line"], 7)
    fm = MagicMock()
    fm.state = "disabled"
    return ConnectionContext(
        local_client,
        connection_id=1,
        address=("127.0.0.1", 12345),
        cursor_registry=cursor_registry,
        op_tracker=ot,
        conn_counter=cc,
        profiler=prof,
        top_stats=top,
        log_buffer=lb,
        free_monitoring=fm,
    )


def assert_ok(doc):
    assert doc.get("ok") == 1.0


def assert_err(doc):
    assert doc.get("ok") == 0


class TestUsers:
    def test_users_info_string_hit(self):
        with _USER_STORE_LOCK:
            _USER_STORE["test.alice"] = {"user": "alice", "db": "test"}
        r = _cmd_users_info(MagicMock(), {"usersInfo": "alice", "$db": "test"}, SEQ)
        assert_ok(r)
        assert len(r["users"]) == 1

    def test_users_info_string_miss(self):
        r = _cmd_users_info(MagicMock(), {"usersInfo": "nobody", "$db": "test"}, SEQ)
        assert_ok(r)
        assert r["users"] == []

    def test_users_info_dict_hit(self):
        with _USER_STORE_LOCK:
            _USER_STORE["other.bob"] = {"user": "bob", "db": "other"}
        r = _cmd_users_info(
            MagicMock(),
            {"usersInfo": {"user": "bob", "db": "other"}, "$db": "test"},
            SEQ,
        )
        assert_ok(r)
        assert len(r["users"]) == 1

    def test_users_info_dict_default_db(self):
        with _USER_STORE_LOCK:
            _USER_STORE["test.carol"] = {"user": "carol", "db": "test"}
        r = _cmd_users_info(
            MagicMock(),
            {"usersInfo": {"user": "carol"}, "$db": "test"},
            SEQ,
        )
        assert_ok(r)
        assert len(r["users"]) == 1

    def test_users_info_true_lists_db(self):
        with _USER_STORE_LOCK:
            _USER_STORE["test.u1"] = {"u": 1}
            _USER_STORE["other.u2"] = {"u": 2}
        r = _cmd_users_info(MagicMock(), {"usersInfo": True, "$db": "test"}, SEQ)
        assert_ok(r)
        assert len(r["users"]) == 1

    def test_users_info_one_lists_db(self):
        with _USER_STORE_LOCK:
            _USER_STORE["test.v1"] = {"v": 1}
        r = _cmd_users_info(MagicMock(), {"usersInfo": 1, "$db": "test"}, SEQ)
        assert_ok(r)
        assert len(r["users"]) == 1

    def test_users_info_all_other_target(self):
        with _USER_STORE_LOCK:
            _USER_STORE["a.b"] = {"x": 1}
        r = _cmd_users_info(MagicMock(), {"usersInfo": 2, "$db": "test"}, SEQ)
        assert_ok(r)
        assert len(r["users"]) == 1

    def test_roles_info_dict_builtin(self):
        r = _cmd_roles_info(
            MagicMock(),
            {"rolesInfo": {"showBuiltinRoles": True}},
            SEQ,
        )
        assert_ok(r)
        assert len(r["roles"]) == 5

    def test_roles_info_dict_no_builtin(self):
        r = _cmd_roles_info(MagicMock(), {"rolesInfo": {"showBuiltinRoles": False}}, SEQ)
        assert_ok(r)
        assert r["roles"] == []

    def test_roles_info_int_one_cmd_flag(self):
        r = _cmd_roles_info(
            MagicMock(),
            {"rolesInfo": 1, "showBuiltinRoles": True},
            SEQ,
        )
        assert_ok(r)
        assert len(r["roles"]) == 5

    def test_create_drop_update_flow(self):
        assert_ok(
            _cmd_create_user(MagicMock(), {"createUser": "u", "$db": "db1", "roles": []}, SEQ)
        )
        assert_err(
            _cmd_create_user(MagicMock(), {"createUser": "u", "$db": "db1"}, SEQ),
        )
        assert_ok(
            _cmd_update_user(MagicMock(), {"updateUser": "u", "$db": "db1", "roles": ["r"]}, SEQ)
        )
        assert_ok(
            _cmd_update_user(
                MagicMock(),
                {"updateUser": "u", "$db": "db1", "mechanisms": ["PLAIN"]},
                SEQ,
            ),
        )
        assert_err(_cmd_update_user(MagicMock(), {"updateUser": "x", "$db": "db1"}, SEQ))
        assert_ok(_cmd_drop_user(MagicMock(), {"dropUser": "u", "$db": "db1"}, SEQ))
        assert_err(_cmd_drop_user(MagicMock(), {"dropUser": "u", "$db": "db1"}, SEQ))

    def test_create_user_empty_name(self):
        assert_err(_cmd_create_user(MagicMock(), {"createUser": "", "$db": "t"}, SEQ))


class TestDiagnostic:
    def test_current_op_filter_own(self, diag_ctx):
        r = _cmd_current_op(diag_ctx, {"currentOp": 1, "$all": False}, SEQ)
        assert_ok(r)
        assert len(r["inprog"]) == 2

    def test_current_op_all(self, diag_ctx):
        r = _cmd_current_op(diag_ctx, {"currentOp": 1, "$all": True}, SEQ)
        assert_ok(r)
        assert len(r["inprog"]) == 3

    def test_kill_op_missing(self, diag_ctx):
        assert_err(_cmd_kill_op(diag_ctx, {"killOp": 1}, SEQ))

    def test_kill_op_found(self, diag_ctx):
        r = _cmd_kill_op(diag_ctx, {"killOp": 1, "op": 99}, SEQ)
        assert_ok(r)
        assert "attempting" in r["info"]

    def test_kill_op_not_found(self, diag_ctx):
        diag_ctx.op_tracker.kill_op.return_value = False
        r = _cmd_kill_op(diag_ctx, {"killOp": 1, "op": 1}, SEQ)
        assert_ok(r)
        assert "not found" in r["info"]

    def test_conn_pool_stats(self, diag_ctx):
        r = _cmd_conn_pool_stats(diag_ctx, {"connPoolStats": 1}, SEQ)
        assert_ok(r)
        assert r["numClientConnections"] == 3

    def test_features(self, diag_ctx):
        assert_ok(_cmd_features(diag_ctx, {"features": 1}, SEQ))

    def test_log_rotate(self, diag_ctx):
        h = MagicMock()
        h.doRollover = MagicMock()
        logging.root.addHandler(h)
        try:
            assert_ok(_cmd_log_rotate(diag_ctx, {"logRotate": 1}, SEQ))
            h.doRollover.assert_called()
        finally:
            logging.root.removeHandler(h)

    def test_top(self, diag_ctx):
        assert_ok(_cmd_top(diag_ctx, {"top": 1}, SEQ))

    def test_profile_levels_and_slowms(self, diag_ctx):
        r = _cmd_profile(
            diag_ctx,
            {"profile": 2, "slowms": 50},
            SEQ,
        )
        assert_ok(r)
        assert r["was"] == 0
        assert diag_ctx.profiler.level == 2
        assert diag_ctx.profiler.slow_ms == 50

    def test_profile_ignores_invalid_level(self, diag_ctx):
        diag_ctx.profiler.level = 1
        r = _cmd_profile(diag_ctx, {"profile": 9}, SEQ)
        assert_ok(r)
        assert r["was"] == 1
        assert diag_ctx.profiler.level == 1

    def test_set_profiling_level(self, diag_ctx):
        assert_ok(_cmd_set_profiling(diag_ctx, {"profile": 0}, SEQ))

    def test_read_profile(self, diag_ctx):
        r = _cmd_read_profile(diag_ctx, {"system.profile": 1, "limit": 5}, SEQ)
        assert_ok(r)
        diag_ctx.profiler.get_entries.assert_called_with(5)
        assert r["cursor"]["firstBatch"] == [{"op": "find"}]

    def test_sharding_state(self, diag_ctx):
        assert_ok(_cmd_sharding_state(diag_ctx, {"shardingState": 1}, SEQ))

    def test_repl_set_get_config(self, diag_ctx):
        r = _cmd_repl_get_config(diag_ctx, {"replSetGetConfig": 1}, SEQ)
        assert_err(r)

    def test_repl_set_get_status_no_sync(self, diag_ctx):
        r = _cmd_repl_status(diag_ctx, {"replSetGetStatus": 1}, SEQ)
        assert_err(r)

    def test_repl_set_get_status_with_sync(self, diag_ctx):
        sm = MagicMock()
        sm.status.return_value = {"ok": True}
        ctx = ConnectionContext(
            diag_ctx.local_client,
            connection_id=1,
            address=("127.0.0.1", 1),
            cursor_registry=diag_ctx.cursor_registry,
            sync_mgr=sm,
            op_tracker=diag_ctx.op_tracker,
        )
        r = _cmd_repl_status(ctx, {"replSetGetStatus": 1}, SEQ)
        assert_ok(r)
        assert r["set"] == "smongo"

    def test_repl_set_get_status_sync_raises(self, diag_ctx):
        sm = MagicMock()
        sm.status.side_effect = RuntimeError("boom")
        ctx = ConnectionContext(
            diag_ctx.local_client,
            connection_id=1,
            address=("127.0.0.1", 1),
            cursor_registry=diag_ctx.cursor_registry,
            sync_mgr=sm,
        )
        r = _cmd_repl_status(ctx, {"replSetGetStatus": 1}, SEQ)
        assert_err(r)

    def test_set_free_monitoring_enable_disable_invalid(self, diag_ctx):
        assert_ok(
            _cmd_set_free_monitoring(diag_ctx, {"setFreeMonitoring": 1, "action": "enable"}, SEQ)
        )
        assert_ok(
            _cmd_set_free_monitoring(diag_ctx, {"setFreeMonitoring": 1, "action": "disable"}, SEQ)
        )
        assert_err(
            _cmd_set_free_monitoring(diag_ctx, {"setFreeMonitoring": 1, "action": "nope"}, SEQ)
        )

    def test_lock_info(self, diag_ctx):
        r = _cmd_lock_info(diag_ctx, {"lockInfo": 1}, SEQ)
        assert_ok(r)
        assert len(r["lockInfo"]) == 3
        modes = [g["mode"] for e in r["lockInfo"] for g in e["granted"]]
        assert "IS" in modes and "IX" in modes

    def test_list_commands(self, diag_ctx):
        r = _cmd_list_commands(diag_ctx, {"listCommands": 1}, SEQ)
        assert_ok(r)
        assert "find" in r["commands"]

    def test_client_sync_none(self, diag_ctx):
        r = _cmd_client_sync(diag_ctx, {"client.sync": 1}, SEQ)
        assert_ok(r)
        assert r["sync"] is None

    def test_client_sync_ok(self, diag_ctx):
        sm = MagicMock()
        sm.status.return_value = {"x": 1}
        ctx = ConnectionContext(
            diag_ctx.local_client,
            connection_id=1,
            address=("127.0.0.1", 1),
            cursor_registry=diag_ctx.cursor_registry,
            sync_mgr=sm,
        )
        r = _cmd_client_sync(ctx, {"client.sync": 1}, SEQ)
        assert_ok(r)
        assert r["sync"] == {"x": 1}

    def test_client_sync_error(self, diag_ctx):
        sm = MagicMock()
        sm.status.side_effect = OSError("down")
        ctx = ConnectionContext(
            diag_ctx.local_client,
            connection_id=1,
            address=("127.0.0.1", 1),
            cursor_registry=diag_ctx.cursor_registry,
            sync_mgr=sm,
        )
        r = _cmd_client_sync(ctx, {"client.sync": 1}, SEQ)
        assert_err(r)
        assert r["code"] == 1


class TestHandshake:
    def test_hello_compression_sets_id(self, wire_ctx, monkeypatch):
        monkeypatch.setattr(
            "smongo.wire.commands.handshake.available_compressors",
            lambda: ["zlib"],
        )
        r = _cmd_hello(wire_ctx, {"hello": 1, "compression": ["zlib"], "$db": "admin"}, SEQ)
        assert_ok(r)
        assert r["compression"] == ["zlib"]
        assert wire_ctx.compressor_id is not None

    def test_hello_compression_skips_if_negotiated(self, wire_ctx, monkeypatch):
        monkeypatch.setattr(
            "smongo.wire.commands.handshake.available_compressors",
            lambda: ["zlib"],
        )
        wire_ctx.compressor_id = 2
        before = wire_ctx.compressor_id
        r = _cmd_hello(wire_ctx, {"hello": 1, "compression": ["zlib"], "$db": "admin"}, SEQ)
        assert_ok(r)
        assert wire_ctx.compressor_id == before

    def test_hello_hello_ok_sasl(self, wire_ctx, monkeypatch):
        monkeypatch.setattr(
            "smongo.wire.commands.handshake.available_compressors",
            lambda: [],
        )
        r = _cmd_hello(
            wire_ctx,
            {"hello": 1, "helloOk": True, "saslSupportedMechs": "x.y", "$db": "admin"},
            SEQ,
        )
        assert_ok(r)
        assert r.get("helloOk") is True
        assert r["saslSupportedMechs"] == ["SCRAM-SHA-1", "SCRAM-SHA-256"]

    def test_ping_buildinfo_buildinfo_alias(self, wire_ctx):
        assert_ok(_cmd_ping(wire_ctx, {"ping": 1}, SEQ))
        assert_ok(_cmd_build_info(wire_ctx, {"buildInfo": 1}, SEQ))
        assert_ok(_cmd_build_info(wire_ctx, {"buildinfo": 1}, SEQ))

    def test_get_log_global_star_startup_unknown(self, wire_ctx):
        assert_ok(_cmd_get_log(wire_ctx, {"getLog": "global"}, SEQ))
        assert_ok(_cmd_get_log(wire_ctx, {"getLog": "*"}, SEQ))
        assert_ok(_cmd_get_log(wire_ctx, {"getLog": "startupWarnings"}, SEQ))
        assert_err(_cmd_get_log(wire_ctx, {"getLog": "nope"}, SEQ))

    def test_free_monitoring_host_cmdline_conn_whatsmyuri(self, wire_ctx):
        assert_ok(_cmd_free_monitoring(wire_ctx, {"getFreeMonitoringStatus": 1}, SEQ))
        assert_ok(_cmd_host_info(wire_ctx, {"hostInfo": 1}, SEQ))
        assert_ok(_cmd_cmdline_opts(wire_ctx, {"getCmdLineOpts": 1}, SEQ))
        assert_ok(_cmd_conn_status(wire_ctx, {"connectionStatus": 1}, SEQ))
        r = _cmd_whatsmyuri(wire_ctx, {"whatsmyuri": 1}, SEQ)
        assert_ok(r)
        assert r["you"] == "127.0.0.1:50000"

    def test_sasl_and_logout(self, wire_ctx):
        assert_err(_cmd_sasl_start(wire_ctx, {"saslStart": 1}, SEQ))
        assert_err(_cmd_sasl_continue(wire_ctx, {"saslContinue": 1}, SEQ))
        assert_ok(_cmd_logout(wire_ctx, {"logout": 1}, SEQ))


class TestSessions:
    def test_start_end_refresh_kill_expire(self, wire_ctx):
        assert_ok(_cmd_start_session(wire_ctx, {"startSession": 1}, SEQ))
        sid = {"id": Binary(uuid.uuid4().bytes, subtype=4)}
        assert_ok(_cmd_end_sessions(wire_ctx, {"endSessions": [sid]}, SEQ))
        assert_ok(_cmd_refresh_sessions(wire_ctx, {"refreshSessions": [sid]}, SEQ))
        assert_ok(_cmd_kill_sessions(wire_ctx, {"killSessions": [sid]}, SEQ))
        assert_ok(_cmd_kill_all_sessions(wire_ctx, {"killAllSessions": 1}, SEQ))

    def test_abort_commit_start_txn_lsid_errors(self, wire_ctx):
        assert_err(_cmd_abort_txn(wire_ctx, {"abortTransaction": 1}, SEQ))
        assert_err(_cmd_commit_txn(wire_ctx, {"commitTransaction": 1}, SEQ))
        assert_err(_cmd_start_txn(wire_ctx, {"startTransaction": 1}, SEQ))

    def test_abort_commit_start_txn_with_lsid(self, wire_ctx):
        lsid = {"id": Binary(uuid.uuid4().bytes, subtype=4)}
        assert_ok(_cmd_start_txn(wire_ctx, {"startTransaction": 1, "lsid": lsid}, SEQ))
        assert_ok(_cmd_commit_txn(wire_ctx, {"commitTransaction": 1, "lsid": lsid}, SEQ))
        assert_ok(_cmd_start_txn(wire_ctx, {"startTransaction": 1, "lsid": lsid}, SEQ))
        assert_ok(_cmd_abort_txn(wire_ctx, {"abortTransaction": 1, "lsid": lsid}, SEQ))


class TestIndexes:
    def test_list_create_drop_reindex_integration(self, wire_ctx):
        from datetime import UTC, datetime

        db = "idb"
        cl = "icol"
        c = wire_ctx.local_client.get_db(db).get_collection(cl)
        c.insert_one({"a": 1, "ttl": datetime.now(UTC)})
        assert_ok(
            _cmd_create_indexes(
                wire_ctx,
                {
                    "createIndexes": cl,
                    "$db": db,
                    "indexes": [
                        {"key": {"a": 1}, "name": "a_1"},
                        {
                            "key": {"ttl": 1},
                            "name": "ttl_1",
                            "expireAfterSeconds": 3600,
                        },
                    ],
                },
                SEQ,
            ),
        )
        r = _cmd_list_indexes(
            wire_ctx,
            {"listIndexes": cl, "$db": db, "cursor": {"batchSize": 2}},
            SEQ,
        )
        assert_ok(r)
        assert isinstance(r["cursor"]["id"], Int64)
        assert len(r["cursor"]["firstBatch"]) >= 2
        assert_ok(_cmd_drop_indexes(wire_ctx, {"dropIndexes": cl, "$db": db, "index": "a_1"}, SEQ))
        assert_ok(_cmd_drop_indexes(wire_ctx, {"dropIndexes": cl, "$db": db, "index": "*"}, SEQ))
        assert_ok(_cmd_reindex(wire_ctx, {"reIndex": cl, "$db": db}, SEQ))

    def test_drop_indexes_list_dict_none(self, wire_ctx, monkeypatch):
        mock_coll = MagicMock()
        mock_coll.list_indexes.return_value = [
            {"name": "n1", "keys": {"f": 1}},
            {"name": "n2", "keys": {"g": 1}},
        ]

        def fake_gc(self, db_name, coll_name):
            return mock_coll

        monkeypatch.setattr(ConnectionContext, "get_collection", fake_gc)
        assert_ok(
            _cmd_drop_indexes(
                wire_ctx,
                {"dropIndexes": "c", "$db": "d", "index": ["n1", 9, "n2"]},
                SEQ,
            ),
        )
        row = [{"name": "k1", "keys": {"z": 1}}]
        mock_coll.list_indexes.side_effect = [row, row]
        assert_ok(
            _cmd_drop_indexes(
                wire_ctx,
                {"dropIndexes": "c", "$db": "d", "index": {"z": 1}},
                SEQ,
            ),
        )
        mock_coll.list_indexes.side_effect = None
        mock_coll.list_indexes.return_value = []
        assert_err(_cmd_drop_indexes(wire_ctx, {"dropIndexes": "c", "$db": "d"}, SEQ))

    def test_list_indexes_formats_flags_via_mock_coll(self, wire_ctx, monkeypatch):
        mock_coll = MagicMock()
        mock_coll.list_indexes.return_value = [
            {
                "name": "q",
                "keys": {"q": 1},
                "unique": True,
                "sparse": True,
                "expireAfterSeconds": 42,
            },
        ]

        def fake_gc(self, db_name, coll_name):
            return mock_coll

        monkeypatch.setattr(ConnectionContext, "get_collection", fake_gc)
        r = _cmd_list_indexes(wire_ctx, {"listIndexes": "c", "$db": "db"}, SEQ)
        assert_ok(r)
        batch = r["cursor"]["firstBatch"]
        extra = batch[1]
        assert extra["unique"] is True
        assert extra["sparse"] is True
        assert extra["expireAfterSeconds"] == 42

    def test_create_indexes_mock(self, wire_ctx, monkeypatch):
        mock_coll = MagicMock()
        mock_coll.list_indexes.side_effect = [[], [{"x": 1}], [{"x": 1}, {"y": 1}]]

        def fake_gc(self, db_name, coll_name):
            return mock_coll

        monkeypatch.setattr(ConnectionContext, "get_collection", fake_gc)
        assert_ok(
            _cmd_create_indexes(
                wire_ctx,
                {
                    "createIndexes": "c",
                    "$db": "d",
                    "indexes": [
                        {
                            "key": {"f": 1},
                            "name": "custom",
                            "unique": True,
                            "sparse": True,
                            "expireAfterSeconds": 10,
                        },
                    ],
                },
                SEQ,
            ),
        )
        mock_coll.create_index.assert_called_once()
        args, kwargs = mock_coll.create_index.call_args
        assert args[0] == [("f", 1)]
        assert kwargs == {
            "name": "custom",
            "unique": True,
            "sparse": True,
            "expireAfterSeconds": 10,
        }
