"""Unit tests for wire protocol session and transaction handling."""

import uuid

import pytest
from bson import Binary

from smongo.wire.commands import dispatch
from smongo.wire.context import ConnectionContext, SessionRegistry
from smongo.wire.cursors import CursorRegistry


@pytest.fixture
def cursor_registry():
    return CursorRegistry(default_batch_size=101)


@pytest.fixture
def session_registry():
    return SessionRegistry()


@pytest.fixture
def ctx(local_client, cursor_registry, session_registry):
    c = ConnectionContext(
        local_client,
        connection_id=1,
        address=("127.0.0.1", 50000),
        cursor_registry=cursor_registry,
        session_registry=session_registry,
    )
    yield c
    from smongo.storage.transaction import _txn_state

    _txn_state.session = None


class TestSessionRegistry:
    def test_create_session(self):
        reg = SessionRegistry()
        sid = reg.create()
        assert isinstance(sid, uuid.UUID)
        assert reg.count == 1

    def test_touch_session(self):
        reg = SessionRegistry()
        sid = reg.create()
        reg.touch({"id": str(sid)})
        assert reg.count == 1

    def test_end_sessions(self):
        reg = SessionRegistry()
        sid = reg.create()
        reg.end([{"id": str(sid)}])
        assert reg.count == 0

    def test_end_unknown_session_is_noop(self):
        reg = SessionRegistry()
        reg.end([{"id": "nonexistent"}])
        assert reg.count == 0

    def test_kill_sessions(self):
        reg = SessionRegistry()
        sid = reg.create()
        reg.kill([{"id": str(sid)}])
        assert reg.count == 0

    def test_refresh_sessions(self):
        reg = SessionRegistry()
        sid = reg.create()
        reg.refresh([{"id": str(sid)}])
        assert reg.count == 1

    def test_expire_removes_old_sessions(self):
        reg = SessionRegistry(timeout_minutes=0)
        reg.create()
        import time

        time.sleep(0.01)
        reg.expire()
        assert reg.count == 0


class TestStartSessionCommand:
    def test_start_session_returns_id(self, ctx):
        resp = dispatch(ctx, {"startSession": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "id" in resp
        assert "id" in resp["id"]
        assert resp["timeoutMinutes"] == 30

    def test_start_session_increments_registry(self, ctx):
        before = ctx.session_registry.count
        dispatch(ctx, {"startSession": 1, "$db": "admin"})
        assert ctx.session_registry.count == before + 1


class TestEndSessionsCommand:
    def test_end_sessions_ok(self, ctx):
        resp = dispatch(ctx, {"endSessions": [], "$db": "admin"})
        assert resp["ok"] == 1.0

    def test_end_sessions_with_ids(self, ctx):
        start_resp = dispatch(ctx, {"startSession": 1, "$db": "admin"})
        sid = start_resp["id"]
        resp = dispatch(ctx, {"endSessions": [sid], "$db": "admin"})
        assert resp["ok"] == 1.0


class TestRefreshSessionsCommand:
    def test_refresh_sessions_ok(self, ctx):
        resp = dispatch(ctx, {"refreshSessions": [], "$db": "admin"})
        assert resp["ok"] == 1.0


class TestKillSessionsCommand:
    def test_kill_sessions_ok(self, ctx):
        resp = dispatch(ctx, {"killSessions": [], "$db": "admin"})
        assert resp["ok"] == 1.0

    def test_kill_all_sessions_ok(self, ctx):
        resp = dispatch(ctx, {"killAllSessions": [], "$db": "admin"})
        assert resp["ok"] == 1.0


class TestTransactionCommands:
    def test_abort_requires_lsid(self, ctx):
        resp = dispatch(ctx, {"abortTransaction": 1, "$db": "admin"})
        assert resp["ok"] == 0

    def test_commit_requires_lsid(self, ctx):
        resp = dispatch(ctx, {"commitTransaction": 1, "$db": "admin"})
        assert resp["ok"] == 0

    def test_full_start_commit_cycle(self, ctx):
        lsid = {"id": Binary(uuid.uuid4().bytes, subtype=4)}
        resp = dispatch(ctx, {"startTransaction": 1, "$db": "admin", "lsid": lsid})
        assert resp["ok"] == 1.0
        resp = dispatch(ctx, {"commitTransaction": 1, "$db": "admin", "lsid": lsid})
        assert resp["ok"] == 1.0

    def test_full_start_abort_cycle(self, ctx):
        lsid = {"id": Binary(uuid.uuid4().bytes, subtype=4)}
        resp = dispatch(ctx, {"startTransaction": 1, "$db": "admin", "lsid": lsid})
        assert resp["ok"] == 1.0
        resp = dispatch(ctx, {"abortTransaction": 1, "$db": "admin", "lsid": lsid})
        assert resp["ok"] == 1.0


class TestLsidOperationTime:
    def test_lsid_adds_operation_time(self, ctx):
        lsid = {"id": Binary(uuid.uuid4().bytes, subtype=4)}
        resp = dispatch(ctx, {"ping": 1, "$db": "admin", "lsid": lsid})
        assert resp["ok"] == 1.0
        assert "operationTime" in resp
        assert "$clusterTime" in resp

    def test_no_lsid_still_has_operation_time(self, ctx):
        resp = dispatch(ctx, {"ping": 1, "$db": "admin"})
        assert resp["ok"] == 1.0
        assert "operationTime" in resp
        assert "$clusterTime" in resp

    def test_cluster_time_has_signature(self, ctx):
        lsid = {"id": Binary(uuid.uuid4().bytes, subtype=4)}
        resp = dispatch(ctx, {"ping": 1, "$db": "admin", "lsid": lsid})
        ct = resp["$clusterTime"]
        assert "clusterTime" in ct
        assert "signature" in ct
        assert "hash" in ct["signature"]
        assert "keyId" in ct["signature"]
