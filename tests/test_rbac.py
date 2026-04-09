"""Tests for Role-Based Access Control (RBAC).

Covers:
- read role allows find but not insert
- readWrite role allows find and insert
- root role allows everything
- grantRolesToUser adds permissions
- revokeRolesFromUser removes permissions
- connectionStatus returns correct user and roles
- Unauthenticated user is still rejected
"""

from __future__ import annotations

import socket
import time

import pymongo
import pytest

from smongo._smongo_core import RedbLocalClient
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


def _stop_server(server):
    server.stop()
    if hasattr(server, "_local_client") and hasattr(server._local_client, "close"):
        try:
            server._local_client.close()
        except Exception:
            pass


class TestRBAC:
    @pytest.fixture(scope="class")
    def rbac_env(self, tmp_path_factory):
        """Bootstrap server with three users: root, reader, writer."""
        db_path = str(tmp_path_factory.mktemp("rbac"))
        port = _find_free_port()

        lc = RedbLocalClient(db_path)
        server1 = WireServer(db_path, "127.0.0.1", port, auth_required=False, local_client=lc)
        server1.start()
        _wait_for_port("127.0.0.1", port)

        mc = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}", directConnection=True)
        mc.admin.command(
            "createUser",
            "superadmin",
            pwd="rootpw",
            roles=[{"role": "root", "db": "admin"}],
        )
        mc.admin.command(
            "createUser",
            "reader",
            pwd="readpw",
            roles=[{"role": "read", "db": "testdb"}],
        )
        mc.admin.command(
            "createUser",
            "writer",
            pwd="writepw",
            roles=[{"role": "readWrite", "db": "testdb"}],
        )
        mc.close()
        server1.stop()
        lc.close()
        time.sleep(0.5)

        server2 = WireServer(db_path, "127.0.0.1", port, auth_required=True)
        server2.start()
        _wait_for_port("127.0.0.1", port)
        yield port
        _stop_server(server2)

    def test_root_can_do_everything(self, rbac_env):
        port = rbac_env
        mc = pymongo.MongoClient(
            f"mongodb://superadmin:rootpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        mc.admin.command("ping")
        mc.testdb.rbac_col.insert_one({"from": "root"})
        doc = mc.testdb.rbac_col.find_one({"from": "root"})
        assert doc is not None
        mc.close()

    def test_reader_can_find(self, rbac_env):
        port = rbac_env
        # Insert data as root first
        root = pymongo.MongoClient(
            f"mongodb://superadmin:rootpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        root.testdb.reader_test.insert_one({"data": "hello"})
        root.close()

        mc = pymongo.MongoClient(
            f"mongodb://reader:readpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        doc = mc.testdb.reader_test.find_one({"data": "hello"})
        assert doc is not None
        mc.close()

    def test_reader_cannot_insert(self, rbac_env):
        port = rbac_env
        mc = pymongo.MongoClient(
            f"mongodb://reader:readpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        with pytest.raises(pymongo.errors.OperationFailure, match="not authorized"):
            mc.testdb.denied_col.insert_one({"denied": True})
        mc.close()

    def test_writer_can_insert_and_find(self, rbac_env):
        port = rbac_env
        mc = pymongo.MongoClient(
            f"mongodb://writer:writepw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        mc.testdb.writer_test.insert_one({"data": "written"})
        doc = mc.testdb.writer_test.find_one({"data": "written"})
        assert doc is not None
        mc.close()

    def test_writer_cannot_create_user(self, rbac_env):
        port = rbac_env
        mc = pymongo.MongoClient(
            f"mongodb://writer:writepw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        with pytest.raises(pymongo.errors.OperationFailure, match="not authorized"):
            mc.testdb.command("createUser", "evil", pwd="hack", roles=[])
        mc.close()

    def test_connection_status_shows_roles(self, rbac_env):
        port = rbac_env
        mc = pymongo.MongoClient(
            f"mongodb://reader:readpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        status = mc.admin.command("connectionStatus")
        auth_info = status["authInfo"]
        users = auth_info["authenticatedUsers"]
        roles = auth_info["authenticatedUserRoles"]
        assert len(users) == 1
        assert users[0]["user"] == "reader"
        assert len(roles) >= 1
        role_names = [r["role"] for r in roles]
        assert "read" in role_names
        mc.close()

    def test_unauthenticated_rejected(self, rbac_env):
        port = rbac_env
        mc = pymongo.MongoClient(
            f"mongodb://127.0.0.1:{port}",
            directConnection=True,
        )
        with pytest.raises(pymongo.errors.OperationFailure, match="authentication"):
            mc.testdb.foo.insert_one({"x": 1})
        mc.close()


class TestGrantRevoke:
    @pytest.fixture(scope="class")
    def grant_env(self, tmp_path_factory):
        """Bootstrap server with a root user and a user with no roles."""
        db_path = str(tmp_path_factory.mktemp("grant"))
        port = _find_free_port()

        lc = RedbLocalClient(db_path)
        server1 = WireServer(db_path, "127.0.0.1", port, auth_required=False, local_client=lc)
        server1.start()
        _wait_for_port("127.0.0.1", port)

        mc = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}", directConnection=True)
        mc.admin.command(
            "createUser",
            "admin",
            pwd="adminpw",
            roles=[{"role": "root", "db": "admin"}],
        )
        mc.admin.command(
            "createUser",
            "noroles",
            pwd="norpw",
            roles=[],
        )
        mc.close()
        server1.stop()
        lc.close()
        time.sleep(0.5)

        server2 = WireServer(db_path, "127.0.0.1", port, auth_required=True)
        server2.start()
        _wait_for_port("127.0.0.1", port)
        yield port
        _stop_server(server2)

    def test_grant_then_use(self, grant_env):
        port = grant_env

        # noroles user can't insert
        mc = pymongo.MongoClient(
            f"mongodb://noroles:norpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        with pytest.raises(pymongo.errors.OperationFailure, match="not authorized"):
            mc.testdb.granttest.insert_one({"x": 1})
        mc.close()

        # Grant readWrite as root
        root = pymongo.MongoClient(
            f"mongodb://admin:adminpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        root.admin.command(
            "grantRolesToUser",
            "noroles",
            roles=[{"role": "readWrite", "db": "testdb"}],
        )
        root.close()

        # Now noroles can insert (new connection picks up updated roles)
        mc = pymongo.MongoClient(
            f"mongodb://noroles:norpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        mc.testdb.granttest.insert_one({"x": 1})
        doc = mc.testdb.granttest.find_one({"x": 1})
        assert doc is not None
        mc.close()

    def test_revoke_removes_access(self, grant_env):
        port = grant_env

        # Revoke the role as root
        root = pymongo.MongoClient(
            f"mongodb://admin:adminpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        root.admin.command(
            "revokeRolesFromUser",
            "noroles",
            roles=[{"role": "readWrite", "db": "testdb"}],
        )
        root.close()

        # Now noroles can't insert again
        mc = pymongo.MongoClient(
            f"mongodb://noroles:norpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        with pytest.raises(pymongo.errors.OperationFailure, match="not authorized"):
            mc.testdb.granttest.insert_one({"y": 2})
        mc.close()
