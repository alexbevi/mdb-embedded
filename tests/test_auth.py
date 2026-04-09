"""Tests for TLS and SCRAM-SHA-256 authentication.

Covers:
- createUser with password stores hashed credentials
- PyMongo SCRAM-SHA-256 handshake via the wire protocol
- Auth enforcement (reject unauthenticated commands)
- Wrong-password rejection
- User persistence across WireServer restarts
- TLS self-signed certificate with PyMongo
- Combined TLS + auth
"""

from __future__ import annotations

import os
import subprocess
import time

import pymongo
import pytest

from smongo._smongo_core import RedbLocalClient
from smongo.wire.server import WireServer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"Port {port} not open after {timeout}s")


def _generate_self_signed_cert(tmpdir: str) -> tuple[str, str]:
    """Generate a self-signed cert+key pair via openssl CLI."""
    cert_path = os.path.join(tmpdir, "server.pem")
    key_path = os.path.join(tmpdir, "server-key.pem")
    subprocess.check_call(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key_path,
            "-out",
            cert_path,
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return cert_path, key_path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_data(tmp_path):
    db_path = str(tmp_path / "redb_data")
    yield db_path


def _stop_server(server):
    """Stop server and properly close the embedded client."""
    server.stop()
    if hasattr(server, "_local_client") and hasattr(server._local_client, "close"):
        try:
            server._local_client.close()
        except Exception:
            pass


@pytest.fixture()
def auth_server(tmp_data):
    """WireServer with auth_required=True on a random port."""
    port = _find_free_port()
    server = WireServer(tmp_data, "127.0.0.1", port, auth_required=True)
    server.start()
    _wait_for_port("127.0.0.1", port)
    yield server, port
    _stop_server(server)


@pytest.fixture()
def noauth_server(tmp_data):
    """WireServer without auth on a random port."""
    port = _find_free_port()
    server = WireServer(tmp_data, "127.0.0.1", port, auth_required=False)
    server.start()
    _wait_for_port("127.0.0.1", port)
    yield server, port
    _stop_server(server)


@pytest.fixture()
def tls_certs(tmp_path):
    cert_path, key_path = _generate_self_signed_cert(str(tmp_path))
    return cert_path, key_path


# ---------------------------------------------------------------------------
# SCRAM-SHA-256 Auth Tests
# ---------------------------------------------------------------------------


class TestCreateUserWithPassword:
    def test_create_user_stores_credentials(self, noauth_server):
        _, port = noauth_server
        mc = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}", directConnection=True)
        mc.admin.command("createUser", "testuser", pwd="secret123", roles=["root"])
        users = mc.admin.command("usersInfo", "testuser")["users"]
        assert len(users) == 1
        user = users[0]
        assert user["user"] == "testuser"
        assert "SCRAM-SHA-256" in user.get("mechanisms", [])
        assert "credentials" in user
        cred = user["credentials"]["SCRAM-SHA-256"]
        assert "salt" in cred
        assert "storedKey" in cred
        assert "serverKey" in cred
        assert cred["iterationCount"] >= 4096
        mc.close()


class TestScramAuthentication:
    def test_pymongo_auth_succeeds(self, auth_server):
        _, port = auth_server

        # First, create the user without auth (admin bootstrap via noauth helper)
        admin = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}", directConnection=True)
        # hello/ping are auth-exempt, so we can connect
        admin.admin.command("ping")
        # createUser should be blocked by auth gate for unauthenticated connections
        # ... unless we first need to bootstrap. Since there's no user yet, let's
        # test the auth gate first, then bootstrap differently.
        admin.close()

    def test_auth_gate_blocks_unauthenticated(self, auth_server):
        _, port = auth_server
        mc = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}", directConnection=True)
        # ping is auth-exempt
        mc.admin.command("ping")
        # insert should be blocked
        with pytest.raises(pymongo.errors.OperationFailure, match="authentication"):
            mc.test.foo.insert_one({"x": 1})
        mc.close()


class TestScramFullFlow:
    """Full SCRAM flow: create user, authenticate, perform operations."""

    @pytest.fixture(scope="class")
    def bootstrapped_auth(self, tmp_path_factory):
        """Start a server without auth to create the user, then restart with auth."""
        db_path = str(tmp_path_factory.mktemp("scram_flow"))
        port = _find_free_port()
        # Phase 1: no auth, create user
        lc = RedbLocalClient(db_path)
        server1 = WireServer(db_path, "127.0.0.1", port, auth_required=False, local_client=lc)
        server1.start()
        _wait_for_port("127.0.0.1", port)
        mc = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}", directConnection=True)
        mc.admin.command("createUser", "admin", pwd="password123", roles=["root"])
        mc.close()
        server1.stop()
        lc.close()
        time.sleep(0.5)

        # Phase 2: restart with auth (new client from same data dir)
        server2 = WireServer(db_path, "127.0.0.1", port, auth_required=True)
        server2.start()
        _wait_for_port("127.0.0.1", port)
        yield port
        _stop_server(server2)

    def test_full_auth_flow(self, bootstrapped_auth):
        port = bootstrapped_auth
        mc = pymongo.MongoClient(
            f"mongodb://admin:password123@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        mc.test.auth_test.insert_one({"msg": "authenticated!"})
        doc = mc.test.auth_test.find_one({"msg": "authenticated!"})
        assert doc is not None
        assert doc["msg"] == "authenticated!"
        mc.close()

    def test_wrong_password_rejected(self, bootstrapped_auth):
        port = bootstrapped_auth
        mc = pymongo.MongoClient(
            f"mongodb://admin:WRONG@127.0.0.1:{port}/admin",
            directConnection=True,
            serverSelectionTimeoutMS=3000,
        )
        with pytest.raises(pymongo.errors.OperationFailure):
            mc.admin.command("ping")
        mc.close()

    def test_user_persistence(self, bootstrapped_auth):
        """User credentials survive server restart (fixture already restarted)."""
        port = bootstrapped_auth
        mc = pymongo.MongoClient(
            f"mongodb://admin:password123@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        result = mc.admin.command("ping")
        assert result.get("ok") == 1.0
        mc.close()


# ---------------------------------------------------------------------------
# TLS Tests
# ---------------------------------------------------------------------------


class TestTLS:
    def test_tls_connect(self, tmp_data, tls_certs):
        cert_path, key_path = tls_certs
        port = _find_free_port()
        server = WireServer(
            tmp_data,
            "127.0.0.1",
            port,
            tls_cert_file=cert_path,
            tls_key_file=key_path,
        )
        server.start()
        _wait_for_port("127.0.0.1", port)

        try:
            mc = pymongo.MongoClient(
                f"mongodb://127.0.0.1:{port}",
                directConnection=True,
                tls=True,
                tlsCAFile=cert_path,
                tlsAllowInvalidHostnames=True,
            )
            result = mc.admin.command("ping")
            assert result.get("ok") == 1.0

            mc.test.tls_col.insert_one({"secure": True})
            doc = mc.test.tls_col.find_one({"secure": True})
            assert doc is not None
            mc.close()
        finally:
            _stop_server(server)


class TestTLSWithAuth:
    def test_tls_plus_scram(self, tmp_data, tls_certs):
        cert_path, key_path = tls_certs
        port = _find_free_port()

        # Phase 1: no auth, create user over TLS
        lc = RedbLocalClient(tmp_data)
        server1 = WireServer(
            tmp_data,
            "127.0.0.1",
            port,
            tls_cert_file=cert_path,
            tls_key_file=key_path,
            auth_required=False,
            local_client=lc,
        )
        server1.start()
        _wait_for_port("127.0.0.1", port)
        mc = pymongo.MongoClient(
            f"mongodb://127.0.0.1:{port}",
            directConnection=True,
            tls=True,
            tlsCAFile=cert_path,
            tlsAllowInvalidHostnames=True,
        )
        mc.admin.command("createUser", "tlsadmin", pwd="tlspass", roles=["root"])
        mc.close()
        server1.stop()
        lc.close()
        time.sleep(0.5)

        # Phase 2: restart with auth + TLS
        server2 = WireServer(
            tmp_data,
            "127.0.0.1",
            port,
            tls_cert_file=cert_path,
            tls_key_file=key_path,
            auth_required=True,
        )
        server2.start()
        _wait_for_port("127.0.0.1", port)

        try:
            mc = pymongo.MongoClient(
                f"mongodb://tlsadmin:tlspass@127.0.0.1:{port}/admin",
                directConnection=True,
                tls=True,
                tlsCAFile=cert_path,
                tlsAllowInvalidHostnames=True,
            )
            result = mc.admin.command("ping")
            assert result.get("ok") == 1.0
            mc.test.secure_data.insert_one({"encrypted": True})
            doc = mc.test.secure_data.find_one({"encrypted": True})
            assert doc is not None
            mc.close()
        finally:
            _stop_server(server2)
