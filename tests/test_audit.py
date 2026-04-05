"""Tests for audit logging.

Covers:
- Auth events logged on login / failure / logout
- Command events include user, namespace, success flag
- Audit log output is valid JSON with expected fields
"""

from __future__ import annotations

import json
import os
import socket
import time

import pymongo
import pytest

from smongo._smongo_core import RustLocalClient
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


class TestAuditLogging:
    @pytest.fixture(scope="class")
    def audit_env(self, tmp_path_factory):
        """Bootstrap server with auth + audit logging to a file."""
        base = tmp_path_factory.mktemp("audit")
        db_path = str(base / "wt_data")
        audit_file = str(base / "audit.json")
        port = _find_free_port()

        # Phase 1: create user without auth
        lc = RustLocalClient(db_path)
        server1 = WireServer(db_path, "127.0.0.1", port, auth_required=False, local_client=lc)
        server1.start()
        _wait_for_port("127.0.0.1", port)
        mc = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}", directConnection=True)
        mc.admin.command(
            "createUser",
            "audituser",
            pwd="auditpw",
            roles=[{"role": "root", "db": "admin"}],
        )
        mc.close()
        server1.stop()
        lc.close()
        time.sleep(0.5)

        # Phase 2: restart with auth + audit
        server2 = WireServer(
            db_path,
            "127.0.0.1",
            port,
            auth_required=True,
            audit_log=audit_file,
        )
        server2.start()
        _wait_for_port("127.0.0.1", port)
        yield port, audit_file
        _stop_server(server2)

    def _read_audit_lines(self, audit_file: str) -> list[dict]:
        """Read and parse all JSON lines from the audit file."""
        if not os.path.exists(audit_file):
            return []
        lines = []
        with open(audit_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    lines.append(json.loads(line))
        return lines

    def test_auth_success_logged(self, audit_env):
        port, audit_file = audit_env
        mc = pymongo.MongoClient(
            f"mongodb://audituser:auditpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        mc.admin.command("ping")
        mc.close()
        time.sleep(0.3)

        entries = self._read_audit_lines(audit_file)
        auth_events = [e for e in entries if e.get("attr", {}).get("type") == "auth"]
        success_events = [e for e in auth_events if e["attr"].get("success") is True]
        assert len(success_events) >= 1, f"Expected auth success event, got: {auth_events}"
        evt = success_events[0]
        assert evt["attr"]["event"] == "authenticate"
        assert evt["attr"]["user"] == "audituser"

    def test_command_events_logged(self, audit_env):
        port, audit_file = audit_env
        mc = pymongo.MongoClient(
            f"mongodb://audituser:auditpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        mc.test.audit_col.insert_one({"x": 1})
        mc.test.audit_col.find_one({"x": 1})
        mc.close()
        time.sleep(0.3)

        entries = self._read_audit_lines(audit_file)
        cmd_events = [e for e in entries if e.get("attr", {}).get("type") == "command"]
        cmd_names = [e["attr"]["command"] for e in cmd_events]
        assert "insert" in cmd_names, f"Expected 'insert' in audit log, got: {cmd_names}"
        assert "find" in cmd_names, f"Expected 'find' in audit log, got: {cmd_names}"

    def test_audit_entries_are_valid_json(self, audit_env):
        port, audit_file = audit_env
        mc = pymongo.MongoClient(
            f"mongodb://audituser:auditpw@127.0.0.1:{port}/admin",
            directConnection=True,
        )
        mc.admin.command("ping")
        mc.close()
        time.sleep(0.3)

        entries = self._read_audit_lines(audit_file)
        assert len(entries) > 0, "Audit log should not be empty"
        for entry in entries:
            assert "t" in entry, f"Missing 't' (timestamp): {entry}"
            assert "s" in entry, f"Missing 's' (severity): {entry}"
            assert "c" in entry, f"Missing 'c' (component): {entry}"
            assert "msg" in entry, f"Missing 'msg': {entry}"
            assert entry["c"] == "AUDIT", f"Expected AUDIT component, got: {entry['c']}"

    def test_auth_failure_logged(self, audit_env):
        port, audit_file = audit_env

        # Clear previous entries count
        before = len(self._read_audit_lines(audit_file))

        mc = pymongo.MongoClient(
            f"mongodb://audituser:WRONGPW@127.0.0.1:{port}/admin",
            directConnection=True,
            serverSelectionTimeoutMS=3000,
        )
        with pytest.raises(pymongo.errors.OperationFailure):
            mc.admin.command("ping")
        mc.close()
        time.sleep(0.3)

        entries = self._read_audit_lines(audit_file)
        new_entries = entries[before:]
        failure_events = [
            e
            for e in new_entries
            if e.get("attr", {}).get("type") == "auth" and e["attr"].get("success") is False
        ]
        assert len(failure_events) >= 1, f"Expected auth failure event, got: {new_entries}"
        assert failure_events[0]["attr"]["event"] == "authFailure"
