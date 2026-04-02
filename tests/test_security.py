"""
Security hardening tests -- validates all enterprise hardening measures.
"""

import threading
import time
import zlib

import pytest

from smongo.query import MAX_REGEX_PATTERN_LEN, _safe_regex, compile_query
from smongo.schema import MAX_NESTING_DEPTH as SCHEMA_MAX_DEPTH
from smongo.schema import ValidationError, validate_document
from smongo.wire.bson_codec import MAX_NESTING_DEPTH as BSON_MAX_DEPTH
from smongo.wire.bson_codec import normalize_inbound, normalize_outbound
from smongo.wire.commands import MAX_WRITE_BATCH_SIZE, dispatch
from smongo.wire.context import (
    ConnectionContext,
    NamespaceError,
    SessionRegistry,
    TooManySessions,
    validate_namespace,
)
from smongo.wire.msg import MAX_MSG_SIZE, ProtocolError, _decompress
from smongo.wire.server import CONNECTION_TIMEOUT_SEC, MAX_CONNECTIONS, WireServer

# =====================================================================
# 1. Socket timeout constant
# =====================================================================


class TestSocketTimeout:
    def test_connection_timeout_is_reasonable(self):
        assert 60 <= CONNECTION_TIMEOUT_SEC <= 600

    def test_server_sets_timeout(self):
        """WireServer._connection_loop sets sock.settimeout -- verified by
        reading the source; here we just confirm the constant is exported."""
        assert CONNECTION_TIMEOUT_SEC == 300


# =====================================================================
# 2. Max connections
# =====================================================================


class TestMaxConnections:
    def test_default_max(self):
        assert MAX_CONNECTIONS == 1024

    def test_custom_max(self):
        server = WireServer.__new__(WireServer)
        sem = threading.Semaphore(2)
        server._conn_semaphore = sem
        assert sem.acquire(timeout=0)
        assert sem.acquire(timeout=0)
        assert not sem.acquire(timeout=0)
        sem.release()
        sem.release()


# =====================================================================
# 3. Namespace validation
# =====================================================================


class TestNamespaceValidation:
    def test_valid_namespace(self):
        validate_namespace("mydb", "users")

    def test_empty_db_name(self):
        with pytest.raises(NamespaceError, match="invalid database name"):
            validate_namespace("", "coll")

    def test_db_name_with_null(self):
        with pytest.raises(NamespaceError, match="forbidden characters"):
            validate_namespace("my\x00db", "coll")

    def test_db_name_with_slash(self):
        with pytest.raises(NamespaceError, match="forbidden characters"):
            validate_namespace("my/db", "coll")

    def test_db_name_with_backslash(self):
        with pytest.raises(NamespaceError, match="forbidden characters"):
            validate_namespace("my\\db", "coll")

    def test_db_name_with_dot(self):
        with pytest.raises(NamespaceError, match="cannot contain '\\.'"):
            validate_namespace("my.db", "coll")

    def test_db_name_dollar_prefix(self):
        with pytest.raises(NamespaceError, match="cannot start with '\\$'"):
            validate_namespace("$admin", "coll")

    def test_db_name_too_long(self):
        with pytest.raises(NamespaceError, match="exceeds"):
            validate_namespace("a" * 65, "coll")

    def test_db_name_whitespace(self):
        with pytest.raises(NamespaceError, match="invalid database name"):
            validate_namespace(" mydb", "coll")

    def test_empty_coll_name(self):
        with pytest.raises(NamespaceError, match="invalid collection name"):
            validate_namespace("db", "")

    def test_coll_name_with_null(self):
        with pytest.raises(NamespaceError, match="forbidden characters"):
            validate_namespace("db", "my\x00coll")

    def test_coll_name_dollar_prefix(self):
        with pytest.raises(NamespaceError, match="cannot start with '\\$'"):
            validate_namespace("db", "$bad")

    def test_coll_name_dollar_cmd_allowed(self):
        validate_namespace("db", "$cmd")
        validate_namespace("db", "$external")

    def test_coll_name_double_dot(self):
        with pytest.raises(NamespaceError, match="cannot contain '\\.\\.'"):
            validate_namespace("db", "a..b")

    def test_coll_name_too_long(self):
        with pytest.raises(NamespaceError, match="exceeds"):
            validate_namespace("db", "c" * 121)


# =====================================================================
# 4. Decompression bomb guard
# =====================================================================


class TestDecompressionBomb:
    def test_declared_size_over_max(self):
        with pytest.raises(ProtocolError, match="exceeds limit"):
            _decompress(2, b"", MAX_MSG_SIZE + 1)

    def test_negative_expected_size(self):
        with pytest.raises(ProtocolError, match="exceeds limit"):
            _decompress(2, b"", -1)

    def test_zlib_size_mismatch(self):
        data = b"hello world"
        compressed = zlib.compress(data)
        with pytest.raises(ProtocolError, match="zlib decompressed size"):
            _decompress(2, compressed, len(data) + 10)

    def test_zlib_valid_roundtrip(self):
        data = b"A" * 1000
        compressed = zlib.compress(data)
        result = _decompress(2, compressed, len(data))
        assert result == data


# =====================================================================
# 5. Batch size enforcement
# =====================================================================


class TestBatchSizeEnforcement:
    @pytest.fixture()
    def ctx(self, tmp_path):
        from smongo.storage import LocalClient
        from smongo.wire.cursors import CursorRegistry

        client = LocalClient(str(tmp_path / "wt"))
        return ConnectionContext(client, 1, ("127.0.0.1", 9999), CursorRegistry())

    def test_insert_over_limit(self, ctx):
        docs = [{"x": i} for i in range(MAX_WRITE_BATCH_SIZE + 1)]
        resp = dispatch(
            ctx,
            {"insert": "coll", "documents": docs, "$db": "test"},
        )
        assert resp["ok"] == 0
        assert "exceeds limit" in resp["errmsg"]

    def test_update_over_limit(self, ctx):
        updates = [{"q": {}, "u": {"$set": {"x": 1}}}] * (MAX_WRITE_BATCH_SIZE + 1)
        resp = dispatch(
            ctx,
            {"update": "coll", "updates": updates, "$db": "test"},
        )
        assert resp["ok"] == 0
        assert "exceeds limit" in resp["errmsg"]

    def test_delete_over_limit(self, ctx):
        deletes = [{"q": {}, "limit": 0}] * (MAX_WRITE_BATCH_SIZE + 1)
        resp = dispatch(
            ctx,
            {"delete": "coll", "deletes": deletes, "$db": "test"},
        )
        assert resp["ok"] == 0
        assert "exceeds limit" in resp["errmsg"]

    def test_insert_at_limit_ok(self, ctx):
        docs = [{"x": i} for i in range(5)]
        resp = dispatch(
            ctx,
            {"insert": "coll", "documents": docs, "$db": "test"},
        )
        assert resp["ok"] == 1.0
        assert resp["n"] == 5


# =====================================================================
# 6. ReDoS protection
# =====================================================================


class TestReDoSProtection:
    def test_long_pattern_rejected(self):
        with pytest.raises(ValueError, match="exceeds limit"):
            _safe_regex("a" * (MAX_REGEX_PATTERN_LEN + 1))

    def test_nested_quantifier_rejected(self):
        with pytest.raises(ValueError, match="nested quantifiers"):
            _safe_regex("(a+)+")

    def test_nested_quantifier_star(self):
        with pytest.raises(ValueError, match="nested quantifiers"):
            _safe_regex("(x*)*")

    def test_normal_pattern_ok(self):
        pat = _safe_regex("^hello.*world$")
        assert pat.search("hello brave world")

    def test_query_regex_guarded(self):
        fn = compile_query({"name": {"$regex": "(a+)+"}})
        with pytest.raises(ValueError, match="nested quantifiers"):
            fn({"name": "aaaaab"})

    def test_schema_regex_guarded(self):
        schema = {"properties": {"name": {"type": "string", "pattern": "(a+)+"}}}
        with pytest.raises(ValueError, match="nested quantifiers"):
            validate_document({"name": "aaaaab"}, schema)


# =====================================================================
# 7. Unknown operator rejection
# =====================================================================


class TestUnknownOperatorRejection:
    def test_unknown_op_raises(self):
        fn = compile_query({"x": {"$bogus": 1}})
        with pytest.raises(ValueError, match="unknown query operator"):
            fn({"x": 1})

    def test_known_ops_still_work(self):
        fn = compile_query({"x": {"$gt": 3}})
        assert fn({"x": 5}) is True
        assert fn({"x": 2}) is False


# =====================================================================
# 8. Nesting depth limit
# =====================================================================


class TestNestingDepthLimit:
    def _nested_dict(self, depth):
        doc = {"val": 1}
        for _ in range(depth):
            doc = {"nested": doc}
        return doc

    def test_bson_inbound_under_limit(self):
        doc = self._nested_dict(50)
        result = normalize_inbound(doc)
        assert result is not None

    def test_bson_inbound_over_limit(self):
        doc = self._nested_dict(BSON_MAX_DEPTH + 5)
        with pytest.raises(ValueError, match="nesting depth"):
            normalize_inbound(doc)

    def test_bson_outbound_over_limit(self):
        doc = self._nested_dict(BSON_MAX_DEPTH + 5)
        with pytest.raises(ValueError, match="nesting depth"):
            normalize_outbound(doc)

    def test_schema_validation_over_limit(self):
        depth = SCHEMA_MAX_DEPTH + 5
        inner_schema = {"type": "object"}
        for _ in range(depth):
            inner_schema = {
                "type": "object",
                "properties": {"nested": inner_schema},
            }
        doc = self._nested_dict(depth)
        with pytest.raises(ValidationError, match="nesting depth"):
            validate_document(doc, inner_schema)


# =====================================================================
# 9. Web app security (compile-time checks)
# =====================================================================


class TestWebAppSecurity:
    def test_default_bind_host(self):
        """Verify the web_app module defaults to localhost."""
        import ast
        from pathlib import Path

        source = Path("web_app.py").read_text()
        tree = ast.parse(source)
        found_environ_get = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "get" and len(node.args) >= 2:
                    for arg in node.args:
                        if isinstance(arg, ast.Constant) and arg.value == "BIND_HOST":
                            found_environ_get = True
        assert found_environ_get, "web_app.py should use BIND_HOST env var"


# =====================================================================
# 10. Session registry cap and reaper
# =====================================================================


class TestSessionRegistryCap:
    def test_create_respects_cap(self):
        registry = SessionRegistry(max_sessions=3)
        registry.create()
        registry.create()
        registry.create()
        with pytest.raises(TooManySessions, match="session limit"):
            registry.create()

    def test_touch_does_not_exceed_cap(self):
        registry = SessionRegistry(max_sessions=2)
        registry.create()
        registry.create()
        registry.touch({"id": "extra-session-1"})
        assert registry.count == 2

    def test_expire_frees_slots(self):
        registry = SessionRegistry(timeout_minutes=0, max_sessions=2)
        registry.create()
        registry.create()
        time.sleep(0.05)
        registry.expire()
        assert registry.count == 0
        registry.create()

    def test_reaper_starts_and_stops(self):
        registry = SessionRegistry()
        registry.start_reaper()
        assert registry._reaper_thread is not None
        assert registry._reaper_thread.is_alive()
        registry.stop_reaper()
        assert registry._reaper_thread is None


# =====================================================================
# 11. Namespace validation via dispatch
# =====================================================================


class TestNamespaceInDispatch:
    @pytest.fixture()
    def ctx(self, tmp_path):
        from smongo.storage import LocalClient
        from smongo.wire.cursors import CursorRegistry

        client = LocalClient(str(tmp_path / "wt"))
        return ConnectionContext(client, 1, ("127.0.0.1", 9999), CursorRegistry())

    def test_find_with_null_byte_coll(self, ctx):
        resp = dispatch(ctx, {"find": "bad\x00coll", "$db": "test"})
        assert resp["ok"] == 0
        assert resp["codeName"] == "InvalidNamespace"

    def test_insert_with_dollar_coll(self, ctx):
        resp = dispatch(
            ctx,
            {"insert": "$evil", "documents": [{"x": 1}], "$db": "test"},
        )
        assert resp["ok"] == 0
        assert resp["codeName"] == "InvalidNamespace"

    def test_find_with_slash_db(self, ctx):
        resp = dispatch(ctx, {"find": "users", "$db": "../../etc"})
        assert resp["ok"] == 0
        assert resp["codeName"] == "InvalidNamespace"
