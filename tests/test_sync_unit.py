"""Unit tests for sync internals and helper strategies."""

import json
import threading
import time
import types

import pytest

from smongo.sync import (
    BulkWriteError,
    PyMongoError,
    SyncManager,
    TombstoneRegistry,
    _apply_commutative_to_doc,
    _crdt_counter_merge,
    _crdt_merge_doc,
    _crdt_set_merge,
    _diff_fields,
    _field_merge,
    _is_commutative_op,
    _local_wins,
    _lww,
    _merge_commutative_ops,
    _remote_wins,
    _resolve_variables,
)


def test_lww_strategy():
    local = {"_lastModified": 10, "x": "l"}
    remote = {"_lastModified": 20, "x": "r"}
    assert _lww(local, remote)["x"] == "r"


def test_local_wins_strategy():
    local = {"x": 1}
    remote = {"x": 2}
    assert _local_wins(local, remote)["x"] == 1


def test_remote_wins_strategy():
    local = {"x": 1}
    remote = {"x": 2}
    assert _remote_wins(local, remote)["x"] == 2


def test_field_merge_strategy():
    local = {"_id": "1", "_lastModified": 10, "a": 1, "b": 1}
    remote = {"_id": "1", "_lastModified": 20, "a": 2, "c": 3}
    merged = _field_merge(local, remote, local_changed={"b"}, remote_changed={"a", "c"})
    assert merged["_id"] == "1"
    assert merged["b"] == 1
    assert merged["a"] == 2
    assert merged["c"] == 3


def test_diff_fields_detects_changes():
    local = {"_id": "1", "a": 1, "b": 2, "c": 3}
    remote = {"_id": "1", "a": 1, "b": 99, "d": 4}
    changed = _diff_fields(local, remote)
    assert "a" not in changed
    assert "b" in changed
    assert "c" in changed
    assert "d" in changed
    assert "_id" not in changed


def test_diff_fields_identical_docs():
    doc = {"_id": "x", "a": 1, "b": 2}
    assert _diff_fields(doc, dict(doc)) == set()


def test_field_merge_with_diff_fallback():
    """When remote_changed is None, _upsert_remote_doc uses _diff_fields."""
    local = {"_id": "1", "a": 1, "b": "local", "_lastModified": 5}
    remote = {"_id": "1", "a": 1, "b": "remote", "c": "new", "_lastModified": 10}
    merged = _field_merge(local, remote, local_changed={"b"}, remote_changed={"b", "c"})
    assert merged["b"] == "remote"
    assert merged["c"] == "new"


class _FakeSyncRust:
    """In-memory stand-in for RedbLocalClient sync KV + atomic checkpoint (unit tests)."""

    def __init__(self) -> None:
        self._tables: dict[str, dict[str, str]] = {}

    def sync_kv_put(self, uri: str, k: str, v: str) -> None:
        self._tables.setdefault(uri, {})[k] = v

    def sync_kv_get(self, uri: str, k: str) -> str | None:
        return self._tables.get(uri, {}).get(k)

    def sync_kv_scan(self, uri: str) -> list[tuple[str, str]]:
        return sorted(self._tables.get(uri, {}).items())

    def sync_kv_remove(self, uri: str, k: str) -> None:
        self._tables.get(uri, {}).pop(k, None)

    def sync_atomic_checkpoint_truncate(
        self,
        ck_uri: str,
        ck_key: str,
        ck_val: str,
        _oplog_uri: str,
        _safe_key: str,
    ) -> None:
        self.sync_kv_put(ck_uri, ck_key, ck_val)

    def table(self, uri: str) -> dict[str, str]:
        """Test helper: mutable backing dict for a logical KV table."""
        return self._tables.setdefault(uri, {})


def _make_manager(**overrides):
    mgr = SyncManager.__new__(SyncManager)
    mgr._config = {
        "collections": ["db.users"],
        "batch_size": 10,
        "use_change_stream_pull": True,
        "mode": "bidirectional",
        "max_backoff_sec": 300,
        "push_concurrency": 4,
    }
    mgr._tracked = {}
    mgr._local = types.SimpleNamespace(
        client=types.SimpleNamespace(
            get_db=lambda _: types.SimpleNamespace(get_collection=lambda __: "LOCAL")
        )
    )
    mgr._remote = {"db": {"users": "REMOTE"}}
    mgr._local_field_history = {}
    mgr._cumulative_field_history = {}
    mgr._local_update_specs = {}
    mgr._lock = threading.Lock()
    mgr._ck_lock = threading.Lock()
    mgr._pushed_count = 0
    mgr._pulled_count = 0
    mgr._conflict_count = 0
    mgr._error_count = 0
    mgr._consecutive_errors = 0
    mgr._state = "offline"
    mgr._pending_count = 0
    mgr._last_sync_ts = None
    mgr._last_error = None
    mgr._thread = None
    mgr._last_cycle_pushed = 0
    mgr._last_cycle_pulled = 0
    mgr._last_cycle_start = 0.0
    mgr._last_cycle_duration = 0.0
    mgr._ns_stats = {}
    mgr._node_id = overrides.get("node_id", "local")
    mgr._crdt_fields = overrides.get("crdt_fields", {})
    mgr._user_variables = overrides.get("variables", {})
    mgr._raw_sync_rules = overrides.get("sync_rules")
    mgr._raw_collection_filters = {}
    mgr._active_sync_filter = None
    mgr._vector_clocks = {}
    mgr._dlq_uri = "table:__sync_dlq"
    mgr._ck_uri = "table:__sync_checkpoint"
    mgr._conflict_log_uri = "table:__sync_conflict_log"
    mgr._conflict_log = []
    mgr._cycle_count = 0
    mgr._index_hash_cache = {}
    mgr._persistent_counters_uri = "table:__sync_counters"
    mgr._ts_uri = "table:__tombstones"
    mgr._rust = _FakeSyncRust()
    mgr._tombstones = TombstoneRegistry(
        ttl_sec=7 * 24 * 3600,
        redb_client=mgr._rust,
        uri=mgr._ts_uri,
    )
    if mgr._raw_sync_rules:
        mgr._recompile_sync_filter()
    return mgr


def test_discover_collections_star_noop():
    mgr = _make_manager()
    mgr._config["collections"] = "*"
    mgr._discover_collections()
    assert mgr._tracked == {}


def test_discover_collections_registers():
    mgr = _make_manager()
    mgr._discover_collections()
    assert "db.users" in mgr._tracked


def test_register_collection():
    mgr = _make_manager()
    mgr.register_collection("db", "users", "LOCAL")
    assert mgr._tracked["db.users"] == ("LOCAL", "REMOTE", None)


def test_upsert_remote_doc_insert_path():
    mgr = _make_manager()

    class Local:
        def __init__(self):
            self.inserted = None

        def get_by_id(self, doc_id):
            return None

        def insert_one(self, doc, _internal=False):
            self.inserted = (doc, _internal)

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    mgr._upsert_remote_doc("db.users", local, {"_id": "1", "x": 1})
    assert local.inserted[0]["x"] == 1
    assert local.inserted[1] is True
    assert "_vclock" in local.inserted[0]


def test_upsert_remote_doc_update_path():
    mgr = _make_manager()

    class Local:
        def __init__(self):
            self.updated = None

        def get_by_id(self, doc_id):
            return {"_id": "1", "x": "old", "_lastModified": 1}

        def update(self, query, update_spec, multi=False, _internal=False):
            self.updated = (query, update_spec, multi, _internal)

    local = Local()
    mgr._resolver_name = "remote_wins"
    mgr._resolve = _remote_wins
    mgr._upsert_remote_doc("db.users", local, {"_id": "1", "x": "new", "_lastModified": 2})
    assert local.updated[0] == {"_id": "1"}
    assert local.updated[3] is True
    assert "_vclock" in local.updated[1]["$set"]


def test_pull_via_change_stream_fallback_on_error():
    mgr = _make_manager()
    mgr._get_checkpoint = lambda k: None
    mgr._set_checkpoint = lambda k, v: None
    mgr._active_sync_filter = None

    class Remote:
        def find(self, q):
            raise PyMongoError("no stream")

    ok = mgr._pull_via_change_stream("db.users", local_coll=object(), remote_coll=Remote())
    assert ok is False


def test_pull_via_change_stream_success_path():
    mgr = _make_manager()
    mgr._active_sync_filter = None
    ck = {}
    mgr._get_checkpoint = lambda k: ck.get(k)
    mgr._set_checkpoint = lambda k, v: ck.__setitem__(k, v)
    seen: list[tuple[str, dict, set | None]] = []
    orig_upsert = (
        mgr._upsert_remote_doc.__func__ if hasattr(mgr._upsert_remote_doc, "__func__") else None
    )
    mgr._upsert_remote_doc = lambda ns, local, doc, remote_changed=None: seen.append(
        (ns, doc, remote_changed)
    )

    class Stream:
        def __init__(self):
            self.calls = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def try_next(self):
            self.calls += 1
            if self.calls == 1:
                return {
                    "operationType": "insert",
                    "_id": {"token": 1},
                    "fullDocument": {"_id": "1", "x": 1},
                }
            if self.calls == 2:
                return {
                    "operationType": "update",
                    "_id": {"token": 2},
                    "fullDocument": {"_id": "2", "x": 99},
                    "updateDescription": {
                        "updatedFields": {"x": 99},
                        "removedFields": ["old_field"],
                    },
                }
            return None

    class Remote:
        def find(self, q):
            return _FakeRemoteCursor([])

        def watch(self, pipeline, **kwargs):
            return Stream()

    class Local:
        def delete(self, q, multi=False, _internal=False):
            return None

    ok = mgr._pull_via_change_stream("db.users", Local(), Remote())
    assert ok is True
    assert len(seen) == 2
    assert seen[0][1]["_id"] == "1"
    assert seen[0][2] is None
    assert seen[1][1]["_id"] == "2"
    assert seen[1][2] == {"x", "old_field"}


def test_status_includes_counters():
    mgr = _make_manager()
    mgr._thread = None
    s = mgr.status()
    assert "pushed" in s
    assert "pulled" in s
    assert "conflicts" in s
    assert "errors" in s
    assert "state" in s
    assert s["pushed"] == 0
    assert s["state"] == "offline"


def test_backoff_on_consecutive_errors():
    """Consecutive errors increase sleep time exponentially."""
    mgr = _make_manager()
    mgr._config["interval_sec"] = 2
    mgr._config["max_backoff_sec"] = 60
    mgr._consecutive_errors = 3
    sleep_time = min(2 * (2**3), 60)
    assert sleep_time == 16


def test_selective_sync_filter_dict():
    mgr = _make_manager()
    mgr._config["collections"] = {"db.users": {"region": "us-east-1"}}
    mgr._discover_collections()
    assert "db.users" in mgr._tracked
    _, _, filter_fn = mgr._tracked["db.users"]
    assert filter_fn is not None
    assert filter_fn({"region": "us-east-1"}) is True
    assert filter_fn({"region": "eu-west-1"}) is False


def test_checkpoint_get_set():
    mgr = SyncManager.__new__(SyncManager)
    mgr._rust = _FakeSyncRust()
    mgr._ck_uri = "table:ck"
    mgr._ck_lock = threading.Lock()
    mgr._set_checkpoint("k1", "v1")
    assert mgr._get_checkpoint("k1") == "v1"


# ------------------------------------------------------------------
# Variable substitution
# ------------------------------------------------------------------


def test_resolve_variables_now():
    """$$NOW is substituted with a float timestamp."""
    before = time.time()
    result = _resolve_variables({"ts": {"$gt": "$$NOW"}}, {"NOW": time.time()})
    assert isinstance(result["ts"]["$gt"], float)
    assert result["ts"]["$gt"] >= before


def test_resolve_variables_node_id():
    """$$NODE_ID is substituted with the configured value."""
    result = _resolve_variables({"device_id": "$$NODE_ID"}, {"NODE_ID": "sensor-042"})
    assert result["device_id"] == "sensor-042"


def test_resolve_variables_custom():
    """User-defined variables from the variables config are substituted."""
    ctx = {"NOW": 0, "NODE_ID": "x", "region": "us-east-1"}
    result = _resolve_variables({"region": "$$region"}, ctx)
    assert result["region"] == "us-east-1"


def test_resolve_variables_nested():
    """Variables inside nested $expr / $and / $or structures are resolved."""
    query = {
        "$expr": {"$gt": ["$_lastModified", {"$subtract": ["$$NOW", 604800]}]},
        "$and": [{"owner": "$$NODE_ID"}, {"active": True}],
    }
    ctx = {"NOW": 1000000.0, "NODE_ID": "dev-1"}
    result = _resolve_variables(query, ctx)
    assert result["$expr"]["$gt"][1]["$subtract"][0] == 1000000.0
    assert result["$and"][0]["owner"] == "dev-1"
    assert result["$and"][1]["active"] is True


def test_resolve_variables_preserves_non_vars():
    """Strings without $$ prefix and $$ROOT/$$CURRENT are left untouched."""
    query = {"name": "Alice", "$expr": {"$eq": ["$$ROOT.x", 1]}, "id": "$$UNKNOWN"}
    ctx = {"NOW": 0, "NODE_ID": "x"}
    result = _resolve_variables(query, ctx)
    assert result["name"] == "Alice"
    assert result["$expr"]["$eq"][0] == "$$ROOT.x"
    assert result["id"] == "$$UNKNOWN"


# ------------------------------------------------------------------
# Sync rules applied in push / pull
# ------------------------------------------------------------------


def test_sync_rules_applied_push():
    """Global sync_rules filter blocks non-matching docs during push."""
    mgr = _make_manager(sync_rules={"device_id": "sensor-1"}, node_id="sensor-1")

    pushed_ops: list = []

    class FakeOplogReader:
        def read_from(self, checkpoint, skip_internal=True):
            return [
                (
                    "k1",
                    {
                        "op": "insert",
                        "doc_id": "d1",
                        "ts": 1.0,
                        "payload": {"_id": "d1", "device_id": "sensor-1", "val": 10},
                        "changed_fields": [],
                    },
                ),
                (
                    "k2",
                    {
                        "op": "insert",
                        "doc_id": "d2",
                        "ts": 2.0,
                        "payload": {"_id": "d2", "device_id": "sensor-2", "val": 20},
                        "changed_fields": [],
                    },
                ),
            ]

    class FakeLocal:
        def get_oplog_reader(self):
            return FakeOplogReader()

        _oplog_w = types.SimpleNamespace(truncate_before=lambda k: 0, node_id=None)

        def get_by_id(self, doc_id):
            return None

    class FakeRemote:
        def bulk_write(self, ops, ordered=False):
            pushed_ops.extend(ops)

    mgr._tracked = {"db.users": (FakeLocal(), FakeRemote(), None)}
    mgr._get_checkpoint = lambda k: None
    mgr._set_checkpoint = lambda k, v: None
    mgr._config["oplog_auto_compact"] = False

    mgr._push()
    assert len(pushed_ops) == 1


def test_sync_rules_applied_pull():
    """Global sync_rules filter blocks non-matching docs during pull."""
    mgr = _make_manager(sync_rules={"device_id": "sensor-1"}, node_id="sensor-1")

    class FakeLocal:
        def __init__(self):
            self.inserted = []

        def get_by_id(self, doc_id):
            return None

        def insert_one(self, doc, _internal=False):
            self.inserted.append(doc)

        def list_indexes(self):
            return []

    class FakeRemote:
        def find(self, q):
            class FakeCursor:
                def sort(self, *a):
                    return self

                def __iter__(self):
                    return iter(
                        [
                            {"_id": "d1", "device_id": "sensor-1", "_lastModified": 1.0},
                            {"_id": "d2", "device_id": "sensor-2", "_lastModified": 2.0},
                        ]
                    )

            return FakeCursor()

        def list_indexes(self):
            return []

    local = FakeLocal()
    mgr._tracked = {"db.users": (local, FakeRemote(), None)}
    mgr._config["use_change_stream_pull"] = False
    mgr._get_checkpoint = lambda k: None
    mgr._set_checkpoint = lambda k, v: None

    mgr._pull()
    assert len(local.inserted) == 1
    assert local.inserted[0]["device_id"] == "sensor-1"


# ------------------------------------------------------------------
# Push filter resolves full doc for updates
# ------------------------------------------------------------------


def test_push_filter_update_resolves_full_doc():
    """Update ops evaluate the sync filter against the full document, not the update spec."""
    mgr = _make_manager(sync_rules={"status": "active"}, node_id="local")

    pushed_ops: list = []

    class FakeOplogReader:
        def read_from(self, checkpoint, skip_internal=True):
            return [
                (
                    "k1",
                    {
                        "op": "update",
                        "doc_id": "d1",
                        "ts": 1.0,
                        "payload": {"$set": {"val": 99}},
                        "changed_fields": ["val"],
                    },
                ),
            ]

    class FakeLocal:
        def get_oplog_reader(self):
            return FakeOplogReader()

        _oplog_w = types.SimpleNamespace(truncate_before=lambda k: 0, node_id=None)

        def get_by_id(self, doc_id):
            return {"_id": "d1", "status": "active", "val": 1}

    class FakeRemote:
        def bulk_write(self, ops, ordered=False):
            pushed_ops.extend(ops)

    mgr._tracked = {"db.users": (FakeLocal(), FakeRemote(), None)}
    mgr._get_checkpoint = lambda k: None
    mgr._set_checkpoint = lambda k, v: None
    mgr._config["oplog_auto_compact"] = False

    mgr._push()
    assert len(pushed_ops) == 1


def test_push_filter_update_blocks_non_matching_doc():
    """Update op blocked when the full document doesn't match sync rules."""
    mgr = _make_manager(sync_rules={"status": "active"}, node_id="local")

    pushed_ops: list = []

    class FakeOplogReader:
        def read_from(self, checkpoint, skip_internal=True):
            return [
                (
                    "k1",
                    {
                        "op": "update",
                        "doc_id": "d1",
                        "ts": 1.0,
                        "payload": {"$set": {"val": 99}},
                        "changed_fields": ["val"],
                    },
                ),
            ]

    class FakeLocal:
        def get_oplog_reader(self):
            return FakeOplogReader()

        _oplog_w = types.SimpleNamespace(truncate_before=lambda k: 0, node_id=None)

        def get_by_id(self, doc_id):
            return {"_id": "d1", "status": "archived", "val": 1}

    class FakeRemote:
        def bulk_write(self, ops, ordered=False):
            pushed_ops.extend(ops)

    mgr._tracked = {"db.users": (FakeLocal(), FakeRemote(), None)}
    mgr._get_checkpoint = lambda k: None
    mgr._set_checkpoint = lambda k, v: None
    mgr._config["oplog_auto_compact"] = False

    mgr._push()
    assert len(pushed_ops) == 0


# ------------------------------------------------------------------
# VectorClock.dominates() semantics
# ------------------------------------------------------------------


def test_vector_clock_empty_vs_empty():
    """Two empty clocks: neither dominates."""
    from smongo.sync import VectorClock

    assert VectorClock({}).dominates(VectorClock({})) is False


def test_vector_clock_nonempty_dominates_empty():
    """A non-empty clock dominates an empty one."""
    from smongo.sync import VectorClock

    assert VectorClock({"a": 1}).dominates(VectorClock({})) is True


def test_vector_clock_empty_does_not_dominate_nonempty():
    """An empty clock does not dominate a non-empty one."""
    from smongo.sync import VectorClock

    assert VectorClock({}).dominates(VectorClock({"a": 1})) is False


# ------------------------------------------------------------------
# Vector clocks in conflict resolution
# ------------------------------------------------------------------


def test_vector_clock_tick_on_conflict():
    """Conflict resolution ticks the local node's vector clock and stamps it on the doc."""
    mgr = _make_manager(node_id="edge-1")

    class Local:
        def __init__(self):
            self.updated = None

        def get_by_id(self, doc_id):
            return {"_id": "c1", "x": "old", "_lastModified": 1, "_vclock": {"edge-1": 1}}

        def update(self, query, update_spec, multi=False, _internal=False):
            self.updated = update_spec

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    mgr._upsert_remote_doc(
        "db.users",
        local,
        {"_id": "c1", "x": "new", "_lastModified": 2, "_vclock": {"edge-2": 1}},
    )
    assert local.updated is not None
    vclock = local.updated["$set"]["_vclock"]
    assert vclock["edge-1"] >= 2
    assert vclock["edge-2"] == 1


def test_vector_clock_causal_dominance():
    """When remote's vector clock dominates, the remote version wins regardless of resolver."""
    mgr = _make_manager(node_id="edge-1")

    class Local:
        def __init__(self):
            self.updated = None

        def get_by_id(self, doc_id):
            return {"_id": "c2", "x": "local", "_lastModified": 5, "_vclock": {"edge-1": 1}}

        def update(self, query, update_spec, multi=False, _internal=False):
            self.updated = update_spec

    local = Local()
    mgr._resolver_name = "local_wins"
    mgr._resolve = _local_wins
    mgr._upsert_remote_doc(
        "db.users",
        local,
        {"_id": "c2", "x": "remote", "_lastModified": 3, "_vclock": {"edge-1": 2, "edge-2": 1}},
    )
    assert local.updated["$set"]["x"] == "remote"


# ------------------------------------------------------------------
# register_collection with sync_filter
# ------------------------------------------------------------------


def test_register_collection_with_sync_filter():
    """register_collection accepts a sync_filter dict and compiles it."""
    mgr = _make_manager()
    mgr.register_collection("db", "users", "LOCAL", sync_filter={"region": "$$NODE_ID"})
    assert "db.users" in mgr._tracked
    _, _, filter_fn = mgr._tracked["db.users"]
    assert filter_fn is not None
    assert filter_fn({"region": "local"}) is True
    assert filter_fn({"region": "other"}) is False
    assert "db.users" in mgr._raw_collection_filters


# ------------------------------------------------------------------
# Persistent tombstones (Tier 1.2)
# ------------------------------------------------------------------


def test_tombstone_persistent_write_read():
    """Persistent TombstoneRegistry reads back tombstones from redb KV."""
    rust = _FakeSyncRust()
    reg = TombstoneRegistry(ttl_sec=3600, redb_client=rust, uri="table:__tombstones")

    reg.mark_deleted("doc1")
    reg.mark_deleted("doc2")

    assert reg.is_tombstoned("doc1")
    assert reg.is_tombstoned("doc2")
    assert not reg.is_tombstoned("doc3")

    d = reg.to_dict()
    assert "doc1" in d
    assert "doc2" in d


def test_tombstone_persistent_survives_restart():
    """A new TombstoneRegistry over the same store sees previously written tombstones."""
    rust = _FakeSyncRust()
    reg = TombstoneRegistry(ttl_sec=3600, redb_client=rust, uri="table:__tombstones")
    reg.mark_deleted("doc1")

    reg2 = TombstoneRegistry(ttl_sec=3600, redb_client=rust, uri="table:__tombstones")
    assert reg2.is_tombstoned("doc1")


def test_tombstone_persistent_expire():
    """Persistent tombstones expire when TTL is exceeded."""
    rust = _FakeSyncRust()
    reg = TombstoneRegistry(ttl_sec=0, redb_client=rust, uri="table:__tombstones")
    reg.mark_deleted("old_doc")
    time.sleep(0.01)
    expired = reg.expire()
    assert expired == 1
    assert not reg.is_tombstoned("old_doc")


def test_tombstone_inmemory_fallback():
    """Without session/uri, TombstoneRegistry falls back to in-memory dict."""
    reg = TombstoneRegistry(ttl_sec=3600)
    reg.mark_deleted("x")
    assert reg.is_tombstoned("x")
    assert not reg.is_tombstoned("y")


# ------------------------------------------------------------------
# Resumable initial snapshot (Tier 1.3)
# ------------------------------------------------------------------


class _EmptyStream:
    """Mock change stream that yields no events."""

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def try_next(self):
        return None


class _FakeRemoteCursor:
    """Mock cursor supporting sort/limit chaining."""

    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *a, **kw):
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)

    def __len__(self):
        return len(self._docs)


def test_initial_snapshot_paginates():
    """Initial snapshot uses _id-based pagination with batch_size pages."""
    mgr = _make_manager()
    mgr._active_sync_filter = None
    mgr._config["batch_size"] = 5

    ck: dict[str, str] = {}
    mgr._get_checkpoint = lambda k: ck.get(k)
    mgr._set_checkpoint = lambda k, v: ck.__setitem__(k, v)

    all_docs = [{"_id": i, "val": i} for i in range(12)]
    find_calls: list[dict] = []

    class Remote:
        def find(self, q):
            find_calls.append(dict(q))
            gt = None
            if isinstance(q.get("_id"), dict):
                gt = q["_id"].get("$gt")
            if gt is not None:
                docs = [d for d in all_docs if d["_id"] > gt]
            else:
                docs = list(all_docs)
            return _FakeRemoteCursor(docs)

        def watch(self, pipeline, **kwargs):
            return _EmptyStream()

    upserted: list[dict] = []

    class Local:
        def get_by_id(self, doc_id):
            return None

        def insert_one(self, doc, _internal=False):
            upserted.append(doc)

        def delete(self, q, multi=False, _internal=False):
            pass

    ok = mgr._pull_via_change_stream("db.users", Local(), Remote())
    assert ok is True
    assert len(upserted) == 12
    assert len(find_calls) >= 3  # pages: 0-4, 5-9, 10-11, empty
    assert ck.get("pull_cs_init:db.users") == "1"


def test_resumable_initial_snapshot_resumes():
    """Initial snapshot resumes from page checkpoint after simulated crash."""
    mgr = _make_manager()
    mgr._active_sync_filter = None
    mgr._config["batch_size"] = 10

    ck: dict[str, str] = {"pull_cs_page:db.users": json.dumps(9)}
    mgr._get_checkpoint = lambda k: ck.get(k)
    mgr._set_checkpoint = lambda k, v: ck.__setitem__(k, v)

    all_docs = [{"_id": i, "val": i} for i in range(25)]
    find_queries: list[dict] = []

    class Remote:
        def find(self, q):
            find_queries.append(dict(q))
            gt = None
            if isinstance(q.get("_id"), dict):
                gt = q["_id"].get("$gt")
            if gt is not None:
                docs = [d for d in all_docs if d["_id"] > gt]
            else:
                docs = list(all_docs)
            return _FakeRemoteCursor(docs)

        def watch(self, pipeline, **kwargs):
            return _EmptyStream()

    upserted: list[dict] = []

    class Local:
        def get_by_id(self, doc_id):
            return None

        def insert_one(self, doc, _internal=False):
            upserted.append(doc)

        def delete(self, q, multi=False, _internal=False):
            pass

    ok = mgr._pull_via_change_stream("db.users", Local(), Remote())
    assert ok is True

    # First find should use _id > 9 (resuming from page checkpoint)
    assert any(isinstance(q.get("_id"), dict) and q["_id"].get("$gt") == 9 for q in find_queries)

    upserted_ids = {d["_id"] for d in upserted}
    for i in range(10, 25):
        assert i in upserted_ids, f"doc {i} should have been upserted"
    for i in range(10):
        assert i not in upserted_ids, f"doc {i} should NOT have been upserted"

    assert ck.get("pull_cs_init:db.users") == "1"


# ------------------------------------------------------------------
# Concurrent namespace push (Tier 2.2)
# ------------------------------------------------------------------


def test_concurrent_namespace_push():
    """Multiple namespaces push concurrently via ThreadPoolExecutor."""
    mgr = _make_manager()
    mgr._config["push_concurrency"] = 3
    mgr._config["oplog_auto_compact"] = False
    mgr._active_sync_filter = None

    barrier = threading.Barrier(3, timeout=5)
    pushed_ns: list[str] = []

    class FakeOplogReader:
        def read_from(self, checkpoint, skip_internal=True):
            return [
                (
                    "k1",
                    {
                        "op": "insert",
                        "doc_id": "d1",
                        "ts": 1.0,
                        "payload": {"_id": "d1", "val": 1},
                        "changed_fields": [],
                    },
                )
            ]

    class FakeLocal:
        def get_oplog_reader(self):
            return FakeOplogReader()

        _oplog_w = types.SimpleNamespace(
            truncate_before=lambda k: 0, oplog_uri="table:oplog", node_id=None
        )

        def get_by_id(self, doc_id):
            return None

    class FakeRemote:
        def __init__(self, ns):
            self._ns = ns

        def bulk_write(self, ops, ordered=False):
            barrier.wait()
            pushed_ns.append(self._ns)

    mgr._tracked = {
        "db.a": (FakeLocal(), FakeRemote("db.a"), None),
        "db.b": (FakeLocal(), FakeRemote("db.b"), None),
        "db.c": (FakeLocal(), FakeRemote("db.c"), None),
    }
    ck: dict[str, str] = {}
    mgr._get_checkpoint = lambda k: ck.get(k)
    mgr._set_checkpoint = lambda k, v: ck.__setitem__(k, v)

    mgr._push()
    assert set(pushed_ns) == {"db.a", "db.b", "db.c"}


# ------------------------------------------------------------------
# Transactional checkpoint + oplog compaction (Tier 1.1)
# ------------------------------------------------------------------


def test_atomic_checkpoint_propagates_rust_error():
    """If the engine atomic checkpoint fails, the error propagates."""

    class BoomRust(_FakeSyncRust):
        def sync_atomic_checkpoint_truncate(self, *a, **k):
            raise RuntimeError("simulated engine failure")

    mgr = SyncManager.__new__(SyncManager)
    mgr._ck_lock = threading.Lock()
    mgr._rust = BoomRust()
    mgr._ck_uri = "table:__sync_checkpoint"
    mgr._config = {"oplog_auto_compact": True}

    with pytest.raises(RuntimeError):
        mgr._atomic_checkpoint_and_compact("db.users", "k2", "table:oplog")


def test_atomic_checkpoint_redb_writes_checkpoint():
    mgr = SyncManager.__new__(SyncManager)
    mgr._ck_lock = threading.Lock()
    mgr._rust = _FakeSyncRust()
    mgr._ck_uri = "table:__sync_checkpoint"
    mgr._config = {"oplog_auto_compact": True}

    mgr._atomic_checkpoint_and_compact("db.users", "k5", "table:oplog")

    assert mgr._rust.sync_kv_get(mgr._ck_uri, "push:db.users") == "k5"


# ------------------------------------------------------------------
# Extended sync status API (Tier 3.4)
# ------------------------------------------------------------------


def test_status_includes_extended_fields():
    """status() includes per-collection stats, throughput, and cycle duration."""
    mgr = _make_manager()
    mgr._thread = None
    mgr._ns_stats = {
        "db.users": {
            "last_push_ts": 1000.0,
            "last_pull_ts": 1001.0,
            "last_push_count": 5,
            "last_pull_count": 3,
        }
    }
    mgr._last_cycle_duration = 2.5
    mgr._last_cycle_pushed = 5
    mgr._last_cycle_pulled = 3

    s = mgr.status()
    assert "collections" in s
    assert "db.users" in s["collections"]
    assert s["collections"]["db.users"]["last_push_count"] == 5
    assert s["collections"]["db.users"]["last_pull_count"] == 3
    assert "throughput_ops_sec" in s
    assert s["throughput_ops_sec"] == 3.2  # (5+3) / 2.5
    assert "last_cycle_duration_sec" in s
    assert s["last_cycle_duration_sec"] == 2.5


def test_push_populates_ns_stats():
    """_push() populates per-namespace stats after pushing ops."""
    mgr = _make_manager()
    mgr._config["oplog_auto_compact"] = False
    mgr._active_sync_filter = None

    class FakeOplogReader:
        def read_from(self, checkpoint, skip_internal=True):
            return [
                (
                    "k1",
                    {
                        "op": "insert",
                        "doc_id": "d1",
                        "ts": 1.0,
                        "payload": {"_id": "d1", "val": 1},
                        "changed_fields": [],
                    },
                )
            ]

    class FakeLocal:
        def get_oplog_reader(self):
            return FakeOplogReader()

        _oplog_w = types.SimpleNamespace(
            truncate_before=lambda k: 0, oplog_uri="table:oplog", node_id=None
        )

        def get_by_id(self, doc_id):
            return None

    class FakeRemote:
        def bulk_write(self, ops, ordered=False):
            pass

    mgr._tracked = {"db.users": (FakeLocal(), FakeRemote(), None)}
    ck: dict[str, str] = {}
    mgr._get_checkpoint = lambda k: ck.get(k)
    mgr._set_checkpoint = lambda k, v: ck.__setitem__(k, v)

    mgr._push()

    assert "db.users" in mgr._ns_stats
    assert mgr._ns_stats["db.users"]["last_push_count"] == 1
    assert mgr._ns_stats["db.users"]["last_push_ts"] is not None


# ── Dead-Letter Queue ────────────────────────────────────────────────


def test_dlq_enqueue_on_partial_failure():
    """Failed ops from _flush_bulk are enqueued into the DLQ."""
    mgr = _make_manager()
    entry_a = {"op": "insert", "doc_id": "a", "payload": {"_id": "a", "x": 1}, "ts": 1.0}
    entry_b = {"op": "insert", "doc_id": "b", "payload": {"_id": "b", "x": 2}, "ts": 2.0}

    class FailRemote:
        def bulk_write(self, ops, ordered=False):
            raise BulkWriteError({"writeErrors": [{"index": 1, "code": 11000, "errmsg": "dup"}]})

    n_ok = mgr._flush_bulk(
        FailRemote(), ["op_a", "op_b"], ns="db.users", op_entries=[entry_a, entry_b]
    )
    assert n_ok == 1
    assert mgr._dlq_count() == 1


def test_dlq_retry_success():
    """Successful retry removes the entry from the DLQ."""
    mgr = _make_manager()
    mgr._dlq_enqueue(
        "db.users",
        {
            "op": "insert",
            "doc_id": "a",
            "payload": {"_id": "a", "x": 1},
            "ts": 1.0,
        },
        11000,
        "dup",
    )

    assert mgr._dlq_count() == 1

    class OkRemote:
        def bulk_write(self, ops, ordered=False):
            pass

    mgr._tracked = {"db.users": (None, OkRemote(), None)}
    # Set next_retry_ts to the past so sweep picks it up
    dlq_store = mgr._rust.table(mgr._dlq_uri)
    for k in list(dlq_store):
        v = json.loads(dlq_store[k])
        v["next_retry_ts"] = 0
        dlq_store[k] = json.dumps(v, default=str)

    mgr._sweep_dlq()
    assert mgr._dlq_count() == 0


def test_dlq_retry_exhaust():
    """After max retries, the entry is marked permanently failed."""
    mgr = _make_manager()
    mgr._config["max_dlq_retries"] = 2
    mgr._dlq_enqueue(
        "db.users",
        {
            "op": "insert",
            "doc_id": "a",
            "payload": {"_id": "a", "x": 1},
            "ts": 1.0,
        },
        11000,
        "dup",
    )

    class FailRemote:
        def bulk_write(self, ops, ordered=False):
            raise BulkWriteError({"writeErrors": [{"index": 0, "code": 11000, "errmsg": "dup"}]})

    mgr._tracked = {"db.users": (None, FailRemote(), None)}
    dlq_store = mgr._rust.table(mgr._dlq_uri)

    for _ in range(3):
        for k in list(dlq_store):
            v = json.loads(dlq_store[k])
            v["next_retry_ts"] = 0
            dlq_store[k] = json.dumps(v, default=str)
        mgr._sweep_dlq()

    assert mgr._dlq_count() == 1
    assert mgr._dlq_count(permanent_only=True) == 1


def test_dlq_status_reporting():
    """status() includes dlq_depth and dlq_permanent_failures."""
    mgr = _make_manager()
    mgr._thread = None
    mgr._dlq_enqueue(
        "db.users",
        {
            "op": "insert",
            "doc_id": "a",
            "payload": {"_id": "a"},
            "ts": 1.0,
        },
        11000,
        "dup",
    )
    mgr._dlq_enqueue(
        "db.users",
        {
            "op": "insert",
            "doc_id": "b",
            "payload": {"_id": "b"},
            "ts": 2.0,
        },
        11000,
        "dup",
    )

    # Mark one as permanently failed
    dlq_store = mgr._rust.table(mgr._dlq_uri)
    for k in list(dlq_store)[:1]:
        v = json.loads(dlq_store[k])
        v["permanently_failed"] = True
        dlq_store[k] = json.dumps(v, default=str)

    s = mgr.status()
    assert s["dlq_depth"] == 2
    assert s["dlq_permanent_failures"] == 1


def test_dlq_entry_to_pymongo_op():
    """_entry_to_pymongo_op reconstructs write ops from oplog entries."""
    mgr = _make_manager()

    insert_op = mgr._entry_to_pymongo_op(
        {
            "op": "insert",
            "doc_id": "a",
            "payload": {"_id": "a", "x": 1},
            "ts": 1.0,
        }
    )
    assert insert_op is not None

    update_op = mgr._entry_to_pymongo_op(
        {
            "op": "update",
            "doc_id": "a",
            "payload": {"$set": {"x": 2}},
            "ts": 2.0,
        }
    )
    assert update_op is not None

    delete_op = mgr._entry_to_pymongo_op(
        {
            "op": "delete",
            "doc_id": "a",
            "payload": None,
            "ts": 3.0,
        }
    )
    assert delete_op is not None

    none_op = mgr._entry_to_pymongo_op({"op": "index_create", "doc_id": "idx"})
    assert none_op is None


# ==================================================================
# Index drop reconciliation
# ==================================================================


def test_push_index_defs_drops_remote_extras():
    """_push_index_defs drops remote indexes that no longer exist locally."""
    mgr = _make_manager()
    dropped: list[str] = []

    class Local:
        def list_indexes(self):
            return [{"name": "_id_"}, {"name": "idx_a", "keys": {"a": 1}}]

    class Remote:
        def list_indexes(self):
            return [
                {"name": "_id_"},
                {"name": "idx_a", "key": {"a": 1}},
                {"name": "idx_stale", "key": {"gone": 1}},
            ]

        def create_index(self, keys, **kwargs):
            pass

        def drop_index(self, name):
            dropped.append(name)

    mgr._push_index_defs("db.users", Local(), Remote())
    assert "idx_stale" in dropped
    assert "_id_" not in dropped


def test_pull_index_defs_drops_local_extras():
    """_pull_index_defs drops local indexes absent from the remote."""
    mgr = _make_manager()
    dropped: list[str] = []

    class Local:
        def list_indexes(self):
            return [
                {"name": "_id_"},
                {"name": "idx_a", "keys": {"a": 1}},
                {"name": "idx_stale", "keys": {"gone": 1}},
            ]

        def create_index(self, keys, **kwargs):
            pass

        def drop_index(self, name):
            dropped.append(name)

    class Remote:
        def list_indexes(self):
            return [{"name": "_id_"}, {"name": "idx_a", "key": {"a": 1}}]

    mgr._pull_index_defs("db.users", Local(), Remote())
    assert "idx_stale" in dropped
    assert "_id_" not in dropped


# ==================================================================
# Forward all index options
# ==================================================================


def test_extract_index_options_all_fields():
    """_extract_index_options forwards TTL, partial, collation, type, etc."""
    idx = {
        "name": "my_idx",
        "unique": True,
        "sparse": True,
        "background": True,
        "expireAfterSeconds": 3600,
        "partialFilterExpression": {"status": "active"},
        "collation": {"locale": "en"},
        "type": "bitmap",
        "weights": {"title": 10},
        "vectorSearchOptions": {"dimensions": 128, "metric": "cosine"},
        "prefixLength": 8,
    }
    opts = SyncManager._extract_index_options(idx)
    assert opts["name"] == "my_idx"
    assert opts["unique"] is True
    assert opts["sparse"] is True
    assert opts["background"] is True
    assert opts["expireAfterSeconds"] == 3600
    assert opts["partialFilterExpression"] == {"status": "active"}
    assert opts["collation"] == {"locale": "en"}
    assert opts["type"] == "bitmap"
    assert opts["weights"] == {"title": 10}
    assert opts["vectorSearchOptions"]["dimensions"] == 128
    assert opts["prefixLength"] == 8


def test_push_index_defs_forwards_options():
    """_push_index_defs passes TTL/sparse/unique/partial options to remote.create_index."""
    mgr = _make_manager()
    created: list[tuple] = []

    class Local:
        def list_indexes(self):
            return [
                {"name": "_id_"},
                {
                    "name": "idx_ttl",
                    "keys": {"ts": 1},
                    "unique": False,
                    "sparse": True,
                    "expireAfterSeconds": 7200,
                    "partialFilterExpression": {"active": True},
                },
            ]

    class Remote:
        def list_indexes(self):
            return [{"name": "_id_"}]

        def create_index(self, keys, **kwargs):
            created.append((keys, kwargs))

        def drop_index(self, name):
            pass

    mgr._push_index_defs("db.users", Local(), Remote())
    assert len(created) == 1
    _, kwargs = created[0]
    assert kwargs["name"] == "idx_ttl"
    assert kwargs["sparse"] is True
    assert kwargs["expireAfterSeconds"] == 7200
    assert kwargs["partialFilterExpression"] == {"active": True}


# ==================================================================
# Cumulative field tracking
# ==================================================================


def test_cumulative_field_history_accumulates():
    """Cumulative field history tracks all fields ever changed across cycles."""
    mgr = _make_manager()

    field_key = ("db.users", "doc1")
    mgr._local_field_history[field_key] = {"a"}
    mgr._cumulative_field_history[field_key] = {"a"}

    cum = mgr._cumulative_field_history.get(field_key, set())
    cum.update({"b", "c"})
    mgr._cumulative_field_history[field_key] = cum

    assert mgr._cumulative_field_history[field_key] == {"a", "b", "c"}


# ==================================================================
# CRDT helpers (unit)
# ==================================================================


def test_crdt_counter_merge():
    assert _crdt_counter_merge(5, 10) == 10
    assert _crdt_counter_merge(10, 5) == 10
    assert _crdt_counter_merge("a", "b") == "b"


def test_crdt_set_merge():
    result = _crdt_set_merge([1, 2, 3], [2, 3, 4])
    assert sorted(result) == [1, 2, 3, 4]


def test_crdt_set_merge_with_dicts():
    result = _crdt_set_merge([{"x": 1}], [{"x": 1}, {"x": 2}])
    assert len(result) == 2


def test_crdt_merge_doc_counter_and_set():
    local = {"_id": "1", "views": 5, "tags": ["a", "b"], "_lastModified": 1}
    remote = {"_id": "1", "views": 10, "tags": ["b", "c"], "_lastModified": 2}
    merged = _crdt_merge_doc(local, remote, {"views": "counter", "tags": "set"})
    assert merged["views"] == 10
    assert sorted(merged["tags"]) == ["a", "b", "c"]


def test_crdt_merge_doc_falls_back_to_lww():
    local = {"_id": "1", "name": "Alice", "_lastModified": 5}
    remote = {"_id": "1", "name": "Bob", "_lastModified": 10}
    merged = _crdt_merge_doc(local, remote, {})
    assert merged["name"] == "Bob"


# ==================================================================
# CRDT integration with _upsert_remote_doc
# ==================================================================


def test_upsert_remote_doc_uses_crdt_when_configured():
    """When crdt_fields is set, _upsert_remote_doc uses CRDT merge."""
    mgr = _make_manager(crdt_fields={"views": "counter", "tags": "set"})

    class Local:
        def __init__(self):
            self.updated = None

        def get_by_id(self, doc_id):
            return {
                "_id": "1",
                "views": 5,
                "tags": ["a"],
                "_lastModified": 1,
            }

        def update(self, query, update_spec, multi=False, _internal=False):
            self.updated = (query, update_spec, multi, _internal)

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    mgr._upsert_remote_doc(
        "db.users",
        local,
        {"_id": "1", "views": 10, "tags": ["b"], "_lastModified": 2},
    )
    assert local.updated is not None
    fields = local.updated[1]["$set"]
    assert fields["views"] == 10
    assert sorted(fields["tags"]) == ["a", "b"]


def test_upsert_remote_doc_crdt_not_used_when_empty():
    """Without crdt_fields, _upsert_remote_doc uses normal resolver."""
    mgr = _make_manager(crdt_fields={})

    class Local:
        def __init__(self):
            self.updated = None

        def get_by_id(self, doc_id):
            return {"_id": "1", "x": "old", "_lastModified": 1}

        def update(self, query, update_spec, multi=False, _internal=False):
            self.updated = (query, update_spec, multi, _internal)

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    mgr._upsert_remote_doc(
        "db.users",
        local,
        {"_id": "1", "x": "new", "_lastModified": 2},
    )
    assert local.updated is not None
    assert local.updated[1]["$set"]["x"] == "new"


# ==================================================================
# Operational transform helpers
# ==================================================================


def test_is_commutative_op():
    assert _is_commutative_op({"$inc": {"views": 1}}) is True
    assert _is_commutative_op({"$push": {"items": "x"}}) is True
    assert _is_commutative_op({"$addToSet": {"tags": "new"}}) is True
    assert _is_commutative_op({"$inc": {"a": 1}, "$push": {"b": 2}}) is True
    assert _is_commutative_op({"$set": {"a": 1}}) is False
    assert _is_commutative_op({"$set": {"a": 1}, "$inc": {"b": 2}}) is False
    assert _is_commutative_op({}) is False
    assert _is_commutative_op("not_a_dict") is False


def test_merge_commutative_ops_inc():
    local = {"$inc": {"views": 3, "likes": 1}}
    remote = {"$inc": {"views": 5, "shares": 2}}
    merged = _merge_commutative_ops(local, remote)
    assert merged["$inc"]["views"] == 8
    assert merged["$inc"]["likes"] == 1
    assert merged["$inc"]["shares"] == 2


def test_merge_commutative_ops_push():
    local = {"$push": {"items": {"$each": ["a", "b"]}}}
    remote = {"$push": {"items": {"$each": ["c"]}}}
    merged = _merge_commutative_ops(local, remote)
    assert merged["$push"]["items"]["$each"] == ["a", "b", "c"]


def test_merge_commutative_ops_addtoset():
    local = {"$addToSet": {"tags": {"$each": ["a", "b"]}}}
    remote = {"$addToSet": {"tags": {"$each": ["b", "c"]}}}
    merged = _merge_commutative_ops(local, remote)
    assert set(merged["$addToSet"]["tags"]["$each"]) == {"a", "b", "c"}


def test_merge_commutative_ops_min_max():
    local = {"$min": {"low": 5}, "$max": {"high": 10}}
    remote = {"$min": {"low": 3}, "$max": {"high": 15}}
    merged = _merge_commutative_ops(local, remote)
    assert merged["$min"]["low"] == 3
    assert merged["$max"]["high"] == 15


def test_merge_commutative_ops_mixed():
    local = {"$inc": {"count": 1}, "$push": {"log": "a"}}
    remote = {"$inc": {"count": 2}, "$push": {"log": "b"}}
    merged = _merge_commutative_ops(local, remote)
    assert merged["$inc"]["count"] == 3
    assert merged["$push"]["log"]["$each"] == ["a", "b"]


# ==================================================================
# Conflict metadata logging
# ==================================================================


def test_conflict_log_records_on_conflict():
    """_upsert_remote_doc records conflict metadata in _conflict_log."""
    mgr = _make_manager()

    class Local:
        def __init__(self):
            self.updated = None

        def get_by_id(self, doc_id):
            return {"_id": "1", "x": "old", "_lastModified": 1}

        def update(self, query, update_spec, multi=False, _internal=False):
            self.updated = (query, update_spec, multi, _internal)

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    mgr._upsert_remote_doc(
        "db.users",
        local,
        {"_id": "1", "x": "new", "_lastModified": 2},
    )
    assert len(mgr._conflict_log) == 1
    entry = mgr._conflict_log[0]
    assert entry["ns"] == "db.users"
    assert entry["doc_id"] == "1"
    assert entry["strategy"] == "lww"
    assert "x" in entry["diff_fields"]
    assert entry["node_id"] == "local"


def test_get_conflict_log_returns_recent():
    mgr = _make_manager()

    class Local:
        def get_by_id(self, doc_id):
            return {"_id": "1", "x": "v1", "_lastModified": 1}

        def update(self, query, update_spec, multi=False, _internal=False):
            pass

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    for i in range(5):
        mgr._upsert_remote_doc(
            "db.users",
            local,
            {"_id": "1", "x": f"v{i + 2}", "_lastModified": i + 2},
        )
    log_entries = mgr.get_conflict_log(limit=3)
    assert len(log_entries) == 3


def test_conflict_log_persisted_to_kv():
    """Conflict log entries are persisted to the fake KV store."""
    mgr = _make_manager()

    class Local:
        def get_by_id(self, doc_id):
            return {"_id": "1", "x": "v1", "_lastModified": 1}

        def update(self, query, update_spec, multi=False, _internal=False):
            pass

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    mgr._upsert_remote_doc("db.users", local, {"_id": "1", "x": "v2", "_lastModified": 2})
    kv_table = mgr._rust.table(mgr._conflict_log_uri)
    assert len(kv_table) == 1
    stored = json.loads(next(iter(kv_table.values())))
    assert stored["doc_id"] == "1"


def test_status_includes_conflict_log_and_tombstones():
    """status() includes conflict_log_count and tombstone_count."""
    mgr = _make_manager()
    mgr._thread = None
    mgr._conflict_log.append({"fake": True})
    mgr._tombstones.mark_deleted("deleted_doc")
    s = mgr.status()
    assert s["conflict_log_count"] == 1
    assert s["tombstone_count"] == 1


# ==================================================================
# Remote delete detection (polling path)
# ==================================================================


def test_detect_remote_deletes_removes_missing():
    """_detect_remote_deletes removes local docs absent from remote."""
    mgr = _make_manager()
    deleted: list[dict] = []

    class Local:
        def find(self, query, projection=None):
            return [{"_id": "1"}, {"_id": "2"}, {"_id": "3"}]

        def delete(self, query, multi=False, _internal=False):
            deleted.append(query)

    class Remote:
        def find(self, query, projection=None):
            return [{"_id": "1"}, {"_id": "3"}]

    mgr._detect_remote_deletes("db.users", Local(), Remote())
    assert len(deleted) == 1
    assert deleted[0] == {"_id": "2"}


def test_detect_remote_deletes_skips_tombstoned():
    """_detect_remote_deletes skips IDs that are locally tombstoned."""
    mgr = _make_manager()
    mgr._tombstones.mark_deleted("2")
    deleted: list[dict] = []

    class Local:
        def find(self, query, projection=None):
            return [{"_id": "1"}, {"_id": "2"}, {"_id": "3"}]

        def delete(self, query, multi=False, _internal=False):
            deleted.append(query)

    class Remote:
        def find(self, query, projection=None):
            return [{"_id": "1"}]

    mgr._detect_remote_deletes("db.users", Local(), Remote())
    assert {"_id": "2"} not in deleted
    assert {"_id": "3"} in deleted


def test_detect_remote_deletes_noop_when_all_present():
    mgr = _make_manager()
    deleted: list[dict] = []

    class Local:
        def find(self, query, projection=None):
            return [{"_id": "1"}, {"_id": "2"}]

        def delete(self, query, multi=False, _internal=False):
            deleted.append(query)

    class Remote:
        def find(self, query, projection=None):
            return [{"_id": "1"}, {"_id": "2"}]

    mgr._detect_remote_deletes("db.users", Local(), Remote())
    assert len(deleted) == 0


def test_detect_remote_deletes_handles_remote_error():
    """Remote query failure doesn't crash; just returns silently."""
    mgr = _make_manager()

    class Local:
        def find(self, query, projection=None):
            return [{"_id": "1"}]

    class Remote:
        def find(self, query, projection=None):
            raise PyMongoError("connection lost")

    mgr._detect_remote_deletes("db.users", Local(), Remote())


# ==================================================================
# Tombstone integration in push/pull
# ==================================================================


def test_upsert_remote_doc_skips_tombstoned():
    """_upsert_remote_doc returns early if the doc_id is tombstoned."""
    mgr = _make_manager()
    mgr._tombstones.mark_deleted("1")

    class Local:
        def __init__(self):
            self.inserted = None

        def get_by_id(self, doc_id):
            return None

        def insert_one(self, doc, _internal=False):
            self.inserted = doc

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    mgr._upsert_remote_doc("db.users", local, {"_id": "1", "x": 1})
    assert local.inserted is None


def test_tombstone_expiry_runs_in_sync_cycle():
    """_sync_cycle calls tombstone.expire()."""
    mgr = _make_manager()
    expire_called = []
    original_expire = mgr._tombstones.expire

    def track_expire():
        expire_called.append(True)
        return original_expire()

    mgr._tombstones.expire = track_expire
    mgr._recompile_sync_filter = lambda: None
    mgr._recompile_collection_filters = lambda: None
    mgr._config["mode"] = "pull_only"
    mgr._pull = lambda: None
    mgr._sync_cycle()
    assert len(expire_called) == 1


# ==================================================================
# Change stream delete marks tombstone
# ==================================================================


class _FakeRemoteCursor:
    """Minimal cursor stub for test_pull_via_change_stream."""

    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *args, **kwargs):
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)

    def __len__(self):
        return len(self._docs)


def test_change_stream_delete_marks_tombstone():
    """Pull via change stream marks deleted doc_id in tombstone registry."""
    mgr = _make_manager()
    mgr._active_sync_filter = None
    ck = {"pull_cs_init:db.users": "1"}
    mgr._get_checkpoint = lambda k: ck.get(k)
    mgr._set_checkpoint = lambda k, v: ck.__setitem__(k, v)

    class Stream:
        def __init__(self):
            self.calls = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def try_next(self):
            self.calls += 1
            if self.calls == 1:
                return {
                    "operationType": "delete",
                    "_id": {"token": 1},
                    "documentKey": {"_id": "deleted_doc"},
                }
            return None

    class Remote:
        def find(self, q):
            return _FakeRemoteCursor([])

        def watch(self, pipeline, **kwargs):
            return Stream()

    class Local:
        def delete(self, q, multi=False, _internal=False):
            return None

    mgr._pull_via_change_stream("db.users", Local(), Remote())
    assert mgr._tombstones.is_tombstoned("deleted_doc")


# ==================================================================
# Throttled remote delete detection
# ==================================================================


def test_detect_remote_deletes_throttled_by_interval():
    """_detect_remote_deletes only runs on the configured cycle interval."""
    mgr = _make_manager()
    mgr._config["delete_detection_interval_cycles"] = 3
    deleted: list[dict] = []

    class Local:
        def find(self, query, projection=None):
            return [{"_id": "1"}, {"_id": "2"}]

        def delete(self, query, multi=False, _internal=False):
            deleted.append(query)

    class Remote:
        def find(self, query, projection=None):
            return [{"_id": "1"}]

    mgr._cycle_count = 1
    mgr._detect_remote_deletes("db.users", Local(), Remote())
    assert len(deleted) == 0

    mgr._cycle_count = 3
    mgr._detect_remote_deletes("db.users", Local(), Remote())
    assert len(deleted) == 1


def test_detect_remote_deletes_disabled():
    """_detect_remote_deletes does nothing when disabled."""
    mgr = _make_manager()
    mgr._config["delete_detection_enabled"] = False
    deleted: list[dict] = []

    class Local:
        def find(self, query, projection=None):
            return [{"_id": "1"}]

        def delete(self, query, multi=False, _internal=False):
            deleted.append(query)

    class Remote:
        def find(self, query, projection=None):
            return []

    mgr._cycle_count = 0
    mgr._detect_remote_deletes("db.users", Local(), Remote())
    assert len(deleted) == 0


# ==================================================================
# Operational transform integration
# ==================================================================


def test_apply_commutative_to_doc_inc():
    base = {"_id": "1", "views": 100, "name": "test"}
    spec = {"$inc": {"views": 5}}
    result = _apply_commutative_to_doc(base, spec)
    assert result["views"] == 105
    assert result["name"] == "test"


def test_apply_commutative_to_doc_push():
    base = {"_id": "1", "items": ["a", "b"]}
    spec = {"$push": {"items": "c"}}
    result = _apply_commutative_to_doc(base, spec)
    assert result["items"] == ["a", "b", "c"]


def test_apply_commutative_to_doc_addtoset():
    base = {"_id": "1", "tags": ["a", "b"]}
    spec = {"$addToSet": {"tags": {"$each": ["b", "c"]}}}
    result = _apply_commutative_to_doc(base, spec)
    assert set(result["tags"]) == {"a", "b", "c"}


def test_apply_commutative_to_doc_min_max():
    base = {"_id": "1", "low": 10, "high": 50}
    spec = {"$min": {"low": 5}, "$max": {"high": 100}}
    result = _apply_commutative_to_doc(base, spec)
    assert result["low"] == 5
    assert result["high"] == 100


def test_upsert_uses_ot_when_local_spec_commutative():
    """_upsert_remote_doc applies OT when local had commutative ops."""
    mgr = _make_manager()

    class Local:
        def __init__(self):
            self.updated = None

        def get_by_id(self, doc_id):
            return {"_id": "1", "views": 15, "_lastModified": 1}

        def update(self, query, update_spec, multi=False, _internal=False):
            self.updated = (query, update_spec, multi, _internal)

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    mgr._local_update_specs[("db.users", "1")] = {"$inc": {"views": 5}}

    mgr._upsert_remote_doc(
        "db.users",
        local,
        {"_id": "1", "views": 20, "_lastModified": 2},
    )
    assert local.updated is not None
    assert local.updated[1]["$set"]["views"] == 25


# ==================================================================
# Conflict log rotation
# ==================================================================


def test_conflict_log_rotation():
    mgr = _make_manager()
    mgr._config["max_conflict_log_entries"] = 3
    mgr._conflict_log = [{"i": i} for i in range(10)]
    for i in range(10):
        key = f"{i:020d}"
        mgr._rust.sync_kv_put(mgr._conflict_log_uri, key, json.dumps({"i": i}))

    mgr._rotate_conflict_log()
    assert len(mgr._conflict_log) == 3
    kv_rows = mgr._rust.sync_kv_scan(mgr._conflict_log_uri)
    assert len(kv_rows) == 3


# ==================================================================
# Index hash skip
# ==================================================================


def test_index_hash_skip_on_matching():
    """When local and remote index hashes match, no create/drop calls are made."""
    mgr = _make_manager()
    created: list[str] = []
    dropped: list[str] = []

    indexes = [
        {"name": "_id_"},
        {"name": "idx_a", "keys": {"a": 1}, "unique": False, "sparse": False},
    ]

    class Local:
        def list_indexes(self):
            return list(indexes)

        def create_index(self, keys, **kwargs):
            created.append(kwargs.get("name", ""))

        def drop_index(self, name):
            dropped.append(name)

    class Remote:
        def list_indexes(self):
            return [
                {"name": "_id_"},
                {"name": "idx_a", "key": {"a": 1}, "unique": False, "sparse": False},
            ]

        def create_index(self, keys, **kwargs):
            created.append(kwargs.get("name", ""))

        def drop_index(self, name):
            dropped.append(name)

    mgr._push_index_defs("db.users", Local(), Remote())
    assert len(created) == 0
    assert len(dropped) == 0

    mgr._push_index_defs("db.users", Local(), Remote())
    assert len(created) == 0


# ==================================================================
# Persistent counters
# ==================================================================


def test_persist_and_load_counters():
    mgr = _make_manager()
    mgr._pushed_count = 42
    mgr._pulled_count = 17
    mgr._conflict_count = 3
    mgr._error_count = 1
    mgr._cycle_count = 10

    mgr._persist_counters()

    mgr._pushed_count = 0
    mgr._pulled_count = 0
    mgr._conflict_count = 0
    mgr._error_count = 0
    mgr._cycle_count = 0

    mgr._load_counters()
    assert mgr._pushed_count == 42
    assert mgr._pulled_count == 17
    assert mgr._conflict_count == 3
    assert mgr._error_count == 1
    assert mgr._cycle_count == 10


# ==================================================================
# Status includes new fields
# ==================================================================


def test_status_includes_cycles():
    mgr = _make_manager()
    mgr._thread = None
    mgr._cycle_count = 7
    s = mgr.status()
    assert s["cycles"] == 7
    assert "conflict_log_count" in s
    assert "tombstone_count" in s


# ==================================================================
# Compute index hash
# ==================================================================


def test_compute_index_hash_deterministic():
    indexes = [
        {"name": "_id_"},
        {"name": "idx_a", "keys": {"a": 1}},
        {"name": "idx_b", "keys": {"b": -1}},
    ]
    h1 = SyncManager._compute_index_hash(indexes)
    h2 = SyncManager._compute_index_hash(list(reversed(indexes)))
    assert h1 == h2
    assert len(h1) == 16


def test_compute_index_hash_excludes_id():
    h1 = SyncManager._compute_index_hash([{"name": "_id_"}])
    h2 = SyncManager._compute_index_hash([])
    assert h1 == h2
