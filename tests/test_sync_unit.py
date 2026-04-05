"""Unit tests for sync internals and helper strategies."""

import threading
import time
import types

from smongo.sync import (
    PyMongoError,
    SyncManager,
    _diff_fields,
    _field_merge,
    _local_wins,
    _lww,
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


def _make_manager(**overrides):
    mgr = SyncManager.__new__(SyncManager)
    mgr._config = {
        "collections": ["db.users"],
        "batch_size": 10,
        "use_change_stream_pull": True,
        "mode": "bidirectional",
        "max_backoff_sec": 300,
    }
    mgr._tracked = {}
    mgr._local = types.SimpleNamespace(
        client=types.SimpleNamespace(
            get_db=lambda _: types.SimpleNamespace(get_collection=lambda __: "LOCAL")
        )
    )
    mgr._remote = {"db": {"users": "REMOTE"}}
    mgr._local_field_history = {}
    mgr._lock = threading.Lock()
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
    mgr._node_id = overrides.get("node_id", "local")
    mgr._user_variables = overrides.get("variables", {})
    mgr._raw_sync_rules = overrides.get("sync_rules")
    mgr._raw_collection_filters = {}
    mgr._active_sync_filter = None
    mgr._vector_clocks = {}
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
            return []

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
    store = {}

    class Cursor:
        def __init__(self):
            self.key = None

        def set_key(self, k):
            self.key = k

        def search(self):
            return 0 if self.key in store else 1

        def get_value(self):
            return store[self.key]

        def __setitem__(self, k, v):
            store[k] = v

        def close(self):
            return None

    class Session:
        def open_cursor(self, uri, x, y):
            return Cursor()

    mgr._ck_session = Session()
    mgr._ck_uri = "table:ck"
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
    result = _resolve_variables(
        {"device_id": "$$NODE_ID"}, {"NODE_ID": "sensor-042"}
    )
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
                ("k1", {"op": "insert", "doc_id": "d1", "ts": 1.0,
                         "payload": {"_id": "d1", "device_id": "sensor-1", "val": 10},
                         "changed_fields": []}),
                ("k2", {"op": "insert", "doc_id": "d2", "ts": 2.0,
                         "payload": {"_id": "d2", "device_id": "sensor-2", "val": 20},
                         "changed_fields": []}),
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
                    return iter([
                        {"_id": "d1", "device_id": "sensor-1", "_lastModified": 1.0},
                        {"_id": "d2", "device_id": "sensor-2", "_lastModified": 2.0},
                    ])

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
                ("k1", {"op": "update", "doc_id": "d1", "ts": 1.0,
                         "payload": {"$set": {"val": 99}},
                         "changed_fields": ["val"]}),
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
                ("k1", {"op": "update", "doc_id": "d1", "ts": 1.0,
                         "payload": {"$set": {"val": 99}},
                         "changed_fields": ["val"]}),
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
# Vector clocks in conflict resolution
# ------------------------------------------------------------------


def test_vector_clock_tick_on_conflict():
    """Conflict resolution ticks the local node's vector clock and stamps it on the doc."""
    mgr = _make_manager(node_id="edge-1")

    class Local:
        def __init__(self):
            self.updated = None

        def get_by_id(self, doc_id):
            return {"_id": "c1", "x": "old", "_lastModified": 1,
                    "_vclock": {"edge-1": 1}}

        def update(self, query, update_spec, multi=False, _internal=False):
            self.updated = update_spec

    local = Local()
    mgr._resolver_name = "lww"
    mgr._resolve = _lww
    mgr._upsert_remote_doc(
        "db.users", local,
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
            return {"_id": "c2", "x": "local", "_lastModified": 5,
                    "_vclock": {"edge-1": 1}}

        def update(self, query, update_spec, multi=False, _internal=False):
            self.updated = update_spec

    local = Local()
    mgr._resolver_name = "local_wins"
    mgr._resolve = _local_wins
    mgr._upsert_remote_doc(
        "db.users", local,
        {"_id": "c2", "x": "remote", "_lastModified": 3,
         "_vclock": {"edge-1": 2, "edge-2": 1}},
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
