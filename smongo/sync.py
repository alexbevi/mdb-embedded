"""
Sync Layer -- bidirectional sync between the local embedded engine and MongoDB Atlas.

Architecture:
    - Background thread tails the local oplog and pushes mutations to Atlas
    - Optionally pulls remote changes via change streams or timestamp polling
    - Conflict resolution strategies: LWW, local-wins, remote-wins, or custom callable
    - Checkpoints, DLQ, and tombstones: WiredTiger tables when using ``local+wt://``,
      or redb KV / atomic truncate when using ``local://`` (default redb backend)
    - Exponential backoff on consecutive errors, per-collection selective filters
    - MQL-native sync rules with $$NOW, $$NODE_ID, and user-defined variable substitution
"""

import copy
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any, cast

try:
    from pymongo import DeleteOne, InsertOne, UpdateOne
    from pymongo import MongoClient as _PyMongoClient
    from pymongo.errors import BulkWriteError, PyMongoError
except ImportError:
    _PyMongoClient = None  # type: ignore[misc, assignment]
    UpdateOne = DeleteOne = InsertOne = None  # type: ignore[misc, assignment]

    class BulkWriteError(Exception):  # type: ignore[no-redef]
        pass

    class PyMongoError(Exception):  # type: ignore[no-redef]
        pass


try:
    from bson import ObjectId as BsonObjectId
except ImportError:
    BsonObjectId = None  # type: ignore[misc, assignment]

from concurrent.futures import ThreadPoolExecutor, as_completed

from smongo._smongo_core import ejson_default as _ejson_default
from smongo._smongo_core import ejson_object_hook as _ejson_object_hook
from smongo._smongo_core import from_pymongo as _from_pymongo
from smongo._smongo_core import to_pymongo as _to_pymongo

from ._types import Document, Predicate
from .index import DuplicateKeyError
from .query import compile_query

log = logging.getLogger("smongo.sync")


# ------------------------------------------------------------------
# Vector clocks
# ------------------------------------------------------------------


class VectorClock:
    """Per-document vector clock for causal ordering across replicas.

    Each writer (identified by a string ``node_id``) maintains a monotonic
    counter.  Two events are concurrent when neither dominates the other.
    """

    def __init__(self, state: dict[str, int] | None = None) -> None:
        self._clock: dict[str, int] = dict(state or {})

    def tick(self, node_id: str) -> "VectorClock":
        self._clock[node_id] = self._clock.get(node_id, 0) + 1
        return self

    def merge(self, other: "VectorClock") -> "VectorClock":
        for nid, ts in other._clock.items():
            self._clock[nid] = max(self._clock.get(nid, 0), ts)
        return self

    def dominates(self, other: "VectorClock") -> bool:
        """True if every entry in *other* is <= our entry, with at least one strictly greater."""
        if not other._clock:
            return bool(self._clock)
        for nid, ts in other._clock.items():
            if self._clock.get(nid, 0) < ts:
                return False
        return any(
            self._clock.get(nid, 0) > other._clock.get(nid, 0)
            for nid in set(self._clock) | set(other._clock)
        )

    def concurrent_with(self, other: "VectorClock") -> bool:
        return not self.dominates(other) and not other.dominates(self)

    def to_dict(self) -> dict[str, int]:
        return dict(self._clock)

    @classmethod
    def from_dict(cls, d: dict[str, int] | None) -> "VectorClock":
        return cls(d)


# ------------------------------------------------------------------
# CRDT helpers
# ------------------------------------------------------------------


def _crdt_counter_merge(local_val: Any, remote_val: Any) -> Any:
    """Merge two counter values (grow-only counter / PNCounter)."""
    if isinstance(local_val, int | float) and isinstance(remote_val, int | float):
        return max(local_val, remote_val)
    return remote_val


def _crdt_set_merge(local_val: Any, remote_val: Any) -> Any:
    """Merge two sets (G-Set / OR-Set approximation): union of elements."""
    if isinstance(local_val, list) and isinstance(remote_val, list):
        seen: set[Any] = set()
        merged: list[Any] = []
        for item in local_val + remote_val:
            key = (
                json.dumps(item, sort_keys=True, default=str)
                if isinstance(item, dict | list)
                else item
            )
            if key not in seen:
                seen.add(key)
                merged.append(item)
        return merged
    return remote_val


def _crdt_merge_doc(
    local_doc: Document, remote_doc: Document, crdt_fields: dict[str, str] | None = None
) -> Document:
    """Merge two documents using CRDT semantics for annotated fields.

    ``crdt_fields`` maps field names to CRDT types (``"counter"`` or ``"set"``).
    Non-annotated fields fall back to LWW.
    """
    crdt_fields = crdt_fields or {}
    merged = dict(local_doc)
    local_ts = (local_doc or {}).get("_lastModified", 0) or 0
    remote_ts = (remote_doc or {}).get("_lastModified", 0) or 0

    for field in set(local_doc) | set(remote_doc):
        if field == "_id":
            continue
        if field in crdt_fields:
            crdt_type = crdt_fields[field]
            lv = local_doc.get(field)
            rv = remote_doc.get(field)
            if crdt_type == "counter":
                merged[field] = _crdt_counter_merge(lv, rv)
            elif crdt_type == "set":
                merged[field] = _crdt_set_merge(lv, rv)
            else:
                merged[field] = rv if remote_ts >= local_ts else lv
        elif field in remote_doc:
            merged[field] = (
                remote_doc[field]
                if remote_ts >= local_ts
                else local_doc.get(field, remote_doc[field])
            )
    return merged


# ------------------------------------------------------------------
# Tombstone TTL
# ------------------------------------------------------------------

DEFAULT_TOMBSTONE_TTL_SEC = 7 * 24 * 3600  # 7 days


class TombstoneRegistry:
    """Track deleted document IDs with timestamps for tombstone expiry.

    When *session* and *uri* are provided, tombstones are persisted in a
    WiredTiger table and survive process restarts.  When *redb_client* and
    *uri* are provided (hybrid sync with :class:`~smongo.storage.redb_engine.RedbClient`),
    persistence uses the Rust redb KV helpers on the same file as the engine.
    Otherwise falls back to an in-memory dict (useful for unit tests).
    """

    def __init__(
        self,
        ttl_sec: int = DEFAULT_TOMBSTONE_TTL_SEC,
        session: Any = None,
        uri: str | None = None,
        redb_client: Any = None,
    ) -> None:
        self._ttl = ttl_sec
        self._lock = threading.Lock()
        self._session = session
        self._uri = uri
        self._redb_client = redb_client
        self._persistent_wt = session is not None and uri is not None
        self._persistent_redb = redb_client is not None and uri is not None
        self._persistent = self._persistent_wt or self._persistent_redb
        if not self._persistent:
            self._tombstones: dict[str, float] = {}

    def mark_deleted(self, doc_id: Any) -> None:
        key = str(doc_id)
        with self._lock:
            if self._persistent_redb:
                assert self._uri is not None and self._redb_client is not None
                self._redb_client.sync_kv_put(self._uri, key, str(time.time()))
            elif self._persistent_wt:
                cursor = self._session.open_cursor(self._uri, None, "overwrite=true")
                cursor[key] = str(time.time())
                cursor.close()
            else:
                self._tombstones[key] = time.time()

    def is_tombstoned(self, doc_id: Any) -> bool:
        key = str(doc_id)
        with self._lock:
            if self._persistent_redb:
                assert self._uri is not None and self._redb_client is not None
                return self._redb_client.sync_kv_get(self._uri, key) is not None
            if self._persistent_wt:
                cursor = self._session.open_cursor(self._uri, None, None)
                cursor.set_key(key)
                found = bool(cursor.search() == 0)
                cursor.close()
                return found
            return key in self._tombstones

    def expire(self) -> int:
        now = time.time()
        with self._lock:
            if self._persistent_redb:
                assert self._uri is not None and self._redb_client is not None
                rows = self._redb_client.sync_kv_scan(self._uri)
                to_remove = [k for k, v in rows if now - float(v) > self._ttl]
                for k in to_remove:
                    self._redb_client.sync_kv_remove(self._uri, k)
                return len(to_remove)
            if self._persistent_wt:
                cursor = self._session.open_cursor(self._uri, None, None)
                to_remove: list[str] = []
                while cursor.next() == 0:
                    k: str = cursor.get_key()
                    ts = float(cursor.get_value())
                    if now - ts > self._ttl:
                        to_remove.append(k)
                cursor.close()
                if to_remove:
                    cursor = self._session.open_cursor(self._uri, None, "overwrite=true")
                    for k in to_remove:
                        cursor.set_key(k)
                        cursor.remove()
                    cursor.close()
                return len(to_remove)
            expired = [k for k, ts in self._tombstones.items() if now - ts > self._ttl]
            for k in expired:
                del self._tombstones[k]
            return len(expired)

    def to_dict(self) -> dict[str, float]:
        with self._lock:
            if self._persistent_redb:
                assert self._uri is not None and self._redb_client is not None
                return {
                    k: float(v) for k, v in self._redb_client.sync_kv_scan(self._uri)
                }
            if self._persistent_wt:
                result: dict[str, float] = {}
                cursor = self._session.open_cursor(self._uri, None, None)
                while cursor.next() == 0:
                    result[cursor.get_key()] = float(cursor.get_value())
                cursor.close()
                return result
            return dict(self._tombstones)

    def load(self, data: dict[str, float]) -> None:
        with self._lock:
            if self._persistent_redb:
                assert self._uri is not None and self._redb_client is not None
                for k, v in data.items():
                    self._redb_client.sync_kv_put(self._uri, k, str(v))
            elif self._persistent_wt:
                cursor = self._session.open_cursor(self._uri, None, "overwrite=true")
                for k, v in data.items():
                    cursor[k] = str(v)
                cursor.close()
            else:
                self._tombstones.update(data)


# ------------------------------------------------------------------
# Conflict resolution
# ------------------------------------------------------------------


def _lww(local_doc: Document, remote_doc: Document) -> Document:
    """Last-write-wins: compare _lastModified timestamps."""
    local_ts = (local_doc or {}).get("_lastModified", 0)
    remote_ts = (remote_doc or {}).get("_lastModified", 0)
    return remote_doc if remote_ts >= local_ts else local_doc


def _local_wins(local_doc: Document, _remote_doc: Document) -> Document:
    return local_doc


def _remote_wins(_local_doc: Document, remote_doc: Document) -> Document:
    return remote_doc


def _field_merge(
    local_doc: Document,
    remote_doc: Document,
    *,
    local_changed: set[str] | None = None,
    remote_changed: set[str] | None = None,
) -> Document:
    """
    Field-level merge strategy.
    - fields changed only locally: keep local
    - fields changed only remotely: keep remote
    - fields changed on both: fall back to per-field LWW using _lastModified
    """
    local_doc = dict(local_doc or {})
    remote_doc = dict(remote_doc or {})
    local_changed = set(local_changed or [])
    remote_changed = set(remote_changed or [])

    merged = dict(local_doc)
    all_fields = set(local_doc.keys()) | set(remote_doc.keys())
    local_ts = local_doc.get("_lastModified", 0) or 0
    remote_ts = remote_doc.get("_lastModified", 0) or 0

    for field in all_fields:
        if field == "_id":
            merged[field] = local_doc.get("_id", remote_doc.get("_id"))
            continue
        in_local = field in local_changed
        in_remote = field in remote_changed
        if in_local and not in_remote:
            merged[field] = local_doc.get(field)
        elif in_remote and not in_local:
            merged[field] = remote_doc.get(field)
        elif in_local and in_remote:
            merged[field] = remote_doc.get(field) if remote_ts >= local_ts else local_doc.get(field)
        else:
            if field in remote_doc:
                merged[field] = remote_doc[field]
    return merged


_RESOLVERS: dict[str, Callable[..., Document]] = {
    "lww": _lww,
    "local_wins": _local_wins,
    "remote_wins": _remote_wins,
    "field_merge": _field_merge,
}


def _diff_fields(local_doc: Document, remote_doc: Document) -> set[str]:
    """Compute which fields actually differ between local and remote documents."""
    changed: set[str] = set()
    all_keys = set(local_doc.keys()) | set(remote_doc.keys())
    for key in all_keys:
        if key == "_id":
            continue
        local_val = local_doc.get(key)
        remote_val = remote_doc.get(key)
        if local_val != remote_val:
            changed.add(key)
    return changed


# ------------------------------------------------------------------
# Variable substitution for sync rules
# ------------------------------------------------------------------


def _resolve_variables(query: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Deep-clone *query* and replace ``$$NAME`` string values with *context* entries.

    Built-in variables (injected by the caller):
        ``$$NOW``      -- ``time.time()`` (epoch float, matches ``_lastModified``)
        ``$$NODE_ID``  -- the configured ``node_id``

    User-defined variables are merged from ``sync_config["variables"]``.
    Strings that start with ``$$`` but have no matching context key are left as-is
    so that ``$$ROOT`` / ``$$CURRENT`` still work inside ``$expr``.
    """

    def _walk(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: _walk(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_walk(v) for v in obj]
        if isinstance(obj, str) and obj.startswith("$$"):
            var_name = obj[2:]
            if var_name in context:
                return context[var_name]
        return obj

    return cast(dict[str, Any], _walk(copy.deepcopy(query)))


# ------------------------------------------------------------------
# SyncManager
# ------------------------------------------------------------------


class SyncManager:
    """
    Manages bidirectional sync between a local MongoClient and a remote Atlas cluster.

    Usage:
        sync = SyncManager(local_client, "mongodb+srv://...@cluster.mongodb.net")
        sync.start()
        # ... app writes locally ...
        sync.status()
        sync.stop()
    """

    DEFAULT_CONFIG: dict[str, Any] = {
        "mode": "bidirectional",  # bidirectional | push_only | pull_only
        "interval_sec": 5,
        "batch_size": 100,
        "conflict_resolution": "lww",  # lww | local_wins | remote_wins | callable
        "collections": "*",  # "*" for all, ["db.coll", ...], or {"db.coll": {filter}}
        "use_change_stream_pull": True,
        "oplog_auto_compact": True,
        "max_backoff_sec": 300,
        "tombstone_ttl_sec": DEFAULT_TOMBSTONE_TTL_SEC,
        "node_id": "local",
        "crdt_fields": {},  # {"field": "counter" | "set"}
        "sync_rules": None,  # Per-document sync rules: MQL filter dict
        "variables": {},  # User-defined $$VAR substitutions for sync rules
        "push_concurrency": 4,
    }

    def __init__(
        self, local_client: Any, atlas_uri: str, sync_config: dict[str, Any] | None = None
    ) -> None:
        if not _PyMongoClient:  # type: ignore[truthy-function]
            raise ImportError("pymongo required for sync to MongoDB Atlas")

        self._local = local_client
        self._atlas_uri = atlas_uri
        self._remote: Any = _PyMongoClient(atlas_uri)

        cfg = dict(self.DEFAULT_CONFIG)
        if sync_config:
            cfg.update(sync_config)
        self._config = cfg

        resolver = cfg["conflict_resolution"]
        self._resolver_name: str = resolver if isinstance(resolver, str) else "custom"
        if callable(resolver):
            self._resolve: Callable[..., Document] = resolver
        else:
            self._resolve = _RESOLVERS.get(resolver, _lww)

        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()

        self._lock = threading.Lock()
        self._last_error: Exception | None = None
        self._pending_count = 0
        self._last_sync_ts: float | None = None

        self._pushed_count = 0
        self._pulled_count = 0
        self._conflict_count = 0
        self._error_count = 0
        self._consecutive_errors = 0
        self._state = "offline"
        self._last_cycle_pushed = 0
        self._last_cycle_pulled = 0

        self._ck_lock = threading.Lock()
        self._ck_uri = "table:__sync_checkpoint"
        self._ts_uri = "table:__tombstones"
        self._dlq_uri = "table:__sync_dlq"

        try:
            from smongo.storage.redb_engine import RedbClient as _RedbClient
        except ImportError:
            _RedbClient = None  # type: ignore[misc, assignment]

        inner = getattr(local_client, "client", None)
        self._redb_sync = bool(
            _RedbClient is not None and isinstance(inner, _RedbClient)
        )
        if self._redb_sync:
            self._rust = inner._rust_client
            self._ck_session = None
            self._tombstones = TombstoneRegistry(
                ttl_sec=int(cfg.get("tombstone_ttl_sec", DEFAULT_TOMBSTONE_TTL_SEC)),
                redb_client=self._rust,
                uri=self._ts_uri,
            )
        else:
            wt_conn = local_client.client.conn
            self._rust = None
            self._ck_session = wt_conn.open_session()
            self._ck_session.create(self._ck_uri, "key_format=S,value_format=S")
            self._ck_session.create(self._ts_uri, "key_format=S,value_format=S")
            self._ck_session.create(self._dlq_uri, "key_format=S,value_format=S")
            self._tombstones = TombstoneRegistry(
                ttl_sec=int(cfg.get("tombstone_ttl_sec", DEFAULT_TOMBSTONE_TTL_SEC)),
                session=self._ck_session,
                uri=self._ts_uri,
            )

        self._tracked: dict[str, tuple[Any, Any, Predicate | None]] = {}
        self._local_field_history: dict[tuple[str, Any], set[str]] = {}
        self._vector_clocks: dict[str, VectorClock] = {}
        self._node_id: str = cfg.get("node_id", "local")
        self._crdt_fields: dict[str, str] = cfg.get("crdt_fields", {})

        self._raw_sync_rules: dict[str, Any] | None = cfg.get("sync_rules")
        self._user_variables: dict[str, Any] = cfg.get("variables", {})
        self._active_sync_filter: Predicate | None = None
        self._recompile_sync_filter()

        self._ns_stats: dict[str, dict[str, Any]] = {}
        self._last_cycle_start: float = 0.0
        self._last_cycle_duration: float = 0.0

        self._raw_collection_filters: dict[str, dict[str, Any]] = {}
        if isinstance(cfg.get("collections"), dict):
            for ns_str, filt in cfg["collections"].items():
                if filt:
                    self._raw_collection_filters[ns_str] = filt

    # -- lifecycle -----------------------------------------------------

    def start(self) -> None:
        """Launch the background sync thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._discover_collections()
        with self._lock:
            self._state = "online"
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="sync")
        self._thread.start()
        log.info(
            "Sync started (mode=%s, interval=%ss)",
            self._config["mode"],
            self._config["interval_sec"],
        )

    def stop(self) -> None:
        """Signal the sync thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=15)
        with self._lock:
            self._state = "offline"
        log.info("Sync stopped")

    def pause(self) -> None:
        self._pause_event.clear()

    def resume(self) -> None:
        self._pause_event.set()

    def sync_now(self) -> None:
        """Run one sync cycle synchronously on the calling thread."""
        self._discover_collections()
        self._sync_cycle()

    def push(self) -> None:
        """Run a push cycle synchronously (local -> remote)."""
        self._discover_collections()
        self._push()
        with self._lock:
            self._last_sync_ts = time.time()

    def pull(self) -> None:
        """Run a pull cycle synchronously (remote -> local)."""
        self._discover_collections()
        self._pull()
        with self._lock:
            self._last_sync_ts = time.time()

    def status(self) -> dict[str, Any]:
        with self._lock:
            cycle_ops = self._last_cycle_pushed + self._last_cycle_pulled
            ops_per_sec = (
                cycle_ops / self._last_cycle_duration if self._last_cycle_duration > 0 else 0.0
            )
            return {
                "running": self._thread is not None and self._thread.is_alive(),
                "pending": self._pending_count,
                "last_sync": self._last_sync_ts,
                "last_error": str(self._last_error) if self._last_error else None,
                "mode": self._config["mode"],
                "state": self._state,
                "pushed": self._pushed_count,
                "pulled": self._pulled_count,
                "conflicts": self._conflict_count,
                "errors": self._error_count,
                "collections": dict(self._ns_stats),
                "dlq_depth": self._dlq_count(),
                "dlq_permanent_failures": self._dlq_count(permanent_only=True),
                "throughput_ops_sec": round(ops_per_sec, 2),
                "last_cycle_duration_sec": round(self._last_cycle_duration, 4),
            }

    # -- sync rule variable resolution ---------------------------------

    def _build_sync_context(self) -> dict[str, Any]:
        """Build the variable context for ``$$NAME`` substitution in sync rules."""
        ctx: dict[str, Any] = {
            "NOW": time.time(),
            "NODE_ID": self._node_id,
        }
        ctx.update(self._user_variables)
        return ctx

    def _recompile_sync_filter(self) -> None:
        """Recompile the global sync-rules predicate with fresh variable values."""
        if not self._raw_sync_rules:
            self._active_sync_filter = None
            return
        ctx = self._build_sync_context()
        resolved = _resolve_variables(self._raw_sync_rules, ctx)
        self._active_sync_filter = compile_query(resolved)

    def _recompile_collection_filters(self) -> None:
        """Recompile per-collection filters with fresh variable values."""
        if not self._raw_collection_filters:
            return
        ctx = self._build_sync_context()
        for ns_str, raw_filt in self._raw_collection_filters.items():
            if ns_str in self._tracked:
                local_coll, remote_coll, _ = self._tracked[ns_str]
                resolved = _resolve_variables(raw_filt, ctx)
                self._tracked[ns_str] = (local_coll, remote_coll, compile_query(resolved))

    def _doc_passes_sync_filter(self, doc: Document) -> bool:
        """Check whether *doc* passes the global sync-rules filter."""
        if not self._active_sync_filter:
            return True
        try:
            return bool(self._active_sync_filter(doc))
        except (KeyError, TypeError):
            return True

    # -- background loop -----------------------------------------------

    def _run_loop(self) -> None:
        interval: int = self._config["interval_sec"]
        max_backoff: int = self._config.get("max_backoff_sec", 300)

        while not self._stop_event.is_set():
            self._pause_event.wait()
            if self._stop_event.is_set():
                break
            try:
                with self._lock:
                    self._state = "syncing"
                self._sync_cycle()
                with self._lock:
                    self._consecutive_errors = 0
                    self._state = "online"
                sleep_time = interval
            except (
                PyMongoError,
                json.JSONDecodeError,
                KeyError,
                ValueError,
                TypeError,
                OSError,
                RuntimeError,
            ) as exc:
                with self._lock:
                    self._last_error = exc
                    self._error_count += 1
                    self._consecutive_errors += 1
                    self._state = "error"
                sleep_time = min(interval * (2**self._consecutive_errors), max_backoff)
                log.exception("Sync cycle error")

            self._stop_event.wait(timeout=sleep_time)

    def _sync_cycle(self) -> None:
        self._recompile_sync_filter()
        self._recompile_collection_filters()
        self._last_cycle_start = time.time()

        with self._lock:
            pushed_before = self._pushed_count
            pulled_before = self._pulled_count

        mode: str = self._config["mode"]

        if mode in ("bidirectional", "push_only"):
            self._push()

        if mode in ("bidirectional", "pull_only"):
            self._pull()

        with self._lock:
            self._last_sync_ts = time.time()
            self._last_cycle_pushed = self._pushed_count - pushed_before
            self._last_cycle_pulled = self._pulled_count - pulled_before
        self._last_cycle_duration = time.time() - self._last_cycle_start

    # -- push (local -> Atlas) -----------------------------------------

    def _push(self) -> None:
        self._sweep_dlq()

        batch_size: int = self._config["batch_size"]
        concurrency = int(self._config.get("push_concurrency", 4))

        namespaces = list(self._tracked.items())
        if not namespaces:
            return

        if concurrency <= 1 or len(namespaces) <= 1:
            for ns, (local_coll, remote_coll, ns_filter) in namespaces:
                self._push_namespace(ns, local_coll, remote_coll, ns_filter, batch_size)
        else:
            with ThreadPoolExecutor(max_workers=min(concurrency, len(namespaces))) as pool:
                futures = {
                    pool.submit(self._push_namespace, ns, lc, rc, nf, batch_size): ns
                    for ns, (lc, rc, nf) in namespaces
                }
                for fut in as_completed(futures):
                    ns_name = futures[fut]
                    try:
                        fut.result()
                    except Exception as exc:
                        log.warning("Push failed for namespace %s: %s", ns_name, exc)
                        with self._lock:
                            self._error_count += 1

        with self._lock:
            self._pending_count = 0

    def _push_namespace(
        self,
        ns: str,
        local_coll: Any,
        remote_coll: Any,
        ns_filter: Predicate | None,
        batch_size: int,
    ) -> None:
        """Push pending oplog entries for a single namespace to the remote."""
        checkpoint = self._get_checkpoint(f"push:{ns}")
        reader = local_coll.get_oplog_reader()
        entries = reader.read_from(checkpoint, skip_internal=True)

        if not entries:
            return

        ops: list[Any] = []
        op_entries: list[Document] = []
        last_key: str | None = None
        safe_key: str | None = None
        batch_start_key: str | None = None
        ns_pushed = 0

        for key, entry in entries:
            op = entry["op"]
            doc_id = entry["doc_id"]
            payload = entry["payload"]
            changed_fields = entry.get("changed_fields") or []

            has_filter = ns_filter or self._active_sync_filter
            if has_filter and payload and op in ("insert", "update", "delete"):
                filter_doc = payload
                if op == "update":
                    try:
                        filter_doc = local_coll.get_by_id(doc_id) or payload
                    except (KeyError, TypeError, RuntimeError):
                        filter_doc = payload
                if ns_filter:
                    try:
                        if not ns_filter(filter_doc):
                            last_key = key
                            if not ops:
                                safe_key = key
                            continue
                    except (KeyError, TypeError):
                        pass
                if not self._doc_passes_sync_filter(filter_doc):
                    last_key = key
                    if not ops:
                        safe_key = key
                    continue

            if op == "insert":
                doc = _to_pymongo(dict(payload))
                doc["_lastModified"] = entry["ts"]
                ops.append(InsertOne(doc))
                op_entries.append(entry)
            elif op == "update":
                update_spec = _to_pymongo(dict(payload))
                if "$set" not in update_spec:
                    update_spec["$set"] = {}
                update_spec["$set"]["_lastModified"] = entry["ts"]
                ops.append(UpdateOne({"_id": _to_pymongo(doc_id)}, update_spec, upsert=True))
                op_entries.append(entry)
                self._local_field_history[(ns, str(doc_id))] = set(changed_fields)
            elif op == "delete":
                ops.append(DeleteOne({"_id": _to_pymongo(doc_id)}))
                op_entries.append(entry)
            elif op == "index_create":
                try:
                    idx_keys = payload.get("keys", [])
                    idx_kwargs = {k: v for k, v in payload.items() if k != "keys"}
                    remote_coll.create_index(idx_keys, **idx_kwargs)
                except PyMongoError as exc:
                    log.warning("Failed to sync index create %s: %s", doc_id, exc)
            elif op == "index_drop":
                try:
                    remote_coll.drop_index(doc_id)
                except PyMongoError as exc:
                    log.warning("Failed to sync index drop %s: %s", doc_id, exc)

            last_key = key
            if batch_start_key is None and ops:
                batch_start_key = key

            if len(ops) >= batch_size:
                n_ok = self._flush_bulk(
                    remote_coll, ops, ns=ns, op_entries=op_entries
                )
                if n_ok > 0:
                    safe_key = key
                    with self._lock:
                        self._pushed_count += n_ok
                    ns_pushed += n_ok
                if n_ok < len(ops):
                    log.warning("Batch failed for %s; entries retained in oplog for retry", ns)
                ops = []
                op_entries = []
                batch_start_key = None

        if ops:
            n_ok = self._flush_bulk(
                remote_coll, ops, ns=ns, op_entries=op_entries
            )
            if n_ok > 0:
                safe_key = last_key
                with self._lock:
                    self._pushed_count += n_ok
                ns_pushed += n_ok
            if n_ok < len(ops):
                log.warning("Final batch failed for %s; entries retained in oplog for retry", ns)

        if safe_key:
            if self._config.get("oplog_auto_compact", True):
                self._atomic_checkpoint_and_compact(ns, safe_key, local_coll._oplog_w.oplog_uri)
            else:
                self._set_checkpoint(f"push:{ns}", safe_key)

        stats = self._ensure_ns_stats(ns)
        stats["last_push_ts"] = time.time()
        stats["last_push_count"] = ns_pushed

    def _atomic_checkpoint_and_compact(self, ns: str, safe_key: str, oplog_uri: str) -> None:
        """Atomically update the push checkpoint and truncate the oplog.

        Both operations run inside a single WiredTiger transaction so a crash
        between checkpoint write and oplog truncation cannot cause duplicate
        ops on restart.  With redb, the engine uses one write transaction for
        the same effect.
        """
        with self._ck_lock:
            if self._redb_sync:
                assert self._rust is not None
                self._rust.sync_atomic_checkpoint_truncate(
                    self._ck_uri,
                    f"push:{ns}",
                    safe_key,
                    oplog_uri,
                    safe_key,
                )
                return
            assert self._ck_session is not None
            self._ck_session.begin_transaction()
            try:
                cursor = self._ck_session.open_cursor(self._ck_uri, None, "overwrite=true")
                cursor[f"push:{ns}"] = safe_key
                cursor.close()

                cursor = self._ck_session.open_cursor(oplog_uri, None, "overwrite=true")
                to_remove: list[str] = []
                while cursor.next() == 0:
                    k: str = cursor.get_key()
                    if k >= safe_key:
                        break
                    to_remove.append(k)
                cursor.close()

                if to_remove:
                    cursor = self._ck_session.open_cursor(oplog_uri, None, "overwrite=true")
                    for k in to_remove:
                        cursor.set_key(k)
                        cursor.remove()
                    cursor.close()

                self._ck_session.commit_transaction()
            except Exception:
                try:
                    self._ck_session.rollback_transaction()
                except Exception:
                    pass
                raise

    def _flush_bulk(
        self,
        remote_coll: Any,
        ops: list[Any],
        *,
        ns: str = "",
        op_entries: list[Document] | None = None,
    ) -> int:
        """Flush a batch of operations to remote.

        Returns the number of successfully written ops (``len(ops)`` on full
        success, 0..n on partial failure, ``-1`` on total failure).
        Failed ops are enqueued into the dead-letter queue when *op_entries*
        is provided.
        """
        try:
            remote_coll.bulk_write(ops, ordered=False)
            return len(ops)
        except BulkWriteError as bwe:
            details = bwe.details or {}
            write_errors = details.get("writeErrors", [])
            n_failed = len(write_errors)
            n_ok = len(ops) - n_failed
            for err in write_errors:
                idx = err.get("index")
                log.warning(
                    "Sync bulk_write error: op_index=%s code=%s msg=%s",
                    idx,
                    err.get("code"),
                    err.get("errmsg", ""),
                )
                if op_entries and idx is not None and idx < len(op_entries):
                    self._dlq_enqueue(
                        ns, op_entries[idx], err.get("code"), err.get("errmsg", "")
                    )
            log.warning("Bulk write partial failure: %d/%d ops succeeded", n_ok, len(ops))
            return n_ok

    # -- dead-letter queue ---------------------------------------------

    def _dlq_enqueue(
        self, ns: str, entry: Document, error_code: Any, error_msg: str
    ) -> None:
        """Add a failed op to the DLQ for later retry."""
        backoff_base = float(self._config.get("dlq_backoff_base_sec", 30))
        now = time.time()
        key = f"{time.time_ns():020d}-{uuid.uuid4()}"
        value = json.dumps(
            {
                "ns": ns,
                "entry": entry,
                "error_code": error_code,
                "error_msg": error_msg,
                "retry_count": 0,
                "next_retry_ts": now + backoff_base,
                "first_failed_ts": now,
                "permanently_failed": False,
            },
            default=str,
        )
        with self._ck_lock:
            if self._redb_sync:
                assert self._rust is not None
                self._rust.sync_kv_put(self._dlq_uri, key, value)
                return
            assert self._ck_session is not None
            cursor = self._ck_session.open_cursor(self._dlq_uri, None, "overwrite=true")
            cursor[key] = value
            cursor.close()

    def _sweep_dlq(self) -> None:
        """Retry eligible DLQ entries.  Called at the start of each push cycle."""
        now = time.time()
        max_retries = int(self._config.get("max_dlq_retries", 5))
        backoff_base = float(self._config.get("dlq_backoff_base_sec", 30))
        backoff_max = float(self._config.get("max_backoff_sec", 300))

        rows: list[tuple[str, str]]
        with self._ck_lock:
            if self._redb_sync:
                assert self._rust is not None
                rows = self._rust.sync_kv_scan(self._dlq_uri)
            else:
                assert self._ck_session is not None
                rows = []
                cursor = self._ck_session.open_cursor(self._dlq_uri, None, None)
                while cursor.next() == 0:
                    rows.append((cursor.get_key(), cursor.get_value()))
                cursor.close()

        eligible: list[tuple[str, dict[str, Any]]] = []
        for k, v_raw in rows:
            v: dict[str, Any] = json.loads(v_raw)
            if v.get("permanently_failed"):
                continue
            if v["next_retry_ts"] <= now:
                eligible.append((k, v))

        if not eligible:
            return

        by_ns: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for k, v in eligible:
            by_ns.setdefault(v["ns"], []).append((k, v))

        for ns, items in by_ns.items():
            tup = self._tracked.get(ns)
            if not tup:
                continue
            _, remote_coll, _ = tup

            ops: list[Any] = []
            item_map: list[tuple[str, dict[str, Any]]] = []
            for k, v in items:
                op = self._entry_to_pymongo_op(v["entry"])
                if op is not None:
                    ops.append(op)
                    item_map.append((k, v))

            if not ops:
                continue

            try:
                remote_coll.bulk_write(ops, ordered=False)
                for k, _ in item_map:
                    self._dlq_remove(k)
            except BulkWriteError as bwe:
                failed_idxs = {
                    e.get("index") for e in (bwe.details or {}).get("writeErrors", [])
                }
                for i, (k, v) in enumerate(item_map):
                    if i in failed_idxs:
                        v["retry_count"] += 1
                        if v["retry_count"] >= max_retries:
                            v["permanently_failed"] = True
                            log.error(
                                "DLQ entry exhausted retries: ns=%s code=%s msg=%s",
                                ns,
                                v.get("error_code"),
                                v.get("error_msg"),
                            )
                        else:
                            delay = min(
                                backoff_base * (2 ** v["retry_count"]), backoff_max
                            )
                            v["next_retry_ts"] = time.time() + delay
                        self._dlq_update(k, v)
                    else:
                        self._dlq_remove(k)
            except (PyMongoError, OSError) as exc:
                log.warning("DLQ retry bulk_write failed for %s: %s", ns, exc)
                for k, v in item_map:
                    v["retry_count"] += 1
                    if v["retry_count"] >= max_retries:
                        v["permanently_failed"] = True
                    else:
                        delay = min(
                            backoff_base * (2 ** v["retry_count"]), backoff_max
                        )
                        v["next_retry_ts"] = time.time() + delay
                    self._dlq_update(k, v)

    def _entry_to_pymongo_op(self, entry: Document) -> Any:
        """Reconstruct a PyMongo write op from an oplog entry dict."""
        op = entry.get("op")
        doc_id = entry.get("doc_id")
        payload = entry.get("payload")
        if op == "insert" and payload:
            doc = _to_pymongo(dict(payload))
            doc["_lastModified"] = entry.get("ts", time.time())
            return InsertOne(doc)
        if op == "update" and payload:
            spec = _to_pymongo(dict(payload))
            if "$set" not in spec:
                spec["$set"] = {}
            spec["$set"]["_lastModified"] = entry.get("ts", time.time())
            return UpdateOne({"_id": _to_pymongo(doc_id)}, spec, upsert=True)
        if op == "delete":
            return DeleteOne({"_id": _to_pymongo(doc_id)})
        return None

    def _dlq_remove(self, key: str) -> None:
        with self._ck_lock:
            if self._redb_sync:
                assert self._rust is not None
                try:
                    self._rust.sync_kv_remove(self._dlq_uri, key)
                except Exception:
                    pass
                return
            assert self._ck_session is not None
            cursor = self._ck_session.open_cursor(self._dlq_uri, None, "overwrite=true")
            cursor.set_key(key)
            try:
                cursor.remove()
            except Exception:
                pass
            cursor.close()

    def _dlq_update(self, key: str, value: dict[str, Any]) -> None:
        with self._ck_lock:
            if self._redb_sync:
                assert self._rust is not None
                self._rust.sync_kv_put(
                    self._dlq_uri, key, json.dumps(value, default=str)
                )
                return
            assert self._ck_session is not None
            cursor = self._ck_session.open_cursor(self._dlq_uri, None, "overwrite=true")
            cursor[key] = json.dumps(value, default=str)
            cursor.close()

    def _dlq_count(self, *, permanent_only: bool = False) -> int:
        with self._ck_lock:
            if self._redb_sync:
                assert self._rust is not None
                rows = self._rust.sync_kv_scan(self._dlq_uri)
                if not permanent_only:
                    return len(rows)
                n = 0
                for _k, v_raw in rows:
                    v = json.loads(v_raw)
                    if v.get("permanently_failed"):
                        n += 1
                return n
            assert self._ck_session is not None
            cursor = self._ck_session.open_cursor(self._dlq_uri, None, None)
            n = 0
            while cursor.next() == 0:
                if permanent_only:
                    v = json.loads(cursor.get_value())
                    if v.get("permanently_failed"):
                        n += 1
                else:
                    n += 1
            cursor.close()
            return n

    # -- pull (Atlas -> local) -----------------------------------------

    def _pull(self) -> None:
        for ns, (local_coll, remote_coll, ns_filter) in self._tracked.items():
            with self._lock:
                pulled_before = self._pulled_count
            try:
                if self._config.get("use_change_stream_pull", True):
                    used_stream = self._pull_via_change_stream(
                        ns, local_coll, remote_coll, ns_filter
                    )
                    if used_stream:
                        self._pull_index_defs(ns, local_coll, remote_coll)
                        continue

                last_ts_str = self._get_checkpoint(f"pull_ts:{ns}")
                last_ts = float(last_ts_str) if last_ts_str else 0.0

                query: dict[str, Any] = {"_lastModified": {"$gt": last_ts}}
                try:
                    remote_docs: list[Document] = list(
                        remote_coll.find(query).sort("_lastModified", 1)
                    )
                except PyMongoError as exc:
                    log.warning("Pull query failed for %s: %s", ns, exc)
                    continue

                if not remote_docs:
                    self._pull_index_defs(ns, local_coll, remote_coll)
                    continue

                max_ts = last_ts

                for rdoc in remote_docs:
                    if ns_filter:
                        try:
                            if not ns_filter(rdoc):
                                continue
                        except (KeyError, TypeError):
                            pass
                    if not self._doc_passes_sync_filter(rdoc):
                        continue
                    remote_ts = rdoc.get("_lastModified", 0)
                    self._upsert_remote_doc(ns, local_coll, rdoc)
                    with self._lock:
                        self._pulled_count += 1

                    if remote_ts > max_ts:
                        max_ts = remote_ts

                self._set_checkpoint(f"pull_ts:{ns}", str(max_ts))
                self._pull_index_defs(ns, local_coll, remote_coll)
            finally:
                self._record_ns_pull(ns, pulled_before)

    _SYNC_META_FIELDS = frozenset({"_lastModified"})

    _VCLOCK_FIELD = "_vclock"

    def _upsert_remote_doc(
        self,
        ns: str,
        local_coll: Any,
        rdoc: Document,
        remote_changed: set[str] | None = None,
    ) -> None:
        rdoc = _from_pymongo(rdoc)
        doc_id = rdoc["_id"]
        local_doc = local_coll.get_by_id(doc_id)
        if local_doc:
            real_diff = (
                _diff_fields(local_doc, rdoc) - self._SYNC_META_FIELDS - {self._VCLOCK_FIELD}
            )
            if not real_diff:
                if rdoc.get("_lastModified") != local_doc.get("_lastModified"):
                    local_coll.update(
                        {"_id": doc_id},
                        {"$set": {"_lastModified": rdoc["_lastModified"]}},
                        multi=False,
                        _internal=True,
                    )
                return

            with self._lock:
                self._conflict_count += 1

            local_vc = VectorClock.from_dict(local_doc.get(self._VCLOCK_FIELD))
            remote_vc = VectorClock.from_dict(rdoc.get(self._VCLOCK_FIELD))

            if remote_vc.dominates(local_vc):
                resolved = rdoc
            elif local_vc.dominates(remote_vc):
                resolved = local_doc
            elif self._resolver_name == "field_merge":
                local_changed = self._local_field_history.get((ns, str(doc_id)), set())
                if remote_changed is None:
                    remote_changed = real_diff
                resolved = _field_merge(
                    local_doc,
                    rdoc,
                    local_changed=local_changed,
                    remote_changed=remote_changed,
                )
            else:
                resolved = self._resolve(local_doc, rdoc)

            merged_vc = VectorClock.from_dict(local_vc.to_dict())
            merged_vc.merge(remote_vc).tick(self._node_id)
            if resolved and resolved.get("_id") == doc_id:
                update_fields = {k: v for k, v in resolved.items() if k != "_id"}
                update_fields[self._VCLOCK_FIELD] = merged_vc.to_dict()
                local_coll.update(
                    {"_id": doc_id},
                    {"$set": update_fields},
                    multi=False,
                    _internal=True,
                )
                self._vector_clocks[str(doc_id)] = merged_vc
        else:
            vc = VectorClock.from_dict(rdoc.get(self._VCLOCK_FIELD))
            vc.tick(self._node_id)
            rdoc[self._VCLOCK_FIELD] = vc.to_dict()
            self._vector_clocks[str(doc_id)] = vc
            local_coll.insert_one(rdoc, _internal=True)

    def _pull_via_change_stream(
        self, ns: str, local_coll: Any, remote_coll: Any, ns_filter: Predicate | None = None
    ) -> bool:
        """
        Pull remote changes using MongoDB Change Streams with resume token checkpointing.
        Returns True when stream path is used; False when falling back to polling.
        """
        init_key = f"pull_cs_init:{ns}"
        if not self._get_checkpoint(init_key):
            page_key = f"pull_cs_page:{ns}"
            page_size = int(self._config.get("batch_size", 100))
            last_id_raw = self._get_checkpoint(page_key)
            last_id: Any = (
                json.loads(last_id_raw, object_hook=_ejson_object_hook) if last_id_raw else None
            )
            try:
                while True:
                    find_q: dict[str, Any] = (
                        {"_id": {"$gt": last_id}} if last_id is not None else {}
                    )
                    page = list(remote_coll.find(find_q).sort("_id", 1).limit(page_size))
                    for rdoc in page:
                        raw_id = rdoc.get("_id")
                        if ns_filter:
                            try:
                                if not ns_filter(rdoc):
                                    continue
                            except (KeyError, TypeError):
                                pass
                        if not self._doc_passes_sync_filter(rdoc):
                            continue
                        self._upsert_remote_doc(ns, local_coll, rdoc)
                        last_id = raw_id
                    if page and last_id is not None:
                        self._set_checkpoint(
                            page_key,
                            json.dumps(last_id, default=_ejson_default),
                        )
                    if len(page) < page_size:
                        break
                self._set_checkpoint(init_key, "1")
            except PyMongoError as exc:
                log.warning("Initial change-stream snapshot failed for %s: %s", ns, exc)
                return False

        token_key = f"pull_cs_token:{ns}"
        token_raw = self._get_checkpoint(token_key)
        resume_token: dict[str, Any] | None = json.loads(token_raw) if token_raw else None

        try:
            watch_kwargs: dict[str, Any] = {
                "full_document": "updateLookup",
                "max_await_time_ms": 200,
            }
            if resume_token:
                watch_kwargs["resume_after"] = resume_token
            with remote_coll.watch([], **watch_kwargs) as stream:
                max_events = int(self._config.get("batch_size", 100))
                processed = 0
                while processed < max_events:
                    change = stream.try_next()
                    if not change:
                        break
                    op = change.get("operationType")
                    doc_id = (change.get("documentKey") or {}).get("_id")
                    if op in ("insert", "replace", "update"):
                        full_doc = change.get("fullDocument")
                        if full_doc:
                            if ns_filter:
                                try:
                                    if not ns_filter(full_doc):
                                        processed += 1
                                        continue
                                except (KeyError, TypeError):
                                    pass
                            if not self._doc_passes_sync_filter(full_doc):
                                processed += 1
                                continue
                            rc: set[str] | None = None
                            if op == "update":
                                ud = change.get("updateDescription") or {}
                                updated = set(ud.get("updatedFields", {}).keys())
                                removed = set(ud.get("removedFields", []))
                                rc = updated | removed if (updated or removed) else None
                            self._upsert_remote_doc(ns, local_coll, full_doc, remote_changed=rc)
                            with self._lock:
                                self._pulled_count += 1
                    elif op == "delete" and doc_id is not None:
                        local_coll.delete(
                            {"_id": _from_pymongo(doc_id)}, multi=False, _internal=True
                        )

                    token = change.get("_id")
                    if token is not None:
                        self._set_checkpoint(token_key, json.dumps(token, default=str))
                    processed += 1
                if processed == 0:
                    initial_token = getattr(stream, "resume_token", None)
                    if initial_token is not None:
                        self._set_checkpoint(
                            token_key, json.dumps(initial_token, default=str)
                        )
            return True
        except PyMongoError as exc:
            log.warning("Change-stream pull unavailable for %s, falling back: %s", ns, exc)
            return False

    def _pull_index_defs(self, ns: str, local_coll: Any, remote_coll: Any) -> None:
        """Ensure local indexes match remote index definitions."""
        try:
            remote_indexes: list[dict[str, Any]] = list(remote_coll.list_indexes())
        except PyMongoError as exc:
            log.debug("Failed to list remote indexes for %s: %s", ns, exc)
            return

        local_names = {idx["name"] for idx in local_coll.list_indexes()}

        for ridx in remote_indexes:
            name = ridx.get("name", "")
            if name == "_id_" or name in local_names:
                continue
            keys = list(ridx.get("key", {}).items())
            if keys:
                try:
                    local_coll.create_index(
                        [(f, int(d)) for f, d in keys],
                        name=name,
                        unique=ridx.get("unique", False),
                        sparse=ridx.get("sparse", False),
                        _internal=True,
                    )
                except (DuplicateKeyError, ValueError, KeyError, RuntimeError) as exc:
                    log.warning("Failed to create pulled index %s: %s", name, exc)
                except Exception as exc:
                    log.warning("Failed to create pulled index %s: %s", name, exc)

    # -- collection discovery ------------------------------------------

    def _discover_collections(self) -> None:
        """
        Build the mapping of namespace -> (LocalCollection, remote collection, filter_fn).
        Supports collections config as:
          - "*" : all collections, no filter
          - ["db.coll", ...] : explicit list, no filter
          - {"db.coll": {query}, ...} : explicit list with per-collection MQL filters
        """
        cfg_colls = self._config["collections"]

        if cfg_colls == "*":
            return

        if isinstance(cfg_colls, dict):
            for ns_str, filt in cfg_colls.items():
                if ns_str in self._tracked:
                    continue
                parts = ns_str.split(".", 1)
                if len(parts) != 2:
                    continue
                db_name, coll_name = parts
                filter_fn = compile_query(filt) if filt else None
                self._register(db_name, coll_name, filter_fn=filter_fn)
            return

        for ns_str in cfg_colls:
            if ns_str in self._tracked:
                continue
            parts = ns_str.split(".", 1)
            if len(parts) != 2:
                continue
            db_name, coll_name = parts
            self._register(db_name, coll_name)

    def register_collection(
        self,
        db_name: str,
        coll_name: str,
        local_collection: Any,
        *,
        sync_filter: dict[str, Any] | None = None,
    ) -> None:
        """Register a collection for syncing with an optional MQL sync filter.

        *sync_filter* is an MQL query dict (supports ``$$`` variable substitution).
        When provided, only documents matching the filter are pushed/pulled.
        """
        ns = f"{db_name}.{coll_name}"
        if ns in self._tracked:
            return
        self._stamp_node_id(local_collection)
        remote_coll = self._remote[db_name][coll_name]
        filter_fn: Predicate | None = None
        if sync_filter:
            ctx = self._build_sync_context()
            resolved = _resolve_variables(sync_filter, ctx)
            filter_fn = compile_query(resolved)
            self._raw_collection_filters[ns] = sync_filter
        self._tracked[ns] = (local_collection, remote_coll, filter_fn)

    def _register(
        self, db_name: str, coll_name: str, *, filter_fn: Predicate | None = None
    ) -> None:
        local_db = self._local.client.get_db(db_name)
        local_coll = local_db.get_collection(coll_name)
        self._stamp_node_id(local_coll)
        remote_coll = self._remote[db_name][coll_name]
        ns = f"{db_name}.{coll_name}"
        self._tracked[ns] = (local_coll, remote_coll, filter_fn)

    def _stamp_node_id(self, local_coll: Any) -> None:
        """Set the node_id on a collection's oplog writer for provenance tracking."""
        try:
            if hasattr(local_coll, "_oplog_w"):
                local_coll._oplog_w.node_id = self._node_id
        except (AttributeError, TypeError):
            pass

    # -- per-namespace stats -------------------------------------------

    def _ensure_ns_stats(self, ns: str) -> dict[str, Any]:
        if ns not in self._ns_stats:
            self._ns_stats[ns] = {
                "last_push_ts": None,
                "last_pull_ts": None,
                "last_push_count": 0,
                "last_pull_count": 0,
            }
        return self._ns_stats[ns]

    def _record_ns_pull(self, ns: str, pulled_before: int) -> None:
        with self._lock:
            ns_pulled = self._pulled_count - pulled_before
        stats = self._ensure_ns_stats(ns)
        stats["last_pull_ts"] = time.time()
        stats["last_pull_count"] = ns_pulled

    # -- checkpoint persistence ----------------------------------------

    def _get_checkpoint(self, key: str) -> str | None:
        with self._ck_lock:
            if self._redb_sync:
                assert self._rust is not None
                return self._rust.sync_kv_get(self._ck_uri, key)
            assert self._ck_session is not None
            cursor = self._ck_session.open_cursor(self._ck_uri, None, None)
            cursor.set_key(key)
            val: str | None = None
            if cursor.search() == 0:
                val = cursor.get_value()
            cursor.close()
            return val

    def _set_checkpoint(self, key: str, value: str) -> None:
        with self._ck_lock:
            if self._redb_sync:
                assert self._rust is not None
                self._rust.sync_kv_put(self._ck_uri, key, value)
                return
            assert self._ck_session is not None
            cursor = self._ck_session.open_cursor(self._ck_uri, None, "overwrite=true")
            cursor[key] = value
            cursor.close()
