"""
Sync Layer -- bidirectional sync between the local WiredTiger engine and MongoDB Atlas.

Architecture:
    - Background thread tails the local oplog and pushes mutations to Atlas
    - Optionally pulls remote changes via change streams or timestamp polling
    - Conflict resolution strategies: LWW, local-wins, remote-wins, or custom callable
    - Checkpoint stored in a dedicated WiredTiger table so sync survives restarts
    - Exponential backoff on consecutive errors, per-collection selective filters
"""

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

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

from ._types import Document, Predicate
from .index import DuplicateKeyError
from .objectid import ObjectId as EngineObjectId
from .query import compile_query

log = logging.getLogger("smongo.sync")


# ------------------------------------------------------------------
# Type bridge: engine ObjectId <-> bson ObjectId for PyMongo
# ------------------------------------------------------------------


def _to_pymongo(value: Any) -> Any:
    """Recursively convert engine ObjectId to bson.ObjectId for PyMongo."""
    if isinstance(value, EngineObjectId):
        return BsonObjectId(str(value)) if BsonObjectId is not None else str(value)
    if isinstance(value, dict):
        return {k: _to_pymongo(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_pymongo(v) for v in value]
    return value


def _from_pymongo(value: Any) -> Any:
    """Recursively convert bson.ObjectId to engine ObjectId after PyMongo read."""
    if BsonObjectId is not None and isinstance(value, BsonObjectId):
        return EngineObjectId(str(value))
    if isinstance(value, dict):
        return {k: _from_pymongo(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_from_pymongo(v) for v in value]
    return value


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
        """True if every entry in *other* is <= our entry."""
        if not other._clock:
            return True
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
    """Track deleted document IDs with timestamps for tombstone expiry."""

    def __init__(self, ttl_sec: int = DEFAULT_TOMBSTONE_TTL_SEC) -> None:
        self._tombstones: dict[str, float] = {}
        self._ttl = ttl_sec
        self._lock = threading.Lock()

    def mark_deleted(self, doc_id: Any) -> None:
        with self._lock:
            self._tombstones[str(doc_id)] = time.time()

    def is_tombstoned(self, doc_id: Any) -> bool:
        with self._lock:
            return str(doc_id) in self._tombstones

    def expire(self) -> int:
        now = time.time()
        with self._lock:
            expired = [k for k, ts in self._tombstones.items() if now - ts > self._ttl]
            for k in expired:
                del self._tombstones[k]
            return len(expired)

    def to_dict(self) -> dict[str, float]:
        with self._lock:
            return dict(self._tombstones)

    def load(self, data: dict[str, float]) -> None:
        with self._lock:
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

        wt_conn = local_client.client.conn
        self._ck_session: Any = wt_conn.open_session()
        self._ck_uri = "table:__sync_checkpoint"
        self._ck_session.create(self._ck_uri, "key_format=S,value_format=S")

        self._tracked: dict[str, tuple[Any, Any, Predicate | None]] = {}
        self._local_field_history: dict[tuple[str, Any], set[str]] = {}
        self._tombstones = TombstoneRegistry(
            ttl_sec=int(cfg.get("tombstone_ttl_sec", DEFAULT_TOMBSTONE_TTL_SEC))
        )
        self._vector_clocks: dict[str, VectorClock] = {}
        self._node_id = cfg.get("node_id", "local")
        self._crdt_fields: dict[str, str] = cfg.get("crdt_fields", {})

        sync_rules = cfg.get("sync_rules")
        self._sync_filter: Predicate | None = compile_query(sync_rules) if sync_rules else None

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
            }

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
        mode: str = self._config["mode"]

        if mode in ("bidirectional", "push_only"):
            self._push()

        if mode in ("bidirectional", "pull_only"):
            self._pull()

        with self._lock:
            self._last_sync_ts = time.time()

    # -- push (local -> Atlas) -----------------------------------------

    def _push(self) -> None:
        batch_size: int = self._config["batch_size"]

        for ns, (local_coll, remote_coll, ns_filter) in self._tracked.items():
            checkpoint = self._get_checkpoint(f"push:{ns}")
            reader = local_coll.get_oplog_reader()
            entries = reader.read_from(checkpoint, skip_internal=True)

            if not entries:
                continue

            ops: list[Any] = []
            last_key: str | None = None
            safe_key: str | None = None
            batch_start_key: str | None = None

            for key, entry in entries:
                op = entry["op"]
                doc_id = entry["doc_id"]
                payload = entry["payload"]
                changed_fields = entry.get("changed_fields") or []

                if ns_filter and payload:
                    try:
                        if not ns_filter(payload):
                            last_key = key
                            if not ops:
                                safe_key = key
                            continue
                    except (KeyError, TypeError):
                        pass

                if op == "insert":
                    doc = _to_pymongo(dict(payload))
                    doc["_lastModified"] = entry["ts"]
                    ops.append(InsertOne(doc))
                elif op == "update":
                    update_spec = _to_pymongo(dict(payload))
                    if "$set" not in update_spec:
                        update_spec["$set"] = {}
                    update_spec["$set"]["_lastModified"] = entry["ts"]
                    ops.append(UpdateOne({"_id": _to_pymongo(doc_id)}, update_spec, upsert=True))
                    self._local_field_history[(ns, str(doc_id))] = set(changed_fields)
                elif op == "delete":
                    ops.append(DeleteOne({"_id": _to_pymongo(doc_id)}))
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
                    if self._flush_bulk(remote_coll, ops):
                        safe_key = key
                        with self._lock:
                            self._pushed_count += len(ops)
                    else:
                        log.warning("Batch failed for %s; entries retained in oplog for retry", ns)
                    ops = []
                    batch_start_key = None

            if ops:
                if self._flush_bulk(remote_coll, ops):
                    safe_key = last_key
                    with self._lock:
                        self._pushed_count += len(ops)
                else:
                    log.warning(
                        "Final batch failed for %s; entries retained in oplog for retry", ns
                    )

            if last_key:
                self._set_checkpoint(f"push:{ns}", last_key)

            if safe_key and self._config.get("oplog_auto_compact", True):
                try:
                    local_coll._oplog_w.truncate_before(safe_key)
                except (RuntimeError, OSError, KeyError, ValueError) as exc:
                    log.debug("Oplog auto-compact failed for %s: %s", ns, exc)

            with self._lock:
                self._pending_count = 0

    def _flush_bulk(self, remote_coll: Any, ops: list[Any]) -> bool:
        """Flush a batch of operations to remote. Returns True on full success."""
        try:
            remote_coll.bulk_write(ops, ordered=False)
            return True
        except BulkWriteError as bwe:
            log.warning("Bulk write partial failure: %s", bwe.details)
            return False

    # -- pull (Atlas -> local) -----------------------------------------

    def _pull(self) -> None:
        for ns, (local_coll, remote_coll, ns_filter) in self._tracked.items():
            if self._config.get("use_change_stream_pull", True):
                used_stream = self._pull_via_change_stream(ns, local_coll, remote_coll, ns_filter)
                if used_stream:
                    self._pull_index_defs(ns, local_coll, remote_coll)
                    continue

            last_ts_str = self._get_checkpoint(f"pull_ts:{ns}")
            last_ts = float(last_ts_str) if last_ts_str else 0.0

            query: dict[str, Any] = {"_lastModified": {"$gt": last_ts}}
            try:
                remote_docs: list[Document] = list(remote_coll.find(query).sort("_lastModified", 1))
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
                remote_ts = rdoc.get("_lastModified", 0)
                self._upsert_remote_doc(ns, local_coll, rdoc)
                with self._lock:
                    self._pulled_count += 1

                if remote_ts > max_ts:
                    max_ts = remote_ts

            self._set_checkpoint(f"pull_ts:{ns}", str(max_ts))
            self._pull_index_defs(ns, local_coll, remote_coll)

    _SYNC_META_FIELDS = frozenset({"_lastModified"})

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
            real_diff = _diff_fields(local_doc, rdoc) - self._SYNC_META_FIELDS
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
            if self._resolver_name == "field_merge":
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
            if resolved and resolved.get("_id") == doc_id:
                local_coll.update(
                    {"_id": doc_id},
                    {"$set": {k: v for k, v in resolved.items() if k != "_id"}},
                    multi=False,
                    _internal=True,
                )
        else:
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
            try:
                for rdoc in remote_coll.find({}):
                    if ns_filter:
                        try:
                            if not ns_filter(rdoc):
                                continue
                        except (KeyError, TypeError):
                            pass
                    self._upsert_remote_doc(ns, local_coll, rdoc)
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

    def register_collection(self, db_name: str, coll_name: str, local_collection: Any) -> None:
        """
        Explicitly register a collection for syncing.
        Called by the user or automatically when collections="*".
        """
        ns = f"{db_name}.{coll_name}"
        if ns in self._tracked:
            return
        remote_coll = self._remote[db_name][coll_name]
        self._tracked[ns] = (local_collection, remote_coll, None)

    def _register(
        self, db_name: str, coll_name: str, *, filter_fn: Predicate | None = None
    ) -> None:
        local_db = self._local.client.get_db(db_name)
        local_coll = local_db.get_collection(coll_name)
        remote_coll = self._remote[db_name][coll_name]
        ns = f"{db_name}.{coll_name}"
        self._tracked[ns] = (local_coll, remote_coll, filter_fn)

    # -- checkpoint persistence ----------------------------------------

    def _get_checkpoint(self, key: str) -> str | None:
        cursor = self._ck_session.open_cursor(self._ck_uri, None, None)
        cursor.set_key(key)
        val: str | None = None
        if cursor.search() == 0:
            val = cursor.get_value()
        cursor.close()
        return val

    def _set_checkpoint(self, key: str, value: str) -> None:
        cursor = self._ck_session.open_cursor(self._ck_uri, None, "overwrite=true")
        cursor[key] = value
        cursor.close()
