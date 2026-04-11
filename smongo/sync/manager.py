"""SyncManager -- orchestrator for bidirectional sync between local redb and MongoDB Atlas."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from .._types import Document, Predicate
from ..query import compile_query
from .conflict import _RESOLVERS, VectorClock, _lww, _resolve_variables
from .dlq import _DLQMixin
from .metrics import _MetricsMixin
from .pull import _PullMixin
from .push import _PushMixin
from .tombstone import DEFAULT_TOMBSTONE_TTL_SEC, TombstoneRegistry

try:
    from pymongo import MongoClient as _PyMongoClient
    from pymongo.errors import PyMongoError
except ImportError:
    _PyMongoClient = None  # type: ignore[misc, assignment]

    class PyMongoError(Exception):  # type: ignore[no-redef]
        pass


log = logging.getLogger("smongo.sync")


class SyncManager(_PushMixin, _PullMixin, _MetricsMixin, _DLQMixin):
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
        "mode": "bidirectional",
        "interval_sec": 5,
        "batch_size": 100,
        "conflict_resolution": "lww",
        "collections": "*",
        "use_change_stream_pull": True,
        "oplog_auto_compact": True,
        "max_backoff_sec": 300,
        "tombstone_ttl_sec": DEFAULT_TOMBSTONE_TTL_SEC,
        "node_id": "local",
        "crdt_fields": {},
        "sync_rules": None,
        "variables": {},
        "push_concurrency": 4,
        "delete_detection_interval_cycles": 5,
        "delete_detection_batch_size": 1000,
        "delete_detection_enabled": True,
        "max_conflict_log_entries": 10_000,
        "validate_on_pull": False,
        "schema_rejection_strategy": "rollback",
        "overflow_strategy": "server_wins",
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
        self._schema_rejection_count = 0
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
        if _RedbClient is None or not isinstance(inner, _RedbClient):
            raise ValueError(
                "SyncManager requires MongoClient in embedded mode (local://) backed by "
                "RedbClient; use local://<path> with sync to Atlas."
            )
        self._rust = inner._rust_client
        self._tombstones = TombstoneRegistry(
            ttl_sec=int(cfg.get("tombstone_ttl_sec", DEFAULT_TOMBSTONE_TTL_SEC)),
            redb_client=self._rust,
            uri=self._ts_uri,
        )

        self._tracked: dict[str, tuple[Any, Any, Predicate | None]] = {}
        self._local_field_history: dict[tuple[str, Any], set[str]] = {}
        self._cumulative_field_history: dict[tuple[str, str], set[str]] = {}
        self._local_update_specs: dict[tuple[str, str], Document] = {}
        self._vector_clocks: dict[str, VectorClock] = {}
        self._node_id: str = cfg.get("node_id", "local")
        self._crdt_fields: dict[str, str] = cfg.get("crdt_fields", {})
        self._conflict_log_uri = "table:__sync_conflict_log"
        self._conflict_log: list[dict[str, Any]] = []
        self._cycle_count: int = 0
        self._index_hash_cache: dict[str, str] = {}
        self._persistent_counters_uri = "table:__sync_counters"

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
        self._load_counters()
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
            conflict_log_count = len(self._conflict_log)
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
                "conflict_log_count": conflict_log_count,
                "errors": self._error_count,
                "cycles": self._cycle_count,
                "collections": dict(self._ns_stats),
                "dlq_depth": self._dlq_count(),
                "dlq_permanent_failures": self._dlq_count(permanent_only=True),
                "schema_rejections": self._schema_rejection_count,
                "throughput_ops_sec": round(ops_per_sec, 2),
                "last_cycle_duration_sec": round(self._last_cycle_duration, 4),
                "tombstone_count": len(self._tombstones.to_dict()),
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
        except (KeyError, TypeError) as exc:
            log.warning("Sync filter raised %s for doc %s; excluding document", exc, doc.get("_id"))
            return False

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
        self._cycle_count += 1

        with self._lock:
            pushed_before = self._pushed_count
            pulled_before = self._pulled_count

        mode: str = self._config["mode"]

        if mode in ("bidirectional", "push_only"):
            self._push()

        if mode in ("bidirectional", "pull_only"):
            self._pull()

        self._tombstones.expire()
        self._rotate_conflict_log()

        with self._lock:
            self._last_sync_ts = time.time()
            self._last_cycle_pushed = self._pushed_count - pushed_before
            self._last_cycle_pulled = self._pulled_count - pulled_before
        self._last_cycle_duration = time.time() - self._last_cycle_start
        self._persist_counters()

    # -- collection discovery ------------------------------------------

    def _discover_collections(self) -> None:
        """
        Build the mapping of namespace -> (local collection handle, remote collection, filter_fn).
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

    # -- full resync ---------------------------------------------------

    def force_full_resync(self, db_name: str, coll_name: str) -> dict[str, Any]:
        """Manually trigger a full resync for a collection (server always wins).

        Resets all sync checkpoints for the namespace, drops local data,
        and re-pulls everything from the remote.
        """
        ns = f"{db_name}.{coll_name}"
        tup = self._tracked.get(ns)
        if not tup:
            self._discover_collections()
            tup = self._tracked.get(ns)
        if not tup:
            raise ValueError(f"Namespace {ns} is not registered for sync")
        local_coll, remote_coll, _ = tup
        return self._force_full_resync(ns, local_coll, remote_coll, winner="server")

    def _force_full_resync(
        self,
        ns: str,
        local_coll: Any,
        remote_coll: Any,
        winner: str = "server",
    ) -> dict[str, Any]:
        """Reset sync state for *ns* and re-pull from the winning side."""
        log.warning("Full resync triggered for %s (winner=%s)", ns, winner)

        for suffix in (
            f"push:{ns}",
            f"pull_ts:{ns}",
            f"pull_cs_init:{ns}",
            f"pull_cs_page:{ns}",
            f"pull_cs_token:{ns}",
        ):
            with self._ck_lock:
                try:
                    self._rust.sync_kv_remove(self._ck_uri, suffix)
                except Exception as exc:
                    log.debug("Failed to remove checkpoint %s: %s", suffix, exc)

        docs_synced = 0
        if winner == "server":
            try:
                all_local = list(local_coll.find({}, projection={"_id": 1}))
                for doc in all_local:
                    local_coll.delete({"_id": doc["_id"]}, multi=False, _internal=True)
            except Exception as exc:
                log.warning("Full resync: error clearing local data for %s: %s", ns, exc)

            try:
                from smongo._smongo_core import from_pymongo as _from_pymongo

                for rdoc in remote_coll.find({}):
                    rdoc = _from_pymongo(rdoc)
                    local_coll.insert_one(rdoc, _internal=True)
                    docs_synced += 1
            except Exception as exc:
                log.warning("Full resync: error pulling remote data for %s: %s", ns, exc)

        with self._lock:
            self._pulled_count += docs_synced

        return {"ns": ns, "winner": winner, "docs_synced": docs_synced}

    # -- checkpoint persistence ----------------------------------------

    def _get_checkpoint(self, key: str) -> str | None:
        with self._ck_lock:
            return self._rust.sync_kv_get(self._ck_uri, key)

    def _set_checkpoint(self, key: str, value: str) -> None:
        with self._ck_lock:
            self._rust.sync_kv_put(self._ck_uri, key, value)
