"""Metrics mixin: counters, conflict log, per-namespace stats, and index hashing."""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any

from .conflict import VectorClock

if TYPE_CHECKING:
    pass

log = logging.getLogger("smongo.sync")


class _MetricsMixin:
    """Mixin providing counter persistence, conflict auditing, and per-NS stats."""

    # -- conflict log --------------------------------------------------

    def _log_conflict(
        self,
        *,
        ns: str,
        doc_id: Any,
        strategy: str,
        local_vc: VectorClock,
        remote_vc: VectorClock,
        diff_fields: set[str],
    ) -> None:
        """Persist a conflict event for debugging and audit."""
        entry = {
            "ts": time.time(),
            "ns": ns,
            "doc_id": str(doc_id),
            "strategy": strategy,
            "node_id": self._node_id,
            "local_vc": local_vc.to_dict(),
            "remote_vc": remote_vc.to_dict(),
            "diff_fields": sorted(diff_fields),
        }
        with self._lock:
            self._conflict_log.append(entry)
        try:
            key = f"{time.time_ns():020d}-{uuid.uuid4()}"
            with self._ck_lock:
                self._rust.sync_kv_put(self._conflict_log_uri, key, json.dumps(entry, default=str))
        except (RuntimeError, OSError, ValueError) as exc:
            log.warning("Failed to persist conflict log entry: %s", exc)
        log.info(
            "Conflict resolved: ns=%s doc_id=%s strategy=%s fields=%s",
            ns,
            doc_id,
            strategy,
            sorted(diff_fields),
        )

    def get_conflict_log(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return the most recent conflict events (from persistent storage if available)."""
        try:
            with self._ck_lock:
                rows = self._rust.sync_kv_scan(self._conflict_log_uri)
            entries = []
            for _k, v in rows:
                try:
                    entries.append(json.loads(v))
                except (json.JSONDecodeError, ValueError):
                    continue
            return entries[-limit:]
        except (RuntimeError, OSError) as exc:
            log.warning("Failed to read persistent conflict log: %s", exc)
            with self._lock:
                return list(self._conflict_log[-limit:])

    def _rotate_conflict_log(self) -> None:
        """Trim the conflict log to ``max_conflict_log_entries``."""
        max_entries = int(self._config.get("max_conflict_log_entries", 10_000))
        with self._lock:
            if len(self._conflict_log) > max_entries:
                self._conflict_log = self._conflict_log[-max_entries:]
        try:
            with self._ck_lock:
                rows = self._rust.sync_kv_scan(self._conflict_log_uri)
            if len(rows) > max_entries:
                to_remove = rows[: len(rows) - max_entries]
                with self._ck_lock:
                    for k, _ in to_remove:
                        self._rust.sync_kv_remove(self._conflict_log_uri, k)
        except (RuntimeError, OSError) as exc:
            log.warning("Failed to rotate persistent conflict log: %s", exc)

    # -- counter persistence -------------------------------------------

    def _persist_counters(self) -> None:
        """Persist sync counters to durable storage for crash recovery."""
        try:
            counters = {
                "pushed": self._pushed_count,
                "pulled": self._pulled_count,
                "conflicts": self._conflict_count,
                "errors": self._error_count,
                "cycles": self._cycle_count,
            }
            with self._ck_lock:
                self._rust.sync_kv_put(
                    self._persistent_counters_uri,
                    "counters",
                    json.dumps(counters),
                )
        except (RuntimeError, OSError, ValueError) as exc:
            log.warning("Failed to persist sync counters: %s", exc)

    def _load_counters(self) -> None:
        """Restore sync counters from persistent storage."""
        try:
            with self._ck_lock:
                raw = self._rust.sync_kv_get(self._persistent_counters_uri, "counters")
            if raw:
                counters = json.loads(raw)
                self._pushed_count = counters.get("pushed", 0)
                self._pulled_count = counters.get("pulled", 0)
                self._conflict_count = counters.get("conflicts", 0)
                self._error_count = counters.get("errors", 0)
                self._cycle_count = counters.get("cycles", 0)
        except (RuntimeError, OSError, json.JSONDecodeError, ValueError) as exc:
            log.warning("Failed to load persisted counters: %s", exc)

    # -- index hash ----------------------------------------------------

    @staticmethod
    def _compute_index_hash(indexes: list[dict[str, Any]]) -> str:
        """Compute a deterministic hash of index definitions for change detection."""
        import hashlib

        normalized = sorted(
            (idx.get("name", ""), json.dumps(idx, sort_keys=True, default=str))
            for idx in indexes
            if idx.get("name", "") != "_id_"
        )
        return hashlib.sha256(str(normalized).encode()).hexdigest()[:16]

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
