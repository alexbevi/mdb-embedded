"""Dead-letter queue mixin: retry failed sync operations with exponential backoff."""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from .._types import Document

try:
    from pymongo import DeleteOne, InsertOne, UpdateOne
    from pymongo.errors import BulkWriteError, PyMongoError
except ImportError:
    UpdateOne = DeleteOne = InsertOne = None  # type: ignore[misc, assignment]

    class BulkWriteError(Exception):  # type: ignore[no-redef]
        pass

    class PyMongoError(Exception):  # type: ignore[no-redef]
        pass


from smongo._smongo_core import to_pymongo as _to_pymongo

log = logging.getLogger("smongo.sync")


class _DLQMixin:
    """Mixin providing dead-letter queue operations for the SyncManager."""

    def _dlq_enqueue(
        self,
        ns: str,
        entry: Document,
        error_code: Any,
        error_msg: str,
        *,
        permanently_failed: bool = False,
    ) -> None:
        """Add a failed op to the DLQ for later retry (or permanent quarantine)."""
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
                "permanently_failed": permanently_failed,
            },
            default=str,
        )
        with self._ck_lock:
            self._rust.sync_kv_put(self._dlq_uri, key, value)

    def _sweep_dlq(self) -> None:
        """Retry eligible DLQ entries.  Called at the start of each push cycle."""
        now = time.time()
        max_retries = int(self._config.get("max_dlq_retries", 5))
        backoff_base = float(self._config.get("dlq_backoff_base_sec", 30))
        backoff_max = float(self._config.get("max_backoff_sec", 300))

        rows: list[tuple[str, str]]
        with self._ck_lock:
            rows = self._rust.sync_kv_scan(self._dlq_uri)

        eligible: list[tuple[str, dict[str, Any]]] = []
        for k, v_raw in rows:
            try:
                v: dict[str, Any] = json.loads(v_raw)
            except (json.JSONDecodeError, ValueError):
                log.warning("Corrupt DLQ entry %s; skipping", k)
                continue
            if v.get("permanently_failed"):
                continue
            next_ts = v.get("next_retry_ts")
            if next_ts is None:
                log.warning("DLQ entry %s missing next_retry_ts; skipping", k)
                continue
            if next_ts <= now:
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
                failed_idxs = {e.get("index") for e in (bwe.details or {}).get("writeErrors", [])}
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
                            delay = min(backoff_base * (2 ** v["retry_count"]), backoff_max)
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
                        delay = min(backoff_base * (2 ** v["retry_count"]), backoff_max)
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
            try:
                self._rust.sync_kv_remove(self._dlq_uri, key)
            except Exception as exc:
                log.debug("Failed to remove DLQ entry %s: %s", key, exc)

    def _dlq_update(self, key: str, value: dict[str, Any]) -> None:
        with self._ck_lock:
            self._rust.sync_kv_put(self._dlq_uri, key, json.dumps(value, default=str))

    def _dlq_count(self, *, permanent_only: bool = False) -> int:
        with self._ck_lock:
            rows = self._rust.sync_kv_scan(self._dlq_uri)
            if not permanent_only:
                return len(rows)
            n = 0
            for k, v_raw in rows:
                try:
                    v = json.loads(v_raw)
                except (json.JSONDecodeError, ValueError):
                    log.warning("Corrupt DLQ entry %s in count; skipping", k)
                    continue
                if v.get("permanently_failed"):
                    n += 1
            return n
