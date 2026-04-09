"""Tombstone registry for tracking deleted document IDs with TTL-based expiry."""

from __future__ import annotations

import threading
import time
from typing import Any

DEFAULT_TOMBSTONE_TTL_SEC = 7 * 24 * 3600  # 7 days


class TombstoneRegistry:
    """Track deleted document IDs with timestamps for tombstone expiry.

    When *redb_client* and *uri* are provided (sync with
    :class:`~smongo.storage.redb_engine.RedbClient`), tombstones persist via the
    Rust redb KV helpers. Otherwise an in-memory dict is used (unit tests).
    """

    def __init__(
        self,
        ttl_sec: int = DEFAULT_TOMBSTONE_TTL_SEC,
        *,
        redb_client: Any = None,
        uri: str | None = None,
    ) -> None:
        self._ttl = ttl_sec
        self._lock = threading.Lock()
        self._uri = uri
        self._redb_client = redb_client
        self._persistent_redb = redb_client is not None and uri is not None
        self._persistent = self._persistent_redb
        if not self._persistent:
            self._tombstones: dict[str, float] = {}

    def mark_deleted(self, doc_id: Any) -> None:
        key = str(doc_id)
        with self._lock:
            if self._persistent_redb:
                assert self._uri is not None and self._redb_client is not None
                self._redb_client.sync_kv_put(self._uri, key, str(time.time()))
            else:
                self._tombstones[key] = time.time()

    def is_tombstoned(self, doc_id: Any) -> bool:
        key = str(doc_id)
        with self._lock:
            if self._persistent_redb:
                assert self._uri is not None and self._redb_client is not None
                return self._redb_client.sync_kv_get(self._uri, key) is not None
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
            expired = [k for k, ts in self._tombstones.items() if now - ts > self._ttl]
            for k in expired:
                del self._tombstones[k]
            return len(expired)

    def to_dict(self) -> dict[str, float]:
        with self._lock:
            if self._persistent_redb:
                assert self._uri is not None and self._redb_client is not None
                return {k: float(v) for k, v in self._redb_client.sync_kv_scan(self._uri)}
            return dict(self._tombstones)

    def load(self, data: dict[str, float]) -> None:
        with self._lock:
            if self._persistent_redb:
                assert self._uri is not None and self._redb_client is not None
                for k, v in data.items():
                    self._redb_client.sync_kv_put(self._uri, k, str(v))
            else:
                self._tombstones.update(data)
