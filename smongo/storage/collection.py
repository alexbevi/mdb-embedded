"""TTL background helper and shared constants for embedded storage.

Document CRUD and indexes live in :mod:`smongo.storage.redb_engine` (``RedbCollection``)
and the Rust ``RedbLocalCollection`` binding (``smongo-engine`` + redb).
"""

from __future__ import annotations

import threading
from typing import Any

from .helpers import log

_TTL_DELETE_BATCH_SIZE = 500


class TTLReaper:
    """Daemon thread that periodically calls ``reap_expired()`` on engine-backed collections."""

    def __init__(
        self,
        collection: Any,
        interval_sec: int = 60,
        batch_size: int = _TTL_DELETE_BATCH_SIZE,
    ) -> None:
        self._collection = collection
        self._interval = interval_sec
        self._batch_size = batch_size
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def maybe_start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        if not self._has_ttl_indexes():
            return
        reap = getattr(self._collection, "reap_expired", None)
        if not callable(reap):
            return
        self._stop.clear()
        coll = self._collection
        dbn = getattr(coll, "db_name", "?")
        cn = getattr(coll, "name", "coll")
        self._thread = threading.Thread(
            target=self._run, daemon=True, name=f"ttl:{dbn}.{cn}"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _has_ttl_indexes(self) -> bool:
        try:
            for idx in self._collection.list_indexes():
                if idx.get("expireAfterSeconds") is not None:
                    return True
                if idx.get("expire_after_seconds") is not None:
                    return True
                opts = idx.get("options")
                if isinstance(opts, dict):
                    if opts.get("expireAfterSeconds") is not None:
                        return True
                    if opts.get("expire_after_seconds") is not None:
                        return True
        except Exception:
            return False
        return False

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._collection.reap_expired()
            except Exception as exc:
                log.debug("TTL reap cycle error: %s", exc)
            self._stop.wait(timeout=self._interval)


__all__ = ["TTLReaper", "_TTL_DELETE_BATCH_SIZE"]
