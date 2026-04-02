"""
Server-side cursor registry for batched query results.

Manages cursor IDs, firstBatch/nextBatch semantics, idle expiration,
and memory-safety guards (max cursor count, LRU eviction).
"""

import random
import threading
import time
from typing import Any

MAX_BSON_OBJECT_SIZE = 16 * 1024 * 1024
MAX_MESSAGE_SIZE = 48 * 1024 * 1024
MAX_WRITE_BATCH_SIZE = 100_000


class _CursorState:
    __slots__ = ("batch_size", "change_stream", "created_at", "docs", "last_accessed", "ns", "offset", "tailable")

    def __init__(self, ns: str, docs: list[Any], offset: int, batch_size: int, *, change_stream: Any = None) -> None:
        self.ns = ns
        self.docs = docs
        self.offset = offset
        self.batch_size = batch_size
        self.change_stream = change_stream
        self.tailable = change_stream is not None
        now = time.monotonic()
        self.created_at = now
        self.last_accessed = now


class CursorRegistry:
    """Thread-safe registry mapping int64 cursor IDs to batched result sets."""

    def __init__(
        self,
        default_batch_size: int = 101,
        idle_timeout_sec: int = 600,
        max_cursors: int = 10_000,
    ) -> None:
        self._cursors: dict[int, _CursorState] = {}
        self._lock = threading.Lock()
        self._default_batch_size = default_batch_size
        self._idle_timeout = idle_timeout_sec
        self._max_cursors = max_cursors
        self._reaper_thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start_reaper(self) -> None:
        if self._reaper_thread and self._reaper_thread.is_alive():
            return
        self._stop.clear()
        self._reaper_thread = threading.Thread(
            target=self._reap_loop, daemon=True, name="cursor-reaper"
        )
        self._reaper_thread.start()

    def stop_reaper(self) -> None:
        self._stop.set()
        if self._reaper_thread:
            self._reaper_thread.join(timeout=2)

    def create(self, ns: str, docs: list[Any], batch_size: int | None = None) -> tuple[int, list[Any]]:
        """Register docs and return (cursor_id, first_batch).

        If all docs fit in one batch, cursor_id is 0 (exhausted).
        """
        bs = batch_size or self._default_batch_size
        if bs <= 0:
            bs = self._default_batch_size

        first_batch = docs[:bs]

        if len(docs) <= bs:
            return 0, first_batch

        with self._lock:
            if len(self._cursors) >= self._max_cursors:
                self._evict_oldest()
            cursor_id = self._generate_id()
            self._cursors[cursor_id] = _CursorState(ns, docs, bs, bs)

        return cursor_id, first_batch

    def get_more(
        self, cursor_id: int, batch_size: int | None = None
    ) -> tuple[int | None, list[Any] | None]:
        """Return (cursor_id, next_batch) or (None, None) if cursor not found."""
        with self._lock:
            state = self._cursors.get(cursor_id)
            if state is None:
                return None, None
            state.last_accessed = time.monotonic()

            bs = batch_size or state.batch_size
            start = state.offset
            end = start + bs
            batch = state.docs[start:end]
            state.offset = end

            if end >= len(state.docs):
                del self._cursors[cursor_id]
                return 0, batch

        return cursor_id, batch

    def kill(self, cursor_ids: list[int]) -> list[int]:
        """Remove the given cursors. Returns list of IDs actually killed."""
        killed: list[int] = []
        with self._lock:
            for cid in cursor_ids:
                if self._cursors.pop(cid, None) is not None:
                    killed.append(cid)
        return killed

    def create_change_stream(self, ns: str, stream: Any, batch_size: int | None = None) -> int:
        """Create a tailable cursor backed by a ChangeStream."""
        bs = batch_size or self._default_batch_size
        with self._lock:
            if len(self._cursors) >= self._max_cursors:
                self._evict_oldest()
            cursor_id = self._generate_id()
            self._cursors[cursor_id] = _CursorState(ns, [], 0, bs, change_stream=stream)
        return cursor_id

    def get_more_change_stream(self, cursor_id: int, batch_size: int | None = None, max_await_ms: int = 1000) -> tuple[int | None, list[Any] | None]:
        """Drain pending events from a change-stream cursor."""
        with self._lock:
            state = self._cursors.get(cursor_id)
            if state is None or state.change_stream is None:
                return None, None
            state.last_accessed = time.monotonic()
            cs = state.change_stream
            bs = batch_size or state.batch_size

        events: list[Any] = []
        deadline = time.monotonic() + (max_await_ms / 1000.0)
        while len(events) < bs:
            event = cs.try_next()
            if event:
                events.append(event)
            elif time.monotonic() >= deadline:
                break
            else:
                time.sleep(0.05)
                if time.monotonic() >= deadline:
                    break

        return cursor_id, events

    # -- internal ---------------------------------------------------------

    def _generate_id(self) -> int:
        """Must be called with self._lock held."""
        while True:
            cid = random.getrandbits(63) | 1
            if cid not in self._cursors:
                return cid

    def _evict_oldest(self) -> None:
        """LRU eviction when at capacity. Must be called with self._lock held."""
        if not self._cursors:
            return
        oldest = min(self._cursors, key=lambda c: self._cursors[c].last_accessed)
        del self._cursors[oldest]

    def _reap_loop(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(timeout=60)
            if self._stop.is_set():
                break
            self._expire_idle()

    def _expire_idle(self) -> None:
        now = time.monotonic()
        with self._lock:
            expired = [
                cid
                for cid, cs in self._cursors.items()
                if now - cs.last_accessed > self._idle_timeout
            ]
            for cid in expired:
                del self._cursors[cid]
