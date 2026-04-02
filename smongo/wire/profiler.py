"""Profiling, operation tracking, and top stats for the wire protocol."""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from itertools import count
from typing import Any


class OpEntry:
    """Describes one in-flight operation."""

    __slots__ = ("cancelled", "command", "connection_id", "ns", "op", "op_id", "start_time")

    def __init__(
        self,
        op_id: int,
        op: str,
        ns: str,
        command: dict[str, Any],
        connection_id: int,
    ) -> None:
        self.op_id = op_id
        self.op = op
        self.ns = ns
        self.command = command
        self.start_time = time.monotonic()
        self.connection_id = connection_id
        self.cancelled = False


class OperationTracker:
    """Global registry of in-flight operations."""

    def __init__(self) -> None:
        self._ops: dict[int, OpEntry] = {}
        self._lock = threading.Lock()
        self._counter = count(1)

    def start_op(self, op: str, ns: str, command: dict[str, Any], connection_id: int) -> int:
        op_id = next(self._counter)
        entry = OpEntry(op_id, op, ns, command, connection_id)
        with self._lock:
            self._ops[op_id] = entry
        return op_id

    def finish_op(self, op_id: int) -> None:
        with self._lock:
            self._ops.pop(op_id, None)

    def kill_op(self, op_id: int) -> bool:
        with self._lock:
            entry = self._ops.get(op_id)
            if entry is None:
                return False
            entry.cancelled = True
            return True

    def active_ops(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        with self._lock:
            return [
                {
                    "opid": e.op_id,
                    "active": True,
                    "op": e.op,
                    "ns": e.ns,
                    "command": e.command,
                    "connectionId": e.connection_id,
                    "secs_running": int(now - e.start_time),
                    "microsecs_running": int((now - e.start_time) * 1_000_000),
                }
                for e in self._ops.values()
            ]


def _empty_timing() -> dict[str, int]:
    return {"time": 0, "count": 0}


class CollectionTimingStats:
    """Per-collection operation counters and cumulative microsecond timings."""

    __slots__ = (
        "commands",
        "getmore",
        "insert",
        "queries",
        "readLock",
        "remove",
        "total",
        "update",
        "writeLock",
    )

    def __init__(self) -> None:
        self.total = _empty_timing()
        self.readLock = _empty_timing()
        self.writeLock = _empty_timing()
        self.queries = _empty_timing()
        self.getmore = _empty_timing()
        self.insert = _empty_timing()
        self.update = _empty_timing()
        self.remove = _empty_timing()
        self.commands = _empty_timing()

    def record(self, op: str, micros: int) -> None:
        bucket = getattr(self, op, None) or self.commands
        bucket["time"] += micros
        bucket["count"] += 1
        self.total["time"] += micros
        self.total["count"] += 1
        if op in ("queries", "getmore"):
            self.readLock["time"] += micros
            self.readLock["count"] += 1
        elif op in ("insert", "update", "remove"):
            self.writeLock["time"] += micros
            self.writeLock["count"] += 1

    def to_dict(self) -> dict[str, dict[str, int]]:
        return {
            "total": dict(self.total),
            "readLock": dict(self.readLock),
            "writeLock": dict(self.writeLock),
            "queries": dict(self.queries),
            "getmore": dict(self.getmore),
            "insert": dict(self.insert),
            "update": dict(self.update),
            "remove": dict(self.remove),
            "commands": dict(self.commands),
        }


class TopStats:
    """Global per-namespace timing stats for the ``top`` command."""

    def __init__(self) -> None:
        self._stats: dict[str, CollectionTimingStats] = {}
        self._lock = threading.Lock()

    def record(self, ns: str, op: str, micros: int) -> None:
        with self._lock:
            if ns not in self._stats:
                self._stats[ns] = CollectionTimingStats()
            self._stats[ns].record(op, micros)

    def snapshot(self) -> dict[str, dict[str, dict[str, int]]]:
        with self._lock:
            return {ns: s.to_dict() for ns, s in self._stats.items()}


class Profiler:
    """Operation profiler that mirrors ``db.setProfilingLevel()`` semantics.

    level 0 = off, level 1 = slow ops only, level 2 = all ops.
    """

    def __init__(self, level: int = 0, slow_ms: int = 100, max_entries: int = 4096) -> None:
        self.level = level
        self.slow_ms = slow_ms
        self._entries: list[dict[str, Any]] = []
        self._max = max_entries
        self._lock = threading.Lock()

    def log(
        self,
        op: str,
        ns: str,
        millis: int,
        command: dict[str, Any] | None = None,
        plan_summary: str = "",
        response_length: int = 0,
        n_returned: int = 0,
    ) -> None:
        if self.level == 0:
            return
        if self.level == 1 and millis < self.slow_ms:
            return
        entry: dict[str, Any] = {
            "op": op,
            "ns": ns,
            "millis": millis,
            "ts": datetime.now(UTC),
            "command": command or {},
            "planSummary": plan_summary,
            "responseLength": response_length,
            "nreturned": n_returned,
        }
        with self._lock:
            self._entries.append(entry)
            if len(self._entries) > self._max:
                self._entries = self._entries[-self._max :]

    def get_entries(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._entries[-limit:])

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
