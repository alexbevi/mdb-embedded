"""Profiling, operation tracking, and top stats for the wire protocol.

OperationTracker -- global registry of in-flight operations.
TopStats -- per-namespace timing stats for the ``top`` command.
Profiler -- ring-buffer profiler mirroring ``db.setProfilingLevel()``.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import (
    OpEntry,
    OperationTracker,
    Profiler,
    TopStats,
)

__all__ = [
    "OpEntry",
    "OperationTracker",
    "Profiler",
    "TopStats",
]
