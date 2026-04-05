"""Server-side cursor registry for batched query results.

Manages cursor IDs, firstBatch/nextBatch semantics, idle expiration,
and memory-safety guards (max cursor count, LRU eviction).

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import (
    MAX_BSON_OBJECT_SIZE,
    MAX_MESSAGE_SIZE,
    MAX_WRITE_BATCH_SIZE,
    CursorRegistry,
)

__all__ = [
    "MAX_BSON_OBJECT_SIZE",
    "MAX_MESSAGE_SIZE",
    "MAX_WRITE_BATCH_SIZE",
    "CursorRegistry",
]
