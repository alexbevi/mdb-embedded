"""
Local Embedded Engine -- WiredTiger storage layer.

Each collection is a WiredTiger B-Tree table (key=_id, value=BSON document).
Write operations are wrapped in WiredTiger transactions for atomicity,
all session access is serialized with a per-collection lock for thread safety,
and the query planner accelerates writes as well as reads.
"""

from __future__ import annotations

from typing import Any

from .._compat import WTError as _WTError
from .collection import _TTL_DELETE_BATCH_SIZE, LocalCollection, TTLReaper
from .engine import LocalClient, LocalDB
from .locking import ReadWriteLock
from .results import DeleteResult, InsertResult, UpdateResult
from .streaming import StreamingCursor
from .transaction import TransactionSession, get_active_txn_session


def __getattr__(name: str) -> Any:
    """Lazy re-export for names not defined in the embedded engine package."""
    if name == "BulkWriteResult":
        from ..client import BulkWriteResult

        return BulkWriteResult
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "_TTL_DELETE_BATCH_SIZE",
    "BulkWriteResult",
    "DeleteResult",
    "InsertResult",
    "LocalClient",
    "LocalCollection",
    "LocalDB",
    "ReadWriteLock",
    "StreamingCursor",
    "TTLReaper",
    "TransactionSession",
    "UpdateResult",
    "_WTError",
    "get_active_txn_session",
]
