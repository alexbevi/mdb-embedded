"""
Embedded storage — redb-backed engine (``RedbClient``, ``RedbCollection``).

Embedded storage is redb-only; use :class:`~smongo.storage.redb_engine.RedbClient`
or :class:`~smongo.client.MongoClient` with a ``local://`` URI.
"""

from __future__ import annotations

from typing import Any

from .._compat import StorageError
from .collection import _TTL_DELETE_BATCH_SIZE, TTLReaper
from .locking import ReadWriteLock
from .redb_engine import RedbClient, RedbCollection, RedbDB
from .results import DeleteResult, InsertResult, UpdateResult
from .streaming import StreamingCursor
from .transaction import TransactionSession, get_active_txn_session

_StorageError = StorageError


def __getattr__(name: str) -> Any:
    if name == "BulkWriteResult":
        from ..client import BulkWriteResult

        return BulkWriteResult
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "_TTL_DELETE_BATCH_SIZE",
    "BulkWriteResult",
    "DeleteResult",
    "InsertResult",
    "RedbClient",
    "RedbCollection",
    "RedbDB",
    "ReadWriteLock",
    "StreamingCursor",
    "TTLReaper",
    "TransactionSession",
    "UpdateResult",
    "StorageError",
    "_StorageError",
    "get_active_txn_session",
]
