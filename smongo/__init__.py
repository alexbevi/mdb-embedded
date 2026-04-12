"""
smongo -- Small MongoDB. Big ambitions.

All the Mongo, none of the overhead. A local-first document engine built on
**redb** and Rust (``smongo-engine``), with a PyMongo-compatible API, B-tree
indexes, a cost-based query planner, and bidirectional sync to Atlas when
you're ready.

pip install smongo  -- and you've got a database.
"""

from __future__ import annotations

import warnings
from typing import Any

from .aggregation import Cursor, DocumentLimitExceeded
from .async_client import (
    AsyncChangeStream,
    AsyncCollection,
    AsyncCursor,
    AsyncDatabase,
    AsyncMongoClient,
)
from .client import (
    BulkWriteError,
    BulkWriteResult,
    Collection,
    CursorNotFound,
    Database,
    DeleteMany,
    DeleteOne,
    InsertOne,
    InsertOneResult,
    MongoClient,
    OperationFailure,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
    WriteConcernError,
    WriteError,
)
from .index import DuplicateKeyError
from .objectid import ObjectId
from .schema import ValidationError
from .storage import DeleteResult, InsertResult, StreamingCursor, UpdateResult
from .sync import SyncManager
from .wire import WireServer

__version__ = "1.2.4"

try:
    from ._smongo_core import __build_version__ as _native_version

    _nv = _native_version()
    if _nv != __version__:
        warnings.warn(
            f"smongo version {__version__} but native extension is {_nv}. "
            "Run `make build-debug` or `pip install -e .` from the repo root.",
            stacklevel=1,
        )
    del _nv, _native_version
except ImportError:
    pass


def connect(
    path: str = "smongo_data",
    db: str = "default",
    *,
    sync: str | None = None,
    sync_config: dict[str, Any] | None = None,
) -> Database:
    """Zero-config quickstart -- like ``sqlite3.connect()``.

    Returns a :class:`Database` backed by a redb data directory at *path*.
    The ``MongoClient`` is stashed on the returned database so you can reach
    it via ``db._client`` if needed, and the database implements the context
    manager protocol for clean shutdown::

        with smongo.connect() as db:
            db["users"].insert_one({"name": "Alice"})
    """
    client = MongoClient(f"local://{path}", sync=sync, sync_config=sync_config)
    return client[db]


__all__ = [
    "AsyncChangeStream",
    "AsyncCollection",
    "AsyncCursor",
    "AsyncDatabase",
    "AsyncMongoClient",
    "BulkWriteError",
    "BulkWriteResult",
    "Collection",
    "Cursor",
    "CursorNotFound",
    "Database",
    "DeleteMany",
    "DeleteOne",
    "DeleteResult",
    "DocumentLimitExceeded",
    "DuplicateKeyError",
    "InsertOne",
    "InsertOneResult",
    "InsertResult",
    "MongoClient",
    "ObjectId",
    "OperationFailure",
    "ReplaceOne",
    "StreamingCursor",
    "SyncManager",
    "UpdateMany",
    "UpdateOne",
    "UpdateResult",
    "ValidationError",
    "WireServer",
    "WriteConcernError",
    "WriteError",
    "connect",
]
