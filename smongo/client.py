"""
Connection Layer -- remote MongoDB (PyMongo) or embedded redb (local URIs).

MongoClient("mongodb://...")  -> real PyMongo
MongoClient("local://./path") -> embedded smongo-engine + redb (only ``local://``; other ``*://`` schemes are rejected)
"""

from __future__ import annotations

import logging
from typing import Any, cast

try:
    from pymongo import MongoClient as _PyMongoClient
except ImportError:
    _PyMongoClient = None  # type: ignore[misc, assignment]

from ._compat import StorageError as _StorageError
from ._types import Document, Filter, IndexKeys, Pipeline, Projection, UpdateSpec
from .aggregation import Cursor
from .index import DuplicateKeyError
from .schema import ValidationError
from .storage import DeleteResult, InsertResult, UpdateResult
from .storage.redb_engine import RedbClient, RedbCollection
from .sync import SyncManager

log = logging.getLogger("smongo.client")


# ------------------------------------------------------------------
# Bulk-write operation descriptors (lightweight PyMongo work-alikes)
# ------------------------------------------------------------------


class InsertOne:
    """Represents an insert_one operation for bulk_write."""

    def __init__(self, document: Document) -> None:
        self.document = document


class UpdateOne:
    """Represents an update_one operation for bulk_write."""

    def __init__(self, filter: Filter, update: UpdateSpec, upsert: bool = False) -> None:
        self.filter = filter
        self.update = update
        self.upsert = upsert


class UpdateMany:
    """Represents an update_many operation for bulk_write."""

    def __init__(self, filter: Filter, update: UpdateSpec, upsert: bool = False) -> None:
        self.filter = filter
        self.update = update
        self.upsert = upsert


class DeleteOne:
    """Represents a delete_one operation for bulk_write."""

    def __init__(self, filter: Filter) -> None:
        self.filter = filter


class DeleteMany:
    """Represents a delete_many operation for bulk_write."""

    def __init__(self, filter: Filter) -> None:
        self.filter = filter


class ReplaceOne:
    """Represents a replace_one operation for bulk_write."""

    def __init__(self, filter: Filter, replacement: Document, upsert: bool = False) -> None:
        self.filter = filter
        self.replacement = replacement
        self.upsert = upsert


class BulkWriteResult:
    """Result of a bulk_write operation (PyMongo-compatible structure)."""

    def __init__(self) -> None:
        self.inserted_count = 0
        self.matched_count = 0
        self.modified_count = 0
        self.deleted_count = 0
        self.upserted_count = 0
        self.upserted_ids: dict[int, Any] = {}
        self.write_errors: list[dict[str, Any]] = []


# ------------------------------------------------------------------
# Client
# ------------------------------------------------------------------


class MongoClient:
    """
    Drop-in client that routes to either real MongoDB or the local engine.
    The URI string dictates which backend is used.
    """

    def __init__(
        self,
        uri: str = "local://local_data",
        sync: str | None = None,
        sync_config: dict[str, Any] | None = None,
        *,
        durable: bool = True,
        backend: str | None = None,
    ) -> None:
        """
        Create a MongoDB client.

        Args:
            uri: Connection URI. Supported formats:
                - "mongodb://" or "mongodb+srv://" — remote MongoDB (PyMongo)
                - "local://<path>" — embedded redb (smongo-engine). Schemes other
                  than exactly ``local`` (e.g. ``local+bad://``) are rejected.
                - A bare filesystem path with no ``://`` — same as ``local://`` with that path
            sync: Remote MongoDB URI for hybrid sync mode
            sync_config: Configuration for sync manager
            durable: Hint for durability (redb is always durable on disk; kept for API compatibility)
            backend: Must be ``None`` or ``\"redb\"`` for local URIs.
        """
        self.uri = uri
        self._sync_mgr: SyncManager | None = None
        self._databases: dict[str, Database] = {}

        if uri.startswith(("mongodb://", "mongodb+srv://")):
            if not _PyMongoClient:  # type: ignore[truthy-function]
                raise ImportError("pymongo required for MongoDB connections")
            self.mode = "remote"
            self.client: Any = _PyMongoClient(uri)
        else:
            self.mode = "hybrid" if sync else "local"

            if backend is not None and str(backend).lower() not in ("redb", ""):
                raise ValueError(
                    f"Unsupported backend {backend!r}; local embedded mode uses redb only."
                )

            # Embedded mode: only `local://` is a valid scheme (avoid silent mis-parsing
            # of mistyped URIs such as `local+foo://...`).
            if "://" in uri:
                scheme, _, rest = uri.partition("://")
                if scheme.lower() != "local":
                    raise ValueError(
                        f"Unsupported URI scheme {scheme!r} for embedded mode. "
                        "Use local://<path> for the embedded engine, or "
                        "mongodb:// or mongodb+srv:// for a remote server."
                    )
                db_path = rest
            else:
                db_path = uri
            db_path = db_path or "local_data"

            log.info("Creating redb client at %s", db_path)
            self.client = RedbClient(db_path, durable=durable)
            self.backend = "redb"

            if sync:
                self._sync_mgr = SyncManager(self, sync, sync_config=sync_config)
                self._sync_mgr.start()

    def __getitem__(self, db_name: str) -> Database:
        if db_name in self._databases:
            return self._databases[db_name]
        if self.mode == "remote":
            db = Database(self.client[db_name], self.mode, client=self, db_name=db_name)
        else:
            db = Database(self.client.get_db(db_name), self.mode, client=self, db_name=db_name)
        self._databases[db_name] = db
        return db

    def get_local_client(self) -> RedbClient:
        """Return the underlying local client (only available in local mode)."""
        if self.mode not in ("local", "hybrid"):
            raise RuntimeError("get_local_client() only available in local mode")
        return self.client  # type: ignore[no-any-return]

    @property
    def sync(self) -> SyncManager | None:
        """Return the auto-managed SyncManager when in hybrid mode."""
        return self._sync_mgr

    def close(self) -> None:
        """Stop sync (if running) and close the underlying client."""
        if self._sync_mgr:
            self._sync_mgr.stop()
            self._sync_mgr = None
        if self.mode in ("local", "hybrid") and hasattr(self.client, "close"):
            self.client.close()

    def __enter__(self) -> MongoClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class Database:
    """Namespace container that maps collection names to :class:`Collection` instances."""

    def __init__(
        self,
        db: Any,
        mode: str,
        client: MongoClient | None = None,
        db_name: str | None = None,
    ) -> None:
        self.db = db
        self.mode = mode
        self._client = client
        self._db_name = db_name
        self._collections: dict[str, Collection] = {}

    def __getitem__(self, name: str) -> Collection:
        if name in self._collections:
            return self._collections[name]

        if self.mode == "remote":
            coll = Collection(self.db[name], self.mode, db=self)
        else:
            coll = Collection(self.db.get_collection(name), self.mode, db=self)
            if self._client and self._client.sync:
                try:
                    self._client.sync.register_collection(self._db_name or "", name, coll.backend)
                except (RuntimeError, KeyError, AttributeError) as exc:
                    log.debug("Sync registration failed for %s.%s: %s", self._db_name, name, exc)

        self._collections[name] = coll
        return coll

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *exc: object) -> None:
        if self._client:
            self._client.close()

    def list_collection_names(self) -> list[str]:
        """Return sorted names of all collections in this database."""
        if self.mode == "remote":
            return self.db.list_collection_names()  # type: ignore[no-any-return]
        return self.db.list_collection_names()  # type: ignore[no-any-return]

    def drop_collection(self, name: str) -> None:
        """Drop a collection and all its indexes."""
        self._collections.pop(name, None)
        if self.mode == "remote":
            self.db.drop_collection(name)
        else:
            self.db.drop_collection(name)

    def create_collection(self, name: str, **kwargs: Any) -> Collection:
        """Create a collection, optionally with a validator."""
        if self.mode == "remote":
            self.db.create_collection(name, **kwargs)
            coll = Collection(self.db[name], self.mode, db=self)
        else:
            validator = kwargs.get("validator")
            local_coll = self.db.create_collection(name, validator=validator)
            coll = Collection(local_coll, self.mode, db=self)
            if self._client and self._client.sync:
                try:
                    self._client.sync.register_collection(self._db_name or "", name, local_coll)
                except (RuntimeError, KeyError, AttributeError) as exc:
                    log.debug("Sync registration failed for %s.%s: %s", self._db_name, name, exc)
        self._collections[name] = coll
        return coll


class OperationFailure(Exception):
    """Raised when a database operation fails (PyMongo-compatible)."""

    def __init__(
        self, message: str, code: int | None = None, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details


class WriteError(OperationFailure):
    """Raised on write operation failure."""


class WriteConcernError(OperationFailure):
    """Raised on write concern failure."""


class BulkWriteError(OperationFailure):
    """Raised on bulk write failure."""


class CursorNotFound(OperationFailure):
    """Raised when a cursor is not found on the server."""


InsertOneResult = InsertResult


class Collection:
    """Unified collection API -- delegates to PyMongo or :class:`RedbCollection`."""

    def __init__(self, backend: Any, mode: str, db: Database | None = None) -> None:
        self.backend = backend
        self.mode = mode
        self._db = db

    def _make_collection_getter(self) -> Any:
        """Return a callable that resolves sibling collections for $lookup."""
        if self._db and self.mode == "local":
            return lambda name: self._db.db.get_collection(name)
        return None

    # -- reads ---------------------------------------------------------

    def find(self, query: Filter | None = None, projection: Projection | None = None) -> Any:
        """Return a cursor over documents matching *query*, optionally applying *projection*.

        In local mode with redb, results are materialized for the cursor API; the
        public ``Collection`` API matches PyMongo.
        """
        query = query or {}
        if self.mode == "remote":
            return self.backend.find(query, projection)
        if isinstance(self.backend, RedbCollection):
            docs = self.backend.find(query, projection=projection)
            coll_getter = self._make_collection_getter()
            return Cursor(docs, collection_getter=coll_getter)
        docs = self.backend.find_streaming(query)
        coll_getter = self._make_collection_getter()
        c = Cursor(docs, collection_getter=coll_getter)
        if projection:
            c = c.projection(projection)
        return c

    def find_one(
        self,
        query: Filter | None = None,
        projection: Projection | None = None,
    ) -> Document | None:
        """Return the first document matching *query*, or ``None``.

        *projection* is supported for remote PyMongo and for :class:`~smongo.storage.redb_engine.RedbCollection`
        (projection is applied in Python for non-redb legacy paths).
        """
        query = query or {}
        if self.mode == "remote":
            return self.backend.find_one(query, projection)  # type: ignore[no-any-return]
        if projection is not None and isinstance(self.backend, RedbCollection):
            return self.backend.find_one(query, projection=projection)
        return self.backend.find_one(query)  # type: ignore[no-any-return]

    def aggregate(self, pipeline: Pipeline) -> list[Document] | Any:
        """Run an aggregation *pipeline* and return the result documents.

        In local mode with redb, the pipeline runs entirely in the Rust engine
        via ``DatabaseContext`` — no FFI round-trips for cross-collection stages.
        """
        if self.mode == "remote":
            return list(self.backend.aggregate(pipeline))
        if isinstance(self.backend, RedbCollection) and hasattr(self.backend, "_rust_coll"):
            return self.backend._rust_coll.aggregate_engine(pipeline)
        docs = self.backend.find_streaming()
        coll_getter = self._make_collection_getter()
        return Cursor(docs, collection_getter=coll_getter).aggregate(pipeline)

    def count_documents(self, query: Filter | None = None) -> int:
        """Return the number of documents matching *query*."""
        query = query or {}
        if self.mode == "remote":
            return self.backend.count_documents(query)  # type: ignore[no-any-return]
        return self.backend.count(query)  # type: ignore[no-any-return]

    def explain(self, query: Filter | None = None) -> dict[str, Any]:
        """Return the query plan (local mode only)."""
        if self.mode == "remote":
            return {"plan": "remote"}
        return self.backend.explain(query or {})  # type: ignore[no-any-return]

    # -- change streams ------------------------------------------------

    def watch(self, pipeline: Pipeline | None = None) -> Any:
        """Open a change stream on this collection, optionally filtered by *pipeline*."""
        if self.mode == "remote":
            return self.backend.watch(pipeline)
        return self.backend.watch(pipeline)

    # -- writes --------------------------------------------------------

    def insert_one(self, doc: Document) -> InsertResult | Any:
        """Insert a single document and return the result."""
        if self.mode == "remote":
            return self.backend.insert_one(doc)
        return self.backend.insert_one(doc)

    def insert_many(self, docs: list[Document]) -> InsertResult | Any:
        """Insert multiple documents in a single transaction."""
        if self.mode == "remote":
            return self.backend.insert_many(docs)
        return self.backend.insert_many(docs)

    def update_one(
        self, query: Filter, update: UpdateSpec, upsert: bool = False
    ) -> UpdateResult | Any:
        """Update the first document matching *query* using *update* operators."""
        if self.mode == "remote":
            return self.backend.update_one(query, update, upsert=upsert)
        return self.backend.update(query, update, multi=False, upsert=upsert)

    def update_many(
        self, query: Filter, update: UpdateSpec, upsert: bool = False
    ) -> UpdateResult | Any:
        """Update all documents matching *query* using *update* operators."""
        if self.mode == "remote":
            return self.backend.update_many(query, update, upsert=upsert)
        return self.backend.update(query, update, multi=True, upsert=upsert)

    def delete_one(self, query: Filter) -> DeleteResult | Any:
        """Delete the first document matching *query*."""
        if self.mode == "remote":
            return self.backend.delete_one(query)
        return self.backend.delete(query, multi=False)

    def delete_many(self, query: Filter) -> DeleteResult | Any:
        """Delete all documents matching *query*."""
        if self.mode == "remote":
            return self.backend.delete_many(query)
        return self.backend.delete(query, multi=True)

    # -- find_one_and_* ------------------------------------------------

    def replace_one(
        self, query: Filter, replacement: Document, upsert: bool = False
    ) -> UpdateResult | Any:
        """Replace a single document matching *query* with *replacement*."""
        if self.mode == "remote":
            return self.backend.replace_one(query, replacement, upsert=upsert)
        result = self.backend.find_one_and_replace(query, replacement, upsert=upsert)
        if result is not None:
            return UpdateResult(1, 1)
        if upsert:
            return UpdateResult(0, 0, upserted_id=replacement.get("_id"))
        return UpdateResult(0, 0)

    def find_one_and_update(
        self, query: Filter, update: UpdateSpec, *, return_document: str = "before"
    ) -> Document | None:
        """Atomically find a document and apply *update*, returning the pre- or post-image."""
        if self.mode == "remote":
            return self.backend.find_one_and_update(query, update, return_document=return_document)  # type: ignore[no-any-return]
        return self.backend.find_one_and_update(query, update, return_document=return_document)  # type: ignore[no-any-return]

    def find_one_and_replace(
        self,
        query: Filter,
        replacement: Document,
        *,
        upsert: bool = False,
        return_document: str = "before",
    ) -> Document | None:
        """Atomically find a document and replace it, returning the pre- or post-image."""
        result = self.backend.find_one_and_replace(
            query, replacement, upsert=upsert, return_document=return_document
        )
        return cast(Document | None, result)

    def find_one_and_delete(self, query: Filter) -> Document | None:
        """Atomically find a document and delete it, returning the deleted document."""
        result = self.backend.find_one_and_delete(query)
        return cast(Document | None, result)

    # -- bulk_write ----------------------------------------------------

    def bulk_write(self, requests: list[Any], ordered: bool = True) -> BulkWriteResult | Any:
        """Execute a batch of write operations."""
        if self.mode == "remote":
            return self.backend.bulk_write(requests, ordered=ordered)

        result = BulkWriteResult()
        for idx, op in enumerate(requests):
            try:
                if isinstance(op, InsertOne):
                    self.backend.insert_one(op.document)
                    result.inserted_count += 1
                elif isinstance(op, UpdateOne):
                    r = self.backend.update(op.filter, op.update, multi=False, upsert=op.upsert)
                    result.matched_count += r.matched_count
                    result.modified_count += r.modified_count
                    if r.upserted_id is not None:
                        result.upserted_count += 1
                        result.upserted_ids[idx] = r.upserted_id
                elif isinstance(op, UpdateMany):
                    r = self.backend.update(op.filter, op.update, multi=True, upsert=op.upsert)
                    result.matched_count += r.matched_count
                    result.modified_count += r.modified_count
                    if r.upserted_id is not None:
                        result.upserted_count += 1
                        result.upserted_ids[idx] = r.upserted_id
                elif isinstance(op, DeleteOne):
                    r = self.backend.delete(op.filter, multi=False)
                    result.deleted_count += r.deleted_count
                elif isinstance(op, DeleteMany):
                    r = self.backend.delete(op.filter, multi=True)
                    result.deleted_count += r.deleted_count
                elif isinstance(op, ReplaceOne):
                    r = self.backend.find_one_and_replace(
                        op.filter, op.replacement, upsert=op.upsert
                    )
                    if r is not None:
                        result.matched_count += 1
                        result.modified_count += 1
                    elif op.upsert:
                        result.upserted_count += 1
            except (
                DuplicateKeyError,
                ValidationError,
                _StorageError,
                KeyError,
                TypeError,
                ValueError,
                RuntimeError,
                OSError,
            ) as exc:
                if ordered:
                    raise
                result.write_errors.append(
                    {
                        "index": idx,
                        "op": type(op).__name__,
                        "errmsg": str(exc),
                    }
                )
                log.debug("bulk_write op %d (%s) failed: %s", idx, type(op).__name__, exc)
        return result

    # -- indexes -------------------------------------------------------

    def create_index(self, keys: IndexKeys, **kwargs: Any) -> str | Any:
        """Create an index on *keys* and return its name."""
        if self.mode == "remote":
            return self.backend.create_index(keys, **kwargs)
        return self.backend.create_index(keys, **kwargs)

    def drop_index(self, name: str) -> None:
        """Drop the index identified by *name*."""
        if self.mode == "remote":
            self.backend.drop_index(name)
            return
        self.backend.drop_index(name)

    def list_indexes(self) -> list[dict[str, Any]]:
        """Return metadata for every index on this collection."""
        if self.mode == "remote":
            return list(self.backend.list_indexes())
        return self.backend.list_indexes()  # type: ignore[no-any-return]

    # -- oplog ---------------------------------------------------------

    def get_oplog(self) -> list[Document]:
        """Return the oplog entries for this collection (local mode only)."""
        if self.mode == "local":
            return self.backend.get_oplog()  # type: ignore[no-any-return]
        return []

    def get_local_collection(self) -> RedbCollection:
        """Return the underlying :class:`RedbCollection` (local mode only)."""
        if self.mode not in ("local", "hybrid"):
            raise RuntimeError("get_local_collection() only available in local mode")
        return self.backend  # type: ignore[no-any-return]
