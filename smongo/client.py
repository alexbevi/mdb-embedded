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


def _get_version() -> str:
    """Resolve smongo.__version__ lazily to avoid circular imports."""
    import smongo

    return smongo.__version__


# ------------------------------------------------------------------
# Helpers for distinct() — path resolution with array flattening
# ------------------------------------------------------------------


def _resolve_distinct_path(obj: Any, parts: list[str]) -> list[Any]:
    """Walk *parts* through *obj*, flattening arrays at each level (MongoDB semantics).

    When the terminal value is a list, each element is yielded individually
    rather than the list itself — matching ``db.coll.distinct("tags")`` when
    ``tags`` is ``["a", "b"]``.
    """
    if not parts:
        if isinstance(obj, list):
            out: list[Any] = []
            for item in obj:
                out.extend(_resolve_distinct_path(item, []))
            return out
        return [obj]

    head, *tail = parts

    if isinstance(obj, list):
        out = []
        for item in obj:
            out.extend(_resolve_distinct_path(item, [head, *tail]))
        return out

    if isinstance(obj, dict):
        if head not in obj:
            return []
        return _resolve_distinct_path(obj[head], tail)

    return []


_SENTINEL = object()


def _canonical_key(val: Any) -> str:
    """Return a hashable canonical representation for dedup in distinct().

    Normalises numeric types so that ``1`` (int) and ``1.0`` (float) with the
    same mathematical value map to the same key, matching MongoDB semantics.
    """
    if isinstance(val, bool):
        return f"bool:{val}"
    if isinstance(val, int | float):
        fv = float(val)
        if fv == int(fv) and fv == fv:  # not NaN
            return f"num:{int(fv)}"
        return f"num:{fv!r}"
    if isinstance(val, str):
        return f"str:{val}"
    if isinstance(val, list):
        return f"arr:{[_canonical_key(v) for v in val]}"
    if isinstance(val, dict):
        return f"doc:{sorted((k, _canonical_key(v)) for k, v in val.items())}"
    return f"other:{val!r}"


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
        if hasattr(self.client, "close"):
            self.client.close()

    def list_database_names(self) -> list[str]:
        """Return the names of all databases.

        For remote connections this queries the server.  For the embedded
        engine it returns the names of databases that have been accessed
        during this session.
        """
        if self.mode == "remote":
            return self.client.list_database_names()  # type: ignore[no-any-return]
        return sorted(self._databases.keys())

    def drop_database(self, name: str) -> None:
        """Drop a database and all its collections."""
        if self.mode == "remote":
            self.client.drop_database(name)
        else:
            if name in self._databases:
                db = self._databases[name]
                for coll_name in db.list_collection_names():
                    db.drop_collection(coll_name)
                del self._databases[name]

    def server_info(self) -> dict[str, Any]:
        """Return server/engine information.

        For remote connections this calls the real ``serverStatus`` command.
        For embedded mode a synthetic dict is returned with engine metadata.
        """
        if self.mode == "remote":
            return self.client.server_info()  # type: ignore[no-any-return]
        return {
            "version": _get_version(),
            "storageEngine": {"name": "redb"},
            "ok": 1.0,
            "smongo": True,
        }

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

        In local mode with redb, a lazy engine-backed iterator feeds the
        :class:`Cursor` so that ``skip``/``limit`` can short-circuit without
        materializing the full result set.  ``sort`` still materializes as
        expected (consistent with MongoDB server behaviour).
        """
        query = query or {}
        if self.mode == "remote":
            return self.backend.find(query, projection)
        # Both RedbCollection and RedbLocalCollection accept projection
        # as a kwarg on find(); use it when the backend supports it.
        if projection and hasattr(self.backend, "find"):
            docs = self.backend.find(query, projection=projection)
            coll_getter = self._make_collection_getter()
            return Cursor(docs, collection_getter=coll_getter)
        streaming = getattr(self.backend, "find_streaming", None) or getattr(
            self.backend, "find_iter", None
        )
        if streaming is not None:
            lazy_iter = streaming(query)
            coll_getter = self._make_collection_getter()
            c = Cursor(lazy_iter, collection_getter=coll_getter)
            if projection:
                c = c.projection(projection)
            return c
        docs = self.backend.find(query)
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

        *projection* is supported for all backends that accept it as a kwarg
        (RedbCollection, RedbLocalCollection, PyMongo).
        """
        query = query or {}
        if self.mode == "remote":
            return self.backend.find_one(query, projection)  # type: ignore[no-any-return]
        if projection is not None:
            return self.backend.find_one(query, projection=projection)  # type: ignore[no-any-return]
        return self.backend.find_one(query)  # type: ignore[no-any-return]

    def aggregate(
        self,
        pipeline: Pipeline,
        *,
        allowDiskUse: bool = False,
        memory_limit_bytes: int | None = None,
    ) -> list[Document] | Any:
        """Run an aggregation *pipeline* and return the result documents.

        In local mode with redb the pipeline runs entirely in the Rust engine
        via ``DatabaseContext`` — no FFI round-trips for cross-collection stages.

        When *allowDiskUse* is ``True``, ``$sort`` and ``$group`` stages that
        exceed the in-memory limit will spill intermediate data to temporary
        files instead of raising :class:`~smongo.aggregation.MemoryLimitExceeded`.
        """
        if self.mode == "remote":
            return list(self.backend.aggregate(pipeline))
        return self.backend.aggregate_engine(
            pipeline,
            memory_limit_bytes=memory_limit_bytes,
            allow_disk_use=allowDiskUse,
        )

    def count_documents(self, query: Filter | None = None) -> int:
        """Return the number of documents matching *query*."""
        query = query or {}
        if self.mode == "remote":
            return self.backend.count_documents(query)  # type: ignore[no-any-return]
        return self.backend.count(query)  # type: ignore[no-any-return]

    def distinct(self, key: str, filter: Filter | None = None) -> list[Any]:
        """Return distinct values for *key* among documents matching *filter*.

        Handles dotted-path traversal through arrays (MongoDB semantics),
        includes explicit ``None``/null values, and deduplicates via a
        canonical representation for consistent equality.
        """
        if self.mode == "remote":
            return self.backend.distinct(key, filter or {})  # type: ignore[no-any-return]

        seen_keys: set[str] = set()
        result: list[Any] = []
        has_none = False

        for doc in self.find(filter):
            for v in _resolve_distinct_path(doc, key.split(".")):
                if v is None:
                    if not has_none:
                        has_none = True
                        result.append(None)
                    continue
                ck = _canonical_key(v)
                if ck not in seen_keys:
                    seen_keys.add(ck)
                    result.append(v)
        return result

    def estimated_document_count(self) -> int:
        """Fast approximate count (uses count_fast when available)."""
        if self.mode == "remote":
            return self.backend.estimated_document_count()  # type: ignore[no-any-return]
        if hasattr(self.backend, "count_fast"):
            return self.backend.count_fast()  # type: ignore[no-any-return]
        return self.count_documents({})

    def explain(self, query: Filter | None = None) -> dict[str, Any]:
        """Return the query plan (local mode only)."""
        if self.mode == "remote":
            return {"plan": "remote"}
        return self.backend.explain(query or {})  # type: ignore[no-any-return]

    # -- change streams ------------------------------------------------

    def watch(
        self,
        pipeline: Pipeline | None = None,
        *,
        resume_after: dict[str, Any] | None = None,
        max_await_time_ms: int | None = None,
    ) -> Any:
        """Open a change stream on this collection.

        Args:
            pipeline: Optional ``$match`` filter pipeline.
            resume_after: Resume token from a previous event's ``_resumeToken``
                field.  The stream will skip past the identified event and
                deliver only subsequent changes.
            max_await_time_ms: Maximum blocking time (ms) for the iterator's
                ``__next__`` before raising ``StopIteration``.  Only applies to
                the embedded (local) backend.  Defaults to 30 000 ms.
        """
        if self.mode == "remote":
            kwargs: dict[str, Any] = {}
            if resume_after is not None:
                kwargs["resume_after"] = resume_after
            return self.backend.watch(pipeline, **kwargs)
        return self.backend.watch(
            pipeline,
            resume_after=resume_after,
            max_await_time_ms=max_await_time_ms,
        )

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

    # -- admin ---------------------------------------------------------

    def drop(self) -> None:
        """Drop this collection (delegates to the parent database)."""
        if self._db is not None:
            self._db.drop_collection(self.backend.name)
        elif self.mode == "remote":
            self.backend.drop()

    def rename(self, new_name: str, **kwargs: Any) -> None:
        """Rename this collection."""
        if self.mode == "remote":
            self.backend.rename(new_name, **kwargs)
        else:
            raise NotImplementedError(
                "Collection.rename() is not yet supported for the embedded engine"
            )

    def get_local_collection(self) -> RedbCollection:
        """Return the underlying :class:`RedbCollection` (local mode only)."""
        if self.mode not in ("local", "hybrid"):
            raise RuntimeError("get_local_collection() only available in local mode")
        return self.backend  # type: ignore[no-any-return]
