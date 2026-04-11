"""Python wrapper layer over the Rust-native ``RedbLocalClient``.

``RedbClient`` / ``RedbDB`` / ``RedbCollection`` provide a higher-level API
(e.g. ``update()``, ``delete()`` with ``multi`` parameter, streaming cursors,
``$jsonSchema`` validation, change-stream wrappers) on top of the underlying
Rust ``RedbLocal*`` types exposed by ``_smongo_core``.

**Who creates these wrappers:**

- ``MongoClient("local://...")`` — the public client entry point.
- ``SyncManager`` — bidirectional sync.
- ``test_storage.py``, ``test_streaming.py`` — Python wrapper tests.

**Wire protocol path does NOT use these wrappers.** ``WireServer`` and
``RustWireServer`` create a bare ``RedbLocalClient`` and Rust command
handlers call ``RedbLocalCollection`` directly.  The Rust
``ConnectionContext`` fallback chains in ``wire_context.rs`` unwrap the
Python wrapper if one happens to be present, but this is only for the
``MongoClient`` / ``SyncManager`` path.
"""

from __future__ import annotations

import os
import time as _time
from collections.abc import Iterator
from typing import Any, cast

from .._smongo_core import RedbLocalClient as _RedbLocalClient
from .._smongo_core import RedbLocalCollection as _RedbLocalCollection
from .._smongo_core import RedbLocalDB as _RedbLocalDB
from ..schema import validate_document
from .collection import TTLReaper
from .helpers import log
from .results import DeleteResult, InsertResult, UpdateResult


class _ChangeStreamWrapper:
    """Wrapper around the Rust RedbChangeStream with resume tokens and long-polling.

    Each change event carries a ``_resumeToken`` containing the oplog
    timestamp and a monotonic sequence number.  Passing this token as
    ``resume_after`` to :meth:`RedbCollection.watch` restarts the stream
    from the event *after* the one identified by the token.
    """

    def __init__(
        self,
        inner: Any,
        *,
        resume_after: dict[str, Any] | None = None,
        max_await_time_ms: int | None = None,
    ) -> None:
        self._inner = inner
        self._resume_token: dict[str, Any] | None = resume_after
        self._seq: int = 0
        self._closed = False
        self._max_await_s = (max_await_time_ms or 30_000) / 1000.0
        self._skipping = resume_after is not None
        self._resume_ts = (resume_after or {}).get("ts")
        self._resume_seq = (resume_after or {}).get("seq", 0)

    @property
    def resume_token(self) -> dict[str, Any] | None:
        """The resume token of the most recently returned event."""
        return self._resume_token

    def try_next(self) -> dict[str, Any] | None:
        """Non-blocking: return the next event or ``None``."""
        while True:
            ev = self._inner.try_next()
            if ev is None:
                return None
            if self._skipping:
                ev_ts = ev.get("_ts") or ev.get("clusterTime")
                if ev_ts is not None and self._resume_ts is not None:
                    if ev_ts < self._resume_ts:
                        continue
                    if ev_ts == self._resume_ts and self._seq <= self._resume_seq:
                        self._seq += 1
                        continue
                self._skipping = False
            self._seq += 1
            token = {"ts": ev.get("_ts") or ev.get("clusterTime"), "seq": self._seq}
            self._resume_token = token
            ev["_resumeToken"] = token
            return ev

    def close(self) -> None:
        self._closed = True
        self._inner.close()

    def __enter__(self) -> _ChangeStreamWrapper:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __iter__(self) -> _ChangeStreamWrapper:
        return self

    def __next__(self) -> dict[str, Any]:
        import time as _t

        deadline = _t.monotonic() + self._max_await_s
        while not self._closed:
            ev = self.try_next()
            if ev is not None:
                return ev
            if _t.monotonic() >= deadline:
                raise StopIteration
            _t.sleep(0.05)
        raise StopIteration


def _is_pk_equality_filter(query: dict[str, Any]) -> bool:
    qid = query.get("_id")
    if qid is None:
        return False
    if isinstance(qid, dict) and "$eq" in qid:
        return True
    return not isinstance(qid, dict)


def _redb_explain_plan_kind(execution_plan: Any, query_filter: dict[str, Any] | None) -> str:
    """Map engine ``execution_plan`` BSON shape to a MongoDB-style ``plan`` string."""
    qf = query_filter or {}
    if execution_plan is None:
        return "collection_scan"
    if isinstance(execution_plan, str):
        if execution_plan == "IXSEEK" and _is_pk_equality_filter(qf):
            return "pk_lookup"
        return {
            "COLLSCAN": "collection_scan",
            "IXSCAN": "index_scan",
            "IXSEEK": "index_scan",
            "GEO": "geo",
            "OR_UNION": "or_union",
        }.get(execution_plan, execution_plan.lower())
    if isinstance(execution_plan, dict):
        if "COLLSCAN" in execution_plan:
            return "collection_scan"
        if "IXSCAN" in execution_plan:
            return "index_scan"
        if "IXSEEK" in execution_plan:
            if _is_pk_equality_filter(qf):
                return "pk_lookup"
            return "index_scan"
        if "GEO" in execution_plan:
            return "geo"
        if "OR_UNION" in execution_plan:
            return "or_union"
    return "collection_scan"


class RedbClient:
    """Top-level redb connection manager (LocalClient-compatible API)."""

    def __init__(self, db_path: str, *, durable: bool = True) -> None:
        """
        Open a redb-backed client at the given path.

        Args:
            db_path: Directory path for the database
            durable: Currently ignored for redb (always durable)
        """
        os.makedirs(db_path, exist_ok=True)
        self._rust_client = _RedbLocalClient(db_path)
        self._db_path = db_path
        self.durable = durable
        self._dbs: dict[str, RedbDB] = {}

    def __repr__(self) -> str:
        return f"RedbClient(path={self._db_path!r})"

    def get_db(self, name: str) -> RedbDB:
        """Get or create a database handle."""
        if name not in self._dbs:
            rust_db = self._rust_client.get_db(name)
            self._dbs[name] = RedbDB(rust_db, name)
        return self._dbs[name]

    def checkpoint(self) -> None:
        """
        Checkpoint operation (no-op for redb - transactions auto-commit).

        redb handles durability through its transaction model,
        so explicit checkpointing is not needed.
        """
        pass

    def close(self) -> None:
        """Close all collections and the redb connection."""
        for db in self._dbs.values():
            db.close()
        self._rust_client.close()

    def __enter__(self) -> RedbClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class RedbDB:
    """Database namespace for redb collections (LocalDB-compatible API)."""

    def __init__(self, rust_db: _RedbLocalDB, name: str) -> None:
        self._rust_db = rust_db
        self.name = name
        self._collections: dict[str, RedbCollection] = {}
        self._validators: dict[str, dict[str, Any] | None] = {}

    def __repr__(self) -> str:
        return f"RedbDB(name={self.name!r})"

    def get_collection(self, name: str) -> RedbCollection:
        """Return (and lazily create) the named collection."""
        if name not in self._collections:
            rust_coll = self._rust_db.collection(name)
            validator = self._validators.get(name)
            self._collections[name] = RedbCollection(
                rust_coll, self.name, name, db=self, validator=validator
            )
        return self._collections[name]

    def collection(self, name: str) -> RedbCollection:
        """Alias for :meth:`get_collection` (PyMongo-style naming)."""
        return self.get_collection(name)

    def create_collection(
        self, name: str, validator: dict[str, Any] | None = None, **kwargs: Any
    ) -> RedbCollection:
        """Create a collection, optionally attaching a $jsonSchema validator."""
        schema: dict[str, Any] | None = None
        if validator:
            schema = validator.get("$jsonSchema", validator)
        self._validators[name] = schema
        coll = self.get_collection(name)
        if schema:
            coll._validator = schema
        return coll

    def drop_collection(self, name: str) -> None:
        """Remove the collection, its secondary index tables, and cached handles (oplog table dropped separately in Rust)."""
        self._collections.pop(name, None)
        self._validators.pop(name, None)
        try:
            self._rust_db.drop_collection(name)
        except RuntimeError as e:
            msg = str(e).lower()
            if "not found" in msg or "collection not found" in msg:
                return
            raise

    def list_collection_names(self) -> list[str]:
        """Return all collection names (on-disk plus opened handles)."""
        try:
            rust_names = list(self._rust_db.list_collection_names())
        except RuntimeError as e:
            # RuntimeError from Rust FFI is expected when the DB has never
            # been written to (empty table set).  All other errors propagate.
            log.debug("list_collection_names: %s", e)
            rust_names = []
        merged = set(rust_names) | set(self._collections.keys())
        return sorted(merged)

    def close(self) -> None:
        """Close all owned collections (no-op for redb)."""
        pass


class RedbCollection:
    """Redb-backed collection: CRUD, indexes, oplog, change streams, and sync hooks (``_internal``, ``get_by_id``, etc.)."""

    def __init__(
        self,
        rust_coll: _RedbLocalCollection,
        db_name: str,
        name: str,
        db: RedbDB | None = None,
        validator: dict[str, Any] | None = None,
    ) -> None:
        self._rust_coll = rust_coll
        self.db_name = db_name
        self.name = name
        self._db = db
        self._validator = validator
        self._ttl_reaper = TTLReaper(self)

    def __repr__(self) -> str:
        return f"RedbCollection(ns={self.db_name!r}.{self.name!r})"

    @property
    def _oplog_w(self) -> Any:
        """Oplog writer bridge (``oplog_uri``, ``node_id``) for :class:`~smongo.sync.SyncManager`."""
        return self._rust_coll._oplog_w

    def get_oplog_reader(self) -> Any:
        """Return a reader with ``read_from(checkpoint, skip_internal=...)`` matching :class:`~smongo.oplog.OplogReader`."""
        return self._rust_coll.get_oplog_reader()

    def get_oplog(self) -> list[dict[str, Any]]:
        """Return all oplog entries (including ``internal``), newest order by key scan."""
        reader = self.get_oplog_reader()
        pairs = reader.read_from(None, skip_internal=False)
        return [dict(entry) for _key, entry in pairs]

    def compact_oplog(self, keep: int) -> int:
        """Remove oldest oplog entries until at most *keep* remain; returns number removed."""
        return int(self._rust_coll.compact_oplog(int(keep)))

    def get_by_id(self, doc_id: Any) -> dict[str, Any] | None:
        """Fetch a document by ``_id`` (used by sync filters on updates)."""
        return self._rust_coll.get_by_id(doc_id)

    def get_by_ids(self, doc_ids: list[Any]) -> list[dict[str, Any]]:
        """Return documents for the given ``_id`` values (skip missing)."""
        out: list[dict[str, Any]] = []
        for doc_id in doc_ids:
            doc = self.get_by_id(doc_id)
            if doc is not None:
                out.append(doc)
        return out

    def watch(
        self,
        pipeline: list[Any] | None = None,
        *,
        resume_after: dict[str, Any] | None = None,
        max_await_time_ms: int | None = None,
    ) -> _ChangeStreamWrapper:
        """Open a change stream on this collection, optionally resuming from a token.

        Args:
            pipeline: Optional ``$match`` filter pipeline for server-side filtering.
            resume_after: Resume token returned by a previous stream (``event["_resumeToken"]``).
                The stream will skip events up to and including the token, then deliver
                subsequent events.
            max_await_time_ms: Maximum time (ms) that :meth:`__next__` blocks waiting
                for a new event before raising ``StopIteration``.  Defaults to 30 000 ms.
        """
        return _ChangeStreamWrapper(
            self._rust_coll.watch(pipeline),
            resume_after=resume_after,
            max_await_time_ms=max_await_time_ms,
        )

    # CRUD operations - delegate directly to Rust

    def insert_one(self, document: dict[str, Any], *, _internal: bool = False) -> InsertResult:
        """Insert a single document."""
        doc = dict(document)
        if not _internal and self._validator:
            validate_document(doc, self._validator)
        result = self._rust_coll.insert_one(doc, internal=_internal)
        # Match PyMongo: ``inserted_ids`` is always a sequence.
        return InsertResult([result["inserted_id"]])

    def insert_many(
        self,
        documents: list[dict[str, Any]],
        ordered: bool = True,
        *,
        _internal: bool = False,
    ) -> InsertResult:
        """Insert multiple documents."""
        _ = ordered
        docs = [dict(d) for d in documents]
        if not _internal and self._validator:
            for d in docs:
                validate_document(d, self._validator)
        result = self._rust_coll.insert_many(docs, internal=_internal)
        # InsertResult stores inserted_ids (can be single ID or list)
        return InsertResult(result["inserted_ids"])

    def find_one(
        self, filter: dict[str, Any] | None = None, projection: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Find a single document (projection uses engine ``$project`` semantics)."""
        query = filter or {}
        return self._rust_coll.find_one(query, projection)

    def find(
        self, filter: dict[str, Any] | None = None, projection: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Find documents matching filter (optional inclusion / exclusion projection)."""
        query = filter or {}
        return self._rust_coll.find(query, projection)

    def find_streaming(self, query: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        """Lazily iterate matches for *query* via the engine's streaming cursor."""
        return self._rust_coll.find_iter(query or {})

    def aggregate_engine(
        self,
        pipeline: list[dict[str, Any]],
        *,
        memory_limit_bytes: int | None = None,
        allow_disk_use: bool = False,
    ) -> list[dict[str, Any]]:
        """Run *pipeline* through the Rust aggregation engine."""
        return list(
            self._rust_coll.aggregate_engine(
                pipeline,
                memory_limit_bytes=memory_limit_bytes,
                allow_disk_use=allow_disk_use,
            )
        )

    def get_all(self) -> list[dict[str, Any]]:
        """Return all documents in the collection (same shape as legacy storage helpers)."""
        return list(self._rust_coll.get_all())

    def find_one_and_update(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        *,
        return_document: str = "before",
        _internal: bool = False,
    ) -> dict[str, Any] | None:
        """Atomically update one matching document; return pre- or post-image."""
        result = self._rust_coll.find_one_and_update(
            query, update, return_document=return_document, internal=_internal
        )
        if result is None:
            return None
        return cast(dict[str, Any], result)

    def find_one_and_replace(
        self,
        query: dict[str, Any],
        replacement: dict[str, Any],
        *,
        upsert: bool = False,
        return_document: str = "before",
        _internal: bool = False,
    ) -> dict[str, Any] | None:
        """Atomically replace one matching document."""
        result = self._rust_coll.find_one_and_replace(
            query,
            replacement,
            upsert=upsert,
            return_document=return_document,
            internal=_internal,
        )
        if result is None:
            return None
        return cast(dict[str, Any], result)

    def find_one_and_delete(
        self, query: dict[str, Any], *, _internal: bool = False
    ) -> dict[str, Any] | None:
        """Atomically delete and return one matching document."""
        result = self._rust_coll.find_one_and_delete(query, internal=_internal)
        if result is None:
            return None
        return cast(dict[str, Any], result)

    def explain(
        self, query: dict[str, Any] | None = None, *, execute: bool = False
    ) -> dict[str, Any]:
        """Query plan from the engine, plus a ``plan`` string for explain-style output.

        Delegates plan retrieval to Rust ``RedbLocalCollection.explain`` and
        adds a human-readable ``plan`` string.  The ``execute=True`` option
        (running the query and timing it) is Python-only — Rust ``explain``
        does not support it.
        """
        q = query or {}
        raw = dict(self._rust_coll.explain(q))
        raw["plan"] = _redb_explain_plan_kind(raw.get("execution_plan"), q)
        if execute:
            t0 = _time.monotonic()
            docs = self.find(q)
            elapsed_ms = int((_time.monotonic() - t0) * 1000)
            raw["executionStats"] = {
                "nReturned": len(docs),
                "executionTimeMillis": elapsed_ms,
            }
        return raw

    def count_documents(self, filter: dict[str, Any] | None = None) -> int:
        """Count documents matching filter."""
        query = filter or {}
        return self._rust_coll.count_documents(query)

    def update_one(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        *,
        _internal: bool = False,
    ) -> UpdateResult:
        """Update a single document."""
        result = self._rust_coll.update_one(filter, update, internal=_internal, upsert=upsert)
        return UpdateResult(
            result["matched_count"],
            result["modified_count"],
            result.get("upserted_id"),
        )

    def update_many(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        *,
        _internal: bool = False,
    ) -> UpdateResult:
        """Update multiple documents."""
        result = self._rust_coll.update_many(filter, update, internal=_internal, upsert=upsert)
        return UpdateResult(
            result["matched_count"],
            result["modified_count"],
            result.get("upserted_id"),
        )

    def delete_one(self, filter: dict[str, Any], *, _internal: bool = False) -> DeleteResult:
        """Delete a single document."""
        result = self._rust_coll.delete_one(filter, internal=_internal)
        return DeleteResult(result["deleted_count"])

    def delete_many(self, filter: dict[str, Any], *, _internal: bool = False) -> DeleteResult:
        """Delete multiple documents."""
        result = self._rust_coll.delete_many(filter, internal=_internal)
        return DeleteResult(result["deleted_count"])

    # Compatibility methods for Collection wrapper

    def update(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        multi: bool = True,
        upsert: bool = False,
        *,
        _internal: bool = False,
    ) -> UpdateResult:
        """Update documents (multi-compatible interface for Collection wrapper)."""
        if multi:
            return self.update_many(query, update, upsert=upsert, _internal=_internal)
        return self.update_one(query, update, upsert=upsert, _internal=_internal)

    def delete(
        self,
        query: dict[str, Any],
        multi: bool = True,
        *,
        _internal: bool = False,
    ) -> DeleteResult:
        """Delete documents (multi-compatible interface for Collection wrapper)."""
        if multi:
            return self.delete_many(query, _internal=_internal)
        return self.delete_one(query, _internal=_internal)

    def count(self, query: dict[str, Any] | None = None) -> int:
        """Count documents (compatibility alias for count_documents)."""
        return self.count_documents(query or {})

    # Index operations

    def create_index(
        self,
        keys: str | list[tuple[str, int | str]] | dict[str, Any],
        name: str | None = None,
        *,
        _internal: bool = False,
        **kwargs: Any,
    ) -> str:
        """Create an index (PyMongo-compatible *keys* and option shapes).

        ``_internal`` is accepted for API parity with CRUD methods (sync
        layer passes it to suppress oplog echo) but Rust ``create_index``
        does not yet support it — index creation always writes to the oplog.
        """
        del _internal  # not yet forwarded to Rust; see docstring
        if isinstance(keys, str):
            keys_doc: dict[str, Any] = {keys: 1}
        elif isinstance(keys, list):
            keys_doc = {}
            for item in keys:
                field, direction = item[0], item[1]
                keys_doc[field] = int(direction) if isinstance(direction, int | float) else 1
        else:
            keys_doc = dict(keys)
        opts_doc: dict[str, Any] = {
            "unique": bool(kwargs.get("unique", False)),
            "sparse": bool(kwargs.get("sparse", False)),
            "background": bool(kwargs.get("background", False)),
        }
        if name is not None:
            opts_doc["name"] = name
        expire = kwargs.get("expireAfterSeconds") or kwargs.get("expire_after_seconds")
        if expire is not None:
            opts_doc["expire_after_seconds"] = int(expire)
        pfe = kwargs.get("partialFilterExpression")
        if pfe is not None:
            opts_doc["partial_filter_expression"] = pfe
        collation = kwargs.get("collation")
        if collation is not None:
            opts_doc["collation"] = collation
            log.warning(
                "Collation options are stored but not enforced in key comparison; "
                "index '%s' will use binary ordering",
                name or "auto",
            )
        idx_type = kwargs.get("type") or kwargs.get("index_type")
        if idx_type is not None:
            opts_doc["index_type"] = idx_type
        weights = kwargs.get("weights")
        if weights is not None:
            opts_doc["text_options"] = {"weights": weights}
        vs = kwargs.get("vectorSearchOptions") or kwargs.get("vector_options")
        if vs is not None:
            opts_doc["vector_options"] = vs
        prefix_len = kwargs.get("prefixLength")
        if prefix_len is not None:
            opts_doc["prefix_options"] = {"prefix_length": int(prefix_len)}
        name_ret = self._rust_coll.create_index(keys_doc, opts_doc)
        self._ttl_reaper.maybe_start()
        return name_ret

    def drop_index(self, index_name: str) -> None:
        """Drop an index."""
        self._rust_coll.drop_index(index_name)

    def list_indexes(self) -> list[dict[str, Any]]:
        """List all indexes."""
        return self._rust_coll.list_indexes()

    def reap_expired(self) -> int:
        """Remove documents past TTL (``expireAfterSeconds`` on a DateTime field). Synchronous; call periodically if needed."""
        return int(self._rust_coll.reap_expired())

    def storage_stats(self) -> dict[str, Any]:
        """Return storage-level statistics (count, sizes, index info)."""
        return dict(self._rust_coll.storage_stats())

    def rebuild_all_indexes(self) -> int:
        """Drop and re-create every secondary index; return the number rebuilt."""
        return int(self._rust_coll.rebuild_all_indexes())

    def count_fast(self) -> int:
        """Fast document count (no filter)."""
        return self.count_documents({})

    def data_size_bytes(self) -> int:
        """Approximate total data size in bytes.

        Uses ``storage_stats()["dataSize"]`` (Rust-reported aggregate).
        Note: Rust ``RedbLocalCollection`` has its own ``data_size_bytes``
        that does a per-document BSON scan — different cost model, same
        semantic intent.
        """
        stats = self.storage_stats()
        return int(stats.get("dataSize", 0))

    def verify(self) -> dict[str, Any]:
        """Run integrity checks on the collection and its indexes.

        Delegates to Rust ``RedbLocalCollection.verify`` so that the
        ``MongoClient`` API path and the wire ``validate`` command produce
        identical results.
        """
        return dict(self._rust_coll.verify())

    def compact(self) -> None:
        """Compact the collection (no-op for redb -- auto-compacts on commit)."""
        pass

    def close(self) -> None:
        """Close the collection (no-op for redb)."""
        pass


__all__ = ["RedbClient", "RedbCollection", "RedbDB"]
