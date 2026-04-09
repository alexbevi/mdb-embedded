"""
Redb-backed storage engine - Python wrapper for Rust RedbLocalClient.

This provides a LocalClient-compatible API backed by smongo-engine's redb backend,
allowing MongoClient to use redb instead of WiredTiger.
"""

from __future__ import annotations

import os
import time as _time
from collections.abc import Iterator
from typing import Any

from .._smongo_core import RedbLocalClient as _RedbLocalClient
from .._smongo_core import RedbLocalCollection as _RedbLocalCollection
from .._smongo_core import RedbLocalDB as _RedbLocalDB
from .helpers import log
from .results import DeleteResult, InsertResult, UpdateResult


def _is_pk_equality_filter(query: dict[str, Any]) -> bool:
    qid = query.get("_id")
    if qid is None:
        return False
    if isinstance(qid, dict) and "$eq" in qid:
        return True
    return not isinstance(qid, dict)


def _redb_explain_plan_kind(
    execution_plan: Any, query_filter: dict[str, Any] | None
) -> str:
    """Map engine ``execution_plan`` BSON shape to WiredTiger-style ``plan`` string."""
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
        self.durable = durable
        self._dbs: dict[str, RedbDB] = {}

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

    def get_collection(self, name: str) -> RedbCollection:
        """Return (and lazily create) the named collection."""
        if name not in self._collections:
            rust_coll = self._rust_db.collection(name)
            validator = self._validators.get(name)
            self._collections[name] = RedbCollection(
                rust_coll, self.name, name, db=self, validator=validator
            )
        return self._collections[name]

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
        self._rust_db.drop_collection(name)

    def list_collection_names(self) -> list[str]:
        """Return all collection names."""
        try:
            return self._rust_db.list_collection_names()
        except Exception as e:
            log.debug("list_collection_names failed: %s", e)
            return []

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

    @property
    def _oplog_w(self) -> Any:
        """Oplog writer bridge (``oplog_uri``, ``node_id``) for :class:`~smongo.sync.SyncManager`."""
        return self._rust_coll._oplog_w

    def get_oplog_reader(self) -> Any:
        """Return a reader with ``read_from(checkpoint, skip_internal=...)`` like WT :class:`~smongo.oplog.OplogReader`."""
        return self._rust_coll.get_oplog_reader()

    def get_oplog(self) -> list[dict[str, Any]]:
        """Return all oplog entries (including ``internal``), newest order by key scan."""
        reader = self.get_oplog_reader()
        pairs = reader.read_from(None, skip_internal=False)
        return [dict(entry) for _key, entry in pairs]

    def get_by_id(self, doc_id: Any) -> dict[str, Any] | None:
        """Fetch a document by ``_id`` (used by sync filters on updates)."""
        return self._rust_coll.get_by_id(doc_id)

    def watch(self, pipeline: list[Any] | None = None) -> Any:
        """Local change stream over the redb oplog hub."""
        return self._rust_coll.watch(pipeline)

    # CRUD operations - delegate directly to Rust

    def insert_one(
        self, document: dict[str, Any], *, _internal: bool = False
    ) -> InsertResult:
        """Insert a single document."""
        result = self._rust_coll.insert_one(document, internal=_internal)
        # Match LocalCollection / PyMongo: ``inserted_ids`` is always a sequence.
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
        result = self._rust_coll.insert_many(documents, internal=_internal)
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

    def find_streaming(
        self, query: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Iterate matches for *query* (materialized via :meth:`find`; lazy streaming TBD)."""
        return iter(self.find(query or {}))

    def explain(
        self, query: dict[str, Any] | None = None, *, execute: bool = False
    ) -> dict[str, Any]:
        """Query plan from the engine, plus a ``plan`` string aligned with :class:`~smongo.storage.collection.LocalCollection`."""
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
        result = self._rust_coll.update_one(
            filter, update, internal=_internal, upsert=upsert
        )
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
        result = self._rust_coll.update_many(
            filter, update, internal=_internal, upsert=upsert
        )
        return UpdateResult(
            result["matched_count"],
            result["modified_count"],
            result.get("upserted_id"),
        )

    def delete_one(
        self, filter: dict[str, Any], *, _internal: bool = False
    ) -> DeleteResult:
        """Delete a single document."""
        result = self._rust_coll.delete_one(filter, internal=_internal)
        return DeleteResult(result["deleted_count"])

    def delete_many(
        self, filter: dict[str, Any], *, _internal: bool = False
    ) -> DeleteResult:
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
            return self.update_many(
                query, update, upsert=upsert, _internal=_internal
            )
        return self.update_one(
            query, update, upsert=upsert, _internal=_internal
        )

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
        """Create an index (same *keys* shapes as :class:`~smongo.storage.collection.LocalCollection`)."""
        _ = _internal
        if isinstance(keys, str):
            keys_doc: dict[str, Any] = {keys: 1}
        elif isinstance(keys, list):
            keys_doc = {}
            for item in keys:
                field, direction = item[0], item[1]
                keys_doc[field] = int(direction) if isinstance(direction, (int, float)) else 1
        else:
            keys_doc = dict(keys)
        # Engine deserializes full IndexOptions (unique / sparse / background required).
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
        return self._rust_coll.create_index(keys_doc, opts_doc)

    def drop_index(self, index_name: str) -> None:
        """Drop an index."""
        self._rust_coll.drop_index(index_name)

    def list_indexes(self) -> list[dict[str, Any]]:
        """List all indexes."""
        return self._rust_coll.list_indexes()

    def close(self) -> None:
        """Close the collection (no-op for redb)."""
        pass


__all__ = ["RedbClient", "RedbDB", "RedbCollection"]
