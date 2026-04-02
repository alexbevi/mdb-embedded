from __future__ import annotations

import os
import threading
from typing import Any

from .._compat import WTError as _WTError
from .._compat import wt
from ..oplog import OplogHub
from .helpers import log


class LocalClient:
    """Top-level WiredTiger connection manager."""

    def __init__(self, db_path: str, *, durable: bool = True) -> None:
        if not wt:
            raise ImportError("wiredtiger required for local embedded mode")
        os.makedirs(db_path, exist_ok=True)
        config = "create"
        if durable:
            try:
                import snappy as _snappy  # noqa: F401

                config += ",log=(enabled=true,compressor=snappy)"
            except ImportError:
                config += ",log=(enabled=true)"
        self.conn: Any = wt.wiredtiger_open(db_path, config)
        self.durable = durable
        self._dbs: dict[str, LocalDB] = {}
        self.oplog_hub = OplogHub()

    def get_db(self, name: str) -> LocalDB:
        if name not in self._dbs:
            self._dbs[name] = LocalDB(self.conn, name, oplog_hub=self.oplog_hub)
        return self._dbs[name]

    def checkpoint(self) -> None:
        """Force a WiredTiger checkpoint (flush committed data to disk)."""
        session = self.conn.open_session()
        try:
            session.checkpoint()
        finally:
            session.close()

    def close(self) -> None:
        """Close all collections and the WiredTiger connection."""
        for db in self._dbs.values():
            db.close()
        try:
            self.conn.close()
        except _WTError:
            pass

    def __enter__(self) -> LocalClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class LocalDB:
    """Namespace container for collections belonging to a single logical database."""

    def __init__(self, conn: Any, name: str, oplog_hub: OplogHub | None = None) -> None:
        self.conn = conn
        self.name = name
        self._collections: dict[str, LocalCollection] = {}
        self._validators: dict[str, dict[str, Any] | None] = {}
        self._lock = threading.Lock()
        self._oplog_hub = oplog_hub

    def get_collection(self, name: str) -> LocalCollection:
        """Return (and lazily create) the named :class:`LocalCollection`."""
        with self._lock:
            if name not in self._collections:
                validator = self._validators.get(name)
                self._collections[name] = LocalCollection(
                    self.conn,
                    self.name,
                    name,
                    db=self,
                    validator=validator,
                    oplog_hub=self._oplog_hub,
                )
            return self._collections[name]

    def create_collection(
        self, name: str, validator: dict[str, Any] | None = None, **kwargs: Any
    ) -> LocalCollection:
        """Create a collection, optionally attaching a ``$jsonSchema`` *validator*."""
        schema: dict[str, Any] | None = None
        if validator:
            schema = validator.get("$jsonSchema", validator)
        with self._lock:
            self._validators[name] = schema
        coll = self.get_collection(name)
        if schema:
            coll._validator = schema
        return coll

    _INTERNAL_TABLE_PREFIXES = ("__oplog_", "__idx_", "__idxmeta_", "__sync_")

    def drop_collection(self, name: str) -> None:
        """Fully drop a collection: data, indexes, oplog, TTL reaper, and WT tables."""
        with self._lock:
            coll = self._collections.get(name)
            if coll:
                coll._ttl_reaper.stop()

                uris_to_drop: list[str] = []
                for idx in list(coll.index_mgr.list_indexes()):
                    idx_def = coll.index_mgr._indexes.get(idx["name"])
                    if idx_def and idx_def.table_uri is not None:
                        uris_to_drop.append(idx_def.table_uri)
                uris_to_drop.extend([coll.index_mgr.meta_uri, coll.oplog_uri, coll.table_uri])

                try:
                    coll.session.close()
                except _WTError:
                    pass

                drop_session = self.conn.open_session()
                drop_session.checkpoint()
                for uri in uris_to_drop:
                    try:
                        drop_session.drop(uri, "force")
                    except _WTError:
                        log.debug("drop_collection: WT drop failed for %s", uri)
                drop_session.close()

            self._collections.pop(name, None)
            self._validators.pop(name, None)

    def list_collection_names(self) -> list[str]:
        """Return all collection names, scanning both WT catalog and in-memory registry."""
        names: set[str] = set()
        try:
            session = self.conn.open_session()
            cursor = session.open_cursor("metadata:", None, None)
            prefix = f"table:{self.name}_"
            while cursor.next() == 0:
                uri: str = cursor.get_key()
                if not uri.startswith(prefix):
                    continue
                if any(
                    uri.startswith(f"table:{tag}{self.name}_")
                    for tag in self._INTERNAL_TABLE_PREFIXES
                ):
                    continue
                coll_name = uri[len(prefix) :]
                if coll_name:
                    names.add(coll_name)
            cursor.close()
            session.close()
        except (_WTError, RuntimeError, OSError, KeyError) as exc:
            log.debug("list_collection_names metadata scan failed: %s", exc)
        with self._lock:
            names.update(self._collections.keys())
        return sorted(names)

    def close(self) -> None:
        """Close all owned collections."""
        with self._lock:
            for coll in self._collections.values():
                coll.close()


from .collection import LocalCollection
