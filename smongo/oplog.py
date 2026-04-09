"""
Oplog (Operations Log) -- records every mutation for sync and audit.

Enhanced format includes namespace, doc versioning, and an internal flag
to prevent echo loops during bidirectional sync.  Supports truncation
for bounded growth in long-running deployments.
"""

import logging
import threading
import time
import uuid
from typing import Any

from smongo._smongo_core import doc_checksum as _doc_checksum
from smongo._smongo_core import from_bson as _from_bson
from smongo._smongo_core import to_bson as _to_bson

from ._compat import StorageError as _StorageError
from ._types import Document, Pipeline

log = logging.getLogger("smongo.oplog")


class OplogHub:
    """Instance-scoped listener registry for oplog change notifications.

    One hub is created per ``LocalClient`` and shared by all collections under
    that engine, ensuring listeners are isolated per embedded client instance.
    """

    def __init__(self) -> None:
        self._listeners: list[ChangeStream] = []
        self._lock = threading.Lock()

    def register(self, listener: "ChangeStream") -> None:
        with self._lock:
            self._listeners.append(listener)

    def unregister(self, listener: "ChangeStream") -> None:
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def notify(self, entry: Document, source_ns: str | None = None) -> None:
        with self._lock:
            dead: list[ChangeStream] = []
            for listener in self._listeners:
                if listener.namespace and source_ns and listener.namespace != source_ns:
                    continue
                try:
                    listener._enqueue(entry)
                except (AttributeError, TypeError, RuntimeError, ValueError):
                    dead.append(listener)
            for d in dead:
                self._listeners.remove(d)


class OplogWriter:
    """Appends structured operations to the storage-backed oplog table."""

    def __init__(
        self,
        session: Any,
        oplog_uri: str,
        namespace: str,
        hub: OplogHub | None = None,
        node_id: str | None = None,
    ) -> None:
        self.session = session
        self.oplog_uri = oplog_uri
        self.namespace = namespace
        self._hub = hub
        self.node_id = node_id

    def log(
        self,
        op: str,
        doc_id: Any,
        payload: Document | None,
        *,
        version: int | None = None,
        internal: bool = False,
        changed_fields: list[str] | None = None,
    ) -> str:
        """
        Write an oplog entry.

        Args:
            op: Operation type (insert, update, delete, index_create, index_drop)
            doc_id: The _id of the affected document (or index name for index ops)
            payload: The document or update spec
            version: Incrementing doc version for conflict detection
            internal: If True, sync layer should skip this entry (echo prevention)
            changed_fields: List of field names modified (for field-level merge)
        """
        oplog_key = f"{time.time_ns():020d}-{uuid.uuid4()}"
        log_entry: Document = {
            "ts": time.time(),
            "ns": self.namespace,
            "op": op,
            "doc_id": doc_id,
            "payload": payload,
            "v": version,
            "checksum": _doc_checksum(payload) if op != "delete" else None,
            "internal": internal,
        }
        if self.node_id:
            log_entry["node_id"] = self.node_id
        if changed_fields:
            log_entry["changed_fields"] = changed_fields

        cursor = self.session.open_cursor(self.oplog_uri, None, "overwrite=true")
        cursor[oplog_key] = _to_bson(log_entry)
        cursor.close()

        self._notify_listeners(log_entry)
        return oplog_key

    # -- compaction ----------------------------------------------------

    def truncate_before(self, key: str) -> int:
        """Delete all oplog entries with keys lexicographically before *key*."""
        cursor = self.session.open_cursor(self.oplog_uri, None, "overwrite=true")
        to_remove: list[str] = []
        while cursor.next() == 0:
            k: str = cursor.get_key()
            if k >= key:
                break
            to_remove.append(k)
        cursor.close()

        if to_remove:
            cursor = self.session.open_cursor(self.oplog_uri, None, "overwrite=true")
            for k in to_remove:
                cursor.set_key(k)
                try:
                    cursor.remove()
                except _StorageError as exc:
                    log.debug("Oplog truncate_before: failed to remove key %s: %s", k, exc)
            cursor.close()
        return len(to_remove)

    def truncate_count(self, max_entries: int) -> int:
        """Keep only the last *max_entries* entries, deleting the oldest."""
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        keys: list[str] = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        cursor.close()

        excess = len(keys) - max_entries
        if excess <= 0:
            return 0

        cursor = self.session.open_cursor(self.oplog_uri, None, "overwrite=true")
        for k in keys[:excess]:
            cursor.set_key(k)
            try:
                cursor.remove()
            except _StorageError as exc:
                log.debug("Oplog truncate_count: failed to remove key %s: %s", k, exc)
        cursor.close()
        return excess

    # -- listeners -----------------------------------------------------

    def _notify_listeners(self, entry: Document) -> None:
        if self._hub is not None:
            self._hub.notify(entry, source_ns=entry.get("ns"))


class OplogReader:
    """Reads oplog entries, optionally from a checkpoint forward."""

    def __init__(self, session: Any, oplog_uri: str) -> None:
        self.session = session
        self.oplog_uri = oplog_uri

    def read_all(self) -> list[Document]:
        """Return all oplog entries in chronological order."""
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        logs: list[Document] = []
        while cursor.next() == 0:
            logs.append(dict(_from_bson(cursor.get_value())))
        cursor.close()
        return logs

    def read_from(
        self, checkpoint_key: str | None = None, *, skip_internal: bool = True
    ) -> list[tuple[str, Document]]:
        """
        Read oplog entries after the given checkpoint key.
        Returns list of (key, entry) tuples.

        Uses ``search_near()`` to seek directly to the checkpoint position
        instead of scanning from the beginning -- O(log n + k) where *k* is
        the number of new entries since the checkpoint.
        """
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        entries: list[tuple[str, Document]] = []

        if checkpoint_key is not None:
            cursor.set_key(checkpoint_key)
            try:
                exact = cursor.search_near()
            except _StorageError:
                cursor.close()
                return entries
            if exact == 0:
                # Landed exactly on the checkpoint; skip past it.
                if cursor.next() != 0:
                    cursor.close()
                    return entries
            elif exact < 0:
                # Checkpoint was compacted; positioned on the largest smaller key.
                # Advance to the first entry after where the checkpoint was.
                if cursor.next() != 0:
                    cursor.close()
                    return entries
            # exact > 0: cursor is already past the checkpoint, start collecting.
        else:
            # No checkpoint -- read everything from the beginning.
            if cursor.next() != 0:
                cursor.close()
                return entries

        while True:
            key: str = cursor.get_key()
            entry: Document = dict(_from_bson(cursor.get_value()))
            if not (skip_internal and entry.get("internal")):
                entries.append((key, entry))
            if cursor.next() != 0:
                break

        cursor.close()
        return entries

    def latest_key(self) -> str | None:
        """Return the key of the most recent oplog entry, or None."""
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        last_key: str | None = None
        while cursor.next() == 0:
            last_key = cursor.get_key()
        cursor.close()
        return last_key

    def count(self) -> int:
        """Return the total number of oplog entries."""
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        n = 0
        while cursor.next() == 0:
            n += 1
        cursor.close()
        return n

    def oldest_key(self) -> str | None:
        """Return the key of the oldest oplog entry, or None."""
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        if cursor.next() == 0:
            key: str = cursor.get_key()
            cursor.close()
            return key
        cursor.close()
        return None


_OP_TO_CHANGE_TYPE: dict[str, str] = {
    "insert": "insert",
    "update": "update",
    "delete": "delete",
    "replace": "replace",
}


class ChangeStream:
    """
    Local change stream -- tails the oplog and yields MongoDB-format change events.
    Supports pipeline filtering and is usable as a context manager and iterator.
    """

    def __init__(
        self,
        namespace: str | None = None,
        pipeline: Pipeline | None = None,
        hub: OplogHub | None = None,
    ) -> None:
        self.namespace = namespace
        self._pipeline = pipeline
        self._filter: Any = None
        self._queue: list[Document] = []
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._closed = False
        self._hub = hub

        if pipeline:
            from .query import compile_query

            for stage in pipeline:
                if "$match" in stage:
                    self._filter = compile_query(stage["$match"])
                    break

        if self._hub is not None:
            self._hub.register(self)

    def _enqueue(self, oplog_entry: Document) -> None:
        op = oplog_entry.get("op", "")
        change_type = _OP_TO_CHANGE_TYPE.get(op)
        if not change_type:
            return

        ns_parts = oplog_entry.get("ns", ".").split(".", 1)
        event: Document = {
            "operationType": change_type,
            "ns": {"db": ns_parts[0], "coll": ns_parts[1] if len(ns_parts) > 1 else ""},
            "documentKey": {"_id": oplog_entry.get("doc_id")},
            "_ts": oplog_entry.get("ts"),
        }

        if change_type in ("insert", "update", "replace"):
            event["fullDocument"] = oplog_entry.get("payload")

        if self._filter and not self._filter(event):
            return

        with self._lock:
            self._queue.append(event)
            self._event.set()

    def __enter__(self) -> "ChangeStream":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def __iter__(self) -> "ChangeStream":
        return self

    def __next__(self) -> Document:
        while not self._closed:
            with self._lock:
                if self._queue:
                    return self._queue.pop(0)
            self._event.wait(timeout=1.0)
            self._event.clear()
        raise StopIteration

    def try_next(self) -> Document | None:
        """Non-blocking: return the next event or None."""
        with self._lock:
            return self._queue.pop(0) if self._queue else None

    def close(self) -> None:
        self._closed = True
        self._event.set()
        if self._hub is not None:
            self._hub.unregister(self)
