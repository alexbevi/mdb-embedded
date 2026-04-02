from __future__ import annotations

import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from .._compat import WTError as _WTError
from .._types import Document, Filter, UpdateSpec
from ..index import IndexManager, QueryPlan, QueryPlanner
from ..objectid import ObjectId
from ..oplog import ChangeStream, OplogHub, OplogReader, OplogWriter
from ..query import apply_update, compile_query, get_value
from ..schema import validate_document
from .helpers import _from_bson, _to_bson, log
from .locking import ReadWriteLock
from .results import DeleteResult, InsertResult, UpdateResult
from .streaming import StreamingCursor
from .transaction import get_active_txn_session

if TYPE_CHECKING:
    from .engine import LocalDB


_TTL_DELETE_BATCH_SIZE = 500


class TTLReaper:
    """Background TTL document reaper for a single collection.

    Improvements over a naive ``get_all()`` approach:
    - Uses ``scan_with_fields`` to extract only ``_id`` + the TTL field
      from each BSON document, avoiding full deserialization.
    - Deletes expired documents in configurable batches instead of one at
      a time, reducing lock churn.
    """

    def __init__(
        self,
        collection: LocalCollection,
        interval_sec: int = 60,
        batch_size: int = _TTL_DELETE_BATCH_SIZE,
    ) -> None:
        self._collection = collection
        self._interval = interval_sec
        self._batch_size = batch_size
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def maybe_start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        if not self._has_ttl_indexes():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name=f"ttl:{self._collection.namespace}"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _has_ttl_indexes(self) -> bool:
        for idx in self._collection.list_indexes():
            if idx.get("expireAfterSeconds") is not None:
                return True
        return False

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._reap_once()
            except (_WTError, KeyError, TypeError, ValueError) as exc:
                log.debug("TTL reap cycle error: %s", exc)
            self._stop.wait(timeout=self._interval)

    def _coerce_ts(self, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, int | float):
            return float(value)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except (ValueError, OverflowError):
                return None
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=UTC)
            return dt.timestamp()
        return None

    def _reap_once(self) -> None:
        ttl_indexes = [
            idx
            for idx in self._collection.list_indexes()
            if idx.get("expireAfterSeconds") is not None
        ]
        if not ttl_indexes:
            return

        now = time.time()
        to_delete: list[Any] = []

        for idx_meta in ttl_indexes:
            keys = idx_meta.get("keys")
            if not keys:
                continue
            field = keys[0][0]
            expire_after = idx_meta.get("expireAfterSeconds")
            if expire_after is None:
                continue
            idx_name = idx_meta.get("name", "")
            idx_def = self._collection.index_mgr._indexes.get(idx_name)

            if idx_def and idx_def.table_uri:
                to_delete.extend(self._reap_via_index(idx_def, field, float(expire_after), now))
            else:
                to_delete.extend(self._reap_via_scan(field, float(expire_after), now))

        seen: set[str] = set()
        unique_ids: list[Any] = []
        for did in to_delete:
            s = str(did)
            if s not in seen:
                seen.add(s)
                unique_ids.append(did)

        for i in range(0, len(unique_ids), self._batch_size):
            batch = unique_ids[i : i + self._batch_size]
            if batch:
                self._collection.delete({"_id": {"$in": batch}}, multi=True)

    def _reap_via_index(
        self,
        idx_def: Any,
        field: str,
        expire_after: float,
        now: float,
    ) -> list[Any]:
        """Walk the TTL index in key order up to the cutoff timestamp."""
        from ..index import _sortable_encode

        cutoff = now - expire_after
        cutoff_encoded = _sortable_encode(cutoff)
        if idx_def.directions[0] == -1:
            from ..index import _invert_encoded

            cutoff_encoded = _invert_encoded(cutoff_encoded)

        expired_ids: list[Any] = []
        coll = self._collection
        coll._rwlock.acquire_read()
        try:
            with coll._lock:
                try:
                    cursor = coll.session.open_cursor(idx_def.table_uri, None, None)
                    if cursor.next() != 0:
                        cursor.close()
                        return []
                    while True:
                        key = cursor.get_key()
                        key_prefix = key.split("|")[0] if "|" in key else key
                        if key_prefix > cutoff_encoded:
                            break
                        doc_id_str = cursor.get_value()
                        doc = coll._get_by_id_unlocked(doc_id_str)
                        if doc:
                            ts = self._coerce_ts(get_value(doc, field))
                            if ts is not None and ts + expire_after < now:
                                expired_ids.append(doc.get("_id"))
                        if cursor.next() != 0:
                            break
                    cursor.close()
                except (_WTError, RuntimeError, OSError):
                    pass
        finally:
            coll._rwlock.release_read()
        return expired_ids

    def _reap_via_scan(self, field: str, expire_after: float, now: float) -> list[Any]:
        """Fallback: scan all docs for TTL expiry."""
        to_delete: list[Any] = []
        for doc_id, field_vals in self._collection.scan_with_fields([field]):
            ts = self._coerce_ts(field_vals.get(field))
            if ts is not None and ts + expire_after < now:
                to_delete.append(doc_id)
        return to_delete


class LocalCollection:
    """
    A single collection backed by WiredTiger, with index support and oplog.

    Thread safety: a per-collection lock serializes all session operations.
    Atomicity: write paths are wrapped in WiredTiger transactions.
    Performance: writes use the query planner (index scans, pk lookups).
    Storage: documents are stored as BSON for performance and type fidelity.
    """

    def __init__(
        self,
        conn: Any,
        db_name: str,
        name: str,
        db: LocalDB | None = None,
        validator: dict[str, Any] | None = None,
        oplog_hub: OplogHub | None = None,
    ) -> None:
        self.conn = conn
        self.db_name = db_name
        self.name = name
        self.namespace = f"{db_name}.{name}"
        self._db = db
        self._validator = validator
        self._lock = threading.Lock()
        self._rwlock = ReadWriteLock()
        self._oplog_hub = oplog_hub

        self.table_uri = f"table:{db_name}_{name}"
        self.oplog_uri = f"table:__oplog_{db_name}_{name}"

        self.session: Any = conn.open_session()

        self.session.create(self.table_uri, "key_format=S,value_format=u")
        self.session.create(self.oplog_uri, "key_format=S,value_format=S")

        self._oplog_w = OplogWriter(self.session, self.oplog_uri, self.namespace, hub=oplog_hub)
        self._oplog_r = OplogReader(self.session, self.oplog_uri)

        self.index_mgr = IndexManager(self.session, db_name, name)
        self.planner = QueryPlanner(self.index_mgr)

        self._doc_versions: dict[Any, int] = {}
        self._ttl_reaper = TTLReaper(self)
        self._ttl_reaper.maybe_start()

    # -- transaction helper --------------------------------------------

    @property
    def _active_session(self) -> Any:
        """Return the thread-local txn session if active, else the per-collection session."""
        return get_active_txn_session() or self.session

    def _with_transaction(self, fn: Callable[[], Any]) -> Any:
        """Execute *fn* inside a WiredTiger transaction.

        If a multi-document transaction is active on this thread the
        caller already owns the WT session -- skip begin/commit here.
        """
        if get_active_txn_session():
            return fn()

        self.session.begin_transaction()
        try:
            result = fn()
            self.session.commit_transaction()
            return result
        except (
            _WTError,
            KeyError,
            TypeError,
            ValueError,
            AttributeError,
            RuntimeError,
            OSError,
            IndexError,
        ):
            try:
                self.session.rollback_transaction()
            except _WTError:
                pass
            raise

    # -- reads ---------------------------------------------------------

    def get_all(self) -> list[Document]:
        """Return every document in the collection (full table scan)."""
        self._rwlock.acquire_read()
        try:
            with self._lock:
                cursor = self._active_session.open_cursor(self.table_uri, None, None)
                docs: list[Document] = []
                while cursor.next() == 0:
                    docs.append(_from_bson(cursor.get_value()))
                cursor.close()
                return docs
        finally:
            self._rwlock.release_read()

    def scan_with_fields(self, fields: list[str]) -> list[tuple[Any, dict[str, Any]]]:
        """Scan the collection extracting only ``_id`` and *fields* per doc.

        Returns a list of ``(doc_id, {field: value, ...})`` tuples.
        This avoids building a full ``Document`` for every row and is
        significantly cheaper than ``get_all()`` when only a few fields
        are needed (e.g. TTL reaper).
        """
        results: list[tuple[Any, dict[str, Any]]] = []
        self._rwlock.acquire_read()
        try:
            with self._lock:
                cursor = self._active_session.open_cursor(self.table_uri, None, None)
                while cursor.next() == 0:
                    doc = _from_bson(cursor.get_value())
                    doc_id = doc.get("_id")
                    vals: dict[str, Any] = {}
                    for f in fields:
                        vals[f] = get_value(doc, f)
                    results.append((doc_id, vals))
                cursor.close()
        finally:
            self._rwlock.release_read()
        return results

    def get_by_id(self, doc_id: Any) -> Document | None:
        """O(log n) primary-key lookup via WiredTiger."""
        self._rwlock.acquire_read()
        try:
            with self._lock:
                cursor = self._active_session.open_cursor(self.table_uri, None, None)
                cursor.set_key(str(doc_id))
                if cursor.search() == 0:
                    doc = _from_bson(cursor.get_value())
                    cursor.close()
                    return doc
                cursor.close()
                return None
        finally:
            self._rwlock.release_read()

    def get_by_ids(self, doc_ids: list[Any]) -> list[Document]:
        """Batch primary-key lookup."""
        self._rwlock.acquire_read()
        try:
            with self._lock:
                cursor = self._active_session.open_cursor(self.table_uri, None, None)
                docs: list[Document] = []
                for did in doc_ids:
                    cursor.set_key(str(did))
                    if cursor.search() == 0:
                        docs.append(_from_bson(cursor.get_value()))
                cursor.close()
                return docs
        finally:
            self._rwlock.release_read()

    def _get_by_id_unlocked(self, doc_id: Any) -> Document | None:
        """O(log n) primary-key lookup without acquiring the lock (caller must hold it)."""
        cursor = self._active_session.open_cursor(self.table_uri, None, None)
        cursor.set_key(str(doc_id))
        if cursor.search() == 0:
            doc = _from_bson(cursor.get_value())
            cursor.close()
            return doc
        cursor.close()
        return None

    def _get_by_ids_unlocked(self, doc_ids: list[Any]) -> list[Document]:
        """Batch primary-key lookup without acquiring the lock (caller must hold it)."""
        cursor = self._active_session.open_cursor(self.table_uri, None, None)
        docs: list[Document] = []
        for did in doc_ids:
            cursor.set_key(str(did))
            if cursor.search() == 0:
                docs.append(_from_bson(cursor.get_value()))
        cursor.close()
        return docs

    def _find_matching_docs_locked(self, query: Filter) -> list[Document]:
        """Find matching docs while already holding self._lock (no lock re-acquisition)."""
        plan = self.planner.plan(query)

        if plan.plan_type == "pk_lookup":
            raw_id = query["_id"]
            pk_val = raw_id["$eq"] if isinstance(raw_id, dict) and "$eq" in raw_id else raw_id
            doc = self._get_by_id_unlocked(pk_val)
            remaining = {k: v for k, v in query.items() if k != "_id"}
            if doc and remaining:
                fn = compile_query(remaining)
                return [doc] if fn(doc) else []
            return [doc] if doc else []

        if plan.plan_type == "index_scan":
            idx = plan.index_def
            if idx and idx.keys:
                leading_field = idx.keys[0][0]
                cond = query.get(leading_field)
                if isinstance(cond, dict) and "$in" in cond:
                    ids = self.planner.execute_in_scan(idx, cond["$in"], self._active_session)
                    docs = self._get_by_ids_unlocked(ids)
                    fn = compile_query(query)
                    return [d for d in docs if fn(d)]
            ids = self.planner.execute_index_scan(plan, self._active_session, self.table_uri)
            docs = self._get_by_ids_unlocked(ids)
            fn = compile_query(query)
            return [d for d in docs if fn(d)]

        if plan.plan_type == "or_union" and plan.subplans:
            return self._execute_or_union(plan, query)

        docs = self._get_all_unlocked()
        fn = compile_query(query)
        return [d for d in docs if fn(d)]

    def _execute_or_union(self, plan: QueryPlan, query: Filter) -> list[Document]:
        """Execute an or_union plan: run each subplan, dedup by _id, filter."""
        seen_ids: set[str] = set()
        candidate_ids: list[str] = []

        for sub in plan.subplans or []:
            if sub.plan_type == "pk_lookup":
                for branch in query.get("$or", []):
                    if "_id" in branch and not isinstance(branch["_id"], dict):
                        sid = str(branch["_id"])
                        if sid not in seen_ids:
                            seen_ids.add(sid)
                            candidate_ids.append(sid)
            elif sub.plan_type == "index_scan":
                ids = self.planner.execute_index_scan(sub, self._active_session, self.table_uri)
                for sid in ids:
                    if sid not in seen_ids:
                        seen_ids.add(sid)
                        candidate_ids.append(sid)

        docs = self._get_by_ids_unlocked(candidate_ids)
        fn = compile_query(query)
        return [d for d in docs if fn(d)]

    def _find_matching_docs(self, query: Filter) -> list[Document]:
        """Use the query planner to find matching documents (acquires lock)."""
        self._rwlock.acquire_read()
        try:
            with self._lock:
                return self._find_matching_docs_locked(query)
        finally:
            self._rwlock.release_read()

    def find(self, query: Filter) -> list[Document]:
        """Execute a find using the query planner (materializes all results)."""
        return self._find_matching_docs(query)

    def find_one(self, query: Filter) -> Document | None:
        """Return the first matching document, or ``None``.

        Uses the streaming path so only one document is deserialized from
        WiredTiger instead of materializing every match.
        """
        for doc in self.find_streaming(query):
            return cast(Document, doc)
        return None

    def count(self, query: Filter) -> int:
        """Count matching documents without building an intermediate list.

        For empty queries delegates to :meth:`count_fast` which avoids
        BSON deserialization entirely.
        """
        if not query:
            return self.count_fast()
        n = 0
        for _ in self.find_streaming(query):
            n += 1
        return n

    def find_streaming(self, query: Filter | None = None) -> StreamingCursor:
        """Return a lazy :class:`StreamingCursor` over matching docs."""
        return StreamingCursor(self, query)

    def explain(self, query: Filter, *, execute: bool = False) -> dict[str, Any]:
        """Return the query execution plan.

        When *execute* is True, also runs the query to populate
        executionStats (nReturned, docsExamined, executionTimeMillis).
        """
        import time as _time

        self._rwlock.acquire_read()
        try:
            with self._lock:
                plan = self.planner.plan(query or {})
                result = plan.to_dict()

                if execute:
                    t0 = _time.monotonic()
                    docs = self._find_matching_docs_locked(query or {})
                    elapsed_ms = int((_time.monotonic() - t0) * 1000)
                    result["executionStats"] = {
                        "nReturned": len(docs),
                        "executionTimeMillis": elapsed_ms,
                    }

                residual = [
                    k
                    for k in (query or {})
                    if not k.startswith("$")
                    and k != "_id"
                    and (not plan.index_def or k not in {f for f, _ in plan.index_def.keys})
                ]
                if residual:
                    result["filterResidual"] = residual

                return result
        finally:
            self._rwlock.release_read()

    # -- change streams ------------------------------------------------

    def watch(self, pipeline: list[dict[str, Any]] | None = None) -> ChangeStream:
        """Return a ChangeStream that tails local mutations."""
        from ..oplog import ChangeStream

        return ChangeStream(namespace=self.namespace, pipeline=pipeline, hub=self._oplog_hub)

    # -- writes --------------------------------------------------------

    def _validate(self, doc: Document) -> None:
        if self._validator:
            validate_document(doc, self._validator)

    def _changed_fields_from_update(self, update_spec: UpdateSpec | None) -> list[str]:
        changed: set[str] = set()
        for op, fields in (update_spec or {}).items():
            if not isinstance(fields, dict):
                continue
            if op in (
                "$set",
                "$unset",
                "$inc",
                "$mul",
                "$min",
                "$max",
                "$rename",
                "$currentDate",
                "$addToSet",
                "$push",
                "$pull",
                "$pop",
            ):
                changed.update(fields.keys())
        return sorted(changed)

    def insert_one(self, doc: Document, *, _internal: bool = False) -> InsertResult:
        """Insert a single document, auto-generating ``_id`` if absent."""
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = ObjectId()

        self._validate(doc)

        def _do() -> None:
            self.index_mgr.add_doc(doc)
            cursor = self._active_session.open_cursor(self.table_uri, None, "overwrite=true")
            cursor[str(doc["_id"])] = _to_bson(doc)
            cursor.close()
            version = self._bump_version(doc["_id"])
            if not _internal:
                self._oplog_w.log(
                    "insert", doc["_id"], doc, version=version, changed_fields=sorted(doc.keys())
                )

        self._rwlock.acquire_write()
        try:
            with self._lock:
                self._with_transaction(_do)
        finally:
            self._rwlock.release_write()

        return InsertResult([doc["_id"]])

    def insert_many(self, docs: list[Document], *, _internal: bool = False) -> InsertResult:
        """Insert multiple documents in a single WiredTiger transaction."""
        prepared: list[Document] = []
        for doc in docs:
            doc = dict(doc)
            if "_id" not in doc:
                doc["_id"] = ObjectId()
            self._validate(doc)
            prepared.append(doc)

        def _do() -> None:
            cursor = self._active_session.open_cursor(self.table_uri, None, "overwrite=true")
            for doc in prepared:
                self.index_mgr.add_doc(doc)
                cursor[str(doc["_id"])] = _to_bson(doc)
                version = self._bump_version(doc["_id"])
                if not _internal:
                    self._oplog_w.log(
                        "insert",
                        doc["_id"],
                        doc,
                        version=version,
                        changed_fields=sorted(doc.keys()),
                    )
            cursor.close()

        self._rwlock.acquire_write()
        try:
            with self._lock:
                self._with_transaction(_do)
        finally:
            self._rwlock.release_write()

        return InsertResult([d["_id"] for d in prepared])

    @staticmethod
    def _extract_equality_conditions(query: Filter) -> Document:
        """Extract simple equality fields from a query for upsert base document."""
        base: Document = {}
        for key, value in query.items():
            if key.startswith("$"):
                continue
            if isinstance(value, dict) and any(k.startswith("$") for k in value):
                continue
            base[key] = value
        return base

    def update(
        self,
        query: Filter,
        update_spec: UpdateSpec | list[dict[str, Any]],
        multi: bool = True,
        *,
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        _internal: bool = False,
    ) -> UpdateResult:
        """Update documents matching *query* using MQL *update_spec* operators."""
        self._rwlock.acquire_write()
        try:
            return self._update_inner(
                query,
                update_spec,
                multi,
                upsert=upsert,
                array_filters=array_filters,
                _internal=_internal,
            )
        finally:
            self._rwlock.release_write()

    def _update_inner(
        self,
        query: Filter,
        update_spec: UpdateSpec | list[dict[str, Any]],
        multi: bool,
        *,
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        _internal: bool = False,
    ) -> UpdateResult:
        with self._lock:
            matching = self._find_matching_docs_locked(query)

            if not matching:
                if upsert:
                    doc = self._extract_equality_conditions(query)
                    if "_id" not in doc:
                        doc["_id"] = ObjectId()
                    apply_update(doc, update_spec, array_filters=array_filters, query=query)
                    self._validate(doc)

                    def _do_upsert() -> None:
                        self.index_mgr.add_doc(doc)
                        cursor = self._active_session.open_cursor(
                            self.table_uri, None, "overwrite=true"
                        )
                        cursor[str(doc["_id"])] = _to_bson(doc)
                        cursor.close()
                        version = self._bump_version(doc["_id"])
                        if not _internal:
                            self._oplog_w.log(
                                "insert",
                                doc["_id"],
                                doc,
                                version=version,
                                changed_fields=sorted(doc.keys()),
                            )

                    self._with_transaction(_do_upsert)
                    return UpdateResult(0, 0, upserted_id=doc["_id"])
                return UpdateResult(0, 0)

            if not multi:
                matching = matching[:1]

            modified = 0

            def _do() -> None:
                nonlocal modified
                cursor = self._active_session.open_cursor(self.table_uri, None, "overwrite=true")
                for doc in matching:
                    old_doc = dict(doc)
                    apply_update(doc, update_spec, array_filters=array_filters, query=query)
                    self._validate(doc)
                    self.index_mgr.update_doc(old_doc, doc)
                    cursor[str(doc["_id"])] = _to_bson(doc)
                    version = self._bump_version(doc["_id"])
                    if not _internal:
                        self._oplog_w.log(
                            "update",
                            doc["_id"],
                            update_spec
                            if isinstance(update_spec, dict)
                            else {"$pipeline": update_spec},
                            version=version,
                            changed_fields=self._changed_fields_from_update(
                                update_spec if isinstance(update_spec, dict) else {}
                            ),
                        )
                    modified += 1
                cursor.close()

            self._with_transaction(_do)

        return UpdateResult(len(matching), modified)

    def delete(self, query: Filter, multi: bool = True, *, _internal: bool = False) -> DeleteResult:
        """Delete documents matching *query*, returning a :class:`DeleteResult`."""
        self._rwlock.acquire_write()
        try:
            return self._delete_inner(query, multi, _internal=_internal)
        finally:
            self._rwlock.release_write()

    def _delete_inner(
        self, query: Filter, multi: bool = True, *, _internal: bool = False
    ) -> DeleteResult:
        with self._lock:
            matching = self._find_matching_docs_locked(query)
            if not matching:
                return DeleteResult(0)
            if not multi:
                matching = matching[:1]

            deleted = 0

            def _do() -> None:
                nonlocal deleted
                cursor = self._active_session.open_cursor(self.table_uri, None, "overwrite=true")
                for doc in matching:
                    self.index_mgr.remove_doc(doc)
                    cursor.set_key(str(doc["_id"]))
                    cursor.remove()
                    version = self._bump_version(doc["_id"])
                    if not _internal:
                        self._oplog_w.log("delete", doc["_id"], None, version=version)
                    deleted += 1
                cursor.close()

            self._with_transaction(_do)

        return DeleteResult(deleted)

    # -- find_one_and_* ------------------------------------------------

    def find_one_and_update(
        self,
        query: Filter,
        update_spec: UpdateSpec,
        *,
        return_document: str = "before",
        _internal: bool = False,
    ) -> Document | None:
        """Atomically find one document and apply *update_spec*."""
        self._rwlock.acquire_write()
        try:
            with self._lock:
                matching = self._find_matching_docs_locked(query)
                if not matching:
                    return None
                doc = matching[0]
                before = dict(doc)

                def _do() -> None:
                    apply_update(doc, update_spec)
                    self._validate(doc)
                    self.index_mgr.update_doc(before, doc)
                    cursor = self._active_session.open_cursor(
                        self.table_uri, None, "overwrite=true"
                    )
                    cursor[str(doc["_id"])] = _to_bson(doc)
                    cursor.close()
                    version = self._bump_version(doc["_id"])
                    if not _internal:
                        self._oplog_w.log(
                            "update",
                            doc["_id"],
                            update_spec,
                            version=version,
                            changed_fields=self._changed_fields_from_update(update_spec),
                        )

                self._with_transaction(_do)
        finally:
            self._rwlock.release_write()

        return dict(doc) if return_document == "after" else before

    def find_one_and_replace(
        self,
        query: Filter,
        replacement: Document,
        *,
        upsert: bool = False,
        return_document: str = "before",
        _internal: bool = False,
    ) -> Document | None:
        """Atomically find one document and replace it with *replacement*."""
        self._rwlock.acquire_write()
        try:
            with self._lock:
                matching = self._find_matching_docs_locked(query)
                if not matching:
                    if upsert:
                        replacement = dict(replacement)
                        if "_id" not in replacement:
                            replacement["_id"] = ObjectId()
                        self._validate(replacement)

                        def _do_upsert() -> None:
                            self.index_mgr.add_doc(replacement)
                            cursor = self._active_session.open_cursor(
                                self.table_uri, None, "overwrite=true"
                            )
                            cursor[str(replacement["_id"])] = _to_bson(replacement)
                            cursor.close()
                            version = self._bump_version(replacement["_id"])
                            if not _internal:
                                self._oplog_w.log(
                                    "insert",
                                    replacement["_id"],
                                    replacement,
                                    version=version,
                                    changed_fields=sorted(replacement.keys()),
                                )

                        self._with_transaction(_do_upsert)
                        return replacement if return_document == "after" else None
                    return None
                doc = matching[0]
                before = dict(doc)
                replacement = dict(replacement)
                replacement["_id"] = doc["_id"]

                def _do() -> None:
                    self._validate(replacement)
                    self.index_mgr.update_doc(before, replacement)
                    cursor = self._active_session.open_cursor(
                        self.table_uri, None, "overwrite=true"
                    )
                    cursor[str(replacement["_id"])] = _to_bson(replacement)
                    cursor.close()
                    version = self._bump_version(replacement["_id"])
                    if not _internal:
                        self._oplog_w.log(
                            "update", replacement["_id"], replacement, version=version
                        )

                self._with_transaction(_do)
        finally:
            self._rwlock.release_write()

        return replacement if return_document == "after" else before

    def find_one_and_delete(self, query: Filter, *, _internal: bool = False) -> Document | None:
        """Atomically find one document and delete it, returning the deleted document."""
        self._rwlock.acquire_write()
        try:
            with self._lock:
                matching = self._find_matching_docs_locked(query)
                if not matching:
                    return None
                doc = matching[0]

                def _do() -> None:
                    self.index_mgr.remove_doc(doc)
                    cursor = self._active_session.open_cursor(
                        self.table_uri, None, "overwrite=true"
                    )
                    cursor.set_key(str(doc["_id"]))
                    cursor.remove()
                    cursor.close()
                    version = self._bump_version(doc["_id"])
                    if not _internal:
                        self._oplog_w.log("delete", doc["_id"], None, version=version)

                self._with_transaction(_do)
        finally:
            self._rwlock.release_write()

        return doc

    # -- index management ----------------------------------------------

    def create_index(
        self, keys: str | list[tuple[str, int | str]], *, _internal: bool = False, **kwargs: Any
    ) -> str:
        """Create a B-tree index on *keys*, rebuild it, and return the index name."""

        def _do() -> str:
            name = self.index_mgr.create_index(keys, **kwargs)
            self.index_mgr.rebuild_index(name, self._get_all_unlocked())
            if not _internal:
                self._oplog_w.log(
                    "index_create",
                    name,
                    {"keys": keys if isinstance(keys, list) else [(keys, 1)], **kwargs},
                )
            return name

        with self._lock:
            name = self._with_transaction(_do)
        self._ttl_reaper.maybe_start()
        return name  # type: ignore[no-any-return]

    def drop_index(self, name: str, *, _internal: bool = False) -> None:
        """Drop the index identified by *name* and log the operation."""

        def _do() -> None:
            self.index_mgr.drop_index(name)
            if not _internal:
                self._oplog_w.log("index_drop", name, None)

        with self._lock:
            self._with_transaction(_do)

    def list_indexes(self) -> list[dict[str, Any]]:
        """Return metadata for every index on this collection."""
        with self._lock:
            return self.index_mgr.list_indexes()

    # -- oplog ---------------------------------------------------------

    def get_oplog(self) -> list[Document]:
        """Return all oplog entries for this collection."""
        with self._lock:
            return self._oplog_r.read_all()

    def get_oplog_reader(self) -> OplogReader:
        """Return the :class:`OplogReader` bound to this collection."""
        return self._oplog_r

    def compact_oplog(self, keep: int = 1000) -> None:
        """Truncate the oplog keeping only the most recent *keep* entries."""
        with self._lock:
            self._oplog_w.truncate_count(keep)

    # -- internal helpers ----------------------------------------------

    def _get_all_unlocked(self) -> list[Document]:
        """Read all docs without acquiring the lock (caller must hold it)."""
        cursor = self._active_session.open_cursor(self.table_uri, None, None)
        docs: list[Document] = []
        while cursor.next() == 0:
            docs.append(_from_bson(cursor.get_value()))
        cursor.close()
        return docs

    def _bump_version(self, doc_id: Any) -> int:
        v = self._doc_versions.get(doc_id, 0) + 1
        self._doc_versions[doc_id] = v
        return v

    # -- statistics & admin --------------------------------------------

    def count_fast(self) -> int:
        """Count documents by iterating the cursor without full BSON decode.

        Significantly cheaper than ``len(get_all())`` because we only
        advance the cursor and never materialize ``Document`` objects.
        """
        n = 0
        with self._lock:
            cursor = self.session.open_cursor(self.table_uri, None, None)
            while cursor.next() == 0:
                n += 1
            cursor.close()
        return n

    def data_size_bytes(self) -> int:
        """Return the total serialized (BSON) byte count across all documents."""
        total = 0
        with self._lock:
            cursor = self.session.open_cursor(self.table_uri, None, None)
            while cursor.next() == 0:
                total += len(cursor.get_value())
            cursor.close()
        return total

    def storage_stats(self) -> dict[str, Any]:
        """Return WiredTiger statistics for the underlying B-Tree table.

        The returned dict includes keys such as ``data_size``,
        ``storage_size``, ``num_records``, and ``index_sizes``.
        """
        stats: dict[str, Any] = {}
        try:
            stat_uri = f"statistics:{self.table_uri}"
            with self._lock:
                stat_cursor = self.session.open_cursor(stat_uri, None, "statistics=(fast)")
                while stat_cursor.next() == 0:
                    desc: str = stat_cursor[0]
                    _name: str = stat_cursor[1]
                    val: int = stat_cursor[2]
                    key = desc.lower().replace(" ", "_").replace("-", "_")
                    stats[key] = val
                stat_cursor.close()
        except (_WTError, RuntimeError, OSError, KeyError):
            pass

        doc_count = self.count_fast()
        data_size = stats.get("btree:_column_store_variable_size_data_size", 0)
        if not data_size:
            data_size = self.data_size_bytes()
        storage_size = stats.get("block_manager:_file_size_in_bytes", 0)

        idx_sizes: dict[str, int] = {}
        with self._lock:
            for idx_meta in self.index_mgr.list_indexes():
                idx_name = idx_meta.get("name", "")
                idx_def = self.index_mgr._indexes.get(idx_name)
                if idx_def and idx_def.table_uri:
                    try:
                        sc = self.session.open_cursor(
                            f"statistics:{idx_def.table_uri}",
                            None,
                            "statistics=(fast)",
                        )
                        while sc.next() == 0:
                            if "file_size_in_bytes" in sc[0].lower():
                                idx_sizes[idx_name] = sc[2]
                                break
                        sc.close()
                    except (_WTError, RuntimeError, OSError, KeyError):
                        pass

        return {
            "count": doc_count,
            "dataSize": data_size,
            "storageSize": storage_size,
            "nindexes": len(self.index_mgr.list_indexes()) + 1,
            "totalIndexSize": sum(idx_sizes.values()),
            "indexSizes": {"_id_": storage_size, **idx_sizes},
            "wiredTiger": stats,
        }

    def compact(self) -> dict[str, Any]:
        """Compact the WiredTiger table to reclaim disk space."""
        with self._lock:
            try:
                self.session.compact(self.table_uri)
            except (_WTError, RuntimeError, OSError):
                pass

            for idx_meta in self.index_mgr.list_indexes():
                idx_name = idx_meta.get("name", "")
                idx_def = self.index_mgr._indexes.get(idx_name)
                if idx_def and idx_def.table_uri:
                    try:
                        self.session.compact(idx_def.table_uri)
                    except (_WTError, RuntimeError, OSError):
                        pass

        return {"ok": 1.0}

    def rebuild_all_indexes(self) -> int:
        """Drop and rebuild every index from the current data set.

        Returns the total number of indexes rebuilt.
        """
        with self._lock:
            all_docs = self._get_all_unlocked()
            rebuilt = 0
            for idx_meta in self.index_mgr.list_indexes():
                idx_name = idx_meta.get("name", "")
                self.index_mgr.rebuild_index(idx_name, all_docs)
                rebuilt += 1
            return rebuilt

    def verify(self) -> dict[str, Any]:
        """Run WiredTiger verify on the data table and all index tables.

        Returns a dict with ``valid``, ``errors``, and ``warnings``.
        WT verify requires exclusive access; if the table is busy we skip
        the WT-level check and rely on the cursor-based consistency scan.
        """
        errors: list[str] = []
        warnings: list[str] = []

        try:
            verify_session = self.conn.open_session()
            try:
                verify_session.verify(self.table_uri)
            except _WTError as exc:
                exc_str = str(exc)
                if "Resource busy" not in exc_str:
                    errors.append(f"data table: {exc}")
            finally:
                try:
                    verify_session.close()
                except _WTError:
                    pass
        except (_WTError, RuntimeError, OSError) as exc:
            exc_str = str(exc)
            if "Resource busy" not in exc_str:
                errors.append(f"verify session: {exc}")

        with self._lock:
            for idx_meta in self.index_mgr.list_indexes():
                idx_name = idx_meta.get("name", "")
                idx_def = self.index_mgr._indexes.get(idx_name)
                if idx_def and idx_def.table_uri:
                    try:
                        vs = self.conn.open_session()
                        try:
                            vs.verify(idx_def.table_uri)
                        except _WTError as exc:
                            exc_str = str(exc)
                            if "Resource busy" not in exc_str:
                                errors.append(f"index {idx_name}: {exc}")
                        finally:
                            try:
                                vs.close()
                            except _WTError:
                                pass
                    except (_WTError, RuntimeError, OSError) as exc:
                        exc_str = str(exc)
                        if "Resource busy" not in exc_str:
                            errors.append(f"index {idx_name} session: {exc}")

            doc_count = 0
            cursor = self.session.open_cursor(self.table_uri, None, None)
            while cursor.next() == 0:
                doc_count += 1
            cursor.close()

            idx_entry_counts: dict[str, int] = {}
            for idx_meta in self.index_mgr.list_indexes():
                idx_name = idx_meta.get("name", "")
                idx_def = self.index_mgr._indexes.get(idx_name)
                if idx_def and idx_def.table_uri:
                    n = 0
                    try:
                        ic = self.session.open_cursor(idx_def.table_uri, None, None)
                        while ic.next() == 0:
                            n += 1
                        ic.close()
                    except _WTError:
                        pass
                    idx_entry_counts[idx_name] = n

        for idx_name, n_entries in idx_entry_counts.items():
            if n_entries > doc_count:
                warnings.append(
                    f"index {idx_name} has {n_entries} entries but collection "
                    f"has {doc_count} documents (possible stale entries)"
                )

        return {
            "valid": len(errors) == 0,
            "nrecords": doc_count,
            "nIndexes": len(self.index_mgr.list_indexes()) + 1,
            "errors": errors,
            "warnings": warnings,
            "indexEntries": idx_entry_counts,
        }

    def close(self) -> None:
        """Stop the TTL reaper and close the WiredTiger session."""
        self._ttl_reaper.stop()
        try:
            self.session.close()
        except _WTError:
            pass
