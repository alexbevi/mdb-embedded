from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .._types import Document, Filter
from ..query import compile_query
from .helpers import _from_bson

if TYPE_CHECKING:
    from .collection import LocalCollection


class StreamingCursor:
    """Lazy iterator over WiredTiger documents.

    Instead of materializing everything into a list, reads docs one at a
    time from the WiredTiger cursor.  The query planner is consulted so
    that index scans, PK lookups, and ``$or``-union plans all stream
    correctly—callers that only need the first *N* docs avoid touching
    the rest.

    Usage::

        for doc in collection.find_streaming(query):
            process(doc)
    """

    def __init__(self, collection: LocalCollection, query: Filter | None = None) -> None:
        self._collection = collection
        self._query = query or {}
        self._fn = compile_query(self._query) if self._query else None

    def __iter__(self) -> Any:
        coll = self._collection
        coll._rwlock.acquire_read()
        try:
            with coll._lock:
                plan = coll.planner.plan(self._query)

                if plan.plan_type == "pk_lookup":
                    yield from self._iter_pk_lookup(coll)
                elif plan.plan_type == "index_scan":
                    yield from self._iter_index_scan(coll, plan)
                elif plan.plan_type == "or_union" and plan.subplans:
                    yield from self._iter_or_union(coll, plan)
                else:
                    yield from self._iter_collection_scan(coll)
        finally:
            coll._rwlock.release_read()

    # -- plan-specific iterators (called while holding both locks) ------

    def _iter_pk_lookup(self, coll: LocalCollection) -> Any:
        raw_id = self._query.get("_id")
        pk_val = raw_id["$eq"] if isinstance(raw_id, dict) and "$eq" in raw_id else raw_id
        doc = coll._get_by_id_unlocked(pk_val)
        if doc:
            remaining = {k: v for k, v in self._query.items() if k != "_id"}
            if remaining:
                fn = compile_query(remaining)
                if fn(doc):
                    yield doc
            elif self._fn is None or self._fn(doc):
                yield doc

    def _iter_index_scan(self, coll: LocalCollection, plan: Any) -> Any:
        idx = plan.index_def
        if idx and idx.keys:
            leading_field = idx.keys[0][0]
            cond = self._query.get(leading_field)
            if isinstance(cond, dict) and "$in" in cond:
                ids = coll.planner.execute_in_scan(idx, cond["$in"], coll._active_session)
                yield from self._yield_from_ids(coll, ids)
                return
        ids = coll.planner.execute_index_scan(plan, coll._active_session, coll.table_uri)
        yield from self._yield_from_ids(coll, ids)

    def _iter_or_union(self, coll: LocalCollection, plan: Any) -> Any:
        seen_ids: set[str] = set()
        candidate_ids: list[str] = []

        for sub in plan.subplans or []:
            if sub.plan_type == "pk_lookup":
                for branch in self._query.get("$or", []):
                    if "_id" in branch and not isinstance(branch["_id"], dict):
                        sid = str(branch["_id"])
                        if sid not in seen_ids:
                            seen_ids.add(sid)
                            candidate_ids.append(sid)
            elif sub.plan_type == "index_scan":
                ids = coll.planner.execute_index_scan(sub, coll._active_session, coll.table_uri)
                for sid in ids:
                    if sid not in seen_ids:
                        seen_ids.add(sid)
                        candidate_ids.append(sid)

        yield from self._yield_from_ids(coll, candidate_ids)

    def _iter_collection_scan(self, coll: LocalCollection) -> Any:
        cursor = coll._active_session.open_cursor(coll.table_uri, None, None)
        try:
            while cursor.next() == 0:
                doc = _from_bson(cursor.get_value())
                if self._fn is None or self._fn(doc):
                    yield doc
        finally:
            cursor.close()

    def _yield_from_ids(self, coll: LocalCollection, ids: list[str]) -> Any:
        """Look up docs one at a time by _id and yield those passing the filter."""
        fn = compile_query(self._query)
        cursor = coll._active_session.open_cursor(coll.table_uri, None, None)
        try:
            for doc_id in ids:
                cursor.set_key(str(doc_id))
                if cursor.search() == 0:
                    doc: Document = _from_bson(cursor.get_value())
                    if fn(doc):
                        yield doc
        finally:
            cursor.close()
