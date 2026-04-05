"""
Index Engine -- WiredTiger B-Tree backed indexes with a query planner.

Each index is its own WiredTiger table. Keys are lexicographically sortable
encodings of field values + _id, so WiredTiger's natural B-Tree ordering gives
us O(log n) lookups, range scans, and ordered iteration.

Supports standard, unique, sparse, TTL, text, hashed, partial, and wildcard indexes.
"""

import json
from typing import Any

from smongo._smongo_core import (
    DuplicateKeyError,
    encode_index_key,
    encode_index_key_prefix,
)
from smongo._smongo_core import (
    invert_encoded as _invert_encoded,
)
from smongo._smongo_core import (
    rs_flatten_doc as _flatten_doc,
)
from smongo._smongo_core import (
    rs_hash_value as _hash_value,
)
from smongo._smongo_core import (
    rs_tokenize as _tokenize,
)
from smongo._smongo_core import (
    sortable_encode as _sortable_encode,
)

from ._compat import WTError as _WTError
from ._types import Document, Filter
from .query import compile_query, get_value

# ------------------------------------------------------------------
# Index definition
# ------------------------------------------------------------------


class IndexDef:
    """Metadata for a single index."""

    def __init__(
        self,
        name: str,
        keys: list[tuple[str, int | str]],
        unique: bool = False,
        sparse: bool = False,
        expire_after_seconds: int | None = None,
        index_type: str = "btree",
        partial_filter: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.keys = keys  # list of (field, direction_or_type)
        self.unique = unique
        self.sparse = sparse
        self.expire_after_seconds = expire_after_seconds
        self.index_type = index_type  # "btree" | "text" | "hashed" | "wildcard"
        self.partial_filter = partial_filter
        self.table_uri: str | None = None

    @property
    def fields(self) -> list[str]:
        return [f for f, _ in self.keys]

    @property
    def directions(self) -> list[int]:
        return [d if isinstance(d, int) else 1 for _, d in self.keys]

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "name": self.name,
            "keys": self.keys,
            "unique": self.unique,
            "sparse": self.sparse,
            "expireAfterSeconds": self.expire_after_seconds,
        }
        if self.index_type != "btree":
            d["type"] = self.index_type
        if self.partial_filter:
            d["partialFilterExpression"] = self.partial_filter
        return d


# ------------------------------------------------------------------
# IndexManager -- manages all indexes for a single collection
# ------------------------------------------------------------------


class IndexManager:
    """Creates, drops, and maintains WiredTiger-backed indexes for a collection."""

    def __init__(self, session: Any, db_name: str, coll_name: str) -> None:
        self.session = session
        self.db_name = db_name
        self.coll_name = coll_name
        self._indexes: dict[str, IndexDef] = {}

        self.meta_uri = f"table:__idxmeta_{db_name}_{coll_name}"
        session.create(self.meta_uri, "key_format=S,value_format=S")
        self._load_metadata()

    def _load_metadata(self) -> None:
        """Restore index definitions from the metadata table on startup."""
        cursor = self.session.open_cursor(self.meta_uri, None, None)
        while cursor.next() == 0:
            name = cursor.get_key()
            defn = json.loads(cursor.get_value())
            idx = IndexDef(
                name,
                [tuple(k) for k in defn["keys"]],
                defn.get("unique", False),
                defn.get("sparse", False),
                defn.get("expireAfterSeconds"),
                defn.get("type", "btree"),
                defn.get("partialFilterExpression"),
            )
            idx.table_uri = f"table:__idx_{self.db_name}_{self.coll_name}_{name}"
            self._indexes[name] = idx
        cursor.close()

    # -- public API ---------------------------------------------------

    def create_index(self, keys: str | list[tuple[str, int | str]], **kwargs: Any) -> str:
        """
        Create an index. Returns the index name.

        Supports:
        - Standard B-Tree: ``[("field", 1)]``
        - Text: ``[("field", "text")]``
        - Hashed: ``[("field", "hashed")]``
        - Wildcard: ``[("$**", 1)]``
        - Partial: ``partialFilterExpression={...}``
        """
        if isinstance(keys, str):
            keys = [(keys, 1)]

        idx_type = "btree"
        for _f, d in keys:
            if d == "text":
                idx_type = "text"
                break
            if d == "hashed":
                idx_type = "hashed"
                break
            if d in ("2dsphere", "2d"):
                raise NotImplementedError(
                    f"{d} indexes are planned but not yet implemented; "
                    "$geoNear aggregation works without an index. "
                    "See WHATSNEXT.md for the geospatial roadmap."
                )
            if _f == "$**":
                idx_type = "wildcard"
                break

        if idx_type == "btree":
            keys = [(f, int(d)) for f, d in keys]

        name: str = kwargs.get("name") or "_".join(f"{f}_{d}" for f, d in keys)
        if name in self._indexes:
            return name

        unique: bool = kwargs.get("unique", False)
        sparse: bool = kwargs.get("sparse", False)
        expire_after_seconds: int | None = kwargs.get("expireAfterSeconds")
        partial_filter: dict[str, Any] | None = kwargs.get("partialFilterExpression")

        table_uri = f"table:__idx_{self.db_name}_{self.coll_name}_{name}"
        self.session.create(table_uri, "key_format=S,value_format=S")

        defn: dict[str, Any] = {
            "keys": keys,
            "unique": unique,
            "sparse": sparse,
            "expireAfterSeconds": expire_after_seconds,
            "type": idx_type,
        }
        if partial_filter:
            defn["partialFilterExpression"] = partial_filter
        cursor = self.session.open_cursor(self.meta_uri, None, "overwrite=true")
        cursor[name] = json.dumps(defn)
        cursor.close()

        idx = IndexDef(name, keys, unique, sparse, expire_after_seconds, idx_type, partial_filter)
        idx.table_uri = table_uri
        self._indexes[name] = idx
        return name

    def drop_index(self, name: str) -> None:
        if name not in self._indexes:
            return
        idx = self._indexes.pop(name)
        self.session.drop(idx.table_uri)

        cursor = self.session.open_cursor(self.meta_uri, None, "overwrite=true")
        cursor.set_key(name)
        cursor.remove()
        cursor.close()

    def list_indexes(self) -> list[dict[str, Any]]:
        return [idx.to_dict() for idx in self._indexes.values()]

    def get_indexes(self) -> dict[str, IndexDef]:
        return dict(self._indexes)

    # -- index maintenance (called by storage on every write) ----------

    def add_doc(self, doc: Document) -> None:
        for idx in self._indexes.values():
            self._insert_entry(idx, doc)

    def remove_doc(self, doc: Document) -> None:
        for idx in self._indexes.values():
            self._delete_entry(idx, doc)

    def update_doc(self, old_doc: Document, new_doc: Document) -> None:
        for idx in self._indexes.values():
            needs_update = any(get_value(old_doc, f) != get_value(new_doc, f) for f in idx.fields)
            if needs_update:
                self._delete_entry(idx, old_doc)
                self._insert_entry(idx, new_doc)

    def rebuild_index(self, name: str, all_docs: list[Document]) -> None:
        """Drop and re-populate an index from the full document set."""
        idx = self._indexes.get(name)
        if not idx:
            return

        cursor = self.session.open_cursor(idx.table_uri, None, None)
        keys_to_remove: list[str] = []
        while cursor.next() == 0:
            keys_to_remove.append(cursor.get_key())
        cursor.close()

        if keys_to_remove:
            cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
            for key in keys_to_remove:
                cursor.set_key(key)
                try:
                    cursor.remove()
                except _WTError:
                    pass
            cursor.close()

        for doc in all_docs:
            self._insert_entry(idx, doc)

    # -- internal helpers ----------------------------------------------

    def _insert_entry(self, idx: IndexDef, doc: Document) -> None:
        if idx.partial_filter:
            fn = compile_query(idx.partial_filter)
            if not fn(doc):
                return

        if idx.index_type == "text":
            self._insert_text_entry(idx, doc)
            return
        if idx.index_type == "hashed":
            self._insert_hashed_entry(idx, doc)
            return
        if idx.index_type == "wildcard":
            self._insert_wildcard_entry(idx, doc)
            return

        field_values = [get_value(doc, f) for f in idx.fields]

        if idx.sparse and all(v is None for v in field_values):
            return

        key = encode_index_key(field_values, str(doc["_id"]), idx.directions)

        if idx.unique:
            prefix = encode_index_key_prefix(field_values, idx.directions)
            if self._has_duplicate(idx, prefix, str(doc["_id"])):
                raise DuplicateKeyError(f"E11000 duplicate key error index: {idx.name}")

        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        cursor[key] = str(doc["_id"])
        cursor.close()

    def _delete_entry(self, idx: IndexDef, doc: Document) -> None:
        if idx.partial_filter:
            fn = compile_query(idx.partial_filter)
            if not fn(doc):
                return

        if idx.index_type == "text":
            self._delete_text_entry(idx, doc)
            return
        if idx.index_type == "hashed":
            self._delete_hashed_entry(idx, doc)
            return
        if idx.index_type == "wildcard":
            self._delete_wildcard_entry(idx, doc)
            return

        field_values = [get_value(doc, f) for f in idx.fields]

        if idx.sparse and all(v is None for v in field_values):
            return

        key = encode_index_key(field_values, str(doc["_id"]), idx.directions)
        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        cursor.set_key(key)
        try:
            cursor.remove()
        except _WTError:
            pass
        cursor.close()

    # -- text index helpers -------------------------------------------

    def _insert_text_entry(self, idx: IndexDef, doc: Document) -> None:
        doc_id = str(doc["_id"])
        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        for field, _ in idx.keys:
            val = get_value(doc, field)
            if isinstance(val, str):
                for token in _tokenize(val):
                    key = f"{token}|{doc_id}"
                    cursor[key] = doc_id
        cursor.close()

    def _delete_text_entry(self, idx: IndexDef, doc: Document) -> None:
        doc_id = str(doc["_id"])
        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        for field, _ in idx.keys:
            val = get_value(doc, field)
            if isinstance(val, str):
                for token in _tokenize(val):
                    cursor.set_key(f"{token}|{doc_id}")
                    try:
                        cursor.remove()
                    except _WTError:
                        pass
        cursor.close()

    def text_search(self, idx_name: str, search_str: str) -> list[str]:
        """Return _id strings matching a ``$text`` search against a text index."""
        idx = self._indexes.get(idx_name)
        if not idx or idx.index_type != "text":
            return []
        assert idx.table_uri is not None
        tokens = _tokenize(search_str)
        if not tokens:
            return []
        id_sets: list[set[str]] = []
        for token in tokens:
            cursor = self.session.open_cursor(idx.table_uri, None, None)
            ids: set[str] = set()
            prefix = f"{token}|"
            cursor.set_key(prefix)
            try:
                exact = cursor.search_near()
            except _WTError:
                cursor.close()
                id_sets.append(ids)
                continue
            if exact < 0:
                if cursor.next() != 0:
                    cursor.close()
                    id_sets.append(ids)
                    continue
            while True:
                key = cursor.get_key()
                if not key.startswith(prefix):
                    break
                ids.add(cursor.get_value())
                if cursor.next() != 0:
                    break
            cursor.close()
            id_sets.append(ids)
        if not id_sets:
            return []
        result = id_sets[0]
        for s in id_sets[1:]:
            result &= s
        return list(result)

    # -- hashed index helpers -----------------------------------------

    def _insert_hashed_entry(self, idx: IndexDef, doc: Document) -> None:
        doc_id = str(doc["_id"])
        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        for field, _ in idx.keys:
            val = get_value(doc, field)
            h = _hash_value(val)
            key = f"{h}|{doc_id}"
            cursor[key] = doc_id
        cursor.close()

    def _delete_hashed_entry(self, idx: IndexDef, doc: Document) -> None:
        doc_id = str(doc["_id"])
        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        for field, _ in idx.keys:
            val = get_value(doc, field)
            h = _hash_value(val)
            cursor.set_key(f"{h}|{doc_id}")
            try:
                cursor.remove()
            except _WTError:
                pass
        cursor.close()

    def hashed_lookup(self, idx_name: str, value: Any) -> list[str]:
        """Equality lookup via hashed index. Returns matching _id strings."""
        idx = self._indexes.get(idx_name)
        if not idx or idx.index_type != "hashed":
            return []
        assert idx.table_uri is not None
        h = _hash_value(value)
        prefix = f"{h}|"
        ids: list[str] = []
        cursor = self.session.open_cursor(idx.table_uri, None, None)
        cursor.set_key(prefix)
        try:
            exact = cursor.search_near()
        except _WTError:
            cursor.close()
            return ids
        if exact < 0:
            if cursor.next() != 0:
                cursor.close()
                return ids
        while True:
            key = cursor.get_key()
            if not key.startswith(prefix):
                break
            ids.append(cursor.get_value())
            if cursor.next() != 0:
                break
        cursor.close()
        return ids

    # -- wildcard index helpers ---------------------------------------

    def _insert_wildcard_entry(self, idx: IndexDef, doc: Document) -> None:
        doc_id = str(doc["_id"])
        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        for path, val in _flatten_doc(doc):
            if path == "_id":
                continue
            encoded = _sortable_encode(val)
            key = f"{path}|{encoded}|{doc_id}"
            cursor[key] = doc_id
        cursor.close()

    def _delete_wildcard_entry(self, idx: IndexDef, doc: Document) -> None:
        doc_id = str(doc["_id"])
        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        for path, val in _flatten_doc(doc):
            if path == "_id":
                continue
            encoded = _sortable_encode(val)
            cursor.set_key(f"{path}|{encoded}|{doc_id}")
            try:
                cursor.remove()
            except _WTError:
                pass
        cursor.close()

    def wildcard_lookup(self, idx_name: str, field: str, value: Any) -> list[str]:
        """Equality lookup via wildcard index on a specific field."""
        idx = self._indexes.get(idx_name)
        if not idx or idx.index_type != "wildcard":
            return []
        assert idx.table_uri is not None
        encoded = _sortable_encode(value)
        prefix = f"{field}|{encoded}|"
        ids: list[str] = []
        cursor = self.session.open_cursor(idx.table_uri, None, None)
        cursor.set_key(prefix)
        try:
            exact = cursor.search_near()
        except _WTError:
            cursor.close()
            return ids
        if exact < 0:
            if cursor.next() != 0:
                cursor.close()
                return ids
        while True:
            key = cursor.get_key()
            if not key.startswith(prefix):
                break
            ids.append(cursor.get_value())
            if cursor.next() != 0:
                break
        cursor.close()
        return ids

    def _has_duplicate(self, idx: IndexDef, prefix: str, exclude_id: str) -> bool:
        cursor = self.session.open_cursor(idx.table_uri, None, None)
        cursor.set_key(prefix)
        try:
            exact = cursor.search_near()
        except _WTError:
            cursor.close()
            return False

        if exact < 0:
            if cursor.next() != 0:
                cursor.close()
                return False

        try:
            while True:
                key = cursor.get_key()
                if not key.startswith(prefix):
                    break
                doc_id = cursor.get_value()
                if doc_id != exclude_id:
                    cursor.close()
                    return True
                if cursor.next() != 0:
                    break
        except _WTError:
            pass

        cursor.close()
        return False


# ------------------------------------------------------------------
# Query planner
# ------------------------------------------------------------------


class QueryPlan:
    """Describes how a query will be executed."""

    def __init__(
        self,
        plan_type: str,
        index_name: str | None = None,
        bounds: Any = None,
        index_def: IndexDef | None = None,
        subplans: list["QueryPlan"] | None = None,
        rejected_plans: list[dict[str, Any]] | None = None,
    ) -> None:
        self.plan_type = plan_type  # "index_scan" | "pk_lookup" | "collection_scan" | "or_union"
        self.index_name = index_name
        self.bounds = bounds
        self.index_def = index_def
        self.subplans = subplans
        self.rejected_plans = rejected_plans
        self.execution_stats: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"plan": self.plan_type}
        if self.index_name:
            d["index"] = self.index_name
        if self.bounds:
            lower, upper = self.bounds
            d["indexBounds"] = {
                "lower": [
                    (seg[0], "inclusive" if seg[1] else "exclusive")
                    for seg in lower
                    if seg[0] is not None
                ],
                "upper": [
                    (seg[0], "inclusive" if seg[1] else "exclusive")
                    for seg in upper
                    if seg[0] is not None
                ],
            }
        if self.subplans:
            d["subplans"] = [sp.to_dict() for sp in self.subplans]
        if self.rejected_plans:
            d["rejectedPlans"] = self.rejected_plans
        if self.execution_stats:
            d["executionStats"] = self.execution_stats
        return d


class QueryPlanner:
    """Chooses the best execution strategy for a given query."""

    def __init__(self, index_manager: IndexManager) -> None:
        self.index_manager = index_manager

    def plan(self, query: Filter) -> QueryPlan:
        if not query:
            return QueryPlan("collection_scan")

        if "_id" in query:
            id_val = query["_id"]
            if not isinstance(id_val, dict):
                return QueryPlan("pk_lookup")
            if isinstance(id_val, dict) and "$eq" in id_val and len(id_val) == 1:
                return QueryPlan("pk_lookup")

        if "$or" in query:
            return self._plan_or(query)

        return self._plan_simple(query)

    def _plan_simple(self, query: Filter) -> QueryPlan:
        """Plan a query without top-level $or."""
        field_conditions: dict[str, Any] = {}
        for key, cond in query.items():
            if not key.startswith("$"):
                field_conditions[key] = cond

        if not field_conditions:
            return QueryPlan("collection_scan")

        best_plan: QueryPlan | None = None
        best_score = 0
        rejected: list[dict[str, Any]] = []

        for name, idx in self.index_manager.get_indexes().items():
            score, bounds = self._score_index(idx, field_conditions)
            if score > best_score:
                if best_plan:
                    rejected.append({"index": best_plan.index_name, "score": best_score})
                best_score = score
                best_plan = QueryPlan("index_scan", name, bounds, idx)
            elif score > 0:
                rejected.append({"index": name, "score": score})

        if best_plan:
            if rejected:
                best_plan.rejected_plans = rejected
            return best_plan
        return QueryPlan("collection_scan")

    def _plan_or(self, query: Filter) -> QueryPlan:
        """Plan a query containing $or by planning each branch independently."""
        branches: list[dict[str, Any]] = query["$or"]
        other_conditions = {k: v for k, v in query.items() if k != "$or"}

        subplans: list[QueryPlan] = []
        for branch in branches:
            merged = {**other_conditions, **branch}

            if "_id" in merged and not isinstance(merged["_id"], dict):
                subplans.append(QueryPlan("pk_lookup"))
            else:
                sub = self._plan_simple(merged)
                if sub.plan_type == "collection_scan":
                    return QueryPlan("collection_scan")
                subplans.append(sub)

        if not subplans:
            return QueryPlan("collection_scan")

        return QueryPlan("or_union", subplans=subplans)

    def _score_index(
        self, idx: IndexDef, field_conditions: dict[str, Any]
    ) -> tuple[int, tuple[list[tuple[str | None, bool]], list[tuple[str | None, bool]]]]:
        """
        Score an index against query conditions and compute WT-domain bounds.

        Returns (score, (lower_segments, upper_segments)) where each segment
        is (wt_encoded_value_or_None, inclusive). None means unbounded on
        that side. All encoding/inversion for direction is handled here so
        _build_bound_key just concatenates.
        """
        score = 0
        lower_segments: list[tuple[str | None, bool]] = []
        upper_segments: list[tuple[str | None, bool]] = []

        for field, direction in idx.keys:
            if field not in field_conditions:
                break

            cond = field_conditions[field]

            if not isinstance(cond, dict):
                # Equality
                score += 2
                encoded = _sortable_encode(cond)
                if direction == -1:
                    encoded = _invert_encoded(encoded)
                lower_segments.append((encoded, True))
                upper_segments.append((encoded, True))
            else:
                low_val: Any = None
                high_val: Any = None
                low_inc = False
                high_inc = False
                matched = False

                for op, val in cond.items():
                    if op in ("$gt", "$gte"):
                        low_val = val
                        low_inc = op == "$gte"
                        matched = True
                    elif op in ("$lt", "$lte"):
                        high_val = val
                        high_inc = op == "$lte"
                        matched = True
                    elif op == "$eq":
                        low_val = high_val = val
                        low_inc = high_inc = True
                        matched = True
                    elif op == "$in":
                        matched = True

                if not matched:
                    break

                score += 1

                if direction == 1:
                    wt_low = _sortable_encode(low_val) if low_val is not None else None
                    wt_high = _sortable_encode(high_val) if high_val is not None else None
                    lower_segments.append((wt_low, low_inc))
                    upper_segments.append((wt_high, high_inc))
                else:
                    # Descending: invert encoding and swap bounds
                    wt_low = (
                        _invert_encoded(_sortable_encode(high_val))
                        if high_val is not None
                        else None
                    )
                    wt_high = (
                        _invert_encoded(_sortable_encode(low_val)) if low_val is not None else None
                    )
                    lower_segments.append((wt_low, high_inc))
                    upper_segments.append((wt_high, low_inc))

                if low_val != high_val:
                    break

        return score, (lower_segments, upper_segments)

    def execute_index_scan(self, plan: QueryPlan, session: Any, table_uri: str) -> list[str]:
        """
        Run an index scan using WiredTiger cursor range operations.
        Returns a list of _id strings matching the bounds.
        """
        idx = plan.index_def
        lower_segments, upper_segments = plan.bounds

        low_key = self._build_bound_key(lower_segments, is_lower=True)
        high_key = self._build_bound_key(upper_segments, is_lower=False)

        assert idx is not None
        cursor = session.open_cursor(idx.table_uri, None, None)
        ids: list[str] = []

        if low_key:
            cursor.set_key(low_key)
            try:
                exact = cursor.search_near()
            except _WTError:
                cursor.close()
                return ids

            if exact < 0:
                if cursor.next() != 0:
                    cursor.close()
                    return ids
        else:
            if cursor.next() != 0:
                cursor.close()
                return ids

        while True:
            key = cursor.get_key()
            if high_key and key > high_key:
                break
            ids.append(cursor.get_value())
            if cursor.next() != 0:
                break

        cursor.close()
        return ids

    def execute_in_scan(
        self,
        idx: IndexDef,
        values: list[Any],
        session: Any,
    ) -> list[str]:
        """Multi-point index scan for ``$in`` queries -- one seek per value."""
        assert idx.table_uri is not None
        cursor = session.open_cursor(idx.table_uri, None, None)
        seen: set[str] = set()
        ids: list[str] = []

        for val in values:
            encoded = _sortable_encode(val)
            if idx.directions[0] == -1:
                encoded = _invert_encoded(encoded)
            prefix = encoded + "|"
            cursor.set_key(prefix)
            try:
                exact = cursor.search_near()
            except _WTError:
                continue
            if exact < 0:
                if cursor.next() != 0:
                    continue
            while True:
                key = cursor.get_key()
                if not key.startswith(prefix):
                    break
                doc_id = cursor.get_value()
                if doc_id not in seen:
                    seen.add(doc_id)
                    ids.append(doc_id)
                if cursor.next() != 0:
                    break

        cursor.close()
        return ids

    def _build_bound_key(
        self, segments: list[tuple[str | None, bool]], is_lower: bool
    ) -> str | None:
        """
        Concatenate pre-encoded WT-domain segments into a single key string.
        None segments are replaced with min/max sentinels.
        """
        if not segments:
            return None

        parts: list[str] = []
        for encoded, _inclusive in segments:
            if encoded is None:
                parts.append("" if is_lower else "\xff" * 20)
            else:
                parts.append(encoded)

        key = "|".join(parts)
        if is_lower:
            key += "|"
        else:
            key += "|" + "\xff" * 40

        return key
