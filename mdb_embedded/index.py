"""
Index Engine -- WiredTiger B-Tree backed indexes with a query planner.

Each index is its own WiredTiger table. Keys are lexicographically sortable
encodings of field values + _id, so WiredTiger's natural B-Tree ordering gives
us O(log n) lookups, range scans, and ordered iteration.
"""

import json
import struct

from .query import get_value

_HEX_INVERT = str.maketrans("0123456789abcdef", "fedcba9876543210")


class DuplicateKeyError(Exception):
    pass


# ------------------------------------------------------------------
# Sortable key encoding
# ------------------------------------------------------------------
# Type prefixes ensure cross-type ordering: None < Number < String < Bool
# Within numbers, IEEE 754 double with sign-bit flip gives correct order.
# Everything is hex-encoded so keys are valid C strings (no null bytes).

def _sortable_encode(value):
    """Encode a single value into a lexicographically sortable hex string."""
    if value is None:
        return "00"
    if isinstance(value, bool):
        return "30" if not value else "31"
    if isinstance(value, (int, float)):
        packed = struct.pack(">d", float(value))
        b = bytearray(packed)
        if b[0] & 0x80:  # negative: invert all bits
            b = bytearray(~x & 0xFF for x in b)
        else:  # non-negative: flip sign bit
            b[0] ^= 0x80
        return "1" + b.hex()
    if isinstance(value, str):
        return "2" + value.encode("utf-8").hex()
    return "2" + json.dumps(value, sort_keys=True).encode("utf-8").hex()


def _invert_encoded(s):
    """Invert a hex-encoded key to reverse sort order (for descending indexes)."""
    return s.translate(_HEX_INVERT)


def encode_index_key(field_values, doc_id, directions):
    """
    Build a composite B-Tree key from field values, doc _id, and sort directions.
    The key is a pipe-separated sequence of encoded segments.
    """
    parts = []
    for val, direction in zip(field_values, directions):
        encoded = _sortable_encode(val)
        if direction == -1:
            encoded = _invert_encoded(encoded)
        parts.append(encoded)
    parts.append(doc_id)
    return "|".join(parts)


def encode_index_key_prefix(field_values, directions):
    """Encode just the field values (no _id), for prefix matching."""
    parts = []
    for val, direction in zip(field_values, directions):
        encoded = _sortable_encode(val)
        if direction == -1:
            encoded = _invert_encoded(encoded)
        parts.append(encoded)
    return "|".join(parts) + "|"


# ------------------------------------------------------------------
# Index definition
# ------------------------------------------------------------------

class IndexDef:
    """Metadata for a single index."""

    def __init__(self, name, keys, unique=False, sparse=False):
        self.name = name
        self.keys = keys  # list of (field, direction)
        self.unique = unique
        self.sparse = sparse
        self.table_uri = None

    @property
    def fields(self):
        return [f for f, _ in self.keys]

    @property
    def directions(self):
        return [d for _, d in self.keys]

    def to_dict(self):
        return {
            "name": self.name,
            "keys": self.keys,
            "unique": self.unique,
            "sparse": self.sparse,
        }


# ------------------------------------------------------------------
# IndexManager -- manages all indexes for a single collection
# ------------------------------------------------------------------

class IndexManager:
    """Creates, drops, and maintains WiredTiger-backed indexes for a collection."""

    def __init__(self, session, db_name, coll_name):
        self.session = session
        self.db_name = db_name
        self.coll_name = coll_name
        self._indexes = {}

        self.meta_uri = f"table:__idxmeta_{db_name}_{coll_name}"
        session.create(self.meta_uri, "key_format=S,value_format=S")
        self._load_metadata()

    def _load_metadata(self):
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
            )
            idx.table_uri = f"table:__idx_{self.db_name}_{self.coll_name}_{name}"
            self._indexes[name] = idx
        cursor.close()

    # -- public API ---------------------------------------------------

    def create_index(self, keys, **kwargs):
        """
        Create an index. Returns the index name.

        Args:
            keys: list of (field, direction) tuples, or a single field string
            name: optional explicit name
            unique: enforce uniqueness (default False)
            sparse: skip docs missing the field (default False)
        """
        if isinstance(keys, str):
            keys = [(keys, 1)]
        keys = [(f, int(d)) for f, d in keys]

        name = kwargs.get("name") or "_".join(f"{f}_{d}" for f, d in keys)
        if name in self._indexes:
            return name

        unique = kwargs.get("unique", False)
        sparse = kwargs.get("sparse", False)

        table_uri = f"table:__idx_{self.db_name}_{self.coll_name}_{name}"
        self.session.create(table_uri, "key_format=S,value_format=S")

        defn = {"keys": keys, "unique": unique, "sparse": sparse}
        cursor = self.session.open_cursor(self.meta_uri, None, "overwrite=true")
        cursor[name] = json.dumps(defn)
        cursor.close()

        idx = IndexDef(name, keys, unique, sparse)
        idx.table_uri = table_uri
        self._indexes[name] = idx
        return name

    def drop_index(self, name):
        if name not in self._indexes:
            return
        idx = self._indexes.pop(name)
        self.session.drop(idx.table_uri)

        cursor = self.session.open_cursor(self.meta_uri, None, "overwrite=true")
        cursor.set_key(name)
        cursor.remove()
        cursor.close()

    def list_indexes(self):
        return [idx.to_dict() for idx in self._indexes.values()]

    def get_indexes(self):
        return dict(self._indexes)

    # -- index maintenance (called by storage on every write) ----------

    def add_doc(self, doc):
        for idx in self._indexes.values():
            self._insert_entry(idx, doc)

    def remove_doc(self, doc):
        for idx in self._indexes.values():
            self._delete_entry(idx, doc)

    def update_doc(self, old_doc, new_doc):
        for idx in self._indexes.values():
            needs_update = any(
                get_value(old_doc, f) != get_value(new_doc, f) for f in idx.fields
            )
            if needs_update:
                self._delete_entry(idx, old_doc)
                self._insert_entry(idx, new_doc)

    def rebuild_index(self, name, all_docs):
        """Drop and re-populate an index from the full document set."""
        idx = self._indexes.get(name)
        if not idx:
            return

        # Collect all existing keys first, then remove them
        cursor = self.session.open_cursor(idx.table_uri, None, None)
        keys_to_remove = []
        while cursor.next() == 0:
            keys_to_remove.append(cursor.get_key())
        cursor.close()

        if keys_to_remove:
            cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
            for key in keys_to_remove:
                cursor.set_key(key)
                try:
                    cursor.remove()
                except Exception:
                    pass
            cursor.close()

        for doc in all_docs:
            self._insert_entry(idx, doc)

    # -- internal helpers ----------------------------------------------

    def _insert_entry(self, idx, doc):
        field_values = [get_value(doc, f) for f in idx.fields]

        if idx.sparse and all(v is None for v in field_values):
            return

        key = encode_index_key(field_values, str(doc["_id"]), idx.directions)

        if idx.unique:
            prefix = encode_index_key_prefix(field_values, idx.directions)
            if self._has_duplicate(idx, prefix, str(doc["_id"])):
                raise DuplicateKeyError(
                    f"E11000 duplicate key error index: {idx.name}"
                )

        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        cursor[key] = str(doc["_id"])
        cursor.close()

    def _delete_entry(self, idx, doc):
        field_values = [get_value(doc, f) for f in idx.fields]

        if idx.sparse and all(v is None for v in field_values):
            return

        key = encode_index_key(field_values, str(doc["_id"]), idx.directions)
        cursor = self.session.open_cursor(idx.table_uri, None, "overwrite=true")
        cursor.set_key(key)
        try:
            cursor.remove()
        except Exception:
            pass
        cursor.close()

    def _has_duplicate(self, idx, prefix, exclude_id):
        cursor = self.session.open_cursor(idx.table_uri, None, None)
        cursor.set_key(prefix)
        try:
            exact = cursor.search_near()
        except Exception:
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
        except Exception:
            pass

        cursor.close()
        return False


# ------------------------------------------------------------------
# Query planner
# ------------------------------------------------------------------

class QueryPlan:
    """Describes how a query will be executed."""

    def __init__(self, plan_type, index_name=None, bounds=None, index_def=None):
        self.plan_type = plan_type  # "index_scan", "pk_lookup", "collection_scan"
        self.index_name = index_name
        self.bounds = bounds
        self.index_def = index_def

    def to_dict(self):
        d = {"plan": self.plan_type}
        if self.index_name:
            d["index"] = self.index_name
        return d


class QueryPlanner:
    """Chooses the best execution strategy for a given query."""

    def __init__(self, index_manager):
        self.index_manager = index_manager

    def plan(self, query):
        if not query:
            return QueryPlan("collection_scan")

        # Direct _id lookup is always fastest
        if "_id" in query and not isinstance(query["_id"], dict):
            return QueryPlan("pk_lookup")

        # Queries with top-level $or can't easily use a single index
        if "$or" in query:
            return QueryPlan("collection_scan")

        field_conditions = {}
        for key, cond in query.items():
            if not key.startswith("$"):
                field_conditions[key] = cond

        if not field_conditions:
            return QueryPlan("collection_scan")

        best_plan = None
        best_score = 0

        for name, idx in self.index_manager.get_indexes().items():
            score, bounds = self._score_index(idx, field_conditions)
            if score > best_score:
                best_score = score
                best_plan = QueryPlan("index_scan", name, bounds, idx)

        return best_plan if best_plan else QueryPlan("collection_scan")

    def _score_index(self, idx, field_conditions):
        """
        Score an index against query conditions and compute WT-domain bounds.

        Returns (score, (lower_segments, upper_segments)) where each segment
        is (wt_encoded_value_or_None, inclusive). None means unbounded on
        that side. All encoding/inversion for direction is handled here so
        _build_bound_key just concatenates.
        """
        score = 0
        lower_segments = []  # (wt_encoded | None, inclusive)
        upper_segments = []

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
                low_val = None
                high_val = None
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
                        break

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
                    wt_low = _invert_encoded(_sortable_encode(high_val)) if high_val is not None else None
                    wt_high = _invert_encoded(_sortable_encode(low_val)) if low_val is not None else None
                    lower_segments.append((wt_low, high_inc))
                    upper_segments.append((wt_high, low_inc))

                if low_val != high_val:
                    break

        return score, (lower_segments, upper_segments)

    def execute_index_scan(self, plan, session, table_uri):
        """
        Run an index scan using WiredTiger cursor range operations.
        Returns a list of _id strings matching the bounds.
        """
        idx = plan.index_def
        lower_segments, upper_segments = plan.bounds

        low_key = self._build_bound_key(lower_segments, is_lower=True)
        high_key = self._build_bound_key(upper_segments, is_lower=False)

        cursor = session.open_cursor(idx.table_uri, None, None)
        ids = []

        if low_key:
            cursor.set_key(low_key)
            try:
                exact = cursor.search_near()
            except Exception:
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

    def _build_bound_key(self, segments, is_lower):
        """
        Concatenate pre-encoded WT-domain segments into a single key string.
        None segments are replaced with min/max sentinels.
        """
        if not segments:
            return None

        parts = []
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
