"""Conflict resolution primitives: vector clocks, CRDT helpers, OT merge, and strategies."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from typing import Any, cast

from .._types import Document


# ------------------------------------------------------------------
# Vector clocks
# ------------------------------------------------------------------


class VectorClock:
    """Per-document vector clock for causal ordering across replicas.

    Each writer (identified by a string ``node_id``) maintains a monotonic
    counter.  Two events are concurrent when neither dominates the other.
    """

    def __init__(self, state: dict[str, int] | None = None) -> None:
        self._clock: dict[str, int] = dict(state or {})

    def tick(self, node_id: str) -> "VectorClock":
        self._clock[node_id] = self._clock.get(node_id, 0) + 1
        return self

    def merge(self, other: "VectorClock") -> "VectorClock":
        for nid, ts in other._clock.items():
            self._clock[nid] = max(self._clock.get(nid, 0), ts)
        return self

    def dominates(self, other: "VectorClock") -> bool:
        """True if every entry in *other* is <= our entry, with at least one strictly greater."""
        if not other._clock:
            return bool(self._clock)
        for nid, ts in other._clock.items():
            if self._clock.get(nid, 0) < ts:
                return False
        return any(
            self._clock.get(nid, 0) > other._clock.get(nid, 0)
            for nid in set(self._clock) | set(other._clock)
        )

    def concurrent_with(self, other: "VectorClock") -> bool:
        return not self.dominates(other) and not other.dominates(self)

    def to_dict(self) -> dict[str, int]:
        return dict(self._clock)

    @classmethod
    def from_dict(cls, d: dict[str, int] | None) -> "VectorClock":
        return cls(d)


# ------------------------------------------------------------------
# CRDT helpers
# ------------------------------------------------------------------


def _crdt_counter_merge(local_val: Any, remote_val: Any) -> Any:
    """Merge two counter values (grow-only counter / PNCounter)."""
    if isinstance(local_val, int | float) and isinstance(remote_val, int | float):
        return max(local_val, remote_val)
    return remote_val


def _crdt_set_merge(local_val: Any, remote_val: Any) -> Any:
    """Merge two sets (G-Set / OR-Set approximation): union of elements."""
    if isinstance(local_val, list) and isinstance(remote_val, list):
        seen: set[Any] = set()
        merged: list[Any] = []
        for item in local_val + remote_val:
            key = (
                json.dumps(item, sort_keys=True, default=str)
                if isinstance(item, dict | list)
                else item
            )
            if key not in seen:
                seen.add(key)
                merged.append(item)
        return merged
    return remote_val


def _crdt_merge_doc(
    local_doc: Document, remote_doc: Document, crdt_fields: dict[str, str] | None = None
) -> Document:
    """Merge two documents using CRDT semantics for annotated fields.

    ``crdt_fields`` maps field names to CRDT types (``"counter"`` or ``"set"``).
    Non-annotated fields fall back to LWW.
    """
    crdt_fields = crdt_fields or {}
    merged = dict(local_doc)
    local_ts = (local_doc or {}).get("_lastModified", 0) or 0
    remote_ts = (remote_doc or {}).get("_lastModified", 0) or 0

    for field in set(local_doc) | set(remote_doc):
        if field == "_id":
            continue
        if field in crdt_fields:
            crdt_type = crdt_fields[field]
            lv = local_doc.get(field)
            rv = remote_doc.get(field)
            if crdt_type == "counter":
                merged[field] = _crdt_counter_merge(lv, rv)
            elif crdt_type == "set":
                merged[field] = _crdt_set_merge(lv, rv)
            else:
                merged[field] = rv if remote_ts >= local_ts else lv
        elif field in remote_doc:
            merged[field] = (
                remote_doc[field]
                if remote_ts >= local_ts
                else local_doc.get(field, remote_doc[field])
            )
    return merged


# ------------------------------------------------------------------
# Operational transform helpers for commutative operations
# ------------------------------------------------------------------


def _is_commutative_op(update_spec: Document) -> bool:
    """Return True if the update spec contains only commutative operators."""
    commutative_ops = {"$inc", "$push", "$addToSet", "$min", "$max"}
    if not isinstance(update_spec, dict):
        return False
    return bool(update_spec) and all(k in commutative_ops for k in update_spec)


def _merge_commutative_ops(local_spec: Document, remote_spec: Document) -> Document:
    """Merge two commutative update specs into a single combined spec.

    For ``$inc``, values are summed. For ``$push``/``$addToSet``/``$min``/``$max``,
    both sides are kept (union of fields, or combined ``$each`` arrays).
    """
    merged: dict[str, dict[str, Any]] = {}

    for op in ("$inc", "$push", "$addToSet", "$min", "$max"):
        local_fields = (local_spec or {}).get(op, {})
        remote_fields = (remote_spec or {}).get(op, {})
        if not local_fields and not remote_fields:
            continue
        combined: dict[str, Any] = {}
        all_keys = set(local_fields) | set(remote_fields)
        for field in all_keys:
            lv = local_fields.get(field)
            rv = remote_fields.get(field)
            if op == "$inc":
                combined[field] = (lv or 0) + (rv or 0)
            elif op in ("$push", "$addToSet"):
                items: list[Any] = []
                for v in (lv, rv):
                    if v is None:
                        continue
                    if isinstance(v, dict) and "$each" in v:
                        items.extend(v["$each"])
                    else:
                        items.append(v)
                if op == "$addToSet":
                    seen: set[Any] = set()
                    deduped: list[Any] = []
                    for item in items:
                        key = (
                            json.dumps(item, sort_keys=True, default=str)
                            if isinstance(item, dict | list)
                            else item
                        )
                        if key not in seen:
                            seen.add(key)
                            deduped.append(item)
                    items = deduped
                combined[field] = {"$each": items}
            elif op == "$min":
                vals = [v for v in (lv, rv) if v is not None]
                combined[field] = min(vals) if vals else lv
            elif op == "$max":
                vals = [v for v in (lv, rv) if v is not None]
                combined[field] = max(vals) if vals else lv
        if combined:
            merged[op] = combined

    return merged


def _apply_commutative_to_doc(base_doc: Document, update_spec: Document) -> Document:
    """Apply commutative update operators to a base document, producing a merged result.

    Used when the local change was a commutative op ($inc, $push, $addToSet)
    and the remote sent a full document -- re-apply the local delta on top of the
    remote state.
    """
    result = dict(base_doc)
    for field, val in update_spec.get("$inc", {}).items():
        cur = result.get(field, 0)
        if isinstance(cur, (int, float)) and isinstance(val, (int, float)):
            result[field] = cur + val
    for field, val in update_spec.get("$push", {}).items():
        cur = result.get(field, [])
        if isinstance(cur, list):
            items = val.get("$each", [val]) if isinstance(val, dict) else [val]
            result[field] = cur + items
    for field, val in update_spec.get("$addToSet", {}).items():
        cur = result.get(field, [])
        if isinstance(cur, list):
            items = val.get("$each", [val]) if isinstance(val, dict) else [val]
            existing: set[Any] = set()
            for x in cur:
                existing.add(
                    json.dumps(x, sort_keys=True, default=str) if isinstance(x, dict | list) else x
                )
            for item in items:
                key = (
                    json.dumps(item, sort_keys=True, default=str)
                    if isinstance(item, dict | list)
                    else item
                )
                if key not in existing:
                    cur.append(item)
                    existing.add(key)
            result[field] = cur
    for field, val in update_spec.get("$min", {}).items():
        cur = result.get(field)
        if cur is None or (isinstance(val, (int, float)) and isinstance(cur, (int, float)) and val < cur):
            result[field] = val
    for field, val in update_spec.get("$max", {}).items():
        cur = result.get(field)
        if cur is None or (isinstance(val, (int, float)) and isinstance(cur, (int, float)) and val > cur):
            result[field] = val
    return result


# ------------------------------------------------------------------
# Conflict resolution strategies
# ------------------------------------------------------------------


def _lww(local_doc: Document, remote_doc: Document) -> Document:
    """Last-write-wins: compare _lastModified timestamps."""
    local_ts = (local_doc or {}).get("_lastModified", 0)
    remote_ts = (remote_doc or {}).get("_lastModified", 0)
    return remote_doc if remote_ts >= local_ts else local_doc


def _local_wins(local_doc: Document, _remote_doc: Document) -> Document:
    return local_doc


def _remote_wins(_local_doc: Document, remote_doc: Document) -> Document:
    return remote_doc


def _field_merge(
    local_doc: Document,
    remote_doc: Document,
    *,
    local_changed: set[str] | None = None,
    remote_changed: set[str] | None = None,
) -> Document:
    """
    Field-level merge strategy.
    - fields changed only locally: keep local
    - fields changed only remotely: keep remote
    - fields changed on both: fall back to per-field LWW using _lastModified
    """
    local_doc = dict(local_doc or {})
    remote_doc = dict(remote_doc or {})
    local_changed = set(local_changed or [])
    remote_changed = set(remote_changed or [])

    merged = dict(local_doc)
    all_fields = set(local_doc.keys()) | set(remote_doc.keys())
    local_ts = local_doc.get("_lastModified", 0) or 0
    remote_ts = remote_doc.get("_lastModified", 0) or 0

    for field in all_fields:
        if field == "_id":
            merged[field] = local_doc.get("_id", remote_doc.get("_id"))
            continue
        in_local = field in local_changed
        in_remote = field in remote_changed
        if in_local and not in_remote:
            merged[field] = local_doc.get(field)
        elif in_remote and not in_local:
            merged[field] = remote_doc.get(field)
        elif in_local and in_remote:
            merged[field] = remote_doc.get(field) if remote_ts >= local_ts else local_doc.get(field)
        else:
            if field in remote_doc:
                merged[field] = remote_doc[field]
    return merged


_RESOLVERS: dict[str, Callable[..., Document]] = {
    "lww": _lww,
    "local_wins": _local_wins,
    "remote_wins": _remote_wins,
    "field_merge": _field_merge,
}


def _diff_fields(local_doc: Document, remote_doc: Document) -> set[str]:
    """Compute which fields actually differ between local and remote documents."""
    changed: set[str] = set()
    all_keys = set(local_doc.keys()) | set(remote_doc.keys())
    for key in all_keys:
        if key == "_id":
            continue
        local_val = local_doc.get(key)
        remote_val = remote_doc.get(key)
        if local_val != remote_val:
            changed.add(key)
    return changed


# ------------------------------------------------------------------
# Variable substitution for sync rules
# ------------------------------------------------------------------


def _resolve_variables(query: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Deep-clone *query* and replace ``$$NAME`` string values with *context* entries.

    Built-in variables (injected by the caller):
        ``$$NOW``      -- ``time.time()`` (epoch float, matches ``_lastModified``)
        ``$$NODE_ID``  -- the configured ``node_id``

    User-defined variables are merged from ``sync_config["variables"]``.
    Strings that start with ``$$`` but have no matching context key are left as-is
    so that ``$$ROOT`` / ``$$CURRENT`` still work inside ``$expr``.
    """

    def _walk(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: _walk(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_walk(v) for v in obj]
        if isinstance(obj, str) and obj.startswith("$$"):
            var_name = obj[2:]
            if var_name in context:
                return context[var_name]
        return obj

    return cast(dict[str, Any], _walk(copy.deepcopy(query)))
