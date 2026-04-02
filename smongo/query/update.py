"""MongoDB update operators -- apply $set, $inc, $push, etc. to documents."""

from __future__ import annotations

import re as _re
import time
from datetime import UTC, datetime
from typing import Any

from .._types import Document, Filter, UpdateSpec
from .compiler import compile_query
from .expressions import resolve_expr
from .paths import get_value, set_value, unset_value


def apply_update(
    doc: Document,
    update: UpdateSpec | list[dict[str, Any]],
    *,
    array_filters: list[dict[str, Any]] | None = None,
    query: Filter | None = None,
) -> None:
    """Apply MongoDB update operators to a document in place.

    Supports:
    - Standard operator updates (``{$set: ...}``)
    - Pipeline updates (``[{$set: ...}, {$unset: ...}]``)
    - Positional ``$`` (requires *query* to identify matching array element)
    - All-positional ``$[]``
    - Filtered positional ``$[<identifier>]`` (requires *array_filters*)
    """
    if isinstance(update, list):
        _apply_pipeline_update(doc, update)
        return

    filter_map: dict[str, dict[str, Any]] = {}
    if array_filters:
        for af in array_filters:
            for af_key in af:
                ident = af_key.split(".", 1)[0]
                filter_map[ident] = af

    for op, fields in update.items():
        if op == "$set":
            for k, v in fields.items():
                _set_with_positional(doc, k, v, query, filter_map)
        elif op == "$inc":
            for k, v in fields.items():
                cur = _get_positional(doc, k, query, filter_map)
                _set_with_positional(doc, k, (cur or 0) + v, query, filter_map)
        elif op == "$push":
            for k, v in fields.items():
                arr = get_value(doc, k) or []
                if not isinstance(arr, list):
                    arr = [arr]
                if isinstance(v, dict) and "$each" in v:
                    items = v["$each"]
                    pos = v.get("$position")
                    if isinstance(pos, int):
                        for i, item in enumerate(items):
                            arr.insert(pos + i, item)
                    else:
                        arr.extend(items)
                    if "$sort" in v:
                        sort_spec = v["$sort"]
                        if isinstance(sort_spec, int):
                            arr.sort(reverse=(sort_spec == -1))
                        elif isinstance(sort_spec, dict):
                            for sf, sd in reversed(list(sort_spec.items())):
                                arr.sort(
                                    key=lambda d, f=sf: (get_value(d, f) if isinstance(d, dict) else d) or 0,
                                    reverse=(sd == -1),
                                )
                    if "$slice" in v:
                        sl = v["$slice"]
                        if isinstance(sl, int):
                            if sl >= 0:
                                arr = arr[:sl]
                            else:
                                arr = arr[sl:]
                else:
                    arr.append(v)
                set_value(doc, k, arr)
        elif op == "$unset":
            for k in fields:
                unset_value(doc, k)
        elif op == "$addToSet":
            for k, v in fields.items():
                arr = get_value(doc, k) or []
                if not isinstance(arr, list):
                    arr = [arr]
                if isinstance(v, dict) and "$each" in v:
                    for item in v["$each"]:
                        if item not in arr:
                            arr.append(item)
                else:
                    if v not in arr:
                        arr.append(v)
                set_value(doc, k, arr)
        elif op == "$pull":
            for k, v in fields.items():
                arr = get_value(doc, k)
                if not isinstance(arr, list):
                    continue
                if isinstance(v, dict):
                    fn = compile_query(v)
                    arr = [item for item in arr if not (fn(item) if isinstance(item, dict) else False)]
                else:
                    arr = [item for item in arr if item != v]
                set_value(doc, k, arr)
        elif op == "$pop":
            for k, v in fields.items():
                arr = get_value(doc, k)
                if not isinstance(arr, list) or not arr:
                    continue
                if v == -1:
                    arr.pop(0)
                else:
                    arr.pop()
                set_value(doc, k, arr)
        elif op == "$min":
            for k, v in fields.items():
                cur = get_value(doc, k)
                if cur is None or v < cur:
                    set_value(doc, k, v)
        elif op == "$max":
            for k, v in fields.items():
                cur = get_value(doc, k)
                if cur is None or v > cur:
                    set_value(doc, k, v)
        elif op == "$mul":
            for k, v in fields.items():
                set_value(doc, k, (get_value(doc, k) or 0) * v)
        elif op == "$rename":
            for old_name, new_name in fields.items():
                val = get_value(doc, old_name)
                if val is not None:
                    unset_value(doc, old_name)
                    set_value(doc, new_name, val)
        elif op == "$currentDate":
            for k, v in fields.items():
                if isinstance(v, dict) and v.get("$type") == "timestamp":
                    set_value(doc, k, time.time())
                else:
                    set_value(doc, k, datetime.now(tz=UTC).isoformat())
        elif op == "$bit":
            for k, v in fields.items():
                cur = int(get_value(doc, k) or 0)
                if isinstance(v, dict):
                    if "and" in v:
                        cur &= int(v["and"])
                    if "or" in v:
                        cur |= int(v["or"])
                    if "xor" in v:
                        cur ^= int(v["xor"])
                set_value(doc, k, cur)
        else:
            raise NotImplementedError(f"Update operator {op} not supported")


def _apply_pipeline_update(doc: Document, pipeline: list[dict[str, Any]]) -> None:
    """Apply an aggregation-pipeline-style update to a document in place."""
    for stage in pipeline:
        op, spec = next(iter(stage.items()))
        if op in ("$set", "$addFields"):
            for field, expr in spec.items():
                set_value(doc, field, resolve_expr(doc, expr))
        elif op == "$unset":
            if isinstance(spec, str):
                unset_value(doc, spec)
            elif isinstance(spec, list):
                for f in spec:
                    unset_value(doc, f)
        elif op == "$replaceRoot":
            new_root = resolve_expr(doc, spec.get("newRoot"))
            if isinstance(new_root, dict):
                doc.clear()
                doc.update(new_root)
        elif op == "$replaceWith":
            new_root = resolve_expr(doc, spec)
            if isinstance(new_root, dict):
                doc.clear()
                doc.update(new_root)


def _find_positional_index(doc: Document, array_path: str, query: Filter | None) -> int | None:
    """Find the index of the first array element matching *query*."""
    if not query:
        return None
    arr = get_value(doc, array_path)
    if not isinstance(arr, list):
        return None
    for qk, qv in query.items():
        if qk.startswith(array_path + "."):
            sub_field = qk[len(array_path) + 1:]
            for i, elem in enumerate(arr):
                if isinstance(elem, dict) and get_value(elem, sub_field) == qv:
                    return i
        elif qk == array_path:
            if isinstance(qv, dict):
                fn = compile_query(qv)
                for i, elem in enumerate(arr):
                    if fn(elem if isinstance(elem, dict) else {qk: elem}):
                        return i
            else:
                for i, elem in enumerate(arr):
                    if elem == qv:
                        return i
    return 0 if arr else None


def _set_with_positional(
    doc: Document, path: str, value: Any,
    query: Filter | None, filter_map: dict[str, dict[str, Any]],
) -> None:
    """Handle ``$``, ``$[]``, ``$[<identifier>]`` in field paths."""
    if ".$." in path or path.endswith(".$"):
        parts = path.split(".$", 1)
        array_path = parts[0]
        idx = _find_positional_index(doc, array_path, query)
        if idx is not None:
            remainder = parts[1].lstrip(".")
            actual_path = f"{array_path}.{idx}" + (f".{remainder}" if remainder else "")
            set_value(doc, actual_path, value)
        return

    if ".$[]" in path:
        parts = path.split(".$[]", 1)
        array_path = parts[0]
        remainder = parts[1].lstrip(".")
        arr = get_value(doc, array_path)
        if isinstance(arr, list):
            for i in range(len(arr)):
                actual_path = f"{array_path}.{i}" + (f".{remainder}" if remainder else "")
                set_value(doc, actual_path, value)
        return

    m = _re.search(r'\.\$\[(\w+)\]', path)
    if m:
        ident = m.group(1)
        array_path = path[:m.start()]
        remainder = path[m.end():].lstrip(".")
        arr = get_value(doc, array_path)
        af = filter_map.get(ident, {})
        if isinstance(arr, list) and af:
            fn = compile_query({k.split(".", 1)[-1] if "." in k else k: v for k, v in af.items()})
            for i, elem in enumerate(arr):
                if fn(elem if isinstance(elem, dict) else {"": elem}):
                    actual_path = f"{array_path}.{i}" + (f".{remainder}" if remainder else "")
                    set_value(doc, actual_path, value)
        return

    set_value(doc, path, value)


def _get_positional(
    doc: Document, path: str,
    query: Filter | None, filter_map: dict[str, dict[str, Any]],
) -> Any:
    """Read the value at a potentially positional path."""
    if ".$." in path or path.endswith(".$"):
        parts = path.split(".$", 1)
        array_path = parts[0]
        idx = _find_positional_index(doc, array_path, query)
        if idx is not None:
            remainder = parts[1].lstrip(".")
            actual_path = f"{array_path}.{idx}" + (f".{remainder}" if remainder else "")
            return get_value(doc, actual_path)
        return None
    return get_value(doc, path)
