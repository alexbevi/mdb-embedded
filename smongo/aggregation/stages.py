from __future__ import annotations

import json
import math
from collections import defaultdict
from copy import deepcopy
from typing import Any

from .._types import Document
from ..query import field_exists, get_value, resolve_expr, set_value, unset_value


def group_stage(
    docs: list[Document],
    spec: dict[str, Any],
    *,
    allow_disk_use: bool = False,
) -> list[Document]:
    if allow_disk_use:
        return _group_stage_disk(docs, spec)
    return _group_stage_mem(docs, spec)


def _group_stage_mem(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """In-memory grouping (original path)."""
    grouped: defaultdict[Any, list[Document]] = defaultdict(list)
    for doc in docs:
        key = resolve_expr(doc, spec["_id"])
        if isinstance(key, (dict, list)):
            key = json.dumps(key, sort_keys=True)
        grouped[key].append(doc)

    results: list[Document] = []
    for key, group_docs in grouped.items():
        try:
            key = json.loads(key)
        except (TypeError, json.JSONDecodeError):
            pass

        out: Document = {"_id": key}
        for field, expr in spec.items():
            if field == "_id":
                continue
            if not isinstance(expr, dict):
                continue
            out[field] = _eval_accumulator(expr, group_docs)

        results.append(out)
    return results


def _group_stage_disk(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """Disk-spill grouping: partition to temp files, reduce per-group."""
    from .constants import DiskSpillGrouper

    grouper = DiskSpillGrouper()
    try:
        for doc in docs:
            key = resolve_expr(doc, spec["_id"])
            grouper.add(key, doc)

        results: list[Document] = []
        for key, group_iter in grouper.iter_groups():
            group_docs = list(group_iter)
            out: Document = {"_id": key}
            for field, expr in spec.items():
                if field == "_id":
                    continue
                if not isinstance(expr, dict):
                    continue
                out[field] = _eval_accumulator(expr, group_docs)
            results.append(out)
        return results
    finally:
        grouper.cleanup()


def project_stage(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    out: list[Document] = []
    for doc in docs:
        new_doc: Document = {}
        for field, expr in spec.items():
            if expr == 1 or expr is True:
                new_doc[field] = get_value(doc, field)
            elif expr == 0 or expr is False:
                continue
            elif isinstance(expr, dict):
                new_doc[field] = resolve_expr(doc, expr)
            else:
                new_doc[field] = resolve_expr(doc, expr)
        out.append(new_doc)
    return out


def sort_stage(
    docs: list[Document],
    spec: dict[str, Any],
    *,
    allow_disk_use: bool = False,
) -> list[Document]:
    from .constants import DEFAULT_MEMORY_LIMIT_BYTES, DiskSpillSorter, _estimate_docs_bytes

    use_spill = allow_disk_use and _estimate_docs_bytes(docs) > DEFAULT_MEMORY_LIMIT_BYTES

    for field, direction in reversed(list(spec.items())):
        key_fn = lambda d, f=field: (get_value(d, f) is not None, get_value(d, f))  # type: ignore[misc]
        if use_spill:
            sorter = DiskSpillSorter(key_fn, reverse=(direction == -1))
            docs = sorter.sort(docs)
        else:
            docs = sorted(docs, key=key_fn, reverse=(direction == -1))
    return docs


def unwind_stage(docs: list[Document], spec: str | dict[str, Any]) -> list[Document]:
    if isinstance(spec, str):
        path = spec[1:] if spec.startswith("$") else spec
        preserve_null = False
    elif isinstance(spec, dict):
        path = spec.get("path", "")
        if path.startswith("$"):
            path = path[1:]
        preserve_null = spec.get("preserveNullAndEmptyArrays", False)
    else:
        raise TypeError(f"$unwind spec must be a string or dict, got {type(spec).__name__}")

    out: list[Document] = []
    for doc in docs:
        val = get_value(doc, path)
        if isinstance(val, list):
            if not val and preserve_null:
                new_doc = deepcopy(doc)
                set_value(new_doc, path, None)
                out.append(new_doc)
            else:
                for item in val:
                    new_doc = deepcopy(doc)
                    set_value(new_doc, path, item)
                    out.append(new_doc)
        elif val is not None or preserve_null:
            out.append(doc)
    return out


def add_fields_stage(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """$addFields / $set -- merge new fields into each doc."""
    out: list[Document] = []
    for doc in docs:
        new_doc = deepcopy(doc)
        for field, expr in spec.items():
            set_value(new_doc, field, resolve_expr(doc, expr))
        out.append(new_doc)
    return out


def replace_root_stage(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """$replaceRoot -- promote a sub-document to the top level."""
    new_root_expr = spec.get("newRoot")
    if new_root_expr is None:
        raise ValueError("$replaceRoot requires 'newRoot' expression")
    out: list[Document] = []
    for doc in docs:
        new_root = resolve_expr(doc, new_root_expr)
        if not isinstance(new_root, dict):
            raise TypeError(
                f"$replaceRoot requires 'newRoot' to evaluate to an object, "
                f"got {type(new_root).__name__}"
            )
        out.append(new_root)
    return out


def limit_stage(docs: list[Document], spec: int) -> list[Document]:
    return docs[:spec]


def skip_stage(docs: list[Document], spec: int) -> list[Document]:
    return docs[spec:]


def count_stage(docs: list[Document], spec: str) -> list[Document]:
    return [{spec: len(docs)}]


def sample_stage(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    import random

    size = spec.get("size", len(docs))
    return random.sample(docs, min(size, len(docs)))


def bucket_stage(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """$bucket -- group docs into fixed-boundary buckets."""
    group_by = spec.get("groupBy")
    boundaries = spec.get("boundaries", [])
    default = spec.get("default")
    output = spec.get("output")

    if len(boundaries) < 2:
        raise ValueError("$bucket requires at least 2 boundaries")

    buckets: dict[Any, list[Document]] = {b: [] for b in boundaries[:-1]}
    if default is not None:
        buckets[default] = []

    for doc in docs:
        val = resolve_expr(doc, group_by)
        placed = False
        for i in range(len(boundaries) - 1):
            if boundaries[i] <= val < boundaries[i + 1]:
                buckets[boundaries[i]].append(doc)
                placed = True
                break
        if not placed and default is not None:
            buckets[default].append(doc)

    results: list[Document] = []
    for bucket_id, group_docs in buckets.items():
        out_doc: Document = {"_id": bucket_id}
        if output:
            for field, accum in output.items():
                out_doc[field] = _eval_accumulator(accum, group_docs)
        else:
            out_doc["count"] = len(group_docs)
        results.append(out_doc)
    return results


def bucket_auto_stage(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """$bucketAuto -- group docs into approximately equal-sized buckets."""
    group_by = spec.get("groupBy")
    granularity = spec.get("buckets", 5)
    output = spec.get("output")

    vals = [(resolve_expr(doc, group_by), doc) for doc in docs]
    vals.sort(key=lambda x: (x[0] is not None, x[0]))

    n = max(1, granularity)
    chunk_size = max(1, math.ceil(len(vals) / n))
    results: list[Document] = []
    for i in range(0, len(vals), chunk_size):
        chunk = vals[i:i + chunk_size]
        if not chunk:
            continue
        lo = chunk[0][0]
        hi = chunk[-1][0]
        group_docs = [d for _, d in chunk]
        out_doc: Document = {"_id": {"min": lo, "max": hi}, "count": len(group_docs)}
        if output:
            for field, accum in output.items():
                out_doc[field] = _eval_accumulator(accum, group_docs)
        results.append(out_doc)
    return results


def unset_stage(docs: list[Document], spec: str | list[str]) -> list[Document]:
    """$unset -- remove fields from documents."""
    fields = [spec] if isinstance(spec, str) else spec
    out: list[Document] = []
    for doc in docs:
        new_doc = deepcopy(doc)
        for f in fields:
            unset_value(new_doc, f)
        out.append(new_doc)
    return out


def redact_stage(docs: list[Document], spec: Any) -> list[Document]:
    """$redact -- field-level access control.

    Evaluates *spec* for each document.  Returns:
    - ``$$DESCEND`` -- keep doc, evaluate sub-documents
    - ``$$PRUNE`` -- remove doc entirely
    - ``$$KEEP`` -- keep doc as-is
    """
    out: list[Document] = []
    for doc in docs:
        result = _redact_doc(doc, spec)
        if result is not None:
            out.append(result)
    return out


def _redact_doc(doc: Document, expr: Any) -> Document | None:
    val = resolve_expr(doc, expr)
    if val == "$$KEEP":
        return doc
    if val == "$$PRUNE":
        return None
    if val == "$$DESCEND":
        new_doc: Document = {}
        for k, v in doc.items():
            if isinstance(v, dict):
                child = _redact_doc(v, expr)
                if child is not None:
                    new_doc[k] = child
            elif isinstance(v, list):
                new_arr: list[Any] = []
                for elem in v:
                    if isinstance(elem, dict):
                        child = _redact_doc(elem, expr)
                        if child is not None:
                            new_arr.append(child)
                    else:
                        new_arr.append(elem)
                new_doc[k] = new_arr
            else:
                new_doc[k] = v
        return new_doc
    return doc


def sort_by_count_stage(docs: list[Document], spec: Any) -> list[Document]:
    """$sortByCount -- group by expression, count, sort descending."""
    counts: defaultdict[Any, int] = defaultdict(int)
    for doc in docs:
        val = resolve_expr(doc, spec)
        key = json.dumps(val, sort_keys=True, default=str) if isinstance(val, (dict, list)) else val
        counts[key] = counts.get(key, 0) + 1
    results = [{"_id": k, "count": v} for k, v in counts.items()]
    results.sort(key=lambda d: d["count"], reverse=True)
    return results


def set_window_fields_stage(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """$setWindowFields -- apply window functions over a partition."""
    partition_by = spec.get("partitionBy")
    sort_by = spec.get("sortBy")
    output = spec.get("output", {})

    if sort_by:
        docs = sort_stage(docs, sort_by)

    partitions: defaultdict[Any, list[Document]] = defaultdict(list)
    for doc in docs:
        key = resolve_expr(doc, partition_by) if partition_by else None
        if isinstance(key, (dict, list)):
            key = json.dumps(key, sort_keys=True, default=str)
        partitions[key].append(doc)

    out: list[Document] = []
    for _pk, part_docs in partitions.items():
        for i, doc in enumerate(part_docs):
            new_doc = deepcopy(doc)
            for field, window_spec in output.items():
                if not isinstance(window_spec, dict):
                    continue
                accum_op, accum_val = next(iter(window_spec.items()))
                if accum_op == "$window":
                    continue
                window = window_spec.get("window", {})
                docs_window = window.get("documents")
                if docs_window and len(docs_window) == 2:
                    lo_bound = docs_window[0]
                    hi_bound = docs_window[1]
                    lo = max(0, i + lo_bound) if isinstance(lo_bound, int) else 0
                    hi = min(len(part_docs), i + hi_bound + 1) if isinstance(hi_bound, int) else len(part_docs)
                else:
                    lo, hi = 0, len(part_docs)
                window_docs = part_docs[lo:hi]
                val = _eval_accumulator({accum_op: accum_val}, window_docs)
                set_value(new_doc, field, val)
            out.append(new_doc)
    return out


def _eval_accumulator(accum: dict[str, Any], group_docs: list[Document]) -> Any:
    """Evaluate a single accumulator expression over a group of docs."""
    accum_op, val = next(iter(accum.items()))
    if accum_op == "$sum":
        if val == 1:
            return len(group_docs)
        return sum(resolve_expr(d, val) or 0 for d in group_docs)
    if accum_op == "$avg":
        vals = [resolve_expr(d, val) for d in group_docs if resolve_expr(d, val) is not None]
        return sum(vals) / len(vals) if vals else 0
    if accum_op == "$min":
        vals = [resolve_expr(d, val) for d in group_docs if resolve_expr(d, val) is not None]
        return min(vals) if vals else None
    if accum_op == "$max":
        vals = [resolve_expr(d, val) for d in group_docs if resolve_expr(d, val) is not None]
        return max(vals) if vals else None
    if accum_op == "$first":
        return resolve_expr(group_docs[0], val) if group_docs else None
    if accum_op == "$last":
        return resolve_expr(group_docs[-1], val) if group_docs else None
    if accum_op == "$push":
        return [resolve_expr(d, val) for d in group_docs]
    if accum_op == "$addToSet":
        seen: list[Any] = []
        for d in group_docs:
            v = resolve_expr(d, val)
            if v not in seen:
                seen.append(v)
        return seen
    if accum_op == "$stdDevPop":
        vals = [resolve_expr(d, val) for d in group_docs if resolve_expr(d, val) is not None]
        if not vals:
            return None
        mean = sum(vals) / len(vals)
        return math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))
    if accum_op == "$stdDevSamp":
        vals = [resolve_expr(d, val) for d in group_docs if resolve_expr(d, val) is not None]
        if len(vals) < 2:
            return None
        mean = sum(vals) / len(vals)
        return math.sqrt(sum((v - mean) ** 2 for v in vals) / (len(vals) - 1))
    if accum_op == "$mergeObjects":
        result: dict[str, Any] = {}
        for d in group_docs:
            v = resolve_expr(d, val)
            if isinstance(v, dict):
                result.update(v)
        return result
    if accum_op in ("$top", "$bottom"):
        sort_by = val.get("sortBy", {}) if isinstance(val, dict) else {}
        output_expr = val.get("output") if isinstance(val, dict) else val
        sorted_docs = sort_stage(group_docs, sort_by) if sort_by else group_docs
        if accum_op == "$bottom":
            sorted_docs = list(reversed(sorted_docs))
        return resolve_expr(sorted_docs[0], output_expr) if sorted_docs else None
    if accum_op in ("$topN", "$bottomN"):
        n = val.get("n", 1) if isinstance(val, dict) else 1
        sort_by = val.get("sortBy", {}) if isinstance(val, dict) else {}
        output_expr = val.get("output") if isinstance(val, dict) else val
        sorted_docs = sort_stage(group_docs, sort_by) if sort_by else group_docs
        if accum_op == "$bottomN":
            sorted_docs = list(reversed(sorted_docs))
        return [resolve_expr(d, output_expr) for d in sorted_docs[:n]]
    if accum_op == "$firstN":
        n = val.get("n", 1) if isinstance(val, dict) else 1
        input_expr = val.get("input") if isinstance(val, dict) else val
        return [resolve_expr(d, input_expr) for d in group_docs[:n]]
    if accum_op == "$lastN":
        n = val.get("n", 1) if isinstance(val, dict) else 1
        input_expr = val.get("input") if isinstance(val, dict) else val
        return [resolve_expr(d, input_expr) for d in group_docs[-n:]]
    return None
