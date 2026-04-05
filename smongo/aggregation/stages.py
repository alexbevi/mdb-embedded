"""Aggregation pipeline stages.

Core stage implementations live in Rust (_smongo_core).  Python fallbacks
for disk-spill grouping and sorting (``_py_group_stage``, ``_py_sort_stage``)
remain here because Rust delegates to them when ``allow_disk_use=True``.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from typing import Any

from smongo._smongo_core import (
    add_fields_stage,  # noqa: F401
    bucket_auto_stage,  # noqa: F401
    bucket_stage,  # noqa: F401
    count_stage,  # noqa: F401
    group_stage,  # noqa: F401
    limit_stage,  # noqa: F401
    project_stage,  # noqa: F401
    redact_stage,  # noqa: F401
    replace_root_stage,  # noqa: F401
    sample_stage,  # noqa: F401
    set_window_fields_stage,  # noqa: F401
    skip_stage,  # noqa: F401
    sort_by_count_stage,  # noqa: F401
    sort_stage,
    unset_stage,  # noqa: F401
    unwind_stage,  # noqa: F401
)

from .._types import Document
from ..query import get_value, resolve_expr


def _py_group_stage(
    docs: list[Document],
    spec: dict[str, Any],
    *,
    allow_disk_use: bool = False,
) -> list[Document]:
    """Python fallback for $group (called by Rust for disk-spill path)."""
    if allow_disk_use:
        return _group_stage_disk(docs, spec)
    return _group_stage_mem(docs, spec)


def _group_stage_mem(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """In-memory grouping (original path)."""
    grouped: defaultdict[Any, list[Document]] = defaultdict(list)
    for doc in docs:
        key = resolve_expr(doc, spec["_id"])
        if isinstance(key, dict | list):
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


def _py_sort_stage(
    docs: list[Document],
    spec: dict[str, Any],
    *,
    allow_disk_use: bool = False,
) -> list[Document]:
    """Python fallback for $sort (called by Rust for disk-spill path)."""
    from .constants import DEFAULT_MEMORY_LIMIT_BYTES, DiskSpillSorter, _estimate_docs_bytes

    use_spill = allow_disk_use and _estimate_docs_bytes(docs) > DEFAULT_MEMORY_LIMIT_BYTES

    for field, direction in reversed(list(spec.items())):

        def key_fn(doc: Document, field_name: str = field) -> tuple[bool, Any]:
            value = get_value(doc, field_name)
            return (value is not None, value)

        if use_spill:
            sorter = DiskSpillSorter(key_fn, reverse=(direction == -1))
            docs = sorter.sort(docs)
        else:
            docs = sorted(docs, key=key_fn, reverse=(direction == -1))
    return docs


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
