from __future__ import annotations

import json
from collections import defaultdict
from copy import deepcopy
from typing import Any

from .._types import CollectionGetter, Document, Pipeline
from ..query import compile_query, get_value, resolve_expr, set_value


def _foreign_has_index(foreign_coll: Any, field: str) -> bool:
    """Check whether *foreign_coll* has an index whose leading key is *field*."""
    try:
        for idx in foreign_coll.list_indexes():
            keys = idx.get("keys", [])
            if keys and keys[0][0] == field:
                return True
    except (AttributeError, TypeError, KeyError):
        pass
    return False


def lookup_stage(
    docs: list[Document],
    spec: dict[str, Any],
    collection_getter: CollectionGetter | None = None,
) -> list[Document]:
    """
    $lookup -- left outer join.
    {from: "other_coll", localField: "field", foreignField: "_id", as: "joined"}

    When the foreign collection has an index on ``foreignField``, the planner
    is used per local document instead of loading all foreign docs into memory.
    """
    from_coll = spec.get("from")
    local_field = spec.get("localField")
    foreign_field = spec.get("foreignField")
    as_field = spec.get("as")

    if not all([from_coll, local_field, foreign_field, as_field]):
        missing = [
            k
            for k, v in {
                "from": from_coll,
                "localField": local_field,
                "foreignField": foreign_field,
                "as": as_field,
            }.items()
            if not v
        ]
        raise ValueError(f"$lookup missing required fields: {', '.join(missing)}")

    assert isinstance(from_coll, str)
    assert isinstance(local_field, str)
    assert isinstance(foreign_field, str)
    assert isinstance(as_field, str)

    foreign_coll = None
    if collection_getter:
        try:
            foreign_coll = collection_getter(from_coll)
        except (KeyError, AttributeError, TypeError):
            pass

    if foreign_coll is not None and _foreign_has_index(foreign_coll, foreign_field):
        out: list[Document] = []
        for doc in docs:
            new_doc = deepcopy(doc)
            local_val = get_value(doc, local_field)
            matches = foreign_coll.find({foreign_field: local_val}) if local_val is not None else []
            set_value(new_doc, as_field, list(matches))
            out.append(new_doc)
        return out

    foreign_docs: list[Document] = []
    if foreign_coll is not None:
        try:
            foreign_docs = (
                foreign_coll.get_all()
                if hasattr(foreign_coll, "get_all")
                else list(foreign_coll.find({}))
            )
        except (KeyError, AttributeError, TypeError):
            pass

    foreign_index: defaultdict[Any, list[Document]] = defaultdict(list)
    for fd in foreign_docs:
        fv = get_value(fd, foreign_field)
        foreign_index[fv].append(fd)

    out = []
    for doc in docs:
        new_doc = deepcopy(doc)
        local_val = get_value(doc, local_field)
        matches = foreign_index.get(local_val, [])
        set_value(new_doc, as_field, matches)
        out.append(new_doc)
    return out


def graph_lookup_stage(
    docs: list[Document],
    spec: dict[str, Any],
    collection_getter: CollectionGetter | None = None,
) -> list[Document]:
    """$graphLookup -- recursive lookup."""
    from_coll_name = spec.get("from")
    start_with = spec.get("startWith")
    connect_from = spec.get("connectFromField")
    connect_to = spec.get("connectToField")
    as_field = spec.get("as")
    max_depth = spec.get("maxDepth")
    depth_field = spec.get("depthField")
    restrict_search = spec.get("restrictSearchWithMatch")

    if not all([from_coll_name, connect_from, connect_to, as_field]):
        raise ValueError("$graphLookup missing required fields")
    if not isinstance(from_coll_name, str):
        raise ValueError("$graphLookup 'from' must be a string")
    if not isinstance(connect_from, str) or not isinstance(connect_to, str):
        raise ValueError("$graphLookup connect fields must be strings")
    if not isinstance(as_field, str):
        raise ValueError("$graphLookup 'as' must be a string")

    foreign_coll = None
    if collection_getter:
        try:
            foreign_coll = collection_getter(from_coll_name)
        except (KeyError, AttributeError, TypeError):
            pass

    foreign_docs: list[Document] = []
    if foreign_coll is not None:
        try:
            foreign_docs = (
                foreign_coll.get_all()
                if hasattr(foreign_coll, "get_all")
                else list(foreign_coll.find({}))
            )
        except (KeyError, AttributeError, TypeError):
            pass

    assert isinstance(connect_to, str)
    assert isinstance(connect_from, str)
    assert isinstance(as_field, str)

    foreign_index: defaultdict[Any, list[Document]] = defaultdict(list)
    for fd in foreign_docs:
        fv = get_value(fd, connect_to)
        foreign_index[fv].append(fd)

    restrict_fn = compile_query(restrict_search) if restrict_search else None

    out: list[Document] = []
    for doc in docs:
        start_val = resolve_expr(doc, start_with)
        frontier = [start_val] if not isinstance(start_val, list) else list(start_val)
        visited: set[Any] = set()
        results: list[Document] = []
        depth = 0

        while frontier:
            if max_depth is not None and depth > max_depth:
                break
            next_frontier: list[Any] = []
            for val in frontier:
                key = (
                    json.dumps(val, sort_keys=True, default=str)
                    if isinstance(val, dict | list)
                    else val
                )
                if key in visited:
                    continue
                visited.add(key)
                for matched in foreign_index.get(val, []):
                    if restrict_fn and not restrict_fn(matched):
                        continue
                    r = deepcopy(matched)
                    if depth_field:
                        r[depth_field] = depth
                    results.append(r)
                    next_val = get_value(matched, connect_from)
                    if next_val is not None:
                        if isinstance(next_val, list):
                            next_frontier.extend(next_val)
                        else:
                            next_frontier.append(next_val)
            frontier = next_frontier
            depth += 1

        new_doc = deepcopy(doc)
        set_value(new_doc, as_field, results)
        out.append(new_doc)
    return out


def union_with_stage(
    docs: list[Document],
    spec: dict[str, Any],
    collection_getter: CollectionGetter | None = None,
    *,
    max_pipeline_docs: int,
) -> list[Document]:
    """$unionWith -- combine current docs with another collection's docs."""
    from .cursor import Cursor

    coll_name = spec.get("coll")
    pipeline = spec.get("pipeline", [])

    if not coll_name:
        raise ValueError("$unionWith requires 'coll'")
    if collection_getter is None:
        raise RuntimeError("$unionWith requires a collection getter")

    foreign_coll = collection_getter(coll_name)
    foreign_docs = (
        foreign_coll.get_all() if hasattr(foreign_coll, "get_all") else list(foreign_coll.find({}))
    )

    if pipeline:
        c = Cursor(foreign_docs, collection_getter=collection_getter)
        foreign_docs = c.aggregate(pipeline, max_pipeline_docs=max_pipeline_docs)

    return docs + foreign_docs


def pipeline_lookup_stage(
    docs: list[Document],
    spec: dict[str, Any],
    collection_getter: CollectionGetter | None = None,
    *,
    max_pipeline_docs: int,
) -> list[Document]:
    """$lookup with a sub-pipeline (not just equality join)."""
    from .cursor import Cursor

    from_coll_name = spec.get("from")
    let_vars = spec.get("let", {})
    pipeline = spec.get("pipeline", [])
    as_field = spec.get("as")

    if not from_coll_name or not as_field:
        raise ValueError("$lookup with pipeline requires 'from' and 'as'")

    if collection_getter is None:
        raise RuntimeError("$lookup with pipeline requires a collection getter")

    assert isinstance(from_coll_name, str)
    assert isinstance(as_field, str)
    foreign_coll = collection_getter(from_coll_name)

    out: list[Document] = []
    for doc in docs:
        scope: dict[str, Any] = {}
        for var_name, var_expr in let_vars.items():
            scope[var_name] = resolve_expr(doc, var_expr)

        bound_pipeline: Pipeline = _bind_pipeline_vars(pipeline, scope)

        foreign_docs = (
            foreign_coll.get_all()
            if hasattr(foreign_coll, "get_all")
            else list(foreign_coll.find({}))
        )
        sub_cursor = Cursor(foreign_docs, collection_getter=collection_getter)
        results = sub_cursor.aggregate(bound_pipeline, max_pipeline_docs=max_pipeline_docs)

        new_doc = deepcopy(doc)
        set_value(new_doc, as_field, results)
        out.append(new_doc)
    return out


def _bind_pipeline_vars(pipeline: Pipeline, scope: dict[str, Any]) -> Pipeline:
    """Replace ``$$var`` references in a pipeline with concrete values."""
    return [_bind_vars_in_expr(stage, scope) for stage in pipeline]


def _bind_vars_in_expr(expr: Any, scope: dict[str, Any]) -> Any:
    if (
        isinstance(expr, str)
        and expr.startswith("$$")
        and not expr.startswith("$$ROOT")
        and not expr.startswith("$$CURRENT")
    ):
        var_name = expr[2:]
        if var_name in scope:
            return scope[var_name]
        return expr
    if isinstance(expr, dict):
        return {k: _bind_vars_in_expr(v, scope) for k, v in expr.items()}
    if isinstance(expr, list):
        return [_bind_vars_in_expr(v, scope) for v in expr]
    return expr
