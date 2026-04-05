"""Aggregation join stages ($lookup, $graphLookup, $unionWith, sub-pipeline $lookup).

Core join implementations live in Rust (_smongo_core).  ``union_with_stage``
and variable-binding helpers remain in Python because Rust delegates I/O-heavy
or recursive-Python stages back here.
"""

from __future__ import annotations

from typing import Any

from smongo._smongo_core import (
    graph_lookup_stage,  # noqa: F401
    lookup_stage,  # noqa: F401
    pipeline_lookup_stage,  # noqa: F401
)

from .._types import CollectionGetter, Document, Pipeline


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
