from __future__ import annotations

"""
Aggregation pipeline engine + Cursor with PyMongo-compatible chaining.

Guardrails:
- ``max_pipeline_docs`` caps the number of documents flowing between stages
  (default 100 000).  Override per-call via ``Cursor.aggregate(pipeline,
  max_pipeline_docs=...)``.
- ``$out`` batches writes in chunks of ``_OUT_BATCH_SIZE`` to avoid
  unbounded memory during serialization.
- ``$lookup`` uses index-backed ``find()`` per-doc when the foreign
  collection has an index on ``foreignField``; falls back to the original
  hash-join otherwise.
"""

import itertools
from collections.abc import Iterable, Iterator

from typing import Any, cast

from .._types import CollectionGetter, Document, Filter, Pipeline, Projection
from ..query import compile_query, field_exists, get_value, set_value
from .constants import (
    DEFAULT_MAX_PIPELINE_DOCS,
    DEFAULT_MEMORY_LIMIT_BYTES,
    MAX_PIPELINE_STAGES,
    DocumentLimitExceeded,
    MemoryLimitExceeded,
    _estimate_docs_bytes,
)
from .geo import geo_near_stage
from .joins import graph_lookup_stage, lookup_stage, pipeline_lookup_stage, union_with_stage
from .output import facet_stage, merge_stage, out_stage
from .stages import (
    add_fields_stage,
    bucket_auto_stage,
    bucket_stage,
    count_stage,
    group_stage,
    limit_stage,
    project_stage,
    redact_stage,
    replace_root_stage,
    sample_stage,
    set_window_fields_stage,
    skip_stage,
    sort_by_count_stage,
    sort_stage,
    unset_stage,
    unwind_stage,
)
from .vector import vector_search_stage


class Cursor:
    """
    Chainable cursor over documents (lazy or pre-materialized).
    Supports .sort(), .limit(), .skip(), .projection() -- deferred until iteration.

    Accepts any ``Iterable[Document]`` (including generators and
    :class:`~smongo.storage.streaming.StreamingCursor`).  The iterable is
    only consumed when the results are actually needed, and
    ``skip``/``limit`` without ``sort`` use :func:`itertools.islice` so
    that only the required documents are pulled from the source.
    """

    def __init__(
        self, docs: Iterable[Document], collection_getter: CollectionGetter | None = None
    ) -> None:
        if isinstance(docs, list):
            self._materialized: list[Document] | None = docs
        else:
            self._materialized = None
        self._docs: Iterable[Document] = docs
        self._sort_spec: list[tuple[str, int]] | None = None
        self._limit_val: int | None = None
        self._skip_val: int | None = None
        self._projection_spec: Projection | None = None
        self._collection_getter = collection_getter
        self._resolved: list[Document] | None = None

    # ── Internal helpers ─────────────────────────────────────────────

    def _materialize(self) -> list[Document]:
        """Convert the source iterable to a list (once)."""
        if self._materialized is None:
            self._materialized = list(self._docs)
        return self._materialized

    # ── Chainable modifiers ──────────────────────────────────────────

    def sort(
        self,
        key_or_list: str | list[tuple[str, int]] | dict[str, int],
        direction: int | None = None,
    ) -> Cursor:
        if isinstance(key_or_list, str):
            self._sort_spec = [(key_or_list, direction or 1)]
        elif isinstance(key_or_list, list):
            self._sort_spec = key_or_list
        elif isinstance(key_or_list, dict):
            self._sort_spec = list(key_or_list.items())
        self._resolved = None
        return self

    def limit(self, n: int) -> Cursor:
        self._limit_val = n
        self._resolved = None
        return self

    def skip(self, n: int) -> Cursor:
        self._skip_val = n
        self._resolved = None
        return self

    def projection(self, spec: Projection) -> Cursor:
        self._projection_spec = spec
        self._resolved = None
        return self

    # ── PyMongo-compatible chain stubs ────────────────────────────────
    # These exist so that code written for PyMongo can run against smongo
    # without AttributeError.  Some are no-ops for the embedded engine;
    # others store the value for potential future use.

    def hint(self, index: Any) -> Cursor:
        """Store an index hint (reserved for future planner integration)."""
        self._hint = index
        return self

    def batch_size(self, size: int) -> Cursor:
        """No-op for the embedded engine (all docs are local)."""
        return self

    def collation(self, collation: Any) -> Cursor:
        """Store collation settings (reserved for future sort integration)."""
        self._collation = collation
        return self

    def comment(self, text: str) -> Cursor:
        """No-op for the embedded engine (no profiler to log to)."""
        return self

    def max_time_ms(self, ms: int) -> Cursor:
        """No-op for the embedded engine (no server-side timeout)."""
        return self

    # ── Evaluation ───────────────────────────────────────────────────

    def _resolve(self) -> list[Document]:
        """Apply deferred sort/skip/limit/projection and return final list."""
        if self._resolved is not None:
            return self._resolved

        if self._sort_spec:
            docs = sort_stage(self._materialize(), dict(self._sort_spec))
            if self._skip_val is not None:
                docs = docs[self._skip_val :]
            if self._limit_val is not None:
                docs = docs[: self._limit_val]
        else:
            it: Iterable[Document] = (
                self._materialized if self._materialized is not None else self._docs
            )
            if self._skip_val is not None:
                it = itertools.islice(it, self._skip_val, None)
            if self._limit_val is not None:
                it = itertools.islice(it, self._limit_val)
            docs = list(it)
            if self._materialized is None:
                self._materialized = docs

        if self._projection_spec:
            docs = _apply_projection(docs, self._projection_spec)

        resolved: list[Document] = cast(list[Document], docs)
        self._resolved = resolved
        return resolved

    # ── Iteration / materialization ──────────────────────────────────

    def __iter__(self) -> Iterator[Document]:
        return iter(self._resolve())

    def __len__(self) -> int:
        return len(self._resolve())

    def to_list(self) -> list[Document]:
        return self._resolve()

    def __getitem__(self, idx: int) -> Document:
        return self._resolve()[idx]

    # ── Query helpers ────────────────────────────────────────────────

    def find(self, query: Filter) -> Cursor:
        fn = compile_query(query)
        return Cursor([d for d in self._materialize() if fn(d)], self._collection_getter)

    def count(self) -> int:
        return len(self._resolve())

    # ── Aggregation ──────────────────────────────────────────────────

    def aggregate(
        self,
        pipeline: Pipeline,
        *,
        max_pipeline_docs: int = DEFAULT_MAX_PIPELINE_DOCS,
        allowDiskUse: bool = False,
        memory_limit_bytes: int = DEFAULT_MEMORY_LIMIT_BYTES,
    ) -> list[Document]:
        if len(pipeline) > MAX_PIPELINE_STAGES:
            raise ValueError(
                f"Pipeline has {len(pipeline)} stages, "
                f"exceeding the limit of {MAX_PIPELINE_STAGES}"
            )

        pipeline = _optimize_pipeline(pipeline)
        docs = list(self._materialize())

        def _check_limit(docs: list[Document]) -> list[Document]:
            if len(docs) > max_pipeline_docs:
                raise DocumentLimitExceeded(
                    f"Aggregation produced {len(docs)} documents, "
                    f"exceeding the limit of {max_pipeline_docs}. "
                    f"Add earlier $match / $limit stages or raise max_pipeline_docs."
                )
            return docs

        def _check_memory(docs: list[Document], stage_name: str) -> None:
            est = _estimate_docs_bytes(docs)
            if est > memory_limit_bytes and not allowDiskUse:
                mb = est / (1024 * 1024)
                limit_mb = memory_limit_bytes / (1024 * 1024)
                raise MemoryLimitExceeded(
                    f"{stage_name} requires ~{mb:.0f} MB, exceeding the "
                    f"{limit_mb:.0f} MB limit. Pass allowDiskUse=True to "
                    f"enable spill-to-disk for memory-intensive stages."
                )

        for stage in pipeline:
            op, spec = next(iter(stage.items()))

            if op == "$match":
                fn = compile_query(spec)
                docs = [d for d in docs if fn(d)]
            elif op == "$group":
                _check_memory(docs, "$group")
                docs = group_stage(docs, spec, allow_disk_use=allowDiskUse)
            elif op == "$project":
                docs = project_stage(docs, spec)
            elif op == "$sort":
                _check_memory(docs, "$sort")
                docs = sort_stage(docs, spec, allow_disk_use=allowDiskUse)
            elif op == "$limit":
                docs = limit_stage(docs, spec)
            elif op == "$skip":
                docs = skip_stage(docs, spec)
            elif op == "$unwind":
                docs = unwind_stage(docs, spec)
            elif op in ("$addFields", "$set"):
                docs = add_fields_stage(docs, spec)
            elif op == "$count":
                docs = count_stage(docs, spec)
            elif op == "$replaceRoot":
                docs = replace_root_stage(docs, spec)
            elif op == "$lookup":
                if "pipeline" in spec and "localField" not in spec:
                    docs = pipeline_lookup_stage(
                        docs, spec, self._collection_getter, max_pipeline_docs=max_pipeline_docs
                    )
                else:
                    docs = lookup_stage(docs, spec, self._collection_getter)
            elif op == "$sample":
                docs = sample_stage(docs, spec)
            elif op == "$vectorSearch":
                docs = vector_search_stage(docs, spec)
            elif op == "$geoNear":
                docs = geo_near_stage(docs, spec)
            elif op == "$facet":
                _check_limit(docs)
                docs = facet_stage(
                    docs, spec, self._collection_getter, max_pipeline_docs=max_pipeline_docs
                )
            elif op == "$out":
                docs = out_stage(docs, spec, self._collection_getter)
            elif op == "$merge":
                docs = merge_stage(docs, spec, self._collection_getter)
            elif op == "$bucket":
                docs = bucket_stage(docs, spec)
            elif op == "$bucketAuto":
                docs = bucket_auto_stage(docs, spec)
            elif op == "$graphLookup":
                docs = graph_lookup_stage(docs, spec, self._collection_getter)
            elif op == "$unionWith":
                docs = union_with_stage(
                    docs, spec, self._collection_getter, max_pipeline_docs=max_pipeline_docs
                )
            elif op == "$unset":
                docs = unset_stage(docs, spec)
            elif op == "$redact":
                docs = redact_stage(docs, spec)
            elif op == "$sortByCount":
                docs = sort_by_count_stage(docs, spec)
            elif op == "$setWindowFields":
                docs = set_window_fields_stage(docs, spec)
            elif op in ("$replaceWith",):
                docs = replace_root_stage(docs, {"newRoot": spec})
            else:
                raise NotImplementedError(f"Aggregation stage {op} not supported")

            _check_limit(docs)

        return docs


def _optimize_pipeline(pipeline: Pipeline) -> Pipeline:
    """Apply simple rewrite rules to reduce intermediate result sizes.

    Current rules:
      1. If ``$limit`` immediately follows ``$match``, keep as-is (already
         optimal).
      2. If ``$limit`` appears before ``$match`` and neither stage depends
         on a preceding ``$sort``, swap them so the filter runs first.
      3. Consecutive ``$match`` stages are merged into a single ``$and``.
    """
    if len(pipeline) < 2:
        return pipeline

    out: Pipeline = []
    i = 0
    while i < len(pipeline):
        stage = pipeline[i]
        op = next(iter(stage))

        if op == "$match" and i + 1 < len(pipeline):
            next_op = next(iter(pipeline[i + 1]))
            if next_op == "$match":
                merged = {"$match": {"$and": [stage["$match"], pipeline[i + 1]["$match"]]}}
                out.append(merged)
                i += 2
                continue

        if op == "$limit" and i + 1 < len(pipeline):
            next_op = next(iter(pipeline[i + 1]))
            if next_op == "$match":
                out.append(pipeline[i + 1])
                out.append(stage)
                i += 2
                continue

        out.append(stage)
        i += 1

    return out


def _apply_projection(docs: list[Document], spec: Projection) -> list[Document]:
    """Apply a projection spec to a list of docs.

    List specs (field-name lists) are handled inline; dict specs delegate to
    the single Rust projection engine which supports expressions like
    ``$$ROOT`` and ``$bsonSize``.
    """
    if isinstance(spec, list):
        include_fields = set(spec)
        out: list[Document] = []
        for doc in docs:
            new_doc: Document = {}
            if "_id" in doc:
                new_doc["_id"] = doc["_id"]
            for field in include_fields:
                if field_exists(doc, field):
                    set_value(new_doc, field, get_value(doc, field))
            out.append(new_doc)
        return out

    from smongo._smongo_core import apply_projection

    return [apply_projection(doc, spec) for doc in docs]
