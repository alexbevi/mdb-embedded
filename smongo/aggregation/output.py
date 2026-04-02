from __future__ import annotations

from copy import deepcopy
from typing import Any

from .._types import CollectionGetter, Document, Pipeline
from .constants import DEFAULT_MAX_PIPELINE_DOCS

_OUT_BATCH_SIZE = 1_000


def facet_stage(
    docs: list[Document],
    spec: dict[str, Pipeline],
    collection_getter: CollectionGetter | None = None,
    *,
    max_pipeline_docs: int = DEFAULT_MAX_PIPELINE_DOCS,
) -> list[Document]:
    """
    $facet -- run multiple sub-pipelines against the same input documents.
    Returns a single document whose keys are the facet names and values
    are the arrays of results from each sub-pipeline.
    """
    from .cursor import Cursor

    result: Document = {}
    for facet_name, pipeline in spec.items():
        sub_cursor = Cursor(deepcopy(docs), collection_getter=collection_getter)
        result[facet_name] = sub_cursor.aggregate(pipeline, max_pipeline_docs=max_pipeline_docs)
    return [result]


def out_stage(
    docs: list[Document],
    spec: str | dict[str, Any],
    collection_getter: CollectionGetter | None = None,
) -> list[Document]:
    """
    $out -- write all pipeline results to a target collection (replaces contents).
    Must be the last stage.  Inserts are batched to avoid unbounded memory use
    during serialization.
    """
    if collection_getter is None:
        raise RuntimeError("$out requires a collection getter (not available in this context)")
    coll_name = spec if isinstance(spec, str) else spec.get("coll", spec.get("db", ""))
    target = collection_getter(coll_name)
    target.delete({}, multi=True)
    for i in range(0, len(docs), _OUT_BATCH_SIZE):
        batch = docs[i : i + _OUT_BATCH_SIZE]
        target.insert_many(batch)
    return docs


def merge_stage(
    docs: list[Document],
    spec: dict[str, Any],
    collection_getter: CollectionGetter | None = None,
) -> list[Document]:
    """
    $merge -- upsert pipeline results into a target collection.
    Supports whenMatched: "replace" (default) and whenNotMatched: "insert" (default).
    """
    if collection_getter is None:
        raise RuntimeError("$merge requires a collection getter (not available in this context)")
    into = spec.get("into", "")
    if isinstance(into, dict):
        coll_name = into.get("coll", "")
    else:
        coll_name = into
    target = collection_getter(coll_name)
    on = spec.get("on", "_id")
    when_matched: str = spec.get("whenMatched", "replace")
    when_not_matched: str = spec.get("whenNotMatched", "insert")

    _SUPPORTED_WHEN_MATCHED = ("replace", "merge", "keepExisting", "fail")
    _SUPPORTED_WHEN_NOT_MATCHED = ("insert", "discard", "fail")
    if when_matched not in _SUPPORTED_WHEN_MATCHED:
        raise ValueError(f"$merge unsupported whenMatched value: {when_matched!r}")
    if when_not_matched not in _SUPPORTED_WHEN_NOT_MATCHED:
        raise ValueError(f"$merge unsupported whenNotMatched value: {when_not_matched!r}")

    for doc in docs:
        match_key = doc.get(on) if isinstance(on, str) else {k: doc.get(k) for k in on}
        if isinstance(on, str):
            query: Document = {on: match_key}
        else:
            query = match_key  # type: ignore[assignment]

        existing = target.find(query)
        if existing:
            if when_matched == "replace":
                replacement = {k: v for k, v in doc.items() if k != "_id"}
                replacement["_id"] = existing[0]["_id"]
                target.update(
                    {"_id": existing[0]["_id"]},
                    {"$set": {k: v for k, v in replacement.items() if k != "_id"}},
                    multi=False,
                )
            elif when_matched == "merge":
                target.update(
                    {"_id": existing[0]["_id"]},
                    {"$set": {k: v for k, v in doc.items() if k != "_id"}},
                    multi=False,
                )
            elif when_matched == "fail":
                raise ValueError(f"$merge: document already exists with {on}={match_key}")
        else:
            if when_not_matched == "insert":
                target.insert_one(doc)
            elif when_not_matched == "fail":
                raise ValueError(f"$merge: no matching document found for {on}={match_key}")

    return docs
