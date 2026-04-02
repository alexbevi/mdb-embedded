from __future__ import annotations

from copy import deepcopy
from typing import Any

from .._types import Document
from ..query import compile_query, get_value

try:
    import numpy as _np
except ImportError:  # pragma: no cover - numpy is expected in normal runtime
    _np = None  # type: ignore[assignment]

try:
    from usearch.index import Index as _USearchIndex
except ImportError:  # pragma: no cover - optional fast backend
    _USearchIndex = None  # type: ignore[assignment, misc]


def _vector_search_numpy(
    vectors: Any, query_vec: Any, limit: int, metric: str
) -> list[tuple[int, float]]:
    if metric == "cosine":
        qnorm = _np.linalg.norm(query_vec)
        if qnorm == 0:
            return []
        vnorm = _np.linalg.norm(vectors, axis=1)
        safe = _np.where(vnorm == 0, 1.0, vnorm)
        scores = (vectors @ query_vec) / (safe * qnorm)
        order = _np.argsort(-scores)[:limit]
        return [(int(i), float(scores[i])) for i in order]
    if metric == "euclidean":
        dists = _np.linalg.norm(vectors - query_vec, axis=1)
        order = _np.argsort(dists)[:limit]
        return [(int(i), float(-dists[i])) for i in order]
    raise ValueError(f"Unsupported vector metric: {metric}")


def _vector_search_usearch(
    vectors: Any, query_vec: Any, limit: int, metric: str
) -> list[tuple[int, float]]:
    metric_map = {"cosine": "cos", "euclidean": "l2sq"}
    if metric not in metric_map:
        raise ValueError(f"Unsupported vector metric: {metric}")
    idx = _USearchIndex(ndim=vectors.shape[1], metric=metric_map[metric])
    keys = _np.arange(vectors.shape[0], dtype=_np.int64)
    idx.add(keys, vectors)
    matches = idx.search(query_vec, min(limit, vectors.shape[0]))
    out: list[tuple[int, float]] = []
    for k, d in zip(matches.keys, matches.distances):
        score = float(-d) if metric == "euclidean" else float(1.0 - d)
        out.append((int(k), score))
    return out


def vector_search_stage(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """
    In-memory vector search stage.

    Supported spec fields:
      - path: dot-path to vector field
      - queryVector: list[float]
      - limit: result size (default 10)
      - numCandidates: pre-limit candidate count
      - filter: optional MQL filter applied before vector scoring
      - metric: "cosine" (default) or "euclidean"
      - scoreField: output field for similarity score (default "_vectorScore")
    """
    if _np is None:
        raise RuntimeError("NumPy is required for $vectorSearch")

    path = spec.get("path")
    query_vector = spec.get("queryVector")
    if not path or not isinstance(query_vector, list) or not query_vector:
        raise ValueError("$vectorSearch requires non-empty 'path' and 'queryVector'")

    limit = int(spec.get("limit", 10))
    num_candidates = int(spec.get("numCandidates", max(limit, len(docs))))
    metric: str = spec.get("metric", "cosine")
    score_field: str = spec.get("scoreField", "_vectorScore")
    mql_filter = spec.get("filter")

    query_arr = _np.asarray(query_vector, dtype=_np.float32)
    dim = int(query_arr.shape[0])

    candidates = docs
    if mql_filter:
        fn = compile_query(mql_filter)
        candidates = [d for d in docs if fn(d)]

    vec_rows: list[Any] = []
    doc_rows: list[Document] = []
    for doc in candidates:
        v = get_value(doc, path)
        if not isinstance(v, list) or len(v) != dim:
            continue
        try:
            vec_rows.append(_np.asarray(v, dtype=_np.float32))
            doc_rows.append(doc)
        except (TypeError, ValueError):
            continue

    if not vec_rows:
        return []

    vectors = _np.vstack(vec_rows)
    search_k = min(max(1, num_candidates), len(doc_rows))

    if _USearchIndex is not None:
        ranked = _vector_search_usearch(vectors, query_arr, search_k, metric)
    else:
        ranked = _vector_search_numpy(vectors, query_arr, search_k, metric)

    out: list[Document] = []
    for idx_val, score in ranked[:limit]:
        d = deepcopy(doc_rows[idx_val])
        d[score_field] = score
        out.append(d)
    return out
