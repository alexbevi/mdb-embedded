"""Geospatial aggregation stages.

Implements ``$geoNear`` -- computes Haversine distance from a query point to
each document's GeoJSON or legacy coordinate-pair field, filters by
min/max distance, and returns results sorted nearest-first.

No spatial index is required.  When a ``2dsphere`` index is added in the
future it will narrow candidates *before* this stage runs; the distance
math and output format stay the same.
"""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any

from .._types import Document
from ..query import compile_query, get_value

EARTH_RADIUS_METERS = 6_371_000.0


def _haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Great-circle distance in meters between two (lon, lat) points."""
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return EARTH_RADIUS_METERS * 2 * math.asin(math.sqrt(a))


def _extract_coords(value: Any) -> tuple[float, float] | None:
    """Extract ``(longitude, latitude)`` from a GeoJSON Point or ``[lon, lat]``."""
    if isinstance(value, dict):
        if value.get("type") == "Point":
            coords = value.get("coordinates")
            if isinstance(coords, list | tuple) and len(coords) >= 2:
                return (float(coords[0]), float(coords[1]))
        return None
    if isinstance(value, list | tuple) and len(value) >= 2:
        try:
            return (float(value[0]), float(value[1]))
        except (TypeError, ValueError):
            return None
    return None


def _extract_query_point(spec: dict[str, Any]) -> tuple[float, float]:
    """Parse the ``near`` field from a ``$geoNear`` spec."""
    near = spec.get("near")
    if near is None:
        raise ValueError("$geoNear requires 'near'")
    coords = _extract_coords(near)
    if coords is None:
        raise ValueError("$geoNear 'near' must be a GeoJSON Point or [longitude, latitude] array")
    return coords


def geo_near_stage(docs: list[Document], spec: dict[str, Any]) -> list[Document]:
    """``$geoNear`` aggregation stage.

    Supported spec fields (matches MongoDB):
      - near:               GeoJSON Point ``{"type": "Point", "coordinates": [lon, lat]}``
                            or ``[lon, lat]`` array  (**required**)
      - distanceField:      output field for computed distance  (**required**)
      - key:                path to the document's location field (default ``"location"``)
      - spherical:          use Haversine (default ``True``; flat not yet supported)
      - maxDistance:         upper bound in meters (inclusive)
      - minDistance:         lower bound in meters (inclusive)
      - distanceMultiplier: multiply computed distance before output
      - query:              MQL filter applied before distance computation
      - includeLocs:        output field to store the matched location value
      - limit:              max results (default: all matching docs)
    """
    query_lon, query_lat = _extract_query_point(spec)

    distance_field: str | None = spec.get("distanceField")
    if not distance_field:
        raise ValueError("$geoNear requires 'distanceField'")

    key: str = spec.get("key", "location")
    max_distance: float | None = spec.get("maxDistance")
    min_distance: float | None = spec.get("minDistance")
    multiplier: float = float(spec.get("distanceMultiplier", 1.0))
    include_locs: str | None = spec.get("includeLocs")
    limit: int | None = spec.get("limit")
    mql_filter = spec.get("query")

    if spec.get("spherical") is False:
        raise NotImplementedError(
            "$geoNear with spherical=false (flat/2d) is not yet supported; "
            "use spherical=true (the default) for Haversine distance"
        )

    candidates = docs
    if mql_filter:
        fn = compile_query(mql_filter)
        candidates = [d for d in docs if fn(d)]

    scored: list[tuple[float, Document, Any]] = []
    for doc in candidates:
        loc_value = get_value(doc, key)
        coords = _extract_coords(loc_value)
        if coords is None:
            continue
        dist = _haversine(query_lon, query_lat, coords[0], coords[1])
        if max_distance is not None and dist > max_distance:
            continue
        if min_distance is not None and dist < min_distance:
            continue
        scored.append((dist, doc, loc_value))

    scored.sort(key=lambda t: t[0])

    if limit is not None:
        scored = scored[:limit]

    out: list[Document] = []
    for dist, doc, loc_value in scored:
        d = deepcopy(doc)
        d[distance_field] = dist * multiplier
        if include_locs:
            d[include_locs] = deepcopy(loc_value)
        out.append(d)
    return out
