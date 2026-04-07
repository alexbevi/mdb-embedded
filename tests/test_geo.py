"""Tests for $geoNear aggregation stage and geospatial stubs.

Covers:
- Haversine distance computation
- GeoJSON Point and legacy [lon, lat] coordinate extraction
- $geoNear stage: distanceField, key, maxDistance, minDistance,
  distanceMultiplier, query filter, includeLocs, limit
- Nearest-first sort ordering
- Stub errors for $near, $nearSphere, $geoWithin, $geoIntersects
- Stub error for 2dsphere/2d index creation
"""

from __future__ import annotations

import math
from typing import Any

import pytest

from smongo.aggregation.geo import (
    EARTH_RADIUS_METERS,
    _extract_coords,
    _haversine,
    geo_near_stage,
)

# ── Test data ────────────────────────────────────────────────────────

NYC = {"type": "Point", "coordinates": [-73.9857, 40.7484]}
SF = {"type": "Point", "coordinates": [-122.4194, 37.7749]}
LA = {"type": "Point", "coordinates": [-118.2437, 34.0522]}
CHI = {"type": "Point", "coordinates": [-87.6298, 41.8781]}
LONDON = {"type": "Point", "coordinates": [-0.1278, 51.5074]}


@pytest.fixture
def places() -> list[dict[str, Any]]:
    return [
        {"_id": "1", "name": "Times Square", "location": NYC, "city": "NYC"},
        {"_id": "2", "name": "Golden Gate", "location": SF, "city": "SF"},
        {"_id": "3", "name": "Hollywood Sign", "location": LA, "city": "LA"},
        {"_id": "4", "name": "Willis Tower", "location": CHI, "city": "CHI"},
        {"_id": "5", "name": "Big Ben", "location": LONDON, "city": "London"},
    ]


# ── Haversine ────────────────────────────────────────────────────────


class TestHaversine:
    def test_same_point_zero_distance(self):
        assert _haversine(-73.98, 40.74, -73.98, 40.74) == 0.0

    def test_nyc_to_sf(self):
        dist = _haversine(-73.9857, 40.7484, -122.4194, 37.7749)
        assert 4_100_000 < dist < 4_200_000

    def test_nyc_to_london(self):
        dist = _haversine(-73.9857, 40.7484, -0.1278, 51.5074)
        assert 5_500_000 < dist < 5_600_000

    def test_symmetry(self):
        d1 = _haversine(-73.98, 40.74, -122.41, 37.77)
        d2 = _haversine(-122.41, 37.77, -73.98, 40.74)
        assert d1 == pytest.approx(d2)

    def test_antipodal_points(self):
        dist = _haversine(0, 0, 180, 0)
        assert dist == pytest.approx(math.pi * EARTH_RADIUS_METERS, rel=1e-6)


# ── Coordinate extraction ────────────────────────────────────────────


class TestExtractCoords:
    def test_geojson_point(self):
        assert _extract_coords(NYC) == (-73.9857, 40.7484)

    def test_legacy_array(self):
        assert _extract_coords([-73.98, 40.74]) == (-73.98, 40.74)

    def test_legacy_tuple(self):
        assert _extract_coords((-73.98, 40.74)) == (-73.98, 40.74)

    def test_invalid_dict(self):
        assert _extract_coords({"type": "LineString", "coordinates": []}) is None

    def test_invalid_type(self):
        assert _extract_coords("not a point") is None

    def test_short_array(self):
        assert _extract_coords([1.0]) is None

    def test_none(self):
        assert _extract_coords(None) is None


# ── $geoNear stage ───────────────────────────────────────────────────


class TestGeoNearStage:
    def test_basic_nearest_sort(self, places):
        result = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
            },
        )
        assert len(result) == 5
        assert result[0]["name"] == "Times Square"
        assert result[0]["dist"] == pytest.approx(0.0, abs=1.0)
        names = [d["name"] for d in result]
        assert names[0] == "Times Square"

    def test_sorted_nearest_first(self, places):
        result = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
            },
        )
        distances = [d["dist"] for d in result]
        assert distances == sorted(distances)

    def test_max_distance_filter(self, places):
        result = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
                "maxDistance": 1_500_000,
            },
        )
        for doc in result:
            assert doc["dist"] <= 1_500_000
        names = {d["name"] for d in result}
        assert "Times Square" in names
        assert "Big Ben" not in names

    def test_min_distance_filter(self, places):
        result = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
                "minDistance": 1_000_000,
            },
        )
        for doc in result:
            assert doc["dist"] >= 1_000_000
        names = {d["name"] for d in result}
        assert "Times Square" not in names

    def test_limit(self, places):
        result = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
                "limit": 2,
            },
        )
        assert len(result) == 2

    def test_distance_multiplier(self, places):
        base = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
            },
        )
        scaled = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
                "distanceMultiplier": 0.001,
            },
        )
        for b, s in zip(base, scaled):
            assert s["dist"] == pytest.approx(b["dist"] * 0.001, rel=1e-9)

    def test_query_filter(self, places):
        result = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
                "query": {"city": "SF"},
            },
        )
        assert len(result) == 1
        assert result[0]["name"] == "Golden Gate"

    def test_include_locs(self, places):
        result = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
                "includeLocs": "matchedLocation",
            },
        )
        assert result[0]["matchedLocation"] == NYC

    def test_custom_key(self):
        docs = [
            {"_id": "1", "name": "A", "geo": {"coords": NYC}},
            {"_id": "2", "name": "B", "geo": {"coords": SF}},
        ]
        result = geo_near_stage(
            docs,
            {
                "near": NYC,
                "distanceField": "dist",
                "key": "geo.coords",
            },
        )
        assert len(result) == 2
        assert result[0]["name"] == "A"

    def test_legacy_coordinate_array(self):
        docs = [
            {"_id": "1", "name": "A", "location": [-73.9857, 40.7484]},
            {"_id": "2", "name": "B", "location": [-122.4194, 37.7749]},
        ]
        result = geo_near_stage(
            docs,
            {
                "near": [-73.9857, 40.7484],
                "distanceField": "dist",
            },
        )
        assert result[0]["name"] == "A"
        assert result[0]["dist"] == pytest.approx(0.0, abs=1.0)

    def test_skips_docs_without_location(self, places):
        places.append({"_id": "6", "name": "No Location", "city": "?"})
        result = geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
            },
        )
        names = {d["name"] for d in result}
        assert "No Location" not in names
        assert len(result) == 5

    def test_does_not_mutate_input(self, places):
        originals = [dict(d) for d in places]
        geo_near_stage(
            places,
            {
                "near": NYC,
                "distanceField": "dist",
            },
        )
        for orig, doc in zip(originals, places):
            assert "dist" not in doc
            assert doc["name"] == orig["name"]

    def test_empty_input(self):
        result = geo_near_stage(
            [],
            {
                "near": NYC,
                "distanceField": "dist",
            },
        )
        assert result == []

    def test_missing_near_raises(self, places):
        with pytest.raises(ValueError, match="requires 'near'"):
            geo_near_stage(places, {"distanceField": "dist"})

    def test_missing_distance_field_raises(self, places):
        with pytest.raises(ValueError, match="requires 'distanceField'"):
            geo_near_stage(places, {"near": NYC})

    def test_invalid_near_raises(self, places):
        with pytest.raises(ValueError, match="GeoJSON Point"):
            geo_near_stage(
                places,
                {
                    "near": "not a point",
                    "distanceField": "dist",
                },
            )

    def test_spherical_false_raises(self, places):
        with pytest.raises(NotImplementedError, match="spherical=false"):
            geo_near_stage(
                places,
                {
                    "near": NYC,
                    "distanceField": "dist",
                    "spherical": False,
                },
            )


# ── Stub error tests ─────────────────────────────────────────────────


class TestGeoStubs:
    def test_2dsphere_index_raises(self, local_collection, sample_docs):
        local_collection.insert_many(sample_docs)
        with pytest.raises(NotImplementedError, match="2dsphere indexes are planned"):
            local_collection.create_index([("location", "2dsphere")])

    def test_2d_index_raises(self, local_collection, sample_docs):
        local_collection.insert_many(sample_docs)
        with pytest.raises(NotImplementedError, match="2d indexes are planned"):
            local_collection.create_index([("location", "2d")])
