"""Benchmarks for Rust/Python interop overhead: dispatch, import caching, cursor lifecycle."""

import pytest

from smongo import MongoClient
from smongo._smongo_core import from_bson, to_bson
from smongo.objectid import ObjectId


@pytest.fixture
def wire_client(tmp_path):
    """Client with a small dataset for dispatch-level benchmarks."""
    c = MongoClient(f"local://{tmp_path}/interop_redb")
    coll = c["bench"]["items"]
    coll.insert_many([{"_id": f"d{i}", "x": i, "tag": f"t{i % 5}"} for i in range(200)])
    coll.create_index([("x", 1)])
    return c


@pytest.fixture
def wire_coll(wire_client):
    return wire_client["bench"]["items"]


@pytest.mark.performance
def test_dispatch_find_simple(benchmark, wire_coll):
    """Measure full dispatch round-trip for a simple find (filter + limit)."""

    def go():
        return list(wire_coll.find({"x": {"$gt": 100}}).limit(10))

    benchmark(go)


@pytest.mark.performance
def test_dispatch_insert_delete_cycle(benchmark, wire_coll):
    """Measure insert + delete dispatch overhead (one doc each)."""
    _counter = [0]

    def go():
        _counter[0] += 1
        key = f"_bench_{_counter[0]}"
        wire_coll.insert_one({"_id": key, "val": 1})
        wire_coll.delete_one({"_id": key})

    benchmark(go)


@pytest.mark.performance
def test_dispatch_aggregate_small(benchmark, wire_coll):
    """Measure aggregate pipeline dispatch on a small collection."""

    def go():
        return list(
            wire_coll.aggregate(
                [
                    {"$match": {"x": {"$gte": 50}}},
                    {"$group": {"_id": "$tag", "total": {"$sum": "$x"}}},
                    {"$sort": {"total": -1}},
                ]
            )
        )

    benchmark(go)


@pytest.mark.performance
def test_dispatch_find_all(benchmark, wire_coll):
    """Measure full find materializing all 200 docs through the dispatch path."""

    def go():
        return list(wire_coll.find({}))

    benchmark(go)


@pytest.mark.performance
def test_dispatch_count(benchmark, wire_coll):
    """Measure count command dispatch."""

    def go():
        return wire_coll.count_documents({"tag": "t2"})

    benchmark(go)


@pytest.mark.performance
def test_dispatch_findandmodify(benchmark, wire_coll):
    """Measure findAndModify dispatch (update one doc)."""

    def go():
        wire_coll.find_one_and_update({"x": 42}, {"$inc": {"x": 0}})

    benchmark(go)


# -- P8: raw BSON encode/decode microbenchmarks --------------------------------


@pytest.fixture
def sample_doc():
    return {
        "_id": ObjectId(),
        "name": "benchmark",
        "value": 42,
        "price": 19.99,
        "tags": ["a", "b", "c"],
        "nested": {"x": 1, "y": 2},
        "active": True,
    }


@pytest.fixture
def sample_bson(sample_doc):
    return to_bson(sample_doc)


@pytest.mark.performance
def test_raw_bson_encode(benchmark, sample_doc):
    """Measure raw BSON encode (single doc, no bson::Document intermediate)."""
    benchmark(to_bson, sample_doc)


@pytest.mark.performance
def test_raw_bson_decode(benchmark, sample_bson):
    """Measure raw BSON decode (single doc, no bson::Document intermediate)."""
    benchmark(from_bson, sample_bson)


@pytest.mark.performance
def test_raw_bson_roundtrip(benchmark, sample_doc):
    """Measure full encode+decode round-trip."""

    def go():
        return from_bson(to_bson(sample_doc))

    benchmark(go)
