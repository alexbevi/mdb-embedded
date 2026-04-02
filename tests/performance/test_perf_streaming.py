"""Streaming read-path performance benchmarks.

Compares streaming (lazy) reads against materialized reads to quantify
the benefit of the streaming architecture for common access patterns.

Uses two fixture layers:
- perf_collection (Collection facade) for client-level API benchmarks
- perf_local_collection (LocalCollection) for storage-level streaming
"""

import random

import pytest

from smongo.aggregation import Cursor
from smongo.storage import LocalClient

pytestmark = pytest.mark.performance


@pytest.fixture
def local_client(tmp_path):
    return LocalClient(str(tmp_path / "stream_wt"))


@pytest.fixture
def local_coll(local_client):
    return local_client.get_db("perf").get_collection("users")


@pytest.fixture
def populated_local_coll(local_coll, docs_10k):
    local_coll.insert_many(docs_10k)
    return local_coll


@pytest.fixture
def indexed_local_coll(populated_local_coll):
    populated_local_coll.create_index([("city", 1)])
    populated_local_coll.create_index([("age", 1)])
    return populated_local_coll


# ── Client-level streaming benchmarks ────────────────────────────────


def test_find_one_streaming_vs_full(benchmark, perf_collection, docs_10k):
    """find_one via the client facade (delegates to LocalCollection.find_one)."""
    perf_collection.insert_many(docs_10k)
    perf_collection.create_index([("city", 1)])

    benchmark(lambda: perf_collection.find_one({"city": "city_3"}))


def test_count_documents_empty_query(benchmark, perf_collection, docs_10k):
    """count_documents({}) -- delegates to count_fast (no BSON decode)."""
    perf_collection.insert_many(docs_10k)

    benchmark(lambda: perf_collection.count_documents({}))


def test_count_documents_filtered(benchmark, perf_collection, docs_10k):
    """count_documents with filter -- streams without building a list."""
    perf_collection.insert_many(docs_10k)
    perf_collection.create_index([("age", 1)])

    benchmark(lambda: perf_collection.count_documents({"age": {"$gt": 50}}))


def test_find_limit_10(benchmark, perf_collection, docs_10k):
    """find({}).limit(10) -- streaming cursor only decodes ~10 docs."""
    perf_collection.insert_many(docs_10k)

    def run():
        perf_collection.find({}).limit(10).to_list()

    benchmark(run)


# ── Storage-level streaming benchmarks ───────────────────────────────


def test_streaming_find_one(benchmark, indexed_local_coll):
    """LocalCollection.find_one via streaming -- single doc deserialized."""
    benchmark(lambda: indexed_local_coll.find_one({"city": "city_3"}))


def test_streaming_count_empty(benchmark, populated_local_coll):
    """LocalCollection.count({}) -- fast path, no BSON decode."""
    benchmark(lambda: populated_local_coll.count({}))


def test_streaming_count_filtered(benchmark, indexed_local_coll):
    """LocalCollection.count with filter -- iterates without list."""
    benchmark(lambda: indexed_local_coll.count({"age": {"$gt": 50}}))


def test_streaming_limit_10(benchmark, populated_local_coll):
    """StreamingCursor + Cursor.limit(10) -- only 10 docs from WiredTiger."""

    def run():
        sc = populated_local_coll.find_streaming({})
        Cursor(sc).limit(10).to_list()

    benchmark(run)


def test_streaming_index_scan(benchmark, indexed_local_coll):
    """Index scan via find_streaming -- one doc at a time from the index."""
    benchmark(lambda: list(indexed_local_coll.find_streaming({"city": "city_5"})))


def test_streaming_pk_lookup(benchmark, populated_local_coll, docs_10k):
    """PK lookup via find_streaming -- single doc."""
    target = random.choice(docs_10k)["_id"]

    benchmark(lambda: list(populated_local_coll.find_streaming({"_id": target})))


def test_streaming_in_scan(benchmark, indexed_local_coll):
    """$in multi-point index scan via find_streaming."""
    benchmark(
        lambda: list(
            indexed_local_coll.find_streaming({"city": {"$in": ["city_1", "city_3", "city_7"]}})
        )
    )


def test_streaming_collection_scan(benchmark, populated_local_coll):
    """Full collection scan via find_streaming (baseline comparison)."""
    benchmark(lambda: list(populated_local_coll.find_streaming({"age": {"$gt": 25}})))
