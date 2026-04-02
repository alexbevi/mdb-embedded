"""Read/query performance benchmarks."""

import random

import pytest

from smongo.query import compile_query

pytestmark = pytest.mark.performance


def test_collection_scan_query(benchmark, perf_collection, docs_10k):
    perf_collection.insert_many(docs_10k)

    def run():
        list(perf_collection.find({"age": {"$gt": 25}}))

    benchmark(run)


def test_pk_lookup_query(benchmark, perf_collection, docs_10k):
    perf_collection.insert_many(docs_10k)
    target = random.choice(docs_10k)["_id"]

    def run():
        list(perf_collection.find({"_id": target}))

    benchmark(run)


def test_index_scan_query(benchmark, perf_collection, docs_10k):
    perf_collection.insert_many(docs_10k)
    perf_collection.create_index([("age", 1)])

    def run():
        list(perf_collection.find({"age": 30}))

    benchmark(run)


def test_compound_index_range_query(benchmark, perf_collection, docs_10k):
    perf_collection.insert_many(docs_10k)
    perf_collection.create_index([("city", 1), ("age", 1)])

    def run():
        list(perf_collection.find({"city": "city_2", "age": {"$gte": 25, "$lte": 40}}))

    benchmark(run)


def test_compile_query_complex(benchmark):
    q = {
        "$and": [
            {"age": {"$gte": 21, "$lte": 65}},
            {"city": {"$in": ["city_1", "city_4", "city_9"]}},
            {"salary": {"$gt": 90000}},
            {"tags": {"$elemMatch": {"value": {"$regex": "^t"}}}},
        ]
    }
    benchmark(lambda: compile_query(q))
