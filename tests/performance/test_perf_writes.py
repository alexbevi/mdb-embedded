"""Write-path performance benchmarks."""

import pytest

pytestmark = pytest.mark.performance


def test_insert_one_throughput(benchmark, perf_collection):
    counter = {"i": 0}

    def run():
        i = counter["i"]
        perf_collection.insert_one({"_id": f"i1_{i}", "x": i})
        counter["i"] += 1

    benchmark(run)


def test_insert_many_1000(benchmark, perf_collection, docs_1k):
    def run():
        perf_collection.delete_many({})
        perf_collection.insert_many(docs_1k)

    benchmark(run)


def test_update_many_without_index(benchmark, perf_collection, docs_1k):
    perf_collection.insert_many(docs_1k)

    def run():
        perf_collection.update_many({"dept": "dept_1"}, {"$inc": {"salary": 1}})

    benchmark(run)


def test_update_many_with_index(benchmark, perf_collection, docs_1k):
    perf_collection.insert_many(docs_1k)
    perf_collection.create_index([("dept", 1)])

    def run():
        perf_collection.update_many({"dept": "dept_1"}, {"$inc": {"salary": 1}})

    benchmark(run)


def test_delete_many_1000(benchmark, perf_collection, docs_1k):
    def run():
        perf_collection.delete_many({})
        perf_collection.insert_many(docs_1k)
        perf_collection.delete_many({"age": {"$gte": 0}})

    benchmark(run)
