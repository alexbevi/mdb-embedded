"""Aggregation performance benchmarks."""

import pytest

pytestmark = pytest.mark.performance


def test_match_group_10k(benchmark, perf_collection, docs_10k):
    perf_collection.insert_many(docs_10k)
    pipeline = [
        {"$match": {"age": {"$gte": 25}}},
        {"$group": {"_id": "$city", "count": {"$sum": 1}, "avg_salary": {"$avg": "$salary"}}},
    ]
    benchmark(lambda: perf_collection.aggregate(pipeline))


def test_sort_10k(benchmark, perf_collection, docs_10k):
    perf_collection.insert_many(docs_10k)
    pipeline = [{"$sort": {"salary": -1, "age": 1}}]
    benchmark(lambda: perf_collection.aggregate(pipeline))


def test_lookup_1k_to_1k(benchmark, perf_client, docs_1k):
    db = perf_client["perf_lookup"]
    users = db["users"]
    depts = db["depts"]
    users.insert_many(
        [{"_id": d["_id"], "dept": f"dept_{int(d['_id'].split('_')[1]) % 20}"} for d in docs_1k]
    )
    depts.insert_many([{"_id": f"d{i}", "name": f"dept_{i}"} for i in range(20)])
    pipeline = [
        {
            "$lookup": {
                "from": "depts",
                "localField": "dept",
                "foreignField": "name",
                "as": "dept_docs",
            }
        }
    ]
    benchmark(lambda: users.aggregate(pipeline))


def test_unwind_arrays(benchmark, perf_collection, docs_1k):
    docs = [{"_id": d["_id"], "arr": list(range(10))} for d in docs_1k]
    perf_collection.insert_many(docs)
    benchmark(lambda: perf_collection.aggregate([{"$unwind": "$arr"}]))


def test_full_pipeline_10k(benchmark, perf_collection, docs_10k):
    perf_collection.insert_many(docs_10k)
    pipeline = [
        {"$match": {"age": {"$gte": 20}}},
        {"$group": {"_id": "$dept", "avg_salary": {"$avg": "$salary"}, "count": {"$sum": 1}}},
        {"$sort": {"avg_salary": -1}},
        {"$project": {"dept": "$_id", "avg_salary": 1, "count": 1}},
    ]
    benchmark(lambda: perf_collection.aggregate(pipeline))
