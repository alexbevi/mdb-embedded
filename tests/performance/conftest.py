"""Performance test fixtures and regression threshold enforcement."""

import pytest

from smongo import MongoClient
from smongo.query import compile_query

# Maximum allowed mean time (seconds) per benchmark.
# These are generous ceilings; tighten as the engine improves.
PERF_THRESHOLDS: dict[str, float] = {
    "test_insert_one_throughput": 0.010,
    "test_insert_many_1000": 2.0,
    "test_update_many_without_index": 1.0,
    "test_update_many_with_index": 1.0,
    "test_delete_many_1000": 3.0,
    "test_collection_scan_query": 1.0,
    "test_pk_lookup_query": 0.005,
    "test_index_scan_query": 0.050,
    "test_compound_index_range_query": 0.100,
    "test_compile_query_complex": 0.001,
    "test_match_group_10k": 2.0,
    "test_sort_10k": 2.0,
    "test_lookup_1k_to_1k": 2.0,
    "test_unwind_arrays": 1.0,
    "test_full_pipeline_10k": 3.0,
}


def pytest_benchmark_compare_machine_info(config, benchmarksession):  # type: ignore[no-untyped-def]
    """Hook: after benchmarks run, check mean times against thresholds."""
    pass


@pytest.fixture(autouse=True)
def _check_perf_threshold(request, benchmark):  # type: ignore[no-untyped-def]
    """After each benchmark, assert its mean stays under the threshold ceiling."""
    yield
    stats = getattr(benchmark, "stats", None)
    if stats is None:
        return
    test_name = request.node.name
    threshold = PERF_THRESHOLDS.get(test_name)
    if threshold is not None:
        mean = stats.stats.mean
        assert mean < threshold, (
            f"Performance regression: {test_name} mean={mean:.4f}s exceeds "
            f"threshold={threshold:.4f}s"
        )


@pytest.fixture
def perf_client(tmp_path):
    return MongoClient(f"local+wt://{tmp_path}/perf_wt")


@pytest.fixture
def perf_collection(perf_client):
    return perf_client["perf"]["users"]


@pytest.fixture
def docs_1k():
    return [
        {
            "_id": f"id_{i}",
            "name": f"user_{i}",
            "age": i % 80,
            "city": f"city_{i % 10}",
            "dept": f"dept_{i % 5}",
            "tags": [f"t{i % 3}", f"t{i % 7}"],
            "salary": 50000 + (i % 100) * 1000,
        }
        for i in range(1000)
    ]


@pytest.fixture
def docs_10k():
    return [
        {
            "_id": f"id_{i}",
            "name": f"user_{i}",
            "age": i % 80,
            "city": f"city_{i % 10}",
            "dept": f"dept_{i % 5}",
            "tags": [f"t{i % 3}", f"t{i % 7}"],
            "salary": 50000 + (i % 100) * 1000,
        }
        for i in range(10000)
    ]


@pytest.fixture
def compiled_complex_query():
    return compile_query(
        {
            "$and": [
                {"age": {"$gte": 25, "$lte": 60}},
                {"city": {"$in": ["city_1", "city_2", "city_3"]}},
                {"tags": {"$elemMatch": {"value": {"$regex": "^t"}}}},
            ]
        }
    )
