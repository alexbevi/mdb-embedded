"""Shared fixtures for the smongo test suite.

Default fixtures use the Rust-native ``RedbLocalClient`` / ``RedbLocalDB`` /
``RedbLocalCollection`` types -- the same types that ``WireServer`` creates in
production.  Tests that specifically exercise the Python ``RedbClient`` wrapper
(e.g. ``test_storage.py``, ``test_streaming.py``) override these fixtures
locally with wrapper-based equivalents.
"""

import pytest

import smongo._smongo_core as _core
from smongo._smongo_core import RedbLocalClient

_REQUIRED_METHODS = [
    "insert_one",
    "insert_many",
    "find",
    "find_one",
    "find_iter",
    "get_all",
    "update_one",
    "update_many",
    "delete_one",
    "delete_many",
    "count_documents",
    "find_one_and_update",
    "find_one_and_delete",
    "find_one_and_replace",
    "aggregate_engine",
    "create_index",
    "drop_index",
    "list_indexes",
    "verify",
    "storage_stats",
    "explain",
    "get_oplog",
    "compact_oplog",
    "rebuild_all_indexes",
]


def pytest_configure(config):
    """Fail fast when the native extension is stale or was never compiled."""
    missing = [m for m in _REQUIRED_METHODS if not hasattr(_core.RedbLocalCollection, m)]
    if missing:
        raise SystemExit(
            f"Stale native extension: RedbLocalCollection is missing {missing}. "
            "Run `make build-debug` or `pip install -e .` from the repo root."
        )


@pytest.fixture
def tmp_redb_dir(tmp_path):
    """Fresh temporary directory for embedded redb data files."""
    return str(tmp_path / "redb_data")


@pytest.fixture
def local_client(tmp_redb_dir):
    """Rust-native RedbLocalClient for a fresh temp directory."""
    client = RedbLocalClient(tmp_redb_dir)
    yield client
    client.close()


@pytest.fixture
def durable_client(tmp_redb_dir):
    """Alias for local_client -- separate instance for tests needing two clients."""
    client = RedbLocalClient(tmp_redb_dir)
    yield client
    client.close()


@pytest.fixture
def local_db(local_client):
    return local_client.get_db("testdb")


@pytest.fixture
def local_collection(local_db):
    return local_db.get_collection("testcoll")


@pytest.fixture
def sample_docs():
    return [
        {
            "name": "Alice",
            "age": 34,
            "city": "NYC",
            "dept": "eng",
            "tags": ["py", "go"],
            "salary": 145000,
        },
        {
            "name": "Bob",
            "age": 28,
            "city": "SF",
            "dept": "eng",
            "tags": ["js", "react"],
            "salary": 128000,
        },
        {
            "name": "Charlie",
            "age": 40,
            "city": "NYC",
            "dept": "mgmt",
            "tags": ["py"],
            "salary": 175000,
        },
        {
            "name": "Diana",
            "age": 25,
            "city": "LA",
            "dept": "design",
            "tags": ["rust"],
            "salary": 98000,
        },
        {
            "name": "Eve",
            "age": 31,
            "city": "SF",
            "dept": "eng",
            "tags": ["py", "ml"],
            "salary": 155000,
        },
        {
            "name": "Frank",
            "age": 36,
            "city": "CHI",
            "dept": "eng",
            "tags": ["go", "k8s"],
            "salary": 140000,
        },
        {
            "name": "Grace",
            "age": 29,
            "city": "NYC",
            "dept": "data",
            "tags": ["py", "spark"],
            "salary": 135000,
        },
        {
            "name": "Hank",
            "age": 45,
            "city": "SF",
            "dept": "mgmt",
            "tags": ["strategy"],
            "salary": 190000,
        },
        {
            "name": "Ivy",
            "age": 27,
            "city": "LA",
            "dept": "design",
            "tags": ["figma"],
            "salary": 105000,
        },
        {
            "name": "Jack",
            "age": 33,
            "city": "NYC",
            "dept": "eng",
            "tags": ["java"],
            "salary": 142000,
        },
    ]


@pytest.fixture
def populated_collection(local_collection, sample_docs):
    """A collection pre-loaded with sample_docs."""
    local_collection.insert_many(sample_docs)
    return local_collection
