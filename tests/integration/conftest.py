"""Integration fixtures using Docker MongoDB via testcontainers."""

import uuid

import pytest
from pymongo import MongoClient as PyMongoClient
from testcontainers.mongodb import MongoDbContainer

from smongo import MongoClient as EmbeddedClient
from smongo import SyncManager


@pytest.fixture(scope="session")
def mongo_container():
    """Real MongoDB container for integration tests."""
    with MongoDbContainer("mongo:7") as container:
        yield container


@pytest.fixture(scope="session")
def mongo_uri(mongo_container):
    return mongo_container.get_connection_url()


@pytest.fixture
def db_name():
    return f"itest_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def remote_client(mongo_uri):
    client = PyMongoClient(mongo_uri)
    yield client
    client.close()


@pytest.fixture
def embedded_client(tmp_path):
    client = EmbeddedClient(f"local://{tmp_path}/wt")
    yield client


@pytest.fixture
def sync_manager(embedded_client, mongo_uri):
    mgr = SyncManager(
        embedded_client,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "interval_sec": 1,
            "batch_size": 100,
            "conflict_resolution": "lww",
            "use_change_stream_pull": False,
        },
    )
    yield mgr
    mgr.stop()
