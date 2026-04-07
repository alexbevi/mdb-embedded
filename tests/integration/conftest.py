"""Integration fixtures using Docker MongoDB via testcontainers."""

import time
import uuid

import pytest
from pymongo import MongoClient as PyMongoClient
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from smongo import MongoClient as EmbeddedClient
from smongo import SyncManager


@pytest.fixture(scope="session")
def mongo_container():
    """Real MongoDB container running as a single-node replica set."""
    container = DockerContainer("mongo:7")
    container.with_exposed_ports(27017)
    container.with_command("mongod --replSet rs0 --bind_ip_all")
    container.start()
    wait_for_logs(container, "Waiting for connections")

    host = container.get_container_host_ip()
    port = container.get_exposed_port(27017)
    direct_uri = f"mongodb://{host}:{port}/?directConnection=true"
    client = PyMongoClient(direct_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("replSetInitiate")
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if client.admin.command("hello").get("isWritablePrimary"):
                break
        except Exception:
            pass
        time.sleep(0.5)
    else:
        client.close()
        container.stop()
        raise RuntimeError("MongoDB replica set failed to elect primary")
    client.close()

    yield container
    container.stop()


@pytest.fixture(scope="session")
def mongo_uri(mongo_container):
    host = mongo_container.get_container_host_ip()
    port = mongo_container.get_exposed_port(27017)
    return f"mongodb://{host}:{port}/?directConnection=true"


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
