"""Shared fixtures for the smongo test suite."""

import os

import pytest
import wiredtiger as wt

from smongo.storage import LocalClient


@pytest.fixture
def tmp_wt_dir(tmp_path):
    """Fresh temporary directory suitable for WiredTiger."""
    return str(tmp_path / "wt_data")


@pytest.fixture
def wt_connection(tmp_wt_dir):
    os.makedirs(tmp_wt_dir, exist_ok=True)
    conn = wt.wiredtiger_open(tmp_wt_dir, "create")
    yield conn
    conn.close()


@pytest.fixture
def wt_session(wt_connection):
    session = wt_connection.open_session()
    yield session
    session.close()


@pytest.fixture
def local_client(tmp_wt_dir):
    return LocalClient(tmp_wt_dir, durable=False)


@pytest.fixture
def durable_client(tmp_wt_dir):
    """LocalClient with WAL enabled for crash-recovery tests."""
    return LocalClient(tmp_wt_dir, durable=True)


@pytest.fixture
def local_db(local_client):
    return local_client.get_db("testdb")


@pytest.fixture
def local_collection(local_db):
    return local_db.get_collection("testcoll")


@pytest.fixture
def sample_docs():
    return [
        {"name": "Alice", "age": 34, "city": "NYC", "dept": "eng", "tags": ["py", "go"], "salary": 145000},
        {"name": "Bob", "age": 28, "city": "SF", "dept": "eng", "tags": ["js", "react"], "salary": 128000},
        {"name": "Charlie", "age": 40, "city": "NYC", "dept": "mgmt", "tags": ["py"], "salary": 175000},
        {"name": "Diana", "age": 25, "city": "LA", "dept": "design", "tags": ["rust"], "salary": 98000},
        {"name": "Eve", "age": 31, "city": "SF", "dept": "eng", "tags": ["py", "ml"], "salary": 155000},
        {"name": "Frank", "age": 36, "city": "CHI", "dept": "eng", "tags": ["go", "k8s"], "salary": 140000},
        {"name": "Grace", "age": 29, "city": "NYC", "dept": "data", "tags": ["py", "spark"], "salary": 135000},
        {"name": "Hank", "age": 45, "city": "SF", "dept": "mgmt", "tags": ["strategy"], "salary": 190000},
        {"name": "Ivy", "age": 27, "city": "LA", "dept": "design", "tags": ["figma"], "salary": 105000},
        {"name": "Jack", "age": 33, "city": "NYC", "dept": "eng", "tags": ["java"], "salary": 142000},
    ]


@pytest.fixture
def populated_collection(local_collection, sample_docs):
    """A collection pre-loaded with sample_docs."""
    local_collection.insert_many(sample_docs)
    return local_collection
