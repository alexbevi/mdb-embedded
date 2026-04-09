"""Integration tests for Flask API in web_app.py."""

import importlib
import time

import pytest
from pymongo import MongoClient as PyMongoClient

from smongo import MongoClient as EmbeddedClient
from smongo import SyncManager

pytestmark = pytest.mark.integration


@pytest.fixture
def web_client(mongo_uri, tmp_path):
    web_app = importlib.import_module("web_app")
    # Rebind globals so each test gets isolated local/remote stores.
    web_app.DB_NAME = f"web_{int(time.time() * 1000)}"
    web_app.client = EmbeddedClient(f"local://{tmp_path}/web_redb")
    web_app.remote = PyMongoClient(mongo_uri)
    web_app.sync_mgr = SyncManager(
        web_app.client,
        mongo_uri,
        sync_config={"mode": "bidirectional", "interval_sec": 1, "use_change_stream_pull": False},
    )
    web_app._collections_cache.clear()
    web_app._watch_streams.clear()
    with web_app.app.test_client() as c:
        yield c
    web_app.sync_mgr.stop()
    web_app.remote.close()


def test_index_page(web_client):
    resp = web_client.get("/")
    assert resp.status_code == 200
    assert b"html" in resp.data.lower()


def test_insert_and_get_docs(web_client):
    r = web_client.post("/api/insert", json={"coll": "users", "doc": {"name": "Alice"}})
    assert r.status_code == 200
    docs = web_client.get("/api/docs?coll=users").get_json()
    assert any(d["name"] == "Alice" for d in docs)


def test_query_endpoint(web_client):
    web_client.post("/api/insert", json={"coll": "users", "doc": {"name": "Bob", "age": 30}})
    resp = web_client.post("/api/query", json={"coll": "users", "query": {"age": {"$gte": 30}}})
    body = resp.get_json()
    assert resp.status_code == 200
    assert body["count"] >= 1
    assert "plan" in body


def test_aggregate_endpoint(web_client):
    web_client.post("/api/insert", json={"coll": "users", "doc": {"city": "NYC"}})
    web_client.post("/api/insert", json={"coll": "users", "doc": {"city": "NYC"}})
    resp = web_client.post(
        "/api/aggregate",
        json={"coll": "users", "pipeline": [{"$group": {"_id": "$city", "count": {"$sum": 1}}}]},
    )
    assert resp.status_code == 200
    assert resp.get_json()["count"] >= 1


def test_update_and_delete_endpoint(web_client):
    web_client.post("/api/insert", json={"coll": "users", "doc": {"name": "Tmp"}})
    u = web_client.post(
        "/api/update",
        json={"coll": "users", "query": {"name": "Tmp"}, "update": {"$set": {"name": "Updated"}}},
    )
    assert u.status_code == 200
    d = web_client.post("/api/delete", json={"coll": "users", "query": {"name": "Updated"}})
    assert d.status_code == 200
    assert d.get_json()["deleted_count"] >= 1


def test_index_crud_endpoints(web_client):
    c = web_client.post(
        "/api/indexes", json={"coll": "users", "keys": [["age", 1]], "unique": False}
    )
    assert c.status_code == 200
    name = c.get_json()["name"]
    lst = web_client.get("/api/indexes?coll=users")
    assert any(i["name"] == name for i in lst.get_json())
    dr = web_client.delete(f"/api/indexes/{name}?coll=users")
    assert dr.status_code == 200


def test_oplog_endpoint(web_client):
    web_client.post("/api/insert", json={"coll": "users", "doc": {"name": "Op"}})
    resp = web_client.get("/api/oplog?coll=users&limit=10")
    assert resp.status_code == 200
    assert isinstance(resp.get_json(), list)


def test_shell_insert_and_find(web_client):
    ins = web_client.post("/api/shell", json={"command": 'db.users.insertOne({"name":"Shell"})'})
    assert ins.status_code == 200
    out = web_client.post(
        "/api/shell", json={"command": 'db.users.find({"name":"Shell"})'}
    ).get_json()
    assert len(out["result"]) >= 1


def test_shell_empty_command_error(web_client):
    resp = web_client.post("/api/shell", json={"command": ""})
    assert resp.status_code == 400


def test_sync_endpoints(web_client):
    web_client.post("/api/insert", json={"coll": "users", "doc": {"_id": "sync1", "name": "Sync"}})
    st = web_client.get("/api/sync/status")
    assert st.status_code == 200
    assert "running" in st.get_json()
    p = web_client.post("/api/sync/push")
    assert p.status_code == 200
    pl = web_client.post("/api/sync/pull")
    assert pl.status_code == 200


def test_schema_endpoint_validation(web_client):
    schema = {"$jsonSchema": {"required": ["name"]}}
    s = web_client.post("/api/schema", json={"coll": "strict", "validator": schema})
    assert s.status_code == 200
    bad = web_client.post("/api/insert", json={"coll": "strict", "doc": {"age": 1}})
    assert bad.status_code == 400
    assert "error" in bad.get_json()
    good = web_client.post(
        "/api/insert", json={"coll": "strict", "doc": {"name": "Valid", "age": 1}}
    )
    assert good.status_code == 200
