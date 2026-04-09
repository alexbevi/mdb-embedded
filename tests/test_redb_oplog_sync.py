"""Redb oplog, sync KV helpers, and hybrid SyncManager."""

from __future__ import annotations

import os
import tempfile

import pytest

pytest.importorskip("pymongo")

from smongo.client import MongoClient
from smongo.storage.redb_engine import RedbClient
from smongo.sync import TombstoneRegistry


def test_redb_oplog_reader_after_insert() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        client = RedbClient(os.path.join(tmp, "db"))
        db = client.get_db("mydb")
        coll = db.collection("items")
        coll.insert_one({"_id": "a", "x": 1})
        reader = coll.get_oplog_reader()
        rows = reader.read_from(None, skip_internal=True)
        assert rows
        _key, entry = rows[-1]
        assert entry["op"] == "insert"


def test_redb_internal_insert_skipped_in_sync_tail() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        client = RedbClient(os.path.join(tmp, "db"))
        db = client.get_db("mydb")
        coll = db.collection("items")
        coll.insert_one({"_id": "u1"}, _internal=True)
        reader = coll.get_oplog_reader()
        public = reader.read_from(None, skip_internal=True)
        assert public == []


def test_redb_sync_kv_and_atomic_checkpoint() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "db")
        client = RedbClient(path)
        rust = client._rust_client
        rust.sync_kv_put("table:__sync_checkpoint", "push:mydb.items", "k0")
        assert rust.sync_kv_get("table:__sync_checkpoint", "push:mydb.items") == "k0"

        db = client.get_db("mydb")
        coll = db.collection("items")
        coll.insert_one({"_id": 1})
        uri = coll._oplog_w.oplog_uri
        reader = coll.get_oplog_reader()
        rows = reader.read_from(None, skip_internal=True)
        assert rows
        last_k = rows[-1][0]
        rust.sync_atomic_checkpoint_truncate(
            "table:__sync_checkpoint",
            "push:mydb.items",
            last_k,
            uri,
            last_k,
        )
        after = reader.read_from(last_k, skip_internal=True)
        assert after == []


def test_tombstone_registry_redb() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        client = RedbClient(os.path.join(tmp, "db"))
        rust = client._rust_client
        reg = TombstoneRegistry(
            ttl_sec=3600,
            redb_client=rust,
            uri="table:__tombstones",
        )
        reg.mark_deleted("id1")
        assert reg.is_tombstoned("id1")


def test_redb_drop_collection_removes_data() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        client = RedbClient(os.path.join(tmp, "db"))
        db = client.get_db("mydb")
        coll = db.collection("items")
        coll.insert_one({"_id": "a", "email": "x@y.z"})
        coll.create_index([("email", 1)], name="synced_idx_from_remote")
        assert coll.count_documents({}) == 1
        assert any(
            idx["name"] == "synced_idx_from_remote" for idx in coll.list_indexes()
        )
        db.drop_collection("items")
        fresh = db.collection("items")
        assert fresh.count_documents({}) == 0
        index_names = {idx["name"] for idx in fresh.list_indexes()}
        assert "synced_idx_from_remote" not in index_names


def test_sync_manager_uses_redb_kv() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "hybrid")
        mc = MongoClient(
            f"local://{db_path}",
            sync="mongodb://127.0.0.1:27017",
            sync_config={"collections": []},
        )
        try:
            assert mc.sync is not None
            assert mc.sync._rust is not None
        finally:
            mc.close()
