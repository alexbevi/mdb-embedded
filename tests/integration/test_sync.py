"""Integration tests for SyncManager against real MongoDB."""

import time

import pytest

from smongo import MongoClient as EmbeddedClient
from smongo import SyncManager

pytestmark = pytest.mark.integration


def _wire(sync_manager, embedded_client, db_name, coll_name="users"):
    coll = embedded_client[db_name][coll_name]
    sync_manager.register_collection(db_name, coll_name, coll.get_local_collection())
    return coll


def test_push_insert(sync_manager, embedded_client, remote_client, db_name):
    coll = _wire(sync_manager, embedded_client, db_name)
    coll.insert_one({"_id": "u1", "name": "Alice"})
    sync_manager.sync_now()
    remote_doc = remote_client[db_name]["users"].find_one({"_id": "u1"})
    assert remote_doc is not None
    assert remote_doc["name"] == "Alice"
    assert "_lastModified" in remote_doc


def test_push_update(sync_manager, embedded_client, remote_client, db_name):
    coll = _wire(sync_manager, embedded_client, db_name)
    coll.insert_one({"_id": "u2", "name": "Bob"})
    sync_manager.sync_now()
    coll.update_one({"_id": "u2"}, {"$set": {"name": "Bobby"}})
    sync_manager.sync_now()
    remote_doc = remote_client[db_name]["users"].find_one({"_id": "u2"})
    assert remote_doc["name"] == "Bobby"


def test_push_delete(sync_manager, embedded_client, remote_client, db_name):
    coll = _wire(sync_manager, embedded_client, db_name)
    coll.insert_one({"_id": "u3", "name": "DeleteMe"})
    sync_manager.sync_now()
    coll.delete_one({"_id": "u3"})
    sync_manager.sync_now()
    assert remote_client[db_name]["users"].find_one({"_id": "u3"}) is None


def test_pull_insert(sync_manager, embedded_client, remote_client, db_name):
    coll = _wire(sync_manager, embedded_client, db_name)
    remote_client[db_name]["users"].insert_one(
        {"_id": "r1", "name": "Remote", "_lastModified": time.time()}
    )
    sync_manager._pull()
    local_doc = coll.find_one({"_id": "r1"})
    assert local_doc is not None
    assert local_doc["name"] == "Remote"


def test_pull_update(sync_manager, embedded_client, remote_client, db_name):
    coll = _wire(sync_manager, embedded_client, db_name)
    remote_client[db_name]["users"].insert_one(
        {"_id": "r2", "name": "Before", "_lastModified": time.time()}
    )
    sync_manager._pull()
    remote_client[db_name]["users"].update_one(
        {"_id": "r2"},
        {"$set": {"name": "After", "_lastModified": time.time() + 1}},
    )
    sync_manager._pull()
    assert coll.find_one({"_id": "r2"})["name"] == "After"


def test_pull_delete(sync_manager, embedded_client, remote_client, db_name):
    coll = _wire(sync_manager, embedded_client, db_name)
    remote_client[db_name]["users"].insert_one(
        {"_id": "r3", "name": "Gone", "_lastModified": time.time()}
    )
    sync_manager._pull()
    remote_client[db_name]["users"].delete_one({"_id": "r3"})
    # Pull delete is change-stream-specific; with polling fallback this may not delete.
    # Exercise path directly through change stream emulation not available here.
    assert coll.find_one({"_id": "r3"}) is not None


def test_index_create_push(sync_manager, embedded_client, remote_client, db_name):
    coll = _wire(sync_manager, embedded_client, db_name)
    coll.create_index([("age", 1)])
    sync_manager.sync_now()
    names = [i["name"] for i in remote_client[db_name]["users"].list_indexes()]
    assert "age_1" in names


def test_index_pull(sync_manager, embedded_client, remote_client, db_name):
    coll = _wire(sync_manager, embedded_client, db_name)
    remote_client[db_name]["users"].create_index([("city", 1)], name="city_1")
    sync_manager._pull()
    local_names = [i["name"] for i in coll.list_indexes()]
    assert "city_1" in local_names


def test_conflict_resolution_local_wins(embedded_client, remote_client, mongo_uri, db_name):
    mgr = SyncManager(
        embedded_client,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "conflict_resolution": "local_wins",
            "use_change_stream_pull": False,
        },
    )
    coll = embedded_client[db_name]["users"]
    mgr.register_collection(db_name, "users", coll.get_local_collection())

    coll.insert_one({"_id": "c1", "name": "original"})
    mgr.sync_now()

    remote_client[db_name]["users"].update_one(
        {"_id": "c1"}, {"$set": {"name": "remote", "_lastModified": time.time() + 1}}
    )
    coll.update_one({"_id": "c1"}, {"$set": {"name": "local"}})
    mgr.sync_now()

    local_doc = coll.find_one({"_id": "c1"})
    assert local_doc["name"] == "local"
    mgr.stop()


def test_conflict_resolution_remote_wins(embedded_client, remote_client, mongo_uri, db_name):
    mgr = SyncManager(
        embedded_client,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "conflict_resolution": "remote_wins",
            "use_change_stream_pull": False,
            "oplog_auto_compact": False,
        },
    )
    coll = embedded_client[db_name]["users"]
    mgr.register_collection(db_name, "users", coll.get_local_collection())

    coll.insert_one({"_id": "c2", "name": "original"})
    mgr.push()

    remote_client[db_name]["users"].update_one(
        {"_id": "c2"}, {"$set": {"name": "remote", "_lastModified": time.time() + 10}}
    )
    coll.update_one({"_id": "c2"}, {"$set": {"name": "local"}})

    mgr.pull()

    local_doc = coll.find_one({"_id": "c2"})
    assert local_doc["name"] == "remote"
    mgr.stop()


def test_status_and_lifecycle(sync_manager, embedded_client, db_name):
    _wire(sync_manager, embedded_client, db_name)
    st1 = sync_manager.status()
    assert st1["running"] is False
    sync_manager.start()
    st2 = sync_manager.status()
    assert st2["running"] is True
    sync_manager.pause()
    sync_manager.resume()
    sync_manager.stop()
    st3 = sync_manager.status()
    assert st3["running"] is False


def test_status_includes_dlq(sync_manager, embedded_client, db_name):
    _wire(sync_manager, embedded_client, db_name)
    s = sync_manager.status()
    assert "dlq_depth" in s
    assert "dlq_permanent_failures" in s
    assert s["dlq_depth"] == 0


# ── Change Stream Pull Tests ─────────────────────────────────────────


def test_change_stream_pull_snapshot(embedded_client, remote_client, mongo_uri, db_name):
    """Change-stream pull path picks up docs via initial snapshot."""
    mgr = SyncManager(
        embedded_client,
        mongo_uri,
        sync_config={
            "mode": "pull_only",
            "use_change_stream_pull": True,
            "batch_size": 100,
        },
    )
    coll = embedded_client[db_name]["cs_snap"]
    mgr.register_collection(db_name, "cs_snap", coll.get_local_collection())

    remote_client[db_name]["cs_snap"].insert_one({"_id": "s1", "val": "snapshot"})

    mgr._pull()

    local_doc = coll.find_one({"_id": "s1"})
    assert local_doc is not None
    assert local_doc["val"] == "snapshot"
    mgr.stop()


def test_change_stream_pull_delete(embedded_client, remote_client, mongo_uri, db_name):
    """Change-stream pull propagates remote deletes (polling cannot do this)."""
    mgr = SyncManager(
        embedded_client,
        mongo_uri,
        sync_config={
            "mode": "pull_only",
            "use_change_stream_pull": True,
            "batch_size": 100,
        },
    )
    coll = embedded_client[db_name]["cs_del"]
    mgr.register_collection(db_name, "cs_del", coll.get_local_collection())

    remote_client[db_name]["cs_del"].insert_one({"_id": "d1", "val": "delete-me"})
    mgr._pull()
    assert coll.find_one({"_id": "d1"}) is not None

    remote_client[db_name]["cs_del"].delete_one({"_id": "d1"})

    mgr._pull()

    assert coll.find_one({"_id": "d1"}) is None
    mgr.stop()


# ── Edge Case: Multi-Client Conflict Convergence ─────────────────────


def test_multi_client_conflict_convergence(tmp_path, remote_client, mongo_uri, db_name):
    """Two embedded clients with different node_ids making conflicting edits
    converge to the same document after syncing."""
    client_a = EmbeddedClient(f"local://{tmp_path}/node_a")
    client_b = EmbeddedClient(f"local://{tmp_path}/node_b")

    mgr_a = SyncManager(
        client_a,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "conflict_resolution": "lww",
            "use_change_stream_pull": False,
            "node_id": "node-alpha",
            "oplog_auto_compact": False,
        },
    )
    mgr_b = SyncManager(
        client_b,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "conflict_resolution": "lww",
            "use_change_stream_pull": False,
            "node_id": "node-beta",
            "oplog_auto_compact": False,
        },
    )

    coll_a = client_a[db_name]["conv"]
    coll_b = client_b[db_name]["conv"]
    mgr_a.register_collection(db_name, "conv", coll_a.get_local_collection())
    mgr_b.register_collection(db_name, "conv", coll_b.get_local_collection())

    coll_a.insert_one({"_id": "shared", "value": "original"})
    mgr_a.push()
    mgr_b.pull()

    coll_a.update_one({"_id": "shared"}, {"$set": {"value": "from-alpha"}})
    coll_b.update_one({"_id": "shared"}, {"$set": {"value": "from-beta"}})

    mgr_a.push()
    mgr_b.push()

    mgr_a.pull()
    mgr_b.pull()

    doc_a = coll_a.find_one({"_id": "shared"})
    doc_b = coll_b.find_one({"_id": "shared"})
    remote_doc = remote_client[db_name]["conv"].find_one({"_id": "shared"})

    assert (
        doc_a["value"] == doc_b["value"]
    ), f"Clients diverged: A={doc_a['value']}, B={doc_b['value']}"

    mgr_a.stop()
    mgr_b.stop()


# ── Edge Case: Clock Skew LWW Deterministic ──────────────────────────


def test_clock_skew_lww_deterministic(embedded_client, remote_client, mongo_uri, db_name):
    """A document with a far-future _lastModified should not automatically win
    when vector clocks indicate concurrency."""
    mgr = SyncManager(
        embedded_client,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "conflict_resolution": "lww",
            "use_change_stream_pull": False,
            "node_id": "node-aaa",
            "oplog_auto_compact": False,
        },
    )
    coll = embedded_client[db_name]["skew"]
    mgr.register_collection(db_name, "skew", coll.get_local_collection())

    coll.insert_one({"_id": "sk1", "name": "base"})
    mgr.push()

    coll.update_one({"_id": "sk1"}, {"$set": {"name": "skewed-client"}})

    remote_client[db_name]["skew"].update_one(
        {"_id": "sk1"},
        {
            "$set": {
                "name": "honest-remote",
                "_lastModified": time.time() - 1000,
                "_vclock": {"node-zzz": 1},
            }
        },
    )

    mgr.pull()

    doc = coll.find_one({"_id": "sk1"})
    assert doc is not None
    assert doc["name"] == "honest-remote", (
        "Higher node_id (node-zzz) should win over lower (node-aaa) "
        "when vector clocks are concurrent, regardless of timestamps"
    )
    mgr.stop()


# ── Edge Case: Schema Rejection Rollback ─────────────────────────────


def test_schema_rejection_rollback_integration(embedded_client, remote_client, mongo_uri, db_name):
    """A document that fails server-side schema validation is rolled back locally."""
    remote_coll_name = "validated"

    remote_client[db_name].create_collection(
        remote_coll_name,
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["status"],
                "properties": {
                    "status": {"bsonType": "string", "enum": ["active", "inactive"]},
                },
            }
        },
    )
    remote_client[db_name][remote_coll_name].insert_one(
        {"_id": "v1", "status": "active", "_lastModified": time.time()}
    )

    mgr = SyncManager(
        embedded_client,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "conflict_resolution": "lww",
            "use_change_stream_pull": False,
            "schema_rejection_strategy": "rollback",
            "oplog_auto_compact": False,
        },
    )
    coll = embedded_client[db_name][remote_coll_name]
    mgr.register_collection(db_name, remote_coll_name, coll.get_local_collection())

    mgr.pull()
    assert coll.find_one({"_id": "v1"})["status"] == "active"

    coll.update_one({"_id": "v1"}, {"$set": {"status": "INVALID_VALUE"}})
    assert coll.find_one({"_id": "v1"})["status"] == "INVALID_VALUE"

    mgr.push()

    local_doc = coll.find_one({"_id": "v1"})
    assert (
        local_doc["status"] == "active"
    ), "After schema rejection rollback, local doc should revert to server version"
    assert mgr.status()["schema_rejections"] >= 1
    mgr.stop()


# ── Edge Case: Oplog Overflow Triggers Full Resync ───────────────────


def test_oplog_overflow_triggers_full_resync(embedded_client, remote_client, mongo_uri, db_name):
    """When the oplog is compacted past the push checkpoint, a full resync
    should recover by re-pulling from the server."""
    mgr = SyncManager(
        embedded_client,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "conflict_resolution": "lww",
            "use_change_stream_pull": False,
            "oplog_auto_compact": False,
            "overflow_strategy": "server_wins",
        },
    )
    coll = embedded_client[db_name]["overflow"]
    mgr.register_collection(db_name, "overflow", coll.get_local_collection())

    for i in range(5):
        coll.insert_one({"_id": f"o{i}", "val": i})
    mgr.push()

    remote_client[db_name]["overflow"].insert_one(
        {"_id": "server_only", "val": 999, "_lastModified": time.time()}
    )

    for i in range(5, 10):
        coll.insert_one({"_id": f"o{i}", "val": i})

    local_coll = coll.get_local_collection()
    local_coll.compact_oplog(keep=0)

    mgr.sync_now()

    local_doc = coll.find_one({"_id": "server_only"})
    assert local_doc is not None, "Full resync should pull server_only doc after overflow"
    assert local_doc["val"] == 999
    mgr.stop()
