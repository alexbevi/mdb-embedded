"""Integration tests for MQL-native sync rules and edge fleet sync patterns."""

import time

import pytest

from smongo import MongoClient as EmbeddedClient
from smongo import SyncManager

pytestmark = pytest.mark.integration


def _wire(mgr, client, db_name, coll_name, *, sync_filter=None):
    coll = client[db_name][coll_name]
    mgr.register_collection(
        db_name, coll_name, coll.get_local_collection(), sync_filter=sync_filter
    )
    return coll


# ------------------------------------------------------------------
# Device-scoped sync
# ------------------------------------------------------------------


def test_device_scoped_push(tmp_path, mongo_uri, remote_client, db_name):
    """Only docs matching device_id == $$NODE_ID are pushed to remote."""
    client = EmbeddedClient(f"local+wt://{tmp_path}/fleet_push")
    mgr = SyncManager(
        client,
        mongo_uri,
        sync_config={
            "mode": "push_only",
            "sync_rules": {"device_id": "$$NODE_ID"},
            "node_id": "sensor-east-001",
            "use_change_stream_pull": False,
        },
    )

    coll = _wire(mgr, client, db_name, "readings")
    coll.insert_one({"_id": "r1", "device_id": "sensor-east-001", "temp": 22.5})
    coll.insert_one({"_id": "r2", "device_id": "sensor-west-042", "temp": 19.0})

    mgr.sync_now()

    remote_coll = remote_client[db_name]["readings"]
    assert remote_coll.find_one({"_id": "r1"}) is not None
    assert remote_coll.find_one({"_id": "r2"}) is None

    mgr.stop()
    client.close()


def test_device_scoped_pull(tmp_path, mongo_uri, remote_client, db_name):
    """Only docs matching device_id == $$NODE_ID are pulled from remote."""
    remote_coll = remote_client[db_name]["readings"]
    remote_coll.insert_one(
        {"_id": "r1", "device_id": "sensor-east-001", "temp": 22.5, "_lastModified": time.time()}
    )
    remote_coll.insert_one(
        {"_id": "r2", "device_id": "sensor-west-042", "temp": 19.0, "_lastModified": time.time()}
    )

    client = EmbeddedClient(f"local+wt://{tmp_path}/fleet_pull")
    mgr = SyncManager(
        client,
        mongo_uri,
        sync_config={
            "mode": "pull_only",
            "sync_rules": {"device_id": "$$NODE_ID"},
            "node_id": "sensor-east-001",
            "use_change_stream_pull": False,
        },
    )

    coll = _wire(mgr, client, db_name, "readings")
    mgr.sync_now()

    assert coll.find_one({"_id": "r1"}) is not None
    assert coll.find_one({"_id": "r2"}) is None

    mgr.stop()
    client.close()


def test_two_devices_isolated(tmp_path, mongo_uri, remote_client, db_name):
    """Two sync managers with different node_ids only push/pull their own data."""
    client_a = EmbeddedClient(f"local+wt://{tmp_path}/dev_a")
    client_b = EmbeddedClient(f"local+wt://{tmp_path}/dev_b")

    mgr_a = SyncManager(
        client_a,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "sync_rules": {"device_id": "$$NODE_ID"},
            "node_id": "device-A",
            "use_change_stream_pull": False,
        },
    )
    mgr_b = SyncManager(
        client_b,
        mongo_uri,
        sync_config={
            "mode": "bidirectional",
            "sync_rules": {"device_id": "$$NODE_ID"},
            "node_id": "device-B",
            "use_change_stream_pull": False,
        },
    )

    coll_a = _wire(mgr_a, client_a, db_name, "data")
    coll_b = _wire(mgr_b, client_b, db_name, "data")

    coll_a.insert_one({"_id": "a1", "device_id": "device-A", "val": 1})
    coll_b.insert_one({"_id": "b1", "device_id": "device-B", "val": 2})

    mgr_a.push()
    mgr_b.push()

    remote_coll = remote_client[db_name]["data"]
    assert remote_coll.find_one({"_id": "a1"}) is not None
    assert remote_coll.find_one({"_id": "b1"}) is not None

    mgr_a.pull()
    mgr_b.pull()

    assert coll_a.find_one({"_id": "b1"}) is None
    assert coll_b.find_one({"_id": "a1"}) is None

    mgr_a.stop()
    mgr_b.stop()
    client_a.close()
    client_b.close()


# ------------------------------------------------------------------
# Time-windowed sync
# ------------------------------------------------------------------


def test_time_windowed_sync(tmp_path, mongo_uri, remote_client, db_name):
    """Sync rules with $$NOW exclude documents outside the time window."""
    now = time.time()
    remote_coll = remote_client[db_name]["events"]
    remote_coll.insert_one({"_id": "old", "val": 1, "_lastModified": now - 86400 * 30})
    remote_coll.insert_one({"_id": "recent", "val": 2, "_lastModified": now - 3600})

    client = EmbeddedClient(f"local+wt://{tmp_path}/time_win")
    mgr = SyncManager(
        client,
        mongo_uri,
        sync_config={
            "mode": "pull_only",
            "sync_rules": {"_lastModified": {"$gt": "$$WINDOW_START"}},
            "variables": {"WINDOW_START": now - 86400 * 7},
            "use_change_stream_pull": False,
        },
    )

    coll = _wire(mgr, client, db_name, "events")
    mgr.sync_now()

    assert coll.find_one({"_id": "recent"}) is not None
    assert coll.find_one({"_id": "old"}) is None

    mgr.stop()
    client.close()


# ------------------------------------------------------------------
# Node ID in oplog
# ------------------------------------------------------------------


def test_node_id_in_oplog(tmp_path, mongo_uri):
    """Oplog entries contain the node_id after sync registration."""
    client = EmbeddedClient(f"local+wt://{tmp_path}/oplog_nid")
    mgr = SyncManager(
        client,
        mongo_uri,
        sync_config={
            "node_id": "edge-42",
            "use_change_stream_pull": False,
        },
    )

    db_name = "oplog_test"
    coll = _wire(mgr, client, db_name, "sensors")
    coll.insert_one({"_id": "s1", "temp": 20})

    local_coll = coll.get_local_collection()
    reader = local_coll.get_oplog_reader()
    entries = reader.read_all()

    insert_entries = [e for e in entries if e.get("op") == "insert"]
    assert len(insert_entries) >= 1
    assert insert_entries[-1].get("node_id") == "edge-42"

    mgr.stop()
    client.close()
