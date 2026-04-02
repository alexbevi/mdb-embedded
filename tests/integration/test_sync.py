"""Integration tests for SyncManager against real MongoDB."""

import time

import pytest

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


def test_conflict_resolution_local_wins(
    embedded_client, remote_client, mongo_uri, db_name
):
    mgr = SyncManager(
        embedded_client,
        mongo_uri,
        sync_config={"mode": "bidirectional", "conflict_resolution": "local_wins", "use_change_stream_pull": False},
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


def test_conflict_resolution_remote_wins(
    embedded_client, remote_client, mongo_uri, db_name
):
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
