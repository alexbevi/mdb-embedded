"""Tests for smongo.oplog -- OplogWriter, OplogReader, ChangeStream."""

import os
import threading
import time

import pytest
import wiredtiger as wt

from smongo.oplog import ChangeStream, OplogHub, OplogReader, OplogWriter, _doc_checksum


@pytest.fixture
def hub():
    return OplogHub()


@pytest.fixture
def oplog_env(tmp_path):
    """Yield (session, oplog_uri, namespace) for oplog tests."""
    db_path = str(tmp_path / "oplog_wt")
    os.makedirs(db_path)
    conn = wt.wiredtiger_open(db_path, "create")
    session = conn.open_session()
    uri = "table:__oplog_test"
    session.create(uri, "key_format=S,value_format=S")
    yield session, uri, "testdb.testcoll"
    session.close()
    conn.close()


@pytest.fixture
def writer(oplog_env, hub):
    session, uri, ns = oplog_env
    return OplogWriter(session, uri, ns, hub=hub)


@pytest.fixture
def reader(oplog_env):
    session, uri, _namespace = oplog_env
    return OplogReader(session, uri)


# ── _doc_checksum ────────────────────────────────────────────────────


class TestDocChecksum:
    def test_same_doc_same_hash(self):
        doc = {"a": 1, "b": 2}
        assert _doc_checksum(doc) == _doc_checksum(doc)

    def test_key_order_irrelevant(self):
        assert _doc_checksum({"a": 1, "b": 2}) == _doc_checksum({"b": 2, "a": 1})

    def test_different_docs_different_hash(self):
        assert _doc_checksum({"a": 1}) != _doc_checksum({"a": 2})

    def test_none_doc(self):
        assert _doc_checksum(None) is None

    def test_returns_16_hex_chars(self):
        h = _doc_checksum({"x": 1})
        assert len(h) == 16
        int(h, 16)  # valid hex


# ── OplogWriter ──────────────────────────────────────────────────────


class TestOplogWriter:
    def test_log_insert(self, writer, reader):
        key = writer.log("insert", "d1", {"_id": "d1", "x": 1}, version=1)
        assert key is not None
        entries = reader.read_all()
        assert len(entries) == 1
        assert entries[0]["op"] == "insert"
        assert entries[0]["doc_id"] == "d1"
        assert entries[0]["v"] == 1

    def test_log_update(self, writer, reader):
        writer.log("update", "d1", {"$set": {"x": 2}}, version=2)
        entries = reader.read_all()
        assert entries[0]["op"] == "update"

    def test_log_delete(self, writer, reader):
        writer.log("delete", "d1", None, version=3)
        entries = reader.read_all()
        assert entries[0]["op"] == "delete"
        assert entries[0]["checksum"] is None

    def test_log_internal_flag(self, writer, reader):
        writer.log("insert", "d1", {"x": 1}, internal=True)
        entries = reader.read_all()
        assert entries[0]["internal"] is True

    def test_log_changed_fields(self, writer, reader):
        writer.log("update", "d1", {"$set": {"x": 2}}, changed_fields=["x"])
        entries = reader.read_all()
        assert entries[0]["changed_fields"] == ["x"]

    def test_log_namespace(self, writer, reader):
        writer.log("insert", "d1", {"x": 1})
        entries = reader.read_all()
        assert entries[0]["ns"] == "testdb.testcoll"

    def test_log_checksum_for_insert(self, writer, reader):
        writer.log("insert", "d1", {"x": 42})
        entries = reader.read_all()
        assert entries[0]["checksum"] is not None
        assert entries[0]["checksum"] == _doc_checksum({"x": 42})

    def test_multiple_entries_ordered(self, writer, reader):
        for i in range(5):
            writer.log("insert", f"d{i}", {"i": i})
        entries = reader.read_all()
        assert len(entries) == 5
        ids = [e["doc_id"] for e in entries]
        assert ids == [f"d{i}" for i in range(5)]


# ── OplogReader ──────────────────────────────────────────────────────


class TestOplogReader:
    def test_read_all_empty(self, reader):
        assert reader.read_all() == []

    def test_read_from_none_returns_all(self, writer, reader):
        for i in range(3):
            writer.log("insert", f"d{i}", {"i": i})
        entries = reader.read_from(None)
        assert len(entries) == 3

    def test_read_from_checkpoint(self, writer, reader):
        keys = []
        for i in range(5):
            keys.append(writer.log("insert", f"d{i}", {"i": i}))
        entries = reader.read_from(keys[2])
        assert len(entries) == 2  # d3 and d4

    def test_read_from_skip_internal(self, writer, reader):
        writer.log("insert", "d1", {"x": 1})
        writer.log("insert", "d2", {"x": 2}, internal=True)
        writer.log("insert", "d3", {"x": 3})
        entries = reader.read_from(None, skip_internal=True)
        assert len(entries) == 2
        assert all(e[1]["doc_id"] != "d2" for e in entries)

    def test_read_from_include_internal(self, writer, reader):
        writer.log("insert", "d1", {"x": 1}, internal=True)
        entries = reader.read_from(None, skip_internal=False)
        assert len(entries) == 1

    def test_latest_key_empty(self, reader):
        assert reader.latest_key() is None

    def test_latest_key(self, writer, reader):
        k1 = writer.log("insert", "d1", {})
        k2 = writer.log("insert", "d2", {})
        assert reader.latest_key() == k2


# ── Listener ─────────────────────────────────────────────────────────


class TestOplogListeners:
    def test_listener_receives_events(self, writer, hub):
        received = []

        class FakeListener:
            namespace = "testdb.testcoll"

            def _enqueue(self, entry):
                received.append(entry)

        listener = FakeListener()
        hub.register(listener)
        writer.log("insert", "d1", {"x": 1})
        assert len(received) == 1

    def test_listener_namespace_filter(self, writer, hub):
        received = []

        class FakeListener:
            namespace = "other.ns"

            def _enqueue(self, entry):
                received.append(entry)

        listener = FakeListener()
        hub.register(listener)
        writer.log("insert", "d1", {"x": 1})
        assert len(received) == 0

    def test_listener_exception_auto_unregistered(self, writer, hub):
        class BadListener:
            namespace = "testdb.testcoll"

            def _enqueue(self, entry):
                raise RuntimeError("boom")

        listener = BadListener()
        hub.register(listener)
        writer.log("insert", "d1", {"x": 1})
        assert listener not in hub._listeners

    def test_unregister_listener(self, hub):
        class FakeListener:
            namespace = None

            def _enqueue(self, entry):
                pass

        listener = FakeListener()
        hub.register(listener)
        hub.unregister(listener)
        assert listener not in hub._listeners


# ── ChangeStream ─────────────────────────────────────────────────────


class TestChangeStream:
    def test_receives_insert_event(self, writer, hub):
        cs = ChangeStream(namespace="testdb.testcoll", hub=hub)
        writer.log("insert", "d1", {"_id": "d1", "x": 1})
        event = cs.try_next()
        assert event is not None
        assert event["operationType"] == "insert"
        assert event["fullDocument"] == {"_id": "d1", "x": 1}
        cs.close()

    def test_receives_update_event(self, writer, hub):
        cs = ChangeStream(namespace="testdb.testcoll", hub=hub)
        writer.log("update", "d1", {"$set": {"x": 2}})
        event = cs.try_next()
        assert event["operationType"] == "update"
        cs.close()

    def test_receives_delete_event(self, writer, hub):
        cs = ChangeStream(namespace="testdb.testcoll", hub=hub)
        writer.log("delete", "d1", None)
        event = cs.try_next()
        assert event["operationType"] == "delete"
        assert "fullDocument" not in event
        cs.close()

    def test_ignores_index_ops(self, writer, hub):
        cs = ChangeStream(namespace="testdb.testcoll", hub=hub)
        writer.log("index_create", "idx1", {"keys": [("age", 1)]})
        event = cs.try_next()
        assert event is None
        cs.close()

    def test_namespace_filter(self, writer, hub):
        cs = ChangeStream(namespace="other.ns", hub=hub)
        writer.log("insert", "d1", {"x": 1})
        event = cs.try_next()
        assert event is None
        cs.close()

    def test_pipeline_match_filter(self, writer, hub):
        cs = ChangeStream(
            namespace="testdb.testcoll",
            pipeline=[{"$match": {"operationType": "delete"}}],
            hub=hub,
        )
        writer.log("insert", "d1", {"x": 1})
        writer.log("delete", "d2", None)
        events = []
        while True:
            e = cs.try_next()
            if e is None:
                break
            events.append(e)
        assert len(events) == 1
        assert events[0]["operationType"] == "delete"
        cs.close()

    def test_try_next_empty(self, writer, hub):
        cs = ChangeStream(namespace="testdb.testcoll", hub=hub)
        assert cs.try_next() is None
        cs.close()

    def test_close_stops_iteration(self, writer, hub):
        cs = ChangeStream(namespace="testdb.testcoll", hub=hub)
        cs.close()
        assert cs._closed is True

    def test_context_manager(self, writer, hub):
        with ChangeStream(namespace="testdb.testcoll", hub=hub) as cs:
            writer.log("insert", "d1", {"x": 1})
            event = cs.try_next()
            assert event is not None
        assert cs._closed is True

    def test_next_blocks_then_returns(self, writer, hub):
        cs = ChangeStream(namespace="testdb.testcoll", hub=hub)

        def delayed_write():
            time.sleep(0.1)
            writer.log("insert", "d1", {"x": 1})

        t = threading.Thread(target=delayed_write)
        t.start()
        event = next(cs)
        t.join()
        assert event["operationType"] == "insert"
        cs.close()

    def test_document_key(self, writer, hub):
        cs = ChangeStream(namespace="testdb.testcoll", hub=hub)
        writer.log("insert", "d1", {"_id": "d1"})
        event = cs.try_next()
        assert event["documentKey"] == {"_id": "d1"}
        cs.close()

    def test_ns_split(self, writer, hub):
        cs = ChangeStream(namespace="testdb.testcoll", hub=hub)
        writer.log("insert", "d1", {"x": 1})
        event = cs.try_next()
        assert event["ns"]["db"] == "testdb"
        assert event["ns"]["coll"] == "testcoll"
        cs.close()


# ── Oplog compaction ─────────────────────────────────────────────────


class TestOplogCompaction:
    def test_truncate_before(self, writer, reader):
        keys = [writer.log("insert", f"d{i}", {"i": i}) for i in range(10)]
        removed = writer.truncate_before(keys[5])
        assert removed == 5
        remaining = reader.read_all()
        assert len(remaining) == 5

    def test_truncate_count(self, writer, reader):
        for i in range(10):
            writer.log("insert", f"d{i}", {"i": i})
        excess = writer.truncate_count(3)
        assert excess == 7
        remaining = reader.read_all()
        assert len(remaining) == 3

    def test_truncate_count_no_excess(self, writer, reader):
        for i in range(3):
            writer.log("insert", f"d{i}", {"i": i})
        excess = writer.truncate_count(10)
        assert excess == 0
        assert len(reader.read_all()) == 3

    def test_count(self, writer, reader):
        assert reader.count() == 0
        for i in range(5):
            writer.log("insert", f"d{i}", {"i": i})
        assert reader.count() == 5

    def test_oldest_key(self, writer, reader):
        assert reader.oldest_key() is None
        k1 = writer.log("insert", "d1", {})
        writer.log("insert", "d2", {})
        assert reader.oldest_key() == k1

    def test_compact_oplog_on_collection(self, local_collection):
        """Test compact_oplog from the collection level."""
        for i in range(20):
            local_collection.insert_one({"_id": f"co{i}"})
        oplog_before = local_collection.get_oplog()
        assert len(oplog_before) == 20
        local_collection.compact_oplog(keep=5)
        oplog_after = local_collection.get_oplog()
        assert len(oplog_after) == 5
