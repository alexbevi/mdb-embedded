"""Tests for smongo.oplog -- OplogWriter, OplogReader, ChangeStream."""

import bisect
import threading
import time

import pytest

from smongo._compat import StorageError
from smongo.oplog import ChangeStream, OplogHub, OplogReader, OplogWriter, _doc_checksum


class _FakeOplogCursor:
    """In-memory cursor matching the storage cursor API used by OplogWriter / OplogReader."""

    def __init__(self, store: dict[str, bytes]) -> None:
        self._store = store
        self.key: str | None = None
        self._iter_keys: list[str] | None = None
        self._pos = -1

    def set_key(self, k: str) -> None:
        self.key = k

    def search_near(self) -> int:
        keys = sorted(self._store.keys())
        self._iter_keys = keys
        if not keys:
            raise StorageError("empty oplog")
        ck = self.key
        assert ck is not None
        i = bisect.bisect_left(keys, ck)
        if i < len(keys) and keys[i] == ck:
            self._pos = i
            self.key = keys[i]
            return 0
        if i == len(keys):
            self._pos = len(keys) - 1
            self.key = keys[self._pos]
            return -1
        self._pos = i
        self.key = keys[i]
        return 1

    def get_key(self) -> str:
        assert self.key is not None
        return self.key

    def get_value(self) -> bytes:
        assert self.key is not None
        return self._store[self.key]

    def next(self) -> int:
        if self._iter_keys is None:
            self._iter_keys = sorted(self._store.keys())
            self._pos = -1
        self._pos += 1
        if self._pos < len(self._iter_keys):
            self.key = self._iter_keys[self._pos]
            return 0
        return 1

    def remove(self) -> None:
        assert self.key is not None
        k = self.key
        self._store.pop(k, None)
        if self._iter_keys is not None and k in self._iter_keys:
            self._iter_keys.remove(k)

    def __setitem__(self, k: str, v: bytes) -> None:
        self._store[k] = v

    def close(self) -> None:
        pass


class _FakeOplogSession:
    def __init__(self) -> None:
        self._tables: dict[str, dict[str, bytes]] = {}

    def _store(self, uri: str) -> dict[str, bytes]:
        return self._tables.setdefault(uri, {})

    def open_cursor(self, uri: str, _x: object, _y: object) -> _FakeOplogCursor:
        return _FakeOplogCursor(self._store(uri))


@pytest.fixture
def hub():
    return OplogHub()


@pytest.fixture
def oplog_env():
    """Yield (session, oplog_uri, namespace) for oplog tests using an in-memory table."""
    session = _FakeOplogSession()
    uri = "table:__oplog_test"
    yield session, uri, "testdb.testcoll"


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
