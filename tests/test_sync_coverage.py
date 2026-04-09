"""Coverage tests for smongo.sync primitives (VectorClock, TombstoneRegistry, etc.)."""

from smongo.sync import (
    DEFAULT_TOMBSTONE_TTL_SEC,
    SyncOverflowError,
    TombstoneRegistry,
    VectorClock,
    _deterministic_lww,
)


class TestVectorClock:
    def test_tick_increments(self):
        """tick() increments counter for a node."""
        vc = VectorClock()
        vc.tick("node1")
        assert vc.to_dict() == {"node1": 1}
        vc.tick("node1")
        assert vc.to_dict() == {"node1": 2}

    def test_tick_multiple_nodes(self):
        """tick() tracks multiple nodes independently."""
        vc = VectorClock()
        vc.tick("node1").tick("node2").tick("node1")
        assert vc.to_dict() == {"node1": 2, "node2": 1}

    def test_merge(self):
        """merge() takes max of each node's counter."""
        vc1 = VectorClock({"node1": 5, "node2": 3})
        vc2 = VectorClock({"node1": 3, "node2": 7})
        vc1.merge(vc2)
        assert vc1.to_dict() == {"node1": 5, "node2": 7}

    def test_dominates_true(self):
        """dominates() returns True when all counters are >=."""
        vc1 = VectorClock({"node1": 5, "node2": 3})
        vc2 = VectorClock({"node1": 4, "node2": 2})
        assert vc1.dominates(vc2)

    def test_dominates_false(self):
        """dominates() returns False when some counters are less."""
        vc1 = VectorClock({"node1": 5, "node2": 1})
        vc2 = VectorClock({"node1": 4, "node2": 3})
        assert not vc1.dominates(vc2)

    def test_dominates_empty_other(self):
        """dominates() with empty other returns True if self has entries."""
        vc1 = VectorClock({"node1": 1})
        vc2 = VectorClock({})
        assert vc1.dominates(vc2)

    def test_concurrent_with_true(self):
        """concurrent_with() returns True when neither dominates."""
        vc1 = VectorClock({"node1": 5, "node2": 1})
        vc2 = VectorClock({"node1": 3, "node2": 4})
        assert vc1.concurrent_with(vc2)
        assert vc2.concurrent_with(vc1)

    def test_concurrent_with_false(self):
        """concurrent_with() returns False when one dominates."""
        vc1 = VectorClock({"node1": 5, "node2": 3})
        vc2 = VectorClock({"node1": 4, "node2": 2})
        assert not vc1.concurrent_with(vc2)
        assert not vc2.concurrent_with(vc1)

    def test_from_dict(self):
        """from_dict() restores VectorClock from dict."""
        vc = VectorClock.from_dict({"node1": 10, "node2": 5})
        assert vc.to_dict() == {"node1": 10, "node2": 5}

    def test_from_dict_none(self):
        """from_dict(None) creates empty clock."""
        vc = VectorClock.from_dict(None)
        assert vc.to_dict() == {}


class TestTombstoneRegistry:
    def test_mark_deleted_in_memory(self):
        """mark_deleted() stores tombstone in-memory."""
        reg = TombstoneRegistry(ttl_sec=60)
        reg.mark_deleted("doc123")
        assert reg.is_tombstoned("doc123")

    def test_is_tombstoned_false_initially(self):
        """is_tombstoned() returns False for unknown IDs."""
        reg = TombstoneRegistry()
        assert not reg.is_tombstoned("unknown")

    def test_expire_removes_old_tombstones(self):
        """expire() removes tombstones older than TTL."""
        import time

        reg = TombstoneRegistry(ttl_sec=1)
        reg.mark_deleted("doc1")
        time.sleep(1.1)
        reg.mark_deleted("doc2")
        count = reg.expire()
        assert count == 1  # doc1 expired
        assert not reg.is_tombstoned("doc1")
        assert reg.is_tombstoned("doc2")

    def test_to_dict(self):
        """to_dict() returns tombstone registry state."""
        reg = TombstoneRegistry()
        reg.mark_deleted("doc1")
        reg.mark_deleted("doc2")
        d = reg.to_dict()
        assert "doc1" in d
        assert "doc2" in d

    def test_load(self):
        """load() restores tombstones from dict."""
        import time

        now = time.time()
        reg = TombstoneRegistry()
        reg.load({"doc1": now - 10, "doc2": now - 5})
        assert reg.is_tombstoned("doc1")
        assert reg.is_tombstoned("doc2")

    def test_default_ttl(self):
        """TombstoneRegistry uses default TTL."""
        reg = TombstoneRegistry()
        assert reg._ttl == DEFAULT_TOMBSTONE_TTL_SEC


class TestDeterministicLWWTiebreaker:
    def test_stable_across_invocations(self):
        """Same inputs always produce the same winner."""
        doc_a = {"_id": "d1", "x": "a", "_lastModified": 10, "_vclock": {"n1": 1}}
        doc_b = {"_id": "d1", "x": "b", "_lastModified": 10, "_vclock": {"n2": 1}}
        results = {_deterministic_lww(doc_a, doc_b)["x"] for _ in range(20)}
        assert len(results) == 1

    def test_symmetric(self):
        """Swapping local/remote produces a consistent winner."""
        doc_a = {"_id": "d1", "x": "a", "_lastModified": 10, "_vclock": {"n1": 1}}
        doc_b = {"_id": "d1", "x": "b", "_lastModified": 10, "_vclock": {"n2": 1}}
        w1 = _deterministic_lww(doc_a, doc_b)["x"]
        w2 = _deterministic_lww(doc_b, doc_a)["x"]
        assert w1 == w2

    def test_higher_node_wins(self):
        """With concurrent clocks and equal timestamps, higher node_id wins."""
        doc_lo = {"_id": "d1", "x": "lo", "_lastModified": 5, "_vclock": {"aaa": 1}}
        doc_hi = {"_id": "d1", "x": "hi", "_lastModified": 5, "_vclock": {"zzz": 1}}
        assert _deterministic_lww(doc_lo, doc_hi)["x"] == "hi"
        assert _deterministic_lww(doc_hi, doc_lo)["x"] == "hi"


class TestSyncOverflowError:
    def test_is_runtime_error(self):
        assert issubclass(SyncOverflowError, RuntimeError)

    def test_message(self):
        err = SyncOverflowError("oplog overflow")
        assert "oplog overflow" in str(err)
