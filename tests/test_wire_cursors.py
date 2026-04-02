"""Unit tests for wire/cursors.py -- CursorRegistry lifecycle and batching."""

import time

import pytest

from smongo.wire.cursors import CursorRegistry


@pytest.fixture
def registry():
    return CursorRegistry(default_batch_size=3, idle_timeout_sec=600, max_cursors=100)


class TestCursorRegistryCreate:
    def test_small_result_exhausted_immediately(self, registry):
        docs = [{"x": i} for i in range(3)]
        cursor_id, batch = registry.create("test.coll", docs)
        assert cursor_id == 0
        assert batch == docs

    def test_exact_batch_size_exhausted(self, registry):
        docs = [{"x": i} for i in range(3)]
        cursor_id, batch = registry.create("test.coll", docs, batch_size=3)
        assert cursor_id == 0
        assert batch == docs

    def test_large_result_creates_cursor(self, registry):
        docs = [{"x": i} for i in range(10)]
        cursor_id, batch = registry.create("test.coll", docs)
        assert cursor_id != 0
        assert len(batch) == 3
        assert batch == docs[:3]

    def test_custom_batch_size(self, registry):
        docs = [{"x": i} for i in range(10)]
        cursor_id, batch = registry.create("test.coll", docs, batch_size=5)
        assert cursor_id != 0
        assert len(batch) == 5


class TestCursorRegistryGetMore:
    def test_get_more_returns_next_batch(self, registry):
        docs = [{"x": i} for i in range(10)]
        cursor_id, first = registry.create("test.coll", docs)
        assert len(first) == 3

        new_id, batch2 = registry.get_more(cursor_id)
        assert len(batch2) == 3
        assert batch2 == docs[3:6]

    def test_get_more_exhausts_cursor(self, registry):
        docs = [{"x": i} for i in range(5)]
        cursor_id, first = registry.create("test.coll", docs)
        assert len(first) == 3

        new_id, batch2 = registry.get_more(cursor_id)
        assert new_id == 0
        assert batch2 == docs[3:5]

    def test_get_more_unknown_cursor(self, registry):
        new_id, batch = registry.get_more(99999)
        assert new_id is None
        assert batch is None

    def test_full_iteration(self, registry):
        docs = [{"x": i} for i in range(8)]
        cursor_id, first = registry.create("test.coll", docs, batch_size=3)

        all_docs = list(first)
        while cursor_id != 0:
            cursor_id, batch = registry.get_more(cursor_id, batch_size=3)
            all_docs.extend(batch)

        assert all_docs == docs

    def test_custom_batch_size_on_get_more(self, registry):
        docs = [{"x": i} for i in range(10)]
        cursor_id, _first_batch = registry.create("test.coll", docs, batch_size=2)
        _next_cursor_id, batch = registry.get_more(cursor_id, batch_size=5)
        assert len(batch) == 5


class TestCursorRegistryKill:
    def test_kill_existing_cursor(self, registry):
        docs = [{"x": i} for i in range(10)]
        cursor_id, _first_batch = registry.create("test.coll", docs)
        killed = registry.kill([cursor_id])
        assert cursor_id in killed

        new_id, batch = registry.get_more(cursor_id)
        assert new_id is None

    def test_kill_nonexistent(self, registry):
        killed = registry.kill([12345])
        assert killed == []

    def test_kill_multiple(self, registry):
        docs = [{"x": i} for i in range(10)]
        c1, _first_batch_a = registry.create("test.a", docs)
        c2, _first_batch_b = registry.create("test.b", docs)
        killed = registry.kill([c1, c2, 99999])
        assert c1 in killed
        assert c2 in killed
        assert len(killed) == 2


class TestCursorRegistryEviction:
    def test_evict_when_at_capacity(self):
        reg = CursorRegistry(default_batch_size=2, max_cursors=2)
        docs = [{"x": i} for i in range(10)]
        c1, _first_batch_a = reg.create("a", docs)
        c2, _first_batch_b = reg.create("b", docs)
        c3, _first_batch_c = reg.create("c", docs)
        assert c3 != 0
        # c1 (oldest) should have been evicted
        nid, batch = reg.get_more(c1)
        assert nid is None

    def test_idle_expiration(self):
        reg = CursorRegistry(default_batch_size=2, idle_timeout_sec=0)
        docs = [{"x": i} for i in range(10)]
        cid, _first_batch = reg.create("a", docs)

        time.sleep(0.05)
        reg._expire_idle()

        nid, batch = reg.get_more(cid)
        assert nid is None


class TestCursorRegistryReaper:
    def test_start_stop_reaper(self):
        reg = CursorRegistry()
        reg.start_reaper()
        assert reg._reaper_thread is not None
        assert reg._reaper_thread.is_alive()
        reg.stop_reaper()
