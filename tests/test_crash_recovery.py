"""Crash-recovery tests using multiprocessing + os._exit().

Each test spawns a child process that writes to a durable redb-backed
directory and then crashes (via ``os._exit(1)`` -- no cleanup).  The
parent reopens the same directory and verifies state.
"""

from __future__ import annotations

import multiprocessing
import os

import pytest

from smongo._smongo_core import RedbLocalClient

# ── Harness helpers ──────────────────────────────────────────────────


def run_and_crash(db_path: str, writer_fn, timeout: int = 30, args_extra: tuple = ()) -> int:
    """Run *writer_fn(db_path, *args_extra)* in a subprocess that crashes."""
    p = multiprocessing.Process(target=writer_fn, args=(db_path, *args_extra))
    p.start()
    p.join(timeout=timeout)
    if p.is_alive():
        p.terminate()
        p.join(timeout=5)
    return p.exitcode or 0


def _reopen_client(db_path: str) -> RedbLocalClient:
    """Reopen a durable client on the same directory."""
    return RedbLocalClient(db_path)


# ── Writer functions (run in child process) ──────────────────────────


def _writer_committed(db_path: str) -> None:
    """Insert 200 docs, commit, then crash."""
    client = RedbLocalClient(db_path)
    db = client.get_db("testdb")
    coll = db.collection("committed")
    for i in range(200):
        coll.insert_one({"_id": f"doc_{i}", "v": i})
    client.checkpoint()
    os._exit(1)


def _writer_uncommitted(db_path: str) -> None:
    """Begin a wire transaction, insert docs, crash WITHOUT committing."""
    client = RedbLocalClient(db_path)
    client.wire_txn_begin()
    db = client.get_db("testdb")
    coll = db.collection("uncommitted")
    for i in range(50):
        coll.insert_one({"_id": f"u_{i}", "v": i})
    os._exit(1)


def _writer_with_index(db_path: str) -> None:
    """Create index, insert docs, crash."""
    client = RedbLocalClient(db_path)
    db = client.get_db("testdb")
    coll = db.collection("indexed")
    coll.create_index({"v": 1}, {})
    for i in range(100):
        coll.insert_one({"_id": f"idx_{i}", "v": i})
    os._exit(1)


def _writer_checkpoint_then_more(db_path: str) -> None:
    """Write batch 1 (committed), start wire txn for batch 2 (uncommitted), crash."""
    client = RedbLocalClient(db_path)
    db = client.get_db("testdb")
    coll = db.collection("partial")
    for i in range(50):
        coll.insert_one({"_id": f"cp_{i}", "v": i})
    client.wire_txn_begin()
    for i in range(50, 100):
        coll.insert_one({"_id": f"cp_{i}", "v": i})
    os._exit(1)


def _writer_cycle(db_path: str, cycle: int) -> None:
    """Write 10 docs for a given cycle, checkpoint, crash."""
    client = RedbLocalClient(db_path)
    db = client.get_db("testdb")
    coll = db.collection("cycles")
    for i in range(10):
        coll.insert_one({"_id": f"c{cycle}_d{i}", "cycle": cycle})
    client.checkpoint()
    os._exit(1)


def _writer_multi_doc_txn_crash(db_path: str) -> None:
    """Start multi-doc wire txn across 2 collections, crash before commit."""
    client = RedbLocalClient(db_path)
    client.wire_txn_begin()
    db = client.get_db("testdb")
    coll_a = db.collection("mc_a")
    coll_b = db.collection("mc_b")
    coll_a.insert_one({"_id": "ta", "v": 1})
    coll_b.insert_one({"_id": "tb", "v": 2})
    os._exit(1)


# ── Tests ────────────────────────────────────────────────────────────


@pytest.fixture
def crash_dir(tmp_path):
    """Provide a fresh directory for crash-recovery tests."""
    d = str(tmp_path / "crash_redb")
    os.makedirs(d, exist_ok=True)
    return d


class TestCrashRecovery:
    def test_committed_data_survives(self, crash_dir):
        """Committed + checkpointed data is present after crash."""
        rc = run_and_crash(crash_dir, _writer_committed)
        assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        coll = db.collection("committed")
        docs = coll.find({})
        assert len(docs) == 200
        client.close()

    def test_uncommitted_data_lost(self, crash_dir):
        """Data written inside an uncommitted wire transaction is not visible."""
        rc = run_and_crash(crash_dir, _writer_uncommitted)
        assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        coll = db.collection("uncommitted")
        docs = coll.find({})
        assert len(docs) == 0
        client.close()

    def test_index_consistency_after_crash(self, crash_dir):
        """After crash, index scan matches collection scan."""
        rc = run_and_crash(crash_dir, _writer_with_index)
        assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        coll = db.collection("indexed")
        all_docs = coll.find({})
        assert len(all_docs) == 100
        found = coll.find({"v": {"$gte": 0}})
        assert len(found) == len(all_docs)
        client.close()

    def test_checkpoint_recovery(self, crash_dir):
        """Checkpointed data present; post-checkpoint uncommitted data absent."""
        rc = run_and_crash(crash_dir, _writer_checkpoint_then_more)
        assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        coll = db.collection("partial")
        docs = coll.find({})
        assert len(docs) == 50
        ids = {d["_id"] for d in docs}
        for i in range(50):
            assert f"cp_{i}" in ids
        client.close()

    def test_multi_doc_txn_crash(self, crash_dir):
        """Multi-doc wire txn that crashes before commit: both colls unmodified."""
        rc = run_and_crash(crash_dir, _writer_multi_doc_txn_crash)
        assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        assert len(db.collection("mc_a").find({})) == 0
        assert len(db.collection("mc_b").find({})) == 0
        client.close()

    def test_repeated_crash_restart_cycles(self, crash_dir):
        """5 rounds of write-crash-reopen to verify cumulative integrity."""
        for cycle in range(5):
            rc = run_and_crash(crash_dir, _writer_cycle, args_extra=(cycle,))
            assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        coll = db.collection("cycles")
        docs = coll.find({})
        assert len(docs) == 50
        for cycle in range(5):
            cycle_docs = [d for d in docs if d.get("cycle") == cycle]
            assert len(cycle_docs) == 10
        client.close()
