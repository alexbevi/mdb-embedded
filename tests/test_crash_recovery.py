"""Crash-recovery tests using multiprocessing + os._exit().

Each test spawns a child process that writes to a durable WiredTiger
directory and then crashes (via ``os._exit(1)`` -- no cleanup).  The
parent reopens the same directory and verifies state.
"""

from __future__ import annotations

import multiprocessing
import os

import pytest

from smongo.storage import LocalClient

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


def _reopen_client(db_path: str) -> LocalClient:
    """Reopen a durable client on the same directory (WT recovery runs)."""
    return LocalClient(db_path, durable=True)


# ── Writer functions (run in child process) ──────────────────────────


def _writer_committed(db_path: str) -> None:
    """Insert 200 docs, commit, then crash."""
    client = LocalClient(db_path, durable=True)
    db = client.get_db("testdb")
    coll = db.get_collection("committed")
    for i in range(200):
        coll.insert_one({"_id": f"doc_{i}", "v": i})
    client.checkpoint()
    os._exit(1)


def _writer_uncommitted(db_path: str) -> None:
    """Begin a WT transaction, insert docs, crash WITHOUT committing."""
    client = LocalClient(db_path, durable=True)
    session = client.conn.open_session()
    session.begin_transaction()
    db = client.get_db("testdb")
    coll = db.get_collection("uncommitted")
    table_uri = coll.table_uri
    cursor = session.open_cursor(table_uri, None, "overwrite=true")
    from smongo.storage.helpers import _to_bson

    for i in range(50):
        doc = {"_id": f"u_{i}", "v": i}
        cursor[str(doc["_id"])] = _to_bson(doc)
    cursor.close()
    os._exit(1)


def _writer_with_index(db_path: str) -> None:
    """Create index, insert docs, checkpoint, crash."""
    client = LocalClient(db_path, durable=True)
    db = client.get_db("testdb")
    coll = db.get_collection("indexed")
    coll.create_index([("v", 1)])
    for i in range(100):
        coll.insert_one({"_id": f"idx_{i}", "v": i})
    client.checkpoint()
    os._exit(1)


def _writer_checkpoint_then_more(db_path: str) -> None:
    """Write batch 1, checkpoint, write batch 2 (no checkpoint), crash."""
    client = LocalClient(db_path, durable=True)
    db = client.get_db("testdb")
    coll = db.get_collection("partial")
    for i in range(50):
        coll.insert_one({"_id": f"cp_{i}", "v": i})
    client.checkpoint()
    session = client.conn.open_session()
    session.begin_transaction()
    table_uri = coll.table_uri
    cursor = session.open_cursor(table_uri, None, "overwrite=true")
    from smongo.storage.helpers import _to_bson

    for i in range(50, 100):
        doc = {"_id": f"cp_{i}", "v": i}
        cursor[str(doc["_id"])] = _to_bson(doc)
    cursor.close()
    os._exit(1)


def _writer_cycle(db_path: str, cycle: int) -> None:
    """Write 10 docs for a given cycle, checkpoint, crash."""
    client = LocalClient(db_path, durable=True)
    db = client.get_db("testdb")
    coll = db.get_collection("cycles")
    for i in range(10):
        coll.insert_one({"_id": f"c{cycle}_d{i}", "cycle": cycle})
    client.checkpoint()
    os._exit(1)


def _writer_multi_doc_txn_crash(db_path: str) -> None:
    """Start multi-doc txn across 2 collections, crash before commit."""
    client = LocalClient(db_path, durable=True)
    db = client.get_db("testdb")
    db.get_collection("mc_a")
    db.get_collection("mc_b")

    from smongo.storage.transaction import TransactionSession

    txn = TransactionSession(client.conn)
    txn.activate()

    coll_a = db.get_collection("mc_a")
    coll_b = db.get_collection("mc_b")
    coll_a.insert_one({"_id": "ta", "v": 1})
    coll_b.insert_one({"_id": "tb", "v": 2})

    os._exit(1)


# ── Tests ────────────────────────────────────────────────────────────


@pytest.fixture
def crash_dir(tmp_path):
    """Provide a fresh directory for crash-recovery tests."""
    d = str(tmp_path / "crash_wt")
    os.makedirs(d, exist_ok=True)
    return d


class TestCrashRecovery:
    def test_committed_data_survives(self, crash_dir):
        """Committed + checkpointed data is present after crash."""
        rc = run_and_crash(crash_dir, _writer_committed)
        assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        coll = db.get_collection("committed")
        docs = coll.get_all()
        assert len(docs) == 200
        client.close()

    def test_uncommitted_data_lost(self, crash_dir):
        """Data written inside an uncommitted WT txn is not visible."""
        client0 = LocalClient(crash_dir, durable=True)
        db0 = client0.get_db("testdb")
        db0.get_collection("uncommitted")
        client0.close()

        rc = run_and_crash(crash_dir, _writer_uncommitted)
        assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        coll = db.get_collection("uncommitted")
        docs = coll.get_all()
        assert len(docs) == 0
        client.close()

    def test_index_consistency_after_crash(self, crash_dir):
        """After crash, index scan matches collection scan."""
        rc = run_and_crash(crash_dir, _writer_with_index)
        assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        coll = db.get_collection("indexed")
        all_docs = coll.get_all()
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
        coll = db.get_collection("partial")
        docs = coll.get_all()
        assert len(docs) == 50
        ids = {d["_id"] for d in docs}
        for i in range(50):
            assert f"cp_{i}" in ids
        client.close()

    def test_multi_doc_txn_crash(self, crash_dir):
        """Multi-doc txn that crashes before commit: both colls unmodified."""
        client0 = LocalClient(crash_dir, durable=True)
        db0 = client0.get_db("testdb")
        db0.get_collection("mc_a")
        db0.get_collection("mc_b")
        client0.close()

        rc = run_and_crash(crash_dir, _writer_multi_doc_txn_crash)
        assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        assert len(db.get_collection("mc_a").get_all()) == 0
        assert len(db.get_collection("mc_b").get_all()) == 0
        client.close()

    def test_repeated_crash_restart_cycles(self, crash_dir):
        """5 rounds of write-crash-reopen to verify cumulative integrity."""
        for cycle in range(5):
            rc = run_and_crash(crash_dir, _writer_cycle, args_extra=(cycle,))
            assert rc == 1

        client = _reopen_client(crash_dir)
        db = client.get_db("testdb")
        coll = db.get_collection("cycles")
        docs = coll.get_all()
        assert len(docs) == 50
        for cycle in range(5):
            cycle_docs = [d for d in docs if d.get("cycle") == cycle]
            assert len(cycle_docs) == 10
        client.close()
