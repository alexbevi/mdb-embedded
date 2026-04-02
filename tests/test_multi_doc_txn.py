"""Tests for WT-native multi-document transactions."""

from __future__ import annotations

import pytest

from smongo.storage import LocalClient
from smongo.storage.transaction import TransactionSession, _txn_state, get_active_txn_session


@pytest.fixture
def client(tmp_path):
    c = LocalClient(str(tmp_path / "wt"), durable=False)
    yield c
    _txn_state.session = None
    c.close()


@pytest.fixture
def db(client):
    return client.get_db("txntest")


# ── TransactionSession unit tests ────────────────────────────────────


class TestTransactionSession:
    def test_activate_deactivate(self, client):
        txn = TransactionSession(client.conn)
        assert get_active_txn_session() is None
        txn.activate()
        assert get_active_txn_session() is txn.session
        txn.deactivate()
        assert get_active_txn_session() is None
        txn.rollback()

    def test_commit(self, client, db):
        coll = db.get_collection("c1")
        txn = TransactionSession(client.conn)
        txn.activate()
        coll.insert_one({"_id": "a", "v": 1})
        txn.commit()
        assert get_active_txn_session() is None
        assert coll.get_by_id("a") is not None

    def test_rollback(self, client, db):
        coll = db.get_collection("c2")
        coll.insert_one({"_id": "pre", "v": 0})

        txn = TransactionSession(client.conn)
        txn.activate()
        coll.insert_one({"_id": "gone", "v": 99})
        txn.rollback()

        assert get_active_txn_session() is None
        assert coll.get_by_id("gone") is None
        assert coll.get_by_id("pre") is not None


# ── Atomicity tests ──────────────────────────────────────────────────


class TestAtomicity:
    def test_abort_multi_collection_insert(self, client, db):
        """Start txn, insert into 2 collections, abort -> both empty."""
        coll_a = db.get_collection("atom_a")
        coll_b = db.get_collection("atom_b")

        txn = TransactionSession(client.conn)
        txn.activate()
        coll_a.insert_one({"_id": "xa", "v": 1})
        coll_b.insert_one({"_id": "xb", "v": 2})
        txn.rollback()

        assert coll_a.get_by_id("xa") is None
        assert coll_b.get_by_id("xb") is None

    def test_commit_multi_collection_insert(self, client, db):
        """Start txn, insert into 2 collections, commit -> both present."""
        coll_a = db.get_collection("atom_ca")
        coll_b = db.get_collection("atom_cb")

        txn = TransactionSession(client.conn)
        txn.activate()
        coll_a.insert_one({"_id": "ya", "v": 10})
        coll_b.insert_one({"_id": "yb", "v": 20})
        txn.commit()

        assert coll_a.get_by_id("ya")["v"] == 10
        assert coll_b.get_by_id("yb")["v"] == 20


# ── Rollback restores original values ────────────────────────────────


class TestRollback:
    def test_rollback_update(self, client, db):
        coll = db.get_collection("rb_upd")
        coll.insert_one({"_id": "u1", "v": 100})

        txn = TransactionSession(client.conn)
        txn.activate()
        coll.update({"_id": "u1"}, {"$set": {"v": 999}})
        txn.rollback()

        doc = coll.get_by_id("u1")
        assert doc["v"] == 100

    def test_rollback_delete(self, client, db):
        coll = db.get_collection("rb_del")
        coll.insert_one({"_id": "d1", "v": 42})

        txn = TransactionSession(client.conn)
        txn.activate()
        coll.delete({"_id": "d1"})
        txn.rollback()

        assert coll.get_by_id("d1") is not None


# ── Cross-collection atomic ops ──────────────────────────────────────


class TestCrossCollection:
    def test_insert_and_delete_across_collections(self, client, db):
        """Insert in A, delete from B in a single txn, then commit."""
        coll_a = db.get_collection("cross_a")
        coll_b = db.get_collection("cross_b")
        coll_b.insert_one({"_id": "b1", "v": 1})

        txn = TransactionSession(client.conn)
        txn.activate()
        coll_a.insert_one({"_id": "a1", "v": 2})
        coll_b.delete({"_id": "b1"})
        txn.commit()

        assert coll_a.get_by_id("a1") is not None
        assert coll_b.get_by_id("b1") is None

    def test_cross_collection_abort(self, client, db):
        coll_a = db.get_collection("cross_abort_a")
        coll_b = db.get_collection("cross_abort_b")
        coll_b.insert_one({"_id": "bk", "v": 1})

        txn = TransactionSession(client.conn)
        txn.activate()
        coll_a.insert_one({"_id": "ak", "v": 2})
        coll_b.delete({"_id": "bk"})
        txn.rollback()

        assert coll_a.get_by_id("ak") is None
        assert coll_b.get_by_id("bk") is not None


# ── Backward compatibility ───────────────────────────────────────────


class TestBackwardCompat:
    def test_non_transactional_writes_still_work(self, client, db):
        """Without an active TransactionSession, writes auto-commit as before."""
        coll = db.get_collection("compat")
        coll.insert_one({"_id": "z1", "v": 1})
        assert coll.get_by_id("z1") is not None

    def test_find_works_without_txn(self, client, db):
        coll = db.get_collection("compat_find")
        coll.insert_many([{"_id": f"f{i}", "v": i} for i in range(5)])
        assert len(coll.find({})) == 5

    def test_update_works_without_txn(self, client, db):
        coll = db.get_collection("compat_upd")
        coll.insert_one({"_id": "u", "v": 1})
        coll.update({"_id": "u"}, {"$set": {"v": 2}})
        assert coll.get_by_id("u")["v"] == 2

    def test_delete_works_without_txn(self, client, db):
        coll = db.get_collection("compat_del")
        coll.insert_one({"_id": "d", "v": 1})
        coll.delete({"_id": "d"})
        assert coll.get_by_id("d") is None


# ── Isolation (basic snapshot) ───────────────────────────────────────


class TestIsolation:
    def test_uncommitted_invisible_to_other_session(self, client, db):
        """Data written in a txn is not visible to other sessions until commit."""
        coll = db.get_collection("iso")

        txn = TransactionSession(client.conn)
        txn.activate()
        coll.insert_one({"_id": "inv", "v": 1})

        _txn_state.session = None
        result = coll.get_by_id("inv")
        assert result is None

        txn.activate()
        txn.commit()
        result = coll.get_by_id("inv")
        assert result is not None
