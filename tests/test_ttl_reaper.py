"""TTL expiry via engine ``reap_expired`` on redb-backed collections."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from smongo.storage import _TTL_DELETE_BATCH_SIZE


class TestTTLReapExpired:
    def test_batch_size_constant(self):
        assert _TTL_DELETE_BATCH_SIZE == 500

    def test_reap_removes_expired_datetime_docs(self, local_collection):
        now = datetime.now(tz=UTC)
        local_collection.create_index([("ts", 1)], expireAfterSeconds=60)
        local_collection.insert_many(
            [
                {"name": "expired_1", "ts": now - timedelta(seconds=200)},
                {"name": "expired_2", "ts": now - timedelta(seconds=200)},
                {"name": "fresh", "ts": now + timedelta(hours=1)},
            ]
        )
        deleted = local_collection.reap_expired()
        assert deleted >= 2
        remaining = local_collection.find({})
        names = [d["name"] for d in remaining]
        assert "fresh" in names
        assert "expired_1" not in names
        assert "expired_2" not in names

    def test_reap_no_ttl_indexes_noop(self, local_collection):
        local_collection.insert_one({"name": "x"})
        assert local_collection.reap_expired() == 0
        assert len(local_collection.find({})) == 1

    def test_reap_idempotent(self, local_collection):
        now = datetime.now(tz=UTC)
        local_collection.create_index([("ts", 1)], expireAfterSeconds=60)
        local_collection.insert_one({"name": "old", "ts": now - timedelta(minutes=5)})
        assert local_collection.reap_expired() >= 1
        assert local_collection.reap_expired() == 0
