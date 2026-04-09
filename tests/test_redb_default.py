"""
Test that MongoClient uses the redb embedded backend for local URIs.
"""

import os
import tempfile

import pytest

from smongo.client import MongoClient


def test_default_backend_is_redb():
    """Verify that local:// URIs use redb."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test")
        client = MongoClient(f"local://{db_path}")

        assert hasattr(client, "backend")
        assert client.backend == "redb"
        assert type(client.client).__name__ == "RedbClient"

        client.close()


def test_embedded_uri_only_accepts_local_scheme():
    """URIs with a scheme must use exactly local:// — not legacy typos or other protocols."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "data")
        with pytest.raises(ValueError, match="Unsupported URI scheme"):
            MongoClient(f"local+bad://{db_path}")
        with pytest.raises(ValueError, match="Unsupported URI scheme"):
            MongoClient(f"file://{db_path}")


def test_embedded_bare_path_without_scheme():
    """A path with no :// is still accepted (same as passing it to local://)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "bare")
        client = MongoClient(db_path)
        assert client.backend == "redb"
        client.close()


def test_redb_crud_through_mongoclient():
    """Test full CRUD operations through MongoClient with redb."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_crud")
        client = MongoClient(f"local://{db_path}")

        db = client["testdb"]
        coll = db["users"]

        # Insert
        result = coll.insert_one({"name": "Alice", "age": 30})
        assert result.inserted_ids is not None

        # Find
        doc = coll.find_one({"name": "Alice"})
        assert doc is not None
        assert doc["name"] == "Alice"
        assert doc["age"] == 30

        # Update
        result = coll.update_one({"name": "Alice"}, {"$set": {"age": 31}})
        assert result.matched_count == 1
        assert result.modified_count == 1

        # Verify
        doc = coll.find_one({"name": "Alice"})
        assert doc["age"] == 31

        # Count
        count = coll.count_documents({})
        assert count == 1

        # Delete
        result = coll.delete_one({"name": "Alice"})
        assert result.deleted_count == 1

        count = coll.count_documents({})
        assert count == 0

        client.close()


def test_redb_indexes_through_mongoclient():
    """Test index operations through MongoClient with redb."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_idx")
        client = MongoClient(f"local://{db_path}")

        db = client["testdb"]
        coll = db["users"]

        # Create index
        index_name = coll.create_index({"email": 1})
        assert "email" in index_name

        # List indexes
        indexes = coll.list_indexes()
        assert len(indexes) > 0

        # Drop index
        coll.drop_index(index_name)

        client.close()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
