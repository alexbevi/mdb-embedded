"""
Test that MongoClient defaults to redb backend.

This verifies Phase 4 - redb is now the primary storage backend.
"""
import os
import tempfile
from smongo.client import MongoClient


def test_default_backend_is_redb():
    """Verify that local:// URIs default to redb."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test")
        client = MongoClient(f"local://{db_path}")

        assert hasattr(client, "backend")
        assert client.backend == "redb"
        assert type(client.client).__name__ == "RedbClient"

        client.close()
        print("✓ Default backend is redb")


def test_explicit_wiredtiger_backend():
    """Verify that local+wt:// URIs use WiredTiger."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_wt")
        client = MongoClient(f"local+wt://{db_path}")

        assert client.backend == "wiredtiger"
        assert type(client.client).__name__ == "LocalClient"

        client.close()
        print("✓ local+wt:// uses WiredTiger")


def test_env_var_backend():
    """Verify SMONGO_BACKEND env var can override."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_env")

        # Force WiredTiger via env var
        os.environ["SMONGO_BACKEND"] = "wiredtiger"
        try:
            client = MongoClient(f"local://{db_path}")
            assert client.backend == "wiredtiger"
            client.close()
        finally:
            del os.environ["SMONGO_BACKEND"]

        print("✓ SMONGO_BACKEND env var works")


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
        print("✓ Full CRUD through MongoClient + redb works!")


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
        print("✓ Index operations through MongoClient + redb work!")


if __name__ == "__main__":
    test_default_backend_is_redb()
    test_explicit_wiredtiger_backend()
    test_env_var_backend()
    test_redb_crud_through_mongoclient()
    test_redb_indexes_through_mongoclient()
    print("\n✅ Phase 4 COMPLETE - redb is now the default backend!")
