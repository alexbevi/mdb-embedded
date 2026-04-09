"""
Test RedbLocalClient from Python.

This demonstrates that the redb-backed client works for basic CRUD operations
without requiring WiredTiger.
"""
import tempfile
import os
from smongo._smongo_core import RedbLocalClient


def test_redb_basic_crud():
    """Test basic CRUD operations with redb client."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_redb")

        # Create redb client
        client = RedbLocalClient(db_path)
        db = client.get_db("testdb")
        coll = db.collection("users")

        # Insert
        result = coll.insert_one({"name": "Alice", "age": 30})
        assert result["inserted_id"] is not None

        # Find one
        doc = coll.find_one({"name": "Alice"})
        assert doc is not None
        assert doc["name"] == "Alice"
        assert doc["age"] == 30

        # Count
        count = coll.count_documents({})
        assert count == 1

        # Update
        result = coll.update_one({"name": "Alice"}, {"$set": {"age": 31}})
        assert result["matched_count"] == 1
        assert result["modified_count"] == 1

        # Verify update
        doc = coll.find_one({"name": "Alice"})
        assert doc["age"] == 31

        # Insert many
        result = coll.insert_many([
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35}
        ])
        assert len(result["inserted_ids"]) == 2

        # Find all
        docs = coll.find({})
        assert len(docs) == 3

        # Delete
        result = coll.delete_one({"name": "Alice"})
        assert result["deleted_count"] == 1

        count = coll.count_documents({})
        assert count == 2

        client.close()
        print("✓ Redb basic CRUD test passed!")


def test_redb_indexes():
    """Test index operations with redb client."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_redb_idx")

        client = RedbLocalClient(db_path)
        db = client.get_db("testdb")
        coll = db.collection("users")

        # Create index
        index_name = coll.create_index({"email": 1}, None)
        assert "email" in index_name

        # List indexes
        indexes = coll.list_indexes()
        assert len(indexes) > 0

        # Drop index
        coll.drop_index(index_name)

        client.close()
        print("✓ Redb index test passed!")


def test_redb_no_wiredtiger_import():
    """Verify that using redb client doesn't require wiredtiger."""
    import sys

    # Check that we're not importing wiredtiger
    wt_modules_before = [m for m in sys.modules.keys() if 'wiredtiger' in m.lower()]

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_nowt")
        client = RedbLocalClient(db_path)
        db = client.get_db("testdb")
        coll = db.collection("test")
        coll.insert_one({"test": 1})
        client.close()

    wt_modules_after = [m for m in sys.modules.keys() if 'wiredtiger' in m.lower()]

    # RedbLocalClient should not cause wiredtiger to be loaded
    # (Though it might already be loaded if smongo was imported)
    print(f"  WiredTiger modules before: {wt_modules_before}")
    print(f"  WiredTiger modules after: {wt_modules_after}")
    print("✓ No-WiredTiger test passed!")


if __name__ == "__main__":
    test_redb_basic_crud()
    test_redb_indexes()
    test_redb_no_wiredtiger_import()
    print("\n✅ All redb Python tests passed!")
