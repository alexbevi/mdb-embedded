"""Coverage tests for storage helpers and streaming."""

import pytest
from bson import ObjectId as BsonObjectId

from smongo.objectid import ObjectId
from smongo.storage.helpers import _denormalize
from smongo.storage.streaming import StreamingCursor


class TestDenormalize:
    def test_denormalize_bson_objectid(self):
        """_denormalize converts BsonObjectId to smongo ObjectId."""
        bson_oid = BsonObjectId()
        result = _denormalize(bson_oid)
        assert isinstance(result, ObjectId)
        assert str(result) == str(bson_oid)

    def test_denormalize_dict_with_objectid(self):
        """_denormalize recursively processes dicts."""
        bson_oid = BsonObjectId()
        doc = {"_id": bson_oid, "nested": {"oid": bson_oid}}
        result = _denormalize(doc)
        assert isinstance(result["_id"], ObjectId)
        assert isinstance(result["nested"]["oid"], ObjectId)

    def test_denormalize_list_with_objectid(self):
        """_denormalize recursively processes lists."""
        bson_oid = BsonObjectId()
        data = [bson_oid, {"_id": bson_oid}]
        result = _denormalize(data)
        assert isinstance(result[0], ObjectId)
        assert isinstance(result[1]["_id"], ObjectId)

    def test_denormalize_primitive_unchanged(self):
        """_denormalize passes through primitives."""
        assert _denormalize(42) == 42
        assert _denormalize("string") == "string"
        assert _denormalize(None) is None


class TestStreamingCursor:
    def test_streaming_cursor_iterates(self):
        """StreamingCursor yields docs from collection.find()."""

        class FakeColl:
            def find(self, q):
                return iter([{"_id": 1}, {"_id": 2}])

        cursor = StreamingCursor(FakeColl())
        docs = list(cursor)
        assert len(docs) == 2
        assert docs[0]["_id"] == 1

    def test_streaming_cursor_with_query(self):
        """StreamingCursor passes query to find()."""

        class FakeColl:
            def find(self, q):
                if q.get("status") == "active":
                    return iter([{"_id": 1, "status": "active"}])
                return iter([])

        cursor = StreamingCursor(FakeColl(), {"status": "active"})
        docs = list(cursor)
        assert len(docs) == 1
        assert docs[0]["status"] == "active"

    def test_streaming_cursor_no_find_raises(self):
        """StreamingCursor raises TypeError if collection has no find()."""

        class InvalidColl:
            pass

        cursor = StreamingCursor(InvalidColl())
        with pytest.raises(TypeError, match="must implement find"):
            list(cursor)
