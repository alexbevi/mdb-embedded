"""Unit tests for wire/bson_codec.py -- BSON boundary normalization."""

from datetime import UTC, datetime

from bson import Decimal128, Regex
from bson import ObjectId as BsonObjectId

from smongo.objectid import ObjectId as EngineObjectId
from smongo.wire.bson_codec import (
    normalize_inbound,
    normalize_outbound,
    normalize_outbound_docs,
)


class TestNormalizeInbound:
    def test_objectid_to_engine_objectid(self):
        oid = BsonObjectId()
        doc = {"_id": oid, "name": "Alice"}
        result = normalize_inbound(doc)
        assert isinstance(result["_id"], EngineObjectId)
        assert str(result["_id"]) == str(oid)
        assert result["name"] == "Alice"

    def test_decimal128_to_float(self):
        doc = {"price": Decimal128("19.99")}
        result = normalize_inbound(doc)
        assert isinstance(result["price"], float)
        assert abs(result["price"] - 19.99) < 0.001

    def test_regex_to_dict(self):
        doc = {"pattern": Regex("^abc", "i")}
        result = normalize_inbound(doc)
        assert isinstance(result["pattern"], dict)
        assert result["pattern"]["$regex"] == "^abc"

    def test_nested_objectid(self):
        oid = BsonObjectId()
        doc = {"ref": {"id": oid}}
        result = normalize_inbound(doc)
        assert isinstance(result["ref"]["id"], EngineObjectId)
        assert str(result["ref"]["id"]) == str(oid)

    def test_list_with_objectids(self):
        oid1, oid2 = BsonObjectId(), BsonObjectId()
        doc = {"ids": [oid1, oid2]}
        result = normalize_inbound(doc)
        assert all(isinstance(v, EngineObjectId) for v in result["ids"])
        assert [str(v) for v in result["ids"]] == [str(oid1), str(oid2)]

    def test_plain_types_passthrough(self):
        doc = {"a": 1, "b": "hello", "c": True, "d": None, "e": 3.14}
        result = normalize_inbound(doc)
        assert result == doc

    def test_datetime_preserved(self):
        now = datetime.now(UTC)
        doc = {"ts": now}
        result = normalize_inbound(doc)
        assert result["ts"] is now

    def test_none_returns_none(self):
        assert normalize_inbound(None) is None

    def test_non_dict_returns_as_is(self):
        assert normalize_inbound("hello") == "hello"

    def test_bytes_preserved(self):
        doc = {"data": b"\x01\x02\x03"}
        result = normalize_inbound(doc)
        assert result["data"] == b"\x01\x02\x03"


class TestNormalizeOutbound:
    def test_objectid_hex_string_to_bson_objectid(self):
        oid = str(BsonObjectId())
        doc = {"_id": oid, "name": "Bob"}
        result = normalize_outbound(doc)
        assert isinstance(result["_id"], BsonObjectId)
        assert str(result["_id"]) == oid
        assert result["name"] == "Bob"

    def test_non_objectid_string_stays_string(self):
        doc = {"_id": "custom-id-123", "name": "Eve"}
        result = normalize_outbound(doc)
        assert result["_id"] == "custom-id-123"

    def test_integer_id_stays_integer(self):
        doc = {"_id": 42, "name": "Frank"}
        result = normalize_outbound(doc)
        assert result["_id"] == 42

    def test_engine_objectid_to_bson(self):
        eid = EngineObjectId()
        doc = {"ref": eid}
        result = normalize_outbound(doc)
        assert isinstance(result["ref"], BsonObjectId)
        assert str(result["ref"]) == str(eid)

    def test_nested_id(self):
        oid = str(BsonObjectId())
        doc = {"nested": {"_id": oid}}
        result = normalize_outbound(doc)
        assert isinstance(result["nested"]["_id"], BsonObjectId)

    def test_list_values(self):
        doc = {"tags": ["a", "b", "c"]}
        result = normalize_outbound(doc)
        assert result["tags"] == ["a", "b", "c"]

    def test_none_returns_none(self):
        assert normalize_outbound(None) is None

    def test_normalize_outbound_docs(self):
        oid1, oid2 = str(BsonObjectId()), str(BsonObjectId())
        docs = [{"_id": oid1, "x": 1}, {"_id": oid2, "x": 2}]
        result = normalize_outbound_docs(docs)
        assert len(result) == 2
        assert isinstance(result[0]["_id"], BsonObjectId)
        assert isinstance(result[1]["_id"], BsonObjectId)

    def test_non_hex_24char_string_not_converted(self):
        doc = {"_id": "zzzzzzzzzzzzzzzzzzzzzzzz"}
        result = normalize_outbound(doc)
        assert isinstance(result["_id"], str)
