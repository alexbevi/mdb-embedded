"""Unit tests for wire/msg.py -- OP_MSG encode/decode round-trips, checksums,
OP_COMPRESSED, and error handling."""

import struct

import pytest
from bson import decode as bson_decode
from bson import encode as bson_encode

from smongo.wire.msg import (
    HEADER_SIZE,
    OP_COMPRESSED,
    OP_MSG,
    OP_QUERY,
    OP_REPLY,
    ProtocolError,
    available_compressors,
    decode_compressed,
    decode_header,
    decode_msg,
    decode_query,
    encode_compressed,
    encode_msg,
    encode_reply,
)


class TestMsgHeader:
    def test_decode_header(self):
        data = struct.pack("<iiii", 100, 1, 0, OP_MSG)
        header = decode_header(data)
        assert header.length == 100
        assert header.request_id == 1
        assert header.response_to == 0
        assert header.op_code == OP_MSG


class TestOpMsg:
    def test_encode_decode_roundtrip(self):
        doc = {"hello": 1, "ok": 1.0, "$db": "admin"}
        encoded = encode_msg(42, 0, doc)

        header, flags, body, seqs = decode_msg(encoded)
        assert header.op_code == OP_MSG
        assert header.request_id == 42
        assert header.response_to == 0
        assert flags == 0
        assert body["hello"] == 1
        assert body["ok"] == 1.0
        assert body["$db"] == "admin"
        assert seqs == {}

    def test_response_to_field(self):
        doc = {"ok": 1.0}
        encoded = encode_msg(10, 5, doc)
        header, _, _, _ = decode_msg(encoded)
        assert header.request_id == 10
        assert header.response_to == 5

    def test_message_length_correct(self):
        doc = {"ping": 1}
        encoded = encode_msg(1, 0, doc)
        reported_len = struct.unpack_from("<i", encoded, 0)[0]
        assert reported_len == len(encoded)

    def test_nested_document(self):
        doc = {"find": "users", "filter": {"age": {"$gt": 25}}, "$db": "test"}
        encoded = encode_msg(1, 0, doc)
        _, _, body, _ = decode_msg(encoded)
        assert body["find"] == "users"
        assert body["filter"]["age"]["$gt"] == 25

    def test_kind1_section_decode(self):
        """Hand-craft an OP_MSG with a Kind 1 (document sequence) section."""
        body_doc = {"insert": "users", "$db": "test"}
        body_bson = bson_encode(body_doc)

        insert_docs = [{"name": "Alice"}, {"name": "Bob"}]
        identifier = b"documents\x00"
        docs_bson = b"".join(bson_encode(d) for d in insert_docs)
        section_payload = identifier + docs_bson
        section_size = 4 + len(section_payload)

        kind0 = b"\x00" + body_bson
        kind1 = b"\x01" + struct.pack("<i", section_size) + section_payload

        flags = struct.pack("<I", 0)
        payload = flags + kind0 + kind1
        length = HEADER_SIZE + len(payload)
        header = struct.pack("<iiii", length, 1, 0, OP_MSG)
        full_msg = header + payload

        hdr, flg, body, seqs = decode_msg(full_msg)
        assert body["insert"] == "users"
        assert "documents" in seqs
        assert len(seqs["documents"]) == 2
        assert seqs["documents"][0]["name"] == "Alice"
        assert seqs["documents"][1]["name"] == "Bob"

    def test_unknown_section_kind_raises(self):
        """Section kind 2 (or any unknown value) must raise ProtocolError."""
        body_doc = {"ping": 1}
        body_bson = bson_encode(body_doc)

        kind0 = b"\x00" + body_bson
        bad_section = b"\x02" + b"\x00" * 8

        flags = struct.pack("<I", 0)
        payload = flags + kind0 + bad_section
        length = HEADER_SIZE + len(payload)
        header = struct.pack("<iiii", length, 1, 0, OP_MSG)
        full_msg = header + payload

        with pytest.raises(ProtocolError, match="unknown OP_MSG section kind"):
            decode_msg(full_msg)

    def test_encode_with_checksum(self):
        doc = {"ping": 1}
        encoded = encode_msg(1, 0, doc, include_checksum=True)
        flags = struct.unpack_from("<I", encoded, HEADER_SIZE)[0]
        assert flags & 0x01 == 0x01
        reported_len = struct.unpack_from("<i", encoded, 0)[0]
        assert reported_len == len(encoded)

    def test_checksum_roundtrip(self):
        doc = {"hello": 1, "ok": 1.0}
        encoded = encode_msg(1, 0, doc, include_checksum=True)
        header, flags, body, seqs = decode_msg(encoded)
        assert body["hello"] == 1
        assert flags & 0x01 == 0x01


class TestOpQuery:
    def test_encode_decode_legacy_hello(self):
        """Build a legacy OP_QUERY for isMaster and decode it."""
        query = {"isMaster": 1}
        query_bson = bson_encode(query)
        coll_name = b"admin.$cmd\x00"

        payload = struct.pack("<i", 0)  # flags
        payload += coll_name
        payload += struct.pack("<ii", 0, -1)  # skip, limit
        payload += query_bson

        length = HEADER_SIZE + len(payload)
        header = struct.pack("<iiii", length, 99, 0, OP_QUERY)
        full_msg = header + payload

        hdr, flags, coll, skip, limit, doc = decode_query(full_msg)
        assert hdr.op_code == OP_QUERY
        assert hdr.request_id == 99
        assert coll == "admin.$cmd"
        assert skip == 0
        assert limit == -1
        assert doc["isMaster"] == 1


class TestOpReply:
    def test_encode_reply_single_doc(self):
        doc = {"ismaster": True, "ok": 1.0}
        encoded = encode_reply(10, 5, [doc])

        header = decode_header(encoded)
        assert header.op_code == OP_REPLY
        assert header.request_id == 10
        assert header.response_to == 5

        offset = HEADER_SIZE
        resp_flags = struct.unpack_from("<i", encoded, offset)[0]
        assert resp_flags == 0
        offset += 4
        cursor_id = struct.unpack_from("<q", encoded, offset)[0]
        assert cursor_id == 0
        offset += 8
        starting_from = struct.unpack_from("<i", encoded, offset)[0]
        assert starting_from == 0
        offset += 4
        num_returned = struct.unpack_from("<i", encoded, offset)[0]
        assert num_returned == 1
        offset += 4
        result = bson_decode(encoded[offset:])
        assert result["ismaster"] is True

    def test_encode_reply_multiple_docs(self):
        docs = [{"a": 1}, {"b": 2}]
        encoded = encode_reply(1, 0, docs)
        offset = HEADER_SIZE + 4 + 8 + 4
        num_returned = struct.unpack_from("<i", encoded, offset)[0]
        assert num_returned == 2

    def test_encode_reply_with_response_flags(self):
        doc = {"ok": 0, "errmsg": "fail"}
        encoded = encode_reply(1, 0, [doc], response_flags=0x02)
        offset = HEADER_SIZE
        resp_flags = struct.unpack_from("<i", encoded, offset)[0]
        assert resp_flags == 0x02


class TestOpCompressed:
    def test_zlib_roundtrip(self):
        doc = {"ping": 1, "ok": 1.0}
        original = encode_msg(42, 0, doc)

        compressed = encode_compressed(original, 2)
        header = decode_header(compressed)
        assert header.op_code == OP_COMPRESSED

        decompressed = decode_compressed(compressed)
        inner_header = decode_header(decompressed)
        assert inner_header.op_code == OP_MSG

        _, _, body, _ = decode_msg(decompressed)
        assert body["ping"] == 1

    def test_noop_compression(self):
        doc = {"hello": 1}
        original = encode_msg(1, 0, doc)
        compressed = encode_compressed(original, 0)
        decompressed = decode_compressed(compressed)
        _, _, body, _ = decode_msg(decompressed)
        assert body["hello"] == 1

    def test_unknown_compressor_raises(self):
        doc = {"x": 1}
        original = encode_msg(1, 0, doc)
        inner_payload = original[HEADER_SIZE:]

        payload = struct.pack("<ii", OP_MSG, len(inner_payload))
        payload += struct.pack("B", 99)
        payload += inner_payload

        length = HEADER_SIZE + len(payload)
        header = struct.pack("<iiii", length, 1, 0, OP_COMPRESSED)
        full_msg = header + payload

        with pytest.raises(ProtocolError, match="unknown compressor"):
            decode_compressed(full_msg)


class TestAvailableCompressors:
    def test_zlib_always_available(self):
        assert "zlib" in available_compressors()
