"""Projection on redb path (engine ``find_with_options`` + ``stage_project``)."""

from __future__ import annotations

import os
import tempfile

from smongo.client import MongoClient


def test_redb_find_one_inclusion_projection() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        client = MongoClient(f"local://{os.path.join(tmpdir, 'db')}")
        try:
            coll = client["mydb"]["items"]
            coll.insert_one({"_id": "k1", "a": 1, "b": 2, "c": 3})
            doc = coll.find_one({"_id": "k1"}, {"a": 1, "c": 1})
            assert doc is not None
            assert doc["_id"] == "k1"
            assert doc["a"] == 1
            assert doc["c"] == 3
            assert "b" not in doc
        finally:
            client.close()


def test_redb_find_one_nested_subdocument_inclusion() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        client = MongoClient(f"local://{os.path.join(tmpdir, 'db')}")
        try:
            coll = client["mydb"]["items"]
            coll.insert_one({"_id": "k2", "outer": {"inner": 42}, "x": 0})
            doc = coll.find_one({"_id": "k2"}, {"outer": 1})
            assert doc is not None
            assert doc["_id"] == "k2"
            assert doc["outer"] == {"inner": 42}
            assert "x" not in doc
        finally:
            client.close()


def test_redb_find_one_dotted_path_leaf_inclusion() -> None:
    """``$project`` stores dotted paths as a single top-level key (engine behavior)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        client = MongoClient(f"local://{os.path.join(tmpdir, 'db')}")
        try:
            coll = client["mydb"]["items"]
            coll.insert_one({"_id": "k2b", "outer": {"inner": 99}})
            doc = coll.find_one({"_id": "k2b"}, {"outer.inner": 1})
            assert doc is not None
            assert doc["_id"] == "k2b"
            assert doc["outer.inner"] == 99
        finally:
            client.close()


def test_redb_find_one_exclusion_and_id_off() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        client = MongoClient(f"local://{os.path.join(tmpdir, 'db')}")
        try:
            coll = client["mydb"]["items"]
            coll.insert_one({"_id": "k3", "keep": 1, "drop": 2})
            doc = coll.find_one({"_id": "k3"}, {"drop": 0, "_id": 0})
            assert doc is not None
            assert "_id" not in doc
            assert doc["keep"] == 1
            assert "drop" not in doc
        finally:
            client.close()


def test_redb_find_cursor_projection() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        client = MongoClient(f"local://{os.path.join(tmpdir, 'db')}")
        try:
            coll = client["mydb"]["items"]
            coll.insert_one({"n": 1, "v": "a"})
            coll.insert_one({"n": 2, "v": "b"})
            rows = list(coll.find({}, {"n": 1}))
            assert {r["n"] for r in rows} == {1, 2}
            assert all("v" not in r for r in rows)
        finally:
            client.close()


def test_redb_empty_projection_is_full_document() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        client = MongoClient(f"local://{os.path.join(tmpdir, 'db')}")
        try:
            coll = client["mydb"]["items"]
            coll.insert_one({"_id": "e", "z": 9})
            doc = coll.find_one({"_id": "e"}, {})
            assert doc is not None
            assert doc["z"] == 9
        finally:
            client.close()
