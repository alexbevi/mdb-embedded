"""Tests for smongo.storage -- LocalClient, LocalDB, LocalCollection, TTLReaper."""

import threading
import time
from datetime import UTC, datetime

import pytest

from smongo.index import DuplicateKeyError
from smongo.objectid import ObjectId
from smongo.schema import ValidationError
from smongo.storage import (
    DeleteResult,
    InsertResult,
    LocalClient,
    LocalCollection,
    LocalDB,
    TTLReaper,
    UpdateResult,
    _WTError,
)

# ── LocalClient ──────────────────────────────────────────────────────


class TestLocalClient:
    def test_create_client(self, tmp_wt_dir):
        client = LocalClient(tmp_wt_dir)
        assert client.conn is not None

    def test_get_db(self, local_client):
        db = local_client.get_db("mydb")
        assert isinstance(db, LocalDB)
        assert db.name == "mydb"


# ── LocalDB ──────────────────────────────────────────────────────────


class TestLocalDB:
    def test_get_collection(self, local_db):
        coll = local_db.get_collection("users")
        assert isinstance(coll, LocalCollection)
        assert coll.name == "users"

    def test_get_collection_cached(self, local_db):
        c1 = local_db.get_collection("users")
        c2 = local_db.get_collection("users")
        assert c1 is c2

    def test_create_collection_no_validator(self, local_db):
        coll = local_db.create_collection("items")
        assert coll._validator is None

    def test_create_collection_with_validator(self, local_db):
        validator = {"$jsonSchema": {"required": ["name"]}}
        coll = local_db.create_collection("strict", validator=validator)
        assert coll._validator is not None
        assert "required" in coll._validator

    def test_list_collection_names_from_catalog(self, local_db):
        """list_collection_names discovers collections from the WT catalog, not just in-memory."""
        local_db.get_collection("alpha").insert_one({"x": 1})
        local_db.get_collection("beta").insert_one({"x": 2})
        names = local_db.list_collection_names()
        assert "alpha" in names
        assert "beta" in names

    def test_list_collection_names_sorted(self, local_db):
        local_db.get_collection("zebra")
        local_db.get_collection("apple")
        names = local_db.list_collection_names()
        assert names == sorted(names)

    def test_drop_collection_removes_from_catalog(self, local_db):
        coll = local_db.get_collection("todrop")
        coll.insert_one({"_id": "d1", "x": 1})
        coll.create_index([("x", 1)])
        assert "todrop" in local_db.list_collection_names()
        local_db.drop_collection("todrop")
        assert "todrop" not in local_db.list_collection_names()

    def test_drop_collection_clears_data(self, local_db):
        coll = local_db.get_collection("clearme")
        coll.insert_one({"_id": "c1", "v": 42})
        local_db.drop_collection("clearme")
        fresh = local_db.get_collection("clearme")
        assert fresh.get_all() == []

    def test_drop_nonexistent_collection(self, local_db):
        local_db.drop_collection("doesnotexist")


# ── LocalCollection CRUD ─────────────────────────────────────────────


class TestInsert:
    def test_insert_one_auto_id(self, local_collection):
        result = local_collection.insert_one({"name": "Alice"})
        assert len(result.inserted_ids) == 1
        assert isinstance(result.inserted_ids[0], ObjectId)

    def test_insert_one_custom_id(self, local_collection):
        result = local_collection.insert_one({"_id": "custom123", "name": "Bob"})
        assert result.inserted_ids == ["custom123"]

    def test_insert_one_does_not_mutate_original(self, local_collection):
        doc = {"name": "Alice"}
        local_collection.insert_one(doc)
        assert "_id" not in doc  # original dict should not gain _id

    def test_insert_many(self, local_collection):
        docs = [{"name": "A"}, {"name": "B"}, {"name": "C"}]
        result = local_collection.insert_many(docs)
        assert len(result.inserted_ids) == 3

    def test_insert_many_count(self, local_collection):
        result = local_collection.insert_many([{"x": 1}, {"x": 2}])
        assert len(result.inserted_ids) == 2


class TestRead:
    def test_get_all_empty(self, local_collection):
        assert local_collection.get_all() == []

    def test_get_all_with_docs(self, local_collection):
        local_collection.insert_many([{"x": 1}, {"x": 2}])
        docs = local_collection.get_all()
        assert len(docs) == 2

    def test_get_by_id_existing(self, local_collection):
        result = local_collection.insert_one({"_id": "abc", "val": 42})
        doc = local_collection.get_by_id("abc")
        assert doc is not None
        assert doc["val"] == 42

    def test_get_by_id_missing(self, local_collection):
        assert local_collection.get_by_id("nonexistent") is None

    def test_get_by_ids(self, local_collection):
        local_collection.insert_many(
            [
                {"_id": "a", "v": 1},
                {"_id": "b", "v": 2},
                {"_id": "c", "v": 3},
            ]
        )
        docs = local_collection.get_by_ids(["a", "c"])
        assert len(docs) == 2
        vals = {d["v"] for d in docs}
        assert vals == {1, 3}


class TestFind:
    def test_find_empty_query_all(self, populated_collection):
        docs = populated_collection.find({})
        assert len(docs) == 10

    def test_find_equality(self, populated_collection):
        docs = populated_collection.find({"city": "NYC"})
        assert all(d["city"] == "NYC" for d in docs)

    def test_find_comparison(self, populated_collection):
        docs = populated_collection.find({"age": {"$gt": 35}})
        assert all(d["age"] > 35 for d in docs)

    def test_find_pk_lookup(self, local_collection):
        local_collection.insert_one({"_id": "pk1", "x": 99})
        docs = local_collection.find({"_id": "pk1"})
        assert len(docs) == 1
        assert docs[0]["x"] == 99

    def test_find_pk_lookup_missing(self, local_collection):
        docs = local_collection.find({"_id": "nope"})
        assert docs == []

    def test_find_index_scan(self, populated_collection):
        populated_collection.create_index([("age", 1)])
        docs = populated_collection.find({"age": 34})
        assert len(docs) == 1
        assert docs[0]["name"] == "Alice"

    def test_find_no_match(self, populated_collection):
        docs = populated_collection.find({"name": "NoOne"})
        assert docs == []

    def test_find_or_with_indexes(self, populated_collection):
        populated_collection.create_index([("city", 1)])
        populated_collection.create_index([("dept", 1)])
        docs = populated_collection.find({"$or": [{"city": "LA"}, {"dept": "mgmt"}]})
        names = {d["name"] for d in docs}
        assert "Diana" in names
        assert "Ivy" in names
        assert "Charlie" in names
        assert "Hank" in names

    def test_find_or_with_pk(self, local_collection):
        local_collection.insert_many(
            [
                {"_id": "a", "x": 1},
                {"_id": "b", "x": 2},
                {"_id": "c", "x": 3},
            ]
        )
        local_collection.create_index([("x", 1)])
        docs = local_collection.find({"$or": [{"_id": "a"}, {"x": 3}]})
        ids = {d["_id"] for d in docs}
        assert ids == {"a", "c"}

    def test_find_or_deduplicates(self, local_collection):
        local_collection.insert_many(
            [
                {"_id": "a", "x": 1, "y": 10},
                {"_id": "b", "x": 2, "y": 20},
            ]
        )
        local_collection.create_index([("x", 1)])
        local_collection.create_index([("y", 1)])
        docs = local_collection.find({"$or": [{"x": 1}, {"y": 10}]})
        assert len(docs) == 1
        assert docs[0]["_id"] == "a"


class TestExplain:
    def test_explain_collection_scan(self, local_collection):
        plan = local_collection.explain({})
        assert plan["plan"] == "collection_scan"

    def test_explain_pk_lookup(self, local_collection):
        plan = local_collection.explain({"_id": "abc"})
        assert plan["plan"] == "pk_lookup"

    def test_explain_index_scan(self, local_collection):
        local_collection.create_index([("age", 1)])
        plan = local_collection.explain({"age": 30})
        assert plan["plan"] == "index_scan"


class TestUpdate:
    def test_update_single(self, local_collection):
        local_collection.insert_one({"_id": "u1", "x": 1})
        result = local_collection.update({"_id": "u1"}, {"$set": {"x": 99}}, multi=False)
        assert result.modified_count == 1
        doc = local_collection.get_by_id("u1")
        assert doc["x"] == 99

    def test_update_multi(self, populated_collection):
        result = populated_collection.update(
            {"city": "NYC"},
            {"$set": {"city": "New York"}},
            multi=True,
        )
        assert result.modified_count > 1
        assert all(d["city"] != "NYC" for d in populated_collection.find({"city": "NYC"}))

    def test_update_no_match(self, local_collection):
        local_collection.insert_one({"_id": "u1", "x": 1})
        result = local_collection.update({"_id": "nope"}, {"$set": {"x": 2}})
        assert result.modified_count == 0

    def test_update_multi_false_stops_at_one(self, populated_collection):
        result = populated_collection.update(
            {"dept": "eng"},
            {"$inc": {"salary": 1000}},
            multi=False,
        )
        assert result.modified_count == 1


class TestUpsert:
    def test_update_upsert_inserts_on_no_match(self, local_collection):
        result = local_collection.update({"x": 99}, {"$set": {"x": 99, "y": 1}}, upsert=True)
        assert result.upserted_id is not None
        assert result.matched_count == 0
        doc = local_collection.get_by_id(result.upserted_id)
        assert doc["x"] == 99
        assert doc["y"] == 1

    def test_update_upsert_false_no_insert(self, local_collection):
        result = local_collection.update({"x": 99}, {"$set": {"y": 1}}, upsert=False)
        assert result.upserted_id is None
        assert result.matched_count == 0
        assert local_collection.get_all() == []

    def test_update_upsert_with_match_does_update(self, local_collection):
        local_collection.insert_one({"_id": "u1", "x": 1})
        result = local_collection.update({"_id": "u1"}, {"$set": {"x": 2}}, upsert=True)
        assert result.upserted_id is None
        assert result.modified_count == 1
        assert local_collection.get_by_id("u1")["x"] == 2

    def test_upsert_extracts_equality_conditions(self, local_collection):
        result = local_collection.update(
            {"city": "NYC", "age": {"$gt": 20}},
            {"$set": {"name": "New"}},
            upsert=True,
        )
        doc = local_collection.get_by_id(result.upserted_id)
        assert doc["city"] == "NYC"
        assert doc["name"] == "New"
        assert "age" not in doc or not isinstance(doc.get("age"), dict)

    def test_find_one_and_replace_upsert(self, local_collection):
        result = local_collection.find_one_and_replace(
            {"_id": "missing"},
            {"_id": "missing", "x": 42},
            upsert=True,
            return_document="after",
        )
        assert result is not None
        assert result["x"] == 42
        assert local_collection.get_by_id("missing")["x"] == 42

    def test_find_one_and_replace_upsert_returns_none_before(self, local_collection):
        result = local_collection.find_one_and_replace(
            {"_id": "miss2"},
            {"_id": "miss2", "x": 1},
            upsert=True,
            return_document="before",
        )
        assert result is None
        assert local_collection.get_by_id("miss2")["x"] == 1


class TestDelete:
    def test_delete_single(self, local_collection):
        local_collection.insert_one({"_id": "d1", "x": 1})
        result = local_collection.delete({"_id": "d1"}, multi=False)
        assert result.deleted_count == 1
        assert local_collection.get_by_id("d1") is None

    def test_delete_multi(self, populated_collection):
        count_before = len(populated_collection.find({"city": "NYC"}))
        result = populated_collection.delete({"city": "NYC"})
        assert result.deleted_count == count_before
        assert populated_collection.find({"city": "NYC"}) == []

    def test_delete_no_match(self, local_collection):
        local_collection.insert_one({"_id": "d1"})
        result = local_collection.delete({"_id": "nope"})
        assert result.deleted_count == 0


# ── Validation ───────────────────────────────────────────────────────


class TestValidation:
    def test_insert_with_valid_doc(self, local_db):
        validator = {
            "$jsonSchema": {"required": ["name"], "properties": {"name": {"bsonType": "string"}}}
        }
        coll = local_db.create_collection("validated", validator=validator)
        coll.insert_one({"name": "Alice"})

    def test_insert_with_invalid_doc_raises(self, local_db):
        validator = {
            "$jsonSchema": {"required": ["name"], "properties": {"name": {"bsonType": "string"}}}
        }
        coll = local_db.create_collection("validated2", validator=validator)
        with pytest.raises(ValidationError):
            coll.insert_one({"age": 30})


# ── Index lifecycle ──────────────────────────────────────────────────


class TestIndexLifecycle:
    def test_create_and_list(self, local_collection):
        local_collection.create_index([("age", 1)])
        indexes = local_collection.list_indexes()
        assert any(i["name"] == "age_1" for i in indexes)

    def test_drop_index(self, local_collection):
        local_collection.create_index([("age", 1)])
        local_collection.drop_index("age_1")
        assert not any(i["name"] == "age_1" for i in local_collection.list_indexes())

    def test_unique_index_blocks_duplicate(self, local_collection):
        local_collection.create_index([("email", 1)], unique=True)
        local_collection.insert_one({"_id": "1", "email": "a@b.com"})
        with pytest.raises(DuplicateKeyError):
            local_collection.insert_one({"_id": "2", "email": "a@b.com"})


# ── Oplog ────────────────────────────────────────────────────────────


class TestStorageOplog:
    def test_insert_creates_oplog_entry(self, local_collection):
        local_collection.insert_one({"_id": "o1", "x": 1})
        oplog = local_collection.get_oplog()
        assert len(oplog) >= 1
        assert oplog[-1]["op"] == "insert"

    def test_update_creates_oplog_entry(self, local_collection):
        local_collection.insert_one({"_id": "o2", "x": 1})
        local_collection.update({"_id": "o2"}, {"$set": {"x": 2}})
        oplog = local_collection.get_oplog()
        ops = [e["op"] for e in oplog]
        assert "update" in ops

    def test_delete_creates_oplog_entry(self, local_collection):
        local_collection.insert_one({"_id": "o3"})
        local_collection.delete({"_id": "o3"})
        oplog = local_collection.get_oplog()
        ops = [e["op"] for e in oplog]
        assert "delete" in ops

    def test_internal_insert_skips_oplog(self, local_collection):
        local_collection.insert_one({"_id": "int1"}, _internal=True)
        oplog = local_collection.get_oplog()
        assert not any(e.get("doc_id") == "int1" for e in oplog)

    def test_index_create_oplog(self, local_collection):
        local_collection.create_index([("age", 1)])
        oplog = local_collection.get_oplog()
        assert any(e["op"] == "index_create" for e in oplog)

    def test_get_oplog_reader(self, local_collection):
        reader = local_collection.get_oplog_reader()
        assert reader is not None


# ── Version tracking ─────────────────────────────────────────────────


class TestBumpVersion:
    def test_version_increments(self, local_collection):
        v1 = local_collection._bump_version("doc1")
        v2 = local_collection._bump_version("doc1")
        assert v2 == v1 + 1

    def test_different_docs_independent(self, local_collection):
        v_a = local_collection._bump_version("a")
        v_b = local_collection._bump_version("b")
        assert v_a == v_b  # both start at 1


# ── Result object ────────────────────────────────────────────────────


class TestResultTypes:
    def test_insert_result(self):
        r = InsertResult(["id1", "id2"])
        assert r.inserted_ids == ["id1", "id2"]

    def test_update_result(self):
        r = UpdateResult(5, 3)
        assert r.matched_count == 5
        assert r.modified_count == 3
        assert r.upserted_id is None

    def test_update_result_with_upsert(self):
        r = UpdateResult(0, 0, upserted_id="abc")
        assert r.upserted_id == "abc"

    def test_delete_result(self):
        r = DeleteResult(7)
        assert r.deleted_count == 7


# ── TTLReaper ────────────────────────────────────────────────────────


class TestTTLReaper:
    def test_coerce_ts_float(self):
        reaper = TTLReaper.__new__(TTLReaper)
        assert reaper._coerce_ts(1000.5) == 1000.5

    def test_coerce_ts_int(self):
        reaper = TTLReaper.__new__(TTLReaper)
        assert reaper._coerce_ts(1000) == 1000.0

    def test_coerce_ts_iso_string(self):
        reaper = TTLReaper.__new__(TTLReaper)
        result = reaper._coerce_ts("2025-01-01T00:00:00+00:00")
        assert isinstance(result, float)

    def test_coerce_ts_datetime(self):
        reaper = TTLReaper.__new__(TTLReaper)
        dt = datetime(2025, 1, 1, tzinfo=UTC)
        result = reaper._coerce_ts(dt)
        assert isinstance(result, float)

    def test_coerce_ts_naive_datetime(self):
        reaper = TTLReaper.__new__(TTLReaper)
        dt = datetime(2025, 1, 1)
        result = reaper._coerce_ts(dt)
        assert isinstance(result, float)

    def test_coerce_ts_none(self):
        reaper = TTLReaper.__new__(TTLReaper)
        assert reaper._coerce_ts(None) is None

    def test_coerce_ts_garbage(self):
        reaper = TTLReaper.__new__(TTLReaper)
        assert reaper._coerce_ts("not-a-date") is None

    def test_coerce_ts_non_string_type(self):
        reaper = TTLReaper.__new__(TTLReaper)
        assert reaper._coerce_ts([1, 2, 3]) is None

    def test_ttl_reap_expired_doc(self, local_collection):
        local_collection.create_index([("expiresAt", 1)], expireAfterSeconds=1)
        past = time.time() - 100
        local_collection.insert_one({"_id": "exp1", "expiresAt": past})
        local_collection.insert_one({"_id": "alive", "expiresAt": time.time() + 1000})

        reaper = local_collection._ttl_reaper
        reaper._reap_once()

        assert local_collection.get_by_id("exp1") is None
        assert local_collection.get_by_id("alive") is not None

    def test_maybe_start_no_ttl_index(self, local_collection):
        reaper = local_collection._ttl_reaper
        reaper.maybe_start()
        assert reaper._thread is None or not reaper._thread.is_alive()

    def test_maybe_start_with_ttl_index(self, local_collection):
        local_collection.create_index([("ts", 1)], expireAfterSeconds=60)
        reaper = local_collection._ttl_reaper
        reaper.maybe_start()
        assert reaper._thread is not None and reaper._thread.is_alive()
        reaper.stop()


# ── Index-accelerated writes ─────────────────────────────────────────


class TestIndexAcceleratedWrites:
    def test_update_uses_pk_lookup(self, local_collection):
        local_collection.insert_many([{"_id": f"d{i}", "x": i} for i in range(50)])
        result = local_collection.update({"_id": "d25"}, {"$set": {"x": 999}}, multi=False)
        assert result.modified_count == 1
        assert local_collection.get_by_id("d25")["x"] == 999

    def test_delete_uses_index_scan(self, local_collection):
        local_collection.insert_many([{"_id": f"d{i}", "age": i % 5} for i in range(50)])
        local_collection.create_index([("age", 1)])
        result = local_collection.delete({"age": 0}, multi=True)
        assert result.deleted_count == 10
        assert local_collection.find({"age": 0}) == []


# ── WiredTiger transactions ──────────────────────────────────────────


class TestTransactions:
    def test_insert_rollback_on_validation_error(self, local_db):
        validator = {
            "$jsonSchema": {"required": ["name"], "properties": {"name": {"bsonType": "string"}}}
        }
        coll = local_db.create_collection("txn_test", validator=validator)
        coll.create_index([("email", 1)], unique=True)
        coll.insert_one({"name": "Alice", "email": "a@b.com"})
        with pytest.raises(Exception):
            coll.insert_one({"age": 30})
        assert len(coll.get_all()) == 1

    def test_update_rollback_on_unique_violation(self, local_db):
        coll = local_db.create_collection("txn_test2")
        coll.create_index([("code", 1)], unique=True)
        coll.insert_one({"_id": "a", "code": "X"})
        coll.insert_one({"_id": "b", "code": "Y"})
        with pytest.raises(Exception):
            coll.update({"_id": "b"}, {"$set": {"code": "X"}}, multi=False)
        assert coll.get_by_id("b")["code"] == "Y"


# ── Thread safety ────────────────────────────────────────────────────


class TestThreadSafety:
    def test_concurrent_inserts(self, local_collection):
        errors = []

        def worker(thread_id):
            try:
                for i in range(10):
                    local_collection.insert_one({"thread": thread_id, "i": i})
            except (
                DuplicateKeyError,
                ValidationError,
                _WTError,
                KeyError,
                TypeError,
                ValueError,
                RuntimeError,
                OSError,
            ) as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(local_collection.get_all()) == 100

    def test_concurrent_update_serialization(self, local_collection):
        """Verify that concurrent updates to the same doc serialize correctly."""
        local_collection.insert_one({"_id": "counter", "n": 0})
        errors = []

        def incrementer():
            try:
                for _ in range(50):
                    local_collection.update({"_id": "counter"}, {"$inc": {"n": 1}}, multi=False)
            except (
                _WTError,
                DuplicateKeyError,
                ValidationError,
                KeyError,
                TypeError,
                ValueError,
                RuntimeError,
                OSError,
            ) as e:
                errors.append(e)

        threads = [threading.Thread(target=incrementer) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        doc = local_collection.get_by_id("counter")
        assert doc["n"] == 200

    def test_concurrent_read_write(self, local_collection):
        local_collection.insert_many([{"_id": f"rw{i}", "v": i} for i in range(20)])
        errors = []

        def reader():
            try:
                for _ in range(20):
                    local_collection.find({"v": {"$gte": 0}})
            except (_WTError, KeyError, TypeError, ValueError, RuntimeError, OSError) as e:
                errors.append(e)

        def writer():
            try:
                for i in range(20):
                    local_collection.update({"_id": f"rw{i % 20}"}, {"$inc": {"v": 1}}, multi=False)
            except (
                _WTError,
                DuplicateKeyError,
                ValidationError,
                KeyError,
                TypeError,
                ValueError,
                RuntimeError,
                OSError,
            ) as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(3)] + [
            threading.Thread(target=writer) for _ in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors

    def test_concurrent_find_one_and_update(self, local_collection):
        """Verify find_one_and_update serializes correctly with concurrent reads."""
        local_collection.insert_one({"_id": "fau", "n": 0})
        errors = []

        def updater():
            try:
                for _ in range(50):
                    local_collection.find_one_and_update(
                        {"_id": "fau"},
                        {"$inc": {"n": 1}},
                        return_document="after",
                    )
            except (
                _WTError,
                DuplicateKeyError,
                ValidationError,
                KeyError,
                TypeError,
                ValueError,
                RuntimeError,
                OSError,
            ) as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(50):
                    local_collection.find({"n": {"$gte": 0}})
            except (_WTError, KeyError, TypeError, ValueError, RuntimeError, OSError) as e:
                errors.append(e)

        threads = [threading.Thread(target=updater) for _ in range(3)] + [
            threading.Thread(target=reader) for _ in range(3)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        doc = local_collection.get_by_id("fau")
        assert doc["n"] == 150


# ── BSON storage ─────────────────────────────────────────────────────


class TestBSONStorage:
    def test_objectid_roundtrip(self, local_collection):
        result = local_collection.insert_one({"name": "Alice"})
        oid = result.inserted_ids[0]
        assert isinstance(oid, ObjectId)
        doc = local_collection.get_by_id(oid)
        assert doc is not None
        assert isinstance(doc["_id"], ObjectId)
        assert doc["_id"] == oid
        assert doc["_id"].generation_time is not None

    def test_bson_roundtrip_types(self, local_collection):
        doc = {
            "_id": "bson1",
            "int_val": 42,
            "float_val": 3.14,
            "str_val": "hello",
            "list_val": [1, "two", 3.0],
            "nested": {"a": 1, "b": [2, 3]},
            "bool_val": True,
            "null_val": None,
        }
        local_collection.insert_one(doc)
        result = local_collection.get_by_id("bson1")
        assert result["int_val"] == 42
        assert result["float_val"] == pytest.approx(3.14)
        assert result["str_val"] == "hello"
        assert result["list_val"] == [1, "two", 3.0]
        assert result["nested"] == {"a": 1, "b": [2, 3]}
        assert result["bool_val"] is True
        assert result["null_val"] is None

    def test_bson_storage_crud(self, local_collection):
        local_collection.insert_one({"_id": "bs1", "x": 1})
        local_collection.update({"_id": "bs1"}, {"$set": {"x": 2}})
        doc = local_collection.get_by_id("bs1")
        assert doc["x"] == 2
        local_collection.delete({"_id": "bs1"})
        assert local_collection.get_by_id("bs1") is None


# ── find_one_and_* ───────────────────────────────────────────────────


class TestFindOneAnd:
    def test_find_one_and_update_returns_before(self, local_collection):
        local_collection.insert_one({"_id": "fau1", "x": 1})
        before = local_collection.find_one_and_update(
            {"_id": "fau1"}, {"$set": {"x": 99}}, return_document="before"
        )
        assert before["x"] == 1
        assert local_collection.get_by_id("fau1")["x"] == 99

    def test_find_one_and_update_returns_after(self, local_collection):
        local_collection.insert_one({"_id": "fau2", "x": 1})
        after = local_collection.find_one_and_update(
            {"_id": "fau2"}, {"$set": {"x": 99}}, return_document="after"
        )
        assert after["x"] == 99

    def test_find_one_and_update_no_match(self, local_collection):
        result = local_collection.find_one_and_update({"_id": "nope"}, {"$set": {"x": 1}})
        assert result is None

    def test_find_one_and_replace(self, local_collection):
        local_collection.insert_one({"_id": "far1", "x": 1, "y": 2})
        before = local_collection.find_one_and_replace({"_id": "far1"}, {"x": 99})
        assert before["x"] == 1
        after = local_collection.get_by_id("far1")
        assert after["x"] == 99
        assert "y" not in after

    def test_find_one_and_delete(self, local_collection):
        local_collection.insert_one({"_id": "fad1", "x": 1})
        deleted = local_collection.find_one_and_delete({"_id": "fad1"})
        assert deleted["x"] == 1
        assert local_collection.get_by_id("fad1") is None

    def test_find_one_and_delete_no_match(self, local_collection):
        result = local_collection.find_one_and_delete({"_id": "nope"})
        assert result is None
