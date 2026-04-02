"""Tests for smongo.index -- key encoding, IndexManager, QueryPlanner."""

import os

import pytest
import wiredtiger as wt

from smongo.index import (
    DuplicateKeyError,
    IndexDef,
    IndexManager,
    QueryPlanner,
    _invert_encoded,
    _sortable_encode,
    encode_index_key,
    encode_index_key_prefix,
)
from smongo.objectid import ObjectId

# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def idx_session(tmp_path):
    db_path = str(tmp_path / "idx_wt")
    os.makedirs(db_path)
    conn = wt.wiredtiger_open(db_path, "create")
    session = conn.open_session()
    yield session
    session.close()
    conn.close()


@pytest.fixture
def idx_manager(idx_session):
    return IndexManager(idx_session, "db", "coll")


@pytest.fixture
def planner(idx_manager):
    return QueryPlanner(idx_manager)


# ── Key encoding ─────────────────────────────────────────────────────


class TestSortableEncode:
    def test_none(self):
        assert _sortable_encode(None) == "00"

    def test_bool_false(self):
        assert _sortable_encode(False) == "30"

    def test_bool_true(self):
        assert _sortable_encode(True) == "31"

    def test_positive_int(self):
        enc = _sortable_encode(42)
        assert enc.startswith("1")

    def test_negative_int(self):
        enc = _sortable_encode(-10)
        assert enc.startswith("1")

    def test_float(self):
        enc = _sortable_encode(3.14)
        assert enc.startswith("1")

    def test_string(self):
        enc = _sortable_encode("hello")
        assert enc.startswith("2")

    def test_objectid(self):
        oid = ObjectId()
        enc = _sortable_encode(oid)
        assert enc.startswith("15")

    def test_dict_fallback(self):
        enc = _sortable_encode({"x": 1})
        assert enc.startswith("2")

    def test_numeric_ordering(self):
        values = [-100, -1, 0, 1, 42, 100, 999]
        encoded = [_sortable_encode(v) for v in values]
        assert encoded == sorted(encoded)

    def test_string_ordering(self):
        values = ["apple", "banana", "cherry"]
        encoded = [_sortable_encode(v) for v in values]
        assert encoded == sorted(encoded)


class TestInvertEncoded:
    def test_invert_roundtrip(self):
        enc = _sortable_encode(42)
        inv = _invert_encoded(enc)
        assert _invert_encoded(inv) == enc

    def test_invert_reverses_ordering(self):
        vals = [1, 2, 3]
        encoded = [_sortable_encode(v) for v in vals]
        inverted = [_invert_encoded(e) for e in encoded]
        assert inverted == sorted(inverted, reverse=True)


class TestEncodeIndexKey:
    def test_single_field_asc(self):
        key = encode_index_key([42], "doc1", [1])
        assert "|" in key
        assert key.endswith("doc1")

    def test_multi_field(self):
        key = encode_index_key(["NYC", 30], "doc1", [1, -1])
        parts = key.split("|")
        assert len(parts) == 3  # 2 fields + doc_id

    def test_prefix(self):
        prefix = encode_index_key_prefix([42], [1])
        assert prefix.endswith("|")


# ── IndexDef ─────────────────────────────────────────────────────────


class TestIndexDef:
    def test_fields(self):
        idx = IndexDef("test", [("age", 1), ("name", -1)])
        assert idx.fields == ["age", "name"]

    def test_directions(self):
        idx = IndexDef("test", [("age", 1), ("name", -1)])
        assert idx.directions == [1, -1]

    def test_to_dict(self):
        idx = IndexDef("test", [("age", 1)], unique=True, expire_after_seconds=60)
        d = idx.to_dict()
        assert d["name"] == "test"
        assert d["unique"] is True
        assert d["expireAfterSeconds"] == 60


# ── IndexManager ─────────────────────────────────────────────────────


class TestIndexManager:
    def test_create_index_single(self, idx_manager):
        name = idx_manager.create_index([("age", 1)])
        assert name == "age_1"
        assert "age_1" in idx_manager.get_indexes()

    def test_create_index_compound(self, idx_manager):
        name = idx_manager.create_index([("city", 1), ("age", -1)])
        assert name == "city_1_age_-1"

    def test_create_index_string_shorthand(self, idx_manager):
        name = idx_manager.create_index("name")
        assert name == "name_1"

    def test_create_index_unique(self, idx_manager):
        name = idx_manager.create_index([("email", 1)], unique=True)
        idx = idx_manager.get_indexes()[name]
        assert idx.unique is True

    def test_create_index_sparse(self, idx_manager):
        name = idx_manager.create_index([("opt", 1)], sparse=True)
        idx = idx_manager.get_indexes()[name]
        assert idx.sparse is True

    def test_create_index_ttl(self, idx_manager):
        name = idx_manager.create_index([("ts", 1)], expireAfterSeconds=300)
        idx = idx_manager.get_indexes()[name]
        assert idx.expire_after_seconds == 300

    def test_create_duplicate_name_noop(self, idx_manager):
        idx_manager.create_index([("age", 1)])
        result = idx_manager.create_index([("age", 1)])
        assert result == "age_1"
        assert len(idx_manager.list_indexes()) == 1

    def test_drop_index(self, idx_manager):
        idx_manager.create_index([("age", 1)])
        idx_manager.drop_index("age_1")
        assert "age_1" not in idx_manager.get_indexes()

    def test_drop_nonexistent_noop(self, idx_manager):
        idx_manager.drop_index("doesnt_exist")

    def test_list_indexes(self, idx_manager):
        idx_manager.create_index([("a", 1)])
        idx_manager.create_index([("b", -1)])
        indexes = idx_manager.list_indexes()
        assert len(indexes) == 2
        names = {i["name"] for i in indexes}
        assert "a_1" in names and "b_-1" in names

    def test_add_and_remove_doc(self, idx_manager, idx_session):
        idx_manager.create_index([("age", 1)])
        doc = {"_id": "d1", "age": 30}
        idx_manager.add_doc(doc)
        idx_manager.remove_doc(doc)

    def test_update_doc(self, idx_manager):
        idx_manager.create_index([("age", 1)])
        old = {"_id": "d1", "age": 30}
        new = {"_id": "d1", "age": 31}
        idx_manager.add_doc(old)
        idx_manager.update_doc(old, new)

    def test_unique_index_rejects_duplicate(self, idx_manager):
        idx_manager.create_index([("email", 1)], unique=True)
        idx_manager.add_doc({"_id": "d1", "email": "a@b.com"})
        with pytest.raises(DuplicateKeyError, match="E11000"):
            idx_manager.add_doc({"_id": "d2", "email": "a@b.com"})

    def test_unique_index_allows_different(self, idx_manager):
        idx_manager.create_index([("email", 1)], unique=True)
        idx_manager.add_doc({"_id": "d1", "email": "a@b.com"})
        idx_manager.add_doc({"_id": "d2", "email": "c@d.com"})

    def test_sparse_skips_none(self, idx_manager, idx_session):
        idx_manager.create_index([("opt", 1)], sparse=True)
        idx_manager.add_doc({"_id": "d1", "opt": None})
        # Verify no entry was added by scanning index table
        idx = idx_manager.get_indexes()["opt_1"]
        cursor = idx_session.open_cursor(idx.table_uri, None, None)
        count = 0
        while cursor.next() == 0:
            count += 1
        cursor.close()
        assert count == 0

    def test_sparse_includes_non_none(self, idx_manager, idx_session):
        idx_manager.create_index([("opt", 1)], sparse=True)
        idx_manager.add_doc({"_id": "d1", "opt": "yes"})
        idx = idx_manager.get_indexes()["opt_1"]
        cursor = idx_session.open_cursor(idx.table_uri, None, None)
        count = 0
        while cursor.next() == 0:
            count += 1
        cursor.close()
        assert count == 1

    def test_rebuild_index(self, idx_manager):
        idx_manager.create_index([("age", 1)])
        docs = [{"_id": f"d{i}", "age": i * 10} for i in range(5)]
        for d in docs:
            idx_manager.add_doc(d)
        idx_manager.rebuild_index("age_1", docs)

    def test_metadata_persists(self, tmp_path):
        db_path = str(tmp_path / "persist_wt")
        os.makedirs(db_path)
        conn = wt.wiredtiger_open(db_path, "create")
        s = conn.open_session()
        mgr = IndexManager(s, "db", "coll")
        mgr.create_index([("age", 1)], unique=True)
        s.close()
        conn.close()

        conn2 = wt.wiredtiger_open(db_path, "create")
        s2 = conn2.open_session()
        mgr2 = IndexManager(s2, "db", "coll")
        indexes = mgr2.get_indexes()
        assert "age_1" in indexes
        assert indexes["age_1"].unique is True
        s2.close()
        conn2.close()


# ── QueryPlanner ─────────────────────────────────────────────────────


class TestQueryPlanner:
    def test_empty_query_collection_scan(self, planner):
        plan = planner.plan({})
        assert plan.plan_type == "collection_scan"

    def test_id_equality_pk_lookup(self, planner):
        plan = planner.plan({"_id": "abc"})
        assert plan.plan_type == "pk_lookup"

    def test_id_dict_not_pk_lookup(self, planner):
        plan = planner.plan({"_id": {"$gt": "abc"}})
        assert plan.plan_type != "pk_lookup"

    def test_or_unindexed_forces_collection_scan(self, planner):
        plan = planner.plan({"$or": [{"x": 1}, {"y": 2}]})
        assert plan.plan_type == "collection_scan"

    def test_or_indexed_branches_use_or_union(self, idx_manager, planner):
        idx_manager.create_index([("x", 1)])
        idx_manager.create_index([("y", 1)])
        plan = planner.plan({"$or": [{"x": 1}, {"y": 2}]})
        assert plan.plan_type == "or_union"
        assert plan.subplans is not None
        assert len(plan.subplans) == 2
        assert all(sp.plan_type == "index_scan" for sp in plan.subplans)

    def test_or_mixed_indexed_unindexed_falls_back(self, idx_manager, planner):
        idx_manager.create_index([("x", 1)])
        plan = planner.plan({"$or": [{"x": 1}, {"y": 2}]})
        assert plan.plan_type == "collection_scan"

    def test_or_with_pk_lookup(self, idx_manager, planner):
        idx_manager.create_index([("x", 1)])
        plan = planner.plan({"$or": [{"_id": "abc"}, {"x": 5}]})
        assert plan.plan_type == "or_union"
        assert plan.subplans is not None
        assert plan.subplans[0].plan_type == "pk_lookup"
        assert plan.subplans[1].plan_type == "index_scan"

    def test_or_union_to_dict(self, idx_manager, planner):
        idx_manager.create_index([("x", 1)])
        idx_manager.create_index([("y", 1)])
        plan = planner.plan({"$or": [{"x": 1}, {"y": 2}]})
        d = plan.to_dict()
        assert d["plan"] == "or_union"
        assert "subplans" in d
        assert len(d["subplans"]) == 2

    def test_indexed_field_uses_index_scan(self, idx_manager, planner):
        idx_manager.create_index([("age", 1)])
        plan = planner.plan({"age": 30})
        assert plan.plan_type == "index_scan"
        assert plan.index_name == "age_1"

    def test_range_query_uses_index(self, idx_manager, planner):
        idx_manager.create_index([("age", 1)])
        plan = planner.plan({"age": {"$gte": 25, "$lte": 35}})
        assert plan.plan_type == "index_scan"

    def test_non_indexed_field_collection_scan(self, planner):
        plan = planner.plan({"unindexed": 42})
        assert plan.plan_type == "collection_scan"

    def test_compound_index_prefix(self, idx_manager, planner):
        idx_manager.create_index([("city", 1), ("age", 1)])
        plan = planner.plan({"city": "NYC"})
        assert plan.plan_type == "index_scan"

    def test_in_uses_index(self, idx_manager, planner):
        idx_manager.create_index([("x", 1)])
        plan = planner.plan({"x": {"$in": [1, 2]}})
        assert plan.plan_type == "index_scan"

    def test_plan_to_dict(self, idx_manager, planner):
        idx_manager.create_index([("age", 1)])
        plan = planner.plan({"age": 30})
        d = plan.to_dict()
        assert d["plan"] == "index_scan"
        assert d["index"] == "age_1"


class TestQueryPlannerExecute:
    def test_execute_index_scan(self, idx_manager, idx_session, planner):
        idx_manager.create_index([("age", 1)])
        docs = [
            {"_id": "d1", "age": 20},
            {"_id": "d2", "age": 30},
            {"_id": "d3", "age": 40},
        ]
        for d in docs:
            idx_manager.add_doc(d)

        plan = planner.plan({"age": 30})
        ids = planner.execute_index_scan(plan, idx_session, "table:__idx_db_coll_age_1")
        assert "d2" in ids

    def test_execute_range_scan(self, idx_manager, idx_session, planner):
        idx_manager.create_index([("age", 1)])
        docs = [{"_id": f"d{i}", "age": i * 10} for i in range(1, 6)]
        for d in docs:
            idx_manager.add_doc(d)

        plan = planner.plan({"age": {"$gte": 20, "$lte": 40}})
        ids = planner.execute_index_scan(plan, idx_session, "table:__idx_db_coll_age_1")
        assert set(ids) == {"d2", "d3", "d4"}

    def test_execute_empty_result(self, idx_manager, idx_session, planner):
        idx_manager.create_index([("age", 1)])
        plan = planner.plan({"age": 999})
        ids = planner.execute_index_scan(plan, idx_session, "table:__idx_db_coll_age_1")
        assert ids == []

    def test_descending_index_scan(self, idx_manager, idx_session, planner):
        idx_manager.create_index([("score", -1)])
        docs = [
            {"_id": "d1", "score": 100},
            {"_id": "d2", "score": 200},
            {"_id": "d3", "score": 300},
        ]
        for d in docs:
            idx_manager.add_doc(d)

        plan = planner.plan({"score": 200})
        ids = planner.execute_index_scan(plan, idx_session, "table:__idx_db_coll_score_-1")
        assert "d2" in ids
