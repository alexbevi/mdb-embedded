"""Tests for smongo streaming architecture.

Covers:
- StreamingCursor with all query plan types (pk_lookup, index_scan, or_union, collection_scan)
- RustLocalCollection.find_one() (streaming-based first-match)
- RustLocalCollection.count() (streaming-based count without materialization)
- Cursor accepting Iterable[Document] with lazy materialization
- find() vs find_streaming() result parity
- Cursor skip/limit using itertools.islice (no sort → no full materialization)
- Client-level Collection.find(), find_one(), count_documents() backed by streaming
"""

from __future__ import annotations

from typing import Any

import pytest

from smongo.aggregation import Cursor
from smongo.client import Collection, MongoClient
from smongo.storage import StreamingCursor

# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def coll(local_collection: Any, sample_docs: list) -> Any:
    """A collection pre-loaded with sample_docs and a city index."""
    local_collection.insert_many(sample_docs)
    local_collection.create_index([("city", 1)])
    return local_collection


@pytest.fixture
def indexed_coll(coll: Any) -> Any:
    """A collection with compound and single-field indexes."""
    coll.create_index([("city", 1), ("age", -1)])
    coll.create_index([("age", 1)])
    coll.create_index("dept")
    return coll


# ── StreamingCursor: plan type coverage ──────────────────────────────


class TestStreamingCursorPlanTypes:
    """Every plan type the query planner can choose is exercised through the streaming path."""

    def test_collection_scan_empty_query(self, coll: Any) -> None:
        results = list(coll.find_streaming({}))
        assert len(results) == 10
        assert all("name" in d for d in results)

    def test_collection_scan_unindexed_field(self, coll: Any) -> None:
        results = list(coll.find_streaming({"dept": "eng"}))
        assert all(d["dept"] == "eng" for d in results)
        assert len(results) > 0

    def test_pk_lookup_direct_id(self, coll: Any) -> None:
        all_docs = coll.find({})
        target_id = all_docs[0]["_id"]
        results = list(coll.find_streaming({"_id": target_id}))
        assert len(results) == 1
        assert results[0]["_id"] == target_id

    def test_pk_lookup_with_eq_operator(self, coll: Any) -> None:
        all_docs = coll.find({})
        target_id = all_docs[0]["_id"]
        results = list(coll.find_streaming({"_id": {"$eq": target_id}}))
        assert len(results) == 1

    def test_pk_lookup_with_residual_filter(self, coll: Any) -> None:
        all_docs = coll.find({})
        target_id = all_docs[0]["_id"]
        target_name = all_docs[0]["name"]
        results = list(coll.find_streaming({"_id": target_id, "name": target_name}))
        assert len(results) == 1

        results_miss = list(coll.find_streaming({"_id": target_id, "name": "NONEXISTENT"}))
        assert len(results_miss) == 0

    def test_pk_lookup_no_match(self, coll: Any) -> None:
        results = list(coll.find_streaming({"_id": "definitely_not_an_id"}))
        assert len(results) == 0

    def test_index_scan_equality(self, indexed_coll: Any) -> None:
        results = list(indexed_coll.find_streaming({"city": "NYC"}))
        assert all(d["city"] == "NYC" for d in results)
        assert len(results) >= 1

    def test_index_scan_range(self, indexed_coll: Any) -> None:
        results = list(indexed_coll.find_streaming({"age": {"$gt": 35}}))
        assert all(d["age"] > 35 for d in results)

    def test_index_scan_compound(self, indexed_coll: Any) -> None:
        results = list(indexed_coll.find_streaming({"city": "NYC", "age": {"$gt": 30}}))
        assert all(d["city"] == "NYC" and d["age"] > 30 for d in results)

    def test_index_scan_in_operator(self, indexed_coll: Any) -> None:
        results = list(indexed_coll.find_streaming({"city": {"$in": ["NYC", "SF"]}}))
        assert all(d["city"] in ("NYC", "SF") for d in results)
        assert len(results) >= 2

    def test_or_union_indexed_branches(self, indexed_coll: Any) -> None:
        results = list(indexed_coll.find_streaming({"$or": [{"city": "NYC"}, {"city": "SF"}]}))
        assert all(d["city"] in ("NYC", "SF") for d in results)

    def test_or_union_pk_branches(self, coll: Any) -> None:
        all_docs = coll.find({})
        id1, id2 = all_docs[0]["_id"], all_docs[1]["_id"]
        results = list(coll.find_streaming({"$or": [{"_id": id1}, {"_id": id2}]}))
        assert len(results) == 2
        result_ids = {d["_id"] for d in results}
        assert id1 in result_ids
        assert id2 in result_ids


class TestStreamingCursorReturnsType:
    def test_returns_streaming_cursor_type(self, coll: Any) -> None:
        sc = coll.find_streaming({"city": "NYC"})
        assert hasattr(sc, "__iter__") and hasattr(sc, "__next__")

    def test_is_iterable(self, coll: Any) -> None:
        sc = coll.find_streaming({})
        it = iter(sc)
        first = next(it)
        assert "_id" in first


# ── find() vs find_streaming() parity ────────────────────────────────


class TestFindParity:
    """find() (materialized) and find_streaming() (lazy) return the same results."""

    def _ids(self, docs: list) -> set:
        return {str(d["_id"]) for d in docs}

    def test_empty_query_parity(self, coll: Any) -> None:
        materialized = coll.find({})
        streamed = list(coll.find_streaming({}))
        assert self._ids(materialized) == self._ids(streamed)
        assert len(materialized) == len(streamed)

    def test_equality_query_parity(self, indexed_coll: Any) -> None:
        materialized = indexed_coll.find({"city": "NYC"})
        streamed = list(indexed_coll.find_streaming({"city": "NYC"}))
        assert self._ids(materialized) == self._ids(streamed)

    def test_range_query_parity(self, indexed_coll: Any) -> None:
        materialized = indexed_coll.find({"age": {"$gte": 30, "$lte": 40}})
        streamed = list(indexed_coll.find_streaming({"age": {"$gte": 30, "$lte": 40}}))
        assert self._ids(materialized) == self._ids(streamed)

    def test_in_query_parity(self, indexed_coll: Any) -> None:
        materialized = indexed_coll.find({"city": {"$in": ["NYC", "LA"]}})
        streamed = list(indexed_coll.find_streaming({"city": {"$in": ["NYC", "LA"]}}))
        assert self._ids(materialized) == self._ids(streamed)

    def test_or_query_parity(self, indexed_coll: Any) -> None:
        q = {"$or": [{"city": "NYC"}, {"city": "CHI"}]}
        materialized = indexed_coll.find(q)
        streamed = list(indexed_coll.find_streaming(q))
        assert self._ids(materialized) == self._ids(streamed)

    def test_pk_lookup_parity(self, coll: Any) -> None:
        all_docs = coll.find({})
        for doc in all_docs[:3]:
            q = {"_id": doc["_id"]}
            materialized = coll.find(q)
            streamed = list(coll.find_streaming(q))
            assert self._ids(materialized) == self._ids(streamed)


# ── Any.find_one() ───────────────────────────────────────


class TestAnyFindOne:
    def test_find_one_returns_matching_doc(self, coll: Any) -> None:
        doc = coll.find_one({"city": "NYC"})
        assert doc is not None
        assert doc["city"] == "NYC"

    def test_find_one_no_match_returns_none(self, coll: Any) -> None:
        doc = coll.find_one({"city": "ATLANTIS"})
        assert doc is None

    def test_find_one_by_id(self, coll: Any) -> None:
        all_docs = coll.find({})
        target = all_docs[0]
        doc = coll.find_one({"_id": target["_id"]})
        assert doc is not None
        assert doc["_id"] == target["_id"]

    def test_find_one_with_index(self, indexed_coll: Any) -> None:
        doc = indexed_coll.find_one({"age": {"$gt": 40}})
        assert doc is not None
        assert doc["age"] > 40

    def test_find_one_empty_collection(self, local_collection: Any) -> None:
        doc = local_collection.find_one({})
        assert doc is None

    def test_find_one_empty_query_returns_a_doc(self, coll: Any) -> None:
        doc = coll.find_one({})
        assert doc is not None
        assert "_id" in doc


# ── Any.count() ──────────────────────────────────────────


class TestAnyCount:
    def test_count_all(self, coll: Any) -> None:
        assert coll.count({}) == 10

    def test_count_filtered(self, coll: Any) -> None:
        count = coll.count({"city": "NYC"})
        expected = len(coll.find({"city": "NYC"}))
        assert count == expected

    def test_count_no_matches(self, coll: Any) -> None:
        assert coll.count({"city": "ATLANTIS"}) == 0

    def test_count_empty_collection(self, local_collection: Any) -> None:
        assert local_collection.count({}) == 0

    def test_count_with_index(self, indexed_coll: Any) -> None:
        count = indexed_coll.count({"age": {"$gt": 35}})
        expected = len([d for d in indexed_coll.find({}) if d["age"] > 35])
        assert count == expected

    def test_count_matches_len_find(self, indexed_coll: Any) -> None:
        for q in [
            {},
            {"city": "NYC"},
            {"age": {"$gte": 30}},
            {"city": {"$in": ["NYC", "SF"]}},
        ]:
            assert indexed_coll.count(q) == len(indexed_coll.find(q))


# ── Cursor with Iterable (lazy materialization) ─────────────────────


class TestCursorIterable:
    """Cursor should accept any Iterable, not just lists."""

    def test_cursor_accepts_generator(self) -> None:
        def gen():
            for i in range(5):
                yield {"_id": str(i), "x": i}

        c = Cursor(gen())
        result = c.to_list()
        assert len(result) == 5
        assert result[0]["x"] == 0

    def test_cursor_accepts_streaming_cursor(self, coll: Any) -> None:
        sc = coll.find_streaming({})
        c = Cursor(sc)
        result = c.to_list()
        assert len(result) == 10

    def test_cursor_sort_materializes(self) -> None:
        def gen():
            for i in [3, 1, 2]:
                yield {"_id": str(i), "x": i}

        c = Cursor(gen()).sort("x", 1)
        result = c.to_list()
        assert [d["x"] for d in result] == [1, 2, 3]

    def test_cursor_limit_without_sort(self) -> None:
        consumed = []

        def tracking_gen():
            for i in range(100):
                doc = {"_id": str(i), "x": i}
                consumed.append(doc)
                yield doc

        c = Cursor(tracking_gen()).limit(5)
        result = c.to_list()
        assert len(result) == 5
        assert len(consumed) == 5

    def test_cursor_skip_without_sort(self) -> None:
        def gen():
            for i in range(10):
                yield {"_id": str(i), "x": i}

        c = Cursor(gen()).skip(7)
        result = c.to_list()
        assert len(result) == 3
        assert result[0]["x"] == 7

    def test_cursor_skip_and_limit_without_sort(self) -> None:
        consumed = []

        def tracking_gen():
            for i in range(100):
                doc = {"_id": str(i), "x": i}
                consumed.append(doc)
                yield doc

        c = Cursor(tracking_gen()).skip(5).limit(3)
        result = c.to_list()
        assert len(result) == 3
        assert result[0]["x"] == 5
        assert result[2]["x"] == 7
        assert len(consumed) == 8

    def test_cursor_resolve_cached(self) -> None:
        c = Cursor([{"_id": "1", "x": 1}, {"_id": "2", "x": 2}])
        first = c.to_list()
        second = c.to_list()
        assert first == second
        assert first is second

    def test_cursor_len(self) -> None:
        def gen():
            for i in range(7):
                yield {"_id": str(i), "x": i}

        c = Cursor(gen())
        assert len(c) == 7

    def test_cursor_getitem(self) -> None:
        def gen():
            for i in range(5):
                yield {"_id": str(i), "x": i}

        c = Cursor(gen())
        assert c[2]["x"] == 2

    def test_cursor_iter_twice(self) -> None:
        c = Cursor([{"_id": "1", "x": 1}, {"_id": "2", "x": 2}])
        first = list(c)
        second = list(c)
        assert first == second

    def test_cursor_projection_on_iterable(self, coll: Any) -> None:
        sc = coll.find_streaming({})
        c = Cursor(sc).projection({"name": 1})
        result = c.to_list()
        assert len(result) == 10
        for d in result:
            assert "name" in d
            assert "_id" in d
            assert "age" not in d

    def test_cursor_list_input_not_double_wrapped(self) -> None:
        docs = [{"_id": "1", "x": 1}]
        c = Cursor(docs)
        assert c._materialized is docs


# ── Client-level streaming integration ───────────────────────────────


class TestClientStreaming:
    """Collection facade (client.py) uses streaming under the hood."""

    @pytest.fixture
    def client_coll(self, tmp_path, sample_docs) -> Collection:
        client = MongoClient(f"local://{tmp_path}/wt")
        coll = client["testdb"]["users"]
        coll.insert_many(sample_docs)
        coll.create_index([("city", 1)])
        coll.create_index([("age", 1)])
        return coll

    def test_find_returns_cursor(self, client_coll: Collection) -> None:
        result = client_coll.find({"city": "NYC"})
        assert isinstance(result, Cursor)

    def test_find_iterates_correctly(self, client_coll: Collection) -> None:
        docs = list(client_coll.find({"city": "NYC"}))
        assert all(d["city"] == "NYC" for d in docs)
        assert len(docs) >= 1

    def test_find_with_projection(self, client_coll: Collection) -> None:
        docs = list(client_coll.find({"city": "NYC"}, {"name": 1}))
        for d in docs:
            assert "name" in d
            assert "_id" in d
            assert "age" not in d

    def test_find_with_sort_limit_skip(self, client_coll: Collection) -> None:
        cursor = client_coll.find({}).sort("age", 1).skip(2).limit(3)
        docs = cursor.to_list()
        assert len(docs) == 3
        ages = [d["age"] for d in docs]
        assert ages == sorted(ages)

    def test_find_one_streaming(self, client_coll: Collection) -> None:
        doc = client_coll.find_one({"city": "NYC"})
        assert doc is not None
        assert doc["city"] == "NYC"

    def test_find_one_no_match(self, client_coll: Collection) -> None:
        doc = client_coll.find_one({"city": "ATLANTIS"})
        assert doc is None

    def test_count_documents_streaming(self, client_coll: Collection) -> None:
        count = client_coll.count_documents({"city": "NYC"})
        docs = list(client_coll.find({"city": "NYC"}))
        assert count == len(docs)

    def test_count_documents_empty_query(self, client_coll: Collection) -> None:
        count = client_coll.count_documents({})
        assert count == 10

    def test_count_documents_no_match(self, client_coll: Collection) -> None:
        assert client_coll.count_documents({"city": "ATLANTIS"}) == 0

    def test_aggregate_from_streaming(self, client_coll: Collection) -> None:
        results = client_coll.aggregate(
            [
                {"$group": {"_id": "$city", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]
        )
        assert len(results) > 0
        assert all("_id" in r and "count" in r for r in results)


# ── Edge cases ───────────────────────────────────────────────────────


class TestStreamingEdgeCases:
    def test_streaming_after_insert(self, coll: Any) -> None:
        coll.insert_one({"name": "NewPerson", "city": "MOON", "age": 99})
        results = list(coll.find_streaming({"city": "MOON"}))
        assert len(results) == 1
        assert results[0]["name"] == "NewPerson"

    def test_streaming_after_delete(self, coll: Any) -> None:
        before = coll.count({})
        doc = coll.find_one({"city": "NYC"})
        assert doc is not None
        coll.delete({"_id": doc["_id"]}, multi=False)
        after = coll.count({})
        assert after == before - 1

    def test_streaming_after_update(self, coll: Any) -> None:
        coll.update({"city": "NYC"}, {"$set": {"city": "CHANGED"}}, multi=True)
        assert coll.count({"city": "NYC"}) == 0
        assert coll.count({"city": "CHANGED"}) > 0

    def test_multiple_iterators_from_same_collection(self, coll: Any) -> None:
        results1 = list(coll.find_streaming({"city": "NYC"}))
        results2 = list(coll.find_streaming({"city": "NYC"}))
        assert len(results1) == len(results2)
        ids1 = {str(d["_id"]) for d in results1}
        ids2 = {str(d["_id"]) for d in results2}
        assert ids1 == ids2

    def test_find_one_consistency_with_find(self, indexed_coll: Any) -> None:
        for q in [
            {"city": "NYC"},
            {"age": {"$gt": 35}},
            {"dept": "eng"},
            {"city": "ATLANTIS"},
        ]:
            find_result = indexed_coll.find(q)
            find_one_result = indexed_coll.find_one(q)
            if find_result:
                assert find_one_result is not None
                assert find_one_result["_id"] in {d["_id"] for d in find_result}
            else:
                assert find_one_result is None
