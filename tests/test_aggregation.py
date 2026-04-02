"""Tests for smongo.aggregation -- Cursor and pipeline stages."""


import pytest

from smongo.aggregation import (
    Cursor,
    _apply_projection,
    sort_stage,
    unwind_stage,
)


@pytest.fixture
def docs():
    return [
        {"_id": "1", "name": "Alice", "age": 34, "city": "NYC", "dept": "eng", "tags": ["py", "go"], "salary": 145000},
        {"_id": "2", "name": "Bob", "age": 28, "city": "SF", "dept": "eng", "tags": ["js"], "salary": 128000},
        {"_id": "3", "name": "Charlie", "age": 40, "city": "NYC", "dept": "mgmt", "tags": ["py"], "salary": 175000},
        {"_id": "4", "name": "Diana", "age": 25, "city": "LA", "dept": "design", "tags": [], "salary": 98000},
        {"_id": "5", "name": "Eve", "age": 31, "city": "SF", "dept": "eng", "tags": ["py", "ml"], "salary": 155000},
    ]


# ── Cursor ───────────────────────────────────────────────────────────


class TestCursor:
    def test_iter(self, docs):
        c = Cursor(docs)
        assert list(c) == docs

    def test_len(self, docs):
        assert len(Cursor(docs)) == 5

    def test_to_list(self, docs):
        assert Cursor(docs).to_list() == docs

    def test_getitem_index(self, docs):
        assert Cursor(docs)[0] == docs[0]

    def test_getitem_slice(self, docs):
        assert Cursor(docs)[1:3] == docs[1:3]

    def test_count(self, docs):
        assert Cursor(docs).count() == 5

    def test_find(self, docs):
        c = Cursor(docs).find({"city": "NYC"})
        result = c.to_list()
        assert all(d["city"] == "NYC" for d in result)
        assert len(result) == 2


class TestCursorSort:
    def test_sort_ascending(self, docs):
        result = Cursor(docs).sort("age", 1).to_list()
        ages = [d["age"] for d in result]
        assert ages == sorted(ages)

    def test_sort_descending(self, docs):
        result = Cursor(docs).sort("age", -1).to_list()
        ages = [d["age"] for d in result]
        assert ages == sorted(ages, reverse=True)

    def test_sort_multi_key(self, docs):
        result = Cursor(docs).sort([("city", 1), ("age", -1)]).to_list()
        for i in range(len(result) - 1):
            if result[i]["city"] == result[i + 1]["city"]:
                assert result[i]["age"] >= result[i + 1]["age"]

    def test_sort_dict(self, docs):
        result = Cursor(docs).sort({"age": 1}).to_list()
        ages = [d["age"] for d in result]
        assert ages == sorted(ages)


class TestCursorSkipLimit:
    def test_skip(self, docs):
        result = Cursor(docs).skip(2).to_list()
        assert len(result) == 3

    def test_skip_zero_returns_all(self, docs):
        result = Cursor(docs).skip(0).to_list()
        assert len(result) == 5

    def test_limit(self, docs):
        result = Cursor(docs).limit(2).to_list()
        assert len(result) == 2

    def test_skip_limit_chain(self, docs):
        result = Cursor(docs).sort("age", 1).skip(1).limit(2).to_list()
        assert len(result) == 2


class TestCursorProjection:
    def test_inclusion(self, docs):
        result = Cursor(docs).projection({"name": 1}).to_list()
        for d in result:
            assert "name" in d
            assert "_id" in d  # default inclusion
            assert "age" not in d

    def test_exclusion(self, docs):
        result = Cursor(docs).projection({"tags": 0}).to_list()
        for d in result:
            assert "tags" not in d
            assert "name" in d


# ── $match ───────────────────────────────────────────────────────────


class TestAggregateMatch:
    def test_match(self, docs):
        result = Cursor(docs).aggregate([{"$match": {"city": "SF"}}])
        assert all(d["city"] == "SF" for d in result)
        assert len(result) == 2


# ── $group ───────────────────────────────────────────────────────────


class TestAggregateGroup:
    def test_group_count(self, docs):
        result = Cursor(docs).aggregate([
            {"$group": {"_id": "$city", "count": {"$sum": 1}}},
        ])
        nyc = next(r for r in result if r["_id"] == "NYC")
        assert nyc["count"] == 2

    def test_group_sum_field(self, docs):
        result = Cursor(docs).aggregate([
            {"$group": {"_id": "$dept", "total_salary": {"$sum": "$salary"}}},
        ])
        eng = next(r for r in result if r["_id"] == "eng")
        assert eng["total_salary"] == 145000 + 128000 + 155000

    def test_group_avg(self, docs):
        result = Cursor(docs).aggregate([
            {"$group": {"_id": None, "avg_age": {"$avg": "$age"}}},
        ])
        assert result[0]["avg_age"] == pytest.approx((34 + 28 + 40 + 25 + 31) / 5)

    def test_group_min_max(self, docs):
        result = Cursor(docs).aggregate([
            {"$group": {"_id": None, "min_age": {"$min": "$age"}, "max_age": {"$max": "$age"}}},
        ])
        assert result[0]["min_age"] == 25
        assert result[0]["max_age"] == 40

    def test_group_push(self, docs):
        result = Cursor(docs).aggregate([
            {"$group": {"_id": "$city", "names": {"$push": "$name"}}},
        ])
        nyc = next(r for r in result if r["_id"] == "NYC")
        assert set(nyc["names"]) == {"Alice", "Charlie"}

    def test_group_add_to_set(self, docs):
        result = Cursor(docs).aggregate([
            {"$group": {"_id": None, "cities": {"$addToSet": "$city"}}},
        ])
        assert set(result[0]["cities"]) == {"NYC", "SF", "LA"}

    def test_group_first_last(self, docs):
        result = Cursor(docs).aggregate([
            {"$sort": {"age": 1}},
            {"$group": {"_id": None, "youngest": {"$first": "$name"}, "oldest": {"$last": "$name"}}},
        ])
        assert result[0]["youngest"] == "Diana"
        assert result[0]["oldest"] == "Charlie"


# ── $project ─────────────────────────────────────────────────────────


class TestAggregateProject:
    def test_inclusion(self, docs):
        result = Cursor(docs).aggregate([{"$project": {"name": 1, "age": 1}}])
        for d in result:
            assert "name" in d
            assert "age" in d
            assert "salary" not in d

    def test_computed_field(self, docs):
        result = Cursor(docs).aggregate([
            {"$project": {"upper_name": {"$toUpper": "$name"}}},
        ])
        assert result[0]["upper_name"] == "ALICE"


# ── $sort ────────────────────────────────────────────────────────────


class TestAggregateSort:
    def test_sort_ascending(self, docs):
        result = Cursor(docs).aggregate([{"$sort": {"age": 1}}])
        ages = [d["age"] for d in result]
        assert ages == sorted(ages)

    def test_sort_descending(self, docs):
        result = Cursor(docs).aggregate([{"$sort": {"age": -1}}])
        ages = [d["age"] for d in result]
        assert ages == sorted(ages, reverse=True)

    def test_sort_none_last(self):
        data = [{"x": 1}, {"x": None}, {"x": 3}, {}]
        result = sort_stage(data, {"x": 1})
        # Current implementation places None/missing values first for ascending.
        assert result[0].get("x") is None or "x" not in result[0]


# ── $limit / $skip ───────────────────────────────────────────────────


class TestAggregateLimitSkip:
    def test_limit(self, docs):
        result = Cursor(docs).aggregate([{"$limit": 2}])
        assert len(result) == 2

    def test_skip(self, docs):
        result = Cursor(docs).aggregate([{"$skip": 3}])
        assert len(result) == 2


# ── $unwind ──────────────────────────────────────────────────────────


class TestAggregateUnwind:
    def test_unwind_string_path(self, docs):
        result = Cursor(docs).aggregate([{"$unwind": "$tags"}])
        assert all(not isinstance(d["tags"], list) for d in result)
        tag_count = sum(len(d.get("tags", []) or []) if isinstance(d.get("tags"), list) else 1 for d in docs)
        # Diana has empty tags, so she's excluded
        expected = sum(len(d["tags"]) for d in docs if d["tags"])
        assert len(result) == expected

    def test_unwind_dict_preserve_null(self, docs):
        result = Cursor(docs).aggregate([
            {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True}},
        ])
        # Diana (empty tags) and others should be preserved
        assert len(result) >= len(docs)

    def test_unwind_non_list_kept(self):
        data = [{"x": "scalar"}]
        result = unwind_stage(data, "$x")
        assert len(result) == 1

    def test_unwind_missing_field_dropped(self):
        data = [{"y": 1}]
        result = unwind_stage(data, "$x")
        assert len(result) == 0

    def test_unwind_missing_preserve(self):
        data = [{"y": 1}]
        result = unwind_stage(data, {"path": "$x", "preserveNullAndEmptyArrays": True})
        assert len(result) == 1


# ── $addFields / $set ────────────────────────────────────────────────


class TestAggregateAddFields:
    def test_add_fields(self, docs):
        result = Cursor(docs).aggregate([
            {"$addFields": {"age_plus_ten": {"$add": ["$age", 10]}}},
        ])
        assert result[0]["age_plus_ten"] == 44

    def test_set_alias(self, docs):
        result = Cursor(docs).aggregate([
            {"$set": {"label": "constant"}},
        ])
        assert all(d["label"] == "constant" for d in result)


# ── $count ───────────────────────────────────────────────────────────


class TestAggregateCount:
    def test_count(self, docs):
        result = Cursor(docs).aggregate([{"$count": "total"}])
        assert result == [{"total": 5}]


# ── $replaceRoot ─────────────────────────────────────────────────────


class TestAggregateReplaceRoot:
    def test_replace_root(self):
        data = [{"_id": 1, "sub": {"a": 10, "b": 20}}]
        result = Cursor(data).aggregate([{"$replaceRoot": {"newRoot": "$sub"}}])
        assert result == [{"a": 10, "b": 20}]

    def test_replace_root_non_dict_raises(self):
        data = [{"_id": 1, "sub": "not_a_dict"}]
        with pytest.raises(TypeError, match="newRoot.*evaluate to an object"):
            Cursor(data).aggregate([{"$replaceRoot": {"newRoot": "$sub"}}])


# ── $lookup ──────────────────────────────────────────────────────────


class TestAggregateLookup:
    def test_lookup_basic(self):
        orders = [{"_id": 1, "product": "A"}, {"_id": 2, "product": "B"}]
        products = [{"_id": "A", "price": 10}, {"_id": "B", "price": 20}]

        def getter(name):
            class FakeColl:
                def get_all(self):
                    return products
            return FakeColl()

        result = Cursor(orders, collection_getter=getter).aggregate([
            {"$lookup": {"from": "products", "localField": "product", "foreignField": "_id", "as": "details"}},
        ])
        assert len(result[0]["details"]) == 1
        assert result[0]["details"][0]["price"] == 10

    def test_lookup_no_match(self):
        orders = [{"_id": 1, "product": "Z"}]
        products = [{"_id": "A", "price": 10}]

        def getter(name):
            class FakeColl:
                def get_all(self):
                    return products
            return FakeColl()

        result = Cursor(orders, collection_getter=getter).aggregate([
            {"$lookup": {"from": "products", "localField": "product", "foreignField": "_id", "as": "details"}},
        ])
        assert result[0]["details"] == []

    def test_lookup_no_getter(self):
        orders = [{"_id": 1, "product": "A"}]
        result = Cursor(orders).aggregate([
            {"$lookup": {"from": "x", "localField": "product", "foreignField": "_id", "as": "details"}},
        ])
        assert result[0]["details"] == []

    def test_lookup_missing_spec_fields_raises(self):
        orders = [{"_id": 1}]
        with pytest.raises(ValueError, match=r"\$lookup missing required fields"):
            Cursor(orders).aggregate([{"$lookup": {"from": "x"}}])


# ── $sample ──────────────────────────────────────────────────────────


class TestAggregateSample:
    def test_sample_size(self, docs):
        result = Cursor(docs).aggregate([{"$sample": {"size": 2}}])
        assert len(result) == 2

    def test_sample_larger_than_docs(self, docs):
        result = Cursor(docs).aggregate([{"$sample": {"size": 100}}])
        assert len(result) == 5


class TestAggregateVectorSearch:
    def test_vector_search_basic_cosine(self):
        data = [
            {"_id": "a", "embedding": [1.0, 0.0, 0.0], "label": "x"},
            {"_id": "b", "embedding": [0.9, 0.1, 0.0], "label": "x"},
            {"_id": "c", "embedding": [0.0, 1.0, 0.0], "label": "y"},
        ]
        result = Cursor(data).aggregate(
            [
                {
                    "$vectorSearch": {
                        "path": "embedding",
                        "queryVector": [1.0, 0.0, 0.0],
                        "limit": 2,
                        "metric": "cosine",
                    }
                }
            ]
        )
        assert len(result) == 2
        assert result[0]["_id"] == "a"
        assert "_vectorScore" in result[0]

    def test_vector_search_with_filter(self):
        data = [
            {"_id": "a", "embedding": [1.0, 0.0], "label": "x"},
            {"_id": "b", "embedding": [0.9, 0.1], "label": "y"},
        ]
        result = Cursor(data).aggregate(
            [
                {
                    "$vectorSearch": {
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "filter": {"label": "y"},
                        "limit": 3,
                    }
                }
            ]
        )
        assert len(result) == 1
        assert result[0]["_id"] == "b"

    def test_vector_search_euclidean(self):
        data = [
            {"_id": "a", "embedding": [0.0, 0.0]},
            {"_id": "b", "embedding": [1.0, 1.0]},
            {"_id": "c", "embedding": [5.0, 5.0]},
        ]
        result = Cursor(data).aggregate(
            [
                {
                    "$vectorSearch": {
                        "path": "embedding",
                        "queryVector": [1.0, 1.0],
                        "limit": 2,
                        "metric": "euclidean",
                        "scoreField": "score",
                    }
                }
            ]
        )
        assert [d["_id"] for d in result] == ["b", "a"]
        assert "score" in result[0]

    def test_vector_search_skips_invalid_vectors(self):
        data = [
            {"_id": "a", "embedding": [1.0, 0.0]},
            {"_id": "b", "embedding": [1.0]},  # wrong dim
            {"_id": "c", "embedding": "oops"},  # wrong type
        ]
        result = Cursor(data).aggregate(
            [{"$vectorSearch": {"path": "embedding", "queryVector": [1.0, 0.0], "limit": 10}}]
        )
        assert len(result) == 1
        assert result[0]["_id"] == "a"

    def test_vector_search_bad_spec_raises(self):
        with pytest.raises(ValueError, match="requires non-empty"):
            Cursor([{"embedding": [1.0]}]).aggregate([{"$vectorSearch": {"path": "embedding"}}])

    def test_vector_search_unsupported_metric_raises(self):
        with pytest.raises(ValueError, match="Unsupported vector metric"):
            Cursor([{"embedding": [1.0]}]).aggregate(
                [{"$vectorSearch": {"path": "embedding", "queryVector": [1.0], "metric": "manhattan"}}]
            )


# ── Unsupported stage ────────────────────────────────────────────────


# ── $facet ────────────────────────────────────────────────────────────


class TestAggregateFacet:
    def test_facet_two_pipelines(self, docs):
        result = Cursor(docs).aggregate([
            {"$facet": {
                "by_city": [{"$group": {"_id": "$city", "count": {"$sum": 1}}}],
                "total": [{"$count": "n"}],
            }},
        ])
        assert len(result) == 1
        assert "by_city" in result[0]
        assert "total" in result[0]
        assert result[0]["total"] == [{"n": 5}]

    def test_facet_preserves_input(self, docs):
        result = Cursor(docs).aggregate([
            {"$facet": {
                "all": [{"$match": {}}],
                "engineers": [{"$match": {"dept": "eng"}}],
            }},
        ])
        assert len(result[0]["all"]) == 5
        assert len(result[0]["engineers"]) == 3


# ── $out / $merge ────────────────────────────────────────────────────


class TestAggregateOut:
    def test_out_replaces_collection(self):
        class FakeColl:
            def __init__(self):
                self.data = [{"_id": "old", "x": 1}]
            def delete(self, q, multi=True):
                self.data.clear()
            def insert_many(self, docs):
                self.data.extend(docs)
            def get_all(self):
                return list(self.data)

        target = FakeColl()

        def getter(name):
            return target

        data = [{"_id": "1", "v": 10}, {"_id": "2", "v": 20}]
        result = Cursor(data, collection_getter=getter).aggregate([
            {"$out": "target_coll"},
        ])
        assert len(result) == 2
        assert len(target.data) == 2
        assert target.data[0]["_id"] == "1"


class TestAggregateMerge:
    def test_merge_upsert_into_collection(self):
        class FakeColl:
            def __init__(self):
                self.data = [{"_id": "1", "v": "old"}]
            def find(self, q):
                return [d for d in self.data if all(d.get(k) == v for k, v in q.items())]
            def update(self, q, update_spec, multi=False):
                for d in self.data:
                    if d["_id"] == q["_id"]:
                        d.update(update_spec.get("$set", {}))
            def insert_one(self, doc):
                self.data.append(doc)
            def get_all(self):
                return list(self.data)

        target = FakeColl()

        def getter(name):
            return target

        data = [{"_id": "1", "v": "new"}, {"_id": "2", "v": "inserted"}]
        result = Cursor(data, collection_getter=getter).aggregate([
            {"$merge": {"into": "target_coll", "on": "_id", "whenMatched": "replace", "whenNotMatched": "insert"}},
        ])
        assert len(result) == 2
        assert len(target.data) == 2
        match_1 = next(d for d in target.data if d["_id"] == "1")
        assert match_1["v"] == "new"
        match_2 = next(d for d in target.data if d["_id"] == "2")
        assert match_2["v"] == "inserted"


class TestAggregateUnsupported:
    def test_unsupported_raises(self, docs):
        with pytest.raises(NotImplementedError, match="not supported"):
            Cursor(docs).aggregate([{"$unknownStage": {}}])


# ── Multi-stage pipeline ─────────────────────────────────────────────


class TestMultiStagePipeline:
    def test_match_group_sort(self, docs):
        result = Cursor(docs).aggregate([
            {"$match": {"dept": "eng"}},
            {"$group": {"_id": "$city", "avg_salary": {"$avg": "$salary"}}},
            {"$sort": {"avg_salary": -1}},
        ])
        assert len(result) >= 1
        salaries = [r["avg_salary"] for r in result]
        assert salaries == sorted(salaries, reverse=True)

    def test_unwind_group_count(self, docs):
        result = Cursor(docs).aggregate([
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ])
        py = next(r for r in result if r["_id"] == "py")
        assert py["count"] == 3


# ── _apply_projection ────────────────────────────────────────────────


class TestApplyProjection:
    def test_inclusion_with_id(self):
        docs = [{"_id": 1, "name": "A", "age": 30}]
        result = _apply_projection(docs, {"name": 1})
        assert result[0] == {"_id": 1, "name": "A"}

    def test_exclusion(self):
        docs = [{"_id": 1, "name": "A", "age": 30}]
        result = _apply_projection(docs, {"age": 0})
        assert "age" not in result[0]
        assert result[0]["name"] == "A"

    def test_no_inclusion_or_exclusion(self):
        docs = [{"_id": 1, "name": "A"}]
        result = _apply_projection(docs, {})
        assert result[0] == {"_id": 1, "name": "A"}

    def test_inclusion_preserves_null(self):
        docs = [{"_id": 1, "name": None, "age": 30}]
        result = _apply_projection(docs, {"name": 1})
        assert result[0] == {"_id": 1, "name": None}

    def test_list_projection_preserves_null(self):
        docs = [{"_id": 1, "name": None, "age": 30}]
        result = _apply_projection(docs, ["name"])
        assert result[0] == {"_id": 1, "name": None}

    def test_inclusion_omits_missing_field(self):
        docs = [{"_id": 1, "age": 30}]
        result = _apply_projection(docs, {"name": 1})
        assert "name" not in result[0]
        assert result[0] == {"_id": 1}
