"""Tests for aggregation scalability guardrails."""

import pytest

from smongo.aggregation import (
    _OUT_BATCH_SIZE,
    DEFAULT_MAX_PIPELINE_DOCS,
    MAX_PIPELINE_STAGES,
    Cursor,
    DocumentLimitExceeded,
    _optimize_pipeline,
    out_stage,
)


@pytest.fixture
def docs():
    return [
        {"_id": str(i), "name": f"doc_{i}", "age": 20 + i, "city": "NYC" if i % 2 else "SF"}
        for i in range(50)
    ]


class TestDocumentLimitExceeded:
    def test_exceeds_limit(self, docs):
        with pytest.raises(DocumentLimitExceeded, match="exceeding the limit"):
            Cursor(docs).aggregate(
                [{"$match": {}}],
                max_pipeline_docs=10,
            )

    def test_within_limit(self, docs):
        result = Cursor(docs).aggregate(
            [{"$match": {"city": "NYC"}}],
            max_pipeline_docs=100,
        )
        assert all(d["city"] == "NYC" for d in result)

    def test_default_limit_is_large(self):
        assert DEFAULT_MAX_PIPELINE_DOCS == 100_000

    def test_limit_zero_raises_for_any_docs(self):
        with pytest.raises(DocumentLimitExceeded):
            Cursor([{"_id": "1"}]).aggregate([{"$match": {}}], max_pipeline_docs=0)


class TestMaxPipelineStages:
    def test_too_many_stages_raises(self, docs):
        pipeline = [{"$match": {}}] * (MAX_PIPELINE_STAGES + 1)
        with pytest.raises(ValueError, match="exceeding the limit"):
            Cursor(docs).aggregate(pipeline)

    def test_exactly_at_limit(self, docs):
        pipeline = [{"$match": {}}] * MAX_PIPELINE_STAGES
        result = Cursor(docs).aggregate(pipeline)
        assert len(result) == len(docs)


class TestOptimizePipeline:
    def test_merge_consecutive_matches(self):
        pipeline = [
            {"$match": {"a": 1}},
            {"$match": {"b": 2}},
        ]
        result = _optimize_pipeline(pipeline)
        assert len(result) == 1
        assert "$and" in result[0]["$match"]

    def test_swap_limit_before_match(self):
        pipeline = [
            {"$limit": 5},
            {"$match": {"a": 1}},
        ]
        result = _optimize_pipeline(pipeline)
        assert next(iter(result[0])) == "$match"
        assert next(iter(result[1])) == "$limit"

    def test_no_change_match_then_limit(self):
        pipeline = [
            {"$match": {"a": 1}},
            {"$limit": 5},
        ]
        result = _optimize_pipeline(pipeline)
        assert len(result) == 2
        assert next(iter(result[0])) == "$match"
        assert next(iter(result[1])) == "$limit"

    def test_single_stage_passthrough(self):
        pipeline = [{"$sort": {"a": 1}}]
        assert _optimize_pipeline(pipeline) == pipeline

    def test_empty_pipeline_passthrough(self):
        assert _optimize_pipeline([]) == []


class TestBatchedOut:
    def test_batch_size_constant(self):
        assert _OUT_BATCH_SIZE == 1_000

    def test_out_stage_writes_all_docs(self, populated_collection, local_db):
        target = local_db.collection("out_target")
        docs = populated_collection.get_all()
        getter = lambda name: local_db.collection(name)
        out_stage(docs, "out_target", collection_getter=getter)
        assert len(target.get_all()) == len(docs)


class TestFacetLimitPropagation:
    def test_facet_respects_limit(self, docs):
        with pytest.raises(DocumentLimitExceeded):
            Cursor(docs).aggregate(
                [{"$facet": {"branch": [{"$match": {}}]}}],
                max_pipeline_docs=5,
            )


class TestLookupOptimization:
    def test_lookup_uses_indexed_path(self, local_db):
        main = local_db.collection("orders")
        foreign = local_db.collection("products")
        foreign.create_index("sku")
        foreign.insert_many(
            [
                {"sku": "A", "price": 10},
                {"sku": "B", "price": 20},
            ]
        )
        main.insert_many(
            [
                {"_id": "o1", "product_sku": "A"},
                {"_id": "o2", "product_sku": "B"},
                {"_id": "o3", "product_sku": "C"},
            ]
        )
        getter = lambda name: local_db.collection(name)
        result = Cursor(main.get_all(), collection_getter=getter).aggregate(
            [
                {
                    "$lookup": {
                        "from": "products",
                        "localField": "product_sku",
                        "foreignField": "sku",
                        "as": "product",
                    }
                }
            ]
        )
        found_a = [d for d in result if d["_id"] == "o1"]
        assert len(found_a) == 1
        assert len(found_a[0]["product"]) == 1
        assert found_a[0]["product"][0]["price"] == 10

        found_c = [d for d in result if d["_id"] == "o3"]
        assert found_c[0]["product"] == []

    def test_lookup_hash_fallback(self, local_db):
        main = local_db.collection("main_fb")
        foreign = local_db.collection("foreign_fb")
        foreign.insert_many([{"k": "x", "v": 1}])
        main.insert_many([{"_id": "1", "fk": "x"}])
        getter = lambda name: local_db.collection(name)
        result = Cursor(main.get_all(), collection_getter=getter).aggregate(
            [
                {
                    "$lookup": {
                        "from": "foreign_fb",
                        "localField": "fk",
                        "foreignField": "k",
                        "as": "joined",
                    }
                }
            ]
        )
        assert len(result[0]["joined"]) == 1


class TestAllowDiskUse:
    def test_accepts_flag(self, docs):
        result = Cursor(docs).aggregate([{"$match": {}}], allowDiskUse=True)
        assert len(result) == len(docs)
