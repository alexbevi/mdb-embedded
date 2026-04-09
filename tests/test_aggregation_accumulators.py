"""Coverage tests for advanced $group accumulators."""

from smongo.aggregation import Cursor


class TestMergeObjectsAccumulator:
    def test_merge_objects(self):
        """$mergeObjects merges dict values across group."""
        docs = [
            {"_id": 1, "dept": "eng", "meta": {"lang": "py"}},
            {"_id": 2, "dept": "eng", "meta": {"level": "senior"}},
            {"_id": 3, "dept": "eng", "meta": {"lang": "go"}},
        ]
        result = Cursor(docs).aggregate(
            [{"$group": {"_id": "$dept", "merged": {"$mergeObjects": "$meta"}}}]
        )
        assert len(result) == 1
        # Last value wins for duplicate keys
        assert result[0]["merged"]["lang"] == "go"
        assert result[0]["merged"]["level"] == "senior"


class TestTopBottomAccumulators:
    def test_top_accumulator(self):
        """$top returns highest value based on sort."""
        docs = [
            {"_id": 1, "dept": "eng", "salary": 100},
            {"_id": 2, "dept": "eng", "salary": 150},
            {"_id": 3, "dept": "eng", "salary": 120},
        ]
        result = Cursor(docs).aggregate(
            [
                {
                    "$group": {
                        "_id": "$dept",
                        "topSalary": {
                            "$top": {"sortBy": {"salary": -1}, "output": "$salary"}
                        },
                    }
                }
            ]
        )
        assert len(result) == 1
        assert result[0]["topSalary"] == 150

    def test_bottom_accumulator(self):
        """$bottom returns lowest value based on sort."""
        docs = [
            {"_id": 1, "dept": "eng", "salary": 100},
            {"_id": 2, "dept": "eng", "salary": 150},
            {"_id": 3, "dept": "eng", "salary": 120},
        ]
        result = Cursor(docs).aggregate(
            [
                {
                    "$group": {
                        "_id": "$dept",
                        "bottomSalary": {
                            "$bottom": {"sortBy": {"salary": -1}, "output": "$salary"}
                        },
                    }
                }
            ]
        )
        assert len(result) == 1
        assert result[0]["bottomSalary"] == 100

    def test_top_single_doc(self):
        """$top with single doc returns that value."""
        result = Cursor([{"_id": 1, "x": 10}]).aggregate(
            [{"$group": {"_id": None, "top": {"$top": {"output": "$x"}}}}]
        )
        assert result[0]["top"] == 10


class TestTopNBottomNAccumulators:
    def test_top_n_accumulator(self):
        """$topN returns top N values."""
        docs = [
            {"_id": 1, "dept": "eng", "score": 95},
            {"_id": 2, "dept": "eng", "score": 88},
            {"_id": 3, "dept": "eng", "score": 92},
            {"_id": 4, "dept": "eng", "score": 85},
        ]
        result = Cursor(docs).aggregate(
            [
                {
                    "$group": {
                        "_id": "$dept",
                        "topScores": {
                            "$topN": {"n": 2, "sortBy": {"score": -1}, "output": "$score"}
                        },
                    }
                }
            ]
        )
        assert len(result) == 1
        assert result[0]["topScores"] == [95, 92]

    def test_bottom_n_accumulator(self):
        """$bottomN returns bottom N values."""
        docs = [
            {"_id": 1, "dept": "eng", "score": 95},
            {"_id": 2, "dept": "eng", "score": 88},
            {"_id": 3, "dept": "eng", "score": 92},
            {"_id": 4, "dept": "eng", "score": 85},
        ]
        result = Cursor(docs).aggregate(
            [
                {
                    "$group": {
                        "_id": "$dept",
                        "bottomScores": {
                            "$bottomN": {"n": 2, "sortBy": {"score": -1}, "output": "$score"}
                        },
                    }
                }
            ]
        )
        assert len(result) == 1
        assert result[0]["bottomScores"] == [85, 88]


class TestFirstNLastNAccumulators:
    def test_first_n_accumulator(self):
        """$firstN returns first N values."""
        docs = [
            {"_id": 1, "dept": "eng", "score": 95},
            {"_id": 2, "dept": "eng", "score": 88},
            {"_id": 3, "dept": "eng", "score": 92},
            {"_id": 4, "dept": "eng", "score": 85},
        ]
        result = Cursor(docs).aggregate(
            [{"$group": {"_id": "$dept", "firstTwo": {"$firstN": {"n": 2, "input": "$score"}}}}]
        )
        assert len(result) == 1
        assert result[0]["firstTwo"] == [95, 88]

    def test_last_n_accumulator(self):
        """$lastN returns last N values."""
        docs = [
            {"_id": 1, "dept": "eng", "score": 95},
            {"_id": 2, "dept": "eng", "score": 88},
            {"_id": 3, "dept": "eng", "score": 92},
            {"_id": 4, "dept": "eng", "score": 85},
        ]
        result = Cursor(docs).aggregate(
            [{"$group": {"_id": "$dept", "lastTwo": {"$lastN": {"n": 2, "input": "$score"}}}}]
        )
        assert len(result) == 1
        assert result[0]["lastTwo"] == [92, 85]


class TestAccumulatorEdgeCases:
    def test_min_with_nulls(self):
        """$min ignores null values."""
        docs = [
            {"_id": 1, "value": 10},
            {"_id": 2, "value": None},
            {"_id": 3, "value": 5},
        ]
        result = Cursor(docs).aggregate([{"$group": {"_id": None, "min": {"$min": "$value"}}}])
        assert result[0]["min"] == 5

    def test_max_with_nulls(self):
        """$max ignores null values."""
        docs = [
            {"_id": 1, "value": 10},
            {"_id": 2, "value": None},
            {"_id": 3, "value": 5},
        ]
        result = Cursor(docs).aggregate([{"$group": {"_id": None, "max": {"$max": "$value"}}}])
        assert result[0]["max"] == 10

    def test_avg_with_nulls(self):
        """$avg ignores null values."""
        docs = [
            {"_id": 1, "value": 10},
            {"_id": 2, "value": None},
            {"_id": 3, "value": 20},
        ]
        result = Cursor(docs).aggregate([{"$group": {"_id": None, "avg": {"$avg": "$value"}}}])
        assert result[0]["avg"] == 15

    def test_std_dev_pop_empty(self):
        """$stdDevPop with no values returns None."""
        result = Cursor([{"_id": 1}]).aggregate(
            [{"$group": {"_id": None, "std": {"$stdDevPop": "$missing"}}}]
        )
        assert result[0]["std"] is None

    def test_std_dev_samp_single_value(self):
        """$stdDevSamp with < 2 values returns None."""
        result = Cursor([{"_id": 1, "value": 10}]).aggregate(
            [{"$group": {"_id": None, "std": {"$stdDevSamp": "$value"}}}]
        )
        assert result[0]["std"] is None

    def test_first_single_doc(self):
        """$first with single doc."""
        result = Cursor([{"_id": 1, "x": 10}]).aggregate(
            [{"$group": {"_id": None, "first": {"$first": "$x"}}}]
        )
        assert result[0]["first"] == 10

    def test_last_single_doc(self):
        """$last with single doc."""
        result = Cursor([{"_id": 1, "x": 10}]).aggregate(
            [{"$group": {"_id": None, "last": {"$last": "$x"}}}]
        )
        assert result[0]["last"] == 10

    def test_min_max_empty(self):
        """$min/$max with no valid values returns None."""
        result = Cursor([{"_id": 1}]).aggregate(
            [{"$group": {"_id": None, "min": {"$min": "$missing"}, "max": {"$max": "$missing"}}}]
        )
        assert result[0]["min"] is None
        assert result[0]["max"] is None

    def test_avg_empty(self):
        """$avg with no valid values returns 0."""
        result = Cursor([{"_id": 1}]).aggregate(
            [{"$group": {"_id": None, "avg": {"$avg": "$missing"}}}]
        )
        assert result[0]["avg"] == 0
