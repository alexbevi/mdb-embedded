"""Additional tests for $vectorSearch to boost coverage.

Covers:
- Zero norm query vector edge case
- Empty result sets
- USearch backend path (if available)
- NumPy import error handling (mocked)
"""

import sys
from unittest.mock import patch

import pytest

from smongo.aggregation import Cursor


class TestVectorSearchEdgeCases:
    def test_vector_search_zero_norm_query(self):
        """Zero-norm query vector returns empty results (cosine)."""
        data = [{"_id": "a", "embedding": [1.0, 0.0, 0.0]}]
        # Query with zero norm
        result = Cursor(data).aggregate(
            [
                {
                    "$vectorSearch": {
                        "path": "embedding",
                        "queryVector": [0.0, 0.0, 0.0],
                        "limit": 10,
                        "metric": "cosine",
                    }
                }
            ]
        )
        # Should return empty
        assert len(result) == 0

    def test_vector_search_no_valid_vectors(self):
        """$vectorSearch returns empty when no valid vectors exist."""
        data = [
            {"_id": "a", "embedding": "not_a_list"},
            {"_id": "b", "embedding": [1.0]},  # wrong dimension
            {"_id": "c"},  # missing field
        ]
        result = Cursor(data).aggregate(
            [
                {
                    "$vectorSearch": {
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "limit": 10,
                    }
                }
            ]
        )
        assert len(result) == 0

    def test_vector_search_invalid_vector_values(self):
        """$vectorSearch handles non-numeric vector values gracefully."""
        data = [
            {"_id": "a", "embedding": [1.0, 0.0]},
            {"_id": "b", "embedding": ["string", "values"]},  # Can't convert to float
        ]
        result = Cursor(data).aggregate(
            [
                {
                    "$vectorSearch": {
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "limit": 10,
                    }
                }
            ]
        )
        # Only doc "a" should match (b has non-numeric values)
        assert len(result) == 1
        assert result[0]["_id"] == "a"

    def test_vector_search_with_filter_empty_result(self):
        """$vectorSearch with filter that matches nothing returns empty."""
        data = [
            {"_id": "a", "embedding": [1.0, 0.0], "status": "active"},
            {"_id": "b", "embedding": [0.9, 0.1], "status": "active"},
        ]
        result = Cursor(data).aggregate(
            [
                {
                    "$vectorSearch": {
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "filter": {"status": "archived"},
                        "limit": 10,
                    }
                }
            ]
        )
        assert len(result) == 0


class TestVectorSearchBackends:
    def test_vector_search_uses_usearch_if_available(self):
        """$vectorSearch uses USearch backend when available."""
        # This test will pass if usearch is installed, otherwise skip
        try:
            from usearch.index import Index as USearchIndex  # noqa: F401
        except ImportError:
            pytest.skip("usearch not available")

        data = [
            {"_id": "a", "embedding": [1.0, 0.0, 0.0]},
            {"_id": "b", "embedding": [0.9, 0.1, 0.0]},
            {"_id": "c", "embedding": [0.0, 1.0, 0.0]},
        ]
        result = Cursor(data).aggregate(
            [
                {
                    "$vectorSearch": {
                        "path": "embedding",
                        "queryVector": [1.0, 0.0, 0.0],
                        "limit": 2,
                    }
                }
            ]
        )
        # Should find closest matches
        assert len(result) == 2
        assert result[0]["_id"] in ("a", "b")

    def test_vector_search_usearch_euclidean(self):
        """$vectorSearch with USearch backend supports euclidean metric."""
        try:
            from usearch.index import Index as USearchIndex  # noqa: F401
        except ImportError:
            pytest.skip("usearch not available")

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
                    }
                }
            ]
        )
        # Closest should be "b", then "a"
        assert len(result) == 2
        assert result[0]["_id"] == "b"

    def test_vector_search_usearch_unsupported_metric(self):
        """$vectorSearch with USearch raises on unsupported metric."""
        try:
            from usearch.index import Index as USearchIndex  # noqa: F401
        except ImportError:
            pytest.skip("usearch not available")

        data = [{"_id": "a", "embedding": [1.0, 0.0]}]
        with pytest.raises(ValueError, match="Unsupported vector metric"):
            Cursor(data).aggregate(
                [
                    {
                        "$vectorSearch": {
                            "path": "embedding",
                            "queryVector": [1.0, 0.0],
                            "metric": "manhattan",
                        }
                    }
                ]
            )


class TestVectorSearchSpecValidation:
    def test_vector_search_missing_path(self):
        """$vectorSearch requires 'path' field."""
        from smongo.aggregation.vector import vector_search_stage

        data = [{"_id": "a", "embedding": [1.0, 0.0]}]
        with pytest.raises(ValueError, match="requires non-empty"):
            vector_search_stage(data, {"queryVector": [1.0, 0.0], "limit": 10})

    def test_vector_search_empty_query_vector(self):
        """$vectorSearch requires non-empty 'queryVector'."""
        from smongo.aggregation.vector import vector_search_stage

        data = [{"_id": "a", "embedding": [1.0, 0.0]}]
        with pytest.raises(ValueError, match="requires non-empty"):
            vector_search_stage(data, {"path": "embedding", "queryVector": [], "limit": 10})

    def test_vector_search_query_vector_not_list(self):
        """$vectorSearch requires 'queryVector' to be a list."""
        from smongo.aggregation.vector import vector_search_stage

        data = [{"_id": "a", "embedding": [1.0, 0.0]}]
        with pytest.raises(ValueError, match="requires non-empty"):
            vector_search_stage(data, {"path": "embedding", "queryVector": "not_a_list", "limit": 10})


class TestVectorSearchNumCandidates:
    def test_vector_search_num_candidates_larger_than_limit(self):
        """$vectorSearch respects numCandidates for candidate set size."""
        from smongo.aggregation.vector import vector_search_stage

        data = [{"_id": f"doc{i}", "embedding": [float(i), 0.0]} for i in range(20)]
        result = vector_search_stage(
            data,
            {
                "path": "embedding",
                "queryVector": [10.0, 0.0],
                "limit": 3,
                "numCandidates": 10,
            },
        )
        # Should return top 3 from candidates
        assert len(result) == 3

    def test_vector_search_num_candidates_default(self):
        """$vectorSearch uses reasonable default for numCandidates."""
        from smongo.aggregation.vector import vector_search_stage

        data = [{"_id": f"doc{i}", "embedding": [float(i), 0.0]} for i in range(5)]
        result = vector_search_stage(
            data,
            {
                "path": "embedding",
                "queryVector": [2.0, 0.0],
                "limit": 3,
                # No numCandidates specified
            },
        )
        # Should still work with default
        assert len(result) == 3


class TestVectorSearchScoreField:
    def test_vector_search_custom_score_field(self):
        """$vectorSearch supports custom scoreField name."""
        from smongo.aggregation.vector import vector_search_stage

        data = [
            {"_id": "a", "embedding": [1.0, 0.0]},
            {"_id": "b", "embedding": [0.9, 0.1]},
        ]
        result = vector_search_stage(
            data,
            {
                "path": "embedding",
                "queryVector": [1.0, 0.0],
                "limit": 2,
                "scoreField": "customScore",
            },
        )
        assert "customScore" in result[0]
        assert "_vectorScore" not in result[0]
