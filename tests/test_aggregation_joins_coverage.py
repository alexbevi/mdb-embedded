"""Coverage tests for aggregation joins variable binding."""

from smongo.aggregation.joins import _bind_pipeline_vars, _bind_vars_in_expr


class TestBindVarsInExpr:
    def test_bind_simple_var(self):
        """$$var is replaced with scope value."""
        result = _bind_vars_in_expr("$$myVar", {"myVar": 42})
        assert result == 42

    def test_bind_var_not_in_scope(self):
        """$$var not in scope is returned unchanged."""
        result = _bind_vars_in_expr("$$unknown", {"known": 10})
        assert result == "$$unknown"

    def test_bind_root_unchanged(self):
        """$$ROOT is not replaced."""
        result = _bind_vars_in_expr("$$ROOT", {"ROOT": "should_not_replace"})
        assert result == "$$ROOT"

    def test_bind_current_unchanged(self):
        """$$CURRENT is not replaced."""
        result = _bind_vars_in_expr("$$CURRENT", {"CURRENT": "should_not_replace"})
        assert result == "$$CURRENT"

    def test_bind_dict_recursive(self):
        """Variables in dicts are replaced."""
        expr = {"field": "$$myVar", "nested": {"value": "$$otherVar"}}
        scope = {"myVar": 100, "otherVar": 200}
        result = _bind_vars_in_expr(expr, scope)
        assert result["field"] == 100
        assert result["nested"]["value"] == 200

    def test_bind_list_recursive(self):
        """Variables in lists are replaced."""
        expr = ["$$var1", {"x": "$$var2"}]
        scope = {"var1": "a", "var2": "b"}
        result = _bind_vars_in_expr(expr, scope)
        assert result[0] == "a"
        assert result[1]["x"] == "b"

    def test_bind_non_var_string_unchanged(self):
        """Regular strings without $$ are unchanged."""
        result = _bind_vars_in_expr("regular_string", {})
        assert result == "regular_string"

    def test_bind_primitives_unchanged(self):
        """Non-string primitives are unchanged."""
        assert _bind_vars_in_expr(42, {}) == 42
        assert _bind_vars_in_expr(None, {}) is None
        assert _bind_vars_in_expr(True, {}) is True


class TestBindPipelineVars:
    def test_bind_pipeline_stages(self):
        """_bind_pipeline_vars processes all stages."""
        pipeline = [
            {"$match": {"value": "$$threshold"}},
            {"$project": {"result": "$$multiplier"}},
        ]
        scope = {"threshold": 100, "multiplier": 5}
        result = _bind_pipeline_vars(pipeline, scope)
        assert result[0]["$match"]["value"] == 100
        assert result[1]["$project"]["result"] == 5

    def test_bind_empty_pipeline(self):
        """_bind_pipeline_vars handles empty pipeline."""
        result = _bind_pipeline_vars([], {"var": 1})
        assert result == []
