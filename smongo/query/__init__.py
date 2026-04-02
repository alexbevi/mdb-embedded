"""MQL Compiler -- query compilation, update operators, expression engine, and path utilities."""

from .compiler import (
    MAX_REGEX_PATTERN_LEN,
    _build_regex_flags,
    _safe_regex,
    compile_query,
)
from .expressions import resolve_expr
from .paths import field_exists, get_value, set_value, unset_value
from .update import apply_update

__all__ = [
    "MAX_REGEX_PATTERN_LEN",
    "_build_regex_flags",
    "_safe_regex",
    "apply_update",
    "compile_query",
    "field_exists",
    "get_value",
    "resolve_expr",
    "set_value",
    "unset_value",
]
