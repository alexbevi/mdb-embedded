"""Aggregation expression engine -- resolves $cond, $concat, $add, etc.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import resolve_expr

__all__ = ["resolve_expr"]
