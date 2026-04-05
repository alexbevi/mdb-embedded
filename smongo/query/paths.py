"""Dot-notation path utilities for traversing and mutating documents.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import field_exists, get_value, set_value, unset_value

__all__ = ["field_exists", "get_value", "set_value", "unset_value"]
