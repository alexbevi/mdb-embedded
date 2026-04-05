"""Lightweight result types for insert, update, and delete operations.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import DeleteResult, InsertResult, UpdateResult

__all__ = ["DeleteResult", "InsertResult", "UpdateResult"]
