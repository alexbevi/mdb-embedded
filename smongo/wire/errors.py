"""Mongo-compatible error codes and response formatting for the wire layer.

Implementation lives in Rust (_smongo_core); this module re-exports it with
typed signatures matching the Rust definitions in ``wire_errors.rs``.
"""

from typing import cast

from smongo._smongo_core import error_response as _error_response
from smongo._smongo_core import make_error as _make_error

from ._types import ResponseDoc


def make_error(name: str, message: str) -> ResponseDoc:
    """Build an error response from a well-known error *name* and *message*.

    See ``wire_errors.rs::lookup_error`` for the canonical name -> code map.
    Unknown names silently fall back to ``InternalError`` (code 1).
    """
    return cast(ResponseDoc, _make_error(name, message))


def error_response(code: int, code_name: str, message: str) -> ResponseDoc:
    """Build an error response with an explicit numeric *code* and *code_name*."""
    return cast(ResponseDoc, _error_response(code, code_name, message))


__all__ = ["error_response", "make_error"]
