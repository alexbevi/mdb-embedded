"""Mongo-compatible error codes and response formatting for the wire layer.

Implementation lives in Rust (_smongo_core); this module re-exports it with
stable return types for static analysis.
"""

from typing import Any, cast

from smongo._smongo_core import error_response as _error_response
from smongo._smongo_core import make_error as _make_error

from ._types import ResponseDoc


def make_error(*args: Any, **kwargs: Any) -> ResponseDoc:
    return cast(ResponseDoc, _make_error(*args, **kwargs))


def error_response(*args: Any, **kwargs: Any) -> ResponseDoc:
    return cast(ResponseDoc, _error_response(*args, **kwargs))


__all__ = ["error_response", "make_error"]
