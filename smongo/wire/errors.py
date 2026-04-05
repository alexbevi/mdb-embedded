"""Mongo-compatible error codes and response formatting for the wire layer.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import error_response, make_error

__all__ = ["error_response", "make_error"]
