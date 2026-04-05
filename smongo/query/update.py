"""MongoDB update operators -- apply $set, $inc, $push, etc. to documents.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import apply_update

__all__ = ["apply_update"]
