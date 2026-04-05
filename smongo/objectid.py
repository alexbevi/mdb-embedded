"""
ObjectId -- MongoDB-compatible document identifier.

Follows the MongoDB ObjectId spec:
    4-byte timestamp (seconds since epoch)
    5-byte random value (per-process, generated once)
    3-byte incrementing counter (per-process)

The 12-byte value is represented as a 24-character hex string.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import ObjectId

__all__ = ["ObjectId"]
