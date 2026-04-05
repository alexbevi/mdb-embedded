"""BSON Boundary Adapter -- isolates all BSON <-> engine type conversion at the wire edge.

Since P8, the wire path uses a single-pass raw BSON byte codec (raw_bson.rs)
that converts directly between wire bytes and engine-ready Python dicts.  The
normalize_inbound / normalize_outbound functions below are still used by the
LocalClient path (Python-side document construction) and are re-exported here
for backward compatibility.

Core logic lives in Rust (_smongo_core); this module re-exports it and keeps
the MAX_NESTING_DEPTH constant that test_security.py references.
"""

from smongo._smongo_core import (
    normalize_inbound,
    normalize_outbound,
    normalize_outbound_docs,
)

MAX_NESTING_DEPTH = 100

__all__ = [
    "MAX_NESTING_DEPTH",
    "normalize_inbound",
    "normalize_outbound",
    "normalize_outbound_docs",
]
