"""Logical session registry for the wire protocol.

Thread-safe registry of logical sessions across all connections, with
background reaper for idle expiry.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import MAX_SESSIONS, SessionRegistry, TooManySessions

__all__ = [
    "MAX_SESSIONS",
    "SessionRegistry",
    "TooManySessions",
]
