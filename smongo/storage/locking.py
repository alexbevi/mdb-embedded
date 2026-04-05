"""Read-write lock for collection-level concurrency.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import ReadWriteLock

__all__ = ["ReadWriteLock"]
