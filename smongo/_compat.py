"""Shared exception types for the Python package."""

from __future__ import annotations


class StorageError(Exception):
    """Raised for storage-layer operational failures (I/O, engine, persistence)."""
