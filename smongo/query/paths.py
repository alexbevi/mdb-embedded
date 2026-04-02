"""Dot-notation path utilities for traversing and mutating documents."""

from __future__ import annotations

from typing import Any

from .._types import Document


def get_value(doc: Document, key: str) -> Any:
    """Traverse a document using dot-notation path."""
    parts = key.split(".")
    val: Any = doc
    for p in parts:
        if isinstance(val, dict):
            val = val.get(p)
        elif isinstance(val, list):
            try:
                val = val[int(p)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if val is None:
            return None
    return val


def field_exists(doc: Document, key: str) -> bool:
    """Check whether a dot-notation path exists in a document.

    Unlike get_value, this distinguishes "field is present with value None/null"
    from "field is missing entirely", matching MongoDB $exists semantics.
    """
    parts = key.split(".")
    val: Any = doc
    for p in parts:
        if isinstance(val, dict):
            if p not in val:
                return False
            val = val[p]
        elif isinstance(val, list):
            try:
                val = val[int(p)]
            except (ValueError, IndexError):
                return False
        else:
            return False
    return True


def set_value(doc: Document, key: str, value: Any) -> None:
    """Set a value in a document using dot-notation path, creating intermediates."""
    parts = key.split(".")
    d: Any = doc
    for p in parts[:-1]:
        if p not in d or not isinstance(d[p], dict):
            d[p] = {}
        d = d[p]
    d[parts[-1]] = value


def unset_value(doc: Document, key: str) -> None:
    """Remove a field from a document using dot-notation path."""
    parts = key.split(".")
    d: Any = doc
    for p in parts[:-1]:
        if p not in d or not isinstance(d[p], dict):
            return
        d = d[p]
    if parts[-1] in d:
        del d[parts[-1]]
