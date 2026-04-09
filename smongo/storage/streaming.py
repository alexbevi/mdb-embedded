from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from .._types import Document, Filter


class StreamingCursor:
    """Lazy iterator over documents via the collection's ``find`` (engine-backed)."""

    def __init__(self, collection: Any, query: Filter | None = None) -> None:
        self._collection = collection
        self._query = query or {}

    def __iter__(self) -> Iterator[Document]:
        find = getattr(self._collection, "find", None)
        if find is None:
            raise TypeError("collection must implement find() for StreamingCursor")
        yield from find(self._query)
