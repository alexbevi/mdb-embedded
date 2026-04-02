from __future__ import annotations

import atexit
import heapq
import json
import os
import sys
import tempfile
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

DEFAULT_MAX_PIPELINE_DOCS = 100_000
MAX_PIPELINE_STAGES = 50
DEFAULT_MEMORY_LIMIT_BYTES = 100 * 1024 * 1024  # 100 MB


class DocumentLimitExceeded(RuntimeError):
    """Raised when an aggregation pipeline exceeds the document-count limit."""


class MemoryLimitExceeded(RuntimeError):
    """Raised when a pipeline stage exceeds the in-memory sort/group limit.

    Suggests using ``allowDiskUse=True`` to enable spill-to-disk.
    """


def _estimate_docs_bytes(docs: list[dict[str, Any]]) -> int:
    """Rough estimate of the memory consumed by a list of documents."""
    if not docs:
        return 0
    sample_size = min(len(docs), 20)
    sample_total = sum(sys.getsizeof(json.dumps(docs[i], default=str)) for i in range(sample_size))
    return (sample_total // sample_size) * len(docs)


def _write_chunk_to_file(chunk: list[dict[str, Any]]) -> str:
    """Write a list of docs as JSON-lines to a temp file, return its path."""
    fd, path = tempfile.mkstemp(suffix=".jsonl", prefix="smongo_spill_")
    try:
        with os.fdopen(fd, "w") as f:
            for doc in chunk:
                f.write(json.dumps(doc, default=str))
                f.write("\n")
    except BaseException:
        os.close(fd)
        raise
    return path


def _iter_jsonl_file(path: str) -> Iterator[dict[str, Any]]:
    """Yield docs from a JSON-lines temp file."""
    with open(path) as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


class _KeyedDoc:
    """Wrapper for heapq comparison that uses (key, tiebreaker) ordering."""

    __slots__ = ("doc", "key", "tie")

    def __init__(self, doc: dict[str, Any], key: Any, tie: int) -> None:
        self.doc = doc
        self.key = key
        self.tie = tie

    def __lt__(self, other: _KeyedDoc) -> bool:
        if self.key == other.key:
            return self.tie < other.tie
        try:
            return self.key < other.key  # type: ignore[no-any-return]
        except TypeError:
            return str(self.key) < str(other.key)


class DiskSpillSorter:
    """External merge sort that spills sorted chunks to temporary files.

    Used by ``$sort`` when ``allowDiskUse=True`` and the document set
    exceeds the in-memory limit.  Each chunk is sorted in-memory, flushed
    to a JSON-lines temp file, then all chunks are merged via a heap.
    """

    _CHUNK_SIZE = 10_000

    def __init__(self, key_fn: Any, reverse: bool = False) -> None:
        self._key_fn = key_fn
        self._reverse = reverse
        self._tmpfiles: list[str] = []
        atexit.register(self.cleanup)

    def sort(self, docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Sort *docs* via chunked external merge through temp files."""
        if len(docs) <= self._CHUNK_SIZE:
            docs.sort(key=self._key_fn, reverse=self._reverse)
            return docs

        for i in range(0, len(docs), self._CHUNK_SIZE):
            chunk = docs[i : i + self._CHUNK_SIZE]
            chunk.sort(key=self._key_fn, reverse=self._reverse)
            path = _write_chunk_to_file(chunk)
            self._tmpfiles.append(path)

        docs.clear()

        result = list(self._merge_files())
        self.cleanup()
        return result

    def _merge_files(self) -> Iterator[dict[str, Any]]:
        """K-way merge across all temp files using a heap."""
        iters: list[Iterator[dict[str, Any]]] = [_iter_jsonl_file(p) for p in self._tmpfiles]

        if self._reverse:
            yield from self._merge_reverse(iters)
            return

        heap: list[_KeyedDoc] = []
        for idx, it in enumerate(iters):
            doc = next(it, None)
            if doc is not None:
                heapq.heappush(heap, _KeyedDoc(doc, self._key_fn(doc), idx))

        sources = list(enumerate(iters))

        while heap:
            entry = heapq.heappop(heap)
            yield entry.doc
            src_it = sources[entry.tie][1]
            nxt = next(src_it, None)
            if nxt is not None:
                heapq.heappush(heap, _KeyedDoc(nxt, self._key_fn(nxt), entry.tie))

    def _merge_reverse(self, iters: list[Iterator[dict[str, Any]]]) -> Iterator[dict[str, Any]]:
        """Max-heap merge for reverse (descending) sorts."""
        heap: list[_NegKeyedDoc] = []
        for idx, it in enumerate(iters):
            doc = next(it, None)
            if doc is not None:
                neg = _KeyedDoc(doc, self._key_fn(doc), idx)
                heapq.heappush(heap, _NegKeyedDoc(neg))

        sources = list(enumerate(iters))

        while heap:
            neg_entry = heapq.heappop(heap)
            entry = neg_entry.inner
            yield entry.doc
            src_it = sources[entry.tie][1]
            nxt = next(src_it, None)
            if nxt is not None:
                neg = _KeyedDoc(nxt, self._key_fn(nxt), entry.tie)
                heapq.heappush(heap, _NegKeyedDoc(neg))

    def cleanup(self) -> None:
        """Remove all temporary files."""
        for p in self._tmpfiles:
            try:
                os.unlink(p)
            except OSError:
                pass
        self._tmpfiles.clear()


class _NegKeyedDoc:
    """Inverted comparison wrapper for max-heap emulation via heapq (min-heap)."""

    __slots__ = ("inner",)

    def __init__(self, inner: _KeyedDoc) -> None:
        self.inner = inner

    def __lt__(self, other: _NegKeyedDoc) -> bool:
        if self.inner.key == other.inner.key:
            return self.inner.tie < other.inner.tie
        try:
            return self.inner.key > other.inner.key  # type: ignore[no-any-return]
        except TypeError:
            return str(self.inner.key) > str(other.inner.key)


class DiskSpillGrouper:
    """Disk-backed grouping for ``$group`` when ``allowDiskUse=True``.

    Partitions documents into bucket files keyed by group key, then
    reduces each bucket from disk.
    """

    _FLUSH_THRESHOLD = 5_000

    def __init__(self) -> None:
        self._buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._bucket_files: dict[str, list[str]] = defaultdict(list)
        self._total_buffered = 0
        atexit.register(self.cleanup)

    def add(self, key: Any, doc: dict[str, Any]) -> None:
        """Buffer a document under its group key."""
        str_key = (
            json.dumps(key, sort_keys=True, default=str)
            if isinstance(key, dict | list)
            else str(key)
        )
        self._buckets[str_key].append(doc)
        self._total_buffered += 1
        if self._total_buffered >= self._FLUSH_THRESHOLD:
            self._flush()

    def _flush(self) -> None:
        """Write all in-memory buckets to temp files."""
        for str_key, docs in self._buckets.items():
            if docs:
                path = _write_chunk_to_file(docs)
                self._bucket_files[str_key].append(path)
        self._buckets.clear()
        self._total_buffered = 0

    def iter_groups(self) -> Iterator[tuple[Any, Iterator[dict[str, Any]]]]:
        """Yield ``(key, docs_iterator)`` for each group."""
        self._flush()

        all_keys = set(self._bucket_files.keys())
        for str_key in sorted(all_keys):
            try:
                key = json.loads(str_key)
            except (json.JSONDecodeError, TypeError):
                key = str_key

            def _docs_for_key(sk: str = str_key) -> Iterator[dict[str, Any]]:
                for path in self._bucket_files.get(sk, []):
                    yield from _iter_jsonl_file(path)

            yield key, _docs_for_key()

    def cleanup(self) -> None:
        """Remove all temporary files."""
        for paths in self._bucket_files.values():
            for p in paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass
        self._bucket_files.clear()
        self._buckets.clear()
        self._total_buffered = 0
