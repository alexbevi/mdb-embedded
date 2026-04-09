"""
Async API for smongo — asyncio-native wrappers around the synchronous engine.

Mirrors the synchronous :class:`MongoClient` / :class:`Database` /
:class:`Collection` API with ``async`` / ``await`` semantics.  All blocking
engine work is offloaded to a thread executor via :func:`asyncio.to_thread`
(Python 3.11+), keeping the event loop responsive.

Usage::

    from smongo.async_client import AsyncMongoClient

    async def main():
        client = AsyncMongoClient("local://data")
        db = client["mydb"]
        coll = db["users"]

        await coll.insert_one({"name": "Alice", "age": 34})
        async for doc in await coll.find({"age": {"$gt": 30}}):
            print(doc["name"])

        results = await coll.aggregate([
            {"$group": {"_id": "$city", "avg_age": {"$avg": "$age"}}},
        ])
"""

from __future__ import annotations

import asyncio
from typing import Any

from ._types import Document, Filter, IndexKeys, Pipeline, Projection, UpdateSpec
from .client import (
    BulkWriteResult,
    Collection,
    Database,
    MongoClient,
)
from .storage.results import DeleteResult, InsertResult, UpdateResult


class AsyncCursor:
    """Async-iterable cursor wrapping a synchronous result set."""

    def __init__(self, docs: list[Document]) -> None:
        self._docs = docs
        self._index = 0

    def __aiter__(self) -> AsyncCursor:
        return self

    async def __anext__(self) -> Document:
        if self._index >= len(self._docs):
            raise StopAsyncIteration
        doc = self._docs[self._index]
        self._index += 1
        return doc

    def to_list(self) -> list[Document]:
        return list(self._docs)

    def __len__(self) -> int:
        return len(self._docs)


class AsyncChangeStream:
    """Async-iterable change stream over the embedded oplog.

    Polls the underlying synchronous change stream in a background thread
    so that ``async for event in stream:`` never blocks the event loop.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._closed = False

    @property
    def resume_token(self) -> dict[str, Any] | None:
        return getattr(self._inner, "resume_token", None)

    def __aiter__(self) -> AsyncChangeStream:
        return self

    async def __anext__(self) -> Document:
        if self._closed:
            raise StopAsyncIteration
        while True:
            event: Document | None = await asyncio.to_thread(self._inner.try_next)
            if event is not None:
                return event
            await asyncio.sleep(0.05)

    async def close(self) -> None:
        self._closed = True
        await asyncio.to_thread(self._inner.close)

    async def __aenter__(self) -> AsyncChangeStream:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


class AsyncCollection:
    """Async wrapper around :class:`~smongo.client.Collection`."""

    def __init__(self, sync_coll: Collection) -> None:
        self._sync = sync_coll

    # -- reads ---------------------------------------------------------

    async def find(
        self, query: Filter | None = None, projection: Projection | None = None
    ) -> AsyncCursor:
        result = await asyncio.to_thread(self._sync.find, query, projection)
        docs = list(result)
        return AsyncCursor(docs)

    async def find_one(
        self, query: Filter | None = None, projection: Projection | None = None
    ) -> Document | None:
        return await asyncio.to_thread(self._sync.find_one, query, projection)

    async def aggregate(
        self,
        pipeline: Pipeline,
        *,
        allowDiskUse: bool = False,
        memory_limit_bytes: int | None = None,
    ) -> AsyncCursor:
        kwargs: dict[str, Any] = {}
        if allowDiskUse:
            kwargs["allowDiskUse"] = True
        if memory_limit_bytes is not None:
            kwargs["memory_limit_bytes"] = memory_limit_bytes
        result = await asyncio.to_thread(lambda: self._sync.aggregate(pipeline, **kwargs))
        docs = list(result)
        return AsyncCursor(docs)

    async def count_documents(self, query: Filter | None = None) -> int:
        return await asyncio.to_thread(self._sync.count_documents, query)

    async def explain(self, query: Filter | None = None) -> dict[str, Any]:
        return await asyncio.to_thread(self._sync.explain, query)

    # -- change streams ------------------------------------------------

    async def watch(
        self,
        pipeline: Pipeline | None = None,
        *,
        resume_after: dict[str, Any] | None = None,
    ) -> AsyncChangeStream:
        stream = await asyncio.to_thread(self._sync.watch, pipeline, resume_after=resume_after)
        return AsyncChangeStream(stream)

    # -- writes --------------------------------------------------------

    async def insert_one(self, doc: Document) -> InsertResult | Any:
        return await asyncio.to_thread(self._sync.insert_one, doc)

    async def insert_many(self, docs: list[Document]) -> InsertResult | Any:
        return await asyncio.to_thread(self._sync.insert_many, docs)

    async def update_one(
        self, query: Filter, update: UpdateSpec, upsert: bool = False
    ) -> UpdateResult | Any:
        return await asyncio.to_thread(self._sync.update_one, query, update, upsert)

    async def update_many(
        self, query: Filter, update: UpdateSpec, upsert: bool = False
    ) -> UpdateResult | Any:
        return await asyncio.to_thread(self._sync.update_many, query, update, upsert)

    async def delete_one(self, query: Filter) -> DeleteResult | Any:
        return await asyncio.to_thread(self._sync.delete_one, query)

    async def delete_many(self, query: Filter) -> DeleteResult | Any:
        return await asyncio.to_thread(self._sync.delete_many, query)

    # -- find_one_and_* ------------------------------------------------

    async def find_one_and_update(
        self, query: Filter, update: UpdateSpec, *, return_document: str = "before"
    ) -> Document | None:
        return await asyncio.to_thread(
            lambda: self._sync.find_one_and_update(query, update, return_document=return_document)
        )

    async def find_one_and_replace(
        self,
        query: Filter,
        replacement: Document,
        *,
        upsert: bool = False,
        return_document: str = "before",
    ) -> Document | None:
        return await asyncio.to_thread(
            lambda: self._sync.find_one_and_replace(
                query, replacement, upsert=upsert, return_document=return_document
            )
        )

    async def find_one_and_delete(self, query: Filter) -> Document | None:
        return await asyncio.to_thread(self._sync.find_one_and_delete, query)

    # -- bulk_write ----------------------------------------------------

    async def bulk_write(self, requests: list[Any], ordered: bool = True) -> BulkWriteResult | Any:
        return await asyncio.to_thread(self._sync.bulk_write, requests, ordered)

    # -- indexes -------------------------------------------------------

    async def create_index(self, keys: IndexKeys, **kwargs: Any) -> str | Any:
        return await asyncio.to_thread(lambda: self._sync.create_index(keys, **kwargs))

    async def drop_index(self, name: str) -> None:
        await asyncio.to_thread(self._sync.drop_index, name)

    async def list_indexes(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._sync.list_indexes)

    # -- oplog ---------------------------------------------------------

    async def get_oplog(self) -> list[Document]:
        return await asyncio.to_thread(self._sync.get_oplog)


class AsyncDatabase:
    """Async wrapper around :class:`~smongo.client.Database`."""

    def __init__(self, sync_db: Database) -> None:
        self._sync = sync_db

    def __getitem__(self, name: str) -> AsyncCollection:
        return AsyncCollection(self._sync[name])

    async def list_collection_names(self) -> list[str]:
        return await asyncio.to_thread(self._sync.list_collection_names)

    async def drop_collection(self, name: str) -> None:
        await asyncio.to_thread(self._sync.drop_collection, name)

    async def create_collection(self, name: str, **kwargs: Any) -> AsyncCollection:
        sync_coll = await asyncio.to_thread(lambda: self._sync.create_collection(name, **kwargs))
        return AsyncCollection(sync_coll)


class AsyncMongoClient:
    """Async wrapper around :class:`~smongo.client.MongoClient`.

    Provides the same URI-based routing as the synchronous client while
    exposing an ``async`` / ``await`` interface suitable for use with
    ``asyncio``, FastAPI, Starlette, and other async frameworks.

    The underlying engine operations are dispatched to a thread executor
    via :func:`asyncio.to_thread`, which keeps the event loop free for
    concurrent I/O while the Rust engine performs reads and writes.
    """

    def __init__(
        self,
        uri: str = "local://local_data",
        sync: str | None = None,
        sync_config: dict[str, Any] | None = None,
        *,
        durable: bool = True,
        backend: str | None = None,
    ) -> None:
        self._sync = MongoClient(
            uri, sync=sync, sync_config=sync_config, durable=durable, backend=backend
        )

    def __getitem__(self, db_name: str) -> AsyncDatabase:
        return AsyncDatabase(self._sync[db_name])

    @property
    def sync_client(self) -> MongoClient:
        """Access the underlying synchronous client for advanced use."""
        return self._sync

    async def close(self) -> None:
        await asyncio.to_thread(self._sync.close)

    async def __aenter__(self) -> AsyncMongoClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
