"""Tests for AsyncCollection, AsyncDatabase, AsyncMongoClient wrappers."""

from __future__ import annotations

import asyncio

import pytest

from smongo.async_client import (
    AsyncChangeStream,
    AsyncCollection,
    AsyncDatabase,
    AsyncMongoClient,
)
from smongo.client import MongoClient


@pytest.fixture
def sync_client(tmp_path):
    c = MongoClient(f"local://{tmp_path}/async_redb")
    yield c
    c.close()


@pytest.fixture
def async_client(tmp_path):
    return AsyncMongoClient(f"local://{tmp_path}/async_redb2")


class TestAsyncCollection:
    def test_insert_find(self, sync_client):
        coll = sync_client["test"]["acol"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"name": "Alice", "age": 30})
            cursor = await acol.find({"name": "Alice"})
            docs = cursor.to_list()
            assert len(docs) == 1
            assert docs[0]["name"] == "Alice"

        asyncio.run(run())

    def test_find_one(self, sync_client):
        coll = sync_client["test"]["fo"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"x": 42})
            doc = await acol.find_one({"x": 42})
            assert doc is not None
            assert doc["x"] == 42

        asyncio.run(run())

    def test_insert_many(self, sync_client):
        coll = sync_client["test"]["im"]
        acol = AsyncCollection(coll)

        async def run():
            result = await acol.insert_many([{"i": 1}, {"i": 2}, {"i": 3}])
            assert len(result.inserted_ids) == 3

        asyncio.run(run())

    def test_update_one(self, sync_client):
        coll = sync_client["test"]["u1"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"k": 1, "v": "old"})
            r = await acol.update_one({"k": 1}, {"$set": {"v": "new"}})
            assert r.modified_count == 1

        asyncio.run(run())

    def test_update_many(self, sync_client):
        coll = sync_client["test"]["um"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_many([{"g": "a"}, {"g": "a"}, {"g": "b"}])
            r = await acol.update_many({"g": "a"}, {"$set": {"done": True}})
            assert r.modified_count == 2

        asyncio.run(run())

    def test_delete_one(self, sync_client):
        coll = sync_client["test"]["d1"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"x": 1})
            r = await acol.delete_one({"x": 1})
            assert r.deleted_count == 1

        asyncio.run(run())

    def test_delete_many(self, sync_client):
        coll = sync_client["test"]["dm"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
            r = await acol.delete_many({"x": 1})
            assert r.deleted_count == 2

        asyncio.run(run())

    def test_replace_one(self, sync_client):
        coll = sync_client["test"]["rep"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"a": 1, "b": 2})
            await acol.replace_one({"a": 1}, {"a": 1, "c": 3})
            doc = await acol.find_one({"a": 1})
            assert doc is not None
            assert "c" in doc

        asyncio.run(run())

    def test_count_documents(self, sync_client):
        coll = sync_client["test"]["cd"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_many([{"x": 1}, {"x": 2}])
            assert await acol.count_documents({}) == 2
            assert await acol.count_documents({"x": 1}) == 1

        asyncio.run(run())

    def test_distinct(self, sync_client):
        coll = sync_client["test"]["dst"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_many([{"c": "a"}, {"c": "b"}, {"c": "a"}])
            vals = await acol.distinct("c")
            assert sorted(vals) == ["a", "b"]

        asyncio.run(run())

    def test_estimated_document_count(self, sync_client):
        coll = sync_client["test"]["edc"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_many([{"x": 1}, {"x": 2}])
            n = await acol.estimated_document_count()
            assert n == 2

        asyncio.run(run())

    def test_explain(self, sync_client):
        coll = sync_client["test"]["expl"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"x": 1})
            plan = await acol.explain({"x": 1})
            assert isinstance(plan, dict)

        asyncio.run(run())

    def test_aggregate(self, sync_client):
        coll = sync_client["test"]["agg"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_many([{"v": 1}, {"v": 2}, {"v": 3}])
            cursor = await acol.aggregate([{"$match": {"v": {"$gte": 2}}}])
            docs = cursor.to_list()
            assert len(docs) == 2

        asyncio.run(run())

    def test_create_drop_index(self, sync_client):
        coll = sync_client["test"]["idx"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"a": 1})
            name = await acol.create_index([("a", 1)])
            indexes = await acol.list_indexes()
            assert any(i["name"] == name for i in indexes)
            await acol.drop_index(name)

        asyncio.run(run())

    def test_find_one_and_update(self, sync_client):
        coll = sync_client["test"]["foau"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"k": 1, "v": "old"})
            doc = await acol.find_one_and_update(
                {"k": 1}, {"$set": {"v": "new"}}, return_document="after"
            )
            assert doc is not None
            assert doc["v"] == "new"

        asyncio.run(run())

    def test_find_one_and_replace(self, sync_client):
        coll = sync_client["test"]["foar"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"k": 1, "v": "old"})
            doc = await acol.find_one_and_replace(
                {"k": 1}, {"k": 1, "v": "replaced"}, return_document="after"
            )
            assert doc is not None
            assert doc["v"] == "replaced"

        asyncio.run(run())

    def test_find_one_and_delete(self, sync_client):
        coll = sync_client["test"]["foad"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"k": 1})
            doc = await acol.find_one_and_delete({"k": 1})
            assert doc is not None
            assert await acol.count_documents({}) == 0

        asyncio.run(run())

    def test_drop(self, sync_client):
        coll = sync_client["test"]["todrop"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"x": 1})
            await acol.drop()

        asyncio.run(run())

    def test_get_oplog(self, sync_client):
        coll = sync_client["test"]["oplog"]
        acol = AsyncCollection(coll)

        async def run():
            await acol.insert_one({"x": 1})
            log = await acol.get_oplog()
            assert isinstance(log, list)
            assert len(log) >= 1

        asyncio.run(run())

    def test_watch(self, sync_client):
        coll = sync_client["test"]["watch"]
        acol = AsyncCollection(coll)

        async def run():
            stream = await acol.watch()
            assert isinstance(stream, AsyncChangeStream)
            await stream.close()

        asyncio.run(run())


class TestAsyncChangeStream:
    def test_context_manager(self, sync_client):
        coll = sync_client["test"]["cscm"]
        acol = AsyncCollection(coll)

        async def run():
            async with await acol.watch() as stream:
                assert stream.resume_token is None or isinstance(stream.resume_token, dict)

        asyncio.run(run())


class TestAsyncDatabase:
    def test_getitem(self, sync_client):
        adb = AsyncDatabase(sync_client["test"])
        acol = adb["mycol"]
        assert isinstance(acol, AsyncCollection)

    def test_list_collection_names(self, sync_client):
        sync_client["test"]["dbtest"].insert_one({"x": 1})
        adb = AsyncDatabase(sync_client["test"])

        async def run():
            names = await adb.list_collection_names()
            assert "dbtest" in names

        asyncio.run(run())

    def test_create_and_drop_collection(self, sync_client):
        adb = AsyncDatabase(sync_client["test"])

        async def run():
            acol = await adb.create_collection("newcol")
            assert isinstance(acol, AsyncCollection)
            await adb.drop_collection("newcol")

        asyncio.run(run())


class TestAsyncMongoClient:
    def test_getitem(self, async_client):
        adb = async_client["test"]
        assert isinstance(adb, AsyncDatabase)

    def test_sync_client_property(self, async_client):
        assert isinstance(async_client.sync_client, MongoClient)

    def test_list_database_names(self, async_client):
        async def run():
            names = await async_client.list_database_names()
            assert isinstance(names, list)

        asyncio.run(run())

    def test_server_info(self, async_client):
        async def run():
            info = await async_client.server_info()
            assert isinstance(info, dict)

        asyncio.run(run())

    def test_context_manager(self, tmp_path):
        async def run():
            async with AsyncMongoClient(f"local://{tmp_path}/ctx_redb") as client:
                db = client["test"]
                coll = db["ctxcol"]
                await coll.insert_one({"x": 1})
                assert await coll.count_documents({}) == 1

        asyncio.run(run())

    def test_drop_database(self, async_client):
        async def run():
            db = async_client["dropdb"]
            coll = db["col"]
            await coll.insert_one({"x": 1})
            await async_client.drop_database("dropdb")

        asyncio.run(run())

    def test_close(self, async_client):
        async def run():
            await async_client.close()

        asyncio.run(run())
