from __future__ import annotations

from typing import Any

from bson import Int64

from .._types import CommandDoc, DocSequences, ResponseDoc
from ..context import ConnectionContext
from ..errors import make_error
from ._registry import _register, log


@_register("listIndexes")
def _cmd_list_indexes(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["listIndexes"]
    coll = ctx.get_collection(db_name, coll_name)
    ns = f"{db_name}.{coll_name}"

    indexes = coll.list_indexes()

    formatted: list[dict[str, Any]] = [{"v": 2, "key": {"_id": 1}, "name": "_id_", "ns": ns}]
    for idx in indexes:
        spec: dict[str, Any] = {
            "v": 2,
            "key": {k: d for k, d in idx.get("keys", [])},
            "name": idx.get("name", ""),
            "ns": ns,
        }
        if idx.get("unique"):
            spec["unique"] = True
        if idx.get("sparse"):
            spec["sparse"] = True
        if idx.get("expireAfterSeconds") is not None:
            spec["expireAfterSeconds"] = idx["expireAfterSeconds"]
        formatted.append(spec)

    batch_size = cmd.get("cursor", {}).get("batchSize", 101)
    cursor_id, first_batch = ctx.cursor_registry.create(
        f"{db_name}.$cmd.listIndexes.{coll_name}", formatted, batch_size
    )
    return {
        "cursor": {
            "id": Int64(cursor_id),
            "ns": f"{db_name}.$cmd.listIndexes.{coll_name}",
            "firstBatch": first_batch,
        },
        "ok": 1.0,
    }


@_register("createIndexes")
def _cmd_create_indexes(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["createIndexes"]
    coll = ctx.get_collection(db_name, coll_name)

    before = len(coll.list_indexes()) + 1

    for idx_spec in cmd.get("indexes", []):
        key = idx_spec.get("key", {})
        keys = list(key.items())
        kwargs: dict[str, Any] = {}
        if "name" in idx_spec:
            kwargs["name"] = idx_spec["name"]
        if idx_spec.get("unique"):
            kwargs["unique"] = True
        if idx_spec.get("sparse"):
            kwargs["sparse"] = True
        if "expireAfterSeconds" in idx_spec:
            kwargs["expireAfterSeconds"] = idx_spec["expireAfterSeconds"]
        coll.create_index(keys, **kwargs)

    after = len(coll.list_indexes()) + 1

    return {"numIndexesBefore": before, "numIndexesAfter": after, "ok": 1.0}


@_register("dropIndexes", help="Drop one or more indexes from a collection")
def _cmd_drop_indexes(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["dropIndexes"]
    coll = ctx.get_collection(db_name, coll_name)

    index = cmd.get("index")
    n_before = len(coll.list_indexes()) + 1

    if isinstance(index, str):
        if index == "*":
            for idx in list(coll.list_indexes()):
                coll.drop_index(idx["name"])
        else:
            coll.drop_index(index)
    elif isinstance(index, list):
        for idx_name in index:
            if isinstance(idx_name, str):
                coll.drop_index(idx_name)
    elif isinstance(index, dict):
        target_keys = set(index.keys())
        for idx in list(coll.list_indexes()):
            idx_keys = set(k for k, _ in idx.get("keys", []))
            if idx_keys == target_keys:
                coll.drop_index(idx["name"])
                break
    elif index is None:
        return make_error("InvalidOptions", "dropIndexes requires an 'index' parameter")

    return {"nIndexesWas": n_before, "ok": 1.0}


@_register("reIndex")
def _cmd_reindex(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["reIndex"]
    coll = ctx.get_collection(db_name, coll_name)
    n = coll.rebuild_all_indexes()
    log.info("reIndex: rebuilt %d indexes on %s.%s", n, db_name, coll_name)
    return {"nIndexesWas": n + 1, "nIndexes": n + 1, "ok": 1.0}
