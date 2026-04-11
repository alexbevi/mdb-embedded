"""Aggregation command handler.

NOTE: All commands in this module have Rust-native implementations that
take priority at runtime via ``rs_dispatch``.  These Python handlers serve
as fallback implementations and reference documentation.  Changes here
will NOT affect normal wire protocol behavior -- update the corresponding
Rust handler in ``rust/smongo-py/src/wire_commands/aggregate.rs`` instead.
"""

from __future__ import annotations

from bson import Int64

from ...aggregation import Cursor
from .._types import CommandDoc, DocSequences, ResponseDoc
from ..bson_codec import normalize_inbound, normalize_outbound_docs
from ..context import ConnectionContext
from ..errors import make_error
from ._registry import _inc_counter, _register


@_register("aggregate")
def _cmd_aggregate(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    _inc_counter("query")
    db_name = cmd.get("$db", "test")
    coll_name = cmd["aggregate"]

    pipeline = [normalize_inbound(stage) for stage in cmd.get("pipeline", [])]
    batch_size = cmd.get("cursor", {}).get("batchSize", 101)

    if coll_name == 1 or coll_name == "1":
        if pipeline and "$currentOp" in pipeline[0]:
            ns = f"{db_name}.$cmd.aggregate"
            return {
                "cursor": {"id": Int64(0), "ns": ns, "firstBatch": []},
                "ok": 1.0,
            }
        return {
            "cursor": {"id": Int64(0), "ns": f"{db_name}.$cmd.aggregate", "firstBatch": []},
            "ok": 1.0,
        }

    coll = ctx.get_collection(db_name, coll_name)
    ns = f"{db_name}.{coll_name}"

    if pipeline and "$changeStream" in pipeline[0]:
        change_pipeline = pipeline[1:] if len(pipeline) > 1 else None
        stream = coll.watch(change_pipeline)
        cursor_id = ctx.cursor_registry.create_change_stream(ns, stream, batch_size)
        return {"cursor": {"id": Int64(cursor_id), "ns": ns, "firstBatch": []}, "ok": 1.0}

    if pipeline and "$listSearchIndexes" in pipeline[0]:
        return {"cursor": {"id": Int64(0), "ns": ns, "firstBatch": []}, "ok": 1.0}

    if pipeline and "$indexStats" in pipeline[0]:
        index_docs = []
        for idx_info in coll.list_indexes():
            index_docs.append(
                {
                    "name": idx_info.get("name", ""),
                    "key": idx_info.get("key", {}),
                    "host": "localhost:embedded",
                    "accesses": {"ops": Int64(0), "since": "2026-01-01T00:00:00.000Z"},
                    "shard": "embedded",
                    "spec": idx_info,
                }
            )
        remaining = pipeline[1:]
        if remaining:
            coll_getter = lambda name: ctx.get_db(db_name).get_collection(name)
            result_docs = normalize_outbound_docs(
                Cursor(index_docs, collection_getter=coll_getter).aggregate(remaining)
            )
        else:
            result_docs = normalize_outbound_docs(index_docs)
        return {"cursor": {"id": Int64(0), "ns": ns, "firstBatch": result_docs}, "ok": 1.0}

    if pipeline and "$collStats" in pipeline[0]:
        spec = pipeline[0]["$collStats"]
        stats = coll.storage_stats()
        doc: dict[str, object] = {"ns": ns}
        if "storageStats" in spec:
            count = stats["count"]
            doc["storageStats"] = {
                "count": count,
                "size": stats["dataSize"],
                "avgObjSize": stats["dataSize"] // max(count, 1),
                "storageSize": stats["storageSize"],
                "freeStorageSize": 0,
                "nindexes": stats["nindexes"],
                "totalIndexSize": stats["totalIndexSize"],
                "totalSize": stats["storageSize"] + stats["totalIndexSize"],
                "indexSizes": stats["indexSizes"],
                "scaleFactor": 1,
                "storageEngine": stats.get("storageEngine", {"name": "redb"}),
            }
        if "count" in spec:
            doc["count"] = stats["count"]
        remaining = pipeline[1:]
        if remaining:
            coll_getter = lambda name: ctx.get_db(db_name).get_collection(name)
            result_docs = normalize_outbound_docs(
                Cursor([doc], collection_getter=coll_getter).aggregate(remaining)
            )
        else:
            result_docs = normalize_outbound_docs([doc])
        return {"cursor": {"id": Int64(0), "ns": ns, "firstBatch": result_docs}, "ok": 1.0}

    if hasattr(coll, "aggregate_engine"):
        result = list(coll.aggregate_engine(pipeline))
    else:
        docs = coll.get_all()
        coll_getter = lambda name: ctx.get_db(db_name).get_collection(name)
        result = Cursor(docs, collection_getter=coll_getter).aggregate(pipeline)
    result_docs = normalize_outbound_docs(result)

    cursor_id, first_batch = ctx.cursor_registry.create(ns, result_docs, batch_size)
    return {"cursor": {"id": Int64(cursor_id), "ns": ns, "firstBatch": first_batch}, "ok": 1.0}


@_register("mapReduce", "mapreduce")
def _cmd_map_reduce(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return make_error(
        "CommandNotSupported",
        "mapReduce is deprecated and not supported by smongo. Use aggregation instead.",
    )
