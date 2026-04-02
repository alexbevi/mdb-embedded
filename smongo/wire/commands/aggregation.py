from __future__ import annotations

from ._registry import _inc_counter, _register
from ...aggregation import Cursor
from .._types import CommandDoc, DocSequences, ResponseDoc
from ..bson_codec import normalize_inbound, normalize_outbound_docs
from ..context import ConnectionContext
from ..errors import make_error


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
                "cursor": {"id": 0, "ns": ns, "firstBatch": []},
                "ok": 1.0,
            }
        return {
            "cursor": {"id": 0, "ns": f"{db_name}.$cmd.aggregate", "firstBatch": []},
            "ok": 1.0,
        }

    coll = ctx.get_collection(db_name, coll_name)
    ns = f"{db_name}.{coll_name}"

    if pipeline and "$changeStream" in pipeline[0]:
        change_pipeline = pipeline[1:] if len(pipeline) > 1 else None
        stream = coll.watch(change_pipeline)
        cursor_id = ctx.cursor_registry.create_change_stream(ns, stream, batch_size)
        return {"cursor": {"id": cursor_id, "ns": ns, "firstBatch": []}, "ok": 1.0}

    docs = coll.get_all()
    coll_getter = lambda name: ctx.get_db(db_name).get_collection(name)
    result = Cursor(docs, collection_getter=coll_getter).aggregate(pipeline)
    result_docs = normalize_outbound_docs(result)

    cursor_id, first_batch = ctx.cursor_registry.create(ns, result_docs, batch_size)
    return {"cursor": {"id": cursor_id, "ns": ns, "firstBatch": first_batch}, "ok": 1.0}


@_register("mapReduce", "mapreduce")
def _cmd_map_reduce(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return make_error(
        "CommandNotSupported",
        "mapReduce is deprecated and not supported by smongo. Use aggregation instead.",
    )
