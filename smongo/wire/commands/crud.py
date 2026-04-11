"""CRUD command handlers (find, insert, update, delete, ...).

NOTE: All commands in this module have Rust-native implementations that
take priority at runtime via ``rs_dispatch``.  These Python handlers serve
as fallback implementations and reference documentation.  Changes here
will NOT affect normal wire protocol behavior -- update the corresponding
Rust handler in ``rust/smongo-py/src/wire_commands/crud.rs`` instead.
"""

from __future__ import annotations

import time
from typing import Any

from bson import Int64

from ...aggregation import Cursor
from ...index import DuplicateKeyError
from ...query import apply_update, get_value
from ...schema import ValidationError
from ...storage import UpdateResult
from .._types import CommandDoc, DocSequences, ResponseDoc
from ..bson_codec import normalize_inbound, normalize_outbound, normalize_outbound_docs
from ..context import ConnectionContext, LastWriteResult
from ..errors import make_error
from ._registry import MAX_WRITE_BATCH_SIZE, _inc_counter, _register


@_register("find", help="Query a collection and return matching documents")
def _cmd_find(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    _inc_counter("query")
    db_name = cmd.get("$db", "test")
    coll_name = cmd["find"]
    coll = ctx.get_collection(db_name, coll_name)

    filter_doc = normalize_inbound(cmd.get("filter") or {})
    projection = cmd.get("projection")
    sort_spec = cmd.get("sort")
    skip_val = cmd.get("skip", 0)
    limit_val = cmd.get("limit", 0)
    batch_size = cmd.get("batchSize", 101)
    single_batch = cmd.get("singleBatch", False)

    plan = coll.explain(filter_doc)
    ctx.last_plan_summary = plan.get("plan", "COLLSCAN")
    if plan.get("index"):
        ctx.last_plan_summary = f"IXSCAN {{ {plan['index']} }}"

    ns = f"{db_name}.{coll_name}"

    # Fast path: no sort -- keep the iterator lazy, apply skip/limit/projection
    # as iterator wrappers, and register via create_from_iter.
    if not sort_spec and not single_batch and batch_size > 0:
        import itertools

        raw_iter = coll.find_streaming(filter_doc)
        it: Any = raw_iter
        if skip_val:
            it = itertools.islice(it, skip_val, None)
        if limit_val:
            it = itertools.islice(it, limit_val)
        if projection:
            norm_proj = normalize_inbound(projection)
            it = (_apply_projection_single(normalize_outbound(d), norm_proj) for d in it)
        else:
            it = (normalize_outbound(d) for d in it)

        cursor_id, first_batch = ctx.cursor_registry.create_from_iter(ns, it, batch_size)
        return {
            "cursor": {"id": Int64(cursor_id), "ns": ns, "firstBatch": first_batch},
            "ok": 1.0,
        }

    # Materialized path: sort requires seeing all docs.
    docs = coll.find_streaming(filter_doc)
    coll_getter = lambda name: ctx.get_db(db_name).get_collection(name)
    cursor = Cursor(docs, collection_getter=coll_getter)

    if sort_spec:
        keys = list(sort_spec.items()) if isinstance(sort_spec, dict) else sort_spec
        cursor = cursor.sort(keys)
    if skip_val:
        cursor = cursor.skip(skip_val)
    if limit_val:
        cursor = cursor.limit(limit_val)
    if projection:
        cursor = cursor.projection(normalize_inbound(projection))

    result_docs = normalize_outbound_docs(cursor.to_list())

    if single_batch or batch_size <= 0:
        return {"cursor": {"id": Int64(0), "ns": ns, "firstBatch": result_docs}, "ok": 1.0}

    cursor_id, first_batch = ctx.cursor_registry.create(ns, result_docs, batch_size)
    return {"cursor": {"id": Int64(cursor_id), "ns": ns, "firstBatch": first_batch}, "ok": 1.0}


@_register("insert")
def _cmd_insert(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    _inc_counter("insert")
    db_name = cmd.get("$db", "test")
    coll_name = cmd["insert"]
    coll = ctx.get_collection(db_name, coll_name)
    ordered = cmd.get("ordered", True)

    documents = seqs.get("documents") or cmd.get("documents") or []
    if len(documents) > MAX_WRITE_BATCH_SIZE:
        return make_error(
            "InvalidLength",
            f"batch size {len(documents)} exceeds limit {MAX_WRITE_BATCH_SIZE}",
        )

    inserted = 0
    write_errors: list[dict[str, Any]] = []

    for i, doc in enumerate(documents):
        try:
            norm = normalize_inbound(doc)
            coll.insert_one(norm)
            inserted += 1
        except DuplicateKeyError as exc:
            write_errors.append({"index": i, "code": 11000, "errmsg": str(exc)})
            if ordered:
                break
        except ValidationError as exc:
            write_errors.append({"index": i, "code": 121, "errmsg": str(exc)})
            if ordered:
                break
        except (KeyError, TypeError, ValueError, RuntimeError, OSError) as exc:
            write_errors.append({"index": i, "code": 1, "errmsg": str(exc)})
            if ordered:
                break

    err_msg = write_errors[0]["errmsg"] if write_errors else None
    ctx.last_write = LastWriteResult(
        op="insert", n=inserted, err=err_msg, write_errors=write_errors
    )

    resp: ResponseDoc = {"n": inserted, "ok": 1.0}
    if write_errors:
        resp["writeErrors"] = write_errors
    return resp


@_register("update")
def _cmd_update(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    _inc_counter("update")
    db_name = cmd.get("$db", "test")
    coll_name = cmd["update"]
    coll = ctx.get_collection(db_name, coll_name)
    ordered = cmd.get("ordered", True)

    updates = seqs.get("updates") or cmd.get("updates") or []
    if len(updates) > MAX_WRITE_BATCH_SIZE:
        return make_error(
            "InvalidLength",
            f"batch size {len(updates)} exceeds limit {MAX_WRITE_BATCH_SIZE}",
        )

    n = 0
    n_modified = 0
    upserted: list[dict[str, Any]] = []
    write_errors: list[dict[str, Any]] = []

    for i, spec in enumerate(updates):
        try:
            q = normalize_inbound(spec.get("q", {}))
            u = normalize_inbound(spec.get("u", {}))
            multi = spec.get("multi", False)
            upsert = spec.get("upsert", False)

            has_operators = isinstance(u, dict) and any(k.startswith("$") for k in u)

            if has_operators:
                result = coll.update(q, u, multi=multi)
            else:
                target = coll.find_one(q)
                if target:
                    coll.find_one_and_replace({"_id": target["_id"]}, u)
                    result = UpdateResult(1, 1)
                else:
                    result = UpdateResult(0, 0)

            if result.modified_count > 0:
                n += result.modified_count
                n_modified += result.modified_count
            elif upsert and result.matched_count == 0:
                if has_operators:
                    new_doc = dict(q)
                    for k in list(new_doc.keys()):
                        if isinstance(new_doc[k], dict) and any(
                            op.startswith("$") for op in new_doc[k]
                        ):
                            del new_doc[k]
                    apply_update(new_doc, u)
                else:
                    new_doc = dict(u)

                from ...objectid import ObjectId

                if "_id" not in new_doc:
                    new_doc["_id"] = ObjectId()
                coll.insert_one(new_doc)
                upserted.append({"index": i, "_id": new_doc["_id"]})
                n += 1
            else:
                n += result.matched_count
        except DuplicateKeyError as exc:
            write_errors.append({"index": i, "code": 11000, "errmsg": str(exc)})
            if ordered:
                break
        except ValidationError as exc:
            write_errors.append({"index": i, "code": 121, "errmsg": str(exc)})
            if ordered:
                break
        except (KeyError, TypeError, ValueError, RuntimeError, OSError) as exc:
            write_errors.append({"index": i, "code": 1, "errmsg": str(exc)})
            if ordered:
                break

    err_msg = write_errors[0]["errmsg"] if write_errors else None
    ctx.last_write = LastWriteResult(
        op="update", n=n, n_modified=n_modified, err=err_msg, write_errors=write_errors
    )

    resp: ResponseDoc = {"n": n, "nModified": n_modified, "ok": 1.0}
    if upserted:
        resp["upserted"] = upserted
    if write_errors:
        resp["writeErrors"] = write_errors
    return resp


@_register("delete")
def _cmd_delete(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    _inc_counter("delete")
    db_name = cmd.get("$db", "test")
    coll_name = cmd["delete"]
    coll = ctx.get_collection(db_name, coll_name)
    ordered = cmd.get("ordered", True)

    deletes = seqs.get("deletes") or cmd.get("deletes") or []
    if len(deletes) > MAX_WRITE_BATCH_SIZE:
        return make_error(
            "InvalidLength",
            f"batch size {len(deletes)} exceeds limit {MAX_WRITE_BATCH_SIZE}",
        )

    n = 0
    write_errors: list[dict[str, Any]] = []

    for i, spec in enumerate(deletes):
        try:
            q = normalize_inbound(spec.get("q", {}))
            limit = spec.get("limit", 0)
            multi = limit == 0

            result = coll.delete(q, multi=multi)
            n += result.deleted_count
        except (KeyError, TypeError, ValueError, RuntimeError, OSError) as exc:
            write_errors.append({"index": i, "code": 1, "errmsg": str(exc)})
            if ordered:
                break

    err_msg = write_errors[0]["errmsg"] if write_errors else None
    ctx.last_write = LastWriteResult(op="delete", n=n, err=err_msg, write_errors=write_errors)

    resp: ResponseDoc = {"n": n, "ok": 1.0}
    if write_errors:
        resp["writeErrors"] = write_errors
    return resp


@_register("count")
def _cmd_count(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    _inc_counter("query")
    db_name = cmd.get("$db", "test")
    coll_name = cmd["count"]
    coll = ctx.get_collection(db_name, coll_name)
    query = normalize_inbound(cmd.get("query") or {})
    return {"n": coll.count(query), "ok": 1.0}


@_register("distinct")
def _cmd_distinct(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    _inc_counter("query")
    db_name = cmd.get("$db", "test")
    coll_name = cmd["distinct"]
    coll = ctx.get_collection(db_name, coll_name)
    key = cmd.get("key", "")
    query = normalize_inbound(cmd.get("query") or {})

    seen: list[Any] = []
    for doc in coll.find_streaming(query):
        v = get_value(doc, key)
        if v not in seen:
            seen.append(v)
    return {"values": seen, "ok": 1.0}


@_register("getMore")
def _cmd_get_more(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    _inc_counter("getmore")
    cursor_id = cmd["getMore"]
    coll_name = cmd.get("collection", "")
    db_name = cmd.get("$db", "test")
    batch_size = cmd.get("batchSize")

    is_tailable = ctx.cursor_registry.is_tailable(cursor_id)

    if is_tailable:
        max_await = cmd.get("maxTimeMS", 1000)
        new_id, batch = ctx.cursor_registry.get_more_change_stream(cursor_id, batch_size, max_await)
        if batch is None:
            return make_error("CursorNotFound", f"cursor id {cursor_id} not found")
        ns = f"{db_name}.{coll_name}"
        out_batch = normalize_outbound_docs(batch) if batch else []
        return {"cursor": {"id": Int64(new_id or 0), "ns": ns, "nextBatch": out_batch}, "ok": 1.0}

    new_id, batch = ctx.cursor_registry.get_more(cursor_id, batch_size)
    if batch is None:
        return make_error("CursorNotFound", f"cursor id {cursor_id} not found")

    ns = f"{db_name}.{coll_name}"
    return {"cursor": {"id": Int64(new_id or 0), "ns": ns, "nextBatch": batch}, "ok": 1.0}


@_register("killCursors")
def _cmd_kill_cursors(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    cursor_ids = cmd.get("cursors", [])
    killed = ctx.cursor_registry.kill(cursor_ids)
    not_found = [cid for cid in cursor_ids if cid not in killed]
    return {
        "cursorsKilled": killed,
        "cursorsNotFound": not_found,
        "cursorsAlive": [],
        "cursorsUnknown": [],
        "ok": 1.0,
    }


@_register("findAndModify", "findandmodify")
def _cmd_find_and_modify(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["findAndModify"] if "findAndModify" in cmd else cmd["findandmodify"]
    coll = ctx.get_collection(db_name, coll_name)

    query = normalize_inbound(cmd.get("query") or {})
    sort_spec = cmd.get("sort")
    remove = cmd.get("remove", False)
    update_spec = cmd.get("update")
    new = cmd.get("new", False)
    upsert = cmd.get("upsert", False)
    fields = cmd.get("fields")
    return_doc = "after" if new else "before"

    if remove:
        if sort_spec:
            matching = coll.find(query)
            if matching:
                matching = _apply_sort(matching, sort_spec)
                doc = coll.find_one_and_delete({"_id": matching[0]["_id"]})
            else:
                doc = None
        else:
            doc = coll.find_one_and_delete(query)
    elif update_spec:
        update_spec = normalize_inbound(update_spec)
        has_operators = any(k.startswith("$") for k in update_spec)

        if sort_spec:
            matching = coll.find(query)
            matching = _apply_sort(matching, sort_spec)
        else:
            first = coll.find_one(query)
            matching = [first] if first else []

        if matching:
            target_filter = {"_id": matching[0]["_id"]}
            if has_operators:
                doc = coll.find_one_and_update(
                    target_filter, update_spec, return_document=return_doc
                )
            else:
                doc = coll.find_one_and_replace(
                    target_filter, update_spec, return_document=return_doc
                )
        elif upsert:
            if has_operators:
                new_doc = dict(query)
                for k in list(new_doc.keys()):
                    if isinstance(new_doc[k], dict) and any(
                        op.startswith("$") for op in new_doc[k]
                    ):
                        del new_doc[k]
                apply_update(new_doc, update_spec)
            else:
                new_doc = dict(update_spec)

            from ...objectid import ObjectId

            if "_id" not in new_doc:
                new_doc["_id"] = ObjectId()
            coll.insert_one(new_doc)
            doc = new_doc if new else None
        else:
            doc = None
    else:
        return make_error("InvalidOptions", "findAndModify requires 'remove' or 'update'")

    resp: ResponseDoc = {"ok": 1.0}
    if doc is not None:
        out = normalize_outbound(doc)
        if fields:
            out = _apply_projection_single(out, fields)
        resp["value"] = out
    else:
        resp["value"] = None
    resp["lastErrorObject"] = {
        "n": 1 if doc is not None else 0,
        "updatedExisting": doc is not None and not upsert,
    }
    return resp


def _apply_sort(
    docs: list[dict[str, Any]], sort_spec: dict[str, int] | list[tuple[str, int]]
) -> list[dict[str, Any]]:
    """Sort a list of docs by the given sort specification."""
    if not docs or not sort_spec:
        return docs
    if isinstance(sort_spec, dict):
        keys = list(sort_spec.items())
    else:
        keys = sort_spec
    for key_name, direction in reversed(keys):
        docs = sorted(
            docs,
            key=lambda d, k=key_name: (get_value(d, k) is None, get_value(d, k) or ""),  # type: ignore[misc]
            reverse=(direction == -1),
        )
    return docs


def _apply_projection_single(doc: dict[str, Any], fields: dict[str, Any]) -> dict[str, Any]:
    """Apply a field projection — delegates to the single Rust implementation."""
    if not fields or not isinstance(fields, dict):
        return doc
    from smongo._smongo_core import apply_projection

    return apply_projection(doc, fields)


@_register("bulkWrite")
def _cmd_bulk_write(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    _inc_counter("command")
    db_name = cmd.get("$db", "test")
    ordered = cmd.get("ordered", True)
    ops = cmd.get("ops", [])
    ns_info = cmd.get("nsInfo", [])
    n_inserted = 0
    n_matched = 0
    n_modified = 0
    n_deleted = 0
    n_upserted = 0
    write_errors: list[dict[str, Any]] = []

    def _resolve_ns(ns_idx: int) -> tuple[str, str]:
        ns = ns_info[ns_idx]["ns"] if ns_idx < len(ns_info) else f"{db_name}.unknown"
        parts = ns.split(".", 1)
        return (parts[0], parts[1]) if len(parts) == 2 else (db_name, parts[0])

    for i, op in enumerate(ops):
        try:
            if "insert" in op:
                coll_db, coll_name = _resolve_ns(op["insert"])
                coll = ctx.get_collection(coll_db, coll_name)
                doc = normalize_inbound(op.get("document", {}))
                coll.insert_one(doc)
                n_inserted += 1

            elif "update" in op:
                coll_db, coll_name = _resolve_ns(op["update"])
                coll = ctx.get_collection(coll_db, coll_name)
                q = normalize_inbound(op.get("filter", {}))
                u = normalize_inbound(op.get("updateMods", {}))
                multi = op.get("multi", False)
                upsert = op.get("upsert", False)
                upd_result = coll.update(q, u, multi=multi, upsert=upsert)
                if upd_result.upserted_id is not None:
                    n_upserted += 1
                else:
                    n_matched += upd_result.matched_count
                    n_modified += upd_result.modified_count

            elif "delete" in op:
                coll_db, coll_name = _resolve_ns(op["delete"])
                coll = ctx.get_collection(coll_db, coll_name)
                q = normalize_inbound(op.get("filter", {}))
                multi = op.get("multi", True)
                del_result = coll.delete(q, multi=multi)
                n_deleted += del_result.deleted_count

        except DuplicateKeyError as exc:
            write_errors.append({"index": i, "code": 11000, "errmsg": str(exc)})
            if ordered:
                break
        except ValidationError as exc:
            write_errors.append({"index": i, "code": 121, "errmsg": str(exc)})
            if ordered:
                break
        except (KeyError, TypeError, ValueError, RuntimeError, OSError) as exc:
            write_errors.append({"index": i, "code": 1, "errmsg": str(exc)})
            if ordered:
                break

    ctx.last_write = LastWriteResult(
        op="bulkWrite",
        n=n_inserted + n_modified + n_deleted,
        n_modified=n_modified,
        err=write_errors[0]["errmsg"] if write_errors else None,
        write_errors=write_errors,
    )

    resp: ResponseDoc = {
        "ok": 1.0,
        "nInserted": n_inserted,
        "nMatched": n_matched,
        "nModified": n_modified,
        "nDeleted": n_deleted,
        "nUpserted": n_upserted,
    }
    if write_errors:
        resp["writeErrors"] = write_errors
    return resp


@_register("getLastError", "getlasterror")
def _cmd_get_last_error(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    lw = ctx.last_write
    if lw is None:
        return {"ok": 1.0, "err": None, "n": 0}
    resp: ResponseDoc = {
        "ok": 1.0,
        "err": lw.err,
        "n": lw.n,
        "nModified": lw.n_modified,
        "connectionId": ctx.connection_id,
    }
    if lw.upserted_id is not None:
        resp["upserted"] = lw.upserted_id
    if lw.write_errors:
        resp["writeErrors"] = lw.write_errors
    return resp


@_register("estimatedDocumentCount")
def _cmd_estimated_doc_count(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    _inc_counter("query")
    db_name = cmd.get("$db", "test")
    coll_name = cmd["estimatedDocumentCount"]
    coll = ctx.get_collection(db_name, coll_name)
    return {"n": coll.count_fast(), "ok": 1.0}


@_register("dataSize", help="Return the data size for a namespace")
def _cmd_data_size(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    key_pattern = cmd.get("keyPattern")
    min_key = cmd.get("min")
    max_key = cmd.get("max")
    ns_raw = cmd.get("dataSize", "")
    if "." in ns_raw:
        db_name, coll_name = ns_raw.split(".", 1)
    else:
        db_name = cmd.get("$db", "test")
        coll_name = ns_raw or ""

    if not coll_name:
        return make_error("InvalidNamespace", "dataSize requires a valid namespace")

    coll = ctx.get_collection(db_name, coll_name)
    t0 = time.monotonic()

    if key_pattern and (min_key or max_key):
        query: dict[str, Any] = {}
        for field in key_pattern:
            bounds: dict[str, Any] = {}
            if min_key and field in min_key:
                bounds["$gte"] = min_key[field]
            if max_key and field in max_key:
                bounds["$lt"] = max_key[field]
            if bounds:
                query[field] = bounds
        doc_iter = coll.find_streaming(query) if query else coll.find_streaming()
        from bson import encode as _bson_encode

        size = 0
        n = 0
        for n, d in enumerate(doc_iter, 1):  # noqa: B007
            size += len(_bson_encode(normalize_outbound(d)))
        estimate = False
    else:
        size = coll.data_size_bytes()
        n = coll.count_fast()
        estimate = False

    millis = int((time.monotonic() - t0) * 1000)
    return {
        "size": size,
        "numObjects": n,
        "millis": millis,
        "estimate": estimate,
        "ok": 1.0,
    }
