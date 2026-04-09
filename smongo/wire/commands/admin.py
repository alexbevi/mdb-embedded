from __future__ import annotations

import os
import platform
import resource
import secrets
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from bson import Binary, Int64

from ..._compat import StorageError as _StorageError
from .._types import CommandDoc, DocSequences, ResponseDoc
from ..bson_codec import normalize_inbound
from ..context import ConnectionContext, get_virtual_memory_mb
from ..errors import error_response, make_error
from ._registry import (
    _SERVER_START,
    _opcounters,
    _opcounters_lock,
    _register,
    log,
)


@_register("listDatabases", help="List all databases with sizes")
def _cmd_list_databases(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    seen_dbs = set(ctx.list_known_dbs())

    if not seen_dbs:
        seen_dbs.add("test")

    name_only = cmd.get("nameOnly", False)
    total_size = 0
    databases: list[dict[str, Any]] = []
    for db_name in sorted(seen_dbs):
        if name_only:
            databases.append({"name": db_name})
            continue
        db_size = 0
        try:
            db = ctx.get_db(db_name)
            for coll_name in db.list_collection_names():
                try:
                    coll = db.get_collection(coll_name)
                    stats = coll.storage_stats()
                    db_size += stats.get("storageSize", 0) + stats.get("dataSize", 0)
                except (KeyError, RuntimeError, OSError):
                    pass
        except (KeyError, RuntimeError, OSError):
            pass
        total_size += db_size
        databases.append({"name": db_name, "sizeOnDisk": db_size, "empty": db_size == 0})

    resp: ResponseDoc = {"databases": databases, "ok": 1.0}
    if not name_only:
        resp["totalSize"] = total_size
    return resp


@_register("listCollections", help="List all collections in a database")
def _cmd_list_collections(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    db = ctx.get_db(db_name)
    coll_names = db.list_collection_names()
    name_only = cmd.get("nameOnly", False)
    filter_doc = cmd.get("filter")

    result: list[dict[str, Any]] = []
    for n in coll_names:
        entry: dict[str, Any] = {"name": n, "type": "collection"}
        if not name_only:
            entry["options"] = {}
            entry["info"] = {
                "readOnly": False,
                "uuid": Binary(uuid.uuid5(uuid.NAMESPACE_DNS, f"{db_name}.{n}").bytes, subtype=4),
            }
        if filter_doc:
            match = True
            for fk, fv in filter_doc.items():
                if entry.get(fk) != fv:
                    match = False
                    break
            if not match:
                continue
        result.append(entry)

    ns = f"{db_name}.$cmd.listCollections"
    batch_size = cmd.get("cursor", {}).get("batchSize", 101)
    cursor_id, first_batch = ctx.cursor_registry.create(ns, result, batch_size)
    return {
        "cursor": {"id": Int64(cursor_id), "ns": ns, "firstBatch": first_batch},
        "ok": 1.0,
    }


@_register("create")
def _cmd_create_collection(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["create"]
    validator = cmd.get("validator")
    db = ctx.get_db(db_name)
    db.create_collection(coll_name, validator=validator)
    return {"ok": 1.0}


@_register("drop")
def _cmd_drop(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["drop"]
    n_indexes_was = 1
    try:
        db = ctx.get_db(db_name)
        coll = db.get_collection(coll_name)
        n_indexes_was = len(coll.list_indexes()) + 1
        for idx in list(coll.list_indexes()):
            try:
                coll.drop_index(idx["name"])
            except (KeyError, RuntimeError, OSError, ValueError, _StorageError):
                log.debug("drop index %s failed during collection drop", idx.get("name"))
        coll.delete({}, multi=True)
        db.drop_collection(coll_name)
    except (KeyError, RuntimeError, OSError, ValueError, _StorageError) as exc:
        log.debug("drop collection %s.%s failed: %s", db_name, coll_name, exc)
    return {"ns": f"{db_name}.{coll_name}", "nIndexesWas": n_indexes_was, "ok": 1.0}


@_register("dropDatabase")
def _cmd_drop_database(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    try:
        db = ctx.get_db(db_name)
        for coll_name in list(db._collections.keys()):
            try:
                coll = db.get_collection(coll_name)
                for idx in list(coll.list_indexes()):
                    try:
                        coll.drop_index(idx["name"])
                    except (KeyError, RuntimeError, OSError, ValueError, _StorageError):
                        pass
                coll.delete({}, multi=True)
            except (KeyError, RuntimeError, OSError, ValueError, TypeError, _StorageError):
                pass
        db._collections.clear()
    except (KeyError, RuntimeError, OSError, ValueError, TypeError, _StorageError) as exc:
        log.debug("dropDatabase %s failed: %s", db_name, exc)
    return {"dropped": db_name, "ok": 1.0}


@_register("explain")
def _cmd_explain(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    inner = cmd.get("explain", {})
    if isinstance(inner, dict):
        db_name = cmd.get("$db", "test")
        coll_name = inner.get("find") or inner.get("aggregate", "")
        if coll_name:
            coll = ctx.get_collection(db_name, coll_name)
            query = normalize_inbound(inner.get("filter") or {})
            return {"queryPlanner": coll.explain(query), "ok": 1.0}
    return {"queryPlanner": {"winningPlan": {"stage": "UNKNOWN"}}, "ok": 1.0}


@_register("collMod", help="Modify collection options (validator, TTL, etc.)")
def _cmd_coll_mod(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["collMod"]
    coll = ctx.get_collection(db_name, coll_name)

    validator = cmd.get("validator")
    if validator:
        schema = validator.get("$jsonSchema", validator)
        coll._validator = schema

    validation_level = cmd.get("validationLevel")
    if validation_level in ("off", "strict", "moderate"):
        if validation_level == "off":
            coll._validator = None

    idx_spec = cmd.get("index")
    if isinstance(idx_spec, dict) and ("keyPattern" in idx_spec or "name" in idx_spec):
        target_name = idx_spec.get("name")
        if not target_name and "keyPattern" in idx_spec:
            kp = idx_spec["keyPattern"]
            for idx in coll.list_indexes():
                idx_keys = dict(idx.get("keys", {}))
                if idx_keys == kp:
                    target_name = idx["name"]
                    break
        if target_name and "expireAfterSeconds" in idx_spec:
            idx_def = coll.index_mgr._indexes.get(target_name)
            if idx_def:
                idx_def.expire_after_seconds = idx_spec["expireAfterSeconds"]

    return {"ok": 1.0}


@_register("renameCollection")
def _cmd_rename_collection(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    src_ns = cmd["renameCollection"]
    dst_ns = cmd.get("to", "")
    drop_target = cmd.get("dropTarget", False)

    if "." not in src_ns or "." not in dst_ns:
        return make_error("InvalidNamespace", "invalid namespace for rename")

    src_db, src_coll = src_ns.split(".", 1)
    dst_db, dst_coll = dst_ns.split(".", 1)

    src_collection = ctx.get_collection(src_db, src_coll)
    docs = src_collection.get_all()

    dst_collection = ctx.get_collection(dst_db, dst_coll)
    existing = dst_collection.get_all()
    if existing and not drop_target:
        return make_error("NamespaceExists", f"target namespace {dst_ns} already exists")
    if existing and drop_target:
        dst_collection.delete({}, multi=True)

    for doc in docs:
        dst_collection.insert_one(doc)

    src_collection.delete({}, multi=True)
    src_dbobj = ctx.get_db(src_db)
    src_dbobj.drop_collection(src_coll)

    return {"ok": 1.0}


@_register("compact", help="Compact a collection to reclaim disk space")
def _cmd_compact(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["compact"]
    coll = ctx.get_collection(db_name, coll_name)
    before = coll.storage_stats().get("storageSize", 0)
    coll.compact()
    after = coll.storage_stats().get("storageSize", 0)
    freed = max(0, before - after)
    log.info("compact: %s.%s freed %d bytes", db_name, coll_name, freed)
    return {"bytesFreed": freed, "ok": 1.0}


@_register("collStats")
def _cmd_coll_stats(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["collStats"]
    coll = ctx.get_collection(db_name, coll_name)
    stats = coll.storage_stats()
    return {
        "ns": f"{db_name}.{coll_name}",
        "count": stats["count"],
        "size": stats["dataSize"],
        "avgObjSize": stats["dataSize"] // max(stats["count"], 1),
        "storageSize": stats["storageSize"],
        "nindexes": stats["nindexes"],
        "totalIndexSize": stats["totalIndexSize"],
        "indexSizes": stats["indexSizes"],
        "storageEngine": stats.get("storageEngine", {"name": "redb"}),
        "ok": 1.0,
    }


@_register("dbStats", help="Return storage statistics for a database")
def _cmd_db_stats(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    db = ctx.get_db(db_name)

    total_objects = 0
    total_data_size = 0
    total_storage_size = 0
    total_indexes = 0
    total_index_size = 0
    coll_count = 0

    for coll_name in db.list_collection_names():
        try:
            coll = db.get_collection(coll_name)
            stats = coll.storage_stats()
            total_objects += stats["count"]
            total_data_size += stats["dataSize"]
            total_storage_size += stats["storageSize"]
            total_indexes += stats["nindexes"]
            total_index_size += stats["totalIndexSize"]
            coll_count += 1
        except (KeyError, RuntimeError, OSError):
            pass

    return {
        "db": db_name,
        "collections": coll_count,
        "objects": total_objects,
        "avgObjSize": total_data_size // max(total_objects, 1),
        "dataSize": total_data_size,
        "storageSize": total_storage_size,
        "indexes": total_indexes,
        "indexSize": total_index_size,
        "scaleFactor": 1,
        "ok": 1.0,
    }


@_register("validate")
def _cmd_validate(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    coll_name = cmd["validate"]
    coll = ctx.get_collection(db_name, coll_name)
    result = coll.verify()
    return {
        "ns": f"{db_name}.{coll_name}",
        "nrecords": result["nrecords"],
        "nIndexes": result["nIndexes"],
        "valid": result["valid"],
        "errors": result["errors"],
        "warnings": result["warnings"],
        "ok": 1.0,
    }


@_register("serverStatus")
def _cmd_server_status(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    uptime = time.time() - _SERVER_START
    with _opcounters_lock:
        counters = dict(_opcounters)

    try:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if platform.system() == "Darwin":
            rss_mb = rss / (1024 * 1024)
        else:
            rss_mb = rss / 1024
    except (ValueError, OSError):
        rss_mb = 0

    storage_stats: dict[str, Any] = {}
    try:
        stats_fn = getattr(ctx.local_client, "connection_stats", None)
        if callable(stats_fn):
            storage_stats = dict(stats_fn())
    except (_StorageError, RuntimeError, OSError, KeyError):
        pass

    resp: ResponseDoc = {
        "host": platform.node() or ctx.address[0],
        "version": "7.0.0-smongo",
        "process": "smongo",
        "pid": os.getpid(),
        "uptime": uptime,
        "uptimeMillis": int(uptime * 1000),
        "uptimeEstimate": int(uptime),
        "localTime": datetime.now(UTC),
        "connections": ctx.conn_counter.snapshot(),
        "opcounters": counters,
        "mem": {
            "bits": 64,
            "resident": int(rss_mb),
            "virtual": get_virtual_memory_mb(),
            "supported": True,
            "note": "virtual memory reported via OS process stats",
        },
        "logicalSessionRecordCache": {
            "activeSessionsCount": ctx.session_registry.count,
        },
        "storageEngine": {
            "name": "redb",
            **storage_stats,
        },
        "ok": 1.0,
    }
    if ctx.sync_mgr:
        try:
            resp["sync"] = ctx.sync_mgr.status()
        except (RuntimeError, OSError, ValueError, KeyError):
            pass
    return resp


@_register("fsync")
def _cmd_fsync(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    lock = cmd.get("lock", False)
    _async = cmd.get("async", False)

    try:
        ctx.local_client.checkpoint()
    except (RuntimeError, OSError) as exc:
        log.warning("fsync / checkpoint failed: %s", exc)
        return error_response(1, "InternalError", f"checkpoint failed: {exc}")

    tables_flushed = 1

    resp: ResponseDoc = {"numFiles": tables_flushed, "ok": 1.0}
    if lock:
        resp["lockCount"] = 1
        resp["info"] = "fsync with lock is advisory only in embedded mode"
    if _async:
        resp["async"] = True
    return resp


@_register("getnonce")
def _cmd_getnonce(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    nonce = secrets.token_hex(8)
    return {"nonce": nonce, "ok": 1.0}


@_register("getParameter")
def _cmd_get_parameter(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    param = cmd.get("getParameter")
    all_params = cmd.get("allParameters", False)

    if all_params or param == "*":
        return {**ctx.param_store.get_all(), "ok": 1.0}

    if isinstance(param, str):
        val = ctx.param_store.get(param)
        if val is not None:
            return {param: val, "ok": 1.0}
        return make_error("InvalidOptions", f"no such parameter: {param!r}")

    return make_error("InvalidOptions", "getParameter requires a string parameter name or '*'")


@_register("setParameter")
def _cmd_set_parameter(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    changed: dict[str, Any] = {}
    for key, val in cmd.items():
        if key in ("setParameter", "$db", "lsid", "txnNumber", "$clusterTime"):
            continue
        old = ctx.param_store.get(key)
        ctx.param_store.set(key, val)
        changed[key] = {"was": old, "now": val}
        log.info("setParameter: %s = %r (was %r)", key, val, old)

    return {"was": changed, "ok": 1.0}
