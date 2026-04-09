"""Shared command dispatch infrastructure -- handler registry, dispatch loop, counters."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from bson import Binary, Int64, Timestamp
from bson import ObjectId as BsonObjectId

from ..._compat import StorageError as _StorageError
from ...index import DuplicateKeyError
from ...schema import ValidationError
from .._types import CommandDoc, DocSequences, ResponseDoc
from ..context import (
    ConnectionContext,
    NamespaceError,
    get_git_version,
)
from ..errors import error_response, make_error
from ..sessions import TooManySessions
from ..transactions import TransactionError

log = logging.getLogger("smongo.wire.commands")

MAX_BSON_OBJECT_SIZE = 16 * 1024 * 1024
MAX_MESSAGE_SIZE = 48 * 1024 * 1024
MAX_WRITE_BATCH_SIZE = 100_000

_CommandHandler = Callable[[ConnectionContext, CommandDoc, DocSequences], ResponseDoc]
_HANDLERS: dict[str, _CommandHandler] = {}
_HELP: dict[str, str] = {
    "hello": "Handshake command that returns server capabilities",
    "ismaster": "Legacy handshake (alias for hello)",
    "isMaster": "Legacy handshake (alias for hello)",
    "ping": "Confirm the server is running",
    "saslStart": "Start SASL authentication (not supported in embedded mode)",
    "saslContinue": "Continue SASL authentication (not supported in embedded mode)",
    "logout": "End the current authenticated session",
    "startSession": "Start a new logical session",
    "endSessions": "End specified logical sessions",
    "refreshSessions": "Refresh sessions to prevent timeout",
    "killSessions": "Kill specified logical sessions",
    "killAllSessions": "Kill all sessions on the server",
    "abortTransaction": "Abort the current multi-document transaction with rollback",
    "commitTransaction": "Commit the current multi-document transaction with checkpoint",
    "startTransaction": "Start a new multi-document transaction on a session",
    "find": "Query a collection and return matching documents",
    "insert": "Insert one or more documents into a collection",
    "update": "Update one or more documents in a collection",
    "delete": "Delete one or more documents from a collection",
    "count": "Count the number of documents matching a query",
    "distinct": "Return distinct values for a field across documents",
    "getMore": "Retrieve the next batch from a cursor",
    "killCursors": "Close one or more server-side cursors",
    "listIndexes": "List all indexes on a collection",
    "createIndexes": "Create one or more indexes on a collection",
    "aggregate": "Execute an aggregation pipeline on a collection",
    "create": "Explicitly create a collection or view",
    "drop": "Drop a collection and all its indexes",
    "dropDatabase": "Drop a database and all its collections",
    "explain": "Return the execution plan for a query",
    "renameCollection": "Rename a collection within or across databases",
    "mapReduce": "Deprecated -- use aggregation pipeline instead",
    "findAndModify": "Atomically find and modify a single document",
    "getLastError": "Return the result of the previous write operation",
    "getnonce": "Generate a random nonce for authentication",
    "fsync": "Flush all pending writes and persist embedded storage",
    "usersInfo": "Return information about database users",
    "rolesInfo": "Return information about database roles",
    "createUser": "Create a new database user",
    "dropUser": "Remove a database user",
    "updateUser": "Update an existing database user",
    "bulkWrite": "Execute multiple write operations in a single batch",
    "getParameter": "Retrieve the value of a server parameter",
    "setParameter": "Set the value of a server parameter",
    "estimatedDocumentCount": "Return an approximate document count (fast path)",
    "collStats": "Return storage statistics for a collection",
    "dbStats": "Return storage statistics for a database",
    "validate": "Run integrity checks on a collection and its indexes",
    "serverStatus": "Return comprehensive server status and statistics",
    "currentOp": "Return information about in-progress operations",
    "killOp": "Request cancellation of a running operation",
    "features": "Return server feature flags",
    "logRotate": "Trigger a log file rotation",
    "top": "Return per-collection operation timing statistics",
    "profile": "Set the operation profiling level",
    "setProfilingLevel": "Set the operation profiling level (alias)",
    "system.profile": "Read captured profiling entries",
    "shardingState": "Return whether the server participates in sharding",
    "replSetGetConfig": "Return the replica set configuration",
    "replSetGetStatus": "Return the replica set status",
    "client.sync": "Return the sync manager status",
    "reIndex": "Rebuild all indexes on a collection",
    "compact": "Compact a collection to reclaim disk space",
    "dataSize": "Return the data size for a namespace",
}

_SERVER_START = time.time()
_TOPOLOGY_PROCESS_ID = BsonObjectId()
_GIT_VERSION = get_git_version()

_opcounters: dict[str, int] = {
    "insert": 0,
    "query": 0,
    "update": 0,
    "delete": 0,
    "getmore": 0,
    "command": 0,
}
_opcounters_lock = threading.Lock()

_EPOCH = Timestamp(0, 0)
_logical_clock = 0
_clock_lock = threading.Lock()


def _next_timestamp() -> Timestamp:
    global _logical_clock
    with _clock_lock:
        _logical_clock += 1
        return Timestamp(int(time.time()), _logical_clock)


def _inc_counter(name: str) -> None:
    with _opcounters_lock:
        _opcounters[name] = _opcounters.get(name, 0) + 1


def _register(*names: str, help: str = "") -> Callable[[_CommandHandler], _CommandHandler]:
    """Decorator that registers a handler under one or more command names."""

    def decorator(fn: _CommandHandler) -> _CommandHandler:
        doc = help or (fn.__doc__ or "").split("\n")[0].strip()
        for n in names:
            _HANDLERS[n] = fn
            if doc:
                _HELP[n] = doc
        return fn

    return decorator


_OP_KIND_MAP: dict[str, str] = {
    "find": "query",
    "aggregate": "query",
    "count": "query",
    "distinct": "query",
    "getMore": "getmore",
    "insert": "insert",
    "update": "update",
    "delete": "remove",
    "findAndModify": "command",
    "findandmodify": "command",
    "bulkWrite": "command",
}

_TOP_BUCKET_MAP: dict[str, str] = {
    "find": "queries",
    "aggregate": "queries",
    "count": "queries",
    "distinct": "queries",
    "getMore": "getmore",
    "insert": "insert",
    "update": "update",
    "delete": "remove",
}


def dispatch(
    ctx: ConnectionContext, command_doc: CommandDoc, doc_sequences: DocSequences | None = None
) -> ResponseDoc:
    """Route a command document to the appropriate handler."""
    if "$db" not in command_doc:
        command_doc["$db"] = "test"

    lsid = command_doc.get("lsid")
    if lsid is not None:
        ctx.session_registry.touch(lsid)

    cmd_name = next(iter(command_doc))
    handler = _HANDLERS.get(cmd_name)
    if handler is None:
        return make_error("CommandNotFound", f"no such command: '{cmd_name}'")

    _inc_counter("command")

    db_name = command_doc.get("$db", "test")
    coll_hint = command_doc.get(cmd_name, "")
    ns = f"{db_name}.{coll_hint}" if isinstance(coll_hint, str) and coll_hint else db_name

    op_kind = _OP_KIND_MAP.get(cmd_name, "command")
    op_id = ctx.op_tracker.start_op(op_kind, ns, command_doc, ctx.connection_id)
    t0 = time.monotonic()

    try:
        resp = handler(ctx, command_doc, doc_sequences or {})
    except NamespaceError as exc:
        resp = make_error("InvalidNamespace", str(exc))
    except TooManySessions as exc:
        resp = error_response(261, "TooManyLogicalSessions", str(exc))
    except TransactionError as exc:
        resp = error_response(251, "NoSuchTransaction", str(exc))
    except DuplicateKeyError as exc:
        resp = error_response(11000, "DuplicateKey", str(exc))
    except ValidationError as exc:
        resp = error_response(121, "DocumentValidationFailure", str(exc))
    except NotImplementedError as exc:
        resp = make_error("CommandNotSupported", str(exc))
    except _StorageError as exc:
        log.exception("Storage engine error in command '%s'", cmd_name)
        resp = error_response(1, "InternalError", str(exc))
    except (
        KeyError,
        TypeError,
        ValueError,
        IndexError,
        RuntimeError,
        OSError,
        AttributeError,
    ) as exc:
        log.exception("Unhandled error in command '%s'", cmd_name)
        resp = error_response(1, "InternalError", str(exc))
    finally:
        elapsed_us = int((time.monotonic() - t0) * 1_000_000)
        ctx.op_tracker.finish_op(op_id)
        top_bucket = _TOP_BUCKET_MAP.get(cmd_name, "commands")
        ctx.top_stats.record(ns, top_bucket, elapsed_us)
        ctx.profiler.log(
            op_kind,
            ns,
            elapsed_us // 1000,
            command=command_doc,
            plan_summary=ctx.last_plan_summary,
        )
        ctx.last_plan_summary = ""

    ts = _next_timestamp()
    resp["operationTime"] = ts
    resp["$clusterTime"] = {
        "clusterTime": ts,
        "signature": {"hash": Binary(b"\x00" * 20), "keyId": Int64(0)},
    }

    return resp
