"""Shared command dispatch infrastructure -- handler registry, counters.

The dispatch loop lives in Rust (``rs_dispatch`` in ``_smongo_core``).
This module owns:

- The Python handler registry (``_HANDLERS``) and ``@_register`` decorator
- The thin ``dispatch()`` wrapper that forwards to ``rs_dispatch``
- Opcounter helpers that delegate to Rust (``_inc_counter``, ``_get_opcounters``)

**Opcounters** are owned by Rust (``wire_dispatch.rs``).  ``_inc_counter``
and ``_get_opcounters`` are thin wrappers so Python fallback handlers and
``serverStatus`` can interact with the same counters without importing
``_smongo_core`` everywhere.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from bson import ObjectId as BsonObjectId

from .._types import CommandDoc, DocSequences, ResponseDoc
from ..context import ConnectionContext, get_git_version
from ..errors import error_response, make_error

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
    # Rust-only commands (no Python fallback handler)
    "grantRolesToUser": "Grant roles to an existing database user",
    "revokeRolesFromUser": "Revoke roles from an existing database user",
    "createSearchIndex": "Create a search index on a collection",
    "createSearchIndexes": "Create one or more search indexes on a collection",
    "listSearchIndexes": "List search indexes on a collection",
    "dropIndexes": "Drop one or more indexes on a collection",
    "buildInfo": "Return build environment and version information",
    "buildinfo": "Return build environment and version information (alias)",
    "hostInfo": "Return information about the host system",
    "whatsmyuri": "Return the client IP address as seen by the server",
    "findandmodify": "Atomically find and modify a single document (alias)",
    "getlasterror": "Return the result of the previous write operation (alias)",
    "mapreduce": "Deprecated -- use aggregation pipeline instead (alias)",
    "connectionStatus": "Return authentication and authorization info for the connection",
    "getLog": "Return recent log entries",
    "getFreeMonitoringStatus": "Return free monitoring status",
    "getCmdLineOpts": "Return server command-line options",
    "listDatabases": "List all databases and their sizes",
    "listCollections": "List all collections in a database",
    "collMod": "Modify collection options",
    "setFreeMonitoring": "Enable or disable free monitoring",
    "lockInfo": "Return information about current locks",
    "listCommands": "List all available commands and their help text",
    "connPoolStats": "Return connection pool statistics",
}

_SERVER_START = time.time()
_TOPOLOGY_PROCESS_ID = BsonObjectId()
_GIT_VERSION = get_git_version()

# Opcounters live in Rust (wire_dispatch.rs).  These thin wrappers avoid
# scattering ``from smongo._smongo_core import ...`` across every handler.
# The underlying functions are cached after first resolution.

_inc_counter_fn: Any = None
_get_opcounters_fn: Any = None


def _inc_counter(name: str) -> None:
    """Increment a Rust-owned opcounter (insert/query/update/delete/command)."""
    global _inc_counter_fn
    if _inc_counter_fn is None:
        from smongo._smongo_core import inc_counter
        _inc_counter_fn = inc_counter
    _inc_counter_fn(name)


def _get_opcounters() -> dict[str, int]:
    """Return a snapshot of all Rust opcounters as a plain dict."""
    global _get_opcounters_fn
    if _get_opcounters_fn is None:
        from smongo._smongo_core import get_opcounters
        _get_opcounters_fn = get_opcounters
    return _get_opcounters_fn()


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


# ── Exception-type dict for rs_dispatch ──────────────────────────────

_EXCEPTION_TYPES: dict[str, Any] = {}


def _init_exception_types() -> None:
    """Populate *_EXCEPTION_TYPES* lazily (avoids import-time cycles)."""
    from smongo._smongo_core import (
        DuplicateKeyError,
        NamespaceError,
        TooManySessions,
        TransactionError,
        ValidationError,
    )

    _EXCEPTION_TYPES.update(
        {
            "NamespaceError": NamespaceError,
            "TooManySessions": TooManySessions,
            "TransactionError": TransactionError,
            "DuplicateKeyError": DuplicateKeyError,
            "ValidationError": ValidationError,
        }
    )

    try:
        from smongo._compat import StorageError

        _EXCEPTION_TYPES["StorageError"] = StorageError
    except ImportError:
        pass


# ── Unified dispatch (delegates to Rust rs_dispatch) ─────────────────

# Lazily resolved singletons to avoid import-time coupling.
_rs_dispatch: Any = None
_audit_mod: Any = None


def dispatch(
    ctx: ConnectionContext, command_doc: CommandDoc, doc_sequences: DocSequences | None = None
) -> ResponseDoc:
    """Route a command document to the appropriate Rust or Python handler.

    Delegates to Rust ``rs_dispatch`` which checks the Rust handler registry
    first and falls back to ``_HANDLERS`` only when no Rust handler exists
    for the command name.

    .. warning::

       ``command_doc`` **may be mutated** by ``rs_dispatch`` (e.g. ``$db``
       is injected if absent).  Callers must not rely on the dict being
       unchanged after this call.
    """
    global _rs_dispatch, _audit_mod

    if _rs_dispatch is None:
        from smongo._smongo_core import rs_dispatch
        _rs_dispatch = rs_dispatch
    if not _EXCEPTION_TYPES:
        _init_exception_types()
    if _audit_mod is None:
        import smongo.audit
        _audit_mod = smongo.audit

    return _rs_dispatch(  # type: ignore[return-value]
        ctx,
        _HANDLERS,
        command_doc,
        doc_sequences,
        make_error,
        error_response,
        _EXCEPTION_TYPES,
        _audit_mod,
    )
