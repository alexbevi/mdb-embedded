from __future__ import annotations

import os
from typing import Any

from bson import Int64

from .._types import CommandDoc, DocSequences, ResponseDoc
from ..context import ConnectionContext
from ..errors import error_response, make_error
from ._registry import _HANDLERS, _HELP, _register, log


@_register("currentOp")
def _cmd_current_op(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    ops = ctx.op_tracker.active_ops()
    all_flag = cmd.get("$all", False)
    if not all_flag:
        own_id = ctx.connection_id
        ops = [o for o in ops if o["connectionId"] == own_id]
    return {"inprog": ops, "ok": 1.0}


@_register("killOp")
def _cmd_kill_op(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    op_id = cmd.get("op")
    if op_id is None:
        return make_error("InvalidOptions", "killOp requires 'op' field")
    killed = ctx.op_tracker.kill_op(int(op_id))
    if not killed:
        return {"info": f"operation {op_id} not found", "ok": 1.0}
    return {"info": f"attempting to kill op {op_id}", "ok": 1.0}


@_register("connPoolStats", help="Return connection pool statistics")
def _cmd_conn_pool_stats(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    snap = ctx.conn_counter.snapshot()
    return {
        "numClientConnections": snap["current"],
        "numAScopedConnections": 0,
        "totalInUse": snap["current"],
        "totalAvailable": snap["available"],
        "totalCreated": snap["totalCreated"],
        "totalRefreshing": 0,
        "pools": {},
        "ok": 1.0,
    }


@_register("features")
def _cmd_features(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return {
        "oidMachine": os.getpid(),
        "ok": 1.0,
    }


@_register("logRotate")
def _cmd_log_rotate(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    import logging as _logging

    for handler in _logging.root.handlers:
        if hasattr(handler, "doRollover"):
            handler.doRollover()
    log.info("logRotate executed")
    return {"ok": 1.0}


@_register("top")
def _cmd_top(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return {"totals": ctx.top_stats.snapshot(), "ok": 1.0}


@_register("profile")
def _cmd_profile(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    new_level = cmd.get("profile")
    slow_ms = cmd.get("slowms")

    old_level = ctx.profiler.level
    old_slow = ctx.profiler.slow_ms

    if isinstance(new_level, int) and new_level in (0, 1, 2):
        ctx.profiler.level = new_level
    if isinstance(slow_ms, int):
        ctx.profiler.slow_ms = slow_ms

    return {"was": old_level, "slowms": old_slow, "ok": 1.0}


@_register("setProfilingLevel")
def _cmd_set_profiling(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return _cmd_profile(ctx, cmd, seqs)


@_register("system.profile")
def _cmd_read_profile(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    limit = cmd.get("limit", 100)
    entries = ctx.profiler.get_entries(limit)
    return {
        "cursor": {"id": Int64(0), "ns": "admin.system.profile", "firstBatch": entries},
        "ok": 1.0,
    }


@_register("shardingState")
def _cmd_sharding_state(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return {"enabled": False, "ok": 1.0}


@_register("replSetGetConfig")
def _cmd_repl_get_config(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    return make_error("NotPrimaryOrSecondary", "smongo is standalone, not a replica set member")


@_register("replSetGetStatus")
def _cmd_repl_status(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    if ctx.sync_mgr:
        try:
            return {
                "set": "smongo",
                "members": [{"_id": 0, "name": "localhost", "state": 1, "stateStr": "PRIMARY"}],
                "sync": ctx.sync_mgr.status(),
                "ok": 1.0,
            }
        except (RuntimeError, OSError, ValueError, KeyError):
            pass
    return make_error("NotPrimaryOrSecondary", "replSetGetStatus requires sync to be configured")


@_register("setFreeMonitoring", help="Enable or disable free monitoring")
def _cmd_set_free_monitoring(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    action = cmd.get("action", "")
    if action not in ("enable", "disable"):
        return make_error("InvalidOptions", f"invalid action: {action!r}")
    ctx.free_monitoring.set(action)
    log.info("setFreeMonitoring: %s", action)
    return {"ok": 1.0}


@_register("lockInfo", help="Return information about currently held locks")
def _cmd_lock_info(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    ops = ctx.op_tracker.active_ops()
    lock_entries = [
        {
            "resourceId": o["ns"],
            "granted": [{"mode": "IS" if o["op"] in ("query", "getmore") else "IX"}],
            "pending": [],
        }
        for o in ops
    ]
    return {"lockInfo": lock_entries, "ok": 1.0}


@_register("listCommands", help="List all registered commands with help text")
def _cmd_list_commands(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    commands: dict[str, dict[str, Any]] = {}
    admin_commands = {
        "fsync",
        "serverStatus",
        "hostInfo",
        "top",
        "logRotate",
        "getParameter",
        "setParameter",
        "currentOp",
        "killOp",
        "listDatabases",
        "replSetGetStatus",
        "replSetGetConfig",
        "shardingState",
        "connPoolStats",
        "getCmdLineOpts",
    }
    for name, _handler in _HANDLERS.items():
        commands[name] = {
            "help": _HELP.get(name, ""),
            "adminOnly": name in admin_commands,
            "slaveOk": True,
        }
    return {"commands": commands, "ok": 1.0}


@_register("client.sync")
def _cmd_client_sync(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    """Custom diagnostic: return full sync manager status."""
    if ctx.sync_mgr:
        try:
            return {"sync": ctx.sync_mgr.status(), "ok": 1.0}
        except (RuntimeError, OSError, ValueError, KeyError) as exc:
            return error_response(1, "InternalError", str(exc))
    return {"sync": None, "ok": 1.0}
