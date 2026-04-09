from __future__ import annotations

from bson import Binary

from .._types import CommandDoc, DocSequences, ResponseDoc
from ..context import ConnectionContext
from ..errors import make_error
from ._registry import _register, log


@_register("startSession")
def _cmd_start_session(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    sid = ctx.session_registry.create()
    return {"id": {"id": Binary(sid.bytes, subtype=4)}, "timeoutMinutes": 30, "ok": 1.0}


@_register("endSessions")
def _cmd_end_sessions(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    session_ids = cmd.get("endSessions", [])
    ctx.session_registry.end(session_ids)
    return {"ok": 1.0}


@_register("refreshSessions")
def _cmd_refresh_sessions(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    session_ids = cmd.get("refreshSessions", [])
    ctx.session_registry.refresh(session_ids)
    return {"ok": 1.0}


@_register("killSessions")
def _cmd_kill_sessions(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    session_ids = cmd.get("killSessions", [])
    ctx.session_registry.kill(session_ids)
    return {"ok": 1.0}


@_register("killAllSessions", help="Kill all sessions on the server")
def _cmd_kill_all_sessions(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    ctx.session_registry.expire_all()
    return {"ok": 1.0}


@_register("abortTransaction")
def _cmd_abort_txn(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    lsid = cmd.get("lsid")
    if lsid is None:
        return make_error("InvalidOptions", "abortTransaction requires lsid")
    rolled_back = ctx.abort_transaction(lsid)
    log.info("abortTransaction: rolled back %d operations", rolled_back)
    return {"ok": 1.0}


@_register("commitTransaction")
def _cmd_commit_txn(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    lsid = cmd.get("lsid")
    if lsid is None:
        return make_error("InvalidOptions", "commitTransaction requires lsid")
    ctx.commit_transaction(lsid)
    log.info("commitTransaction: checkpoint forced")
    return {"ok": 1.0}


@_register("startTransaction")
def _cmd_start_txn(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    lsid = cmd.get("lsid")
    if lsid is None:
        return make_error("InvalidOptions", "startTransaction requires lsid")
    txn = ctx.start_transaction(lsid)
    log.info("startTransaction: txnNumber=%d on engine session", txn.txn_number)
    return {"ok": 1.0}
