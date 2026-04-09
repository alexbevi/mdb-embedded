from __future__ import annotations

import os
import platform
import sys
from datetime import UTC, datetime

from bson import Int64

from .._types import CommandDoc, DocSequences, ResponseDoc
from ..context import ConnectionContext, get_total_memory_mb
from ..errors import make_error
from ..msg import _COMPRESSOR_IDS, available_compressors
from ._registry import (
    _GIT_VERSION,
    _TOPOLOGY_PROCESS_ID,
    MAX_BSON_OBJECT_SIZE,
    MAX_MESSAGE_SIZE,
    MAX_WRITE_BATCH_SIZE,
    _register,
)


@_register("hello", "ismaster", "isMaster")
def _cmd_hello(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    compressors: list[str] = []
    client_compressors = cmd.get("compression", [])
    server_compressors = available_compressors()
    for c in client_compressors:
        if c in server_compressors:
            compressors.append(c)

    if compressors and ctx.compressor_id is None:
        ctx.compressor_id = _COMPRESSOR_IDS.get(compressors[0])

    resp: ResponseDoc = {
        "ismaster": True,
        "isWritablePrimary": True,
        "topologyVersion": {"processId": _TOPOLOGY_PROCESS_ID, "counter": Int64(0)},
        "maxBsonObjectSize": MAX_BSON_OBJECT_SIZE,
        "maxMessageSizeBytes": MAX_MESSAGE_SIZE,
        "maxWriteBatchSize": MAX_WRITE_BATCH_SIZE,
        "localTime": datetime.now(UTC),
        "logicalSessionTimeoutMinutes": 30,
        "connectionId": ctx.connection_id,
        "minWireVersion": 0,
        "maxWireVersion": 21,
        "readOnly": False,
        "ok": 1.0,
    }
    if compressors:
        resp["compression"] = compressors
    if cmd.get("helloOk"):
        resp["helloOk"] = True
    if "saslSupportedMechs" in cmd:
        resp["saslSupportedMechs"] = ["SCRAM-SHA-1", "SCRAM-SHA-256"]
    return resp


@_register("ping")
def _cmd_ping(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return {"ok": 1.0}


@_register("buildInfo", "buildinfo", help="Return build summary for this server")
def _cmd_build_info(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return {
        "version": "7.0.0-smongo",
        "gitVersion": _GIT_VERSION,
        "sysInfo": f"{platform.system()} {platform.release()} {platform.machine()}",
        "versionArray": [7, 0, 0, 0],
        "bits": 64,
        "modules": ["embedded", "redb"],
        "allocator": "system",
        "javascriptEngine": "none",
        "openssl": {"running": "disabled"},
        "buildEnvironment": {
            "cc": "",
            "cxx": "",
            "ccflags": "",
            "cxxflags": "",
            "target_arch": platform.machine(),
            "target_os": platform.system().lower(),
        },
        "ok": 1.0,
    }


@_register("getLog", help="Retrieve recent log entries")
def _cmd_get_log(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    log_type = cmd.get("getLog", "global")
    if log_type == "global" or log_type == "*":
        lines, total = ctx.log_buffer.get_lines()
        return {"totalLinesWritten": total, "log": lines, "ok": 1.0}
    if log_type == "startupWarnings":
        return {"totalLinesWritten": 0, "log": [], "ok": 1.0}
    return make_error("InvalidOptions", f"unknown getLog type: {log_type}")


@_register("getFreeMonitoringStatus", help="Report the state of free monitoring")
def _cmd_free_monitoring(
    ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences
) -> ResponseDoc:
    return {"state": ctx.free_monitoring.state, "ok": 1.0}


@_register("hostInfo", help="Return system information about the host machine")
def _cmd_host_info(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    uname = platform.uname()
    return {
        "system": {
            "currentTime": datetime.now(UTC),
            "hostname": platform.node(),
            "cpuAddrSize": 64,
            "cpuArch": platform.machine(),
            "numCores": os.cpu_count() or 1,
            "memSizeMB": get_total_memory_mb(),
        },
        "os": {
            "type": uname.system,
            "name": platform.platform(),
            "version": uname.release,
        },
        "extra": {
            "pageSize": os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096,
        },
        "ok": 1.0,
    }


@_register("getCmdLineOpts", help="Return the command line options used to start this process")
def _cmd_cmdline_opts(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return {"argv": sys.argv, "parsed": {}, "ok": 1.0}


@_register("connectionStatus", help="Return information about the current connection")
def _cmd_conn_status(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return {
        "authInfo": {"authenticatedUsers": [], "authenticatedUserRoles": []},
        "ok": 1.0,
    }


@_register("whatsmyuri", help="Return the client's IP address and port")
def _cmd_whatsmyuri(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return {"you": f"{ctx.address[0]}:{ctx.address[1]}", "ok": 1.0}


@_register("saslStart")
def _cmd_sasl_start(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return make_error(
        "AuthenticationFailed",
        "smongo embedded mode does not support authentication. "
        "Connect without credentials (remove username/password from your URI).",
    )


@_register("saslContinue")
def _cmd_sasl_continue(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return make_error(
        "AuthenticationFailed",
        "smongo embedded mode does not support authentication. "
        "Connect without credentials (remove username/password from your URI).",
    )


@_register("logout")
def _cmd_logout(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    return {"ok": 1.0}
