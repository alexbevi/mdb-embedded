from __future__ import annotations

import threading
from typing import Any

from bson import ObjectId as BsonObjectId

from ._registry import _register, log
from .._types import CommandDoc, DocSequences, ResponseDoc
from ..context import ConnectionContext
from ..errors import make_error

_USER_STORE: dict[str, dict[str, Any]] = {}
_USER_STORE_LOCK = threading.Lock()


@_register("usersInfo")
def _cmd_users_info(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    target = cmd.get("usersInfo")
    with _USER_STORE_LOCK:
        if isinstance(target, str):
            key = f"{db_name}.{target}"
            user = _USER_STORE.get(key)
            return {"users": [user] if user else [], "ok": 1.0}
        if isinstance(target, dict):
            u = target.get("user", "")
            d = target.get("db", db_name)
            key = f"{d}.{u}"
            user = _USER_STORE.get(key)
            return {"users": [user] if user else [], "ok": 1.0}
        if target == 1 or target is True:
            users = [u for k, u in _USER_STORE.items() if k.startswith(f"{db_name}.")]
            return {"users": users, "ok": 1.0}
        return {"users": list(_USER_STORE.values()), "ok": 1.0}


@_register("rolesInfo")
def _cmd_roles_info(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    builtin_roles = [
        {"role": "read", "db": "admin", "isBuiltin": True, "roles": [], "inheritedRoles": []},
        {"role": "readWrite", "db": "admin", "isBuiltin": True, "roles": [], "inheritedRoles": []},
        {"role": "dbAdmin", "db": "admin", "isBuiltin": True, "roles": [], "inheritedRoles": []},
        {"role": "dbOwner", "db": "admin", "isBuiltin": True, "roles": [], "inheritedRoles": []},
        {"role": "root", "db": "admin", "isBuiltin": True, "roles": [], "inheritedRoles": []},
    ]
    show_builtin = False
    ri = cmd.get("rolesInfo")
    if isinstance(ri, dict):
        show_builtin = ri.get("showBuiltinRoles", False)
    elif isinstance(ri, int) and ri == 1:
        show_builtin = cmd.get("showBuiltinRoles", False)

    return {"roles": builtin_roles if show_builtin else [], "ok": 1.0}


@_register("createUser")
def _cmd_create_user(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    user = cmd.get("createUser", "")
    if not user:
        return make_error("InvalidOptions", "createUser requires a username")
    key = f"{db_name}.{user}"
    roles = cmd.get("roles", [])
    with _USER_STORE_LOCK:
        if key in _USER_STORE:
            return make_error("DuplicateKey", f"User \"{user}@{db_name}\" already exists")
        _USER_STORE[key] = {
            "_id": f"{db_name}.{user}",
            "userId": BsonObjectId(),
            "user": user,
            "db": db_name,
            "roles": roles,
            "mechanisms": ["SCRAM-SHA-256"],
        }
    log.info("createUser: %s@%s", user, db_name)
    return {"ok": 1.0}


@_register("dropUser")
def _cmd_drop_user(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    user = cmd.get("dropUser", "")
    key = f"{db_name}.{user}"
    with _USER_STORE_LOCK:
        if key not in _USER_STORE:
            return make_error("UserNotFound", f"User \"{user}@{db_name}\" not found")
        del _USER_STORE[key]
    log.info("dropUser: %s@%s", user, db_name)
    return {"ok": 1.0}


@_register("updateUser")
def _cmd_update_user(ctx: ConnectionContext, cmd: CommandDoc, seqs: DocSequences) -> ResponseDoc:
    db_name = cmd.get("$db", "test")
    user = cmd.get("updateUser", "")
    key = f"{db_name}.{user}"
    with _USER_STORE_LOCK:
        if key not in _USER_STORE:
            return make_error("UserNotFound", f"User \"{user}@{db_name}\" not found")
        if "roles" in cmd:
            _USER_STORE[key]["roles"] = cmd["roles"]
        if "mechanisms" in cmd:
            _USER_STORE[key]["mechanisms"] = cmd["mechanisms"]
    log.info("updateUser: %s@%s", user, db_name)
    return {"ok": 1.0}
