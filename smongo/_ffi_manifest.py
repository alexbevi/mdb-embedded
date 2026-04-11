"""ABI contract between Python and Rust.

Every name that ``CachedImports::from_python`` (``wire_context.rs``) resolves
via ``py.import`` / ``getattr`` is re-exported here.  Renaming or removing
anything in this file will break the Rust layer at import time.

The companion test ``tests/test_ffi_manifest.py`` validates that every
expected symbol is present and correctly typed.
"""

from __future__ import annotations

# ── smongo.wire.commands._registry ────────────────────────────────────
from smongo.wire.commands._registry import (
    _GIT_VERSION,
    _HANDLERS,
    _HELP,
    _SERVER_START,
    _TOPOLOGY_PROCESS_ID,
)

# ── smongo.wire.commands.users ────────────────────────────────────────
from smongo.wire.commands.users import _USER_STORE, _USER_STORE_LOCK

# ── smongo.audit (module reference, not individual attrs) ─────────────
import smongo.audit as audit_mod

__all__ = [
    "_GIT_VERSION",
    "_HANDLERS",
    "_HELP",
    "_SERVER_START",
    "_TOPOLOGY_PROCESS_ID",
    "_USER_STORE",
    "_USER_STORE_LOCK",
    "audit_mod",
]
