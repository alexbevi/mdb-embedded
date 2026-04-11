"""Validate that every FFI name Rust depends on is present and correctly typed.

This catches silent breakage when a Python symbol that Rust resolves via
``getattr`` is renamed, deleted, or changes type.  The manifest is the
single source of truth for the Rust ↔ Python ABI boundary.

``TestABIDrift`` goes further: it reads ``wire_context.rs`` and asserts
that the set of ``#[py(attr = "...")]`` annotations matches ``__all__``
exactly — catching drift in either direction.
"""

from __future__ import annotations

import re
import threading
import types
from pathlib import Path

import smongo.wire.commands  # triggers @_register side effects
from smongo._ffi_manifest import (
    _GIT_VERSION,
    _HANDLERS,
    _HELP,
    _SERVER_START,
    _TOPOLOGY_PROCESS_ID,
    _USER_STORE,
    _USER_STORE_LOCK,
    audit_mod,
)


class TestFFIManifestPresence:
    """Every symbol must be importable and non-None."""

    def test_git_version_is_str(self):
        assert isinstance(_GIT_VERSION, str), f"expected str, got {type(_GIT_VERSION)}"

    def test_handlers_is_dict(self):
        assert isinstance(_HANDLERS, dict), f"expected dict, got {type(_HANDLERS)}"
        assert len(_HANDLERS) > 0, "_HANDLERS should not be empty"

    def test_help_is_dict(self):
        assert isinstance(_HELP, dict), f"expected dict, got {type(_HELP)}"
        assert len(_HELP) > 0, "_HELP should not be empty"

    def test_server_start_is_float(self):
        assert isinstance(_SERVER_START, float), f"expected float, got {type(_SERVER_START)}"
        assert _SERVER_START > 0, "_SERVER_START should be a positive timestamp"

    def test_topology_process_id_is_not_none(self):
        assert _TOPOLOGY_PROCESS_ID is not None

    def test_user_store_is_dict(self):
        assert isinstance(_USER_STORE, dict), f"expected dict, got {type(_USER_STORE)}"

    def test_user_store_lock_is_lock(self):
        assert isinstance(_USER_STORE_LOCK, type(threading.Lock()))

    def test_audit_mod_is_module(self):
        assert isinstance(audit_mod, types.ModuleType)
        assert audit_mod.__name__ == "smongo.audit"


class TestFFIManifestCompleteness:
    """__all__ must list every symbol Rust depends on."""

    EXPECTED_NAMES = {
        "_GIT_VERSION",
        "_HANDLERS",
        "_HELP",
        "_SERVER_START",
        "_TOPOLOGY_PROCESS_ID",
        "_USER_STORE",
        "_USER_STORE_LOCK",
        "audit_mod",
    }

    def test_all_exports_match(self):
        from smongo._ffi_manifest import __all__

        assert set(__all__) == self.EXPECTED_NAMES, (
            f"__all__ mismatch: missing={self.EXPECTED_NAMES - set(__all__)}, "
            f"extra={set(__all__) - self.EXPECTED_NAMES}"
        )

    def test_no_none_values(self):
        """Every exported name must resolve to a non-None value."""
        import smongo._ffi_manifest as m

        for name in self.EXPECTED_NAMES:
            val = getattr(m, name, None)
            assert val is not None, f"{name} is None in _ffi_manifest"


# Path from repo root to the Rust source that consumes the manifest.
_WIRE_CONTEXT_RS = (
    Path(__file__).resolve().parent.parent
    / "rust"
    / "smongo-py"
    / "src"
    / "wire_context.rs"
)


class TestABIDrift:
    """Rust ``#[py(attr = "...")]`` annotations must match ``__all__`` exactly."""

    @staticmethod
    def _rust_py_attr_names() -> set[str]:
        """Parse wire_context.rs and return every name declared via #[py(attr = "...")]."""
        src = _WIRE_CONTEXT_RS.read_text()
        return set(re.findall(r'#\[py\(attr\s*=\s*"([^"]+)"\)\]', src))

    def test_no_rust_names_missing_from_manifest(self):
        """Every name Rust declares via #[py(attr = "...")] must be in __all__."""
        from smongo._ffi_manifest import __all__

        rust_names = self._rust_py_attr_names()
        manifest_names = set(__all__)
        missing = rust_names - manifest_names
        assert not missing, (
            f"Rust declares #[py(attr)] names not in __all__: {sorted(missing)}"
        )

    def test_no_manifest_names_unused_by_rust(self):
        """Every name in __all__ must have a corresponding #[py(attr = "...")] in Rust."""
        from smongo._ffi_manifest import __all__

        rust_names = self._rust_py_attr_names()
        manifest_names = set(__all__)
        unused = manifest_names - rust_names
        assert not unused, (
            f"__all__ exports names Rust never declares: {sorted(unused)}"
        )
