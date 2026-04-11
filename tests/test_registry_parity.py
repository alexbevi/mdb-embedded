"""Verify that the Rust and Python command registries stay in sync.

This test catches four classes of drift:

1. A Rust handler exists but ``_HELP`` has no entry (user-visible gap in
   ``listCommands``).
2. A command is registered in Python ``_HANDLERS`` but has no Rust handler
   *and* no ``_HELP`` entry.
3. ``_HELP`` references a command name that neither registry knows about
   (stale entry after a rename/removal).
4. A Python handler exists with no Rust equivalent and is not in the
   explicit ``_KNOWN_PYTHON_ONLY`` allowlist (hard failure — prevents
   silent regression when porting commands to Rust).
"""

from __future__ import annotations

import smongo.wire.commands  # triggers @_register side effects
from smongo._smongo_core import rust_handler_names as _rust_handler_names_list
from smongo.wire.commands._registry import _HANDLERS, _HELP


def _rust_handler_names() -> set[str]:
    """Return the set of command names registered in the Rust RUST_HANDLERS map.

    Uses the ``rust_handler_names()`` function exported from ``_smongo_core``,
    which reads directly from the Rust ``RUST_HANDLERS`` LazyLock HashMap.
    """
    return set(_rust_handler_names_list())


class TestRegistryParity:
    """Ensure RUST_HANDLERS, _HANDLERS, and _HELP stay aligned."""

    def test_every_rust_handler_has_help(self):
        rust = _rust_handler_names()
        missing = rust - set(_HELP)
        assert not missing, (
            f"Rust handlers without _HELP entries (will be invisible in listCommands): {sorted(missing)}"
        )

    def test_every_python_handler_has_help(self):
        missing = set(_HANDLERS) - set(_HELP)
        assert not missing, (
            f"Python handlers without _HELP entries: {sorted(missing)}"
        )

    def test_no_stale_help_entries(self):
        rust = _rust_handler_names()
        all_known = rust | set(_HANDLERS)
        stale = set(_HELP) - all_known
        assert not stale, (
            f"_HELP entries for commands that exist in neither registry: {sorted(stale)}"
        )

    def test_rust_covers_all_python_handlers(self):
        """Every Python handler must either have a Rust equivalent or be
        explicitly listed in ``_KNOWN_PYTHON_ONLY``.

        If this test fails, either port the handler to Rust or add it to the
        allowlist below with a comment explaining why it stays in Python.
        """
        rust = _rust_handler_names()
        python_only = set(_HANDLERS) - rust

        # Explicit allowlist — add entries here with a reason when a command
        # intentionally stays Python-only.  This set should shrink over time.
        _KNOWN_PYTHON_ONLY: set[str] = set()

        unexpected = python_only - _KNOWN_PYTHON_ONLY
        assert not unexpected, (
            f"Python-only handlers with no Rust equivalent (and not in allowlist): "
            f"{sorted(unexpected)}. Either port to Rust or add to _KNOWN_PYTHON_ONLY "
            f"in this test with an explanation."
        )
