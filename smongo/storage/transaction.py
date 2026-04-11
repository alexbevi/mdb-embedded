"""Thread-local marker for multi-document wire transactions (redb / engine).

``TransactionSession`` is constructed with the Rust :class:`RedbLocalClient` (or a
Python :class:`~smongo.storage.redb_engine.RedbClient`, which delegates to it).
``activate`` / ``commit`` / ``rollback`` map to ``wire_txn_*`` on that client so
all collection operations on the connection share one engine transaction.
"""

from __future__ import annotations

import threading
from typing import Any

_txn_state = threading.local()


def _rust_client(local_client: Any) -> Any:
    """Resolve the PyO3 ``RedbLocalClient`` from a ``RedbClient`` wrapper or pass-through."""
    inner = getattr(local_client, "_rust_client", None)
    return inner if inner is not None else local_client


class TransactionSession:
    """Binds ``wire_txn_begin`` / ``commit`` / ``abort`` on ``RedbLocalClient``."""

    def __init__(self, local_client: Any) -> None:
        self._rust = _rust_client(local_client)

    def activate(self) -> None:
        """Start a wire transaction and publish the client for diagnostics."""
        self._rust.wire_txn_begin()
        _txn_state.session = self._rust

    def deactivate(self) -> None:
        _txn_state.session = None

    def commit(self) -> None:
        try:
            self._rust.wire_txn_commit()
        finally:
            self.deactivate()

    def rollback(self) -> None:
        try:
            self._rust.wire_txn_abort()
        finally:
            self.deactivate()


def get_active_txn_session() -> Any | None:
    """Return the thread-local RedbLocalClient while a wire transaction is active, else ``None``."""
    return getattr(_txn_state, "session", None)
