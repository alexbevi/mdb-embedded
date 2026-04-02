"""Transaction state and commit/abort helpers using WT-native sessions.

Each active transaction holds a :class:`~smongo.storage.TransactionSession`
whose underlying WiredTiger session spans all collection operations on the
current thread.  ``commit`` / ``abort`` map directly to WiredTiger
``commit_transaction`` / ``rollback_transaction``.
"""

from __future__ import annotations

import enum
import time

from ..storage import LocalClient
from ..storage.transaction import TransactionSession


class TransactionState(enum.Enum):
    NONE = "none"
    ACTIVE = "active"
    COMMITTED = "committed"
    ABORTED = "aborted"


class SessionTransaction:
    """Per-session transaction state backed by a real WiredTiger transaction."""

    __slots__ = ("start_time", "state", "txn_number", "txn_session")

    def __init__(self, txn_number: int, txn_session: TransactionSession) -> None:
        self.state = TransactionState.ACTIVE
        self.txn_number = txn_number
        self.start_time = time.monotonic()
        self.txn_session = txn_session


class TransactionError(RuntimeError):
    """Raised on illegal transaction state transitions."""


def commit_active_transaction(local_client: LocalClient, txn: SessionTransaction | None) -> None:
    """Commit the active WT-native transaction."""
    if txn is None or txn.state != TransactionState.ACTIVE:
        raise TransactionError("No transaction in progress")
    txn.txn_session.commit()
    txn.state = TransactionState.COMMITTED


def abort_active_transaction(txn: SessionTransaction | None) -> int:
    """Roll back the active WT-native transaction.

    Returns 0 (kept for backward-compat; the WT rollback is atomic).
    """
    if txn is None or txn.state != TransactionState.ACTIVE:
        raise TransactionError("No transaction in progress")
    txn.txn_session.rollback()
    txn.state = TransactionState.ABORTED
    return 0
