"""Transaction state and commit/abort helpers using WT-native sessions.

Each active transaction holds a reference to a TransactionSession whose
underlying WiredTiger session spans all collection operations on the
current thread.  ``commit`` / ``abort`` map directly to WiredTiger
``commit_transaction`` / ``rollback_transaction``.

Implementation lives in Rust (_smongo_core); this module re-exports it.
"""

from smongo._smongo_core import (
    SessionTransaction,
    TransactionError,
    TransactionState,
    abort_active_transaction,
    commit_active_transaction,
)

__all__ = [
    "SessionTransaction",
    "TransactionError",
    "TransactionState",
    "abort_active_transaction",
    "commit_active_transaction",
]
