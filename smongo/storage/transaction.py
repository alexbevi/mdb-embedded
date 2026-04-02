"""Thread-local WiredTiger transaction session for multi-document transactions.

When a wire-protocol ``startTransaction`` command is received, a new
WiredTiger session is opened and ``begin_transaction()`` is called on it.
The session is stored in a thread-local so that all collection operations
on the same thread use this shared transactional session instead of their
own per-collection session.  ``commit()`` / ``rollback()`` map directly
to the WiredTiger session calls, giving true ACID guarantees.
"""

from __future__ import annotations

import threading
from typing import Any

_txn_state = threading.local()


class TransactionSession:
    """A WiredTiger session with an open transaction spanning multiple collections."""

    def __init__(self, conn: Any) -> None:
        self._session = conn.open_session()
        self._session.begin_transaction()

    @property
    def session(self) -> Any:
        """The underlying WiredTiger session."""
        return self._session

    def activate(self) -> None:
        """Set this as the active transaction for the current thread."""
        _txn_state.session = self._session

    def deactivate(self) -> None:
        """Clear the thread-local transaction session."""
        _txn_state.session = None

    def commit(self) -> None:
        """Commit the transaction and close the session."""
        self._session.commit_transaction()
        self.deactivate()
        self._session.close()

    def rollback(self) -> None:
        """Roll back the transaction and close the session."""
        self._session.rollback_transaction()
        self.deactivate()
        self._session.close()


def get_active_txn_session() -> Any | None:
    """Return the thread-local transactional WT session, or ``None``."""
    return getattr(_txn_state, "session", None)
