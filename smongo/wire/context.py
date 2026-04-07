"""
Per-connection server context.

Each TCP connection gets its own ConnectionContext which caches LocalDB
instances (and thus WiredTiger sessions) for the lifetime of the connection.
Also tracks compression negotiation, logical sessions, per-session transaction
state with undo-journal rollback, per-connection write result tracking,
active-operation monitoring, and an operation profiler.

Helper classes (NamespaceError, validate_namespace, LastWriteResult,
ParameterStore, ConnectionCounter, FreeMonitoringState) live in Rust
(_smongo_core).  ConnectionContext and LogBuffer remain in Python.
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
from itertools import count
from typing import TYPE_CHECKING, Any

from smongo._smongo_core import (
    ConnectionCounter,
    FreeMonitoringState,
    LastWriteResult,
    NamespaceError,
    ParameterStore,
    validate_namespace,
)

from ..storage import LocalClient, LocalCollection, LocalDB
from ..storage.transaction import TransactionSession as _StorageTxnSession
from . import transactions as _txn
from .cursors import CursorRegistry
from .profiler import (
    OperationTracker,
    Profiler,
    TopStats,
)
from .sessions import SessionRegistry, TooManySessions  # noqa: F401
from .transactions import (
    SessionTransaction,
    TransactionError,
    TransactionState,
)

__all__ = [
    "ConnectionContext",
    "ConnectionCounter",
    "FreeMonitoringState",
    "LastWriteResult",
    "LogBuffer",
    "NamespaceError",
    "ParameterStore",
    "validate_namespace",
]

if TYPE_CHECKING:
    from ..sync import SyncManager


# =====================================================================
# Log buffer (getLog)
# =====================================================================


class LogBuffer(logging.Handler):
    """Logging handler that captures recent log messages for ``getLog``."""

    def __init__(self, max_lines: int = 1024) -> None:
        super().__init__()
        self._lines: list[str] = []
        self._max = max_lines
        self._lock = threading.Lock()
        self._total = 0
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)s  %(name)s  %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        line = self.format(record)
        with self._lock:
            self._lines.append(line)
            self._total += 1
            if len(self._lines) > self._max:
                self._lines = self._lines[-self._max :]

    def get_lines(self) -> tuple[list[str], int]:
        with self._lock:
            return list(self._lines), self._total

    def install(self) -> None:
        """Attach this handler to the root logger (idempotent)."""
        root = logging.getLogger()
        if self not in root.handlers:
            root.addHandler(self)


# =====================================================================
# System memory helpers
# =====================================================================


def get_total_memory_mb() -> int:
    """Return total physical memory in MB using OS-level APIs."""
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        return (pages * page_size) // (1024 * 1024)
    except (ValueError, OSError, AttributeError):
        pass
    try:
        result = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True,
            text=True,
            timeout=2,
        )
        return int(result.stdout.strip()) // (1024 * 1024)
    except (OSError, ValueError, subprocess.SubprocessError, FileNotFoundError):
        pass
    return 0


def get_virtual_memory_mb() -> int:
    """Return the process virtual memory size in MB."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmSize:"):
                    return int(line.split()[1]) // 1024
    except (OSError, ValueError):
        pass
    try:
        result = subprocess.run(
            ["ps", "-o", "vsz=", "-p", str(os.getpid())],
            capture_output=True,
            text=True,
            timeout=2,
        )
        return int(result.stdout.strip()) // 1024
    except (OSError, ValueError, subprocess.SubprocessError, FileNotFoundError):
        return 0


def get_git_version() -> str:
    """Return the current git HEAD hash, or a descriptive fallback."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (OSError, subprocess.SubprocessError, FileNotFoundError):
        pass
    return "embedded"


# =====================================================================
# Connection context
# =====================================================================


class ConnectionContext:
    """Holds per-connection state shared across all commands on that connection."""

    def __init__(
        self,
        local_client: LocalClient,
        connection_id: int,
        address: tuple[str, int],
        cursor_registry: CursorRegistry,
        sync_mgr: SyncManager | None = None,
        session_registry: SessionRegistry | None = None,
        op_tracker: OperationTracker | None = None,
        param_store: ParameterStore | None = None,
        top_stats: TopStats | None = None,
        profiler: Profiler | None = None,
        log_buffer: LogBuffer | None = None,
        conn_counter: ConnectionCounter | None = None,
        free_monitoring: FreeMonitoringState | None = None,
    ) -> None:
        self.local_client = local_client
        self.connection_id = connection_id
        self.address = address
        self.cursor_registry = cursor_registry
        self.sync_mgr = sync_mgr
        self._dbs: dict[str, LocalDB] = {}
        self.compressor_id: int | None = None
        self.session_registry = session_registry or SessionRegistry()
        self.op_tracker = op_tracker or OperationTracker()
        self.param_store = param_store or ParameterStore()
        self.top_stats = top_stats or TopStats()
        self.profiler = profiler or Profiler()
        self.log_buffer = log_buffer or LogBuffer()
        self.conn_counter = conn_counter or ConnectionCounter()
        self.free_monitoring = free_monitoring or FreeMonitoringState()

        self.last_write: LastWriteResult | None = None
        self.last_plan_summary: str = ""
        self._txn_sessions: dict[str, SessionTransaction] = {}
        self._txn_number_gen = count(1)

    def get_db(self, db_name: str) -> LocalDB:
        if db_name not in self._dbs:
            self._dbs[db_name] = self.local_client.get_db(db_name)
        return self._dbs[db_name]

    def get_collection(self, db_name: str, coll_name: str) -> LocalCollection:
        validate_namespace(db_name, coll_name)
        return self.get_db(db_name).get_collection(coll_name)

    def list_known_dbs(self) -> list[str]:
        """Return database names this connection has accessed."""
        return list(self._dbs.keys())

    # -- transaction state machine -------------------------------------

    @staticmethod
    def _session_key(lsid: Any) -> str:
        if isinstance(lsid, dict):
            return str(lsid.get("id", ""))
        return str(lsid)

    def start_transaction(self, lsid: Any) -> SessionTransaction:
        """Begin a new WT-native transaction on the given logical session."""
        key = self._session_key(lsid)
        existing = self._txn_sessions.get(key)
        if existing and existing.state == TransactionState.ACTIVE:
            raise TransactionError("Transaction already in progress on this session")
        storage_txn = _StorageTxnSession(self.local_client.conn)
        storage_txn.activate()
        txn = SessionTransaction(next(self._txn_number_gen), storage_txn)
        self._txn_sessions[key] = txn
        return txn

    def get_transaction(self, lsid: Any) -> SessionTransaction | None:
        """Return the active transaction for a session, or None."""
        if lsid is None:
            return None
        key = self._session_key(lsid)
        txn = self._txn_sessions.get(key)
        if txn and txn.state == TransactionState.ACTIVE:
            return txn
        return None

    def commit_transaction(self, lsid: Any) -> None:
        """Commit the active transaction and force a WiredTiger checkpoint."""
        key = self._session_key(lsid)
        txn = self._txn_sessions.get(key)
        _txn.commit_active_transaction(self.local_client, txn)

    def abort_transaction(self, lsid: Any) -> int:
        """Abort the active transaction and roll back via the undo journal.

        Returns the number of operations rolled back.
        """
        key = self._session_key(lsid)
        txn = self._txn_sessions.get(key)
        return int(_txn.abort_active_transaction(txn))
