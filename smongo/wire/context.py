"""
Per-connection server context.

Each TCP connection gets its own ConnectionContext which caches LocalDB
instances (and thus WiredTiger sessions) for the lifetime of the connection.
Also tracks compression negotiation, logical sessions, per-session transaction
state with undo-journal rollback, per-connection write result tracking,
active-operation monitoring, and an operation profiler.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import threading
from itertools import count
from typing import TYPE_CHECKING, Any

from ..storage import LocalClient, LocalCollection, LocalDB
from ..storage.transaction import TransactionSession as _StorageTxnSession
from . import transactions as _txn
from .cursors import CursorRegistry
from .profiler import (
    OperationTracker,
    Profiler,
    TopStats,
)
from .sessions import SessionRegistry
from .transactions import (
    SessionTransaction,
    TransactionError,
    TransactionState,
)

if TYPE_CHECKING:
    from ..sync import SyncManager

_MAX_DB_NAME_LEN = 64
_MAX_COLL_NAME_LEN = 120
_INVALID_NS_CHARS = re.compile(r"[\x00/\\]")
_SYSTEM_PREFIX_EXCEPTIONS = frozenset({"$cmd", "$external"})


class NamespaceError(ValueError):
    """Raised when a database or collection name violates naming rules."""


def validate_namespace(db_name: str, coll_name: str) -> None:
    """Enforce MongoDB namespace naming rules.

    Raises NamespaceError if the names contain forbidden characters, exceed
    length limits, or are otherwise invalid.
    """
    if not isinstance(db_name, str) or not db_name or db_name != db_name.strip():
        raise NamespaceError(f"invalid database name: {db_name!r}")
    if len(db_name) > _MAX_DB_NAME_LEN:
        raise NamespaceError(f"database name exceeds {_MAX_DB_NAME_LEN} characters")
    if _INVALID_NS_CHARS.search(db_name):
        raise NamespaceError(f"database name contains forbidden characters: {db_name!r}")
    if "." in db_name:
        raise NamespaceError(f"database name cannot contain '.': {db_name!r}")
    if db_name.startswith("$"):
        raise NamespaceError(f"database name cannot start with '$': {db_name!r}")

    if not isinstance(coll_name, str) or not coll_name or coll_name != coll_name.strip():
        raise NamespaceError(f"invalid collection name: {coll_name!r}")
    if len(coll_name) > _MAX_COLL_NAME_LEN:
        raise NamespaceError(f"collection name exceeds {_MAX_COLL_NAME_LEN} characters")
    if _INVALID_NS_CHARS.search(coll_name):
        raise NamespaceError(f"collection name contains forbidden characters: {coll_name!r}")
    if coll_name.startswith("$") and coll_name not in _SYSTEM_PREFIX_EXCEPTIONS:
        raise NamespaceError(f"collection name cannot start with '$': {coll_name!r}")
    if ".." in coll_name:
        raise NamespaceError(f"collection name cannot contain '..': {coll_name!r}")


# =====================================================================
# Per-connection last-write tracking (getLastError)
# =====================================================================


class LastWriteResult:
    """Captures the outcome of the most recent write on a connection."""

    __slots__ = ("err", "n", "n_modified", "op", "upserted_id", "write_errors")

    def __init__(
        self,
        *,
        op: str = "unknown",
        n: int = 0,
        n_modified: int = 0,
        err: str | None = None,
        upserted_id: Any = None,
        write_errors: list[dict[str, Any]] | None = None,
    ) -> None:
        self.op = op
        self.n = n
        self.n_modified = n_modified
        self.err = err
        self.upserted_id = upserted_id
        self.write_errors = write_errors or []


# =====================================================================
# Mutable parameter store (getParameter / setParameter)
# =====================================================================


class ParameterStore:
    """Thread-safe mutable parameter store for ``getParameter``/``setParameter``."""

    _DEFAULTS: dict[str, Any] = {
        "featureCompatibilityVersion": {"version": "7.0"},
        "logLevel": 0,
        "authenticationMechanisms": [],
        "quiet": False,
        "notablescan": False,
        "maxTransactionLockRequestTimeoutMillis": 5000,
        "transactionLifetimeLimitSeconds": 60,
        "cursorTimeoutMillis": 600_000,
        "internalQueryExecMaxBlockingSortBytes": 104_857_600,
        "failIndexKeyTooLong": True,
        "slowOpThresholdMs": 100,
        "slowOpSampleRate": 1.0,
    }

    def __init__(self) -> None:
        self._params: dict[str, Any] = dict(self._DEFAULTS)
        self._lock = threading.Lock()

    def get(self, name: str) -> Any:
        with self._lock:
            return self._params.get(name)

    def get_all(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._params)

    def set(self, name: str, value: Any) -> None:
        with self._lock:
            self._params[name] = value


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
# Connection counter (serverStatus / connPoolStats)
# =====================================================================


class ConnectionCounter:
    """Thread-safe connection counter shared across all ``ConnectionContext`` instances."""

    def __init__(self, max_connections: int = 1024) -> None:
        self._current = 0
        self._total = 0
        self._max = max_connections
        self._lock = threading.Lock()

    def connect(self) -> None:
        with self._lock:
            self._current += 1
            self._total += 1

    def disconnect(self) -> None:
        with self._lock:
            self._current = max(0, self._current - 1)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "current": self._current,
                "available": self._max - self._current,
                "totalCreated": self._total,
            }


# =====================================================================
# Free monitoring state
# =====================================================================


class FreeMonitoringState:
    """Tracks whether free monitoring is enabled (setFreeMonitoring/getFreeMonitoringStatus)."""

    def __init__(self) -> None:
        self._state: str = "disabled"
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def set(self, action: str) -> None:
        with self._lock:
            if action == "enable":
                self._state = "enabled"
            elif action == "disable":
                self._state = "disabled"


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
        return _txn.abort_active_transaction(txn)
