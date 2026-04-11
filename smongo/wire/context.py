"""
Per-connection server context.

Each TCP connection gets its own ConnectionContext which caches database
handles for the lifetime of the connection.
Also tracks compression negotiation, logical sessions, per-session transaction
state with undo-journal rollback, per-connection write result tracking,
active-operation monitoring, and an operation profiler.

All classes except LogBuffer and the system-memory helpers live in Rust
(_smongo_core).  ConnectionContext is the Rust #[pyclass] re-exported here
so that existing ``from smongo.wire.context import ConnectionContext``
continues to work.
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading

from smongo._smongo_core import (
    ConnectionContext,
    ConnectionCounter,
    FreeMonitoringState,
    LastWriteResult,
    NamespaceError,
    ParameterStore,
    validate_namespace,
)

from .sessions import SessionRegistry, TooManySessions  # noqa: F401

__all__ = [
    "ConnectionContext",
    "ConnectionCounter",
    "FreeMonitoringState",
    "LastWriteResult",
    "LogBuffer",
    "NamespaceError",
    "ParameterStore",
    "SessionRegistry",
    "TooManySessions",
    "get_git_version",
    "get_total_memory_mb",
    "get_virtual_memory_mb",
    "validate_namespace",
]


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
