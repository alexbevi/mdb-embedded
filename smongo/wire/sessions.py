"""Logical session registry for the wire protocol."""
from __future__ import annotations

import threading
import time
import uuid
from typing import Any


class SessionEntry:
    """Tracks a single logical session."""
    __slots__ = ("created_at", "last_use", "session_id")

    def __init__(self, session_id: Any) -> None:
        self.session_id = session_id
        self.created_at = time.monotonic()
        self.last_use = self.created_at


MAX_SESSIONS = 10_000


class TooManySessions(RuntimeError):
    """Raised when the session registry reaches its capacity."""


class SessionRegistry:
    """Thread-safe registry of logical sessions across all connections."""

    def __init__(self, timeout_minutes: int = 30, max_sessions: int = MAX_SESSIONS) -> None:
        self._sessions: dict[str, SessionEntry] = {}
        self._lock = threading.Lock()
        self._timeout = timeout_minutes * 60
        self._max_sessions = max_sessions
        self._reaper_stop = threading.Event()
        self._reaper_thread: threading.Thread | None = None

    def start_reaper(self) -> None:
        """Start a background thread that periodically expires idle sessions."""
        if self._reaper_thread is not None and self._reaper_thread.is_alive():
            return
        self._reaper_stop.clear()
        self._reaper_thread = threading.Thread(
            target=self._reap_loop, daemon=True, name="session-reaper"
        )
        self._reaper_thread.start()

    def stop_reaper(self) -> None:
        """Signal the reaper thread to stop."""
        self._reaper_stop.set()
        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=3)
            self._reaper_thread = None

    def _reap_loop(self) -> None:
        while not self._reaper_stop.is_set():
            self._reaper_stop.wait(timeout=60)
            if self._reaper_stop.is_set():
                break
            self.expire()

    def create(self) -> uuid.UUID:
        """Create a new session and return its UUID."""
        sid = uuid.uuid4()
        key = str(sid)
        with self._lock:
            if len(self._sessions) >= self._max_sessions:
                raise TooManySessions(
                    f"session limit {self._max_sessions} reached"
                )
            self._sessions[key] = SessionEntry(sid)
        return sid

    def touch(self, lsid: Any) -> None:
        """Update last-use timestamp for a session. Creates if not present."""
        if lsid is None:
            return
        if isinstance(lsid, dict):
            key = str(lsid.get("id", ""))
        else:
            key = str(lsid)
        with self._lock:
            entry = self._sessions.get(key)
            if entry:
                entry.last_use = time.monotonic()
            elif len(self._sessions) < self._max_sessions:
                self._sessions[key] = SessionEntry(lsid)

    def refresh(self, session_ids: list[Any]) -> None:
        """Refresh one or more sessions to prevent timeout."""
        with self._lock:
            for sid in session_ids:
                if isinstance(sid, dict):
                    key = str(sid.get("id", ""))
                else:
                    key = str(sid)
                entry = self._sessions.get(key)
                if entry:
                    entry.last_use = time.monotonic()

    def end(self, session_ids: list[Any]) -> None:
        """Remove the given sessions."""
        with self._lock:
            for sid in session_ids:
                if isinstance(sid, dict):
                    key = str(sid.get("id", ""))
                else:
                    key = str(sid)
                self._sessions.pop(key, None)

    def kill(self, session_ids: list[Any]) -> None:
        """Forcibly terminate sessions (same as end for this engine)."""
        self.end(session_ids)

    def expire(self) -> None:
        """Remove sessions that have exceeded the timeout."""
        now = time.monotonic()
        with self._lock:
            expired = [
                key for key, entry in self._sessions.items()
                if now - entry.last_use > self._timeout
            ]
            for key in expired:
                del self._sessions[key]

    def expire_all(self) -> None:
        """Forcibly terminate all sessions."""
        with self._lock:
            self._sessions.clear()

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._sessions)
