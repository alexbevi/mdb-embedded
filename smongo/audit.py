"""
Audit logging -- MongoDB-compatible audit trail for security-sensitive operations.

Records authentication events (login, logout, failed auth) and command
execution to a dedicated ``smongo.audit`` logger.  When used with
:class:`smongo.logging.StructuredJSONFormatter`, each entry is a single-line
JSON object matching MongoDB's structured log format.

Usage::

    from smongo.audit import configure_audit
    configure_audit("/var/log/smongo-audit.json")

Or via CLI::

    python -m smongo.wire --audit-log /var/log/smongo-audit.json
"""

from __future__ import annotations

import logging
import sys

from .logging import StructuredJSONFormatter

_audit_log = logging.getLogger("smongo.audit")
_enabled = False


def is_enabled() -> bool:
    """Return whether audit logging is active."""
    return _enabled


def log_auth_event(
    event: str,
    user: str,
    db: str,
    remote: str,
    success: bool,
    reason: str = "",
) -> None:
    """Log an authentication event (authenticate, logout, authFailure)."""
    if not _enabled:
        return
    status = "succeeded" if success else "failed"
    _audit_log.info(
        "%s %s for %s",
        event,
        status,
        user,
        extra={
            "attr": {
                "type": "auth",
                "event": event,
                "user": user,
                "db": db,
                "remote": remote,
                "success": success,
                "reason": reason,
            }
        },
    )


def log_command(
    user: str,
    db: str,
    command: str,
    ns: str,
    remote: str,
    success: bool,
    duration_ms: float,
) -> None:
    """Log a command execution event."""
    if not _enabled:
        return
    _audit_log.info(
        "%s on %s",
        command,
        ns,
        extra={
            "attr": {
                "type": "command",
                "user": user,
                "db": db,
                "command": command,
                "ns": ns,
                "remote": remote,
                "success": success,
                "duration_ms": duration_ms,
            }
        },
    )


def configure_audit(
    destination: str | None = None,
    *,
    level: str = "info",
) -> None:
    """Enable audit logging.

    Args:
        destination: File path for the audit log.  Use ``"stderr"`` or
            ``None`` to write to stderr.
        level: Minimum log level (default ``"info"``).
    """
    global _enabled

    _audit_log.handlers.clear()
    _audit_log.propagate = False
    _audit_log.setLevel(getattr(logging, level.upper(), logging.INFO))

    if destination and destination != "stderr":
        handler: logging.Handler = logging.FileHandler(destination, encoding="utf-8")
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setFormatter(StructuredJSONFormatter(include_stacktrace=False))
    _audit_log.addHandler(handler)
    _enabled = True
