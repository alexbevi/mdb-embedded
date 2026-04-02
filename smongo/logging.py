"""
Structured JSON logging -- MongoDB-compatible structured log format.

Usage::

    import smongo.logging
    smongo.logging.configure(level="info", json=True)

When ``json=True``, each log line is a single JSON object with fields:

    {"t": {"$date": "..."}, "s": "I", "c": "STORAGE", "id": 12345,
     "ctx": "conn1", "msg": "...", "attr": {...}}

This matches MongoDB's structured log format from 4.4+, making it trivial
to parse with standard log aggregation tools.
"""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

_SEVERITY_MAP = {
    logging.DEBUG: "D",
    logging.INFO: "I",
    logging.WARNING: "W",
    logging.ERROR: "E",
    logging.CRITICAL: "F",
}

_COMPONENT_MAP: dict[str, str] = {
    "smongo.storage": "STORAGE",
    "smongo.wire": "NETWORK",
    "smongo.wire.commands": "COMMAND",
    "smongo.wire.server": "NETWORK",
    "smongo.sync": "REPL",
    "smongo.index": "INDEX",
    "smongo.query": "QUERY",
    "smongo.aggregation": "AGG",
    "smongo.client": "ACCESS",
}


class StructuredJSONFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object."""

    def __init__(self, include_stacktrace: bool = True) -> None:
        super().__init__()
        self._include_stacktrace = include_stacktrace
        self._id_counter = 0

    def format(self, record: logging.LogRecord) -> str:
        self._id_counter += 1
        component = _COMPONENT_MAP.get(record.name, "DEFAULT")
        severity = _SEVERITY_MAP.get(record.levelno, "I")

        entry: dict[str, Any] = {
            "t": {"$date": datetime.fromtimestamp(record.created, tz=UTC).isoformat()},
            "s": severity,
            "c": component,
            "id": self._id_counter,
            "ctx": getattr(record, "ctx", record.name),
            "msg": record.getMessage(),
        }

        if hasattr(record, "attr") and record.attr:
            entry["attr"] = record.attr

        if self._include_stacktrace and record.exc_info and record.exc_info[1]:
            entry["error"] = str(record.exc_info[1])

        return json.dumps(entry, default=str)


class PlainFormatter(logging.Formatter):
    """Human-readable format for development use."""

    def __init__(self) -> None:
        super().__init__(
            "%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )


def configure(
    level: str = "info",
    *,
    json_format: bool = False,
    stream: Any = None,
) -> None:
    """Configure smongo's root logger.

    Args:
        level: Log level (debug, info, warning, error).
        json_format: Emit structured JSON when True.
        stream: Output stream (defaults to stderr).
    """
    root = logging.getLogger("smongo")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    root.handlers.clear()
    handler = logging.StreamHandler(stream or sys.stderr)

    if json_format:
        handler.setFormatter(StructuredJSONFormatter())
    else:
        handler.setFormatter(PlainFormatter())

    root.addHandler(handler)
