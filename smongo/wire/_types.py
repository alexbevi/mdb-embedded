"""Shared type aliases for the wire protocol layer."""

from typing import Any

CommandDoc = dict[str, Any]
DocSequences = dict[str, list[Any]]
ResponseDoc = dict[str, Any]
