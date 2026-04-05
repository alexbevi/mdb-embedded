"""MQL query compiler -- translates MongoDB query dicts into callable predicates.

The compile_query function and all regex safety logic for the hot path are
implemented in Rust (_smongo_core). These thin wrappers exist only for Python
consumers (schema validation, tests) that need a ``re.Pattern`` object.
"""

from __future__ import annotations

import re

from smongo._smongo_core import compile_query

MAX_REGEX_PATTERN_LEN = 1024
_NESTED_QUANTIFIER_RE = re.compile(r"[+*]\s*[)]\s*[+*?{]")


def _build_regex_flags(opts: str) -> int:
    """Convert a MongoDB regex options string to Python ``re`` flags."""
    flags = 0
    if "i" in opts:
        flags |= re.IGNORECASE
    if "m" in opts:
        flags |= re.MULTILINE
    if "s" in opts:
        flags |= re.DOTALL
    if "x" in opts:
        flags |= re.VERBOSE
    return flags


def _safe_regex(pattern: str, flags: int = 0) -> re.Pattern[str]:
    """Compile and guard a regex pattern against ReDoS.

    Thin Python wrapper kept for ``smongo.schema`` and tests.
    The Rust hot path handles regex entirely without the GIL.
    """
    if len(pattern) > MAX_REGEX_PATTERN_LEN:
        raise ValueError(
            f"regex pattern length {len(pattern)} exceeds limit {MAX_REGEX_PATTERN_LEN}"
        )
    if _NESTED_QUANTIFIER_RE.search(pattern):
        raise ValueError("regex pattern rejected: nested quantifiers are not allowed")
    return re.compile(pattern, flags)


__all__ = [
    "MAX_REGEX_PATTERN_LEN",
    "_build_regex_flags",
    "_safe_regex",
    "compile_query",
]
