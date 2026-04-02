"""MQL query compiler -- translates MongoDB query dicts into callable predicates."""

from __future__ import annotations

import re
from typing import Any

from .._types import Document, Filter, Predicate
from .paths import field_exists, get_value

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

    Rejects patterns longer than MAX_REGEX_PATTERN_LEN or containing
    nested quantifiers (the most common catastrophic-backtracking pattern).
    """
    if len(pattern) > MAX_REGEX_PATTERN_LEN:
        raise ValueError(
            f"regex pattern length {len(pattern)} exceeds limit " f"{MAX_REGEX_PATTERN_LEN}"
        )
    if _NESTED_QUANTIFIER_RE.search(pattern):
        raise ValueError("regex pattern rejected: nested quantifiers are not allowed")
    return re.compile(pattern, flags)


_TYPE_MAP: dict[str, tuple[type, ...]] = {
    "double": (float,),
    "string": (str,),
    "object": (dict,),
    "array": (list,),
    "bool": (bool,),
    "int": (int,),
    "long": (int,),
    "null": (type(None),),
    "number": (int, float),
}


def compile_query(query: Filter) -> Predicate:
    """Compile a MongoDB-style query dict into a callable predicate."""
    from .expressions import resolve_expr

    def match(doc: Document) -> bool:
        for key, condition in query.items():
            if key == "$or":
                if not any(compile_query(sub)(doc) for sub in condition):
                    return False
                continue
            if key == "$and":
                if not all(compile_query(sub)(doc) for sub in condition):
                    return False
                continue
            if key == "$nor":
                if any(compile_query(sub)(doc) for sub in condition):
                    return False
                continue

            if key == "$expr":
                if not resolve_expr(doc, condition):
                    return False
                continue
            if key == "$comment":
                continue
            if key == "$text":
                search_str = (
                    condition.get("$search", "") if isinstance(condition, dict) else str(condition)
                )
                if not _text_match(doc, search_str, query):
                    return False
                continue

            value = get_value(doc, key)

            if isinstance(condition, dict):
                regex_flags = 0
                if "$options" in condition:
                    opts = condition.get("$options", "")
                    if "i" in opts:
                        regex_flags |= re.IGNORECASE
                    if "m" in opts:
                        regex_flags |= re.MULTILINE
                    if "s" in opts:
                        regex_flags |= re.DOTALL
                for op, cond_val in condition.items():
                    if not _eval_op(op, value, cond_val, doc, key, regex_flags=regex_flags):
                        return False
            else:
                if value != condition:
                    return False

        return True

    return match


def _text_match(doc: Document, search_str: str, query: Filter) -> bool:
    """Check if any string field in *doc* contains all search tokens."""
    tokens = search_str.lower().split()
    if not tokens:
        return True

    def _extract_strings(v: Any) -> list[str]:
        if isinstance(v, str):
            return [v]
        if isinstance(v, dict):
            result: list[str] = []
            for val in v.values():
                result.extend(_extract_strings(val))
            return result
        if isinstance(v, list):
            result = []
            for item in v:
                result.extend(_extract_strings(item))
            return result
        return []

    all_text = " ".join(_extract_strings(doc)).lower()
    return all(t in all_text for t in tokens)


def _eval_op(
    op: str, value: Any, cond_val: Any, doc: Document, key: str, regex_flags: int = 0
) -> bool:
    """Evaluate a single comparison/element/logical operator."""
    if op == "$gt":
        return value is not None and value > cond_val
    if op == "$lt":
        return value is not None and value < cond_val
    if op == "$gte":
        return value is not None and value >= cond_val
    if op == "$lte":
        return value is not None and value <= cond_val
    if op == "$eq":
        return bool(value == cond_val)
    if op == "$ne":
        return bool(value != cond_val)
    if op == "$in":
        if isinstance(value, list):
            return any(v in cond_val for v in value)
        return value in cond_val
    if op == "$nin":
        if isinstance(value, list):
            return not any(v in cond_val for v in value)
        return value not in cond_val
    if op == "$exists":
        present = field_exists(doc, key)
        return present if cond_val else not present
    if op == "$regex":
        if value is None or not isinstance(value, str):
            return False
        return _safe_regex(cond_val, regex_flags).search(value) is not None
    if op == "$options":
        return True
    if op == "$not":
        if isinstance(cond_val, dict):
            return not all(_eval_op(k, value, v, doc, key) for k, v in cond_val.items())
        return False
    if op == "$all":
        if not isinstance(value, list):
            return False
        return all(item in value for item in cond_val)
    if op == "$elemMatch":
        if not isinstance(value, list):
            return False
        fn = compile_query(cond_val)
        return any(fn(elem if isinstance(elem, dict) else {"value": elem}) for elem in value)
    if op == "$size":
        if not isinstance(value, list):
            return False
        return bool(len(value) == cond_val)
    if op == "$type":
        type_names = [cond_val] if isinstance(cond_val, str) else cond_val
        for tn in type_names:
            py_types = _TYPE_MAP.get(tn, ())
            if isinstance(value, py_types):
                return True
        return False
    if op == "$mod":
        if not isinstance(cond_val, list) or len(cond_val) != 2:
            return False
        divisor, remainder = cond_val
        if not isinstance(value, int | float) or divisor == 0:
            return False
        return int(value) % int(divisor) == int(remainder)
    if op == "$bitsAllSet":
        return _bits_check(value, cond_val, "all_set")
    if op == "$bitsAnySet":
        return _bits_check(value, cond_val, "any_set")
    if op == "$bitsAllClear":
        return _bits_check(value, cond_val, "all_clear")
    if op == "$bitsAnyClear":
        return _bits_check(value, cond_val, "any_clear")
    raise ValueError(f"unknown query operator: {op}")


def _bits_check(value: Any, bitmask: Any, mode: str) -> bool:
    """Evaluate bitwise query operators against *value*."""
    if not isinstance(value, int | float):
        return False
    val = int(value)
    if isinstance(bitmask, int):
        mask = bitmask
    elif isinstance(bitmask, list):
        mask = 0
        for pos in bitmask:
            mask |= 1 << int(pos)
    else:
        return False
    if mode == "all_set":
        return (val & mask) == mask
    if mode == "any_set":
        return (val & mask) != 0
    if mode == "all_clear":
        return (val & mask) == 0
    if mode == "any_clear":
        return (val & mask) != mask
    return False
