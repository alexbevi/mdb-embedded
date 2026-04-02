"""Aggregation expression engine -- resolves $cond, $concat, $add, etc."""

from __future__ import annotations

import math
import re
from datetime import UTC, datetime
from typing import Any

from .._types import Document
from .compiler import _build_regex_flags, _safe_regex
from .paths import get_value


def resolve_expr(doc: Document, expr: Any) -> Any:
    """Resolve a MongoDB aggregation expression.

    Handles field references ($field), literal values, and operator expressions.
    """
    if expr is None:
        return None

    if isinstance(expr, str):
        if expr.startswith("$$"):
            if expr == "$$ROOT":
                return doc
            if expr == "$$CURRENT":
                return doc
            var_name = expr[2:]
            if var_name in doc:
                return doc[var_name]
            raise ValueError(f"Unsupported system variable: {expr}")
        if expr.startswith("$"):
            return get_value(doc, expr[1:])
        return expr

    if not isinstance(expr, dict):
        return expr

    if len(expr) == 1:
        op, arg = next(iter(expr.items()))
        if op.startswith("$"):
            return _eval_expr_op(op, arg, doc)

    return {k: resolve_expr(doc, v) for k, v in expr.items()}


def _eval_expr_op(op: str, arg: Any, doc: Document) -> Any:
    """Evaluate a single aggregation expression operator."""

    # ── Conditional ──
    if op == "$cond":
        if isinstance(arg, dict):
            cond = resolve_expr(doc, arg.get("if"))
            return (
                resolve_expr(doc, arg.get("then")) if cond else resolve_expr(doc, arg.get("else"))
            )
        if isinstance(arg, list) and len(arg) == 3:
            return (
                resolve_expr(doc, arg[1])
                if resolve_expr(doc, arg[0])
                else resolve_expr(doc, arg[2])
            )
        return None

    if op == "$ifNull":
        if isinstance(arg, list) and len(arg) >= 2:
            val = resolve_expr(doc, arg[0])
            return val if val is not None else resolve_expr(doc, arg[1])
        return None

    if op == "$switch":
        branches = arg.get("branches", [])
        for branch in branches:
            if resolve_expr(doc, branch.get("case")):
                return resolve_expr(doc, branch.get("then"))
        return resolve_expr(doc, arg.get("default"))

    # ── String ──
    if op == "$concat":
        parts = [resolve_expr(doc, a) for a in arg]
        if any(p is None for p in parts):
            return None
        return "".join(str(p) for p in parts)

    if op == "$toUpper":
        val = resolve_expr(doc, arg)
        return val.upper() if isinstance(val, str) else None

    if op == "$toLower":
        val = resolve_expr(doc, arg)
        return val.lower() if isinstance(val, str) else None

    if op == "$substr":
        if isinstance(arg, list) and len(arg) == 3:
            s = resolve_expr(doc, arg[0])
            start = resolve_expr(doc, arg[1])
            length = resolve_expr(doc, arg[2])
            if isinstance(s, str):
                return s[start : start + length]
        return None

    if op == "$strLenCP":
        val = resolve_expr(doc, arg)
        return len(val) if isinstance(val, str) else None

    # ── Array ──
    if op == "$arrayElemAt":
        if isinstance(arg, list) and len(arg) == 2:
            arr = resolve_expr(doc, arg[0])
            idx = resolve_expr(doc, arg[1])
            if isinstance(arr, list) and isinstance(idx, int) and -len(arr) <= idx < len(arr):
                return arr[idx]
        return None

    if op == "$size":
        val = resolve_expr(doc, arg)
        return len(val) if isinstance(val, list) else None

    if op == "$filter":
        input_arr = resolve_expr(doc, arg.get("input"))
        as_name = arg.get("as", "this")
        cond_expr = arg.get("cond")
        if not isinstance(input_arr, list):
            return None
        result: list[Any] = []
        for item in input_arr:
            scoped = dict(doc)
            scoped[as_name] = item
            if resolve_expr(scoped, cond_expr):
                result.append(item)
        return result

    if op == "$concatArrays":
        result_arr: list[Any] = []
        for a in arg:
            val = resolve_expr(doc, a)
            if not isinstance(val, list):
                return None
            result_arr.extend(val)
        return result_arr

    if op == "$in":
        if isinstance(arg, list) and len(arg) == 2:
            val = resolve_expr(doc, arg[0])
            arr = resolve_expr(doc, arg[1])
            return val in arr if isinstance(arr, list) else False
        return False

    # ── Arithmetic ──
    if op == "$add":
        vals = [resolve_expr(doc, a) for a in arg]
        if any(v is None for v in vals):
            return None
        return sum(vals)

    if op == "$subtract":
        if isinstance(arg, list) and len(arg) == 2:
            a, b = resolve_expr(doc, arg[0]), resolve_expr(doc, arg[1])
            if a is not None and b is not None:
                return a - b
        return None

    if op == "$multiply":
        vals = [resolve_expr(doc, a) for a in arg]
        if any(v is None for v in vals):
            return None
        product = 1
        for v in vals:
            product *= v
        return product

    if op == "$divide":
        if isinstance(arg, list) and len(arg) == 2:
            a, b = resolve_expr(doc, arg[0]), resolve_expr(doc, arg[1])
            if a is not None and b is not None and b != 0:
                return a / b
        return None

    if op == "$mod":
        if isinstance(arg, list) and len(arg) == 2:
            a, b = resolve_expr(doc, arg[0]), resolve_expr(doc, arg[1])
            if a is not None and b is not None and b != 0:
                return a % b
        return None

    if op == "$abs":
        val = resolve_expr(doc, arg)
        return abs(val) if val is not None else None

    if op == "$ceil":
        val = resolve_expr(doc, arg)
        return math.ceil(val) if val is not None else None

    if op == "$floor":
        val = resolve_expr(doc, arg)
        return math.floor(val) if val is not None else None

    if op == "$round":
        if isinstance(arg, list):
            val = resolve_expr(doc, arg[0])
            places = resolve_expr(doc, arg[1]) if len(arg) > 1 else 0
            return round(val, places) if val is not None else None
        return None

    # ── Comparison (expression form) ──
    if op == "$eq":
        if isinstance(arg, list) and len(arg) == 2:
            return resolve_expr(doc, arg[0]) == resolve_expr(doc, arg[1])
        return False

    if op == "$ne":
        if isinstance(arg, list) and len(arg) == 2:
            return resolve_expr(doc, arg[0]) != resolve_expr(doc, arg[1])
        return True

    if op == "$gt":
        if isinstance(arg, list) and len(arg) == 2:
            a, b = resolve_expr(doc, arg[0]), resolve_expr(doc, arg[1])
            return a > b if a is not None and b is not None else False
        return False

    if op == "$lt":
        if isinstance(arg, list) and len(arg) == 2:
            a, b = resolve_expr(doc, arg[0]), resolve_expr(doc, arg[1])
            return a < b if a is not None and b is not None else False
        return False

    if op == "$gte":
        if isinstance(arg, list) and len(arg) == 2:
            a, b = resolve_expr(doc, arg[0]), resolve_expr(doc, arg[1])
            return a >= b if a is not None and b is not None else False
        return False

    if op == "$lte":
        if isinstance(arg, list) and len(arg) == 2:
            a, b = resolve_expr(doc, arg[0]), resolve_expr(doc, arg[1])
            return a <= b if a is not None and b is not None else False
        return False

    # ── Boolean ──
    if op == "$and":
        return all(resolve_expr(doc, a) for a in arg)

    if op == "$or":
        return any(resolve_expr(doc, a) for a in arg)

    if op == "$not":
        if isinstance(arg, list) and len(arg) == 1:
            return not resolve_expr(doc, arg[0])
        return not resolve_expr(doc, arg)

    # ── Type ──
    if op == "$type":
        val = resolve_expr(doc, arg)
        if val is None:
            return "null"
        if isinstance(val, bool):
            return "bool"
        if isinstance(val, int):
            return "int"
        if isinstance(val, float):
            return "double"
        if isinstance(val, str):
            return "string"
        if isinstance(val, list):
            return "array"
        if isinstance(val, dict):
            return "object"
        return "unknown"

    if op == "$literal":
        return arg

    # ── Date ──
    if op == "$dateFromString":
        if isinstance(arg, dict):
            date_str = resolve_expr(doc, arg.get("dateString"))
            if isinstance(date_str, str):
                try:
                    return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
                except (ValueError, TypeError):
                    on_error = arg.get("onError")
                    return resolve_expr(doc, on_error) if on_error is not None else None
        return None

    if op == "$toDate":
        val = resolve_expr(doc, arg)
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00")).isoformat()
            except (ValueError, TypeError):
                return None
        if isinstance(val, int | float):
            return datetime.fromtimestamp(val / 1000, tz=UTC).isoformat()
        return None

    if op == "$dateToString":
        if isinstance(arg, dict):
            date_val = resolve_expr(doc, arg.get("date"))
            fmt = arg.get("format", "%Y-%m-%dT%H:%M:%S.%fZ")
            if isinstance(date_val, str):
                try:
                    dt = datetime.fromisoformat(date_val.replace("Z", "+00:00"))
                    return dt.strftime(fmt)
                except (ValueError, TypeError):
                    return None
        return None

    if op == "$convert":
        if isinstance(arg, dict):
            input_val = resolve_expr(doc, arg.get("input"))
            to_type = arg.get("to")
            on_error = arg.get("onError")
            try:
                if to_type == "string":
                    return str(input_val) if input_val is not None else None
                if to_type == "int":
                    return int(input_val)
                if to_type in ("double", "decimal"):
                    return float(input_val)
                if to_type == "bool":
                    return bool(input_val)
                if to_type == "date":
                    if isinstance(input_val, str):
                        return datetime.fromisoformat(input_val.replace("Z", "+00:00")).isoformat()
                    if isinstance(input_val, int | float):
                        return datetime.fromtimestamp(input_val / 1000, tz=UTC).isoformat()
            except (ValueError, TypeError):
                return resolve_expr(doc, on_error) if on_error is not None else None
        return None

    # ── Variable binding ──
    if op == "$let":
        if isinstance(arg, dict):
            vars_spec = arg.get("vars", {})
            in_expr = arg.get("in")
            scoped = dict(doc)
            for var_name, var_expr in vars_spec.items():
                scoped[var_name] = resolve_expr(doc, var_expr)
            return resolve_expr(scoped, in_expr)
        return None

    # ── Array higher-order ──
    if op == "$map":
        if isinstance(arg, dict):
            input_arr = resolve_expr(doc, arg.get("input"))
            as_name = arg.get("as", "this")
            in_expr = arg.get("in")
            if not isinstance(input_arr, list):
                return None
            result = []
            for item in input_arr:
                scoped = dict(doc)
                scoped[as_name] = item
                result.append(resolve_expr(scoped, in_expr))
            return result
        return None

    if op == "$reduce":
        if isinstance(arg, dict):
            input_arr = resolve_expr(doc, arg.get("input"))
            initial = resolve_expr(doc, arg.get("initialValue"))
            in_expr = arg.get("in")
            if not isinstance(input_arr, list):
                return None
            accum = initial
            for item in input_arr:
                scoped = dict(doc)
                scoped["value"] = accum
                scoped["this"] = item
                accum = resolve_expr(scoped, in_expr)
            return accum
        return None

    if op == "$range":
        if isinstance(arg, list) and len(arg) >= 2:
            start = resolve_expr(doc, arg[0])
            end = resolve_expr(doc, arg[1])
            step = resolve_expr(doc, arg[2]) if len(arg) > 2 else 1
            if isinstance(start, int | float) and isinstance(end, int | float):
                return list(range(int(start), int(end), int(step or 1)))
        return None

    if op == "$zip":
        if isinstance(arg, dict):
            inputs = arg.get("inputs", [])
            use_longest = arg.get("useLongestLength", False)
            defaults = arg.get("defaults", [])
            resolved = [resolve_expr(doc, inp) for inp in inputs]
            if any(not isinstance(r, list) for r in resolved):
                return None
            if use_longest:
                max_len = max((len(r) for r in resolved), default=0)
                out = []
                for i in range(max_len):
                    row = []
                    for j, arr in enumerate(resolved):
                        if i < len(arr):
                            row.append(arr[i])
                        elif j < len(defaults):
                            row.append(defaults[j])
                        else:
                            row.append(None)
                    out.append(row)
                return out
            else:
                min_len = min((len(r) for r in resolved), default=0)
                return [[arr[i] for arr in resolved] for i in range(min_len)]
        return None

    if op == "$reverseArray":
        val = resolve_expr(doc, arg)
        return list(reversed(val)) if isinstance(val, list) else None

    if op == "$slice":
        if isinstance(arg, list) and len(arg) >= 2:
            arr = resolve_expr(doc, arg[0])
            if not isinstance(arr, list):
                return None
            if len(arg) == 2:
                n = resolve_expr(doc, arg[1])
                if isinstance(n, int):
                    return arr[:n] if n >= 0 else arr[n:]
            elif len(arg) == 3:
                pos = resolve_expr(doc, arg[1])
                n = resolve_expr(doc, arg[2])
                if isinstance(pos, int) and isinstance(n, int):
                    return arr[pos : pos + n]
        return None

    if op == "$isArray":
        val = resolve_expr(doc, arg)
        return isinstance(val, list)

    if op == "$indexOfArray":
        if isinstance(arg, list) and len(arg) >= 2:
            arr = resolve_expr(doc, arg[0])
            search = resolve_expr(doc, arg[1])
            if not isinstance(arr, list):
                return -1
            start = resolve_expr(doc, arg[2]) if len(arg) > 2 else 0
            end = resolve_expr(doc, arg[3]) if len(arg) > 3 else len(arr)
            try:
                return arr.index(search, int(start or 0), int(end or len(arr)))
            except ValueError:
                return -1
        return -1

    # ── Object ──
    if op == "$objectToArray":
        val = resolve_expr(doc, arg)
        if isinstance(val, dict):
            return [{"k": k, "v": v} for k, v in val.items()]
        return None

    if op == "$arrayToObject":
        val = resolve_expr(doc, arg)
        if isinstance(val, list):
            result_obj: dict[str, Any] = {}
            for item in val:
                if isinstance(item, dict) and "k" in item and "v" in item:
                    result_obj[item["k"]] = item["v"]
                elif isinstance(item, list) and len(item) == 2:
                    result_obj[str(item[0])] = item[1]
            return result_obj
        return None

    if op == "$mergeObjects":
        if isinstance(arg, list):
            merged_obj: dict[str, Any] = {}
            for a in arg:
                val = resolve_expr(doc, a)
                if isinstance(val, dict):
                    merged_obj.update(val)
            return merged_obj
        val = resolve_expr(doc, arg)
        return val if isinstance(val, dict) else {}

    if op == "$getField":
        if isinstance(arg, dict):
            field_name = resolve_expr(doc, arg.get("field"))
            input_obj = resolve_expr(doc, arg.get("input")) or doc
        elif isinstance(arg, str):
            field_name = arg
            input_obj = doc
        else:
            return None
        if isinstance(input_obj, dict) and isinstance(field_name, str):
            return input_obj.get(field_name)
        return None

    if op == "$setField":
        if isinstance(arg, dict):
            field_name = resolve_expr(doc, arg.get("field"))
            input_obj = resolve_expr(doc, arg.get("input")) or dict(doc)
            value = resolve_expr(doc, arg.get("value"))
            if isinstance(input_obj, dict) and isinstance(field_name, str):
                result_obj = dict(input_obj)
                result_obj[field_name] = value
                return result_obj
        return None

    # ── Regex ──
    if op == "$regexMatch":
        if isinstance(arg, dict):
            input_val = resolve_expr(doc, arg.get("input"))
            regex_str = arg.get("regex", "")
            opts = arg.get("options", "")
            if isinstance(input_val, str):
                flags = _build_regex_flags(opts)
                try:
                    return _safe_regex(regex_str, flags).search(input_val) is not None
                except (ValueError, re.error):
                    return False
        return False

    if op == "$regexFind":
        if isinstance(arg, dict):
            input_val = resolve_expr(doc, arg.get("input"))
            regex_str = arg.get("regex", "")
            opts = arg.get("options", "")
            if isinstance(input_val, str):
                flags = _build_regex_flags(opts)
                try:
                    m = _safe_regex(regex_str, flags).search(input_val)
                    if m:
                        return {"match": m.group(), "idx": m.start(), "captures": list(m.groups())}
                except (ValueError, re.error):
                    pass
        return None

    if op == "$regexFindAll":
        if isinstance(arg, dict):
            input_val = resolve_expr(doc, arg.get("input"))
            regex_str = arg.get("regex", "")
            opts = arg.get("options", "")
            if isinstance(input_val, str):
                flags = _build_regex_flags(opts)
                try:
                    results: list[dict[str, Any]] = []
                    for m in _safe_regex(regex_str, flags).finditer(input_val):
                        results.append(
                            {"match": m.group(), "idx": m.start(), "captures": list(m.groups())}
                        )
                    return results
                except (ValueError, re.error):
                    pass
        return []

    # ── String extras ──
    if op == "$toString":
        val = resolve_expr(doc, arg)
        return str(val) if val is not None else None

    if op == "$toInt":
        val = resolve_expr(doc, arg)
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    if op == "$toDouble":
        val = resolve_expr(doc, arg)
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    if op == "$toBool":
        val = resolve_expr(doc, arg)
        return bool(val)

    if op == "$trim":
        if isinstance(arg, dict):
            input_val = resolve_expr(doc, arg.get("input"))
            chars = arg.get("chars")
            if isinstance(input_val, str):
                return input_val.strip(chars) if chars else input_val.strip()
        return None

    if op == "$ltrim":
        if isinstance(arg, dict):
            input_val = resolve_expr(doc, arg.get("input"))
            chars = arg.get("chars")
            if isinstance(input_val, str):
                return input_val.lstrip(chars) if chars else input_val.lstrip()
        return None

    if op == "$rtrim":
        if isinstance(arg, dict):
            input_val = resolve_expr(doc, arg.get("input"))
            chars = arg.get("chars")
            if isinstance(input_val, str):
                return input_val.rstrip(chars) if chars else input_val.rstrip()
        return None

    if op == "$split":
        if isinstance(arg, list) and len(arg) == 2:
            s = resolve_expr(doc, arg[0])
            delim = resolve_expr(doc, arg[1])
            if isinstance(s, str) and isinstance(delim, str):
                return s.split(delim)
        return None

    if op == "$indexOfCP":
        if isinstance(arg, list) and len(arg) >= 2:
            s = resolve_expr(doc, arg[0])
            sub = resolve_expr(doc, arg[1])
            if isinstance(s, str) and isinstance(sub, str):
                start = resolve_expr(doc, arg[2]) if len(arg) > 2 else 0
                end = resolve_expr(doc, arg[3]) if len(arg) > 3 else len(s)
                idx = s.find(sub, int(start or 0), int(end or len(s)))
                return idx
        return -1

    if op == "$replaceOne":
        if isinstance(arg, dict):
            input_val = resolve_expr(doc, arg.get("input"))
            find_val = resolve_expr(doc, arg.get("find"))
            replacement = resolve_expr(doc, arg.get("replacement"))
            if (
                isinstance(input_val, str)
                and isinstance(find_val, str)
                and isinstance(replacement, str)
            ):
                return input_val.replace(find_val, replacement, 1)
        return None

    if op == "$replaceAll":
        if isinstance(arg, dict):
            input_val = resolve_expr(doc, arg.get("input"))
            find_val = resolve_expr(doc, arg.get("find"))
            replacement = resolve_expr(doc, arg.get("replacement"))
            if (
                isinstance(input_val, str)
                and isinstance(find_val, str)
                and isinstance(replacement, str)
            ):
                return input_val.replace(find_val, replacement)
        return None

    # ── Math extras ──
    if op == "$sqrt":
        val = resolve_expr(doc, arg)
        return math.sqrt(val) if isinstance(val, int | float) and val >= 0 else None

    if op == "$pow":
        if isinstance(arg, list) and len(arg) == 2:
            base = resolve_expr(doc, arg[0])
            exp = resolve_expr(doc, arg[1])
            if isinstance(base, int | float) and isinstance(exp, int | float):
                return base**exp
        return None

    if op == "$log":
        if isinstance(arg, list) and len(arg) == 2:
            val = resolve_expr(doc, arg[0])
            base = resolve_expr(doc, arg[1])
            if (
                isinstance(val, int | float)
                and isinstance(base, int | float)
                and val > 0
                and base > 0
            ):
                return math.log(val, base)
        return None

    if op == "$log10":
        val = resolve_expr(doc, arg)
        if isinstance(val, int | float) and val > 0:
            return math.log10(val)
        return None

    if op == "$ln":
        val = resolve_expr(doc, arg)
        if isinstance(val, int | float) and val > 0:
            return math.log(val)
        return None

    if op == "$exp":
        val = resolve_expr(doc, arg)
        if isinstance(val, int | float):
            return math.exp(val)
        return None

    if op == "$trunc":
        if isinstance(arg, list):
            val = resolve_expr(doc, arg[0])
            places = resolve_expr(doc, arg[1]) if len(arg) > 1 else 0
            if isinstance(val, int | float):
                factor = 10 ** int(places or 0)
                return int(val * factor) / factor
        else:
            val = resolve_expr(doc, arg)
            if isinstance(val, int | float):
                return int(val)
        return None

    raise ValueError(f"Unsupported expression operator: {op}")
