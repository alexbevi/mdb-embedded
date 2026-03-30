"""
MQL Compiler -- translates MongoDB query dicts into executable Python predicates
and applies update operators to documents.
"""


def compile_query(query):
    """Compile a MongoDB-style query dict into a callable predicate."""

    def match(doc):
        for key, condition in query.items():
            if key == "$or":
                if not any(compile_query(sub)(doc) for sub in condition):
                    return False
                continue
            if key == "$and":
                if not all(compile_query(sub)(doc) for sub in condition):
                    return False
                continue

            value = get_value(doc, key)

            if isinstance(condition, dict):
                for op, cond_val in condition.items():
                    if op == "$gt" and not (value is not None and value > cond_val):
                        return False
                    elif op == "$lt" and not (value is not None and value < cond_val):
                        return False
                    elif op == "$gte" and not (value is not None and value >= cond_val):
                        return False
                    elif op == "$lte" and not (value is not None and value <= cond_val):
                        return False
                    elif op == "$eq" and not (value == cond_val):
                        return False
                    elif op == "$in" and not (value in cond_val):
                        return False
                    elif op == "$ne" and not (value != cond_val):
                        return False
                    elif op == "$exists":
                        exists = value is not None
                        if cond_val and not exists:
                            return False
                        if not cond_val and exists:
                            return False
            else:
                if value != condition:
                    return False

        return True

    return match


def apply_update(doc, update):
    """Apply MongoDB update operators ($set, $inc, $push, $unset) to a document in place."""
    for op, fields in update.items():
        if op == "$set":
            for k, v in fields.items():
                set_value(doc, k, v)
        elif op == "$inc":
            for k, v in fields.items():
                current = get_value(doc, k) or 0
                set_value(doc, k, current + v)
        elif op == "$push":
            for k, v in fields.items():
                arr = get_value(doc, k) or []
                if not isinstance(arr, list):
                    arr = [arr]
                arr.append(v)
                set_value(doc, k, arr)
        elif op == "$unset":
            for k in fields.keys():
                unset_value(doc, k)
        else:
            raise NotImplementedError(f"Update operator {op} not supported")


def get_value(doc, key):
    """Traverse a document using dot-notation path (e.g. 'user.stats.logins')."""
    parts = key.split(".")
    val = doc
    for p in parts:
        if not isinstance(val, dict):
            return None
        val = val.get(p)
        if val is None:
            return None
    return val


def set_value(doc, key, value):
    """Set a value in a document using dot-notation path, creating intermediates."""
    parts = key.split(".")
    d = doc
    for p in parts[:-1]:
        if p not in d or not isinstance(d[p], dict):
            d[p] = {}
        d = d[p]
    d[parts[-1]] = value


def unset_value(doc, key):
    """Remove a field from a document using dot-notation path."""
    parts = key.split(".")
    d = doc
    for p in parts[:-1]:
        if p not in d or not isinstance(d[p], dict):
            return
        d = d[p]
    if parts[-1] in d:
        del d[parts[-1]]


def resolve_expr(doc, expr):
    """Resolve a MongoDB expression -- field references ($field) or literal values."""
    if isinstance(expr, str) and expr.startswith("$"):
        return get_value(doc, expr[1:])
    return expr
