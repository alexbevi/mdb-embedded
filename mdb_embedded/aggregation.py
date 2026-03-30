"""
Aggregation pipeline engine -- executes MongoDB-style aggregation stages
over in-memory document lists.
"""

import json
from collections import defaultdict

from .query import compile_query, get_value, set_value, resolve_expr


class Cursor:
    """Chainable cursor over a list of documents, supporting find and aggregate."""

    def __init__(self, docs):
        self.docs = docs

    def find(self, query):
        fn = compile_query(query)
        return Cursor([d for d in self.docs if fn(d)])

    def aggregate(self, pipeline):
        docs = self.docs

        for stage in pipeline:
            op, spec = list(stage.items())[0]

            if op == "$match":
                fn = compile_query(spec)
                docs = [d for d in docs if fn(d)]
            elif op == "$group":
                docs = group_stage(docs, spec)
            elif op == "$project":
                docs = project_stage(docs, spec)
            elif op == "$sort":
                docs = sort_stage(docs, spec)
            elif op == "$limit":
                docs = docs[:spec]
            elif op == "$skip":
                docs = docs[spec:]
            elif op == "$unwind":
                docs = unwind_stage(docs, spec)
            else:
                raise NotImplementedError(f"{op} not supported")

        return docs

    def to_list(self):
        return self.docs

    def __iter__(self):
        return iter(self.docs)


def group_stage(docs, spec):
    grouped = defaultdict(list)
    for doc in docs:
        key = resolve_expr(doc, spec["_id"])
        if isinstance(key, (dict, list)):
            key = json.dumps(key, sort_keys=True)
        grouped[key].append(doc)

    results = []
    for key, group_docs in grouped.items():
        try:
            key = json.loads(key)
        except (TypeError, json.JSONDecodeError):
            pass

        out = {"_id": key}
        for field, expr in spec.items():
            if field == "_id":
                continue
            op, val = list(expr.items())[0]
            if op == "$sum":
                if val == 1:
                    out[field] = len(group_docs)
                else:
                    out[field] = sum(resolve_expr(d, val) or 0 for d in group_docs)
            elif op == "$push":
                out[field] = [resolve_expr(d, val) for d in group_docs]
            elif op == "$avg":
                vals = [resolve_expr(d, val) for d in group_docs if resolve_expr(d, val) is not None]
                out[field] = sum(vals) / len(vals) if vals else 0
            elif op == "$min":
                vals = [resolve_expr(d, val) for d in group_docs if resolve_expr(d, val) is not None]
                out[field] = min(vals) if vals else None
            elif op == "$max":
                vals = [resolve_expr(d, val) for d in group_docs if resolve_expr(d, val) is not None]
                out[field] = max(vals) if vals else None

        results.append(out)
    return results


def project_stage(docs, spec):
    out = []
    for doc in docs:
        new_doc = {}
        for field, expr in spec.items():
            if expr == 1:
                new_doc[field] = get_value(doc, field)
            elif expr == 0:
                continue
            else:
                new_doc[field] = resolve_expr(doc, expr)
        out.append(new_doc)
    return out


def sort_stage(docs, spec):
    for field, direction in reversed(list(spec.items())):
        docs = sorted(
            docs,
            key=lambda d, f=field: (get_value(d, f) is not None, get_value(d, f)),
            reverse=(direction == -1),
        )
    return docs


def unwind_stage(docs, spec):
    out = []
    path = spec[1:] if spec.startswith("$") else spec
    for doc in docs:
        val = get_value(doc, path)
        if isinstance(val, list):
            for item in val:
                new_doc = doc.copy()
                set_value(new_doc, path, item)
                out.append(new_doc)
        elif val is not None:
            out.append(doc)
    return out
