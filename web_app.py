"""
mdb-embedded Web Dashboard  (v2 -- full experience)
=====================================================
Flask app exposing the complete embedded engine + sync layer.
"""

import json
import os
import time
import logging
import traceback

from flask import Flask, render_template, request, jsonify
from pymongo import MongoClient as PyMongoClient
from mdb_embedded import MongoClient, SyncManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("web")

app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "sync_demo"

client = MongoClient("local://local_wt_data")
remote = PyMongoClient(MONGO_URI)

sync_mgr = SyncManager(
    client, MONGO_URI,
    sync_config={"mode": "bidirectional", "interval_sec": 5, "batch_size": 100, "conflict_resolution": "lww"},
)

_collections_cache = {}


def _coll(name="users"):
    if name not in _collections_cache:
        _collections_cache[name] = client[DB_NAME][name]
        sync_mgr.register_collection(DB_NAME, name, _collections_cache[name].get_local_collection())
    return _collections_cache[name]


def _clean(doc):
    if doc is None:
        return None
    return {k: (str(v) if k == "_id" else v) for k, v in doc.items()}


# ── Pages ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── Shell (mongosh-style) ───────────────────────────────────────────

@app.route("/api/shell", methods=["POST"])
def shell():
    """
    Accept a simplified mongosh command string, execute it, return results.
    Supports: db.<coll>.find/findOne/insertOne/insertMany/updateMany/
              deleteMany/aggregate/createIndex/dropIndex/explain/count
    """
    body = request.json
    cmd = (body.get("command") or "").strip()
    if not cmd:
        return jsonify({"error": "Empty command"}), 400

    t0 = time.perf_counter()
    try:
        result = _exec_shell(cmd)
        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        return jsonify({"result": result, "ms": elapsed})
    except Exception as exc:
        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        return jsonify({"error": str(exc), "ms": elapsed}), 400


def _exec_shell(cmd):
    """Parse and execute a mongosh-style command."""
    if not cmd.startswith("db."):
        raise ValueError('Commands must start with "db.<collection>."')

    parts = cmd[3:]
    dot = parts.index(".")
    coll_name = parts[:dot]
    rest = parts[dot + 1:]

    coll = _coll(coll_name)

    if rest.startswith("find("):
        arg = _parse_arg(rest, "find")
        docs = [_clean(d) for d in coll.find(arg or {})]
        return docs
    if rest.startswith("findOne("):
        arg = _parse_arg(rest, "findOne")
        return _clean(coll.find_one(arg or {}))
    if rest.startswith("insertOne("):
        arg = _parse_arg(rest, "insertOne")
        r = coll.insert_one(arg)
        return {"acknowledged": True, "insertedId": str(r.inserted_ids[0])}
    if rest.startswith("insertMany("):
        arg = _parse_arg(rest, "insertMany")
        r = coll.insert_many(arg)
        return {"acknowledged": True, "insertedCount": len(r.inserted_ids)}
    if rest.startswith("updateMany("):
        args = _parse_two_args(rest, "updateMany")
        r = coll.update_many(args[0], args[1])
        return {"acknowledged": True, "modifiedCount": r.modified_count}
    if rest.startswith("updateOne("):
        args = _parse_two_args(rest, "updateOne")
        r = coll.update_one(args[0], args[1])
        return {"acknowledged": True, "modifiedCount": r.modified_count}
    if rest.startswith("deleteMany("):
        arg = _parse_arg(rest, "deleteMany")
        r = coll.delete_many(arg or {})
        return {"acknowledged": True, "deletedCount": r.deleted_count}
    if rest.startswith("deleteOne("):
        arg = _parse_arg(rest, "deleteOne")
        r = coll.delete_one(arg or {})
        return {"acknowledged": True, "deletedCount": r.deleted_count}
    if rest.startswith("aggregate("):
        arg = _parse_arg(rest, "aggregate")
        return coll.aggregate(arg or [])
    if rest.startswith("createIndex("):
        args_raw = rest[len("createIndex("):-1]
        parts_j = _split_json_args(args_raw)
        keys = json.loads(parts_j[0])
        opts = json.loads(parts_j[1]) if len(parts_j) > 1 else {}
        name = coll.create_index(keys, **opts)
        return {"name": name}
    if rest.startswith("dropIndex("):
        arg = _parse_arg(rest, "dropIndex")
        coll.drop_index(arg)
        return {"ok": True}
    if rest.startswith("getIndexes("):
        return coll.list_indexes()
    if rest.startswith("countDocuments("):
        arg = _parse_arg(rest, "countDocuments")
        return coll.count_documents(arg or {})
    if rest.startswith("explain("):
        arg = _parse_arg(rest, "explain")
        return coll.explain(arg or {})

    raise ValueError(f"Unknown command: {rest.split('(')[0]}")


def _parse_arg(rest, name):
    inner = rest[len(name) + 1:-1].strip()
    if not inner:
        return None
    return json.loads(inner)


def _parse_two_args(rest, name):
    inner = rest[len(name) + 1:-1].strip()
    parts = _split_json_args(inner)
    if len(parts) < 2:
        raise ValueError(f"{name} requires 2 arguments")
    return [json.loads(p) for p in parts]


def _split_json_args(s):
    """Split a string like '{...}, {...}' into separate JSON chunks."""
    parts = []
    depth = 0
    start = 0
    in_str = False
    for i, c in enumerate(s):
        if c == '"' and (i == 0 or s[i - 1] != '\\'):
            in_str = not in_str
        if in_str:
            continue
        if c in ('{', '['):
            depth += 1
        elif c in ('}', ']'):
            depth -= 1
        elif c == ',' and depth == 0:
            parts.append(s[start:i].strip())
            start = i + 1
    parts.append(s[start:].strip())
    return [p for p in parts if p]


# ── Collection API ───────────────────────────────────────────────────

@app.route("/api/docs")
def get_docs():
    coll = _coll(request.args.get("coll", "users"))
    return jsonify([_clean(d) for d in coll.find({})])


@app.route("/api/query", methods=["POST"])
def run_query():
    body = request.json
    coll = _coll(body.get("coll", "users"))
    query = body.get("query", {})
    t0 = time.perf_counter()
    try:
        plan = coll.explain(query)
        docs = [_clean(d) for d in coll.find(query)]
        ms = round((time.perf_counter() - t0) * 1000, 2)
        return jsonify({"plan": plan, "docs": docs, "count": len(docs), "ms": ms})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/aggregate", methods=["POST"])
def run_aggregate():
    body = request.json
    coll = _coll(body.get("coll", "users"))
    pipeline = body.get("pipeline", [])
    t0 = time.perf_counter()
    try:
        results = coll.aggregate(pipeline)
        ms = round((time.perf_counter() - t0) * 1000, 2)
        return jsonify({"results": [_clean(d) if isinstance(d, dict) else d for d in results], "count": len(results), "ms": ms})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/insert", methods=["POST"])
def insert_doc():
    body = request.json
    coll = _coll(body.get("coll", "users"))
    try:
        r = coll.insert_one(body.get("doc", {}))
        return jsonify({"inserted_id": str(r.inserted_ids[0])})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/update", methods=["POST"])
def update_docs():
    body = request.json
    coll = _coll(body.get("coll", "users"))
    try:
        r = coll.update_many(body.get("query", {}), body.get("update", {}))
        return jsonify({"modified_count": r.modified_count})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/delete", methods=["POST"])
def delete_docs():
    body = request.json
    coll = _coll(body.get("coll", "users"))
    try:
        r = coll.delete_many(body.get("query", {}))
        return jsonify({"deleted_count": r.deleted_count})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/stats")
def collection_stats():
    coll = _coll(request.args.get("coll", "users"))
    docs = coll.find({})
    idxs = coll.list_indexes()
    return jsonify({
        "doc_count": len(docs) if isinstance(docs, list) else coll.count_documents({}),
        "index_count": len(idxs),
        "indexes": idxs,
    })


# ── Index API ────────────────────────────────────────────────────────

@app.route("/api/indexes")
def list_indexes():
    return jsonify(_coll(request.args.get("coll", "users")).list_indexes())


@app.route("/api/indexes", methods=["POST"])
def create_index():
    body = request.json
    try:
        name = _coll(body.get("coll", "users")).create_index(body.get("keys", []), unique=body.get("unique", False))
        return jsonify({"name": name})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/indexes/<name>", methods=["DELETE"])
def drop_index(name):
    try:
        _coll(request.args.get("coll", "users")).drop_index(name)
        return jsonify({"dropped": name})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


# ── Oplog API ────────────────────────────────────────────────────────

@app.route("/api/oplog")
def get_oplog():
    coll = _coll(request.args.get("coll", "users"))
    limit = int(request.args.get("limit", 50))
    return jsonify(coll.get_oplog()[-limit:])


# ── Sync API ─────────────────────────────────────────────────────────

@app.route("/api/sync/status")
def sync_status():
    return jsonify(sync_mgr.status())


@app.route("/api/sync/push", methods=["POST"])
def sync_push():
    try:
        sync_mgr.sync_now()
        return jsonify({"ok": True, "status": sync_mgr.status()})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/sync/start", methods=["POST"])
def sync_start():
    sync_mgr.start()
    return jsonify({"ok": True, "status": sync_mgr.status()})


@app.route("/api/sync/stop", methods=["POST"])
def sync_stop():
    sync_mgr.stop()
    return jsonify({"ok": True, "status": sync_mgr.status()})


@app.route("/api/remote/docs")
def remote_docs():
    try:
        return jsonify([_clean(d) for d in remote[DB_NAME][request.args.get("coll", "users")].find({}).limit(200)])
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/remote/insert", methods=["POST"])
def remote_insert():
    body = request.json
    doc = body.get("doc", {})
    doc["_lastModified"] = time.time()
    try:
        remote[DB_NAME][body.get("coll", "users")].insert_one(doc)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Seed ─────────────────────────────────────────────────────────────

SAMPLE_DOCS = [
    {"name": "Alice",   "age": 34, "city": "NYC", "dept": "engineering", "tags": ["python", "mongodb"],  "salary": 145000},
    {"name": "Bob",     "age": 28, "city": "SF",  "dept": "engineering", "tags": ["js", "react"],        "salary": 128000},
    {"name": "Charlie", "age": 40, "city": "NYC", "dept": "management",  "tags": ["python", "go"],       "salary": 175000},
    {"name": "Diana",   "age": 25, "city": "LA",  "dept": "design",      "tags": ["rust", "figma"],      "salary": 98000},
    {"name": "Eve",     "age": 31, "city": "SF",  "dept": "engineering", "tags": ["python", "ml"],       "salary": 155000},
    {"name": "Frank",   "age": 36, "city": "CHI", "dept": "engineering", "tags": ["go", "k8s"],          "salary": 140000},
    {"name": "Grace",   "age": 29, "city": "NYC", "dept": "data",        "tags": ["python", "spark"],    "salary": 135000},
    {"name": "Hank",    "age": 45, "city": "SF",  "dept": "management",  "tags": ["strategy"],           "salary": 190000},
    {"name": "Ivy",     "age": 27, "city": "LA",  "dept": "design",      "tags": ["figma", "css"],       "salary": 105000},
    {"name": "Jack",    "age": 33, "city": "NYC", "dept": "engineering", "tags": ["java", "spring"],     "salary": 142000},
]


@app.route("/api/seed", methods=["POST"])
def seed_data():
    coll = _coll("users")
    coll.delete_many({})
    remote[DB_NAME]["users"].drop()
    coll.create_index([("age", 1)])
    coll.create_index([("city", 1), ("age", -1)])
    coll.create_index("name", unique=True)
    coll.create_index([("dept", 1)])
    coll.create_index([("salary", -1)])
    coll.insert_many(SAMPLE_DOCS)
    sync_mgr.sync_now()
    return jsonify({"ok": True, "count": len(SAMPLE_DOCS)})


def _auto_seed():
    coll = _coll("users")
    if coll.count_documents({}) > 0:
        return
    log.info("First run -- seeding sample data")
    remote[DB_NAME]["users"].drop()
    coll.create_index([("age", 1)])
    coll.create_index([("city", 1), ("age", -1)])
    coll.create_index("name", unique=True)
    coll.create_index([("dept", 1)])
    coll.create_index([("salary", -1)])
    coll.insert_many(SAMPLE_DOCS)
    sync_mgr.sync_now()
    log.info("Seeded %d docs + 5 indexes, pushed to remote", len(SAMPLE_DOCS))


if __name__ == "__main__":
    _auto_seed()
    app.run(host="0.0.0.0", port=5000, debug=False)
