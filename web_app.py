"""
smongo Web Dashboard -- Small MongoDB, full experience.

Flask app exposing the complete embedded engine + sync layer.
Optionally starts a wire protocol server in the same process so that
MongoDB Compass (or any driver) can connect over TCP.

Environment variables:
- ``WIRE_PORT``        -- if set (non-zero), start the wire server on this port.
- ``WIRE_HOST``        -- bind address for the wire server (default: BIND_HOST).
- ``SMONGO_API_KEY``   -- if set, all ``/api/*`` requests must include
  ``Authorization: Bearer <key>``.
- ``SMONGO_RATE_LIMIT`` -- max requests per minute per IP (default 60).
"""

import json
import logging
import os
import re
import threading
import time
from typing import Any

from bson import ObjectId as BsonObjectId
from flask import Flask, Response, jsonify, render_template, request
from flask.json.provider import DefaultJSONProvider
from pymongo import MongoClient as PyMongoClient
from pymongo.errors import PyMongoError

from smongo import (
    Collection,
    DuplicateKeyError,
    MongoClient,
    ObjectId,
    SyncManager,
    ValidationError,
)
from smongo.wire import WireServer


class _EngineJSONProvider(DefaultJSONProvider):
    """Teach Flask how to serialize ObjectId instances (engine + bson)."""

    def default(self, o: object) -> object:
        if isinstance(o, ObjectId | BsonObjectId):
            return str(o)
        return super().default(o)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("web")

app = Flask(__name__)
app.json_provider_class = _EngineJSONProvider
app.json = _EngineJSONProvider(app)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB

# ---------------------------------------------------------------------------
# Security: API-key auth
# ---------------------------------------------------------------------------

_API_KEY = os.environ.get("SMONGO_API_KEY")

# ---------------------------------------------------------------------------
# Security: token-bucket rate limiter (per-IP, in-process)
# ---------------------------------------------------------------------------

_RATE_LIMIT = int(os.environ.get("SMONGO_RATE_LIMIT", "60"))


class _TokenBucket:
    """Simple per-IP token-bucket rate limiter (no external dependency)."""

    def __init__(self, rate: int, per: float = 60.0) -> None:
        self._rate = rate
        self._per = per
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(key, (float(self._rate), now))
            elapsed = now - last
            tokens = min(float(self._rate), tokens + elapsed * (self._rate / self._per))
            if tokens >= 1.0:
                self._buckets[key] = (tokens - 1.0, now)
                return True
            self._buckets[key] = (tokens, now)
            return False


_limiter = _TokenBucket(_RATE_LIMIT)

# ---------------------------------------------------------------------------
# Security: input validation helpers
# ---------------------------------------------------------------------------

MAX_SHELL_COMMAND_LEN = 16 * 1024  # 16 KB
MAX_PIPELINE_STAGES = 50
_BAD_COLL_NAME_RE = re.compile(r"[\x00$]")
_MAX_COLL_NAME_LEN = 120


def _validate_collection_name(name: str) -> None:
    """Reject collection names that violate MongoDB namespace rules."""
    if not name or len(name) > _MAX_COLL_NAME_LEN:
        raise ValueError(f"Collection name must be 1-{_MAX_COLL_NAME_LEN} characters")
    if _BAD_COLL_NAME_RE.search(name):
        raise ValueError("Collection name must not contain '$' or null bytes")


def _json_body() -> dict[str, Any]:
    """Return request JSON as an object payload."""
    payload = request.get_json(silent=True)
    return payload if isinstance(payload, dict) else {}


MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "sync_demo"

client = MongoClient("local://local_wt_data")
remote: PyMongoClient[dict[str, Any]] = PyMongoClient(MONGO_URI)

sync_mgr = SyncManager(
    client,
    MONGO_URI,
    sync_config={
        "mode": "bidirectional",
        "interval_sec": 5,
        "batch_size": 100,
        "conflict_resolution": "lww",
    },
)

_collections_cache: dict[str, Collection] = {}
_watch_streams: dict[str, Any] = {}


def _coll(name: str = "users") -> Collection:
    _validate_collection_name(name)
    if name not in _collections_cache:
        _collections_cache[name] = client[DB_NAME][name]
        sync_mgr.register_collection(DB_NAME, name, _collections_cache[name].get_local_collection())
    return _collections_cache[name]


def _watch_stream(name: str = "users") -> Any:
    if name not in _watch_streams:
        _watch_streams[name] = _coll(name).watch()
    return _watch_streams[name]


def _clean(doc: Any) -> Any:
    """Recursively convert ObjectId instances (engine + bson) to strings for JSON."""
    if doc is None:
        return None
    if isinstance(doc, ObjectId | BsonObjectId):
        return str(doc)
    if isinstance(doc, dict):
        return {k: _clean(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [_clean(v) for v in doc]
    return doc


@app.before_request
def _enforce_auth() -> Response | tuple[Response, int] | None:
    """Reject /api/* requests when SMONGO_API_KEY is set but not provided."""
    if _API_KEY and request.path.startswith("/api/"):
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {_API_KEY}":
            return jsonify(
                {"error": "Unauthorized -- set Authorization: Bearer <SMONGO_API_KEY>"}
            ), 401
    return None


@app.before_request
def _enforce_rate_limit() -> Response | tuple[Response, int] | None:
    """Token-bucket rate limiting on /api/* routes."""
    if request.path.startswith("/api/"):
        ip = request.remote_addr or "unknown"
        if not _limiter.allow(ip):
            return jsonify({"error": "Rate limit exceeded -- try again shortly"}), 429
    return None


@app.after_request
def _set_security_headers(response: Response) -> Response:
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "connect-src 'self'"
    )
    return response


@app.errorhandler(404)
def handle_not_found(exc: Exception) -> tuple[Response, int]:
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(Exception)
def handle_unexpected_error(exc: Exception) -> tuple[Response, int]:
    log.exception("Unhandled error in request")
    return jsonify({"error": "Internal server error"}), 500


# ── Pages ────────────────────────────────────────────────────────────


@app.route("/")
def index() -> str:
    return render_template("index.html")


# ── Shell (mongosh-style) ───────────────────────────────────────────


@app.route("/api/shell", methods=["POST"])
def shell() -> Response | tuple[Response, int]:
    """
    Accept a simplified mongosh command string, execute it, return results.
    Supports: db.<coll>.find/findOne/insertOne/insertMany/updateMany/
              deleteMany/aggregate/createIndex/dropIndex/explain/count
    """
    body = _json_body()
    cmd = (body.get("command") or "").strip()
    if not cmd:
        return jsonify({"error": "Empty command"}), 400
    if len(cmd) > MAX_SHELL_COMMAND_LEN:
        return jsonify({"error": f"Command exceeds {MAX_SHELL_COMMAND_LEN} byte limit"}), 400

    t0 = time.perf_counter()
    try:
        result = _exec_shell(cmd)
        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        return jsonify({"result": result, "ms": elapsed})
    except (
        ValueError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
        NotImplementedError,
        DuplicateKeyError,
        ValidationError,
    ) as exc:
        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        return jsonify({"error": str(exc), "ms": elapsed}), 400


def _exec_shell(cmd: str) -> Any:
    """Parse and execute a mongosh-style command."""
    if not cmd.startswith("db."):
        raise ValueError('Commands must start with "db.<collection>."')

    parts = cmd[3:]
    dot = parts.index(".")
    coll_name = parts[:dot]
    rest = parts[dot + 1 :]

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
        args_raw = rest[len("createIndex(") : -1]
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


def _parse_arg(rest: str, name: str) -> Any:
    inner = rest[len(name) + 1 : -1].strip()
    if not inner:
        return None
    return json.loads(inner)


def _parse_two_args(rest: str, name: str) -> list[Any]:
    inner = rest[len(name) + 1 : -1].strip()
    parts = _split_json_args(inner)
    if len(parts) < 2:
        raise ValueError(f"{name} requires 2 arguments")
    return [json.loads(p) for p in parts]


def _split_json_args(s: str) -> list[str]:
    """Split a string like '{...}, {...}' into separate JSON chunks."""
    parts = []
    depth = 0
    start = 0
    in_str = False
    for i, c in enumerate(s):
        if c == '"' and (i == 0 or s[i - 1] != "\\"):
            in_str = not in_str
        if in_str:
            continue
        if c in ("{", "["):
            depth += 1
        elif c in ("}", "]"):
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(s[start:i].strip())
            start = i + 1
    parts.append(s[start:].strip())
    return [p for p in parts if p]


# ── Collection API ───────────────────────────────────────────────────


@app.route("/api/docs")
def get_docs() -> Response:
    coll = _coll(request.args.get("coll", "users"))
    return jsonify([_clean(d) for d in coll.find({})])


@app.route("/api/query", methods=["POST"])
def run_query() -> Response | tuple[Response, int]:
    body = _json_body()
    coll = _coll(body.get("coll", "users"))
    query = body.get("query", {})
    t0 = time.perf_counter()
    try:
        plan = coll.explain(query)
        docs = [_clean(d) for d in coll.find(query)]
        ms = round((time.perf_counter() - t0) * 1000, 2)
        return jsonify({"plan": plan, "docs": docs, "count": len(docs), "ms": ms})
    except (ValueError, KeyError, TypeError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/aggregate", methods=["POST"])
def run_aggregate() -> Response | tuple[Response, int]:
    body = _json_body()
    coll = _coll(body.get("coll", "users"))
    pipeline = body.get("pipeline", [])
    if len(pipeline) > MAX_PIPELINE_STAGES:
        return jsonify({"error": f"Pipeline exceeds {MAX_PIPELINE_STAGES} stage limit"}), 400
    t0 = time.perf_counter()
    try:
        results = coll.aggregate(pipeline)
        ms = round((time.perf_counter() - t0) * 1000, 2)
        return jsonify(
            {
                "results": [_clean(d) if isinstance(d, dict) else d for d in results],
                "count": len(results),
                "ms": ms,
            }
        )
    except (ValueError, KeyError, TypeError, NotImplementedError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/insert", methods=["POST"])
def insert_doc() -> Response | tuple[Response, int]:
    body = _json_body()
    coll = _coll(body.get("coll", "users"))
    try:
        r = coll.insert_one(body.get("doc", {}))
        return jsonify({"inserted_id": str(r.inserted_ids[0])})
    except (ValueError, KeyError, TypeError, DuplicateKeyError, ValidationError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/update", methods=["POST"])
def update_docs() -> Response | tuple[Response, int]:
    body = _json_body()
    coll = _coll(body.get("coll", "users"))
    try:
        r = coll.update_many(body.get("query", {}), body.get("update", {}))
        return jsonify({"modified_count": r.modified_count})
    except (ValueError, KeyError, TypeError, ValidationError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/delete", methods=["POST"])
def delete_docs() -> Response | tuple[Response, int]:
    body = _json_body()
    coll = _coll(body.get("coll", "users"))
    try:
        r = coll.delete_many(body.get("query", {}))
        return jsonify({"deleted_count": r.deleted_count})
    except (ValueError, KeyError, TypeError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/stats")
def collection_stats() -> Response:
    coll = _coll(request.args.get("coll", "users"))
    docs = coll.find({})
    idxs = coll.list_indexes()
    return jsonify(
        {
            "doc_count": len(docs) if isinstance(docs, list) else coll.count_documents({}),
            "index_count": len(idxs),
            "indexes": idxs,
        }
    )


# ── Index API ────────────────────────────────────────────────────────


@app.route("/api/indexes")
def list_indexes() -> Response:
    return jsonify(_coll(request.args.get("coll", "users")).list_indexes())


@app.route("/api/indexes", methods=["POST"])
def create_index() -> Response | tuple[Response, int]:
    body = _json_body()
    try:
        kwargs = {"unique": body.get("unique", False)}
        if body.get("expireAfterSeconds") is not None:
            kwargs["expireAfterSeconds"] = body.get("expireAfterSeconds")
        name = _coll(body.get("coll", "users")).create_index(body.get("keys", []), **kwargs)
        return jsonify({"name": name})
    except (ValueError, KeyError, TypeError, DuplicateKeyError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/indexes/<name>", methods=["DELETE"])
def drop_index(name: str) -> Response | tuple[Response, int]:
    try:
        _coll(request.args.get("coll", "users")).drop_index(name)
        return jsonify({"dropped": name})
    except (ValueError, KeyError) as exc:
        return jsonify({"error": str(exc)}), 400


# ── Oplog API ────────────────────────────────────────────────────────


@app.route("/api/oplog")
def get_oplog() -> Response:
    coll = _coll(request.args.get("coll", "users"))
    limit = int(request.args.get("limit", 50))
    return jsonify(coll.get_oplog()[-limit:])


# ── Sync API ─────────────────────────────────────────────────────────


@app.route("/api/sync/status")
def sync_status() -> Response:
    return jsonify(sync_mgr.status())


@app.route("/api/sync/push", methods=["POST"])
def sync_push() -> Response | tuple[Response, int]:
    try:
        sync_mgr.sync_now()
        return jsonify({"ok": True, "status": sync_mgr.status()})
    except (PyMongoError, ConnectionError, OSError, RuntimeError) as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/sync/pull", methods=["POST"])
def sync_pull() -> Response | tuple[Response, int]:
    try:
        sync_mgr.pull()
        return jsonify({"ok": True, "status": sync_mgr.status()})
    except (PyMongoError, ConnectionError, OSError, RuntimeError) as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/sync/start", methods=["POST"])
def sync_start() -> Response:
    sync_mgr.start()
    return jsonify({"ok": True, "status": sync_mgr.status()})


@app.route("/api/sync/stop", methods=["POST"])
def sync_stop() -> Response:
    sync_mgr.stop()
    return jsonify({"ok": True, "status": sync_mgr.status()})


@app.route("/api/remote/docs")
def remote_docs() -> Response | tuple[Response, int]:
    try:
        return jsonify(
            [
                _clean(d)
                for d in remote[DB_NAME][request.args.get("coll", "users")].find({}).limit(200)
            ]
        )
    except PyMongoError as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/remote/insert", methods=["POST"])
def remote_insert() -> Response | tuple[Response, int]:
    body = _json_body()
    doc = body.get("doc", {})
    doc["_lastModified"] = time.time()
    try:
        remote[DB_NAME][body.get("coll", "users")].insert_one(doc)
        return jsonify({"ok": True})
    except PyMongoError as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/schema", methods=["POST"])
def set_schema() -> Response | tuple[Response, int]:
    body = _json_body()
    coll_name = body.get("coll", "users")
    validator = body.get("validator")
    if not validator:
        return jsonify({"error": "validator is required"}), 400
    try:
        client[DB_NAME].create_collection(coll_name, validator=validator)
        return jsonify({"ok": True})
    except (ValueError, KeyError, TypeError, ValidationError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/watch/next")
def watch_next() -> Response | tuple[Response, int]:
    coll_name = request.args.get("coll", "users")
    try:
        event = _watch_stream(coll_name).try_next()
        return jsonify({"event": _clean(event) if event else None})
    except (RuntimeError, StopIteration) as exc:
        return jsonify({"error": str(exc)}), 500


# ── Seed ─────────────────────────────────────────────────────────────

SAMPLE_DOCS = [
    {
        "name": "Alice",
        "age": 34,
        "city": "NYC",
        "dept": "engineering",
        "tags": ["python", "mongodb"],
        "salary": 145000,
    },
    {
        "name": "Bob",
        "age": 28,
        "city": "SF",
        "dept": "engineering",
        "tags": ["js", "react"],
        "salary": 128000,
    },
    {
        "name": "Charlie",
        "age": 40,
        "city": "NYC",
        "dept": "management",
        "tags": ["python", "go"],
        "salary": 175000,
    },
    {
        "name": "Diana",
        "age": 25,
        "city": "LA",
        "dept": "design",
        "tags": ["rust", "figma"],
        "salary": 98000,
    },
    {
        "name": "Eve",
        "age": 31,
        "city": "SF",
        "dept": "engineering",
        "tags": ["python", "ml"],
        "salary": 155000,
    },
    {
        "name": "Frank",
        "age": 36,
        "city": "CHI",
        "dept": "engineering",
        "tags": ["go", "k8s"],
        "salary": 140000,
    },
    {
        "name": "Grace",
        "age": 29,
        "city": "NYC",
        "dept": "data",
        "tags": ["python", "spark"],
        "salary": 135000,
    },
    {
        "name": "Hank",
        "age": 45,
        "city": "SF",
        "dept": "management",
        "tags": ["strategy"],
        "salary": 190000,
    },
    {
        "name": "Ivy",
        "age": 27,
        "city": "LA",
        "dept": "design",
        "tags": ["figma", "css"],
        "salary": 105000,
    },
    {
        "name": "Jack",
        "age": 33,
        "city": "NYC",
        "dept": "engineering",
        "tags": ["java", "spring"],
        "salary": 142000,
    },
]


@app.route("/metrics")
def prometheus_metrics() -> Response:
    """Prometheus-compatible metrics endpoint.

    Exports serverStatus counters in Prometheus text exposition format.
    """
    lines: list[str] = []

    for coll_name in client[DB_NAME].list_collection_names():
        c = _coll(coll_name)
        try:
            stats = c.get_local_collection().storage_stats()
        except (RuntimeError, AttributeError):
            continue
        prefix = f'smongo_collection_{coll_name.replace("-", "_")}'
        lines.append(f'{prefix}_documents_total {stats.get("count", 0)}')
        lines.append(f'{prefix}_data_size_bytes {stats.get("dataSize", 0)}')
        lines.append(f'{prefix}_storage_size_bytes {stats.get("storageSize", 0)}')
        lines.append(f'{prefix}_index_count {stats.get("nindexes", 0)}')
        lines.append(f'{prefix}_total_index_size_bytes {stats.get("totalIndexSize", 0)}')

    sync_st = sync_mgr.status() if sync_mgr else {}
    lines.append(f'smongo_sync_pushed_total {sync_st.get("pushed", 0)}')
    lines.append(f'smongo_sync_pulled_total {sync_st.get("pulled", 0)}')
    lines.append(f'smongo_sync_conflicts_total {sync_st.get("conflicts", 0)}')
    lines.append(f'smongo_sync_errors_total {sync_st.get("errors", 0)}')
    lines.append(f'smongo_sync_running {1 if sync_st.get("running") else 0}')

    return Response("\n".join(lines) + "\n", mimetype="text/plain; version=0.0.4")


@app.route("/api/seed", methods=["POST"])
def seed_data() -> Response:
    coll = _coll("users")
    coll.delete_many({})
    remote[DB_NAME]["users"].drop()
    _coll("departments").delete_many({})
    remote[DB_NAME]["departments"].drop()
    coll.create_index([("age", 1)])
    coll.create_index([("city", 1), ("age", -1)])
    coll.create_index("name", unique=True)
    coll.create_index([("dept", 1)])
    coll.create_index([("salary", -1)])
    coll.insert_many(SAMPLE_DOCS)
    _coll("departments").insert_many(
        [
            {"name": "engineering", "costCenter": "RND"},
            {"name": "management", "costCenter": "OPS"},
            {"name": "design", "costCenter": "DES"},
            {"name": "data", "costCenter": "ANA"},
        ]
    )
    sync_mgr.sync_now()
    return jsonify({"ok": True, "count": len(SAMPLE_DOCS)})


def _auto_seed() -> None:
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
    _coll("departments").delete_many({})
    _coll("departments").insert_many(
        [
            {"name": "engineering", "costCenter": "RND"},
            {"name": "management", "costCenter": "OPS"},
            {"name": "design", "costCenter": "DES"},
            {"name": "data", "costCenter": "ANA"},
        ]
    )
    sync_mgr.sync_now()
    log.info("Seeded %d docs + 5 indexes, pushed to remote", len(SAMPLE_DOCS))


_wire_server: WireServer | None = None


def _start_wire_server() -> None:
    """Start the wire protocol server in the same process, sharing the WiredTiger connection.

    Controlled by environment variables:
      WIRE_PORT  -- TCP port (0 or unset = disabled)
      WIRE_HOST  -- bind address (defaults to BIND_HOST, then 127.0.0.1)

    Failures are logged but never propagate — the web dashboard keeps running
    even if the wire server can't bind.
    """
    global _wire_server
    wire_port = int(os.environ.get("WIRE_PORT", "0"))
    if wire_port == 0:
        return
    wire_host = os.environ.get("WIRE_HOST", os.environ.get("BIND_HOST", "127.0.0.1"))
    try:
        local_client = client.get_local_client()
        _wire_server = WireServer(
            host=wire_host,
            port=wire_port,
            local_client=local_client,
            sync=sync_mgr,
        )
        _wire_server.start()
        log.info("Wire server listening on %s:%d (Compass-ready)", wire_host, wire_port)
    except OSError as exc:
        log.error("Wire server failed to start on %s:%d -- %s", wire_host, wire_port, exc)
        log.error("The web dashboard will continue without wire protocol access.")
        _wire_server = None
    except Exception as exc:
        log.error("Wire server startup error: %s", exc)
        _wire_server = None


if __name__ == "__main__":
    _auto_seed()
    _start_wire_server()
    bind_host = os.environ.get("BIND_HOST", "127.0.0.1")
    if bind_host == "0.0.0.0" and not _API_KEY:
        log.warning(
            "⚠ Server binding to 0.0.0.0 WITHOUT authentication. "
            "Set SMONGO_API_KEY to require Bearer-token auth on all /api/* routes."
        )
    app.run(
        host=bind_host,
        port=int(os.environ.get("BIND_PORT", "5000")),
        debug=False,
    )
