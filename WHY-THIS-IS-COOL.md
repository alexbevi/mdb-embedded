# Why This Is Cool

---

## 1. It's Not a Mock

Most "embedded MongoDB" solutions are mocks. They intercept PyMongo calls, store documents in Python dictionaries, and approximate query behavior with hand-rolled filtering. They break on edge cases. They don't support real aggregation. They don't have indexes. They don't crash-recover.

smongo uses **WiredTiger** -- the same B-Tree storage engine that MongoDB acquired in 2014 and made the default in 3.2. Documents are stored as **native BSON bytes** in WiredTiger B-Tree tables. Writes are **ACID transactions**. Indexes are **real B-Trees** with lexicographically sortable keys. The query planner does **heuristic prefix-scoring index selection**.

When you test against this, you're testing against the real thing. Not an approximation.

---

## 2. One Query Language, Everywhere

Most offline-first or edge architectures force you to use a different database and query language locally. SQLite on the device. Postgres in the cloud. Two schemas, two query languages, two sets of bugs.

smongo lets you write MQL once:

```python
users.find({"city": "NYC", "age": {"$gt": 30}})
```

That query runs identically against:
- A local WiredTiger store on your laptop
- A MongoDB Atlas cluster in the cloud
- A hybrid setup where both exist simultaneously

The `MongoClient` URI is the only thing that changes:

```python
client = MongoClient("local://data")                              # embedded
client = MongoClient("mongodb+srv://...")                          # Atlas
client = MongoClient("local://data", sync="mongodb+srv://...")     # both
```

Same API. Same operators. Same aggregation pipeline. Same index semantics.

---

## 3. Real Drivers Can Connect

The wire protocol server isn't a toy. It speaks OP_MSG (opcode 2013) -- the actual binary protocol that every MongoDB driver uses. Start the server and connect `mongosh`:

```bash
$ python -m smongo.wire --port 27017
$ mongosh mongodb://localhost:27017
```

Or connect PyMongo. Or the Node.js driver. Or the Go driver. Or Compass. Any MongoDB tool works because the protocol is the real one.

The server handles 80+ commands: `find`, `insert`, `update`, `delete`, `aggregate`, `createIndexes`, `listCollections`, `findAndModify`, `getMore`, `killCursors`, and more. It advertises wire version 0-21, 16MB max BSON object size, and 48MB max message size -- matching production MongoDB's capabilities.

This means you can use smongo as a **development server** for applications written in any language. Not just Python.

---

## 4. Bidirectional Sync That Actually Works

Local-first databases are easy until you need to sync. Then you discover the hard problems: conflict resolution, echo prevention, checkpoint persistence, partial failure recovery, selective filtering, and backoff strategies.

smongo's `SyncManager` solves all of them:

- **Push**: Tail the local oplog, batch `bulk_write` to Atlas. Checkpoint after each batch. Auto-compact the oplog after successful push.
- **Pull**: MongoDB Change Streams with resume token persistence (preferred), or timestamp-based polling (fallback). Initial full snapshot on first pull.
- **Conflict resolution**: Last-write-wins, local-wins, remote-wins, field-level merge, or a custom callable. Field-level merge uses oplog `changed_fields` to merge non-conflicting edits and falls back to per-field LWW for conflicts.
- **Echo prevention**: The `_internal=True` flag on writes from the sync layer suppresses oplog entries, preventing infinite push-pull loops.
- **Exponential backoff**: On consecutive failures, sleep doubles up to 5 minutes. On success, it resets.
- **Selective filters**: Per-collection MQL filters control which documents sync. Only sync US users: `{"region": "us-east-1"}`. Only sync large orders: `{"$expr": {"$gt": ["$total", 100]}}`.

The sync state is checkpointed in a dedicated WiredTiger table (`table:__sync_checkpoint`), so it survives crashes and restarts.

---

## 5. The Aggregation Pipeline Is Real

This isn't a handful of `$match` and `$group` stages bolted on as an afterthought. The pipeline engine supports 25+ stages:

`$match`, `$group`, `$project`, `$sort`, `$limit`, `$skip`, `$unwind`, `$lookup`, `$addFields`/`$set`, `$count`, `$replaceRoot`, `$sample`, `$vectorSearch`, `$facet`, `$out`, `$merge`

With 17 group accumulators: `$sum`, `$avg`, `$min`, `$max`, `$push`, `$addToSet`, `$first`, `$last`, `$firstN`, `$lastN`, `$stdDevPop`, `$stdDevSamp`, `$mergeObjects`, `$top`, `$bottom`, `$topN`, `$bottomN`

And a full expression engine: conditionals (`$cond`, `$ifNull`, `$switch`), string ops (`$concat`, `$toUpper`, `$toLower`), array ops (`$filter`, `$arrayElemAt`, `$concatArrays`), arithmetic (`$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`), comparisons, booleans, and type introspection.

You can build analytics dashboards, denormalized views, materialized aggregations, and semantic search -- all running locally with zero network dependency.

---

## 6. Vector Search Runs In-Process

`$vectorSearch` is a first-class aggregation stage:

```python
db.articles.aggregate([{
    "$vectorSearch": {
        "path": "embedding",
        "queryVector": [0.1, 0.2, ...],
        "limit": 5,
        "metric": "cosine",
        "filter": {"published": True}
    }
}])
```

No vector database. No network round-trip. No API key. Embeddings stored alongside documents, searched in-memory with NumPy (brute-force) or USearch (approximate nearest neighbor).

For edge AI applications -- RAG on a laptop, semantic search on a device, local document retrieval -- this means the entire stack runs locally.

---

## 7. ACID Transactions Are Not Optional

Every write -- insert, update, delete -- is wrapped in a WiredTiger transaction. Data table, index tables, and oplog are committed or rolled back as a single atomic unit.

Unique index violation on the third of five indexes? All three index writes are rolled back. The data table write is rolled back. The oplog entry is never written. The collection is untouched.

This isn't a nice-to-have. It's what prevents data corruption in concurrent, crash-prone environments -- exactly the environments where an embedded database lives.

---

## 8. Lazy Reads -- No Wasted Work

Most embedded databases materialize every matching document into a list, even when you only need the first one. A `find_one()` on a million-document collection deserializes a million BSON blobs just to return `[0]`.

smongo's read path is **streaming**. `Collection.find()` returns a `Cursor` backed by a `StreamingCursor` that pulls documents from WiredTiger one at a time:

- `find({}).limit(10)` → deserializes **exactly 10** BSON documents, not the entire collection
- `find_one({"city": "NYC"})` → deserializes **exactly 1** document, then stops
- `count_documents({})` → iterates the WiredTiger cursor **without any BSON deserialization**

The streaming cursor uses the same query planner as everything else -- PK lookups, index scans, `$in` multi-point seeks, and `$or`-unions all stream lazily. No intermediate lists. No wasted CPU cycles.

---

## 9. The Query Planner Accelerates Writes Too

In most embedded databases, `update({"_id": "abc"}, ...)` scans every document looking for the one with `_id` = `"abc"`. That's O(n).

smongo's write path routes through the same query planner that serves reads:

- `update({"_id": "abc"}, ...)` → **PK lookup**: O(log n)
- `update({"email": "alice@..."}, ...)` with `email_1` index → **index scan**: O(log n + k)
- `delete({"status": "expired"})` with no index → **collection scan**: O(n)

The planner doesn't just accelerate reads. It accelerates every write that targets a subset of documents.

---

## 10. The Oplog Makes Everything Possible

Every mutation -- every insert, update, delete, index create, index drop -- is append-logged to a WiredTiger oplog table with timestamps, version counters, checksums, and changed-field tracking.

This oplog enables:
- **Sync**: The push path tails the oplog to know what changed since the last checkpoint
- **Change streams**: `collection.watch()` tails the oplog and emits MongoDB-compatible events
- **Conflict resolution**: Version counters and timestamps detect and resolve conflicts
- **Field-level merge**: The `changed_fields` list lets the sync layer merge non-conflicting edits from different sources
- **Audit trail**: Every mutation is inspectable in the web dashboard

The oplog supports **compaction** to bound disk growth: `compact_oplog(keep=1000)` or automatic truncation after sync push.

---

## 11. ObjectId Is Spec-Compliant

Not a UUID. Not a random string. A proper 12-byte MongoDB ObjectId:

```
┌──────────┬──────────────┬──────────────┐
│ 4 bytes  │   5 bytes    │   3 bytes    │
│timestamp │ random value │  counter     │
│(seconds) │ (per-process)│ (mod 2^24)   │
└──────────┴──────────────┴──────────────┘
```

Timestamp-prefixed for natural insertion-order sorting in WiredTiger's B-Tree. Thread-safe counter for uniqueness within a process. Random bytes for uniqueness across processes.

`generation_time` extracts the creation timestamp from any ObjectId -- the same API as PyMongo's ObjectId.

---

## 12. Schema Validation at the Edge

`$jsonSchema` validation enforces document structure on every insert and update:

```python
db.create_collection("users", validator={
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["name", "email"],
        "properties": {
            "name": {"bsonType": "string", "minLength": 1},
            "email": {"bsonType": "string", "pattern": "^.+@.+$"},
            "age": {"bsonType": "int", "minimum": 0, "maximum": 150}
        },
        "additionalProperties": False
    }
})
```

Validation errors include the full dot-path to the failing field. Invalid documents are rejected before the WiredTiger transaction starts, so they never touch disk.

This matters for edge deployments where you can't trust the input source. The schema enforces data quality locally, and clean data syncs to the cloud.

---

## 13. Runtime dependencies (no MongoDB server)

Only two runtime dependencies: **WiredTiger** for storage and **PyMongo** for BSON encoding. **No running MongoDB server** is required.

For local-only mode:

```bash
pip install wiredtiger pymongo
python -c "from smongo import MongoClient; c = MongoClient('local://data')"
```

No running `mongod`. No Docker container. No network connection. No Atlas account for the embedded engine itself. WiredTiger ships as a Python package; PyMongo supplies BSON encode/decode.

Add `flask` for the dashboard. Add `numpy` and `usearch` for vector search. Sync to Atlas uses the remote URI you pass to `MongoClient`.

---

## 14. 960+ Tests Across Every Layer

The test suite covers the full stack:

| Module | Tests | What's covered |
|---|---|---|
| `test_query.py` | Query compilation, all operators, dot-notation, update engine, expressions |
| `test_storage.py` | CRUD, transactions, thread safety, BSON roundtrip, TTL, find_one_and_* |
| `test_streaming.py` | StreamingCursor (all plan types), lazy Cursor, find_one, count, islice, parity |
| `test_index.py` | Key encoding, index CRUD, query planner scoring, index scan execution |
| `test_aggregation.py` | All 25+ stages, cursor chaining, projection, multi-stage pipelines |
| `test_oplog.py` | Append, read, compaction, change streams |
| `test_sync_unit.py` | All conflict strategies, collection discovery, backoff, selective filters |
| `test_client.py` | Client routing, database/collection facades, bulk write |
| `test_objectid.py` | Construction, parsing, timestamp extraction, ordering |
| `test_schema.py` | All $jsonSchema constraints |
| `test_wire_*.py` | Message framing, 80+ commands, cursor batching, BSON codec, PyMongo integration |

Plus integration tests against real MongoDB via Docker and benchmark suites for performance regression.

---

## The Big Picture

smongo takes the most widely-used document database in the world and makes it run locally -- with full query fidelity, real storage, ACID transactions, and bidirectional sync. It doesn't approximate MongoDB. It doesn't mock it. It runs the same engine, speaks the same protocol, and syncs to the same cloud.

That's what makes it cool.
