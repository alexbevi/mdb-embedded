# smongo

**Small MongoDB. Big ambitions.**

MongoDB's document model and MQL are the most productive way to work with data -- but only if you can use them *everywhere*. Cloud, edge, laptop, airplane mode, CI pipeline, embedded device. **smongo** makes that real: a local-first MongoDB engine in Python, powered by WiredTiger (the same storage engine family that runs MongoDB itself), with bidirectional sync to Atlas when you're ready.

Write your app once. Run it against a local B-Tree. Ship it against Atlas. The query language never changes. The "S" stands for Small. The rest is all Mongo.

```
"Same everywhere" -- the architectural bet that the local engine, the query
language, the wire protocol, and the cloud database should all be the same
thing, with no translation layer in between.
```

```python
from smongo import MongoClient

# Flip the URI -- same code, different backend
client = MongoClient("local://data")                              # embedded WiredTiger
# client = MongoClient("mongodb+srv://...")                        # Atlas / any mongod
# client = MongoClient("local://data", sync="mongodb+srv://...")   # local-first + auto sync

db = client["myapp"]
users = db["users"]

users.insert_one({"name": "Alice", "age": 34, "city": "NYC"})
users.create_index([("city", 1), ("age", -1)])

for doc in users.find({"city": "NYC", "age": {"$gt": 30}}):
    print(doc["name"])

results = users.aggregate([
    {"$group": {"_id": "$city", "avg_age": {"$avg": "$age"}}},
    {"$sort": {"avg_age": -1}},
])
```

---

## Why smongo?

| Problem | How smongo fixes it |
|---|---|
| Local dev requires a running `mongod` or Docker container | Embedded WiredTiger -- only two runtime deps (WiredTiger + PyMongo for BSON). No `mongod` required |
| `mongomock` doesn't support real aggregation pipelines | Full pipeline engine: 25+ stages incl. `$facet`, `$merge`, `$out`, `$vectorSearch`, `$lookup` with 17 group accumulators |
| Edge / offline-first apps need a different DB and query language | Same MQL everywhere -- one codebase, portable across environments |
| Syncing local state to the cloud is a custom nightmare | Built-in oplog-driven bidirectional sync with metrics, backoff, selective filters, and conflict resolution |
| Mock databases don't have indexes or query planners | Real B-Tree indexes with a heuristic prefix-scoring query planner that accelerates reads *and* writes |
| Embedded databases lack ACID writes or thread safety | WiredTiger transactions wrap every write (data + indexes + oplog), per-collection ReadWriteLock allows concurrent reads while serializing writes |

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                    Your Application                     │
│              from smongo import MongoClient              │
└────────────────────┬───────────────────────────────────┘
                     │  URI routing
          ┌──────────┴──────────┐
          ▼                     ▼
   local://path          mongodb://host
          │                     │
   ┌──────┴──────┐       ┌─────┴─────┐
   │  Embedded   │       │  PyMongo  │
   │   Engine    │       │  Driver   │
   │             │       └───────────┘
   │  ┌───────┐  │
   │  │ MQL   │  │  ◄── compile_query, apply_update
   │  │Compiler│  │      $gt $lt $in $ne $or $and ...
   │  └───┬───┘  │
   │      │      │
   │  ┌───┴───┐  │
   │  │ Query │  │  ◄── heuristic prefix-scoring plan selection
   │  │Planner│  │      index scan / pk lookup / coll scan
   │  └───┬───┘  │
   │      │      │
   │  ┌───┴───┐  │
   │  │B-Tree │  │  ◄── WiredTiger-backed indexes
   │  │Indexes│  │      single, compound, unique, sparse
   │  └───┬───┘  │
   │      │      │
   │  ┌───┴───┐  │
   │  │WiredTi│  │  ◄── same engine family as MongoDB
   │  │  ger  │  │      key=_id, value=BSON (transactional)
   │  └───┬───┘  │
   │      │      │
   │  ┌───┴───┐  │       ┌──────────────┐
   │  │ Oplog │  │──────►│  SyncManager  │──► Atlas
   │  └───────┘  │       │  push / pull  │
   └─────────────┘       │  conflict res │
                         └──────────────┘
```

---

## Features

### Storage -- WiredTiger B-Trees with Streaming Reads
MongoDB acquired WiredTiger in 2014 and made it the default storage engine. smongo uses the same technology locally: documents are stored as **native BSON bytes** in WiredTiger B-Tree tables keyed by `_id`. Every write is wrapped in a **WiredTiger transaction** (data + indexes + oplog in a single atomic unit), a **per-collection ReadWriteLock** ensures thread safety with concurrent reader access, and the **query planner accelerates writes** (update/delete by `_id` or indexed field are O(log n), not O(n)). ACID atomicity, crash recovery, and efficient disk I/O -- for free.

**Reads are lazy.** `Collection.find()` returns a chainable `Cursor` backed by a `StreamingCursor` that pulls documents from WiredTiger one at a time. The streaming cursor consults the query planner and executes the optimal strategy (PK lookup, index scan, `$in` multi-point scan, `$or`-union, or collection scan) -- all lazily. Chained `.limit(10)` without `.sort()` deserializes only 10 documents from BSON regardless of how many match. `find_one()` and `count_documents()` use the same streaming path so they never build intermediate lists.

### MQL Compiler
A pure-Python compiler translates MongoDB query dictionaries into executable predicates. Supported query operators: `$gt`, `$lt`, `$gte`, `$lte`, `$eq`, `$ne`, `$in`, `$nin`, `$exists`, `$regex`, `$not`, `$nor`, `$all`, `$elemMatch`, `$size`, `$type`, `$or`, `$and`. Update operators: `$set`, `$inc`, `$push`, `$unset`, `$addToSet`, `$pull`, `$pop`, `$min`, `$max`, `$rename`, `$currentDate`, `$mul`. Dot-notation paths work everywhere (`"address.city"`).

### Aggregation Pipeline
In-memory pipeline execution with 25+ stages: `$match`, `$group`, `$project`, `$sort`, `$limit`, `$skip`, `$unwind`, `$lookup`, `$graphLookup`, `$unionWith`, `$addFields`/`$set`, `$count`, `$replaceRoot`/`$replaceWith`, `$sample`, `$bucket`, `$bucketAuto`, `$sortByCount`, `$redact`, `$setWindowFields`, `$unset`, `$vectorSearch`, `$facet`, `$out`, `$merge`. Memory-bounded with spill-to-disk for `$sort` and `$group` when `allowDiskUse=True`. Group accumulators: `$sum`, `$avg`, `$min`, `$max`, `$push`, `$addToSet`, `$first`, `$last`, `$firstN`, `$lastN`, `$stdDevPop`, `$stdDevSamp`, `$mergeObjects`, `$top`, `$bottom`, `$topN`, `$bottomN`.

`$vectorSearch` runs fully in memory with:
- **USearch** (`usearch`) for fast RAM-native vector indexing/search
- **NumPy** fallback when USearch is unavailable

`$facet` runs independent sub-pipelines against the same input. `$out` replaces a target collection's contents. `$merge` upserts into a target collection with `whenMatched`/`whenNotMatched` semantics.

Build analytics and similarity queries that run locally with no external vector DB.

### B-Tree Indexes & Query Planner
Create single-field, compound, unique, and sparse indexes backed by dedicated WiredTiger tables. The query planner scores candidate indexes and picks the optimal execution path:
- **Index Scan** -- range or equality scan on the best-matching index
- **PK Lookup** -- O(log n) direct `_id` fetch
- **Collection Scan** -- fallback full-table scan

Sortable key encoding (IEEE 754 bit-flipping for numbers, hex inversion for descending fields) ensures correct lexicographic ordering across mixed types.

### Oplog (Operations Log)
Every mutation (insert, update, delete, index create/drop) is append-logged to a dedicated WiredTiger table with timestamps, version counters, and checksums. The oplog supports **compaction** (`compact_oplog(keep=N)`) to bound growth in long-running deployments, and auto-compacts after successful sync push cycles.

### Bidirectional Sync
`SyncManager` syncs local state to any MongoDB-compatible remote:
- **Push**: tail the oplog, batch `bulk_write` to remote, auto-compact after checkpoint
- **Pull**: change streams (preferred) or timestamp-based polling, merge remote changes locally
- **Index sync**: index definitions flow both directions
- **Conflict resolution**: Last-Write-Wins, local-wins, remote-wins, field-level merge, or a custom callable
- **Checkpointing**: survives crashes and restarts via a WiredTiger checkpoint table
- **Auto-sync**: background thread with configurable interval
- **Hybrid mode**: `MongoClient("local://...", sync="mongodb+srv://...")` auto-registers and starts sync
- **Exponential backoff**: on consecutive failures, backoff doubles up to 300s
- **Sync metrics**: `status()` returns `pushed`, `pulled`, `conflicts`, `errors` counters and a `state` field
- **Selective sync filters**: per-collection MQL filters control which documents are pushed/pulled

### Wire Protocol Server
smongo speaks the real MongoDB binary protocol (OP_MSG, OP_COMPRESSED, OP_QUERY). Point `mongosh`, PyMongo, Compass, or any MongoDB driver at `localhost:27017` and they'll talk to the embedded engine as if it were a real `mongod`. The Docker Compose setup exposes the wire server on port 27018 alongside the web dashboard -- `docker compose up` and connect Compass immediately. Small database, real protocol.

### Interactive Web Dashboard
A full-featured GUI at `localhost:5000` with:

| Tab | What it does |
|---|---|
| **Shell** | mongosh-compatible terminal -- `db.users.find({})`, `db.users.aggregate([...])`, arrow-key history, execution timing |
| **Documents** | Browse, insert, delete docs in a rich table with formatted values |
| **Find & Query** | Clickable query chips, plan badges (INDEX SCAN / COLL SCAN / PK LOOKUP), timing |
| **Aggregation** | Visual pipeline builder with drag stages, pre-built example pipelines |
| **Indexes** | List, create, drop B-Tree indexes; index template chips; query plan tester |
| **Sync** | Live visualization of local <-> remote, push/pull controls, remote client simulator, conflict metrics |
| **Oplog** | Color-coded mutation log with timestamps and version numbers |

---

## Quick Start

### Docker Compose (recommended)

```bash
docker compose up --build
# open http://localhost:5000         -- web dashboard
# Compass: mongodb://localhost:27018 -- wire protocol (browse with Compass)
```

This starts a MongoDB container (stands in for Atlas), the smongo dashboard, and a wire protocol server. Compass connects to `localhost:27018` out of the box. Sample data is auto-seeded on first run: 10 employees, 5 indexes, everything synced. See [SMONGO-COMPASS.md](SMONGO-COMPASS.md) for the full Compass guide.

### Standalone (no Docker, no network)

```bash
pip install wiredtiger pymongo flask numpy usearch
python demo.py
```

Runs the full embedded engine locally -- indexes, queries, aggregation, oplog -- no MongoDB server; core runtime uses WiredTiger and PyMongo (demo extras above add dashboard and vector search).

---

## Wire Protocol Server

smongo includes a wire protocol server so that **real drivers** can connect to the embedded engine over TCP.

```bash
# Start the server on the default port
python -m smongo.wire --port 27017

# Or with the installed entry point
smongo-wire --port 27017
```

Then connect with any standard MongoDB client:

```bash
mongosh mongodb://localhost:27017/mydb
```

```python
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017")
db = client["mydb"]
db["things"].insert_one({"hello": "wire protocol"})
```

Or use the `WireServer` API directly in Python:

```python
from smongo.wire import WireServer

with WireServer("./data", port=27017) as srv:
    input("Press Enter to stop...")  # __enter__ starts the server; __exit__ stops it
```

---

## Project Structure

```
smongo/
  __init__.py        MongoClient, SyncManager, DuplicateKeyError,
                     InsertOne, UpdateOne, UpdateMany,
                     DeleteOne, DeleteMany, ReplaceOne, BulkWriteResult
  client.py          URI-based routing, bulk_write, find_one_and_* facade
  storage/           WiredTiger-backed storage engine package
    engine.py          LocalClient, LocalDB
    collection.py      LocalCollection (BSON, txns, locks, streaming find/count)
    locking.py         ReadWriteLock
    results.py         InsertResult, UpdateResult, DeleteResult
    streaming.py       StreamingCursor (lazy iteration, all plan types)
    helpers.py         BSON encode/decode helpers
  query/             MQL compiler package
    compiler.py        compile_query, query operators
    update.py          apply_update, positional operators
    expressions.py     resolve_expr, 60+ expression operators
    paths.py           get_value, set_value, unset_value
  aggregation/       Pipeline engine package (25+ stages)
    cursor.py          Cursor class (lazy Iterable input), aggregate dispatch
    stages.py          Core stages: $match, $group, $sort, etc.
    joins.py           $lookup, $graphLookup, $unionWith
    output.py          $facet, $out, $merge
    vector.py          $vectorSearch (NumPy / USearch)
  index.py           B-Tree index manager + query planner
  oplog.py           Append-only operations log with compaction
  sync.py            Bidirectional sync with metrics, backoff, selective filters
  objectid.py        MongoDB-style ObjectId implementation
  schema.py          $jsonSchema validation layer
  wire/              MongoDB binary protocol server (OP_MSG, OP_COMPRESSED)
    commands/          80+ command handlers
    sessions.py        Session registry
    transactions.py    Transaction state, undo journal
    profiler.py        Profiler, OpTracker, TopStats

web_app.py           Flask API + shell endpoint
templates/
  index.html         Single-page dashboard
static/              CSS, JS assets for dashboard

examples/
  basic/
    01_crud.py           Insert, find, update, delete, cursor chaining
    02_indexes.py        B-tree indexes, query planner, unique constraints
    03_aggregation.py    $group, $sort, $project, $unwind, $lookup, $facet
    04_streaming.py      Lazy reads: find_one, count, limit short-circuit
    05_schema_validation.py  $jsonSchema enforcement on insert and update
    06_bulk_write.py     Batch InsertOne, UpdateOne, ReplaceOne, DeleteOne
    07_change_streams.py Real-time watch() + raw oplog inspection
    08_advanced_queries.py $or, $regex, $elemMatch, dot-notation, $not, $all
  patterns/
    ecommerce.py         Shopping cart, orders, revenue analytics, dashboards
    iot_timeseries.py    1000+ sensor readings, anomaly detection, facility stats
    content_cms.py       Blog CMS: tagging, search, author leaderboard, facets

demo.py              Standalone CLI demo (no Docker needed)
Dockerfile           Python 3.11 + WiredTiger build deps
docker-compose.yml   App + MongoDB for the full sync experience
```

---

## Dev Commands

```bash
make install-test   # install test/lint dependencies
make lint           # ruff checks
make format         # ruff formatter
make test           # unit suite (960+ tests)
make integration    # docker-backed integration suite
make perf           # benchmark suite
make coverage       # coverage report (85%+ enforced)
make typecheck      # mypy strict
```

---

## The API

```python
from smongo import MongoClient, InsertOne, UpdateOne, DeleteOne

client = MongoClient("local://data")
db = client["mydb"]
coll = db["things"]

# CRUD
coll.insert_one({"x": 1})
coll.insert_many([{"x": 2}, {"x": 3}])
coll.find({"x": {"$gt": 1}})
coll.find_one({"x": 2})
coll.update_one({"x": 1}, {"$set": {"x": 10}})
coll.update_many({}, {"$inc": {"x": 1}})
coll.delete_one({"x": 2})
coll.delete_many({"x": {"$lt": 5}})
coll.count_documents({"x": {"$gte": 1}})

# Atomic find-and-modify
coll.find_one_and_update({"x": 1}, {"$set": {"x": 10}}, return_document="after")
coll.find_one_and_replace({"x": 1}, {"x": 99, "replaced": True})
coll.find_one_and_delete({"x": 99})

# Bulk writes
coll.bulk_write([
    InsertOne({"x": 100}),
    UpdateOne({"x": 100}, {"$set": {"x": 200}}),
    DeleteOne({"x": 3}),
])

# Indexes
coll.create_index([("x", 1)])
coll.create_index("name", unique=True)
coll.create_index([("city", 1), ("age", -1)])
coll.list_indexes()
coll.drop_index("x_1")
coll.explain({"x": {"$gt": 5}})

# Aggregation
coll.aggregate([
    {"$match": {"status": "active"}},
    {"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}},
    {"$sort": {"total": -1}},
    {"$limit": 10},
])

# $facet -- run parallel sub-pipelines
coll.aggregate([
    {"$facet": {
        "by_dept": [{"$group": {"_id": "$dept", "count": {"$sum": 1}}}],
        "top_5":   [{"$sort": {"salary": -1}}, {"$limit": 5}],
    }},
])

# $merge -- upsert results into another collection
coll.aggregate([
    {"$group": {"_id": "$dept", "avg_salary": {"$avg": "$salary"}}},
    {"$merge": {"into": "dept_stats", "on": "_id", "whenMatched": "replace"}},
])

# Transparent hybrid sync
hybrid = MongoClient("local://data", sync="mongodb+srv://user:pass@cluster.mongodb.net")
hybrid.sync.status()   # includes pushed, pulled, conflicts, errors, state
hybrid.sync.sync_now()
```

---

## License

See [LICENSE](LICENSE).
