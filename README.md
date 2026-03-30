# mdb-embedded

**One query language. Every environment. Zero compromises.**

MongoDB's document model and MQL are the most productive way to work with data -- but only if you can use them *everywhere*. Cloud, edge, laptop, airplane mode, CI pipeline, embedded device. `mdb-embedded` makes that real: a local-first MongoDB engine in Python, powered by WiredTiger (the same storage engine family that runs MongoDB itself), with bidirectional sync to Atlas when you're ready.

Write your app once. Run it against a local B-Tree. Ship it against Atlas. The query language never changes.

```python
from mdb_embedded import MongoClient

# Flip the URI -- nothing else changes
client = MongoClient("local://data")          # embedded WiredTiger
# client = MongoClient("mongodb+srv://...")    # Atlas / any mongod

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

## Why this exists

| Problem | How mdb-embedded solves it |
|---|---|
| Local dev requires a running `mongod` or Docker container | Embedded WiredTiger -- zero external dependencies at runtime |
| `mongomock` doesn't support real aggregation pipelines | Full pipeline engine: `$match`, `$group`, `$sort`, `$project`, `$unwind`, `$limit`, `$skip` with `$sum`, `$avg`, `$min`, `$max`, `$push` |
| Edge / offline-first apps need a different database and query language | Same MQL everywhere -- one codebase, portable across environments |
| Syncing local state to the cloud is a custom nightmare | Built-in oplog-driven bidirectional sync with conflict resolution |
| Mock databases don't have indexes or query planners | Real B-Tree indexes with a cost-based query planner that picks index scans, range scans, or collection scans |

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                    Your Application                     │
│           from mdb_embedded import MongoClient          │
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
   │  │ Query │  │  ◄── cost-based plan selection
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
   │  │  ger  │  │      key=_id, value=JSON document
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

### Storage -- WiredTiger B-Trees
MongoDB acquired WiredTiger in 2014 and made it the default storage engine. We use the same technology locally: documents are serialized to JSON and stored in WiredTiger B-Tree tables keyed by `_id`. You get document-level concurrency, crash recovery, and efficient disk I/O for free.

### MQL Compiler
A pure-Python compiler translates MongoDB query dictionaries into executable predicates. Supported operators: `$gt`, `$lt`, `$gte`, `$lte`, `$eq`, `$ne`, `$in`, `$exists`, `$or`, `$and`. Update operators: `$set`, `$inc`, `$push`, `$unset`. Dot-notation paths work everywhere (`"address.city"`).

### Aggregation Pipeline
In-memory pipeline execution with stages: `$match`, `$group`, `$project`, `$sort`, `$limit`, `$skip`, `$unwind`. Group accumulators: `$sum`, `$avg`, `$min`, `$max`, `$push`. Build analytics queries that run identically on local data and against Atlas.

### B-Tree Indexes & Query Planner
Create single-field, compound, unique, and sparse indexes backed by dedicated WiredTiger tables. The query planner scores candidate indexes and picks the optimal execution path:
- **Index Scan** -- range or equality scan on the best-matching index
- **PK Lookup** -- O(log n) direct `_id` fetch
- **Collection Scan** -- fallback full-table scan

Sortable key encoding (IEEE 754 bit-flipping for numbers, hex inversion for descending fields) ensures correct lexicographic ordering across mixed types.

### Oplog (Operations Log)
Every mutation (insert, update, delete, index create/drop) is append-logged to a dedicated WiredTiger table with timestamps, version counters, and checksums. This is the foundation for sync -- and it's inspectable in the dashboard.

### Bidirectional Sync
`SyncManager` syncs local state to any MongoDB-compatible remote:
- **Push**: tail the oplog, batch `bulk_write` to remote
- **Pull**: timestamp-based polling, merge remote changes locally
- **Index sync**: index definitions flow both directions
- **Conflict resolution**: Last-Write-Wins, local-wins, remote-wins, or a custom callable
- **Checkpointing**: survives crashes and restarts via a WiredTiger checkpoint table
- **Auto-sync**: background thread with configurable interval

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
# open http://localhost:5000
```

This starts a MongoDB container (stands in for Atlas) and the mdb-embedded dashboard. Sample data is auto-seeded on first run: 10 employees, 5 indexes, everything synced.

### Standalone (no Docker, no network)

```bash
pip install wiredtiger pymongo flask
python demo.py
```

Runs the full embedded engine locally -- indexes, queries, aggregation, oplog -- with zero external dependencies beyond WiredTiger.

---

## Project Structure

```
mdb_embedded/
  __init__.py        MongoClient, SyncManager, DuplicateKeyError
  client.py          URI-based routing: local:// vs mongodb://
  storage.py         WiredTiger-backed LocalCollection with full CRUD
  query.py           MQL compiler: compile_query, apply_update
  aggregation.py     Pipeline engine: Cursor.aggregate()
  index.py           B-Tree index manager + query planner
  oplog.py           Append-only operations log
  sync.py            Bidirectional sync with conflict resolution

web_app.py           Flask API + shell endpoint
templates/
  index.html         Single-page dashboard

demo.py              Standalone CLI demo (no Docker needed)
Dockerfile           Python 3.11 + WiredTiger build deps
docker-compose.yml   App + MongoDB for the full sync experience
```

---

## The API

```python
from mdb_embedded import MongoClient, SyncManager

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

# Sync
sync = SyncManager(client, "mongodb+srv://user:pass@cluster.mongodb.net",
                   sync_config={"conflict_resolution": "lww"})
sync.register_collection("mydb", "things", coll.get_local_collection())
sync.start()          # background bidirectional sync
sync.sync_now()       # immediate sync cycle
sync.status()         # {"running": True, "pending": 0, ...}
sync.stop()
```

---

## License

See [LICENSE](LICENSE).
