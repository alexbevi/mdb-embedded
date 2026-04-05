# Local-First MongoDB: The Offline-First Story

**How smongo turns every device into a fully autonomous MongoDB node that syncs to Atlas when it feels like it.**

---

## The Pitch

What if your application never waited for the network? What if every read was a local B-tree lookup, every write landed on local disk in microseconds, and the cloud was just an eventually-consistent mirror that caught up whenever connectivity existed?

That's not a hypothetical. That's smongo in hybrid mode.

```python
from smongo import MongoClient

client = MongoClient(
    "local://./my_data",
    sync="mongodb+srv://user:pass@cluster.mongodb.net",
)

db = client["myapp"]
users = db["users"]

# This write hits local WiredTiger. No network. No latency.
users.insert_one({"name": "Alice", "role": "admin", "region": "us-east"})

# This read scans a local B-tree index. Sub-millisecond.
users.find({"region": "us-east", "role": "admin"}).limit(10)

# Meanwhile, a background thread is quietly pushing your mutations to Atlas
# and pulling remote changes back. You never think about it.
```

One constructor argument. That's the difference between "local database" and "distributed data system with bidirectional cloud sync."

---

## The Architecture: Two Write Paths, One Truth

Traditional client-server databases put the network between you and your data. Every read is a round trip. Every write is a round trip. Go offline and you go dark.

smongo inverts this. Your data lives **with you** -- on disk, in WiredTiger B-trees, with ACID transactions, real indexes, and the full MongoDB query language. The cloud is a replication target, not a dependency.

```
┌────────────────────────────────────────────────────────────────────┐
│                        Your Application                            │
│                                                                    │
│   MongoClient("local://data", sync="mongodb+srv://atlas")          │
└───────────┬──────────────────────────────────────┬─────────────────┘
            │                                      │
            ▼                                      ▼
   ┌─────────────────┐                    ┌─────────────────┐
   │  Local Engine    │                    │  SyncManager    │
   │                  │    oplog tailing   │  (background)   │
   │  WiredTiger      │◄──────────────────►│                 │
   │  B-tree storage  │                    │  PUSH ──► Atlas │
   │  ACID txns       │                    │  PULL ◄── Atlas │
   │  Real indexes    │                    │                 │
   │  Full MQL        │                    │  Conflict       │
   │  Wire protocol   │                    │  resolution     │
   │  Change streams  │                    │  Checkpointing  │
   └─────────────────┘                    └─────────────────┘
         ▲                                         ▲
         │ TCP :27017                               │ pymongo
         │                                         │
   ┌─────┴──────┐                          ┌───────┴───────┐
   │ Local      │                          │ MongoDB Atlas │
   │ clients    │                          │ (or any       │
   │ (pymongo,  │                          │  mongod)      │
   │  mongosh,  │                          └───────────────┘
   │  Compass,  │
   │  LangChain)│
   └────────────┘
```

### What happens when you write

1. Your `insert_one` / `update_one` / `delete_one` hits the local Rust engine.
2. WiredTiger wraps the mutation in a transaction: data table + index tables + oplog entry, atomically.
3. The write returns. You're done. Sub-100μs for a single insert.
4. In the background, the `SyncManager` thread wakes up (default: every 5 seconds), tails the oplog from its last checkpoint, batches mutations into pymongo `bulk_write` operations, and pushes them to Atlas.
5. The oplog checkpoint advances. Successfully pushed entries are compacted.

### What happens when you read

1. Your `find` / `find_one` / `aggregate` hits the local Rust engine.
2. The query planner picks the optimal index (or falls back to collection scan).
3. A `RustStreamingCursor` lazily pulls documents from WiredTiger -- only the documents you actually consume are deserialized from BSON.
4. No network. No round trip. No cold start.

### What happens when you go offline

Nothing changes. Writes keep landing in WiredTiger. The oplog keeps accumulating. The sync thread notices connectivity is gone, backs off exponentially (up to 5 minutes between retries), and waits. When the network returns, it picks up from its last checkpoint and pushes everything that was missed. Zero data loss. Zero manual intervention.

---

## The Wire Protocol: Your Local smongo Is a Real MongoDB

This is where it gets wild. smongo doesn't just have an API that looks like MongoDB -- it speaks the actual MongoDB binary wire protocol. Run the wire server and any MongoDB client in any language connects to it natively:

```bash
# Start smongo's wire protocol server
python -m smongo.wire --port 27017
```

Now any of these work against your local embedded engine:

```bash
# mongosh
mongosh mongodb://localhost:27017/mydb

# PyMongo
python -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017').mydb.users.find_one())"

# MongoDB Compass
# Just point it at mongodb://localhost:27017

# Any MongoDB driver -- Node.js, Go, Java, C#, Ruby...
```

This means **every tool in the MongoDB ecosystem** is a potential local-first client. Your monitoring dashboards, your admin scripts, your ORM layers, your AI frameworks -- if they talk to MongoDB, they can talk to smongo.

### What about LangChain?

LangChain's MongoDB integrations (`MongoDBChatMessageHistory`, `MongoDBAtlasVectorSearch`, etc.) use pymongo under the hood. Point them at `mongodb://localhost:27017` instead of an Atlas URI, and they'll read and write against your local engine. Your chat history, your vector embeddings, your document stores -- all local-first, all syncing to Atlas in the background.

The same applies to **any** framework that uses a standard MongoDB driver: Beanie, Motor, Mongoose, mongoid, the Go driver. If it speaks MongoDB wire protocol, it works.

---

## Bidirectional Sync: The Master-Master Story

The `SyncManager` isn't a one-way replication pipe. It's a full bidirectional sync engine with conflict resolution, making smongo behave like a **multi-master eventually-consistent node** in a distributed system.

### Push: Local to Atlas

The push path tails the local oplog and batches mutations into pymongo bulk operations:

- `insert` oplog entry → `InsertOne` with `_lastModified` timestamp
- `update` oplog entry → `UpdateOne` with upsert and `_lastModified` in `$set`
- `delete` oplog entry → `DeleteOne`
- `index_create` / `index_drop` → direct remote index management via pymongo

Operations are flushed in configurable batch sizes. Partial failures are logged, and unflushed entries remain in the oplog for retry. After a successful push, the oplog auto-compacts entries that have been safely delivered.

### Pull: Atlas to Local

The pull path prefers **MongoDB Change Streams** for near-real-time delivery:

1. **First run**: full snapshot via `find({})` on the remote collection
2. **Subsequent runs**: resume from the last change stream token with `full_document="updateLookup"`
3. **Fallback**: if change streams aren't available (standalone `mongod` without a replica set), fall back to timestamp-based polling: `find({_lastModified: {$gt: last_ts}}).sort("_lastModified", 1)`

Pulled documents are merged locally using the configured conflict resolution strategy. Index definitions are also synced -- if a new index appears on Atlas, it's automatically created locally.

### Echo Prevention

When the sync layer writes a pulled document to the local engine, it passes `_internal=True`. This flags the resulting oplog entry so the push path skips it, preventing infinite ping-pong between local and remote.

### Checkpointing: Crash-Safe Progress

All sync state is persisted in a dedicated WiredTiger table (`table:__sync_checkpoint`):

| Key Pattern | Value | Purpose |
|---|---|---|
| `push:{namespace}` | Last oplog key pushed | Resume push from here |
| `pull_cs_init:{namespace}` | `"1"` when complete | Track initial snapshot |
| `pull_cs_token:{namespace}` | JSON resume token | Resume change stream |
| `pull_ts:{namespace}` | Timestamp string | Resume timestamp polling |

Kill the process mid-sync, restart it, and the `SyncManager` picks up exactly where it left off.

---

## Conflict Resolution: When Two Masters Disagree

In any multi-master system, the same document can be modified on both sides before sync catches up. smongo handles this with four built-in strategies and support for custom logic.

### Last-Write-Wins (LWW) -- Default

Compare `_lastModified` timestamps. The newer document wins. Simple, deterministic, and correct for the vast majority of applications.

```python
client = MongoClient("local://data", sync="mongodb+srv://...", sync_config={
    "conflict_resolution": "lww",
})
```

### Local Wins

The local document always takes precedence. Ideal for edge devices that are the **authoritative source** -- IoT sensors, point-of-sale terminals, field equipment.

```python
sync_config={"conflict_resolution": "local_wins"}
```

### Remote Wins

The cloud document always takes precedence. Ideal for **read caches** where the cloud is the source of truth and local is just a fast mirror.

```python
sync_config={"conflict_resolution": "remote_wins"}
```

### Field-Level Merge

The most sophisticated strategy. Instead of choosing an entire document, it merges at the field level:

- Fields changed **only locally** → keep local value
- Fields changed **only remotely** → keep remote value
- Fields changed **on both sides** → fall back to per-field LWW using `_lastModified`
- Fields unchanged on either side → take the latest known state

This means two users can update different fields of the same document concurrently and both changes survive.

```python
sync_config={"conflict_resolution": "field_merge"}
```

### Custom Callable

For domain-specific logic, pass any function:

```python
def my_resolver(local_doc, remote_doc):
    # Your business logic here
    if local_doc.get("priority") > remote_doc.get("priority"):
        return local_doc
    return remote_doc

sync_config={"conflict_resolution": my_resolver}
```

### CRDT Support

For data types that can be merged mathematically, smongo supports CRDT-annotated fields:

```python
sync_config={
    "crdt_fields": {
        "view_count": "counter",    # grow-only counter: max(local, remote)
        "tags": "set",              # OR-Set: union of both sides
    }
}
```

Counter fields resolve to `max(local, remote)`. Set fields resolve to the union. No conflict, no data loss.

### Vector Clocks

Under the hood, the sync layer maintains per-document vector clocks for causal ordering across replicas. Each writer (identified by `node_id`) maintains a monotonic counter. Two events are concurrent when neither dominates the other -- that's when conflict resolution kicks in.

---

## Selective Sync: Not Everything Needs to Go Everywhere

Not every document belongs in every location. The sync layer supports per-collection MQL filters that control what gets pushed and pulled:

```python
sync_config={
    "collections": {
        "mydb.users": {"region": "us-east-1"},           # only sync US East users
        "mydb.orders": {"total": {"$gt": 100}},          # only sync large orders
        "mydb.telemetry": None,                           # sync all telemetry
    }
}
```

An edge node in a retail store only syncs its own store's data. A regional gateway only syncs its region. A mobile device only syncs the logged-in user's documents. The filter is compiled MQL -- the same query language you already know.

---

## Sync Modes: Choose Your Topology

| Mode | Push | Pull | Use Case |
|---|---|---|---|
| `bidirectional` | Yes | Yes | Full two-way sync (default) |
| `push_only` | Yes | No | Edge device publishing data upstream |
| `pull_only` | No | Yes | Local read cache of cloud data |

Mix these across a fleet:

- **Field sensors**: `push_only` -- they generate data, never need to read from the cloud
- **Dashboard servers**: `pull_only` -- they display cloud data locally for fast rendering
- **Mobile apps**: `bidirectional` -- users create and consume data on both sides

---

## Observability: Know What Your Sync Is Doing

```python
status = client.sync.status()
```

```python
{
    "running": True,
    "mode": "bidirectional",
    "state": "online",       # online | syncing | error | offline
    "pending": 0,            # mutations waiting to push
    "last_sync": 1711929600.123,
    "last_error": None,
    "pushed": 1042,          # cumulative documents pushed
    "pulled": 587,           # cumulative documents pulled
    "conflicts": 23,         # conflict resolution invocations
    "errors": 2,             # cumulative sync cycle errors
}
```

Manual controls when you need them:

```python
client.sync.pause()       # pause sync (accumulate oplog)
client.sync.resume()      # resume background sync
client.sync.sync_now()    # force an immediate sync cycle
client.sync.push()        # manual push only
client.sync.pull()        # manual pull only
```

On consecutive failures, the sync thread applies exponential backoff: `min(interval * 2^consecutive_errors, 300s)`. On success, the counter resets. The `state` field transitions to `"error"` during backoff so monitoring can alert.

---

## Real-World Deployment Patterns

### Pattern 1: Offline-First Mobile/Desktop App

```
┌──────────────────────┐         ┌──────────────────┐
│   Desktop / Mobile   │         │  MongoDB Atlas    │
│                      │  sync   │                   │
│   smongo (embedded)  │◄───────►│  Cloud database   │
│   WiredTiger on disk │         │                   │
│   Full MQL locally   │         │  Other clients    │
│                      │         │  also write here  │
└──────────────────────┘         └──────────────────┘
```

The app works fully offline. Users create, read, update, delete. When WiFi or cellular is available, sync catches up. Field-level merge means concurrent edits to different fields don't conflict.

### Pattern 2: Edge Computing / IoT Gateway

```
┌─────────┐ ┌─────────┐ ┌─────────┐
│ Sensor  │ │ Sensor  │ │ Sensor  │
│  Node   │ │  Node   │ │  Node   │
└────┬────┘ └────┬────┘ └────┬────┘
     │           │           │
     ▼           ▼           ▼
┌────────────────────────────────┐         ┌──────────────┐
│       Edge Gateway             │  sync   │              │
│                                │────────►│  Atlas       │
│  smongo wire server :27017     │         │              │
│  Sensors connect via pymongo   │         │  Central     │
│  Local aggregation & alerting  │         │  analytics   │
│  push_only to cloud            │         │              │
└────────────────────────────────┘         └──────────────┘
```

Sensors write to the local smongo instance over the wire protocol (standard pymongo). The gateway runs aggregation pipelines locally for real-time alerting. Summarized data pushes to Atlas for central analytics. If the uplink goes down, the gateway keeps collecting and processing -- nothing is lost.

### Pattern 3: Development / CI Without Infrastructure

```bash
# No Docker. No Atlas account. No network.
pip install smongo
python -c "
from smongo import MongoClient
client = MongoClient('local://test_data')
db = client['mydb']
db['users'].insert_one({'name': 'test', 'role': 'admin'})
print(db['users'].find_one({'role': 'admin'}))
"
```

Integration tests run against the real storage engine with real query semantics. No mocks, no behavioral gaps between test and production. CI pipelines don't need a MongoDB service container.

### Pattern 4: Local AI / LLM Data Layer

```python
from smongo import MongoClient
from smongo.wire import WireServer

# Start the embedded engine + wire server
client = MongoClient("local://ai_data", sync="mongodb+srv://...")
server = WireServer("./ai_data", port=27017)
server.start()

# Now LangChain, LlamaIndex, or any AI framework connects via pymongo
# to mongodb://localhost:27017 -- reads are local, writes sync to Atlas
```

Vector embeddings, chat histories, document stores, RAG retrieval -- all running against local WiredTiger with `$vectorSearch` support (NumPy / USearch). No network latency on the inference hot path. Training data syncs from Atlas. Generated artifacts sync back.

### Pattern 5: Multi-Region / Multi-Site

```
  Site A (NYC)              Site B (London)           Site C (Tokyo)
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│  smongo      │         │  smongo      │         │  smongo      │
│  local://    │         │  local://    │         │  local://    │
│              │         │              │         │              │
│  clients     │         │  clients     │         │  clients     │
│  hit local   │         │  hit local   │         │  hit local   │
│  wire server │         │  wire server │         │  wire server │
└──────┬───────┘         └──────┬───────┘         └──────┬───────┘
       │                        │                        │
       │    bidirectional       │    bidirectional        │
       └────────────┬───────────┴────────────┬───────────┘
                    │                        │
                    ▼                        ▼
              ┌──────────────────────────────────┐
              │         MongoDB Atlas             │
              │                                   │
              │   Hub for all sites               │
              │   Global queries                  │
              │   Cross-site analytics            │
              └──────────────────────────────────┘
```

Each site runs its own smongo instance. Local clients get sub-millisecond reads against local data. Writes sync to Atlas, which acts as the hub. Other sites pull those changes on their next sync cycle. Selective sync filters ensure each site only gets the data it needs.

This isn't true multi-master with linearizable consistency -- it's **eventually consistent multi-writer** with configurable conflict resolution. For many real-world use cases (retail chains, distributed offices, fleet management, multi-region SaaS), that's exactly the right tradeoff.

---

## What Makes This Different

### It's not Realm / Atlas Device Sync

MongoDB's own Atlas Device Sync (formerly Realm Sync) is purpose-built for mobile and has its own SDK, its own data model (Realm objects), and its own conflict resolution (operational transforms). smongo is different:

- **Same query language everywhere** -- MQL, not a subset or a different ORM
- **Same storage engine** -- WiredTiger, not a custom mobile database
- **Same wire protocol** -- any MongoDB driver connects natively
- **No proprietary SDK** -- it's pymongo (or any driver) all the way down
- **You own the sync** -- configurable strategies, custom resolvers, selective filters, CRDT fields

### It's not CouchDB / PouchDB

CouchDB pioneered offline-first with master-master replication, but it has its own query language, its own HTTP API, and its own conflict model (revision trees). smongo gives you the MongoDB ecosystem:

- The query language you already know (MQL)
- The drivers you already use (pymongo, mongosh, Compass)
- The cloud you already run (Atlas)
- The aggregation framework you already depend on (25+ stages)

### It's not SQLite + custom sync

SQLite is an incredible embedded database, but bolting MongoDB-compatible sync onto it means building a translation layer between SQL and MQL, between relational and document, between SQLite's type system and BSON. smongo skips all that -- it's documents in, documents out, WiredTiger underneath, same as the real thing.

---

## The Numbers

The local engine is fast because there's no network and because the hot path runs in Rust:

| Operation | Latency | Ops/sec |
|---|---:|---:|
| `findAndModify` | 64 μs | 15,600 |
| `insert` + `delete` | 91 μs | 11,000 |
| `find` + `limit(10)` | 287 μs | 3,490 |
| `find({})` (200 docs) | 361 μs | 2,770 |
| `count_documents` | 378 μs | 2,650 |
| `aggregate` (3 stages) | 457 μs | 2,190 |

Compare that to a round trip to Atlas (typically 5-50ms depending on region). Local-first isn't just about offline resilience -- it's about **speed**.

---

## Getting Started

### Minimal: embedded, no sync

```python
from smongo import MongoClient

client = MongoClient("local://my_data")
db = client["app"]
db["things"].insert_one({"hello": "world"})
```

### With sync: local-first + Atlas

```python
from smongo import MongoClient

client = MongoClient(
    "local://my_data",
    sync="mongodb+srv://user:pass@cluster.mongodb.net",
    sync_config={
        "mode": "bidirectional",
        "interval_sec": 5,
        "conflict_resolution": "field_merge",
        "collections": {
            "app.users": None,              # sync all users
            "app.orders": {"total": {"$gt": 50}},  # only sync large orders
        },
    },
)

# Use it like normal MongoDB
db = client["app"]
db["users"].insert_one({"name": "Alice", "role": "admin"})

# Check sync status
print(client.sync.status())

# Clean shutdown
client.close()
```

### With wire protocol: local clients connect over TCP

```python
from smongo import MongoClient
from smongo.wire import WireServer

# Embedded engine with sync
client = MongoClient("local://my_data", sync="mongodb+srv://...")

# Wire server so other tools can connect
server = WireServer("./my_data", port=27017)
server.start()

# Now: mongosh, Compass, pymongo, LangChain, any driver → localhost:27017
# All reads are local. All writes sync to Atlas in the background.
```

### Docker Compose: full stack in one command

```bash
docker compose up --build
# Web dashboard:  http://localhost:5000
# Wire protocol:  mongodb://localhost:27018  (connect Compass here)
# MongoDB Atlas:  mongodb://localhost:27017  (bundled container)
```

---

## The Vision

```
One query language.  One document model.  One sync protocol.  Every platform.
```

smongo today is Python + Rust. The roadmap includes multi-language bindings (Node.js via napi-rs, Go via CGo, Ruby via magnus, Java via JNI) and a WebAssembly build that puts the full engine in the browser with OPFS or IndexedDB as the storage backend.

The wire protocol already makes smongo accessible from any language over TCP. Native bindings add in-process embedding with zero network hop. WASM adds the browser. Atlas sync ties them all together.

Every device becomes a MongoDB node. Every node works offline. Every node syncs when it can. The cloud is the rendezvous point, not the bottleneck.

That's the local-first story. And it's already working.
