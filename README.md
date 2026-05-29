# smongo

**SQLite for the MongoDB world.**

`pip install smongo` -- an embedded, local-first MongoDB engine built on **[redb](https://github.com/cberner/redb)** and Rust (**smongo-engine**). Same document model, same MQL, same wire protocol. No `mongod`, no Docker, no network. Just `import` and go.

The same ecological niche as SQLite -- embedded, zero-config, in-process -- but for the document model instead of relational. It's not a replacement for `mongod` in production. For local-first apps, dev/test without Docker, edge computing, AI/RAG pipelines, and "same MQL everywhere" architectures, it fills a gap that nothing else quite does.

```
"Same everywhere" -- the architectural bet that the local engine, the query
language, the wire protocol, and the cloud database should all be the same
thing, with no translation layer in between.
```

```python
from smongo import MongoClient

# Flip the URI -- same code, different backend
client = MongoClient("local://data")                              # embedded redb
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

## Documentation

**Design notes:** [ARCHITECTURE.md](ARCHITECTURE.md), [WIRE-PROTOCOL.md](WIRE-PROTOCOL.md), [AGGREGATION.md](AGGREGATION.md), [RUST-PY.md](RUST-PY.md), [ACID-TRANSACTIONS.md](ACID-TRANSACTIONS.md), [QUERY-PLANNER.md](QUERY-PLANNER.md), [MQL-COMPILER.md](MQL-COMPILER.md), [LOCAL-FIRST.md](LOCAL-FIRST.md), [SYNC-NOTES.md](SYNC-NOTES.md), [BYE-BYE-GIL.md](BYE-BYE-GIL.md), [PERF.md](PERF.md). **Zero-FFI pipeline:** [ZERO-FFI-STATUS.md](ZERO-FFI-STATUS.md), [ZERO-FFI-AGGREGATION.md](ZERO-FFI-AGGREGATION.md). **Browser / WASM demos:** [rust/smongo-engine/wasm/README.md](rust/smongo-engine/wasm/README.md).

---

## Why smongo?

| Problem | How smongo fixes it |
|---|---|
| Local dev requires a running `mongod` or Docker container | Embedded **redb** + **smongo-engine** in a Rust extension. No `mongod` required |
| `mongomock` doesn't support real aggregation pipelines | Full pipeline engine: 25+ stages incl. `$facet`, `$merge`, `$out`, `$vectorSearch`, `$lookup` with 17 group accumulators |
| Edge / offline-first apps need a different DB and query language | Same MQL everywhere -- one codebase, portable across environments |
| Syncing local state to the cloud is a custom nightmare | Built-in oplog-driven bidirectional sync with metrics, backoff, selective filters, and conflict resolution |
| Mock databases don't have indexes or query planners | Real B-Tree indexes with a heuristic prefix-scoring query planner that accelerates reads *and* writes |
| Embedded databases lack ACID writes or thread safety | **redb** transactions wrap engine writes (data + indexes + oplog); the stack is thread-safe for typical app use |

---

## Who Is This For?

The key distinction is **in-process / embedded**. If you can run MongoDB Community or Atlas in your environment, you probably should -- smongo is not a MongoDB replacement at scale. It fills a different niche entirely:

| Audience | Why smongo makes sense |
|---|---|
| **Offline-first app developers** | You want a MongoDB-compatible query API without running a server process. Reads and writes hit a local file; the oplog accumulates while disconnected and syncs when connectivity returns. |
| **Prototypers** | Drop-in PyMongo-style API over a local file (redb) means zero infrastructure to start. Build with `MongoClient("local://data")`, then flip the URI to `MongoClient("mongodb+srv://...")` when you're ready for Atlas. Same code, same queries, different backend. |
| **Embedded / edge deployments** | IoT dashboards, kiosk apps, or Electron apps that need a document database bundled into the binary (Python, Node, C, or WASM) without requiring `mongod`. Each device runs its own smongo engine and syncs to a central Atlas cluster. |
| **Browser-first app developers** | The WASM build with OPFS persistence gives a real query engine (indexes, aggregation pipeline, `$lookup`) running entirely in the browser tab. Useful for local-first SaaS, collaborative tools, or privacy-sensitive apps where data shouldn't leave the device by default. |
| **Dev/test without Docker** | Unit and integration tests run against the embedded engine with no containers, no network, no cleanup. Same MQL you'll use in production. |

### Who would *not* use it

Anyone needing production multi-node MongoDB, replica sets, sharding, or full wire protocol compatibility. smongo is an embedded engine that speaks a subset of the MongoDB API -- it's SQLite for the document world, not a MongoDB replacement at scale. If you can `apt install mongod` or spin up Atlas, and you don't need offline/embedded/browser, use the real thing.

---

## AI & LLM Integration

smongo speaks the real MongoDB wire protocol. That means **LangChain, CrewAI, mongosh, Compass, and any MongoDB driver** connect to the embedded engine over TCP and work unchanged -- they don't know it's not Atlas.

```python
from smongo import WireServer

with WireServer(db_path, port=27018) as srv:
    # Any MongoDB client connects here -- LangChain, pymongo, mongosh, Compass
    from pymongo import MongoClient as PyMongoClient

    client = PyMongoClient("mongodb://localhost:27018", directConnection=True)
    coll = client["langchain_db"]["vectors"]

    # Official LangChain class -- zero custom code, zero wrappers
    from langchain_mongodb import MongoDBAtlasVectorSearch

    vectorstore = MongoDBAtlasVectorSearch(
        collection=coll,
        embedding=embeddings,
        index_name="default",
        text_key="text",
        embedding_key="embedding",
        relevance_score_fn="cosine",
    )

    results = vectorstore.similarity_search_with_score("How do AI agents work?", k=2)
    # [0.8055] Agents use LLMs to decide what actions to take and which tools to call...
    # [0.7749] Vector search finds semantically similar documents using cosine simila...
```

**What works out of the box:**

| Framework | How it connects | What it does |
|---|---|---|
| **LangChain** `MongoDBAtlasVectorSearch` | Standard PyMongo collection | `$vectorSearch` over the wire -- RAG retrieval, similarity search |
| **LangChain** `MongoDBChatMessageHistory` | Standard PyMongo collection | Persistent chat memory for agents and chains |
| **CrewAI** agent tools | PyMongo-based `@tool` functions | Agents query the embedded database with `find()`, `aggregate()` |
| **mongosh** | `mongodb://localhost:27018` | Interactive shell, ad-hoc queries |
| **MongoDB Compass** | `mongodb://localhost:27018` | Visual document browser, aggregation builder |
| **Any PyMongo code** | `MongoClient("mongodb://localhost:...")` | Existing MongoDB code works as-is |

**Why this matters for AI:**

- **`$vectorSearch`** runs cosine/euclidean/dotProduct similarity via a vendored HNSW index (ANN) or flat exhaustive scan -- no external vector database needed
- **Local-first** means zero network latency for RAG retrieval, chat memory, and agent tool calls
- **Offline-capable** -- the oplog accumulates mutations while disconnected; sync catches up when connectivity returns
- **Free-threaded Python (3.13t)** -- no GIL means concurrent request handling with true thread parallelism for mixed AI workloads

See the [`examples/ai_examples/`](examples/ai_examples/) directory for complete working examples: vector search RAG, chat memory, LangChain integration, and CrewAI agent tools.

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
   │  Rust Engine│       │  PyMongo  │
   │ (_smongo_   │       │  Driver   │
   │   core)     │       └───────────┘
   │  ┌───────┐  │
   │  │ MQL   │  │  ◄── compile_query, apply_update (Rust)
   │  │Compiler│  │      $gt $lt $in $ne $or $and ...
   │  └───┬───┘  │
   │      │      │
   │  ┌───┴───┐  │
   │  │ Query │  │  ◄── RustQueryPlanner: prefix-scoring
   │  │Planner│  │      index scan / pk lookup / coll scan
   │  └───┬───┘  │
   │      │      │
   │  ┌───┴───┐  │
   │  │Indexes│  │  ◄── B-tree, text, 2dsphere, bitmap, prefix
   │  │& HNSW │  │      + HNSW / flat vector indexes ($vectorSearch)
   │  └───┬───┘  │
   │      │      │
   │  ┌───┴───┐  │
   │  │ redb  │  │  ◄── Pure Rust storage (ACID, single-file)
   │  │ store │  │      key=_id, value=BSON (transactional)
   │  └───┬───┘  │
   │      │      │
   │  ┌───┴───┐  │       ┌──────────────┐
   │  │ Oplog │  │──────►│  SyncManager  │──► Atlas
   │  └───────┘  │       │  push / pull  │
   └─────────────┘       │  conflict res │
                         └──────────────┘
```

### Rust-Powered Engine (Required)

The compiled Rust extension (`_smongo_core`) is **required** and provides all performance-critical paths via [PyO3](https://pyo3.rs/). `MongoClient("local://...")` uses **`RedbClient`** / **`RedbLocalClient`** and **`smongo-engine`** for storage, query compilation, expression evaluation, and updates:

- **Storage engine** -- **`RedbLocalClient`**, **`RedbLocalDB`**, **`RedbLocalCollection`** on **`smongo-engine`** with the **`redb`** backend. Every insert, find, update, delete, and index operation flows through Rust.
- **B-Tree indexes & query planner** -- same engine as **Node** (`smongo-node`): real indexes and plans on the embedded store.
- **Streaming cursors** -- lazy iteration over matching documents (engine-backed); hot paths avoid materializing full result sets.
- **ACID transactions** -- multi-document transactions use the engine session model (wire / API).
- **BSON Serialization** -- encode/decode via the official Rust `bson` crate (maintained by MongoDB) for both wire protocol and storage paths, guaranteeing spec-compliant output compatible with every MongoDB driver and tool
- **MQL Query Compiler** -- `compile_query` with all 18 query operators, compiled predicate evaluation
- **Expression Engine** -- `resolve_expr` with all 72 aggregation expression operators
- **Update Engine** -- `apply_update` with all 14 update operators, positional operators, and pipeline updates
- **Aggregation Pipeline** -- Full pipeline dispatch in Rust via `aggregate_pipeline`. All 27 stages run in **`smongo-engine`** with zero FFI round-trips per stage: `$group` (17 accumulators), `$lookup` (equality + sub-pipeline), `$graphLookup`, `$facet`, `$out`, `$merge`, `$unionWith`, `$vectorSearch`, and more. A single FFI crossing enters the engine; all stage execution stays in Rust.
- **Wire protocol** -- Tokio-based async TCP server (`RustWireServer`) with Rust command handlers for core commands; Python `WireServer` for the default loop. BSON handling, cursor registry, session management, and profiler integrate across both paths. `find` / `aggregate` delegate to the engine where applicable.
- **Schema validation** -- `$jsonSchema` document validation runs in Rust (`rust/smongo-py/src/schema.rs`). Supports `required`, `properties`, `type`/`bsonType`, numeric/string/array constraints, `enum`, `pattern`, `additionalProperties`, and nested objects with ReDoS-safe regex matching.

The Python modules that remain are high-level orchestration (aggregation `Cursor` for the Python API, `SyncManager`) that calls *into* the Rust storage layer. See [BYE-BYE-GIL.md](BYE-BYE-GIL.md) for the full story.

- **Free-Threaded Python** -- smongo supports Python 3.13+ free-threaded builds (`python3.13t`). The extension declares `gil_used = false` and uses `PyOnceLock` for deadlock-free initialization. All `unsafe impl Send/Sync` are backed by Rust-native locks, not the GIL. Under the free-threaded interpreter, the wire protocol server can handle concurrent connections with true thread parallelism.

---

## Features

### Storage -- redb + engine B-trees with streaming reads
Documents are stored as **native BSON bytes** in **smongo-engine** collections backed by **[redb](https://github.com/cberner/redb)** on disk. Writes are **ACID** at the engine layer (data + indexes + oplog where enabled). The **query planner** accelerates reads and indexed writes (e.g. update/delete by `_id` or indexed fields) without scanning the whole collection.

**Reads are lazy where possible.** `Collection.find()` returns a chainable `Cursor` that pulls matches from the engine without building huge intermediate lists unnecessarily. Chained `.limit(10)` avoids pulling unbounded results. `find_one()` and `count_documents()` use planner-backed paths.

### MQL Compiler
A Rust-accelerated compiler translates MongoDB query dictionaries into executable predicates. Supported query operators: `$gt`, `$lt`, `$gte`, `$lte`, `$eq`, `$ne`, `$in`, `$nin`, `$exists`, `$regex`, `$not`, `$nor`, `$all`, `$elemMatch`, `$size`, `$type`, `$or`, `$and`. Update operators: `$set`, `$inc`, `$push`, `$unset`, `$addToSet`, `$pull`, `$pop`, `$min`, `$max`, `$rename`, `$currentDate`, `$mul`. Dot-notation paths work everywhere (`"address.city"`).

### Aggregation Pipeline
Pipeline execution with 27 stages (all running in Rust via `smongo-engine`): `$match`, `$group`, `$project`, `$sort`, `$limit`, `$skip`, `$unwind`, `$lookup`, `$graphLookup`, `$unionWith`, `$addFields`/`$set`, `$count`, `$replaceRoot`/`$replaceWith`, `$sample`, `$bucket`, `$bucketAuto`, `$sortByCount`, `$redact`, `$setWindowFields`, `$unset`, `$vectorSearch`, `$facet`, `$out`, `$merge`. Memory-bounded with spill-to-disk for `$sort` and `$group` when `allowDiskUse=True`. Group accumulators: `$sum`, `$avg`, `$min`, `$max`, `$push`, `$addToSet`, `$first`, `$last`, `$firstN`, `$lastN`, `$stdDevPop`, `$stdDevSamp`, `$mergeObjects`, `$top`, `$bottom`, `$topN`, `$bottomN`.

`$vectorSearch` runs fully in Rust with:
- **Vendored HNSW** (Hierarchical Navigable Small Worlds) for approximate nearest-neighbor search on larger datasets
- **Flat (exact) scan** via `exact: true` or `indexingMethod: "flat"` for exhaustive brute-force search -- optimal for multi-tenant workloads where each tenant has < 10K vectors after pre-filtering
- **Atlas-compatible score normalization** in `[0, 1]` for `cosine`, `euclidean`, and `dotProduct` metrics
- **Multi-tenant support** -- `tenant_id` pre-filter, one collection for all tenants, matching [Atlas multi-tenant guidance](https://www.mongodb.com/docs/atlas/atlas-vector-search/multi-tenant-architecture/)

`$facet` runs independent sub-pipelines against the same input. `$out` replaces a target collection's contents. `$merge` upserts into a target collection with `whenMatched`/`whenNotMatched` semantics.

Build analytics and similarity queries that run locally with no external vector DB.

**Spill-to-disk:** Pass `allowDiskUse=True` to `Collection.aggregate()` and `$sort` / `$group` stages that exceed the 100 MB in-memory limit will spill intermediate data to temporary files instead of raising `MemoryLimitExceeded`. This makes smongo viable for larger-than-memory aggregation workloads on resource-constrained edge devices.

```python
results = coll.aggregate(
    [{"$sort": {"ts": 1}}, {"$group": {"_id": "$sensor", "avg": {"$avg": "$reading"}}}],
    allowDiskUse=True,
)
```

### B-Tree indexes & query planner
Create single-field, compound, unique, sparse, and TTL indexes backed by the engine’s index tables on **redb**. The query planner scores candidate indexes and picks the optimal execution path:
- **Index Scan** -- range or equality scan on the best-matching index
- **PK Lookup** -- O(log n) direct `_id` fetch
- **Collection Scan** -- fallback full-table scan

Sortable key encoding (IEEE 754 bit-flipping for numbers, hex inversion for descending fields) ensures correct lexicographic ordering across mixed types.

### Oplog (Operations Log)
Every mutation (insert, update, delete, index create/drop) is append-logged to a dedicated **per-collection oplog table** in the engine with timestamps, version counters, and checksums. The oplog supports **compaction** (`compact_oplog(keep=N)`) to bound growth in long-running deployments, and auto-compacts after successful sync push cycles.

### Bidirectional Sync
`SyncManager` syncs local state to any MongoDB-compatible remote:
- **Push**: tail the oplog, batch `bulk_write` to remote, auto-compact after checkpoint
- **Pull**: change streams (preferred) or timestamp-based polling, merge remote changes locally
- **Index sync**: index definitions flow both directions
- **Conflict resolution**: Last-Write-Wins, local-wins, remote-wins, field-level merge, or a custom callable
- **Vector clocks**: per-document causal ordering across replicas -- concurrent conflicts invoke the resolver, causal updates apply automatically
- **Checkpointing**: survives crashes and restarts via persisted sync checkpoint keys in the local store
- **Auto-sync**: background thread with configurable interval
- **Hybrid mode**: `MongoClient("local://...", sync="mongodb+srv://...")` auto-registers and starts sync
- **Exponential backoff**: on consecutive failures, backoff doubles up to 300s
- **Sync metrics**: `status()` returns `pushed`, `pulled`, `conflicts`, `errors` counters and a `state` field
- **MQL sync rules**: the same query language controls what syncs -- no separate DSL (see below)
- **Node provenance**: oplog entries record the `node_id` of the originating device

### MQL Sync Rules

Sync rules use the same MQL you already know. No separate DSL, no translation layer -- one query language everywhere, including sync policy.

**Variable substitution** makes rules dynamic. Built-in variables are resolved fresh each sync cycle:

| Variable | Value | Example |
|---|---|---|
| `$$NOW` | `time.time()` (epoch float) | Time-windowed sync |
| `$$NODE_ID` | Configured `node_id` | Device-scoped sync |
| `$$<custom>` | Any key from `sync_config["variables"]` | Region, tenant, etc. |

**Device-scoped sync** -- each edge node syncs only its own data:

```python
client = MongoClient("local://data", sync="mongodb+srv://...", sync_config={
    "node_id": "sensor-east-001",
    "sync_rules": {"device_id": "$$NODE_ID"},
})
```

**Time-windowed sync** -- only sync the last 7 days:

```python
client = MongoClient("local://data", sync="mongodb+srv://...", sync_config={
    "sync_rules": {"_lastModified": {"$gt": "$$WINDOW_START"}},
    "variables": {"WINDOW_START": time.time() - 7 * 86400},
})
```

**Combining rules** with `$and`:

```python
sync_config = {
    "node_id": "sensor-east-001",
    "sync_rules": {
        "$and": [
            {"device_id": "$$NODE_ID"},
            {"_lastModified": {"$gt": "$$WINDOW_START"}},
        ]
    },
    "variables": {"WINDOW_START": time.time() - 7 * 86400},
}
```

Per-collection filters also support variable substitution via `collections`:

```python
sync_config = {
    "collections": {
        "iot.readings": {"device_id": "$$NODE_ID"},
        "iot.config": {},  # sync all config docs
    },
    "node_id": "sensor-east-001",
}
```

### Local-first architecture
All reads and writes hit **local redb** (via **smongo-engine**) — zero network latency, works fully offline. The oplog accumulates mutations while disconnected; nothing is lost. When connectivity returns, the sync thread picks up from its last checkpoint and pushes/pulls everything that was missed. The wire protocol server means local clients (other apps, mongosh, Compass, LangChain) can connect over TCP without knowing it's not a "real" MongoDB.

### Edge Computing
smongo turns any device into a MongoDB-compatible edge node. Each device runs its own embedded engine, writes locally at full speed, and syncs to a central Atlas cluster with MQL-scoped filters. The central hub aggregates data from the entire fleet; each device sees only its own data.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ sensor-north │     │ sensor-south │     │ sensor-east  │
│  smongo +    │     │  smongo +    │     │  smongo +    │
│  redb local  │     │  redb local  │     │  redb local  │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │  push/pull          │  push/pull          │  push/pull
       │  device_id=self     │  device_id=self     │  device_id=self
       └─────────┬───────────┴───────────┬─────────┘
                 ▼                       ▼
         ┌───────────────────────────────────┐
         │        MongoDB Atlas (central)     │
         │   All devices' data aggregated     │
         └───────────────────────────────────┘
```

```python
for device in fleet:
    client = MongoClient(f"local://{device.data_dir}", sync=ATLAS_URI, sync_config={
        "node_id": device.id,
        "sync_rules": {"device_id": "$$NODE_ID"},
    })
```

See [`examples/patterns/edge_fleet_sync.py`](examples/patterns/edge_fleet_sync.py) for a complete working example.

### Sync Edge Cases We Handle

Bidirectional sync is easy on the happy path. The hard part is what happens when things go wrong. smongo's sync layer has been hardened against the following real-world edge cases, with unit and integration tests for each:

**Multiple conflicting changes on both server and multiple clients, then bring everything back online.**
Each document carries a `_vclock` (vector clock) with per-node counters. When a client reconnects and pulls, the sync layer compares clocks: if one version causally dominates, it wins automatically. If the edits are truly concurrent (neither clock dominates), the configured conflict resolver decides -- LWW, local-wins, remote-wins, field-level merge, CRDT merge, or a custom callable. After resolution, the merged clock is ticked and stamped on the winning document so all replicas converge to the same state.

**Clock skew breaking Last-Write-Wins.**
The LWW resolver no longer trusts wall-clock timestamps blindly. When both documents carry `_vclock`, the tiebreaker uses the lexicographically highest `node_id` in each vector clock -- deterministic and clock-independent. A client with a skewed system clock cannot steal wins. The `_lastModified` timestamp is only consulted as a secondary signal for legacy documents that lack vector clocks.

**A change that succeeds locally but fails server-side schema validation.**
When a push fails with MongoDB error code `121` (DocumentValidationFailure), the sync layer reacts based on `schema_rejection_strategy`:
- `"rollback"` (default): pull the server's version of the document and overwrite the local copy (or delete it if the server has no copy).
- `"quarantine"`: leave the local document untouched; the failed op is permanently quarantined in the dead-letter queue.
- `"ignore"`: legacy behavior.

```python
sync_config = {"schema_rejection_strategy": "rollback"}
```

**Oplog overflow -- too much data written without syncing.**
If the client writes so much data that oplog auto-compaction (or manual `compact_oplog`) removes entries the push checkpoint refers to, the sync layer detects this by comparing the checkpoint against the oldest oplog key. When overflow is detected, it triggers a full resync based on `overflow_strategy`:
- `"server_wins"` (default): reset all checkpoints, drop local data, re-pull everything from the server.
- `"error"`: raise `SyncOverflowError` so the application can handle recovery.

A manual escape hatch is always available:

```python
sync.force_full_resync("mydb", "mycollection")
# → {"ns": "mydb.mycollection", "winner": "server", "docs_synced": 1234}
```

The general correctness property: **all clients and server(s) end up with the same data after a sync completes**, either not losing changes or losing them in a predictable, documented way. When sync *can't* complete (oplog overflow, permanent DLQ failures), one winning side is picked deterministically so replicas converge rather than silently diverge. See [SYNC-NOTES.md](SYNC-NOTES.md) for the full engineering notes.

### Wire Protocol Server
smongo speaks the real MongoDB binary protocol (OP_MSG, OP_COMPRESSED, OP_QUERY). Point `mongosh`, PyMongo, Compass, or any MongoDB driver at `localhost:27018` and they'll talk to the embedded engine as if it were a real `mongod`. The Docker Compose setup exposes the wire server on port 27018 alongside the web dashboard -- `docker compose up` and connect Compass immediately. Small database, real protocol.

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
pip install -e ".[all]"       # installs smongo + builds the Rust extension via maturin
python demo.py
```

Runs the full embedded engine locally -- indexes, queries, aggregation, oplog -- no MongoDB server. The Rust extension is built automatically by the maturin build backend.

**Development install:** Run `pip install` / `make install-dev` from the **repository root** (next to `pyproject.toml`). That file pins `[tool.maturin]` — `manifest-path = rust/smongo-py/Cargo.toml` and `module-name = smongo._smongo_core`. Running `maturin develop` only inside `rust/smongo-py/` can install an extension that does not match the editable `smongo` package, so you see missing methods on `RedbLocalCollection` and similar foot-guns. Use `make install-dev`, `pip install -e ".[dev,all]"`, or `make build-debug` (rebuild extension only, still from root). For a release build of the extension: from root, `python -m maturin develop --release --manifest-path rust/smongo-py/Cargo.toml`.

**One Python:** `make` defaults to `./.venv/bin/python` when that path exists, so `pip install -e` and `maturin develop` target the same interpreter. If `PYTHON` is unset and you have no `.venv`, it falls back to `python3` on `PATH`. Creating a venv first (`python3 -m venv .venv && make install-dev`) avoids mixing pyenv/global installs with a project `.venv`.

---

## Wire Protocol Server

smongo includes a wire protocol server so that **real drivers** can connect to the embedded engine over TCP.

```bash
# Start the server with the installed CLI
smongo-wire --db-path ./data --host 127.0.0.1 --port 27018

# Equivalent module form, useful from a source checkout
python -m smongo.wire --db-path ./data --host 127.0.0.1 --port 27018
```

Then connect with any standard MongoDB client:

```bash
mongosh mongodb://localhost:27018/mydb
```

```python
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27018")
db = client["mydb"]
db["things"].insert_one({"hello": "wire protocol"})
```

Or use the `WireServer` API directly in Python:

```python
from smongo.wire import WireServer

with WireServer("./data", port=27018) as srv:
    input("Press Enter to stop...")
```

**Security features (Rust wire server):**
- **TLS** via [rustls](https://github.com/rustls/rustls) -- available when using the Rust-native `RustWireServer`
- **SCRAM-SHA-256** authentication (RFC 7677) -- PBKDF2-hashed credentials persisted in the local engine (`__users` KV table)
- **Auth gate** enforces authentication on all commands (handshake commands exempted)

> **Note:** TLS and SCRAM authentication are implemented in the Rust wire server (`RustWireServer`). The default Python `WireServer` provides plain TCP without auth. See [WIRE-PROTOCOL.md](WIRE-PROTOCOL.md) for details on both server paths.

---

## Project Structure

```
smongo/
  __init__.py        MongoClient, AsyncMongoClient, SyncManager,
                     DuplicateKeyError, InsertOne, UpdateOne, UpdateMany,
                     DeleteOne, DeleteMany, ReplaceOne, BulkWriteResult
  _smongo_core/      Compiled Rust extension (PyO3) -- the actual engine
  async_client.py    AsyncMongoClient, AsyncDatabase, AsyncCollection,
                     AsyncCursor, AsyncChangeStream (asyncio-native API)
  client.py          URI-based routing, bulk_write, find_one_and_* facade
  storage/           Storage layer (Python + Rust bridge)
    redb_engine.py     RedbClient / RedbCollection (default `local://`)
    collection.py      TTLReaper helper + shared constants (CRUD lives in `redb_engine.py`)
    locking.py         ReadWriteLock (Python fallback; runtime uses Rust)
    results.py         InsertResult, UpdateResult, DeleteResult
    streaming.py       StreamingCursor (Python API; engine-backed reads on redb)
    helpers.py         BSON encode/decode helpers
  query/             MQL compiler package (Rust-accelerated)
    compiler.py        compile_query, query operators
    update.py          apply_update, positional operators
    expressions.py     resolve_expr, 60+ expression operators
    paths.py           get_value, set_value, unset_value
  aggregation/       Pipeline engine package (25+ stages, Rust-accelerated)
    cursor.py          Cursor class (lazy Iterable input), aggregate dispatch, $out/$merge
    stages.py          Core stages: $match, $group, $sort, etc.
    joins.py           $lookup, $graphLookup, $unionWith
    vector.py          $vectorSearch (NumPy / USearch)
  index.py           Index key encoding, helpers, DuplicateKeyError (engine-backed indexes on redb)
  oplog.py           Append-only operations log with compaction
  sync.py            Bidirectional sync with MQL rules, variable substitution, vector clocks
  objectid.py        MongoDB-style ObjectId implementation
  schema.py          $jsonSchema validation layer (delegates to Rust)
  wire/              MongoDB binary protocol server (OP_MSG, OP_COMPRESSED)
    commands/          ~77 Rust command handlers (Python fallback for extensions)
    sessions.py        Session registry
    transactions.py    Transaction state, undo journal
    profiler.py        Profiler, OpTracker, TopStats

rust/                Rust workspace
  smongo-engine/     Core engine: redb (native), Mem/OPFS (WASM), MQL, indexes, geo
  smongo-py/         PyO3 `_smongo_core`: RedbLocalClient, wire server, query, schema, wire_commands/
  smongo-node/       napi-rs bindings (same engine as Python local)
  smongo-c/          C API / cbindgen surface

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
    09_wire_protocol.py  Start wire server, connect with PyMongo over TCP
  ai_examples/
    01_vector_search_rag.py  $vectorSearch RAG pipeline over the wire protocol
    02_chat_memory.py        AI chat memory storage via standard PyMongo
    03_langchain_rag_chain.py Official LangChain MongoDBAtlasVectorSearch locally
    04_crewai_agent_tool.py  CrewAI agents querying smongo via PyMongo tools
  patterns/
    ecommerce.py         Shopping cart, orders, revenue analytics, dashboards
    iot_timeseries.py    1000+ sensor readings, anomaly detection, facility stats
    content_cms.py       Blog CMS: tagging, search, author leaderboard, facets
    edge_fleet_sync.py   Edge fleet: MQL sync rules, device scoping, time windows

demo.py              Standalone CLI demo (no Docker needed)
Dockerfile           Python 3.11 + Rust toolchain for the extension
docker-compose.yml   App + MongoDB for the full sync experience
```

---

## Dev Commands

```bash
make install-dev    # editable smongo + dev/optional extras + Rust extension
make lint           # ruff checks
make format         # ruff formatter
make test           # unit suite (pytest default addopts)
make test-integration   # docker-backed integration suite
make test-perf      # benchmark suite
make coverage       # coverage report (70% enforced)
make typecheck      # mypy strict
```

### YCSB Benchmarks

The `benchmark` branch includes the YCSB harness as a submodule at
`tools/benchmark` and exposes it through `tools/smongo-bench`:

```bash
tools/smongo-bench --source smongo --record-count 1000 --operation-count 1000
tools/smongo-bench --source mongodb --record-count 1000 --operation-count 1000
tools/smongo-bench --source ferretdb --record-count 1000 --operation-count 1000
tools/smongo-bench --source postgresql --record-count 1000 --operation-count 1000
tools/smongo-bench --source mysql --record-count 1000 --operation-count 1000
tools/smongo-bench --source sqlite --record-count 1000 --operation-count 1000
tools/smongo-bench --source documentdb --record-count 1000 --operation-count 1000
tools/smongo-bench --source smongo --target sqlite --record-count 1000 --operation-count 1000
```

Docker-backed targets start with `docker compose`; `smongo` starts the local
wire server automatically. Comparison tables include source/target version
headers and a colored difference column. See `tools/benchmark/README.md` for
generic JDBC, DocumentDB URI overrides, and other target-specific options.

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

### Async API

Full `asyncio`-native client for FastAPI, Starlette, and other async frameworks. All blocking engine work is dispatched via `asyncio.to_thread` -- the event loop stays responsive while Rust handles reads and writes.

```python
from smongo import AsyncMongoClient

async def main():
    async with AsyncMongoClient("local://data") as client:
        db = client["myapp"]
        users = db["users"]

        await users.insert_one({"name": "Alice", "age": 34, "city": "NYC"})

        async for doc in await users.find({"city": "NYC"}):
            print(doc["name"])

        results = await users.aggregate([
            {"$group": {"_id": "$city", "avg_age": {"$avg": "$age"}}},
            {"$sort": {"avg_age": -1}},
        ])

        # Change streams work async too
        async with await users.watch() as stream:
            async for event in stream:
                print(event["operationType"], event["documentKey"])
```

### Change Streams with Resume Tokens

Watch for real-time changes on any collection. Each event includes a `_resumeToken` that survives process restarts -- pass it to `resume_after` to pick up exactly where you left off.

```python
from smongo import MongoClient

client = MongoClient("local://data")
coll = client["mydb"]["events"]

saved_token = None

with coll.watch() as stream:
    for event in stream:
        print(event["operationType"], event["documentKey"])
        saved_token = event["_resumeToken"]
        break  # process one event

# Later (even after restart) -- resume from where we left off
with coll.watch(resume_after=saved_token) as stream:
    for event in stream:
        print("Resumed:", event["operationType"])
```

---

## License

See [LICENSE](LICENSE).
