# Architecture: smongo

**A MongoDB-compatible experience locally on `smongo-engine` + redb, with bidirectional sync to Atlas when configured.**

---

## The Core Thesis

MongoDB's power was never just the query language or the document model — it was the **storage engine underneath** and the **protocol that speaks to it**. This project takes those two pillars and brings them to the edge: an embedded engine backed by **`smongo-engine` on redb** (pure Rust, WASM-friendly), speaking the **real MongoDB wire protocol (OP_MSG)**, and maintaining a **persistent oplog** for **sync** with **MongoDB Atlas** or other remotes.

The result is not a mock. It is not SQLite pretending to be Mongo. It is **ACID document storage in Rust**, an **MQL compiler executing real query predicates**, a **query planner choosing real index scans**, and a **TCP server that real MongoDB drivers can connect to**. When the network is available, local mutations can flow upstream and remote changes back — with conflict resolution.

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Application Layer                             │
│   MongoClient("local://...")  ·  PyMongo driver  ·  mongosh         │
└──────────────┬────────────────────────────┬──────────────────────────┘
               │ Python API                 │ TCP :27018
               ▼                            ▼
┌──────────────────────┐    ┌──────────────────────────────────────────┐
│   smongo.client│    │          Wire Protocol Server            │
│   (MongoClient)      │    │  OP_MSG decode → dispatch → OP_MSG encode│
│                      │    │  OP_QUERY (legacy handshake support)     │
│  mode: local │hybrid │    │  BSON boundary normalization             │
└──────┬───────────────┘    └──────────────┬───────────────────────────┘
       │                                   │
       ▼                                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     Embedded Engine Core                              │
│                                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │ MQL Compiler │  │ Query Planner │  │  Aggregation │  │  Schema  │ │
│  │ Rust +       │  │ RustQuery-    │  │  Pipeline    │  │ Validator│ │
│  │ query/ shims │  │ Planner       │  │  aggregation/│  │ schema.rs│ │
│  │              │  │               │  │              │  │          │ │
│  │ compile_query│  │ plan()        │  │              │  │ $json    │ │
│  │ apply_update │  │ score_index() │  │  25+ stages  │  │ Schema   │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └────┬─────┘ │
│         │                 │                  │               │       │
│         ▼                 ▼                  ▼               ▼       │
│  ┌──────────────────────────────────────────────────────────────────┐│
│  │                    Storage Layer (Rust via PyO3)                 ││
│  │                                                                  ││
│  │  RedbLocalClient → RedbLocalDB → RedbLocalCollection              ││
│  │      │              │            │                               ││
│  │      │              │            ├── Collection + index tables    ││
│  │      │              │            ├── Oplog: __oplog_{db}_{coll}   ││
│  │      │              │            └── Engine `StorageSession`     ││
│  │      │              │                                            ││
│  │      ▼              ▼                                            ││
│  │  redb `Database::open(path)`  →  transactional reads/writes       ││
│  └──────────────────────────────────────────────────────────────────┘│
│                              │                                       │
│                              ▼                                       │
│  ┌──────────────────────────────────────────────────────────────────┐│
│  │                    Oplog (oplog.py)                              ││
│  │                                                                  ││
│  │  OplogWriter.log(op, doc_id, payload)                           ││
│  │  OplogReader.read_from(checkpoint)                              ││
│  │  ChangeStream → register_listener → _enqueue events             ││
│  └──────────────────────────┬───────────────────────────────────────┘│
└─────────────────────────────┼────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    Sync Layer (sync.py)                               │
│                                                                      │
│  SyncManager: background thread, configurable interval               │
│                                                                      │
│  PUSH: tail oplog → bulk_write(InsertOne/UpdateOne/DeleteOne)        │
│  PULL: change streams (preferred) or timestamp polling               │
│  CONFLICT: LWW · local_wins · remote_wins · field_merge · callable   │
│  CHECKPOINT: logical `__sync_checkpoint` (redb KV)                   │
│                                                                      │
│  ┌─────────┐     oplog entries     ┌─────────────────────────────┐   │
│  │  LOCAL   │ ──────────────────►  │   MongoDB Atlas / Cloud     │   │
│  │  redb    │ ◄────────────────── │   (PyMongo bulk_write)      │   │
│  └─────────┘   change streams      └─────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

Hot paths in the wire server and `smongo-py` use typed **`RedbLocalCollection`** access and Rust command handlers to avoid per-operation Python dispatch where it matters.


---

## 1. Embedded storage (`smongo-engine` + redb)

Python `local://` and the wire server talk to **`RedbLocalClient` → `RedbLocalDB` → `RedbLocalCollection`**, backed by a single **`redb`** database file (or in-memory / WASM backends in other targets). Documents, secondary indexes, and per-collection oplog tables live in the engine; **`storage_stats`** and **`serverStatus`** report a MongoDB-shaped **`storageEngine`** document (`name: "redb"`), aligned with driver expectations.

Writes are transactional in the engine: data rows, index maintenance, and oplog append succeed or roll back together. The query planner (index scan, PK equality, collection scan, geo) runs in Rust; **`collStats` / `$collStats`** surface counts and sizes through the same stats helpers.

Legacy **`table:`** URI strings still appear in sync checkpoint keys for compatibility with older sync code paths; the redb layer strips the prefix before KV access.

## 2. Indexes and the planner

Index definitions and B-tree rows are owned by **`smongo-engine`**. Keys use MongoDB-aware encodings so range scans and unique checks match server semantics. Python **`smongo/index.py`** retains **planner helpers and encoders** used by tests and tooling; **`IndexManager`** with **`table:__idx_*`** URIs is a **legacy** layout, not the default embedded path.

---

## 3. The MQL Compiler: MongoDB's Query Language

### Query Compilation

`compile_query(query)` transforms a MongoDB query document into a Python predicate function. The compiler handles the full MQL grammar:

**Logical operators**: `$or`, `$and`, `$nor` — recursively compiled into nested predicate chains.

**Comparison operators**: `$gt`, `$lt`, `$gte`, `$lte`, `$eq`, `$ne` — with proper null handling.

**Element operators**: `$exists`, `$type` (maps BSON type names to Python types via `_TYPE_MAP`).

**Array operators**: `$in`, `$nin`, `$all`, `$elemMatch`, `$size` — including array-to-array matching for `$in`.

**String operators**: `$regex` with `$options` support for `i` (case-insensitive), `m` (multiline), `s` (dotall).

**Negation**: `$not` — inverts any nested operator expression.

**Dot-notation path traversal** supports nested documents and array index access: `"address.city"`, `"scores.0"`.

### Update Engine

`apply_update(doc, update_spec)` implements 14 update operators that mutate documents in place:

| Operator | Behavior |
|---|---|
| `$set` | Set field value (with dot-path support) |
| `$inc` | Atomic increment |
| `$mul` | Multiply |
| `$min` / `$max` | Conditional floor/ceiling |
| `$unset` | Remove field |
| `$rename` | Atomic rename |
| `$push` | Append to array (with `$each` support) |
| `$addToSet` | Set-semantic append (with `$each`) |
| `$pull` | Remove from array by value or sub-query |
| `$pop` | Remove first (-1) or last (1) array element |
| `$currentDate` | Set to current timestamp or ISO string |

### Expression Engine

`resolve_expr()` powers the aggregation framework's `$project`, `$addFields`, `$group`, and `$cond` stages. It evaluates a rich expression tree:

- **Field references**: `"$fieldName"`, `"$$ROOT"`, `"$$CURRENT"`
- **Conditionals**: `$cond`, `$ifNull`, `$switch`
- **String**: `$concat`, `$toUpper`, `$toLower`, `$substr`, `$strLenCP`
- **Array**: `$arrayElemAt`, `$size`, `$filter`, `$concatArrays`, `$in`
- **Arithmetic**: `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`
- **Comparison**: `$eq`, `$ne`, `$gt`, `$lt`, `$gte`, `$lte` (expression form)
- **Boolean**: `$and`, `$or`, `$not`
- **Type introspection**: `$type`, `$literal`

---

## 4. The Aggregation Pipeline: 27 Stages Deep

All 27 pipeline stages run in **`smongo-engine`** (pure Rust) with a single FFI crossing per pipeline. Streaming stages use lazy iterator adapters; blocking stages materialize at their boundary only. See [ZERO-FFI-STATUS.md](ZERO-FFI-STATUS.md) for the architecture.

The pipeline engine processes documents through a chain of stages, each transforming the document stream:

| Stage | Description |
|---|---|
| `$match` | Filter via compiled MQL predicate |
| `$group` | Group with 17 accumulators: `$sum`, `$avg`, `$min`, `$max`, `$push`, `$addToSet`, `$first`, `$last`, `$firstN`, `$lastN`, `$stdDevPop`, `$stdDevSamp`, `$mergeObjects`, `$top`, `$bottom`, `$topN`, `$bottomN` |
| `$project` | Field inclusion/exclusion and expression computation |
| `$sort` | Multi-key stable sort with null-aware ordering; spill-to-disk when `allowDiskUse=True` |
| `$limit` / `$skip` | Result windowing |
| `$unwind` | Array expansion with `preserveNullAndEmptyArrays` |
| `$addFields` / `$set` | Computed field injection |
| `$count` | Document count as named field |
| `$replaceRoot` / `$replaceWith` | Promote sub-document to root |
| `$lookup` | Left outer join (equality or pipeline) across collections |
| `$graphLookup` | Recursive graph traversal across collections |
| `$unionWith` | Union documents from another collection (SQL `UNION ALL`) |
| `$sample` | Random sampling |
| `$bucket` | Fixed-boundary histogram grouping |
| `$bucketAuto` | Auto-computed equal-range histogram |
| `$sortByCount` | Group by expression and sort by frequency |
| `$redact` | Field-level access control ($$DESCEND / $$PRUNE / $$KEEP) |
| `$setWindowFields` | Window functions (rank, running totals, moving averages) |
| `$unset` | Remove fields (shorthand for `$project` with exclusion) |
| `$vectorSearch` | Semantic similarity search (NumPy / USearch) |
| `$facet` | Run independent sub-pipelines against the same input documents |
| `$out` | Write results to a target collection (replaces contents) — terminal stage |
| `$merge` | Upsert results into a target collection (`whenMatched` / `whenNotMatched`) — terminal stage |

### $vectorSearch: AI-Native from Day One

The embedded engine includes a `$vectorSearch` stage that performs in-memory vector similarity search:

```python
db.articles.aggregate([{
    "$vectorSearch": {
        "path": "embedding",
        "queryVector": [0.1, 0.2, ...],
        "limit": 5,
        "metric": "cosine",
        "filter": {"category": "tech"},
        "scoreField": "_score"
    }
}])
```

- **Metrics**: Cosine similarity and Euclidean distance
- **Pre-filtering**: MQL filter applied before vector scoring
- **Backend**: NumPy brute-force by default, optional USearch ANN for speed
- **Score injection**: Similarity score added to each result document

---

## 5. The Wire Protocol: Real MongoDB Drivers, Real Connections

### Why a Wire Protocol Server Matters

The wire protocol server transforms `smongo` from a Python library into something far more powerful: **a drop-in replacement for `mongod` that any MongoDB driver in any language can connect to**. `mongosh`, Compass, PyMongo, the Node.js driver, the Go driver — they all speak OP_MSG over TCP. So does this server.

```
$ python -m smongo.wire --port 27018
$ mongosh mongodb://localhost:27018
```

### Message Format

Every MongoDB wire protocol message starts with a 16-byte header:

```
┌───────────┬───────────┬──────────────┬──────────┐
│  length   │ requestId │ responseTo   │ opCode   │
│  int32    │  int32    │   int32      │  int32   │
└───────────┴───────────┴──────────────┴──────────┘
     4 bytes    4 bytes     4 bytes      4 bytes
```

**OP_MSG (opcode 2013)** — the modern protocol:
- 4-byte flags (bit 0 = checksum present, bit 1 = moreToCome)
- Section Kind 0: single BSON body document (the command)
- Section Kind 1: document sequences (e.g., batch insert documents)
- Optional 4-byte CRC-32C checksum

**OP_QUERY (opcode 2004)** — legacy, used during initial driver handshake:
- Collection name, skip, limit, BSON query
- Handled for `isMaster`/`hello` compatibility, then routed through the same dispatcher

### The Command Dispatcher

The first key of the BSON body document names the command. The dispatcher routes to registered handlers:

```
Handshake:     hello, ismaster, isMaster, ping, buildInfo, hostInfo, getLog
CRUD:          find, insert, update, delete, count, distinct, findAndModify,
               getMore, killCursors, bulkWrite, getLastError, estimatedDocumentCount
Indexes:       listIndexes, createIndexes, dropIndexes, reIndex
Aggregation:   aggregate, mapReduce
Admin:         listDatabases, listCollections, create, drop, dropDatabase,
               explain, collMod, renameCollection, compact, collStats, dbStats,
               validate, fsync, getParameter, setParameter
Sessions:      startSession, endSessions, abortTransaction, commitTransaction
Users:         usersInfo, rolesInfo, createUser, dropUser, updateUser
Diagnostic:    currentOp, killOp, top, profile, connPoolStats, lockInfo,
               listCommands, replSetGetConfig, replSetGetStatus
Sync:          client.sync, setFreeMonitoring, shardingState
Auth:          saslStart, saslContinue, connectionStatus (SCRAM-SHA-256 auth + RBAC in RustWireServer)
```

**Read-path dispatch:** `find` and `aggregate` are fully handled in Rust: `find` performs sort, skip, limit, and projection without a Python `Cursor`, and `aggregate` calls `aggregate_pipeline` directly. The wire command path therefore has no Python method dispatch for those read operations.

### BSON Boundary Normalization

The wire layer maintains a clean boundary between the BSON world (drivers) and the engine world (Python dicts with `smongo.ObjectId`, floats, regex dicts).

The wire path uses the official Rust `bson` crate (`rust/smongo-py/src/raw_bson.rs`) for both encoding and decoding, guaranteeing spec-compliant BSON that is byte-compatible with every MongoDB driver and tool.

**Decode (wire bytes → engine):** `raw_decode_document` calls `bson::from_slice` then `doc_to_pydict` to produce engine-ready Python dicts with correct types (ObjectId, datetime, Decimal128, Regex, Timestamp, UUID, etc.).

**Encode (engine → wire bytes):** `raw_encode_document` calls `pydict_to_doc` then `bson::to_vec` to serialize Python dicts to spec-compliant BSON bytes.

The Python-facing `normalize_inbound` / `normalize_outbound` functions in `wire_codec.rs` remain available for the LocalClient path but are no longer called on the wire hot path.

### Connection Model

Each TCP connection gets its own task (Tokio async) with a private `ConnectionContext`. TLS is supported via rustls when `tls_cert_file`/`tls_key_file` are provided. SCRAM-SHA-256 authentication and RBAC are enforced when `auth_required=True`. The server shares a single `RedbLocalClient` (one **`smongo-engine`** database handle over **redb**) and a single `CursorRegistry` across all connections. Cursor IDs are random 63-bit integers with an idle reaper (600s default).

The server advertises itself as wire version 0–21, maxBsonObjectSize of 16MB, and maxMessageSizeBytes of 48MB — matching production MongoDB's capabilities.

---

## 6. The Oplog: Every Mutation, Recorded

### Format

Every write operation appends a structured entry to the collection's oplog table:

```json
{
    "ts": 1711929600.123,
    "ns": "mydb.users",
    "op": "insert",
    "doc_id": "660a1b2c3d4e5f6789012345",
    "payload": { ... },
    "v": 3,
    "checksum": "a1b2c3d4e5f6g7h8",
    "internal": false,
    "changed_fields": ["name", "email"]
}
```

| Field | Purpose |
|---|---|
| `ts` | Unix timestamp (float) — used for LWW conflict resolution |
| `ns` | Namespace (`db.collection`) — used for change stream routing |
| `op` | Operation type: `insert`, `update`, `delete`, `index_create`, `index_drop` |
| `doc_id` | The `_id` of the affected document (or index name) |
| `payload` | Full document (insert), update spec (update), or null (delete) |
| `v` | Monotonic version counter — per-document conflict detection |
| `checksum` | SHA-256 prefix of the payload — integrity verification |
| `internal` | Echo prevention flag — sync layer skips internal entries |
| `changed_fields` | Field names modified — enables field-level merge |

The oplog key is `{time_ns:020d}-{uuid4}`, ensuring **lexicographic time ordering** in the engine’s ordered key space while remaining globally unique.

Oplog read/write goes through **`smongo-engine`** (`OplogWriter` / `OplogReader` over the collection’s oplog table) and PyO3 bridges (`RedbOplogWriterBridge` / `RedbOplogReaderBridge`) so hot paths stay in Rust. `OplogHub` registers listeners as `Py<ChangeStream>` instead of `Py<PyAny>`.

### Oplog Compaction

The oplog grows with every mutation. For long-running embedded deployments, unbounded growth is a disk-space and performance problem. The oplog now supports bounded growth:

- **`OplogWriter.truncate_before(key)`**: Delete all entries lexicographically before a given key (used by sync after pushing entries to Atlas).
- **`OplogWriter.truncate_count(max_entries)`**: Keep only the last N entries, deleting the oldest.
- **`OplogReader.count()`** / **`OplogReader.oldest_key()`**: Monitoring primitives.
- **Manual compaction**: use `OplogWriter.truncate_count(max_entries)` / `truncate_before(key)` from the collection’s oplog writer (or disable `oplog_auto_compact` if multiple consumers need the full tail).
- **Auto-compact after sync push**: When `oplog_auto_compact` is enabled (default), `SyncManager._push()` calls `truncate_before(last_pushed_key)` after each successful push cycle, reclaiming space for entries safely stored in Atlas.

### Change Streams

`ChangeStream` registers as a listener on the `OplogWriter` class and receives real-time notifications of local mutations. Events are translated into MongoDB-compatible change event format:

```python
with collection.watch([{"$match": {"operationType": "insert"}}]) as stream:
    for event in stream:
        print(event["fullDocument"])
```

The pipeline's `$match` stage is compiled with the same `compile_query` used for regular finds, applied against the **change event dict** (not the raw document).

---

## 7. The Sync Layer: Local-First, Cloud-Connected

### The Architecture of Bidirectional Sync

The sync layer is what transforms a standalone embedded database into a **distributed data system**. It bridges the gap between "works offline" and "works everywhere" by maintaining eventual consistency between the local **`smongo-engine` + redb** store and a remote MongoDB Atlas cluster.

```
┌────────────────────────────────────────────────────────────┐
│                    SyncManager                              │
│                                                            │
│  Background Thread (configurable interval, default 5s)     │
│                                                            │
│  ┌──────────────────┐        ┌──────────────────────────┐  │
│  │      PUSH        │        │         PULL             │  │
│  │                  │        │                          │  │
│  │ Tail local oplog │        │ MongoDB Change Streams   │  │
│  │ ────────────────►│        │◄──────────────────────── │  │
│  │                  │        │   (with resume tokens)   │  │
│  │ InsertOne        │        │                          │  │
│  │ UpdateOne(upsert)│        │ Fallback: timestamp poll │  │
│  │ DeleteOne        │        │ find({_lastModified:     │  │
│  │ create_index     │        │        {$gt: last_ts}})  │  │
│  │ drop_index       │        │                          │  │
│  │                  │        │                          │  │
│  │ bulk_write(      │        │ _upsert_remote_doc()     │  │
│  │   ordered=False) │        │ → conflict resolution    │  │
│  └────────┬─────────┘        └────────────┬─────────────┘  │
│           │                               │                │
│           ▼                               ▼                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Checkpoint Table                        │   │
│  │         table:__sync_checkpoint                      │   │
│  │                                                      │   │
│  │  push:{ns}          → last oplog key pushed          │   │
│  │  pull_cs_init:{ns}  → initial snapshot completed     │   │
│  │  pull_cs_token:{ns} → change stream resume token     │   │
│  │  pull_ts:{ns}       → last _lastModified timestamp   │   │
│  └─────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────┘
```

### Push: Local Mutations to the Cloud

The push path tails the local oplog from the last checkpoint forward:

1. **Read** uncommitted entries via `OplogReader.read_from(checkpoint, skip_internal=True)`
2. **Transform** each oplog entry into a PyMongo bulk operation:
   - `insert` → `InsertOne` with `_lastModified` timestamp injected
   - `update` → `UpdateOne` with upsert, `_lastModified` in `$set`
   - `delete` → `DeleteOne`
   - `index_create` / `index_drop` → direct remote index management
3. **Flush** via `bulk_write(ops, ordered=False)` in configurable batch sizes
4. **Checkpoint** the last oplog key to `push:{ns}` in local storage (sync metadata table)

### Pull: Cloud Changes to Local

The pull path prefers **MongoDB Change Streams** for real-time, resumable change delivery:

**First run**: Full snapshot via `find({})` on the remote collection, then mark `pull_cs_init:{ns} = "1"`.

**Subsequent runs**: Open a change stream with `resume_after` token, `full_document="updateLookup"`, and a 200ms await timeout. Process up to `batch_size` events per cycle. Persist the resume token after each event.

**Fallback**: If change streams are unavailable (e.g., standalone mongod without replica set), fall back to timestamp-based polling: `find({_lastModified: {$gt: last_ts}}).sort("_lastModified", 1)`.

### Pull also syncs index definitions

After pulling documents, the sync layer inspects remote index definitions and mirrors any non-`_id_` indexes that don't exist locally, using `create_index(..., _internal=True)` to avoid oplog echo.

### Conflict Resolution

When a pulled document already exists locally, the sync layer must decide which version wins. Four built-in strategies plus custom callables:

**Last-Write-Wins (LWW)** — *default*: Compare `_lastModified` timestamps. The newer document wins. Simple, deterministic, and works well for most applications:
```python
def _lww(local_doc, remote_doc):
    local_ts = (local_doc or {}).get("_lastModified", 0)
    remote_ts = (remote_doc or {}).get("_lastModified", 0)
    return remote_doc if remote_ts >= local_ts else local_doc
```

**Local Wins**: The local document always takes precedence. Useful for edge devices that are the authoritative source.

**Remote Wins**: The cloud document always takes precedence. Useful for read-heavy edge caches.

**Field-Level Merge**: The most sophisticated strategy. Tracks which fields were changed locally (via oplog `changed_fields`) and which fields exist in the remote document. For each field:
- Changed only locally → keep local value
- Changed only remotely → keep remote value
- Changed on both sides → fall back to per-field LWW using `_lastModified`
- Unchanged → take from remote (latest known state)

**Custom Callable**: Pass any `(local_doc, remote_doc) → resolved_doc` function.

### Sync Modes

| Mode | Push | Pull | Use Case |
|---|---|---|---|
| `bidirectional` | Yes | Yes | Full two-way sync (default) |
| `push_only` | Yes | No | Edge device publishing data |
| `pull_only` | No | Yes | Local read cache of cloud data |

### Sync Metrics & Observability

`SyncManager.status()` returns a rich status object for monitoring:

```python
{
    "running": True,
    "mode": "bidirectional",
    "state": "online",           # online | syncing | error | offline
    "pending": 0,
    "last_sync": 1711929600.123,
    "last_error": None,
    "pushed": 1042,              # cumulative documents pushed to Atlas
    "pulled": 587,               # cumulative documents pulled from Atlas
    "conflicts": 23,             # conflict resolution invocations
    "errors": 2,                 # cumulative sync cycle errors
}
```

### Exponential Backoff

On consecutive sync failures, the sleep interval between cycles doubles: `min(interval_sec * 2^consecutive_errors, max_backoff_sec)`. The `state` field transitions to `"error"`. On a successful cycle, the counter resets to zero and sleep returns to the configured `interval_sec`. Default `max_backoff_sec` is 300 (5 minutes).

### Selective Sync Filters

The `collections` config supports per-collection MQL filters that control which documents are pushed and pulled:

```python
sync_config = {
    "collections": {
        "mydb.users": {"region": "us-east-1"},            # only sync US users
        "mydb.orders": {"$expr": {"$gt": ["$total", 100]}},  # only big orders
    }
}
```

Backward-compatible: string lists (`["db.coll"]`) and `"*"` still work — they set the filter to `None` (no filtering).

### Echo Prevention

When the sync layer writes a pulled document locally, it passes `_internal=True` to the storage layer. This flag is designed to mark oplog entries as internal so the push path skips them, preventing the same change from bouncing back and forth infinitely.

---

## 8. ObjectId: Spec-Compliant Document Identity

The `ObjectId` implementation follows the MongoDB ObjectId specification exactly:

```
┌──────────┬──────────────┬──────────────┐
│ 4 bytes  │   5 bytes    │   3 bytes    │
│timestamp │ random value │  counter     │
│(seconds) │ (per-process)│ (mod 2^24)   │
└──────────┴──────────────┴──────────────┘
         12 bytes total = 24 hex chars
```

- **Timestamp**: Big-endian 4-byte Unix time in seconds — extractable via `generation_time`
- **Random**: 5 bytes from `os.urandom()`, generated once per process — ensures uniqueness across processes
- **Counter**: 3-byte incrementing value (thread-safe via lock), seeded from random — ensures uniqueness within a process

ObjectIds are naturally time-ordered (the timestamp prefix sorts first), which means **`_id`**-keyed storage tends to preserve insertion order — similar to production MongoDB’s behavior.

---

## 9. Schema Validation: $jsonSchema at the Edge

The validation engine enforces MongoDB's `$jsonSchema` on every insert and update:

- **Type checking**: `bsonType` / `type` with full BSON type name mapping
- **Required fields**: Explicit field presence enforcement
- **Property schemas**: Recursive validation of nested documents
- **Additional properties**: `additionalProperties: false` blocks unexpected fields (ignores `_id`)
- **Numeric constraints**: `minimum`, `maximum`, `exclusiveMinimum`, `exclusiveMaximum`
- **String constraints**: `minLength`, `maxLength`, `pattern` (regex)
- **Array constraints**: `minItems`, `maxItems`, `uniqueItems`, `items` (per-element schema)
- **Enum**: Whitelist of allowed values
- **Property count**: `minProperties`, `maxProperties`

Validation errors include the full dot-path to the failing field and a descriptive message, matching MongoDB's error format.

---

## 10. The Client: Three Modes, One API

`MongoClient` provides a unified interface that adapts to the connection URI:

```python
# Mode 1: Pure local — redb-backed engine, no network
client = MongoClient("local://my_data")

# Mode 2: Pure remote — delegates to PyMongo
client = MongoClient("mongodb+srv://cluster.mongodb.net/mydb")

# Mode 3: Hybrid — local redb + background sync to Atlas
client = MongoClient("local://my_data", sync="mongodb+srv://cluster.mongodb.net")
```

| Mode | Storage | Network | Sync |
|---|---|---|---|
| `local` | `smongo-engine` + redb on disk | None | None |
| `remote` | MongoDB Atlas | Required | N/A (direct) |
| `hybrid` | `smongo-engine` + redb on disk | Optional | Bidirectional to Atlas |

In hybrid mode, the client constructs a `SyncManager` and starts it automatically. Collections are registered for sync as they are accessed through `Database.__getitem__` or `create_collection`.

The `Collection` wrapper provides the full MongoDB API surface: `find`, `find_one`, `insert_one`, `insert_many`, `update_one`, `update_many`, `delete_one`, `delete_many`, `find_one_and_update`, `find_one_and_replace`, `find_one_and_delete`, `bulk_write`, `aggregate`, `count_documents`, `watch`, `create_index`, `drop_index`, `list_indexes`, `explain`, `get_oplog`.

In local mode, **`find()`** uses engine-backed iteration (and wire paths use Rust streaming helpers). Documents are decoded from BSON as the cursor advances. **`find_one()`** and **`count_documents()`** use engine primitives that stop early or count without materializing full result lists. **`aggregate()`** consumes input via the same lazy patterns where applicable.

`bulk_write` accepts a list of operation descriptors (`InsertOne`, `UpdateOne`, `UpdateMany`, `DeleteOne`, `DeleteMany`, `ReplaceOne`) and executes them in order (or unordered), returning a `BulkWriteResult` with `inserted_count`, `matched_count`, `modified_count`, and `deleted_count`.

---

## 11. The Power of Localized True MongoDB

### What "True MongoDB" Means

This is not a document database that happens to use similar method names. The fidelity runs deep:

1. **Embedded B-tree storage**: **redb** backs **`smongo-engine`** on native targets with ACID commits and a single-file layout. Documents are stored as **native BSON bytes** — the same binary format MongoDB uses on the wire.

2. **Same write semantics**: Each write runs inside an **engine transaction** (`StorageSession::begin_transaction` / `commit_transaction`): data rows, indexes, and oplog updates commit or roll back together. Validation failures or unique-index violations surface as errors with no partial durable state.

3. **Same query language**: MQL queries compiled from the same JSON grammar with the same operators, the same dot-notation path semantics, the same type comparison rules. The query planner accelerates **both reads and writes**.

4. **Same wire protocol**: OP_MSG with BSON serialization — any MongoDB driver in any language connects natively. The `findAndModify` command works out of the box.

5. **Same aggregation framework**: 25+ pipeline stages that behave like their server counterparts, including `$lookup` joins, `$vectorSearch`, `$facet` for parallel sub-pipelines, and `$merge`/`$out` for materialized views.

6. **Same index semantics**: B-tree indexes with compound keys, unique constraints, sparse behavior, and TTL expiration.

7. **Same change streams**: Event-driven reactive programming with the same `operationType`, `documentKey`, `fullDocument` shape.

8. **Same ObjectId format**: 12-byte identifiers with embedded timestamps, process uniqueness, and natural ordering.

9. **Same bulk operations**: `bulk_write` with `InsertOne`, `UpdateOne`, `UpdateMany`, `DeleteOne`, `DeleteMany`, `ReplaceOne` — the same operation descriptors as PyMongo.

### What This Enables

**Offline-first applications**: Write locally with full query power. When connectivity returns, sync catches up automatically.

**Edge computing**: Run the full MongoDB experience on IoT devices, mobile backends, or air-gapped systems — with eventual consistency to the cloud.

**Development without infrastructure**: No Docker, no Atlas account, no network. `pip install` and go.

**Testing without mocks**: Integration tests run against the real storage engine with the real query semantics. No behavioral gaps between test and production.

**AI-native local data**: Vector search runs in-process against local data. No network round-trip for semantic queries.

**Hybrid architectures**: Local speed for reads, cloud durability for writes, bidirectional sync for everything.

---

## 12. Dependency Map

| Package | Role | Required |
|---|---|---|
| **redb** (via **`smongo-engine`**) | Embedded B-tree persistence (data, oplog, indexes, sync metadata) | Yes (local `local://` mode) |
| **pymongo** | Remote mode driver, sync target, BSON codec for wire protocol | Yes (sync/remote/wire) |
| **flask** | Web dashboard (demo application) | No (demo only) |
| **numpy** | Vector math for `$vectorSearch` | No (vector search only) |
| **usearch** | Approximate nearest neighbor acceleration | No (optional perf) |

---

## 13. File Map

```
smongo/
├── __init__.py           # Public API: MongoClient, Database, Collection,
│                         #   SyncManager, DuplicateKeyError, ObjectId,
│                         #   ValidationError, WireServer, InsertOne,
│                         #   UpdateOne, UpdateMany, DeleteOne, DeleteMany,
│                         #   ReplaceOne, BulkWriteResult
├── client.py             # MongoClient (local/remote/hybrid), Database, Collection,
│                         #   streaming find/find_one/count, bulk_write, find_one_and_*
├── _smongo_core/         # Compiled Rust extension (PyO3) -- the actual engine
├── storage/              # Local backends and helpers around the PyO3 engine
│   ├── redb_engine.py    #   RedbCollection — CRUD, indexes, oplog bridges, change streams
│   ├── collection.py     #   TTLReaper and shared collection helpers
│   ├── locking.py        #   ReadWriteLock helpers
│   ├── results.py        #   InsertResult, UpdateResult, DeleteResult
│   ├── streaming.py      #   StreamingCursor (Python-side cursor wrapper where used)
│   └── helpers.py        #   BSON encode/decode helpers
├── query/                # MQL compiler package
│   ├── compiler.py       #   compile_query, query operators
│   ├── update.py         #   apply_update, positional operators
│   ├── expressions.py    #   resolve_expr, 60+ expression operators
│   └── paths.py          #   get_value, set_value, unset_value
├── aggregation/          # Pipeline engine package
│   ├── cursor.py         #   Cursor (accepts Iterable, lazy materialization), aggregate dispatch, $out/$merge
│   ├── stages.py         #   Core stages: $match, $group, $sort, etc.
│   ├── joins.py          #   $lookup, $graphLookup, $unionWith
│   └── vector.py         #   $vectorSearch (NumPy / USearch)
├── index.py              # Index key encoding, helpers, DuplicateKeyError
├── oplog.py              # OplogWriter (with compaction), OplogReader, ChangeStream
├── sync.py               # SyncManager, conflict resolvers, checkpoint persistence,
│                         #   metrics, exponential backoff, selective sync filters
├── schema.py             # $jsonSchema validator
├── objectid.py           # MongoDB-compatible ObjectId
└── wire/                 # MongoDB binary protocol server
    ├── server.py         #   TCP server, connection threads, message dispatch
    ├── msg.py            #   OP_MSG / OP_QUERY / OP_REPLY framing
    ├── commands/         #   80+ command handlers
    │   ├── _registry.py  #     dispatch, handler registry, shared state
    │   ├── handshake.py  #     hello, ping, buildInfo, sasl
    │   ├── crud.py       #     find, insert, update, delete, findAndModify
    │   ├── aggregation.py#     aggregate, mapReduce
    │   ├── indexes.py    #     createIndexes, dropIndexes, listIndexes
    │   ├── admin.py      #     serverStatus, collStats, fsync, explain
    │   ├── sessions.py   #     startSession, transactions
    │   ├── diagnostic.py #     currentOp, top, profile, replSet stubs
    │   └── users.py      #     createUser, dropUser, usersInfo
    ├── cursors.py        #   CursorRegistry with idle reaper
    ├── context.py        #   ConnectionContext (per-connection state)
    ├── sessions.py       #   SessionRegistry
    ├── transactions.py   #   Transaction state, undo journal
    ├── profiler.py       #   Profiler, OpTracker, TopStats
    ├── bson_codec.py     #   BSON ↔ engine type normalization (LocalClient path)
    └── errors.py         #   Mongo-compatible error response formatting
```
