# Architecture: smongo

**A true MongoDB experience running locally on WiredTiger, with bidirectional sync to the real cloud.**

---

## The Core Thesis

MongoDB's power was never just the query language or the document model — it was the **storage engine underneath** and the **protocol that speaks to it**. This project takes those two pillars and brings them to the edge: a fully embedded MongoDB-compatible engine backed by **WiredTiger's B-tree tables**, speaking the **real MongoDB wire protocol (OP_MSG)**, and maintaining a **persistent oplog** that enables bidirectional synchronization with **MongoDB Atlas**.

The result is not a mock. It is not SQLite pretending to be Mongo. It is **WiredTiger running your data in B-trees on disk**, an **MQL compiler executing real query predicates**, a **query planner choosing real index scans**, and a **TCP server that real MongoDB drivers can connect to**. When the network is available, every local mutation flows upstream to Atlas, and every remote change flows back — with conflict resolution.

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Application Layer                             │
│   MongoClient("local://...")  ·  PyMongo driver  ·  mongosh         │
└──────────────┬────────────────────────────┬──────────────────────────┘
               │ Python API                 │ TCP :27017
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
│  │ query/       │  │ index.py      │  │  Pipeline    │  │ Validator│ │
│  │              │  │               │  │  aggregation/│  │ schema.py│ │
│  │ compile_query│  │ plan()        │  │              │  │          │ │
│  │ apply_update │  │ score_index() │  │              │  │ $json    │ │
│  │ resolve_expr │  │ execute_scan()│  │  25+ stages  │  │ Schema   │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └────┬─────┘ │
│         │                 │                  │               │       │
│         ▼                 ▼                  ▼               ▼       │
│  ┌──────────────────────────────────────────────────────────────────┐│
│  │                    Storage Layer (storage/)                     ││
│  │                                                                  ││
│  │  LocalClient → LocalDB → LocalCollection                        ││
│  │      │              │            │                               ││
│  │      │              │            ├── Data Table: table:{db}_{col}││
│  │      │              │            ├── Oplog Table: table:__oplog_ ││
│  │      │              │            ├── Index Tables: table:__idx_  ││
│  │      │              │            └── Metadata: table:__idxmeta_  ││
│  │      │              │                                            ││
│  │      ▼              ▼                                            ││
│  │  wiredtiger_open("create")  →  session.create("key=S,value=u")  ││
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
│  CHECKPOINT: WiredTiger table:__sync_checkpoint                      │
│                                                                      │
│  ┌─────────┐     oplog entries     ┌─────────────────────────────┐   │
│  │  LOCAL   │ ──────────────────►  │   MongoDB Atlas / Cloud     │   │
│  │ WiredTiger│ ◄────────────────── │   (PyMongo bulk_write)      │   │
│  └─────────┘   change streams      └─────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 1. WiredTiger: The Same Engine That Powers MongoDB

### Why WiredTiger Changes Everything

WiredTiger is not an abstraction. It is **the production storage engine inside every MongoDB server since 3.2**. When this project calls `wiredtiger_open(db_path, "create")`, it creates the same B-tree file structures, the same page cache, the same checkpoint mechanism, and the same MVCC concurrency control that runs in production MongoDB clusters handling millions of operations per second.

This is not "MongoDB-like" storage. This **is** MongoDB's storage.

### Table Architecture

The data table uses `key_format=S, value_format=u` — string keys mapping to **raw BSON bytes**. Documents are encoded via `bson.encode()` (PyMongo's C-optimized codec) and decoded via `bson.decode()`. This preserves MongoDB type fidelity (int32 vs int64 vs double, ObjectId, datetime, etc.) and avoids the serialization overhead of JSON. Oplog and index metadata tables remain `key_format=S, value_format=S` (JSON strings) for debuggability.

```
WiredTiger Data Directory
├── table:{db}_{collection}              # Document data (key = _id, value = BSON bytes)
├── table:__oplog_{db}_{collection}      # Operation log per collection (JSON)
├── table:__idx_{db}_{coll}_{index}      # One B-tree per secondary index
├── table:__idxmeta_{db}_{collection}    # Index definitions (survives restarts)
└── table:__sync_checkpoint              # Sync progress markers
```

Each `LocalCollection` opens its own WiredTiger **session** (from the shared connection) and protects it with a **per-collection `ReadWriteLock` + `threading.Lock`**:

- **Thread safety**: A `ReadWriteLock` allows concurrent readers while serializing writers. An inner `threading.Lock` protects WiredTiger session cursor operations. `LocalDB` also holds a lock protecting its `_collections` dict.
- **Isolation**: Each collection's cursor operations don't interfere with others.
- **Crash safety**: WiredTiger's checkpointing ensures data survives process crashes.
- **B-tree ordering**: Primary keys are stored in sorted order, enabling efficient range scans.

### The Write Path

Every write — insert, update, delete — is wrapped in a **WiredTiger transaction** that ensures atomicity across the data table, all affected index tables, and the oplog. If any step fails (e.g. a unique index violation during update), the entire transaction is rolled back cleanly.

For updates and deletes, the **query planner accelerates document matching**: the new `_find_matching_docs(query)` method reuses the same `QueryPlanner` that powers `find()` — using `pk_lookup` (O(log n) by `_id`), `index_scan` (O(log n + k) via B-tree range), or `collection_scan` (O(n) fallback) — so that writes against indexed fields never scan the entire collection.

```
Application write
    │
    ▼
┌──────────────────┐
│  Acquire Lock    │  ← per-collection ReadWriteLock + Lock
└────────┬─────────┘
         ▼
┌──────────────────┐
│  Begin Txn       │  ← session.begin_transaction()
└────────┬─────────┘
         ▼
┌──────────────────┐
│ Schema Validation │  ← $jsonSchema enforcement
└────────┬─────────┘
         ▼
┌──────────────────┐
│ Index Maintenance │  ← add_doc / update_doc / remove_doc on every index
└────────┬─────────┘
         ▼
┌──────────────────┐
│  WiredTiger Write │  ← cursor[_id] = bson.encode(doc) with overwrite=true
└────────┬─────────┘
         ▼
┌──────────────────┐
│  Version Bump    │  ← monotonic counter per document for conflict detection
└────────┬─────────┘
         ▼
┌──────────────────┐
│  Oplog Append    │  ← timestamped, checksummed, with changed_fields tracking
└────────┬─────────┘
         ▼
┌──────────────────┐
│  Commit Txn      │  ← session.commit_transaction() (or rollback on error)
└──────────────────┘
```

The `_internal=True` flag allows the sync layer to write documents pulled from the cloud without re-logging them to the oplog, preventing infinite echo loops in bidirectional sync.

### Streaming Read Path

The read architecture is designed around **lazy iteration**. Instead of materializing every matching document into a Python list, the engine yields documents one at a time from WiredTiger:

```
Application: coll.find({"city": "NYC"}).limit(10)
    │
    ▼
┌──────────────────────────────────────────┐
│  Collection.find()  (client.py)          │
│  → calls LocalCollection.find_streaming()│
│  → wraps result in Cursor(iterable)      │
└────────────────┬─────────────────────────┘
                 ▼
┌──────────────────────────────────────────┐
│  StreamingCursor.__iter__()              │
│                                          │
│  1. Acquire read lock                    │
│  2. Consult QueryPlanner.plan(query)     │
│  3. Branch on plan_type:                 │
│     ├── pk_lookup   → single cursor.search(), yield 0-1 docs
│     ├── index_scan  → walk index B-tree, yield per-id lookup
│     ├── $in scan    → multi-point seek, yield per-id lookup
│     ├── or_union    → execute subplans, dedup, yield per-id
│     └── coll_scan   → cursor.next() loop, yield per-doc
│  4. Release read lock on generator exit  │
└────────────────┬─────────────────────────┘
                 ▼
┌──────────────────────────────────────────┐
│  Cursor._resolve()  (aggregation/)       │
│                                          │
│  • No sort: itertools.islice(source, N)  │
│    → only N docs pulled from generator   │
│  • With sort: materialize, sort, slice   │
│  • Results cached after first resolution │
└──────────────────────────────────────────┘
```

**Why this matters**: A `find({}).limit(10)` on a million-document collection deserializes exactly 10 BSON documents. `find_one()` deserializes exactly 1. `count_documents()` iterates the WiredTiger cursor without building a list. The streaming path uses the same query planner as the materialized `find()`, so index scans, PK lookups, and `$or`-union plans all benefit.

The materialized `LocalCollection.find(query) → list[Document]` remains available for internal callers (write paths, wire protocol commands that need the full list for sorting), but the public `Collection.find()` facade and the wire protocol `find` command both use the streaming path.

### Primary Key Lookups

When querying by `_id`, WiredTiger's B-tree gives us a direct O(log n) lookup:

```python
cursor = session.open_cursor(table_uri)
cursor.set_key(str(doc_id))
if cursor.search() == 0:
    doc = bson.decode(cursor.get_value())
```

No table scan. No index. Just the raw power of a B-tree seek — the same operation that serves every `findOne({_id: ...})` in production MongoDB.

---

## 2. The Index Engine: WiredTiger B-Trees All the Way Down

### Lexicographic Key Encoding

The index engine solves a fundamental problem: how do you make WiredTiger's lexicographic byte ordering match MongoDB's type-aware comparison semantics? The answer is a carefully designed encoding scheme:

```
Type Prefix Hierarchy (ensures cross-type ordering):
    "00"              → None (sorts lowest)
    "1" + IEEE 754    → Numbers (with sign-bit manipulation for correct ordering)
    "15" + hex        → ObjectId
    "2" + UTF-8 hex   → Strings
    "30" / "31"       → Boolean false / true (sorts highest)
```

For numbers, the encoder performs **IEEE 754 sign-bit manipulation**: negative numbers have all bits inverted, positive numbers have only the sign bit flipped. This transforms the floating-point binary representation into one where lexicographic byte comparison produces numerically correct ordering:

```python
packed = struct.pack(">d", float(value))
b = bytearray(packed)
if b[0] & 0x80:           # negative: invert all bits
    b = bytearray(~x & 0xFF for x in b)
else:                       # non-negative: flip sign bit
    b[0] ^= 0x80
return "1" + b.hex()
```

For **descending indexes**, the encoded key is run through a hex digit inversion (`0↔f, 1↔e, ...`), reversing the sort order without changing the B-tree's native ascending traversal.

### Composite Index Keys

Multi-field indexes encode each field value with its direction applied, separated by pipes, with the document `_id` appended as a tiebreaker:

```
encoded_field_1 | encoded_field_2 | ... | _id

Example for index {age: 1, name: -1} on doc {_id: "abc", age: 30, name: "Alice"}:
"1[encoded_30]|[inverted_encoded_Alice]|abc"
```

### The Query Planner

The planner scores candidate indexes by **longest prefix match** against query conditions:

| Condition Type | Score | Effect |
|---|---|---|
| Equality (`field: value`) | +2 | Tight bound on both sides |
| Range (`$gt`, `$lt`, `$gte`, `$lte`) | +1 | Open or closed bound |
| `$in` | +1 | Multi-point scan: one seek per value, merge results |
| `$or` at top level | — | `or_union` if every branch is indexed, else collection scan |

The winning index's bounds are compiled into WiredTiger-domain keys. The planner then executes a **cursor range scan**:

```
1. search_near(lower_bound_key)
2. Advance cursor forward
3. Collect _id values until key > upper_bound_key
4. Fetch full documents by _id
5. Re-filter with compiled MQL predicate (index is acceleration, not full pushdown)
```

### Index Types

| Type | Description |
|---|---|
| **Single field** | `create_index("email")` → `email_1` |
| **Compound** | `create_index([("age", 1), ("name", -1)])` → `age_1_name_-1` |
| **Unique** | `create_index("email", unique=True)` — enforced via prefix scan before insert |
| **Sparse** | `create_index("phone", sparse=True)` — skips docs where field is None |
| **TTL** | `create_index("createdAt", expireAfterSeconds=3600)` — background reaper thread |

---

## 3. The MQL Compiler: MongoDB's Query Language in Pure Python

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

## 4. The Aggregation Pipeline: 25+ Stages Deep

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
$ python -m smongo.wire --port 27017
$ mongosh mongodb://localhost:27017
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
Auth:          saslStart, saslContinue (graceful rejection — embedded mode)
```

### BSON Boundary Normalization

The wire layer maintains a clean boundary between the BSON world (drivers) and the JSON world (engine):

**Inbound** (`normalize_inbound`): `bson.ObjectId` → `str`, `Decimal128` → `float`, `Regex` → `{"$regex", "$options"}` dict.

**Outbound** (`normalize_outbound`): 24-character hex string `_id` values → `bson.ObjectId` for proper BSON encoding on the wire.

### Connection Model

Each TCP connection gets its own daemon thread with a private `ConnectionContext`. The server shares a single `LocalClient` (and thus a single WiredTiger connection) and a single `CursorRegistry` across all connections. Cursor IDs are random 63-bit integers with an idle reaper (600s default).

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

The oplog key is `{time_ns:020d}-{uuid4}`, ensuring **lexicographic time ordering** in WiredTiger's B-tree while remaining globally unique.

### Oplog Compaction

The oplog grows with every mutation. For long-running embedded deployments, unbounded growth is a disk-space and performance problem. The oplog now supports bounded growth:

- **`OplogWriter.truncate_before(key)`**: Delete all entries lexicographically before a given key (used by sync after pushing entries to Atlas).
- **`OplogWriter.truncate_count(max_entries)`**: Keep only the last N entries, deleting the oldest.
- **`OplogReader.count()`** / **`OplogReader.oldest_key()`**: Monitoring primitives.
- **`LocalCollection.compact_oplog(keep=1000)`**: Public API for manual compaction.
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

The sync layer is what transforms a standalone embedded database into a **distributed data system**. It bridges the gap between "works offline" and "works everywhere" by maintaining eventual consistency between the local WiredTiger engine and a remote MongoDB Atlas cluster.

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
4. **Checkpoint** the last oplog key to `push:{ns}` in WiredTiger

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

ObjectIds are naturally time-ordered (the timestamp prefix sorts first), which means WiredTiger's B-tree stores documents roughly in insertion order — similar to production MongoDB's behavior.

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
# Mode 1: Pure local — WiredTiger only, no network
client = MongoClient("local://my_data")

# Mode 2: Pure remote — delegates to PyMongo
client = MongoClient("mongodb+srv://cluster.mongodb.net/mydb")

# Mode 3: Hybrid — local WiredTiger + background sync to Atlas
client = MongoClient("local://my_data", sync="mongodb+srv://cluster.mongodb.net")
```

| Mode | Storage | Network | Sync |
|---|---|---|---|
| `local` | WiredTiger on disk | None | None |
| `remote` | MongoDB Atlas | Required | N/A (direct) |
| `hybrid` | WiredTiger on disk | Optional | Bidirectional to Atlas |

In hybrid mode, the client constructs a `SyncManager` and starts it automatically. Collections are registered for sync as they are accessed through `Database.__getitem__` or `create_collection`.

The `Collection` wrapper provides the full MongoDB API surface: `find`, `find_one`, `insert_one`, `insert_many`, `update_one`, `update_many`, `delete_one`, `delete_many`, `find_one_and_update`, `find_one_and_replace`, `find_one_and_delete`, `bulk_write`, `aggregate`, `count_documents`, `watch`, `create_index`, `drop_index`, `list_indexes`, `explain`, `get_oplog`.

In local mode, **`find()` returns a lazy `Cursor`** backed by a `StreamingCursor`. Documents are deserialized from WiredTiger only as the cursor is consumed. **`find_one()`** delegates to `LocalCollection.find_one()` which stops after the first match. **`count_documents()`** delegates to `LocalCollection.count()` which iterates without building a list (and uses `count_fast()` for empty queries to skip BSON deserialization entirely). **`aggregate()`** pulls its input documents from `find_streaming()` rather than `get_all()`.

`bulk_write` accepts a list of operation descriptors (`InsertOne`, `UpdateOne`, `UpdateMany`, `DeleteOne`, `DeleteMany`, `ReplaceOne`) and executes them in order (or unordered), returning a `BulkWriteResult` with `inserted_count`, `matched_count`, `modified_count`, and `deleted_count`.

---

## 11. The Power of Localized True MongoDB

### What "True MongoDB" Means

This is not a document database that happens to use similar method names. The fidelity runs deep:

1. **Same storage engine**: WiredTiger's B-trees provide the same durability guarantees, page-level concurrency, and checkpoint-based recovery. Documents are stored as **native BSON bytes** — the same binary format MongoDB uses on disk.

2. **Same write semantics**: Every write is wrapped in a **WiredTiger transaction** (data + indexes + oplog as a single atomic unit). Validation failures or unique-index violations trigger a clean rollback. A **per-collection ReadWriteLock** ensures thread safety with concurrent reader access.

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
| **wiredtiger** | B-tree storage engine (data, oplog, indexes, checkpoints) | Yes (local mode) |
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
├── storage/              # WiredTiger storage engine package
│   ├── engine.py         #   LocalClient, LocalDB
│   ├── collection.py     #   LocalCollection (CRUD, txns, streaming find_one/count)
│   ├── locking.py        #   ReadWriteLock
│   ├── results.py        #   InsertResult, UpdateResult, DeleteResult
│   ├── streaming.py      #   StreamingCursor (all plan types: PK, index, $in, or_union, scan)
│   └── helpers.py        #   BSON encode/decode helpers
├── query/                # MQL compiler package
│   ├── compiler.py       #   compile_query, query operators
│   ├── update.py         #   apply_update, positional operators
│   ├── expressions.py    #   resolve_expr, 60+ expression operators
│   └── paths.py          #   get_value, set_value, unset_value
├── aggregation/          # Pipeline engine package
│   ├── cursor.py         #   Cursor (accepts Iterable, lazy materialization), aggregate dispatch
│   ├── stages.py         #   Core stages: $match, $group, $sort, etc.
│   ├── joins.py          #   $lookup, $graphLookup, $unionWith
│   ├── output.py         #   $facet, $out, $merge
│   └── vector.py         #   $vectorSearch (NumPy / USearch)
├── index.py              # IndexManager, QueryPlanner, key encoding, DuplicateKeyError
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
    ├── bson_codec.py     #   BSON ↔ JSON normalization at the wire boundary
    └── errors.py         #   Mongo-compatible error response formatting
```
