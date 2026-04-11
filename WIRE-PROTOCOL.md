# Wire Protocol

**How smongo speaks native MongoDB binary protocol so that any driver -- in any language -- can connect.**

---

## Why a Wire Protocol Matters

Without a wire protocol, smongo is a Python library. With one, it becomes a **drop-in replacement for `mongod`**.

```bash
$ python -m smongo.wire --port 27018
$ mongosh mongodb://localhost:27018
```

`mongosh` connects. PyMongo connects. The Node.js driver connects. The Go driver connects. Compass connects. Every MongoDB tool in the ecosystem speaks the same binary protocol over TCP, and this server speaks it back.

This is what transforms an embedded engine into something that can serve as a development server, a test double, or an edge database that real applications talk to using their real drivers.

---

## Message Anatomy

Every MongoDB wire protocol message starts with a 16-byte header:

```
 Byte offset:  0         4         8         12        16
               ┌─────────┬─────────┬─────────┬─────────┐
               │  length  │requestId│responseTo│ opCode  │
               │  int32   │  int32  │  int32   │  int32  │
               └─────────┴─────────┴─────────┴─────────┘
```

| Field | Size | Purpose |
|---|---|---|
| `length` | 4 bytes | Total message size including this header |
| `requestId` | 4 bytes | Client-assigned ID for request/response correlation |
| `responseTo` | 4 bytes | The `requestId` this message is responding to |
| `opCode` | 4 bytes | Message type: `2013` (OP_MSG) or `2004` (OP_QUERY) |

The header is decoded with a single `struct.unpack`:

```python
length, req_id, resp_to, op_code = struct.unpack_from("<iiii", data, 0)
```

All integers are little-endian, following MongoDB's convention.

---

## OP_MSG (Opcode 2013) -- The Modern Protocol

OP_MSG is the current MongoDB wire protocol. Every modern driver uses it for all operations after the initial handshake.

### Structure

```
┌──────────────────────────────────────────────────────────────┐
│  Header (16 bytes)                                            │
├──────────────────────────────────────────────────────────────┤
│  Flags (4 bytes, uint32)                                      │
│    bit 0: checksumPresent                                     │
│    bit 1: moreToCome                                          │
├──────────────────────────────────────────────────────────────┤
│  Sections (variable length)                                   │
│    Kind 0: Single BSON document (the command)                 │
│    Kind 1: Document sequence (batch documents)                │
├──────────────────────────────────────────────────────────────┤
│  Optional CRC-32C checksum (4 bytes, if bit 0 set)            │
└──────────────────────────────────────────────────────────────┘
```

### Section Kind 0 -- The Command

Every OP_MSG contains exactly one Kind 0 section: a BSON document whose **first key** names the command. Examples:

```
{"find": "users", "filter": {"age": {"$gt": 30}}, "$db": "myapp"}
{"insert": "users", "documents": [...], "$db": "myapp"}
{"aggregate": "orders", "pipeline": [...], "$db": "myapp"}
```

### Section Kind 1 -- Document Sequences

Kind 1 sections carry batches of documents outside the main command body. This is how drivers send large insert batches efficiently -- the documents are encoded as a separate sequence, identified by a string name (e.g., `"documents"`, `"updates"`, `"deletes"`).

```
Kind 1 section layout:
┌──────────┬────────────┬──────────────────────────────────┐
│ size     │ identifier │ BSON doc | BSON doc | BSON doc   │
│ int32    │ cstring    │ (repeated until size exhausted)   │
└──────────┴────────────┴──────────────────────────────────┘
```

The decoder:

```python
section_size = struct.unpack_from("<i", data, offset)[0]
section_end = offset + section_size
offset += 4
null_pos = data.index(b"\x00", offset)
identifier = data[offset:null_pos].decode("utf-8")
offset = null_pos + 1
docs = []
while offset < section_end:
    doc_size = struct.unpack_from("<i", data, offset)[0]
    docs.append(_bson_decode(data[offset : offset + doc_size]))
    offset += doc_size
```

### The moreToCome Flag

When bit 1 of the flags is set, the client is indicating it doesn't need a response for this message. The server skips the reply. This is used for unacknowledged writes.

---

## OP_QUERY (Opcode 2004) -- Legacy Handshake

OP_QUERY is the legacy protocol. Modern drivers only use it for the initial handshake (`isMaster`/`hello` discovery), then switch to OP_MSG for everything else.

```
┌──────────────────────────────────────────────────────────────┐
│  Header (16 bytes)                                            │
├──────────────────────────────────────────────────────────────┤
│  Flags (4 bytes, int32)                                       │
│  Collection name (cstring, null-terminated)                   │
│  Skip (4 bytes, int32)                                        │
│  Limit (4 bytes, int32)                                       │
│  Query document (BSON)                                        │
└──────────────────────────────────────────────────────────────┘
```

The server detects OP_QUERY messages and checks for handshake commands. If the query contains `isMaster`, `ismaster`, or `hello`, it routes through the standard dispatcher. Everything else also goes through dispatch.

The response is an OP_REPLY (opcode 1):

```python
payload = struct.pack("<iqii", 0, cursor_id, starting_from, len(docs))
payload += docs_bson
```

---

## The Command Dispatcher

The dispatcher is the routing layer between the wire protocol and the embedded engine. It extracts the command name (first key of the BSON body), looks up a registered handler, calls it, and wraps any exceptions into Mongo-compatible error responses.

```
OP_MSG arrives
    │
    ▼
decode_msg(data) → header, flags, body_doc, doc_sequences
    │
    ▼
dispatch(ctx, body_doc, doc_sequences)
    │
    ▼
command_name = first key of body_doc (e.g., "find", "insert", "hello")
    │
    ▼
_HANDLERS[command_name](ctx, body_doc, doc_sequences)
    │
    ▼
response_doc → encode_msg() → send over TCP
```

**Performance / Rust dispatch:** Wire `find` and `aggregate` bypass the Python aggregation `Cursor` entirely. For `find`, sort, skip, limit, and projection are applied in Rust before the first batch is sent; for `aggregate`, the handler calls the Rust `aggregate_pipeline` directly. Admin and diagnostic paths use typed **`RedbLocalCollection`** / Rust handlers where possible instead of per-call Python `getattr` on hot paths.

### Registered Commands (80+)

| Category | Commands |
|---|---|
| **Handshake** | `hello`, `ismaster`, `isMaster`, `ping`, `buildInfo`, `hostInfo`, `getLog`, `getCmdLineOpts`, `whatsmyuri` |
| **CRUD** | `find`, `insert`, `update`, `delete`, `count`, `distinct`, `findAndModify`, `bulkWrite`, `getLastError`, `estimatedDocumentCount`, `dataSize` |
| **Cursors** | `getMore`, `killCursors` |
| **Indexes** | `listIndexes`, `createIndexes`, `dropIndexes`, `reIndex` |
| **Aggregation** | `aggregate`, `mapReduce` |
| **Admin** | `listDatabases`, `listCollections`, `create`, `drop`, `dropDatabase`, `explain`, `collMod`, `renameCollection`, `compact`, `collStats`, `dbStats`, `validate`, `fsync`, `getnonce`, `getParameter`, `setParameter` |
| **Sessions** | `startSession`, `endSessions`, `abortTransaction`, `commitTransaction` |
| **Users** | `usersInfo`, `rolesInfo`, `createUser`, `dropUser`, `updateUser` |
| **Diagnostic** | `currentOp`, `killOp`, `connPoolStats`, `features`, `logRotate`, `top`, `profile`, `setProfilingLevel`, `system.profile`, `shardingState`, `replSetGetConfig`, `replSetGetStatus`, `setFreeMonitoring`, `lockInfo`, `listCommands` |
| **Sync** | `client.sync` |
| **Auth** | `saslStart`, `saslContinue` (graceful rejection) |

### Handler Registration

Handlers are registered via decorator:

```python
@_register("hello", "ismaster", "isMaster")
def _cmd_hello(ctx, cmd, seqs):
    return {
        "ismaster": True,
        "maxBsonObjectSize": 16 * 1024 * 1024,
        "maxWireVersion": 21,
        "ok": 1.0,
        ...
    }
```

Multiple command names can map to the same handler. This is how `ismaster`, `isMaster`, and `hello` all route to the same function.

### Error Safety

The dispatcher wraps every handler call in a try/except that catches known error types (`DuplicateKeyError`, `ValidationError`) and maps them to standard MongoDB error codes. Unknown exceptions are caught by a safety net that returns code 1 (`InternalError`):

```python
try:
    return handler(ctx, command_doc, doc_sequences or {})
except DuplicateKeyError as exc:
    return error_response(11000, "DuplicateKey", str(exc))
except ValidationError as exc:
    return error_response(121, "DocumentValidationFailure", str(exc))
except Exception as exc:
    return error_response(1, "InternalError", str(exc))
```

This guarantees the wire layer always returns valid BSON -- it never drops a connection due to an unhandled exception.

---

## BSON Boundary Normalization

The wire protocol operates in BSON land (binary BSON over TCP). The embedded engine operates in Python dict land (with `smongo.ObjectId`, floats, regex dicts). The BSON codec (`rust/smongo-py/src/raw_bson.rs`) delegates to the official Rust `bson` crate (maintained by MongoDB) for both encoding and decoding, guaranteeing spec-compliant output compatible with every MongoDB driver and tool (PyMongo, Compass, mongosh, Node.js driver).

### Decode (Wire bytes → Engine dicts)

`raw_decode_document` delegates to `bson::from_slice` (Rust `bson` crate) to parse raw BSON into a `bson::Document`, then converts to a Python dict via `doc_to_pydict`. All BSON types are handled correctly:

```
BSON ObjectId   → smongo.ObjectId
BSON DateTime   → Python datetime.datetime (UTC)
BSON Decimal128 → bson.Decimal128
BSON Regex      → bson.Regex
BSON Timestamp  → bson.Timestamp
BSON Binary     → bytes / uuid.UUID (subtype 4) / bson.Binary
BSON Int32/Int64→ int / bson.Int64
BSON Double     → float
BSON Document   → dict (recursive)
BSON Array      → list (recursive)
BSON MinKey/MaxKey → bson.MinKey / bson.MaxKey
```

### Encode (Engine dicts → Wire bytes)

`raw_encode_document` converts Python dicts to a `bson::Document` via `pydict_to_doc`, then serializes with `bson::to_vec`. All Python types are mapped to their correct BSON counterparts:

```
smongo.ObjectId    → BSON ObjectId
datetime.datetime  → BSON DateTime
bson.Decimal128    → BSON Decimal128
bson.Regex         → BSON Regex
bson.Timestamp     → BSON Timestamp
bson.Int64         → BSON Int64 (preserved, not demoted to Int32)
bson.Binary        → BSON Binary (with subtype)
uuid.UUID          → BSON Binary subtype 4
int                → BSON Int32 / Int64 (auto-sized)
float              → BSON Double
```

---

## Connection Model

```
┌──────────────────────────────────────────────────────────────┐
│                        WireServer                             │
│                                                              │
│  ┌───────────────┐    ┌───────────────────────────────────┐  │
│  │ Accept Thread  │    │ Shared State                       │ │
│  │ (1 per server)│    │                                     │ │
│  │               │    │  RedbLocalClient (smongo-engine + redb) │ │
│  │ Accepts TCP   │    │  CursorRegistry (cross-connection)  │ │
│  │ connections   │    │  SyncManager (optional)              │ │
│  └───────┬───────┘    └───────────────────────────────────┘  │
│          │                                                    │
│    ┌─────┼────────────────────┐                              │
│    ▼     ▼                    ▼                              │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                      │
│  │ Conn #1 │  │ Conn #2 │  │ Conn #3 │   ...                │
│  │ Thread  │  │ Thread  │  │ Thread  │                       │
│  │         │  │         │  │         │                       │
│  │ Context │  │ Context │  │ Context │                       │
│  │ (private│  │ (private│  │ (private│                       │
│  │  DBs)   │  │  DBs)   │  │  DBs)   │                       │
│  └─────────┘  └─────────┘  └─────────┘                      │
└──────────────────────────────────────────────────────────────┘
```

Each TCP connection is handled by a Tokio async task with its own `ConnectionContext`. The context caches **`RedbLocalDB`** handles for the lifetime of the connection. A `Semaphore` limits concurrent connections.

The server shares a single **`RedbLocalClient`** and `CursorRegistry` across all connections. Thread safety comes from:
- Engine-side locking on **`RedbLocalCollection`**
- `Mutex`-guarded state in `CursorRegistry`
- `Mutex`-guarded collection cache in **`RedbLocalDB`**

---

## Server-Side Cursors

When a `find` or `aggregate` command returns more results than the batch size (default 101), the server creates a cursor and returns the first batch plus a non-zero cursor ID. The client calls `getMore` to retrieve subsequent batches.

```
Client                              Server
  │                                   │
  │  find({}, batchSize: 2)           │
  │ ──────────────────────────────►   │
  │                                   │  Creates cursor #7291...
  │  firstBatch: [doc1, doc2]         │
  │  cursorId: 7291...                │
  │ ◄──────────────────────────────   │
  │                                   │
  │  getMore(7291..., batchSize: 2)   │
  │ ──────────────────────────────►   │
  │                                   │
  │  nextBatch: [doc3, doc4]          │
  │  cursorId: 7291...                │
  │ ◄──────────────────────────────   │
  │                                   │
  │  getMore(7291...)                 │
  │ ──────────────────────────────►   │
  │                                   │  Cursor exhausted, deleted
  │  nextBatch: [doc5]                │
  │  cursorId: 0                      │
  │ ◄──────────────────────────────   │
```

Cursor IDs are random 63-bit integers. The registry supports:
- **LRU eviction** when at capacity (default 10,000 cursors)
- **Idle timeout** (default 600 seconds) with a background reaper thread
- **Explicit kill** via `killCursors` command

---

## Handshake: What Drivers See

When a MongoDB driver connects, it sends either an OP_QUERY or OP_MSG `hello`/`isMaster` command. The server responds with capabilities that tell the driver what it can do:

```python
{
    "ismaster": True,
    "isWritablePrimary": True,
    "maxBsonObjectSize": 16777216,     # 16 MB
    "maxMessageSizeBytes": 50331648,   # 48 MB
    "maxWriteBatchSize": 100000,
    "minWireVersion": 0,
    "maxWireVersion": 21,
    "connectionId": 1,
    "ok": 1.0
}
```

Wire version 0-21 tells the driver this server supports the full modern protocol. The driver then negotiates features based on wire version and proceeds with normal operations.

---

## Authentication

smongo has two wire server implementations with different authentication capabilities:

**Python `WireServer` (default):** No authentication. When a driver attempts SASL authentication (`saslStart`), the server returns a clear error:

```python
{
    "ok": 0,
    "errmsg": "Authentication is not supported in embedded mode. Connect without credentials.",
    "code": 18,
    "codeName": "AuthenticationFailed"
}
```

Drivers that connect without credentials in the URI work immediately.

**Rust `RustWireServer`:** Supports **SCRAM-SHA-256** authentication (RFC 7677) with PBKDF2-hashed credentials persisted in the local engine (`__users` KV table). RBAC role checks enforce authorization on all commands (handshake commands exempted). **TLS** is available via [rustls](https://github.com/rustls/rustls) when `tls_cert_file`/`tls_key_file` are provided.

---

## findAndModify

The wire protocol implements the full `findAndModify` command, which PyMongo's `find_one_and_update`, `find_one_and_replace`, and `find_one_and_delete` all use under the hood:

```python
cmd = {
    "findAndModify": "users",
    "query": {"_id": "abc123"},
    "update": {"$set": {"status": "active"}},
    "new": True,         # return the modified document
    "$db": "myapp"
}
```

The handler detects whether the update document contains `$`-prefixed operators (update) or plain keys (replacement) and routes accordingly.
