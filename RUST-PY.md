# Rust / Python Architecture

> How (and why) smongo splits a MongoDB-compatible database across two languages.

---

## The Split at a Glance

| Layer | Language | Lines | Purpose |
|-------|----------|-------|---------|
| WiredTiger FFI (`wt_safe/`) | Pure Rust | ~1,200 | RAII wrappers around the WiredTiger C ABI via `libloading`. Zero PyO3. |
| WiredTiger bridge (`wt_bridge.rs`) | Rust + PyO3 | ~400 | `#[pyclass]` types (`RustWtSession`, `RustWtCursor`) that expose `wt_safe` to Python. |
| Core engine | Rust + PyO3 | ~22,800 | Query compiler, BSON codec, index encoding, SCRAM-SHA-256, aggregation, wire protocol, storage engine, streaming cursors, oplog, sync/CRDT. |
| Orchestration & config | Python | ~9,000 | Server boot (`engine.py`), collection helpers, aggregation output stages, user-facing API (`LocalClient`), test harness. |
| **Total** | | **~33,000** | ~73% Rust, ~27% Python. Rust codebase ~24,083 LOC. |

50 Rust source files. 54 Python source files. ~50 `#[pyclass]` types. ~80 `#[pyfunction]` exports.

---

## Why This Split

**Rust owns the hot path.** Every CPU-bound, latency-sensitive, or security-critical operation runs in compiled Rust:

- **Wire protocol**: TCP accept, framing, compression (Snappy/Zstd/Zlib), CRC32C checksums, OP_MSG / OP_QUERY / OP_COMPRESSED decode/encode.
- **BSON codec**: Inbound/outbound normalization, ObjectId generation.
- **Query engine**: Predicate compilation (`$eq`, `$gt`, `$in`, `$elemMatch`, `$regex`, ...), full scan + filter, index-accelerated scans.
- **Update engine**: All update operators (`$set`, `$inc`, `$push`, `$pull`, `$addToSet`, `$rename`, ...) with positional and array filter support.
- **Aggregation**: 25+ pipeline stages (`$group`, `$sort`, `$unwind`, `$project`, `$lookup`, `$graphLookup`, `$facet`, `$bucket`, `$setWindowFields`, ...).
- **Index management**: B-tree encoding, text index tokenization, unique/TTL/partial/hashed index support, `explain()` plans.
- **Authentication**: SCRAM-SHA-256 (PBKDF2 + HMAC), RBAC role checks.
- **Storage engine**: WiredTiger session lifecycle, cursor operations, batch insert/update, crash recovery.
- **Transactions**: Multi-document ACID transactions with snapshot isolation.
- **Replication primitives**: Oplog, change streams, vector clocks, CRDT merge strategies.
- **Wire commands**: 40+ MongoDB commands dispatched from Rust (`find`, `insert`, `update`, `delete`, `aggregate`, `createIndexes`, `hello`, `getMore`, `commitTransaction`, ...).

**Python owns orchestration.** Things that run once at startup, touch user-facing APIs, or change frequently during development:

- Server boot and configuration (`engine.py`)
- The `LocalClient` / `LocalDB` convenience wrappers
- Aggregation output stages (`$out`, `$merge`) that do cross-collection writes
- Command registration (`_register` decorator, `_HANDLERS` dict)
- User store management
- Test harness and fixtures

### Three Runtime Components

| Component | What it provides | How it's used |
|-----------|-----------------|---------------|
| **WiredTiger** (`wiredtiger` pip package) | B-tree storage, ACID transactions, crash recovery, checkpoints | Storage engine — all data, indexes, oplog, and sync checkpoints live in WiredTiger tables |
| **PyMongo** (`pymongo` pip package) | BSON codec, `ObjectId`, Atlas connectivity, bulk write operations | BSON encode/decode on the **storage** path; sync layer uses PyMongo as a real MongoDB driver to Atlas |
| **Rust extension** (`_smongo_core` via PyO3/maturin) | Query compiler, update engine, wire protocol, streaming cursors, index manager, SCRAM auth, TLS | The performance-critical engine — every request-serving path runs through compiled Rust |

PyMongo is not a client library here. It is a BSON codec and a sync transport. The actual database engine is WiredTiger + Rust. PyMongo provides `bson.encode()`/`bson.decode()` for the storage layer and `MongoClient` for syncing to Atlas.

---

## The Interop Boundary

### PyO3 0.28

The entire Rust crate compiles to a single `_smongo_core.so` (cdylib) via [maturin](https://www.maturin.rs/). Python imports it as `import _smongo_core`.

The module is organized into six registration groups:

```
_smongo_core
├── core        ObjectId, path accessors, BSON helpers, query compiler, schema validation
├── storage     Locking, result types, scan/filter, storage engine, indexes, query planner, streaming
├── aggregation 25+ stage functions + join stages
├── sync        Oplog, change streams, vector clocks, CRDTs, sync manager
├── wire        Msg framing, codec, errors, cursors, sessions, profiler, transactions, context, dispatch, TCP server
└── wt_bridge   WiredTiger session/cursor wrappers, transaction sessions
```

### The Two BSON Paths

smongo has two distinct BSON serialization paths, optimized for different use cases.

**Wire path — raw BSON in Rust.** When a MongoDB driver connects via TCP, the wire protocol server receives raw BSON bytes. These are decoded directly in Rust by `raw_bson.rs`:

```
Driver sends OP_MSG
    │
    ▼
┌──────────────────────────────────────┐
│  raw_bson::raw_decode_document()     │
│                                      │
│  BSON bytes → Python dict, inline:   │
│  • ObjectId (12 bytes) → smongo.ObjectId
│  • DateTime (int64 ms) → Python datetime
│  • Decimal128 → Python float          │
│  • Regex → {"$regex", "$options"} dict│
│  • Int32/Int64 → Python int           │
│  • Double → Python float              │
│  • Nested docs → recursive decode     │
│                                      │
│  No intermediate bson::Document.      │
│  No PyMongo bson.decode() call.       │
│  Single pass, zero copies.            │
└──────────────────────────────────────┘
    │
    ▼
Engine-ready Python dict
```

Response encoding works the same way in reverse: `raw_encode_document()` serializes Python dicts directly to BSON bytes — all in Rust, without touching PyMongo.

**Storage path — PyMongo BSON.** When documents are written to or read from WiredTiger data tables, they use PyMongo's C-optimized `bson.encode()`/`bson.decode()`. PyMongo's codec is battle-tested, handles every BSON type correctly, and provides full round-trip type fidelity for durable storage.

| Concern | Wire path (Rust `raw_bson`) | Storage path (PyMongo `bson`) |
|---------|---------------------------|------------------------------|
| **Latency** | Critical (per-request) | Amortized (write-once, read-few) |
| **Type mapping** | Wire types → engine types (normalize inline) | Full BSON round-trip fidelity |
| **GIL** | No GIL needed (pure Rust) | Requires GIL (`bson.encode` is C extension) |
| **Allocation** | Zero intermediate `bson::Document` | `bson::Document` as intermediate |

### ObjectId: The Bridge Type

`smongo.ObjectId` is a Rust `#[pyclass]` that follows the MongoDB ObjectId spec exactly (4-byte timestamp + 5-byte random + 3-byte counter). It bridges the wire protocol and storage engine:

```
Wire decode:   12 raw bytes   → smongo.ObjectId (Rust-native)
Storage read:  bson.ObjectId  → converted to smongo.ObjectId at collection boundary
User code:     smongo.ObjectId used everywhere
Storage write: smongo.ObjectId → 24-char hex string as WT key + bson.ObjectId in encoded doc
Wire encode:   smongo.ObjectId → 12 raw bytes in OP_MSG response
```

The conversion between `bson.ObjectId` (PyMongo) and `smongo.ObjectId` (Rust) happens at the collection boundary — when documents exit the storage layer, `bson.ObjectId` instances are converted via the `normalize_inbound` adapter. The class reference is resolved once via `PyOnceLock`.

### Typed Handler Dispatch

Every wire command handler has this signature:

```rust
pub type HandlerFn = fn(
    Python<'_>,
    &Bound<'_, ConnectionContext>,  // typed — not PyAny
    &Bound<'_, PyDict>,             // command document
    &Bound<'_, PyAny>,              // document sequences
) -> PyResult<Py<PyAny>>;
```

`rs_dispatch` casts `ctx` to `ConnectionContext` **once**, then passes the typed reference to every handler. Handlers access connection state (session registry, cursor registry, op tracker, cached imports, transaction state) via direct Rust field access — zero `getattr()` calls, zero Python attribute protocol overhead.

Any commands without a Rust handler fall through to Python via the cached `_HANDLERS` dict.

### CachedImports

Python module references that would otherwise require a `py.import()` per command are resolved once at server startup and shared across all connections via `Arc<CachedImports>`:

```rust
pub(crate) struct CachedImports {
    pub user_store: Py<PyAny>,
    pub user_store_lock: Py<PyAny>,
    pub audit_mod: Py<PyAny>,
    pub topology_pid: Py<PyAny>,
    pub git_version: Py<PyAny>,
    pub server_start: f64,
    pub help_dict: Py<PyAny>,
    pub handlers: Py<PyDict>,         // cached _HANDLERS dict
}
```

For all frequently-used modules (both stdlib and smongo internals), a process-wide `PyOnceLock` cache (pyo3 0.28's free-threading-safe equivalent of `OnceLock`) in `cached_modules.rs` turns Python dict lookups into Rust atomic loads:

```rust
macro_rules! cached_module {
    ($lock:ident, $fn_name:ident, $module:literal) => {
        static $lock: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
        pub fn $fn_name(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> { ... }
    };
}

// 18 stdlib modules: datetime, json, re, builtins, copy, time, operator, uuid,
//   platform, os, sys, logging, random, secrets, resource, types, threading
// 2 third-party: bson, bson.json_util
// 9 smongo internals: aggregation.stages, aggregation.constants,
//   aggregation.output, aggregation.joins, aggregation.vector,
//   wire.context, audit, objectid, storage.transaction
// (schema module removed -- validation is now pure Rust in schema.rs)
// 8 cached class attributes (cached_attr! / cached_nested_attr!): bson.ObjectId,
//   bson.Decimal128, bson.Regex, builtins.int, builtins.float, builtins.round,
//   datetime.datetime, datetime.timezone.utc
// CachedSystemInfo: platform/OS fields resolved once per process
// cached_pid(): process ID resolved once per process
```

**Zero `py.import()` calls remain on any request-serving path.** The only imports that run are in `wire_server.rs` startup (once per process) and inside the `PyOnceLock` macro (once per module, amortized to an atomic load).

Wire protocol `find` and `aggregate` no longer construct the Python `Cursor` class. The `find` command applies sort, skip, limit, and projection in pure Rust. The `aggregate` command calls `aggregate_pipeline` directly from Rust.

### WiredTiger typed borrow (oplog / admin)

`RustWtSession` and `RustWtCursor` expose `pub(crate)` helpers (`open_cursor_typed`, `next_rc`, `get_key_str`, and related accessors) that use the underlying `wt_safe` types directly and bypass Python method dispatch. Oplog code (47 call sites) and admin WiredTiger work (metadata walks, statistics, user persistence) use these typed paths. `OplogHub` change-stream listeners hold `Py<ChangeStream>` instead of `Py<PyAny>`.

### Rust-native schema validation

`$jsonSchema` document validation runs entirely in Rust (`schema.rs`). `ValidationError` is a Rust-defined PyO3 exception. `RustLocalCollection::validate_doc` calls `crate::schema::validate_document` directly -- zero `call_method`, zero `py.import()`. The Python `smongo/schema.py` re-exports the Rust symbols for backward compatibility. Regex pattern matching reuses the ReDoS-safe helpers from `query_compiler.rs`.

### Pure-Rust Wire Compression

Wire protocol compression (OP_COMPRESSED) uses Rust crates directly — no Python bridging:

| Compressor | Crate | Notes |
|------------|-------|-------|
| Snappy | `snap 1.x` | Raw format (no framing) per MongoDB spec |
| Zstandard | `zstd 0.13` | Default compression level |
| Zlib | `flate2 1.x` | Deflate/inflate |

The `available_compressors()` function is pure Rust — no Python probe needed.

---

## The Four Tiers

```
┌─────────────────────────────────────────────────┐
│                   Python Layer                   │
│  engine.py · LocalClient · aggregation output    │
│  command registration · test harness             │
├─────────────────────────────────────────────────┤
│              Wire Protocol (Rust)                │
│  wire_server.rs (Tokio TCP) · wire_dispatch.rs   │
│  wire_commands/ (40+ handlers) · wire_msg.rs     │
│  wire_context.rs · wire_sessions.rs              │
├─────────────────────────────────────────────────┤
│             Core Engine (Rust)                   │
│  query_compiler · query_expressions · query_update│
│  aggregation · index_manager · storage · scram   │
│  bson_helpers · objectid · streaming · oplog     │
├─────────────────────────────────────────────────┤
│           WiredTiger FFI (Pure Rust)             │
│  wt_safe/ — RAII Connection/Session/Cursor       │
│  wiredtiger-sys/ — dlopen + ABI verification     │
└─────────────────────────────────────────────────┘
```

**Tier 1 (bottom)** has zero PyO3 dependency — it could be extracted into a standalone Rust crate.

**Tier 2** is pure computation — it touches `PyAny` at boundaries but the algorithms are Rust-native.

**Tier 3** is the seam — PyO3 `#[pyclass]`/`#[pyfunction]` types that bridge Rust logic to the Python event loop.

**Tier 4 (top)** is Python-only orchestration that delegates heavy lifting downward.

---

## The PyMongo Sync Transport

When smongo runs in hybrid mode (`MongoClient("local://data", sync="mongodb+srv://...")`), PyMongo transforms from a BSON codec into a full MongoDB driver. The `SyncManager` uses PyMongo's `MongoClient` to:

1. **Push** local mutations to Atlas via `bulk_write(InsertOne, UpdateOne, DeleteOne)`
2. **Pull** remote changes via MongoDB Change Streams (with resume tokens)
3. **Manage indexes** — mirror remote index definitions locally
4. **Handle authentication** — PyMongo manages Atlas credentials, TLS, and connection pooling

```
Local write
    │
    ▼
RustLocalCollection.insert()  ← Rust engine, WiredTiger
    │
    ├── Oplog append (WiredTiger table)
    │
    ▼
SyncManager._push()
    │
    ├── Tail oplog entries
    ├── Convert to PyMongo bulk operations
    │
    ▼
PyMongo bulk_write() → Atlas  ← PyMongo as transport
```

PyMongo's strength here is its production-grade connection pooling, automatic retries, server monitoring, and full Atlas support. smongo delegates the cloud transport to PyMongo and focuses the Rust engine on the local path.

---

## Typed Boundaries

The interop is tightened at key boundaries to eliminate runtime type erasure:

| Boundary | Parameter | Type |
|----------|-----------|------|
| `HandlerFn` context | `ctx` | `&Bound<'_, ConnectionContext>` (not `PyAny`) |
| `eval_query` document | `doc` | `&Bound<'_, PyDict>` (not `PyAny`) |
| `handle_message` context | `ctx` | `&Bound<'_, ConnectionContext>` (not `PyAny`) |
| `check_auth_gate` context | `ctx` | `&Bound<'_, ConnectionContext>` (not `PyAny`) |
| `create_connection_context` return | — | `Bound<'py, ConnectionContext>` (not `PyAny`) |
| `normalize_inbound_dict` return | — | `Bound<'py, PyDict>` (not `Py<PyAny>`) |
| `CursorRegistry::create` | via Rust borrow | Direct `pub(crate)` call (not Python method dispatch) |
| `CursorRegistry::create_change_stream` | via Rust borrow | Direct `pub(crate)` call (not Python method dispatch) |

The `paths` module (`get_value`, `set_value`, `unset_value`) intentionally stays `PyAny`-based because it traverses mixed dict/list structures where static typing would add casts without benefit.

---

## Free-Threaded Python (3.13t+)

The Rust extension is compatible with CPython's free-threaded build (no GIL). Key adaptations:

- **`#[pymodule(gil_used = false)]`**: The `_smongo_core` module declares that it does not depend on the GIL for thread safety.
- **`PyOnceLock`**: All Python-valued statics (`Py<PyModule>`, `Py<PyAny>`) use `pyo3::sync::PyOnceLock` instead of `std::sync::OnceLock` to avoid deadlocks during free-threading stop-the-world synchronization events.
- **No `Python::assume_attached()`**: All code paths that need a `Python<'_>` token receive it as a parameter.
- **`unsafe impl Send/Sync`**: All 18+ instances across 9 files are documented with explicit safety rationale referencing Rust-native synchronization primitives (`InlineRwLock`, `parking_lot::Mutex`, PyO3 borrow checking), not the GIL.

CI tests against `python3.13t` with `PYTHON_GIL=0`. See [BYE-BYE-GIL.md](BYE-BYE-GIL.md) for the full story.

---

## Build & Test

### Build

```bash
cd rust && maturin develop --release
# or: cargo build (for Rust-only checks)
```

Zero warnings policy enforced by `clippy::unwrap_used = "deny"` and `clippy::expect_used = "deny"`.

### Rust Tests

```bash
cd rust && cargo test
# 80 tests — all passing
```

Tests cover: ObjectId generation, SCRAM-SHA-256 handshake, query compilation, wire message framing, compression roundtrips, WiredTiger ABI integration (open/close, CRUD, transactions, checkpoint, verify, compact), RBAC role logic, sync utilities, BSON encoding.

### Python Tests

```bash
pytest tests/ -x -q
# 1,010+ tests
```

The Python suite exercises the full stack end-to-end: CRUD, aggregation pipelines, indexes, transactions, wire protocol, streaming cursors, oplog, change streams, security, crash recovery, performance.

### Interop Benchmarks

```bash
pytest tests/performance/test_perf_interop.py -m performance -v --benchmark-disable-gc
```

Measures dispatch-level overhead isolated from data size: `findAndModify` (~64 us), `insert+delete` (~91 us), `find+limit` (~287 us), `aggregate` 3-stage (~457 us), `count` (~378 us). See [PERF.md](PERF.md) for full results.

---

## Dependency Graph

```toml
[dependencies]
pyo3 = "0.28"              # Python ↔ Rust bridge
tokio = "1"                # Async TCP server runtime
wiredtiger-sys = { path }  # WiredTiger C ABI bindings (dlopen)
parking_lot = "0.12"       # Fast mutexes
snap = "1"                 # Snappy compression
zstd = "0.13"              # Zstandard compression
flate2 = "1"               # Zlib compression
sha2 = "0.10"              # SHA-256 for SCRAM
hmac = "0.12"              # HMAC for SCRAM
pbkdf2 = "0.12"            # Key derivation for SCRAM
bson = "2"                 # BSON serialization
regex = "1"                # $regex query operator
tokio-rustls = "0.26"      # TLS support
hex = "0.4"                # ObjectId hex encoding
uuid = "1"                 # UUID v4 generation
crc32c = "0.6"             # Wire protocol checksums
getrandom = "0.2"          # OS randomness for ObjectId
```

Release profile: `lto = "thin"`, `codegen-units = 1`, `strip = "symbols"`.

---

## Design Principles

1. **Rust owns compute, Python owns orchestration.** If it touches every request, it's in Rust. If it runs once at startup or changes frequently, it stays in Python.

2. **Type at the boundary, erase inside.** Handler signatures are typed (`ConnectionContext`, `PyDict`). Internal path traversal stays `PyAny` because documents are schema-free.

3. **Cache imports, not results.** Python module references are resolved once and stored in `Arc<CachedImports>` or `PyOnceLock<Py<PyModule>>` (free-threading-safe). The data they produce stays in Python-managed memory.

4. **No unsafe without proof.** The only `unsafe` blocks are in `wt_safe/` (C FFI) and `wiredtiger-sys` (dlopen). The PyO3 layer is 100% safe Rust.

5. **One cast, not thirty.** `rs_dispatch` casts `ctx` once; handlers get a typed reference. `eval_query` takes `&Bound<'_, PyDict>` once; callers that have `PyAny` cast at the call site.

6. **Pure Rust where possible.** Wire compression, SCRAM crypto, index encoding, and CRC32C use Rust crates — no Python library bridging on the hot path.
