# Future Plans: The Path to Rust

**Same engine. Every language. Every platform.**

smongo today is a fully functional embedded MongoDB engine in Python -- 11,000+ lines of code, 960+ tests, 25+ aggregation stages, a wire protocol server, lazy streaming reads, and bidirectional Atlas sync. It proves the architecture works. The next step is to push the core down into a compiled language so that the same engine can be embedded in Python, Node.js, Go, Ruby, the browser, and anywhere else a developer writes code.

The target language is **Rust**. The strategy is **incremental replacement** -- not a rewrite-from-scratch.

```
Today                                    Tomorrow
┌─────────────────────────┐              ┌─────────────────────────┐
│     Python Application  │              │   Any Application       │
│                         │              │   Python · Node · Go    │
│   from smongo import    │              │   Ruby · Browser (WASM) │
│       MongoClient       │              │                         │
└────────────┬────────────┘              └────────────┬────────────┘
             │                                        │
             ▼                                        ▼
┌─────────────────────────┐              ┌─────────────────────────┐
│   Pure Python Engine    │              │     libsmongo (Rust)    │
│                         │     ───►     │                         │
│  storage/ · query/      │              │  storage · query ·      │
│  aggregation/ · index   │              │  aggregation · index    │
│  oplog · sync · wire/   │              │  oplog · sync · wire    │
│                         │              │                         │
│      WiredTiger (C)     │              │      WiredTiger (C)     │
└─────────────────────────┘              └─────────────────────────┘
     Python-only                           Embeds in any language
     GIL-bound concurrency                 Fearless concurrency
     ~11K lines Python                     ~15-20K lines Rust
```

---

## Why Rust

| Criterion | Python (today) | Rust (tomorrow) |
|---|---|---|
| **Embedding** | Python-only; other languages must use the wire protocol server as a separate process | Any language via C FFI; in-process embedding everywhere |
| **Performance** | GIL limits concurrency; pure-Python MQL compilation and aggregation hit CPU ceilings on large datasets | Zero-cost abstractions; no GIL; SIMD-friendly data paths |
| **Memory safety** | Runtime errors (KeyError, TypeError) discovered in production | Compile-time ownership and borrowing; if it compiles, it won't segfault |
| **Distribution** | Requires Python runtime + pip install | Single static library (`libsmongo.a`) or shared object (`.so` / `.dylib` / `.dll`) |
| **WebAssembly** | Not viable | Compile to WASM; run in the browser with IndexedDB/OPFS as the storage backend |
| **C interop** | ctypes/cffi -- workable but fragile | `bindgen` generates safe wrappers from C headers; `unsafe` blocks are explicit and auditable |

Rust is not chosen for speed alone. It is chosen because it is the only modern systems language that provides **memory safety without a garbage collector**, **fearless concurrency without a GIL**, and **first-class FFI to both C (downward) and every high-level language (upward)**.

---

## Current State: The Python Proof

The existing Python codebase is the **blueprint and the test harness**. Every algorithm, every edge case, every operator behavior has already been designed, implemented, and tested. The Rust port is not R&D -- it is a translation with a 960+ test correctness oracle.

| Module | Lines | What It Does |
|---|---:|---|
| `smongo/storage/` | 1,540 | WiredTiger wrapper, BSON I/O, transactions, locking, streaming cursors (all plan types), find_one, count |
| `smongo/query/` | 1,279 | MQL compiler (`compile_query`), update engine (`apply_update`), 60+ expression operators, dot-path traversal |
| `smongo/aggregation/` | 1,512 | 25+ pipeline stages, 17 group accumulators, joins, vector search, spill-to-disk |
| `smongo/index.py` | 903 | B-Tree index manager, query planner, IEEE 754 key encoding, prefix scoring |
| `smongo/sync.py` | 838 | Bidirectional sync, conflict resolution, checkpointing, exponential backoff |
| `smongo/wire/` | 3,810 | TCP server, OP_MSG/OP_QUERY framing, 80+ command handlers, sessions, profiler |
| `smongo/client.py` | 476 | URI routing (local/remote/hybrid), streaming find/find_one/count, bulk_write, find_one_and_* |
| `smongo/oplog.py` | 348 | Append-only operations log, compaction, change streams |
| Remaining | 397 | `schema.py`, `objectid.py`, `logging.py`, `_compat.py`, `_types.py` |
| **Total** | **11,161** | |

---

## The Strategy: Strangler Fig

The migration follows the [Strangler Fig](https://martinfowler.com/bliki/StranglerFigApplication.html) pattern. Instead of rewriting everything in a vacuum and hoping it works, each Python module is replaced by a Rust equivalent **one at a time**, exposed back to Python via [PyO3](https://pyo3.rs/). The existing Python tests validate each swap. At no point does the project stop working.

```
Phase 1    Phase 2    Phase 3    Phase 4    Phase 5    Phase 6+
┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐
│Storag│   │Query │   │ Agg  │   │Oplog │   │ Wire │   │Multi-│
│  e   │   │  +   │   │      │   │  +   │   │Proto │   │ Lang │
│Core  │   │Index │   │Pipe  │   │Sync  │   │      │   │  +   │
│      │   │Engine│   │      │   │      │   │      │   │ WASM │
└──┬───┘   └──┬───┘   └──┬───┘   └──┬───┘   └──┬───┘   └──┬───┘
   │          │          │          │          │          │
   ▼          ▼          ▼          ▼          ▼          ▼
 PyO3       PyO3       PyO3      PyO3/      tokio     napi-rs
 bridge     bridge     bridge    tokio      async     cgo/ffi
                                                      wasm-pack
```

Each phase produces a **hybrid binary**: a Python package (`pip install smongo`) backed by a compiled Rust extension (`.so` / `.pyd`). Python users see zero API changes. Performance improves. And with each phase, more of the engine becomes language-agnostic.

---

## Phase 1: Storage Core

**Goal**: Replace `smongo/storage/` with a Rust library that wraps WiredTiger and handles BSON document I/O.

**Replaces**: `smongo/storage/` (~1,492 lines) -- `engine.py`, `collection.py`, `locking.py`, `results.py`, `streaming.py`, `helpers.py`, `transaction.py`

**What gets built in Rust**:
- WiredTiger C FFI bindings via `bindgen` (session management, cursor operations, transactions)
- Safe Rust wrappers: `WtConnection`, `WtSession`, `WtCursor`, `WtTransaction`
- BSON document storage using the `bson` crate (encode/decode, type preservation)
- `RwLock<T>` replaces the Python `ReadWriteLock` -- Rust's standard library provides this with compile-time safety
- Streaming cursor as a Rust iterator exposed to Python via PyO3's `__iter__` / `__next__` (the Python `StreamingCursor` already handles all plan types -- PK lookup, index scan, `$in` multi-point, `$or`-union, and collection scan -- so the Rust port has a clear spec)

**Key Rust crates**:
| Crate | Purpose |
|---|---|
| `bindgen` | Generate Rust FFI bindings from WiredTiger's `wiredtiger.h` |
| `bson` | Official MongoDB BSON library for Rust |
| `pyo3` | Expose Rust structs and functions as a Python C extension |
| `maturin` | Build and publish the PyO3 extension as a Python wheel |

**Why this is first**: Storage is the foundation. Every other module depends on it. Once the storage layer is Rust, every read and write flows through compiled code, and the biggest performance bottleneck (BSON serialization + WiredTiger cursor operations) is eliminated.

**Validation**: Run the full `tests/test_storage.py` suite against the Rust-backed storage. Zero test changes required -- the Python API surface is identical.

**The hard part**: WiredTiger's C API uses raw pointers, thread-local sessions, and manual memory management. Wrapping this in Rust's ownership model requires careful `unsafe` blocks and a clear ownership hierarchy (`Connection` owns `Session`s, `Session` owns `Cursor`s).

---

## Phase 2: MQL Compiler + Index Engine

**Goal**: Move query parsing, predicate compilation, update application, expression evaluation, and the B-Tree index manager into Rust.

**Replaces**: `smongo/query/` (~1,279 lines) + `smongo/index.py` (~903 lines)

**What gets built in Rust**:
- `compile_query()` -- parse a BSON document into a predicate tree; evaluate against candidate documents
- All query operators: `$gt`, `$lt`, `$gte`, `$lte`, `$eq`, `$ne`, `$in`, `$nin`, `$exists`, `$regex`, `$not`, `$all`, `$elemMatch`, `$size`, `$type`, `$or`, `$and`, `$nor`
- `apply_update()` -- all 14+ update operators (`$set`, `$inc`, `$push`, `$unset`, `$addToSet`, `$pull`, `$pop`, `$min`, `$max`, `$rename`, `$currentDate`, `$mul`)
- `resolve_expr()` -- 60+ aggregation expression operators
- Dot-notation path traversal (`get_value`, `set_value`, `unset_value`) over BSON documents
- `IndexManager` -- B-Tree index CRUD, IEEE 754 key encoding, hex inversion for descending fields
- `QueryPlanner` -- prefix-scoring plan selection, bound compilation, index scan execution, `$in` multi-point scan, `$or` union

**Key Rust crates**:
| Crate | Purpose |
|---|---|
| `bson` | Document traversal and manipulation |
| `regex` | `$regex` operator support |
| `ordered-float` | NaN-safe float comparisons for sort keys |

**The hard part**: Python's dynamic typing makes MQL compilation trivial -- a `dict` is a `dict`, a value is a value. In Rust, every BSON value is an enum (`Bson::Int32`, `Bson::String`, `Bson::Document`, etc.). Evaluating `{"age": {"$gt": 30}}` requires matching on the BSON variant, handling type coercion (Int32 vs Int64 vs Double), and managing `Option<T>` for missing fields. This is verbose but correct -- and the Rust compiler guarantees every case is handled.

**Validation**: Run `tests/test_query.py` and `tests/test_index.py` against the Rust-backed modules.

---

## Phase 3: Aggregation Pipeline

**Goal**: Move the pipeline engine into Rust for high-throughput in-memory data processing.

**Replaces**: `smongo/aggregation/` (~1,512 lines) -- `cursor.py`, `stages.py`, `joins.py`, `output.py`, `vector.py`, `constants.py`

**What gets built in Rust**:
- Pipeline executor: chain of stage functions consuming and producing `Vec<Document>` (or a streaming iterator for memory efficiency)
- All 25+ stages, with particular focus on the compute-heavy ones:
  - `$group` with 17 accumulators (hash-based grouping with `HashMap<Bson, Accumulator>`)
  - `$sort` with multi-key comparators and spill-to-disk (external merge sort via temp files)
  - `$lookup` / `$graphLookup` with nested engine calls
  - `$vectorSearch` with SIMD-accelerated distance computation
- `$facet` as parallel sub-pipeline execution (Rust's `rayon` for data parallelism)

**Key Rust crates**:
| Crate | Purpose |
|---|---|
| `bson` | Document manipulation within pipeline stages |
| `rayon` | Data-parallel execution for `$facet` sub-pipelines |
| `ndarray` | Vector math for `$vectorSearch` (NumPy equivalent) |
| `usearch` | ANN index for approximate nearest neighbor (optional, via FFI) |
| `tempfile` | Spill-to-disk for memory-bounded `$sort` and `$group` |

**The payoff**: Aggregation is where Python hurts the most. A `$group` over 1M documents with multiple accumulators involves millions of hash lookups and BSON field accesses. In Rust, this runs 10-50x faster with zero GC pauses.

**Validation**: Run `tests/test_aggregation.py` and `tests/test_aggregation_guardrails.py`.

---

## Phase 4: Oplog + Sync

**Goal**: Move the oplog and sync layer into Rust, using async Rust (`tokio`) for background sync threads.

**Replaces**: `smongo/oplog.py` (~348 lines) + `smongo/sync.py` (~838 lines)

**What gets built in Rust**:
- `OplogWriter` / `OplogReader` backed by WiredTiger (already in Rust from Phase 1)
- Change stream event emission via Rust channels (`tokio::sync::broadcast`)
- `SyncManager` as a `tokio` async task:
  - Push: tail oplog, batch operations, bulk write to remote via the official `mongodb` Rust driver
  - Pull: change streams with resume tokens (preferred) or timestamp polling (fallback)
  - Conflict resolution: LWW, local-wins, remote-wins, field-level merge, custom callback (via PyO3 callable bridge)
  - Checkpoint persistence in WiredTiger
  - Exponential backoff with configurable ceiling
  - Selective sync filters compiled as Rust predicates

**Key Rust crates**:
| Crate | Purpose |
|---|---|
| `tokio` | Async runtime for background sync tasks |
| `mongodb` | Official MongoDB Rust driver for remote push/pull |
| `sha2` | Oplog entry checksum computation |

**The payoff**: The sync layer runs in a background thread today, limited by Python's GIL. In Rust with `tokio`, push and pull can run concurrently on separate async tasks with true parallelism. Change stream processing becomes non-blocking.

**Validation**: Run `tests/test_oplog.py` and `tests/test_sync_unit.py`. Integration tests (`tests/integration/`) against a real MongoDB via Docker.

---

## Phase 5: Wire Protocol Server

**Goal**: Replace the Python TCP server with a high-performance async Rust server using `tokio`.

**Replaces**: `smongo/wire/` (~3,810 lines) -- `server.py`, `msg.py`, `bson_codec.py`, `context.py`, `cursors.py`, `sessions.py`, `transactions.py`, `profiler.py`, `errors.py`, `commands/`

**What gets built in Rust**:
- Async TCP listener (`tokio::net::TcpListener`) with per-connection tasks (no thread-per-connection overhead)
- Zero-copy OP_MSG parsing using the `bytes` crate (BytesMut for read buffers, Buf trait for cursor advancement)
- 80+ command handlers ported from Python, dispatched via a `HashMap<&str, fn(...)>`
- `CursorRegistry` with atomic cursor IDs and idle reaper task
- BSON boundary normalization (ObjectId string ↔ BSON ObjectId) in Rust
- CRC-32C checksum validation for `OP_COMPRESSED`

**Key Rust crates**:
| Crate | Purpose |
|---|---|
| `tokio` | Async TCP server and task scheduling |
| `bytes` | Zero-copy buffer management for wire protocol parsing |
| `bson` | BSON encode/decode at the wire boundary |
| `crc32c` | Checksum validation for OP_COMPRESSED |

**The payoff**: The wire protocol server is the gateway for every non-Python client. A Rust async server can handle thousands of concurrent `mongosh` / driver connections with microsecond-level message parsing. This transforms smongo from a Python library into a legitimate lightweight `mongod` replacement for development and edge deployments.

**Validation**: Run `tests/test_wire_*.py` (6 test files). Connect `mongosh` and PyMongo to the Rust server and verify command parity.

---

## Phase 6: Multi-Language Bindings

**Goal**: Once the core is 100% Rust (`libsmongo`), expose it to every major language ecosystem.

At this point, the Rust core is a standalone library with a C-compatible FFI surface. Language bindings are thin wrappers.

| Language | Binding Technology | Distribution |
|---|---|---|
| **Python** | PyO3 (already in place from Phase 1) | `pip install smongo` |
| **Node.js** | `napi-rs` | `npm install smongo` |
| **Go** | CGo wrapping the C FFI | `go get smongo` |
| **Ruby** | `magnus` | `gem install smongo` |
| **Java/Kotlin** | JNI via `jni` crate | Maven Central |
| **C/C++** | Direct `libsmongo.h` header | Static/shared library |

Each binding exposes the same API surface:

```
client = SmongoClient("local://data")
db = client["mydb"]
coll = db["users"]

coll.insert_one({"name": "Alice", "age": 34})
coll.find({"age": {"$gt": 30}})
coll.aggregate([{"$group": {"_id": "$city", "count": {"$sum": 1}}}])
```

The wire protocol server (Phase 5) already makes smongo accessible from any language over TCP. The bindings go further: **in-process embedding** with no network hop, no serialization overhead, and no separate server process. This is the SQLite model.

---

## Phase 7: WebAssembly

**Goal**: Compile the Rust core to WebAssembly and run smongo in the browser.

**The challenge**: WiredTiger is a C library that assumes POSIX file I/O. It cannot run in a browser sandbox. The WASM build replaces the WiredTiger storage backend with a browser-native alternative.

**Storage backend options**:
| Backend | Pros | Cons |
|---|---|---|
| **IndexedDB** | Universal browser support; async key-value store | Async-only; callback-heavy API |
| **OPFS (Origin Private File System)** | Synchronous file access in a Worker; closest to POSIX semantics | Requires a Web Worker; relatively new API |
| **In-memory only** | Simplest; no persistence API needed | Data lost on page refresh |

**Architecture**:

```
┌──────────────────────────────────────────────────┐
│              Browser Application                  │
│                                                   │
│   import { MongoClient } from "smongo-wasm";      │
│   const db = new MongoClient("local://data");     │
└──────────────────┬───────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────┐
│              smongo-wasm (Rust → WASM)            │
│                                                   │
│  MQL Compiler · Query Planner · Aggregation       │
│  Index Engine · Oplog · Sync (fetch API)          │
│                                                   │
│  Storage trait:                                   │
│    impl WiredTigerBackend  (native builds)        │
│    impl OpfsBackend        (WASM + Worker)        │
│    impl IndexedDbBackend   (WASM + main thread)   │
│    impl InMemoryBackend    (WASM, no persistence) │
└──────────────────┬───────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────┐
│          SyncManager (fetch API → Atlas)          │
│          Push/pull over HTTPS                     │
└──────────────────────────────────────────────────┘
```

**Build toolchain**: `wasm-pack` compiles the Rust core to a `.wasm` module + JS glue. Published to npm as `smongo-wasm`.

**The payoff**: A JavaScript developer installs `smongo-wasm`, writes MQL queries against a local document store in the browser, and syncs to Atlas in the background. Full offline-first web applications with MongoDB's query language. No server. No Docker. No backend.

---

## Risk Register

| Risk | Severity | Mitigation |
|---|---|---|
| **WiredTiger C FFI complexity** | High | WiredTiger's API uses raw pointers, thread-local sessions, and manual memory management. Mitigation: build a narrow, well-tested `wiredtiger-sys` crate first; audit every `unsafe` block; run under Miri and AddressSanitizer. |
| **BSON dynamic typing in Rust** | Medium | MQL operates on schema-less documents. Rust requires explicit type matching for every BSON variant. Mitigation: build a `BsonExt` trait with ergonomic helpers (`doc.get_i64("age")`, `doc.get_str("name")`) that return `Option<T>`. |
| **Python API compatibility** | Medium | The PyO3 bridge must expose the exact same Python API. Mitigation: the 960+ existing tests are the contract. Every phase runs the full test suite before merging. |
| **Two codebases during migration** | Medium | During phases 1-5, the project contains both Python and Rust code. Mitigation: the Strangler Fig approach means each module is swapped atomically. CI runs tests against both the pure-Python fallback and the Rust extension. |
| **Rust learning curve for contributors** | Low | Mitigation: the Python codebase remains the reference implementation. Contributors can read the Python to understand intent, then write the Rust equivalent. Comprehensive doc comments on every public Rust function. |
| **WASM storage backend performance** | Medium | IndexedDB is async and slower than POSIX file I/O. OPFS is synchronous but requires a Web Worker. Mitigation: abstract the storage backend behind a trait so backends are swappable. Benchmark each option. |

---

## Milestone Timeline

| Phase | Scope | Estimated Duration | Cumulative Rust LOC |
|---|---|---|---|
| **Phase 1** | Storage Core | 4-6 weeks | ~3,000 |
| **Phase 2** | MQL Compiler + Index Engine | 6-8 weeks | ~7,000 |
| **Phase 3** | Aggregation Pipeline | 6-8 weeks | ~11,000 |
| **Phase 4** | Oplog + Sync | 3-4 weeks | ~13,000 |
| **Phase 5** | Wire Protocol Server | 4-6 weeks | ~18,000 |
| **Phase 6** | Multi-Language Bindings | 2-4 weeks per language | ~19,000 + bindings |
| **Phase 7** | WebAssembly | 4-6 weeks | ~20,000 + WASM backend |

**Total estimated timeline**: 6-9 months for a single experienced Rust developer to reach Phase 5 (full Rust core with Python bindings). Phases 6 and 7 can be parallelized across contributors.

Each phase ships independently. After Phase 1, `pip install smongo` already benefits from compiled storage. After Phase 5, the engine is language-agnostic. After Phase 7, it runs in the browser.

---

## Repository Structure (Post-Migration)

```
smongo/
├── Cargo.toml                    # Rust workspace root
├── crates/
│   ├── smongo-core/              # The engine: storage, query, aggregation, index, oplog
│   │   ├── src/
│   │   │   ├── storage/          # WiredTiger wrapper
│   │   │   ├── query/            # MQL compiler, update engine, expressions
│   │   │   ├── aggregation/      # Pipeline stages, accumulators, vector search
│   │   │   ├── index.rs          # B-Tree index manager, query planner
│   │   │   ├── oplog.rs          # Operations log, change streams
│   │   │   ├── schema.rs         # $jsonSchema validation
│   │   │   └── objectid.rs       # MongoDB-compatible ObjectId
│   │   └── Cargo.toml
│   ├── smongo-sync/              # SyncManager (depends on smongo-core + tokio + mongodb)
│   ├── smongo-wire/              # Wire protocol server (depends on smongo-core + tokio)
│   ├── smongo-ffi/               # C FFI surface for multi-language bindings
│   └── smongo-wasm/              # WASM build (alternative storage backend)
├── bindings/
│   ├── python/                   # PyO3 bridge → pip install smongo
│   ├── node/                     # napi-rs bridge → npm install smongo
│   ├── go/                       # CGo bridge
│   └── ruby/                     # magnus bridge
├── python/                       # Pure-Python fallback (original smongo/)
│   └── smongo/                   # Kept during migration as reference + fallback
├── tests/                        # Original Python test suite (correctness oracle)
├── web_app.py                    # Flask dashboard (unchanged)
├── templates/                    # Dashboard HTML
├── static/                       # Dashboard CSS/JS
└── README.md
```

---

## How to Contribute

### If you know Rust

Pick a module from the phase list. Read the corresponding Python source -- it is the specification. Write the Rust equivalent. Run the Python tests. Open a PR.

The best starting points:
1. **`smongo/objectid.py`** (108 lines) -- a self-contained module with clear spec, zero dependencies on the rest of the engine. Perfect first Rust PR.
2. **`smongo/query/paths.py`** (dot-notation path traversal) -- small, well-tested, foundational for everything else.
3. **`smongo/storage/locking.py`** (ReadWriteLock) -- nearly a 1:1 map to `std::sync::RwLock`.

### If you know Python

The Python codebase is the reference implementation. Help by:
- Adding more tests (especially edge cases that will catch Rust port regressions)
- Improving doc comments that explain *why*, not just *what*
- Profiling hot paths to guide Rust optimization priorities

### If you know WebAssembly

Phase 7 needs someone who understands browser storage APIs (IndexedDB, OPFS), `wasm-bindgen`, and the `wasm-pack` toolchain. The Rust core provides the engine; the WASM work is about the storage backend abstraction and the JavaScript API surface.

---

## The End State

When Phase 7 is complete, smongo is:

- **A Rust library** (`libsmongo`) that any language can embed in-process
- **A Python package** (`pip install smongo`) with compiled Rust underneath
- **An npm package** (`npm install smongo`) for Node.js applications
- **A WASM module** (`npm install smongo-wasm`) for browser applications
- **A wire protocol server** that any MongoDB driver can connect to over TCP
- **A sync engine** that bidirectionally replicates with MongoDB Atlas

One query language. One document model. One sync protocol. Every platform.

```
         ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
         │ Python  │  │ Node.js │  │   Go    │  │ Browser │
         │  app    │  │  app    │  │  app    │  │  app    │
         └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘
              │            │            │            │
              ▼            ▼            ▼            ▼
         ┌─────────────────────────────────────────────────┐
         │              libsmongo (Rust)                     │
         │                                                   │
         │   MQL · Aggregation · Indexes · Oplog · Sync     │
         │                                                   │
         │   ┌───────────────┐    ┌───────────────────────┐ │
         │   │  WiredTiger   │    │  OPFS / IndexedDB     │ │
         │   │  (native)     │    │  (WASM)               │ │
         │   └───────────────┘    └───────────────────────┘ │
         └──────────────────────────┬──────────────────────┘
                                    │
                                    ▼
                          ┌──────────────────┐
                          │  MongoDB Atlas   │
                          │  (cloud sync)    │
                          └──────────────────┘
```

Small MongoDB. Big ambitions. Everywhere.
