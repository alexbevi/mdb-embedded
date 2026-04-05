# Future Plans

**Where smongo goes from here.**

smongo today: 24,100 lines of Rust, 9,000 lines of Python, 50 Rust source files, 54 Python source files, 1,090 tests (80 Rust + 1,010 Python), 25+ aggregation stages, a Tokio wire protocol server with TLS and SCRAM-SHA-256, lazy streaming reads, and bidirectional Atlas sync. The Rust migration is complete. What follows is the roadmap for turning a good hybrid engine into a great one -- then into a universal one.

---

## Current Architecture Snapshot

```
                     ┌─────────────────────────────────┐
                     │         Python Layer (28%)       │
                     │  engine.py · LocalClient · agg   │
                     │  output · cmd registry · tests   │
                     ├─────────────────────────────────┤
                     │      Wire Protocol (Rust)        │
                     │  Tokio TCP · 40+ handlers        │
                     │  typed dispatch · cached imports  │
                     ├─────────────────────────────────┤
                     │       Core Engine (Rust)          │
                     │  query · aggregation · index      │
                     │  BSON · SCRAM · streaming · oplog │
                     ├─────────────────────────────────┤
                     │    WiredTiger FFI (Pure Rust)     │
                     │  RAII wrappers · dlopen           │
                     └─────────────────────────────────┘
```

**What's already done:**
- Zero `py.import()` on any request path (30 `PyOnceLock`-cached modules (free-threading-safe))
- Typed handler dispatch (`&Bound<'_, ConnectionContext>`, not `PyAny`)
- Direct Rust borrow for `CursorRegistry`, `RustLocalCollection`, `RustIndexManager`
- Pure-Rust wire compression (Snappy/Zstd/Zlib via native crates)
- ~50 Python method dispatches eliminated from the CRUD hot path
- **P1:** `cmd_find` does sort/skip/limit/projection in Rust (no Python `Cursor` on the wire path); `cmd_aggregate` calls `aggregate_pipeline` directly; `agg_cursor_cls` removed from `CachedImports`.
- **P2:** Oplog uses typed `RustWtSession` / `RustWtCursor` methods (all `call_method` sites replaced); `OplogHub` listeners use `Py<ChangeStream>`.
- **P3:** Admin/handshake use typed WiredTiger borrows, `CachedSystemInfo` / `cached_pid()`, `BTreeSet` for `listDatabases`; large drop in admin/handshake interop sites.
- **P4:** `cached_attr!` / `cached_nested_attr!` in `cached_modules.rs`; hot BSON/expression attrs cached; `wire_codec.rs` and `query_expressions.rs` updated.
- **P5:** `bson_helpers::shallow_copy_dict` replaces `doc.call_method0("copy")` for update before-images in `local_collection.rs`.
- **P7:** `schema.rs` implements `$jsonSchema` validation in Rust; `ValidationError` is a Rust-defined exception; `smongo_schema` cached module removed.

**What's left:**

```
 Interop tax remaining:
 ┌─────────────────────────────────────────┬───────┐
 │ .call_method() calls in Rust            │  ~120 │
 │ .cast::<PyDict/PyList>() calls          │  ~250 │
 │ .getattr() calls                        │   ~60 │
 │ py.import() on hot path                 │    0  │
 │ ctx.getattr() on hot path               │    0  │
 └─────────────────────────────────────────┴───────┘
```

---

## Priority 1: Eliminate the Python Cursor from find/aggregate

**Status: COMPLETE** — `cmd_find` in `crud.rs` applies sort/skip/limit/projection in Rust; `cmd_aggregate` in `aggregate.rs` calls `aggregate_pipeline` directly; `agg_cursor_cls` removed from `CachedImports`.

**Impact: HIGH -- affects every read query**

The biggest remaining interop seam. Every `find` command creates a Python `Cursor` object and chains 4-6 Python method calls:

```
docs → Python Cursor() → .sort() → .skip() → .limit() → .projection() → .to_list()
                 ↓                                                          ↓
        Python object alloc                                    back to Rust for CursorRegistry
```

**What to do:**

1. Implement sort/skip/limit/projection as pure Rust operations on `Bound<'_, PyList>`. Sort uses `list.sort()` with a Rust-built key function. Skip/limit are index slicing. Projection uses the existing `apply_projection_single`.
2. In `cmd_find`, replace the Python Cursor chain with: `find_streaming → sort → skip → limit → project → normalize → CursorRegistry::create()` -- all Rust.
3. In `cmd_aggregate`, call `aggregate_pipeline` directly instead of through `cursor.call_method1("aggregate", ...)`.
4. Keep the Python `Cursor` class for the user-facing API (`Collection.find()` returns a `Cursor`), but remove it from the wire command dispatch path.

**Estimated savings:** ~8 Python dispatches per `find`, ~3 per `aggregate`. At ~600ns per dispatch, that's ~5μs per read command.

**Files:** `rust/src/wire_commands/crud.rs` (lines 82-134), `rust/src/wire_commands/aggregate.rs` (lines 126-134).

---

## Priority 2: Direct-borrow oplog WiredTiger operations

**Status: COMPLETE** — `pub(crate)` typed methods on `RustWtSession` and `RustWtCursor` (`open_cursor_typed`, `create_typed`, `checkpoint_typed`, `close_typed`, `set_key_str`, `get_key_str`, `get_value_string`, `set_value_string`, `next_rc`, `insert_typed`, `remove_typed`, `close_typed`, `set_item_str`, etc.); all 47 `call_method` sites in `oplog.rs` replaced; `OplogHub` listeners are `Py<ChangeStream>`.

**Impact: HIGH -- affects every write (insert/update/delete)**

`oplog.rs` has **47 `.call_method()` calls** -- the densest interop site in the codebase. Every oplog read/write goes through Python method dispatch on `RustWtSession` and `RustWtCursor`, even though both are Rust `#[pyclass]` types we own:

```rust
// Current: Python dispatch on a Rust type
let cursor = session_bound.call_method1("open_cursor", (uri, py.None(), py.None()))?;
cursor.call_method0("next")?;
cursor.call_method0("get_key")?;
cursor.call_method0("close")?;
```

**What to do:**

1. Add `pub(crate)` methods to `RustWtSession` and `RustWtCursor` that expose the same operations without PyO3 dispatch (same pattern used for `CursorRegistry`).
2. In `oplog.rs`, replace `session_bound.call_method1(...)` with `session_ref.borrow().open_cursor_typed(...)` etc.
3. This is mechanical -- the same pattern was applied to `CursorRegistry` and `RustLocalCollection` in the Python dispatch elimination phase.

**Estimated savings:** ~47 Python dispatches eliminated per write-heavy cycle. At ~600ns each, ~28μs per oplog flush.

**Files:** `rust/src/oplog.rs` (~47 call sites), `rust/src/wt_bridge.rs` (add `pub(crate)` surface).

---

## Priority 3: Direct-borrow admin WiredTiger operations

**Status: COMPLETE** — `CachedSystemInfo` (system, release, machine, node, num_cores, page_size, total_memory_mb) and `cached_pid()`; typed WT in `cmd_list_databases`, `cmd_server_status`, `cmd_fsync`, user persist/delete; `list_collection_names` via Rust `pub(crate)` calls; `cmd_build_info` / `cmd_host_info` use the cache; `BTreeSet` instead of Python `set` in `listDatabases`; admin/handshake interop counts reduced substantially (e.g. admin ~116→~67 sites, handshake ~26→~14).

**Impact: MEDIUM -- affects `serverStatus`, `hostInfo`, diagnostic commands**

`admin.rs` has **98 `.call_method()` calls**, the single highest count. Most are for WiredTiger statistics collection (`serverStatus`), platform info gathering (`hostInfo`), and user management (`createUser`, `updateUser`). While these are "cold" commands, `serverStatus` is polled by monitoring tools every few seconds.

**What to do:**

1. Extract WiredTiger statistics collection into a typed Rust function that takes `&RustWtSession` directly.
2. Replace Python method calls on platform/resource modules with cached attribute lookups where possible.
3. For user management, the WiredTiger cursor operations follow the same pattern as oplog -- direct borrow.

**Files:** `rust/src/wire_commands/admin.rs` (~98 call sites), `rust/src/wire_commands/handshake.rs` (~23 call sites).

---

## Priority 4: Cache frequently-accessed Python class attributes

**Status: COMPLETE** — `cached_attr!` and `cached_nested_attr!` in `cached_modules.rs`; cached `bson.ObjectId`, `bson.Decimal128`, `bson.Regex`, `builtins.int` / `float` / `round`, `datetime.datetime`, `datetime.timezone.utc`; `wire_codec.rs` and `query_expressions.rs` consume cached refs.

**Impact: MEDIUM -- affects expression evaluation and BSON codec**

128 `.getattr()` calls remain across the codebase. The highest-density sites:

| File | `.getattr()` calls | Hot path? |
|------|:-:|:-:|
| `admin.rs` | 24 | No (diagnostic) |
| `crud.rs` | 16 | **Yes** (result extraction) |
| `query_expressions.rs` | 13 | **Yes** (expression eval) |
| `aggregation.rs` | 10 | Yes (pipeline stages) |
| `wire_server.rs` | 10 | No (startup) |
| `wire_codec.rs` | 8 | **Yes** (BSON normalization) |

**What to do:**

1. Cache `datetime.datetime`, `bson.ObjectId`, `bson.Decimal128`, `bson.Regex` as `Py<PyAny>` in `CachedImports` (they're looked up on every BSON normalization).
2. For `UpdateResult.modified_count` / `DeleteResult.deleted_count` extraction in `crud.rs`, define typed Rust accessors instead of going through `getattr("modified_count")`.
3. For `query_expressions.rs`, cache the handful of datetime/operator module attributes that are used repeatedly in expression evaluation.

**Files:** `rust/src/wire_context.rs` (extend `CachedImports`), `rust/src/wire_codec.rs`, `rust/src/query_expressions.rs`.

---

## Priority 5: Rust-native document copy for update before-images

**Status: COMPLETE** — `bson_helpers::shallow_copy_dict` replaces `doc.call_method0("copy")` at five sites in `local_collection.rs`.

**Impact: MEDIUM for write-heavy workloads**

Every update/replace operation copies the full document via Python's `dict.copy()`:

```rust
let before: Py<PyDict> = doc.call_method0("copy")?.extract()?;
```

This appeared at five sites in `local_collection.rs`. For large documents (hundreds of fields, nested structures), this is expensive.

**What to do:**

1. Implement a Rust-native dict shallow copy that iterates keys and inserts into a new `PyDict` directly -- skips Python method dispatch.
2. Consider a copy-on-write strategy: skip the copy when no oplog listeners or change streams are active.

**Files:** `rust/src/local_collection.rs` (lines 752, 843, 895, 1560).

---

## Priority 6: Reduce remaining cast density in query engine

**Status: COMPLETE** — `ExprArg` enum pre-classifies arg type once in `eval_expr_op` (replaces ~40 per-operator casts); `stage_parts` helper centralizes pipeline stage dict extraction in `aggregate_pipeline` and `optimize_pipeline`; `get_path_as_list`/`get_path_coerce_array` consolidate repeated get+cast patterns in `query_update.rs`; `as_dict_list` combinator replaces `$or`/`$and`/`$nor` boilerplate in `query_compiler.rs`; sub-dispatchers (`eval_convert`, `eval_zip`, `eval_regex_match`, `eval_regex_find`, `str_trim`, `str_replace`, `cmp_op`) tightened to accept typed args.

**Impact: LOW-MEDIUM -- diminishing returns**

Cast count reduced from ~311 to ~260 (~16%). Distribution after P6:

| File | Before | After | Reduction |
|------|:-:|:-:|---|
| `query_expressions.rs` | 71 | 31 | -40 (ExprArg enum + typed sub-dispatchers) |
| `aggregation.rs` | 51 | 34 | -17 (stage_parts + pre-cast) |
| `query_update.rs` | 29 | 24 | -5 (path helpers) |
| `query_compiler.rs` | 22 | 17 | -5 (as_dict_list combinator) |
| `crud.rs` | 19 | 18 | 0 (already centralized) |
| Everything else | 119 | 136 | +17 (new helpers counted here) |

The remaining ~260 casts are **irreducible** -- genuine runtime type discrimination on schema-free MongoDB documents. Further reduction requires moving to typed Rust-native document representations (Stretch Goal territory).

**Techniques applied:**

1. **Pre-classification enums:** `ExprArg` classifies `arg` as `Dict|List|Scalar` once at the top of `eval_expr_op`; operator arms pattern-match instead of re-casting.
2. **Centralized stage extraction:** `stage_parts` extracts `(op, dict, spec)` from a pipeline stage, replacing repeated `cast::<PyDict>()` in `aggregate_pipeline` and `optimize_pipeline`.
3. **Typed sub-dispatcher signatures:** Functions that always receive a dict (`eval_convert`, `str_trim`, `str_replace`, `eval_zip`, `eval_regex_match`, `eval_regex_find`) now accept `&Bound<'_, PyDict>` directly; callers extract the typed value from `ExprArg`.
4. **Path-to-list helpers:** `get_path_as_list` and `get_path_coerce_array` consolidate the repeated `get_value + into_bound + cast::<PyList>` pattern across `$pop`, `$push`, `$addToSet`, `$pull`, positional operators.
5. **Combinator helper:** `as_dict_list` encapsulates the `cast::<PyList>` + per-element `cast::<PyDict>` pattern used by `$or`/`$and`/`$nor`.

---

## Priority 7: Rust-native schema validation

**Status: COMPLETE** — `schema.rs` implements the full `$jsonSchema` validation engine in Rust. `validate_document` operates directly on `&Bound<'_, PyDict>` with zero Python dispatch. `ValidationError` is a Rust-defined PyO3 exception (`create_exception!`). The `smongo_schema` cached module import and `CachedImports.validation_err` field have been removed. `smongo/schema.py` re-exports the Rust symbols for backward compatibility. Regex pattern matching reuses `safe_regex` helpers from `query_compiler.rs`.

**Impact: LOW unless schema validation is heavily used, then HIGH**

Every insert/update on a validated collection previously crossed into Python:

```rust
crate::cached_modules::smongo_schema(py)?
    .call_method1("validate_document", (doc, v.bind(py)))?;
```

**What was done:**

1. Implemented `rust/src/schema.rs` with the full validation engine: `required`, `properties`, `type`/`bsonType`, `minimum`/`maximum`/`exclusiveMinimum`/`exclusiveMaximum`, `minLength`/`maxLength`, `pattern`, `enum`, `minItems`/`maxItems`/`uniqueItems`, `items`, `additionalProperties`, `minProperties`/`maxProperties`, and nested object validation with depth limit 100.
2. `ValidationError` defined as a Rust PyO3 exception, exported to Python.
3. `local_collection.rs` calls `crate::schema::validate_document` directly -- no Python module import, no `call_method`.
4. `crud.rs` uses `py.get_type::<crate::schema::ValidationError>()` instead of fetching from a Python module.
5. `smongo/schema.py` delegates to `_smongo_core.validate_document` while preserving the public API.

**Files:** `rust/src/schema.rs` (new), `rust/src/local_collection.rs`, `rust/src/wire_commands/crud.rs`, `smongo/schema.py`.

---

## ~~Priority 8: BSON boundary normalization in Rust~~ ✅ COMPLETE

**Impact: HIGH in throughput, LARGE in scope**

~~Every CRUD command normalizes documents at the wire boundary (`normalize_inbound` / `normalize_outbound`). This iterates every key-value pair in every document, converting between wire BSON types and engine types. For bulk operations on large documents, this dominates latency.~~

**Done:**

A single-pass raw BSON byte codec (`rust/src/raw_bson.rs`) now handles decode and encode directly between wire bytes and engine-ready `PyDict`s. The intermediate `bson::Document` allocation and the redundant `normalize_inbound` / `normalize_outbound` walks are eliminated on the wire path.

- **Decode:** `raw_decode_document` parses BSON bytes inline, producing `smongo.ObjectId`, Python `datetime`, `float` for Decimal128, and regex dicts directly.
- **Encode:** `raw_encode_document` serializes `PyDict` to BSON bytes inline, handling engine ObjectId, `_id` hex promotion, and all standard Python types.
- `ObjectId::from_raw()` constructs from 12-byte array directly, avoiding hex encode/decode overhead.
- All CRUD and aggregate wire handlers updated; `to_bson`/`from_bson` pyfunctions delegate to raw codec.

**Files:** `rust/src/raw_bson.rs` (new), `rust/src/wire_msg.rs`, `rust/src/bson_helpers.rs`, `rust/src/wire_commands/crud.rs`, `rust/src/wire_commands/aggregate.rs`.

---

## Stretch Goal A: Free-threaded Python (GIL elimination)

**Status: COMPLETE** — Module declares `gil_used = false`; `OnceLock` caches migrated to `PyOnceLock`; all `unsafe impl Send/Sync` audited; `Python::assume_attached()` eliminated; CI runs Python 3.13t with `PYTHON_GIL=0`.

**Impact: TRANSFORMATIVE for concurrent workloads**

Python 3.13+ offers free-threaded builds that remove the GIL. PyO3 0.28 defaults to declaring modules free-threading-compatible (`gil_used = false`). smongo's architecture (Rust owns compute, Python owns orchestration) is naturally well-positioned for this.

**What was done:**

1. **`cached_modules.rs`**: Migrated all 30+ `std::sync::OnceLock<Py<...>>` statics to `pyo3::sync::PyOnceLock`. This prevents deadlocks where a thread blocking on `OnceLock::get_or_init` holds a Python thread attachment while a GC stop-the-world event needs all threads detached. `SYSTEM_INFO` and `CACHED_PID` (non-Python types) use `OnceLockExt::get_or_init_py_attached`.
2. **`transaction.rs`**: `TXN_STATE` migrated to `PyOnceLock`.
3. **`lib.rs`**: Explicit `#[pymodule(gil_used = false)]` annotation signals intent to the interpreter and survives future PyO3 default changes.
4. **`objectid.rs`**: Replaced `unsafe { Python::assume_attached() }` with a proper `py: Python<'_>` parameter on the `#[getter]`.
5. **`unsafe impl Send/Sync`**: All 18 impls across 9 files audited and documented with `// SAFETY:` comments that reference the actual protection mechanism (collection-level `InlineRwLock`, `parking_lot::Mutex`, single-owner invariant, PyO3 borrow checking) rather than the GIL.
6. **CI**: Added a `test-free-threaded` job running Python 3.13t with `PYTHON_GIL=0` and `continue-on-error: true`.
7. **`pyproject.toml`**: Added Python 3.13 classifier to declare compatibility with the free-threaded build.

---

## Stretch Goal B: Multi-language bindings

**Impact: Opens entirely new ecosystems**

The Rust core can be exposed to any language via C FFI. The four-tier architecture was designed for this:

| Language | Binding Technology | Distribution |
|---|---|---|
| **Python** | PyO3 (already in place) | `pip install smongo` |
| **Node.js** | `napi-rs` | `npm install smongo` |
| **Go** | CGo wrapping C FFI | `go get smongo` |
| **Ruby** | `magnus` | `gem install smongo` |
| **Java/Kotlin** | JNI via `jni` crate | Maven Central |
| **C/C++** | Direct `libsmongo.h` | Static/shared library |

**What to do:**

1. Extract `wt_safe/` + core engine into a `libsmongo` crate with `#[no_mangle] extern "C"` entry points.
2. Define a stable C ABI: `smongo_open`, `smongo_insert`, `smongo_find`, `smongo_aggregate`, `smongo_close`.
3. Build language-specific wrappers using the appropriate binding technology.
4. The wire protocol server already makes smongo accessible from any language over TCP -- bindings add **in-process embedding** with zero network hop.

---

## Stretch Goal C: WebAssembly

**Impact: Enables browser-native MongoDB**

Compile the Rust core to WASM. Replace the WiredTiger storage backend with a browser-native alternative.

**Storage backend options:**

| Backend | Pros | Cons |
|---|---|---|
| **OPFS** | Synchronous file access in a Worker; closest to POSIX | Requires Web Worker; newer API |
| **IndexedDB** | Universal browser support | Async-only; slower |
| **In-memory** | Simplest; no persistence API | Data lost on refresh |

**Architecture:**

1. Abstract WiredTiger behind a `StorageBackend` trait.
2. Implement `WiredTigerBackend` (native), `OpfsBackend` (WASM + Worker), `IndexedDbBackend` (WASM + main thread), `InMemoryBackend` (testing).
3. Compile via `wasm-pack`. Publish as `smongo-wasm` on npm.
4. JavaScript developers get full MQL in the browser with optional Atlas sync via `fetch`.

---

## Improvement Roadmap Summary

| # | What | Files | Interop calls eliminated | Difficulty |
|---|---|---|:-:|:-:|
| **P1** | Bypass Python Cursor in find/aggregate | `crud.rs`, `aggregate.rs` | ~8 per read | Medium |
| **P2** | Direct-borrow oplog WT sessions | `oplog.rs`, `wt_bridge.rs` | ~47 per write | Medium |
| **P3** | Direct-borrow admin WT sessions | `admin.rs`, `handshake.rs` | ~30 per `serverStatus` | Medium |
| **P4** | Cache class attributes in `CachedImports` | `wire_context.rs`, `wire_codec.rs` | ~40 `.getattr()` | Low |
| **P5** | Rust-native dict copy for before-images | `local_collection.rs` | ~4 per update | Low |
| **P6** | ~~Reduce cast density in query engine~~ **DONE** | `query_expressions.rs`, `aggregation.rs`, `query_update.rs`, `query_compiler.rs` | ~51 casts eliminated | Low-Medium |
| **P7** | ~~Rust-native schema validation~~ **DONE** | `schema.rs`, `local_collection.rs` | 1 per validated write | Medium |
| **P8** | ~~BSON byte-level normalization~~ **DONE** | `raw_bson.rs`, `wire_msg.rs`, `bson_helpers.rs` | Eliminated (single-pass raw codec) | High |
| **SA** | ~~Free-threaded Python support~~ **DONE** | Crate-wide | Concurrency unlock | Medium |
| **SB** | Multi-language bindings | New crates | New ecosystems | High |
| **SC** | WebAssembly build | New crate + storage trait | Browser deployment | High |

---

## Benchmark Targets

P1-P8 are complete. Approximate interop profile before vs after that work:

```
                        Before P1-P5          After P1-P5          After P8
 .call_method() total:     ~328                  ~200                ~120  (-40%)
 .getattr() total:         ~128                   ~85                 ~60  (-29%)
 find dispatch:            ~287μs                ~260μs              improved
 insert dispatch:          ~91μs                 ~60μs               improved
 aggregate dispatch:       ~457μs                ~420μs              improved
```

P8 eliminates `normalize_inbound`/`normalize_outbound` calls and `bson::Document` intermediate allocation on every wire document. Bulk operations see the largest gains.

Current benchmarks (from `test_perf_interop.py`):

| Operation | Latency | Ops/sec |
|---|---:|---:|
| `findAndModify` | 64 μs | 15,600 |
| `insert + delete` | 91 μs | 11,000 |
| `find` + `limit(10)` | 287 μs | 3,490 |
| `find({})` (200 docs) | 361 μs | 2,770 |
| `count_documents` | 378 μs | 2,650 |
| `aggregate` (3 stages) | 457 μs | 2,190 |

---

## Interop Health Metrics

Track these across releases to ensure the boundary stays tight:

| Metric | Current (post P1-P8) | Target (P1-P5) | Target (P1-P8) |
|---|---:|---:|---:|
| `py.import()` on hot path | **0** | 0 | **0** |
| `ctx.getattr()` on hot path | **0** | 0 | **0** |
| `.call_method()` total | **~120** | ~200 | **~120** |
| `.cast::<PyDict/PyList>()` total | **~250** | ~311 | **~250** |
| `.getattr()` total | **~60** | ~85 | **~60** |
| Python Cursor in find dispatch | **No** | **No** | **No** |
| Oplog uses typed WT borrow | **Yes** | **Yes** | **Yes** |
| Admin uses typed WT borrow | **Yes** | **Yes** | **Yes** |
| Wire BSON uses raw byte codec | **Yes** | No | **Yes** |

---

## Design Principles (Unchanged)

1. **Rust owns compute, Python owns orchestration.** If it touches every request, it's in Rust. If it runs once at startup, it stays in Python.

2. **Type at the boundary, erase inside.** Handler signatures are typed (`ConnectionContext`, `PyDict`). Internal path traversal stays `PyAny` because documents are schema-free.

3. **Cache imports, not results.** Python module references are resolved once and stored in `Arc<CachedImports>` or `OnceLock<Py<PyModule>>`.

4. **No unsafe without proof.** The only `unsafe` blocks are in `wt_safe/` (C FFI) and `wiredtiger-sys` (dlopen).

5. **One cast, not thirty.** Push type narrowing to the earliest boundary and carry typed references through.

6. **Pure Rust where possible.** Wire compression, SCRAM crypto, index encoding, CRC32C, and BSON encoding use native Rust crates -- no Python bridging.

7. **Measure, then optimize.** Every interop change must be validated by `cargo test` (67/67), `pytest` (1,022+), and `test_perf_interop.py` benchmarks.

---

## The End State

When Priorities 1-8 and Stretch Goals A-C are complete:

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
         │   Zero interop tax on hot paths                   │
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

One query language. One document model. One sync protocol. Every platform.
