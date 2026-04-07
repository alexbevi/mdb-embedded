# The SQLite Path: Multi-Language smongo

> SQLite is multi-language because it is **one C library** with **one stable ABI**.
> Every language writes a thin binding to the same functions.
> The engine exists once — in C — with no host-language dependencies.
>
> smongo has a **Rust engine that doesn't know it's an engine**.
> The algorithms are there. The PyO3 coupling is what holds us back.
> This plan fixes that.

## Current State (Honest Assessment)

| Metric | Value |
|--------|-------|
| Total `.rs` files in `rust/src/` | 50 |
| Files that import `pyo3` | **46** |
| Files that are PyO3-free | **4** (`wt_safe/{mod,connection,session,cursor}.rs`) |
| Core data type everywhere | `Py<PyDict>`, `Bound<'_, PyDict>` |
| Crate output | `cdylib` (Python extension only) |
| Can compile without Python | **No** |

The engine logic — CRUD, query matching, aggregation, indexes, oplog, wire
protocol — is **implemented in Rust**. But it operates on **Python objects**
(`PyDict`, `PyList`, `PyAny`), returns `PyResult`, and callbacks into Python
via `cached_modules.rs`. There is no way to call any of it from C, Node, or
even plain Rust.

## Target Architecture

```
┌──────────────────────────────────────────────────────┐
│                  smongo-engine (rlib)                 │
│                                                      │
│  Pure Rust. No PyO3. bson::Document throughout.      │
│  Storage, Query, Aggregation, Indexes, Oplog.        │
│  WiredTiger via wiredtiger-sys.                      │
│                                                      │
│  pub fn open(path, config) -> Result<Engine>         │
│  pub fn insert(col, doc) -> Result<InsertResult>     │
│  pub fn find(col, filter) -> Result<Cursor>          │
│  pub fn aggregate(col, pipeline) -> Result<Cursor>   │
│  pub fn create_index(col, keys) -> Result<String>    │
│  ...                                                 │
├────────────┬───────────────┬──────────────┬──────────┤
│ C ABI      │  PyO3 shim    │  napi-rs     │  WASM*   │
│ (cdylib +  │  (cdylib)     │  (cdylib)    │          │
│  cbindgen) │               │              │          │
├────────────┼───────────────┼──────────────┼──────────┤
│ C/C++/Go/  │  Python       │  Node.js     │  Browser │
│ Swift/Java │  (smongo pkg) │  (npm pkg)   │          │
└────────────┴───────────────┴──────────────┴──────────┘
```

\* WASM is aspirational — WiredTiger's C dependency makes it non-trivial.

## The Phases

Each phase is scoped to be completable in **one focused session** with AI
assistance. Every phase leaves the project in a **working state** — the
existing Python path never breaks.

---

### Phase 1: Workspace + Foundation Crate

**Goal:** Create the `smongo-engine` crate and prove it compiles independently.

**What happens:**

1. Convert `rust/` to a **Cargo workspace** with three members:
   - `wiredtiger-sys` (existing, unchanged)
   - `smongo-engine` (new `rlib` — **no PyO3 dependency**)
   - `smongo-py` (renamed from current root — `cdylib`, keeps all PyO3 code)

2. Move `wt_safe/` into `smongo-engine` as `smongo-engine::wt` — these 4
   files (~480 lines) are already PyO3-free with pure-Rust types (`WtError`,
   `WtResult<T>`, `WtConnection`, `WtSession`, `WtCursor`).

3. Have `smongo-py` depend on `smongo-engine` and re-import `wt_safe` types
   from there (so `wt_bridge.rs` still works).

4. Add a minimal public API to `smongo-engine`:
   ```rust
   pub use wt::{WtConnection, WtSession, WtCursor, WtError, WtResult};
   ```

**Verify:**
- `cargo build` in workspace succeeds
- `cargo test -p smongo-engine` runs the existing `wt_safe` tests
- `maturin develop` still builds the Python extension
- `python -c "import smongo"` still works

**Files touched:** `Cargo.toml` (workspace root, new), `smongo-engine/Cargo.toml`
(new), `smongo-engine/src/lib.rs` (new), `smongo-engine/src/wt/` (moved from
`src/wt_safe/`), `smongo-py/Cargo.toml` (renamed, add dep).

**Risk:** Low. Only build system changes + moving already-clean files.

**Estimated scope:** ~200 lines of new code/config, mostly `Cargo.toml` edits.

---

### Phase 2: Pure-Rust Document Path Operations

**Goal:** Port `paths.rs` to work on `bson::Document` instead of `PyDict`.

**What happens:**

1. Create `smongo-engine::paths` module with these functions operating on
   `bson::Document` and `bson::Bson`:

   ```rust
   pub fn get_value(doc: &Document, key: &str) -> Option<&Bson>
   pub fn field_exists(doc: &Document, key: &str) -> bool
   pub fn set_value(doc: &mut Document, key: &str, value: Bson)
   pub fn unset_value(doc: &mut Document, key: &str)
   ```

2. The current `paths.rs` is ~130 lines of logic: dot-notation traversal
   with dict/list branching. Direct translation — `PyDict.get_item("x")`
   becomes `doc.get("x")`, `PyList.get_item(idx)` becomes
   `arr.get(idx)`.

3. Write Rust unit tests (no Python needed).

4. Keep `smongo-py::paths` (the PyO3 version) unchanged — Python still
   calls the old one.

**Verify:**
- `cargo test -p smongo-engine` — new path tests pass
- Python tests unchanged

**Files created:** `smongo-engine/src/paths.rs` (~150 lines)

**Risk:** Very low. Greenfield code with clear semantics.

---

### Phase 3: Pure-Rust Query Compiler

**Goal:** Port `query_compiler.rs` to evaluate predicates against `bson::Document`.

**What happens:**

1. Create `smongo-engine::query` module with:

   ```rust
   pub fn eval_query(doc: &Document, query: &Document) -> Result<bool>
   pub fn compile_query(query: Document) -> CompiledQuery
   ```

2. Port `eval_query` (~90 lines) and `eval_op` (~170 lines) — the
   operator implementations (`$gt`, `$lt`, `$in`, `$exists`, `$regex`,
   `$elemMatch`, `$all`, `$not`, `$and`, `$or`, `$nor`, `$text`,
   `$type`, `$mod`, `$bits*`).

3. Key type mapping:
   - `value.gt(cond_val)` → `bson_compare(a, b) == Ordering::Greater`
   - `value.is_instance_of::<PyInt>()` → `matches!(val, Bson::Int32(_) | Bson::Int64(_))`
   - `cond_val.contains(value)` → iterate `Bson::Array` and compare

4. Regex: already uses the `regex` crate for the happy path. The Python
   fallback (`re_mod.call_method1("compile", ...)`) becomes unnecessary —
   we use `regex::Regex` exclusively and return an error for
   PCRE-only patterns.

5. Port `query_expressions.rs` (`resolve_expr`) — the `$expr` aggregation
   expression evaluator for query context.

**Verify:**
- `cargo test -p smongo-engine` — query matching tests cover every operator
- Python tests unchanged

**Files created:** `smongo-engine/src/query/mod.rs`, `query/compiler.rs`,
`query/expressions.rs`, `query/update.rs` (stub) (~500 lines total)

**Risk:** Medium. BSON comparison semantics (cross-type ordering) need care.
MongoDB has a specific type comparison order
(`MinKey < Null < Number < String < Object < Array < Binary < ObjectId < ...`).
We should implement this from the start.

---

### Phase 4: Pure-Rust Update Operations

**Goal:** Port `query_update.rs` — the `$set`, `$unset`, `$inc`, `$push`, etc.
operators — to work on `bson::Document`.

**What happens:**

1. Create `smongo-engine::query::update` with:

   ```rust
   pub fn apply_update(doc: &mut Document, spec: &Document, array_filters: Option<&[Document]>) -> Result<()>
   ```

2. Port each update operator. The current `apply_update` handles:
   `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`,
   `$currentDate`, `$addToSet`, `$push`, `$pull`, `$pop`, `$bit`,
   `$setOnInsert`.

3. Uses `paths::set_value` / `paths::unset_value` from Phase 2.

4. Array filters (`$[<identifier>]` syntax) reuse `eval_query` from Phase 3.

**Verify:**
- Unit tests for every update operator
- Python tests unchanged

**Files created:** `smongo-engine/src/query/update.rs` (~400 lines)

**Risk:** Medium. Array filter evaluation and positional operators (`$`,
`$[]`, `$[<id>]`) have edge cases. Match MongoDB semantics carefully.

---

### Phase 5: Pure-Rust Collection + CRUD

**Goal:** Implement `insert_one`, `find`, `find_one`, `delete`, `update`
operating entirely in Rust with `bson::Document` in, `bson::Document` out.

**What happens:**

1. Create `smongo-engine::storage` module:

   ```rust
   pub struct Engine { conn: WtConnection }
   pub struct Database { /* ... */ }
   pub struct Collection { /* ... */ }

   impl Engine {
       pub fn open(path: &str, config: Option<&EngineConfig>) -> Result<Self>
       pub fn database(&self, name: &str) -> Database
       pub fn close(self) -> Result<()>
   }

   impl Collection {
       pub fn insert_one(&self, doc: Document) -> Result<InsertResult>
       pub fn insert_many(&self, docs: Vec<Document>) -> Result<InsertResult>
       pub fn find(&self, query: &Document) -> Result<Vec<Document>>
       pub fn find_one(&self, query: &Document) -> Result<Option<Document>>
       pub fn update(&self, query: &Document, update: &Document, opts: UpdateOpts) -> Result<UpdateResult>
       pub fn delete(&self, query: &Document, multi: bool) -> Result<DeleteResult>
       pub fn count(&self, query: Option<&Document>) -> Result<i64>
   }
   ```

2. BSON serialization: `bson::to_vec(&doc)` / `bson::from_slice(&bytes)` —
   the `bson` crate handles this natively. No custom codec needed.

3. WiredTiger access pattern (same as current code, minus Python):
   - Open cursor on `table:db_collection`
   - `cursor.set_key_str(&doc_id)` / `cursor.set_value_raw(&bson_bytes)`
   - Transaction wrapping with `begin_transaction` / `commit` / `rollback`

4. Query execution: delegate to Phase 3's `eval_query` for full scans.
   Primary key lookup extracted from `_id` field.

5. `_id` generation: implement `ObjectId::generate()` in pure Rust (the
   current `objectid.rs` already generates IDs — just needs PyO3 stripped).

6. WiredTiger library loading: support `WIREDTIGER_LIB` env var (already
   exists) and add `WIREDTIGER_LIB_DIR` for system-installed WT.
   `load_from_pip` stays in `smongo-py` only.

**Verify:**
- Integration test: `Engine::open` → `insert_one` → `find_one` → roundtrip
- Python path unchanged — `smongo-py` still uses its own `RustLocalCollection`

**Files created:** `smongo-engine/src/storage/{mod,engine,collection}.rs`,
`smongo-engine/src/objectid.rs` (~600 lines total)

**Risk:** Medium. WiredTiger session/cursor lifecycle management is the tricky
part. Borrow from the existing `wt_safe` patterns.

---

### Phase 6: Pure-Rust Index Manager

**Goal:** Port index creation, encoding, and query planning to pure Rust.

**What happens:**

1. Move `index_encoding.rs` to `smongo-engine::index::encoding` — the
   sortable-encode logic is already mostly PyO3-free in its core, just
   wrapped in `#[pyfunction]`.

2. Create `smongo-engine::index::manager`:

   ```rust
   pub struct IndexManager { /* ... */ }

   impl IndexManager {
       pub fn create_index(&mut self, keys: &Document, opts: IndexOpts) -> Result<String>
       pub fn drop_index(&mut self, name: &str) -> Result<()>
       pub fn add_doc(&self, doc: &Document) -> Result<()>
       pub fn remove_doc(&self, doc: &Document) -> Result<()>
       pub fn update_doc(&self, old: &Document, new: &Document) -> Result<()>
       pub fn list_indexes(&self) -> Vec<IndexDef>
   }
   ```

3. Create `smongo-engine::index::planner`:

   ```rust
   pub struct QueryPlanner { /* ... */ }

   impl QueryPlanner {
       pub fn plan(&self, query: &Document) -> QueryPlan
       pub fn execute(&self, plan: &QueryPlan) -> Result<Vec<String>>
   }
   ```

4. Index metadata stored in `table:__idxmeta_db_coll` (same layout as
   current implementation).

**Verify:**
- Integration test: create index → insert docs → query hits index
- Python unchanged

**Files created:** `smongo-engine/src/index/{mod,encoding,manager,planner}.rs`
(~800 lines)

**Risk:** Medium-high. Index encoding must be **byte-identical** to the
current implementation or existing databases won't be readable. Test
against known encoded values.

---

### Phase 7: Pure-Rust Oplog

**Goal:** Port `OplogWriter`, `OplogReader`, and basic `ChangeStream` to pure Rust.

**What happens:**

1. Create `smongo-engine::oplog`:

   ```rust
   pub struct OplogWriter { /* ... */ }
   pub struct OplogReader { /* ... */ }
   pub struct ChangeStream { /* ... */ }

   impl OplogWriter {
       pub fn log(&self, op: &str, doc_id: &Bson, payload: Option<&Document>, ...) -> Result<String>
   }

   impl OplogReader {
       pub fn read_all(&self) -> Result<Vec<OplogEntry>>
       pub fn read_from(&self, checkpoint: &str) -> Result<Vec<OplogEntry>>
   }
   ```

2. Oplog entry format: currently JSON in WiredTiger. Keep compatible format
   for now (`serde_json`), with a future option for BSON (per
   `UPNEXT4SYNC.md` roadmap).

3. ChangeStream: in-process pub/sub using `tokio::sync::broadcast` or
   `std::sync::mpsc`. No Python callback dependency.

**Verify:**
- Integration test: insert → oplog entry appears → read_from checkpoint
- Python unchanged

**Files created:** `smongo-engine/src/oplog.rs` (~300 lines)

**Risk:** Low-medium. Oplog format compatibility is important for sync.

**Dependency:** Adds `serde`, `serde_json` to `smongo-engine`.

---

### Phase 8: Pure-Rust Schema Validation

**Goal:** Port `schema.rs` JSON Schema validation to pure Rust.

**What happens:**

1. Create `smongo-engine::schema`:

   ```rust
   pub fn validate(doc: &Document, schema: &Document) -> Result<(), ValidationError>
   ```

2. The current implementation validates `type`, `required`, `properties`,
   `enum`, `minimum`/`maximum`, `minLength`/`maxLength`, `pattern`,
   `additionalProperties`. Straightforward translation.

**Verify:**
- Unit tests for each validation rule
- Python unchanged

**Files created:** `smongo-engine/src/schema.rs` (~200 lines)

**Risk:** Low.

---

### Phase 9: Pure-Rust Aggregation Pipeline (Core Stages)

**Goal:** Port the most-used aggregation stages to pure Rust.

This is the **largest phase** and may span **2 sessions** if needed. Split
at the `---` line below if it feels too big.

**Session A — Stateless stages:**

| Stage | Complexity |
|-------|-----------|
| `$match` | Trivial (delegates to `query::eval_query`) |
| `$project` / `$addFields` / `$unset` | Medium (field selection + expressions) |
| `$sort` | Low (BSON comparison) |
| `$limit` / `$skip` | Trivial |
| `$count` | Trivial |
| `$replaceRoot` | Low |
| `$sample` | Low |
| `$redact` | Medium |

---

**Session B — Stateful/join stages:**

| Stage | Complexity |
|-------|-----------|
| `$group` | High (accumulators: `$sum`, `$avg`, `$min`, `$max`, `$push`, `$first`, `$last`, `$addToSet`) |
| `$unwind` | Medium |
| `$lookup` (basic + pipeline) | High |
| `$graphLookup` | High |
| `$facet` | Medium (runs sub-pipelines) |
| `$bucket` / `$bucketAuto` | Medium |
| `$setWindowFields` | High |
| `$sortByCount` | Low (sugar for `$group` + `$sort`) |

**API:**

```rust
pub fn aggregate(
    collection: &Collection,
    pipeline: &[Document],
) -> Result<Vec<Document>>
```

**Verify:**
- Unit tests per stage
- Integration tests with real WiredTiger data
- Python unchanged

**Files created:** `smongo-engine/src/aggregation/{mod,stages,joins,expressions}.rs`
(~1500 lines total across both sessions)

**Risk:** High for Session B. `$group` accumulators, `$lookup` with
sub-pipeline, and `$setWindowFields` are complex. Start with Session A
and ship it; Session B can come later.

---

### Phase 10: Rewire smongo-py as a Thin Binding

**Goal:** Make `smongo-py` delegate to `smongo-engine` for all core operations.

**What happens:**

1. `RustLocalCollection` methods (`insert_one`, `find`, `update`, `delete`,
   etc.) now call `smongo_engine::storage::Collection` internally.

2. PyO3 boundary code shrinks to:
   - `PyDict` → `bson::Document` (inbound conversion)
   - `bson::Document` → `PyDict` (outbound conversion)
   - Error mapping: `smongo_engine::Error` → `PyErr`

3. `CompiledQuery` wraps `smongo_engine::query::CompiledQuery`.

4. `cached_modules.rs` usage drops dramatically — only needed for
   Python-specific features (datetime class interop, `snappy` check).

5. All existing Python tests must pass identically.

**Verify:**
- **Full Python test suite passes**
- `maturin develop && pytest` green
- Benchmark: should be same speed or faster (less Python↔Rust crossing)

**Files changed:** Most of `smongo-py/src/*.rs` — but changes are
**narrowing**, not expanding. Each file gets simpler.

**Risk:** High. This is the integration phase. Type mismatches and subtle
behavioral differences will surface here. Budget extra time for debugging.

---

### Phase 11: Stable C ABI

**Goal:** Expose `smongo-engine` functionality through `extern "C"` functions.

**What happens:**

1. Create `smongo-engine/src/ffi.rs` (or a separate `smongo-c` crate):

   ```c
   // smongo.h (generated by cbindgen)
   typedef struct SmongoEngine SmongoEngine;
   typedef struct SmongoCollection SmongoCollection;
   typedef struct SmongoCursor SmongoCursor;

   int smongo_open(const char *path, const char *config, SmongoEngine **out);
   int smongo_close(SmongoEngine *engine);

   int smongo_collection(SmongoEngine *engine, const char *db,
                         const char *name, SmongoCollection **out);

   int smongo_insert(SmongoCollection *col,
                     const uint8_t *bson_doc, size_t len,
                     uint8_t **result, size_t *result_len);

   int smongo_find(SmongoCollection *col,
                   const uint8_t *bson_filter, size_t filter_len,
                   SmongoCursor **cursor);

   int smongo_cursor_next(SmongoCursor *cursor,
                          const uint8_t **doc, size_t *doc_len);

   void smongo_cursor_close(SmongoCursor *cursor);
   void smongo_free(uint8_t *ptr, size_t len);
   ```

2. Data format at the ABI boundary: **raw BSON bytes**. Every language
   already has a BSON library. This matches how MongoDB drivers work over
   the wire protocol — and avoids inventing a new serialization format.

3. Error handling: return codes + `smongo_last_error()` for message string
   (same pattern as SQLite's `sqlite3_errmsg()`).

4. Add `cbindgen` to generate `smongo.h` automatically.

5. Build produces `libsmongo.{so,dylib,dll}` + `smongo.h`.

**Verify:**
- Write a C test program that opens a database, inserts a document, and
  reads it back. Compile with `gcc`, link against `libsmongo`.
- Valgrind clean (no leaks from the Rust side).

**Files created:** `smongo-engine/src/ffi.rs` (~300 lines), `smongo-engine/cbindgen.toml`,
test C program

**Risk:** Medium. Lifetime management across FFI boundary needs careful
design. Opaque handles + explicit close/free is the proven pattern.

---

### Phase 12: Node.js Binding

**Goal:** Ship an npm package that provides an embedded MongoDB-compatible engine.

**What happens:**

1. Create `smongo-node/` directory with `napi-rs` bindings:

   ```typescript
   import { MongoClient } from '@smongo/embedded';

   const client = new MongoClient('local://./my_data');
   const db = client.db('mydb');
   const col = db.collection('users');

   await col.insertOne({ name: 'Alice', age: 30 });
   const user = await col.findOne({ name: 'Alice' });
   ```

2. `napi-rs` calls directly into `smongo-engine` (Rust → Rust, no C ABI
   overhead needed). BSON conversion uses the `bson` crate's
   `serde` integration with `napi-rs`.

3. Pre-built binaries for `{linux,darwin,win32}-{x64,arm64}` via
   `napi-rs`'s GitHub Actions template.

4. WiredTiger library: bundle as a native dependency or require system
   install. Long-term, consider static linking.

**Verify:**
- `npm test` — insert, find, update, delete roundtrip
- `npm run bench` — basic throughput numbers

**Files created:** `smongo-node/` directory (~500 lines of TS/Rust binding)

**Risk:** Medium. WiredTiger distribution for Node is the main packaging
challenge. The Rust binding itself is straightforward.

---

## Phase Dependency Graph

```
Phase 1 ─── Workspace + Foundation
  │
  ├── Phase 2 ─── Path Operations
  │     │
  │     ├── Phase 3 ─── Query Compiler
  │     │     │
  │     │     ├── Phase 4 ─── Update Operations
  │     │     │
  │     │     └── Phase 9A ── Aggregation (stateless)
  │     │           │
  │     │           └── Phase 9B ── Aggregation (stateful)
  │     │
  │     └── Phase 8 ─── Schema Validation
  │
  ├── Phase 5 ─── Collection + CRUD
  │     │
  │     ├── Phase 6 ─── Index Manager
  │     │
  │     └── Phase 7 ─── Oplog
  │
  ├── Phase 10 ── Rewire smongo-py ← (needs 2-9)
  │
  ├── Phase 11 ── C ABI ← (needs 1-9)
  │
  └── Phase 12 ── Node.js ← (needs 11 or directly 1-9)
```

Phases 2-9 can be worked on in **any order** within their dependency chains.
Phases 2-4 (query/document layer) and Phases 5-7 (storage layer) are
**independent tracks** that can progress in parallel.

## What NOT to Port (Keep in Host Language)

These features are inherently language-specific and should remain in the
binding layer, not in `smongo-engine`:

- **SyncManager** — uses pymongo/MongoDB Node driver for remote sync. Each
  language gets its own sync implementation using its own MongoDB driver.
- **TTL Reaper** — timer thread / event loop integration is language-specific.
- **Wire Server TLS** — keep using `tokio-rustls` in the engine, but
  certificate loading paths may differ per platform.
- **Web Dashboard** — Python Flask app stays Python. Node equivalent would
  be Express/Fastify.

## Success Criteria

The SQLite path is **complete** when:

1. `cargo build -p smongo-engine` succeeds with **zero** PyO3 imports
2. A C program can link `libsmongo` and perform CRUD without Python installed
3. `npm test` passes for the Node.js binding
4. The Python `smongo` package delegates to `smongo-engine` and all existing
   tests pass
5. The same WiredTiger database directory is readable from Python, Node,
   and C simultaneously (with proper locking)

## Estimated Timeline

| Phase | Sessions | Cumulative |
|-------|----------|------------|
| 1. Workspace | 1 | 1 |
| 2. Paths | 1 | 2 |
| 3. Query Compiler | 1 | 3 |
| 4. Update Ops | 1 | 4 |
| 5. Collection + CRUD | 1 | 5 |
| 6. Index Manager | 1 | 6 |
| 7. Oplog | 1 | 7 |
| 8. Schema | 1 | 8 |
| 9. Aggregation (A+B) | 2 | 10 |
| 10. Rewire smongo-py | 1-2 | 12 |
| 11. C ABI | 1 | 13 |
| 12. Node.js | 1-2 | 15 |

**~15 sessions** from current state to a working multi-language engine.
Phases 1-9 produce a standalone Rust engine.
Phase 10 proves backward compatibility.
Phases 11-12 unlock the first non-Python consumer.

## Guiding Principles

1. **Never break Python.** Every phase leaves `maturin develop && pytest`
   green. The pure-Rust engine is built **alongside** the PyO3 code, not
   by ripping PyO3 out.

2. **`bson::Document` is the lingua franca.** It is the internal document
   type for smongo-engine. At every language boundary, documents cross as
   **raw BSON bytes** — the same format MongoDB uses on the wire.

3. **Delegate, don't duplicate.** Once a pure-Rust implementation exists
   in `smongo-engine`, `smongo-py` should call it — not maintain a
   parallel PyO3 implementation.

4. **Test at the engine level.** Every `smongo-engine` feature gets Rust
   integration tests that run without Python. This is how we prove the
   engine stands alone.

5. **Same database, any language.** A WiredTiger directory written by
   Python must be readable by Node and vice versa. BSON encoding and
   WiredTiger table layouts are the contract.
