# Changelog

Notable changes to **smongo** are recorded here. Earlier history lives in git.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0] — 2026-04-11

### Added

- **`#[derive(PythonImports)]` proc macro** (`smongo-macros` crate) — generates
  `CachedImports::from_python` from declarative struct annotations. Type-driven
  resolution: `Py<PyAny>` → `.unbind()`, `Py<T>` → `.cast::<T>()`, primitives
  → `.extract()`. Zero hand-written `getattr` calls remain.
- **Rust-native disk spill** — `disk_spill.rs` implements external merge sort
  for `$sort` and hash-partitioned grouping for `$group` when `allow_disk_use`
  is enabled and intermediate data exceeds `memory_limit_bytes`.

### Removed

- **Deleted `smongo/aggregation/output.py`** — `facet_stage` imported directly
  from Rust; `$out`/`$merge` implementations moved into `cursor.py`.
- **Dead Rust code** — removed unused `plan_simple_query`,
  `evaluate_index_for_query`, `CoveringIndexStream` enum variant, and
  `ScramConversation::client_nonce` field.
- **Redundant deps** — removed unused `proc-macro2` from `smongo-macros`,
  deduplicated `tempfile` in `smongo-engine`.
- **Dead Python fallback** — `client.py` `allowDiskUse` branch no longer
  falls back to `Cursor.aggregate()`; calls Rust engine unconditionally.

### Fixed

- `server_info()` version now reads from `__version__` instead of hardcoded
  `"1.0.3"`.
- Stale `output.py` references removed from README, ARCHITECTURE, CONTRIBUTING,
  and RUST-PY docs.
- `TestABIDrift` CI lint updated to parse `#[py(attr = "...")]` macro
  annotations instead of `manifest.getattr("...")` calls.

## [1.1.9] — 2026-04-11

### Fixed — Wire protocol fidelity (Compass compatibility)

- **Added `$indexStats` aggregation stage** — both Python and Rust wire handlers
  now return index name, key, and stub usage statistics. Compass no longer errors
  when inspecting collection indexes.
- **Added `$listSearchIndexes` stage** to the Python aggregate handler — returns
  an empty cursor (no Atlas Search in embedded mode) instead of "Unknown stage".
- **`$collStats` pipeline continuation** — the Rust aggregate handler now runs
  remaining pipeline stages (e.g. `$project`, `$group`) after producing the
  `$collStats` document. Previously it returned immediately, breaking Compass's
  `$collStats → $project {$objectToArray} → $unwind → $group` pipeline.
- **Added `$$ROOT` / `$$CURRENT` / `$$REMOVE`** system variable support in the
  Rust engine expression evaluator.
- **Added `$objectToArray`** and **`$arrayToObject`** expression operators to the
  Rust engine — previously these returned `Bson::Null` silently, breaking
  `$collStats` index-size pipelines.
- **Added `$bsonSize` expression** to the Rust engine.
- **Added `$concatArrays`, `$reduce`, `$slice`, `$reverseArray`, `$isArray`,
  `$sum` (expression form)** to the Rust engine expression evaluator.
- **Fixed Clippy `collapsible_match` warning** in `$arrayToObject`.

## [1.1.8] — 2026-04-11

### Fixed — Compass document display

- **MongoDB Compass now displays all document fields.** Compass sends `find`
  projections with `$$ROOT` and `$bsonSize` expressions; the old projection
  engine treated these as literal field names and returned empty documents.
- **Consolidated projection engine into a single Rust implementation**
  (`apply_projection` in `query_expressions.rs`). Both the Python wire handler
  and the Rust wire handler now call the same code — no more diverging
  include/exclude logic that could drift.
- **Added `$bsonSize` expression operator** — evaluates to the BSON byte length
  of a document. Used by Compass to show document sizes in the UI.
- **Fixed `_id: 0` handling** — the old projection forced `_id` into the
  inclusion set even when explicitly excluded.
- **Removed unused imports** (`Document`, `Collection`) in test files.
- **Cleaned up deprecated `downcast` calls** — replaced with `cast` per pyo3 API.

## [1.1.7] — 2026-04-11

### Changed — Cleanup & docs

- **Removed dead code** (`ObjectId::from_raw`) instead of suppressing with
  `#[allow(dead_code)]`.
- **Added `bson.Timestamp` handler** to `py_to_bson` — previously fell through
  to the `str()` fallback, causing warnings on every wire response.
- **Added `bson.Int64` handler** to `py_to_bson` — preserves BSON int64 instead
  of demoting to int32 when the value fits in 32 bits.
- **Updated docs** (`README.md`, `ARCHITECTURE.md`, `RUST-PY.md`,
  `WIRE-PROTOCOL.md`) to reflect the `bson` crate-backed codec architecture.
- **Cleaned up `local_data/`** and added it (plus `*.redb`) to `.gitignore`.

## [1.1.6] — 2026-04-10

### Changed — BSON codec rewrite (library-backed)

- **Replaced custom byte-level BSON encoder and decoder** with the `bson` Rust
  crate (maintained by the MongoDB team).  Both `raw_encode_document` and
  `raw_decode_document` now delegate to `bson::to_vec` / `bson::from_slice`
  via the `pydict_to_doc` / `doc_to_pydict` conversion layer.  This guarantees
  spec-compliant output that is byte-compatible with every MongoDB driver and
  tool (Compass, mongosh, Node.js driver, PyMongo).
- **Removed ~400 lines of hand-rolled BSON encoder code** (`EncodeContext`,
  `encode_doc_into`, `encode_array_into`, `encode_element`, write helpers,
  array index cache).  Zero custom serialization logic remains.
- **Removed ~300 lines of hand-rolled BSON decoder code** (custom `decode_value`
  match tree, little-endian read helpers, cstring parser, decimal128-to-f64
  fallback).  The `bson` crate handles all type tags correctly.

### Fixed — type handling in `py_to_bson` (shared encoder path)

- **`bson.Timestamp`** objects are now properly converted to `Bson::Timestamp`
  instead of falling through to the `str()` fallback.
- **`bson.Int64`** values are preserved as BSON int64, not demoted to int32.
- **`bson.Decimal128`**, **`bson.Regex`**, **`re.Pattern`**, **`uuid.UUID`**,
  **`bson.Binary`** (with subtype), **MinKey/MaxKey** sentinel strings, and
  **Python tuples** are all handled correctly in the shared `py_to_bson` path.
- **`str()` fallback now warns**: When the encoder encounters an unrecognized
  Python type, it emits `warnings.warn(...)` instead of silently converting.

## [1.1.5] — 2026-04-10

### Fixed

- **BSON encoder: Python tuples now serialize as BSON arrays**.  Previously, tuples
  fell through to the `str()` fallback and were encoded as strings (e.g.
  `buildInfo.versionArray` became `"(7, 0, 0, 0)"` instead of `[7, 0, 0, 0]`).
  This caused MongoDB Compass and the Node.js driver to misinterpret server metadata,
  leading to broken document display (only `_id` visible).  Both the wire-path
  (`raw_bson::encode_element`) and the storage-path (`bson_helpers::py_to_bson`)
  encoders are fixed.

### Changed — Examples

- **Example 02 (chat memory)**: Complete rewrite with multi-session conversations,
  token tracking, cross-session regex search, aggregation analytics (messages per
  role/session, average tokens per message), context-window assembly, and LLM prompt
  construction.  Indexes for session lookup, TTL expiry, and unique sessions.
- **Example 03 (LangChain RAG)**: Complete rewrite showing scored similarity search
  with visual bars, metadata-filtered search (`pre_filter`), `add_documents` via
  LangChain (manages inserts + embeddings), retriever integration, and full RAG prompt
  assembly — all using the official `MongoDBAtlasVectorSearch` class.
- **Example 06 (Compass demo)**: Added vector search index creation on the knowledge
  base, updated descriptions to mention vendored HNSW instead of NumPy, and added
  self-demo queries (find+sort, aggregation, `$lookup`, `$vectorSearch` with
  `{$meta: "vectorSearchScore"}`, and `$facet`) that run automatically before the
  server enters interactive mode.

## [1.1.4] — 2026-04-10

### Added

- **`createSearchIndex` / `createSearchIndexes` wire commands**: LangChain and
  PyMongo's `Collection.create_search_index()` now work out of the box.  Atlas-style
  `definition.fields` array is translated to smongo's internal index format.
- **`listSearchIndexes`** command and `$listSearchIndexes` aggregate stage, returning
  `queryable: true` and `latestDefinition` so LangChain's index polling succeeds.
- **Metric resolution from index definition**: `$vectorSearch` no longer requires
  `metric`/`similarity` in the query — the metric is resolved from the vector index
  definition, matching Atlas behavior exactly.
- 2 new LangChain-exact pipeline tests (`test_langchain_exact_pipeline_no_metric`,
  `test_langchain_dotproduct_pipeline`).

### Changed — Performance Optimizations

- **`$regex`**: thread-local compiled regex cache — each pattern compiles once per
  thread instead of per document.
- **OPFS `search()`**: O(log n) binary search on sorted entries instead of O(n) linear
  scan.
- **`$lookup` pipeline**: when no `let` variables, the sub-pipeline result is computed
  once and reused instead of re-running per outer row.
- **`$setWindowFields`**: `$sum`/`$avg`/`$min`/`$max`/`$count` computed inline over
  partition indices — eliminates O(partition_size) full-document clones per output field.
- **`$graphLookup`**: hash-indexed `connectToField` for O(1) BFS expansion instead of
  O(|foreign|) linear scan; canonical BSON keys replace fragile `Debug` formatting for
  visited set.
- **`$all`**: `HashSet<Vec<u8>>` canonical key membership instead of O(p×q) nested loops.
- **`$bucket`**: `partition_point()` binary search for O(log b) bucket placement.
- **`$map` / `$filter`**: variable substitution (`replace_var_refs`) precomputed once
  outside the per-element loop.
- **`cmd_distinct`**: Python `set` for O(1) dedup instead of O(n²) `list.contains`.
- **OPFS `serialize_file`**: pre-allocated output buffer.

### Changed — Examples & Docs

- All AI examples (`01`, `03`, `05`) updated to create vector search indexes before
  querying, use Atlas-compatible `{$meta: "vectorSearchScore"}` pipeline, and display
  real similarity scores.
- Example 05 now shows scored retrieval results with visual bars alongside RAG answers.

## [1.1.3] — 2026-04-10

### Added

- **Multi-tenant vector search architecture**, matching
  [Atlas multi-tenant guidance](https://www.mongodb.com/docs/atlas/atlas-vector-search/multi-tenant-architecture/).
  Single collection with `tenant_id` pre-filter, `exact: true`, and `indexingMethod: "flat"`.
- **Flat (exact) index type** for `$vectorSearch`.  Set `indexingMethod: "flat"` in
  the index definition or `"exact": true` in the query to use exhaustive brute-force
  search — optimal for multi-tenant workloads where each tenant has < 10K vectors.
- `VectorIndexOptions` accepts Atlas-native field names: `numDimensions`, `similarity`,
  `indexingMethod`.
- `VectorSearchSpec` parses the `exact` flag from `$vectorSearch` stage documents.
- `IndexProvider::vector_search` reads the index's `indexingMethod` to auto-route
  flat vs HNSW, even without `exact: true`.
- 11 new tests for multi-tenant pre-filtering, flat scan, score normalization, and
  HNSW-vs-exact equivalence.

### Changed

- `search_exact` reuses cached prepared vectors (`graph_vectors`) instead of
  re-normalizing per query — eliminates O(n × d) allocation per call for cosine.
- Extracted `atlas_score` helper to DRY up score normalization between HNSW and flat paths.
- Extracted `ensure_prepared_vectors` to share vector preparation between `rebuild_hnsw`
  and `search_exact`.
- Updated README, CHANGELOG, and all engine-level doc comments to reflect HNSW + flat
  dual-path architecture and multi-tenant support.

## [1.1.2] — 2026-04-10

### Added

- **Vendored HNSW implementation** (`hnsw.rs`): replaces unmaintained `hora` crate
  with a zero-dependency, in-tree Hierarchical Navigable Small Worlds graph.
- Diversified neighbor selection (Algorithm 4 from the Malkov paper) for better recall.
- Generation-counter `VisitedSet` for O(1) resets (no per-search allocation).
- SIMD-friendly distance functions (`chunks_exact(4)`, four accumulators).
- Pre-allocated `BinaryHeap`s in the HNSW search hot loop.
- `numCandidates` parameter now actively used in `IndexProvider::vector_search`.
- 5 HNSW-specific benchmarks in `engine_bench.rs`.

### Changed

- Atlas-compatible score normalization clamped to `[0, 1]` for all metrics.
- Pre-sized serialization buffer in `VectorIndex::to_bytes`.
- Removed `hora` dependency from `Cargo.toml`.

## [1.0.0] — 2026-04-09

### Added

- **`allowDiskUse` for aggregation pipelines.** `Collection.aggregate(pipeline, allowDiskUse=True)`
  enables spill-to-disk for `$sort` and `$group` stages that exceed the in-memory limit (default
  100 MB).  Uses chunked external merge sort (`DiskSpillSorter`) and file-backed group partitioning
  (`DiskSpillGrouper`) via temporary JSON-lines files.  The fast Rust engine path remains the default
  when `allowDiskUse` is `False`.

- **Async Python API (`AsyncMongoClient`).** Full `asyncio`-native client:
  `AsyncMongoClient`, `AsyncDatabase`, `AsyncCollection`, `AsyncCursor`, and
  `AsyncChangeStream`.  All blocking engine work is dispatched via `asyncio.to_thread`
  (Python 3.11+), keeping the event loop responsive for FastAPI, Starlette, and other
  async frameworks.

  ```python
  from smongo import AsyncMongoClient

  async def main():
      client = AsyncMongoClient("local://data")
      db = client["mydb"]
      coll = db["users"]
      await coll.insert_one({"name": "Alice"})
      async for doc in await coll.find({"name": "Alice"}):
          print(doc)
  ```

- **Change streams with resume tokens.** `Collection.watch()` now returns events with
  `_resumeToken` fields containing `{ts, seq}` pairs.  Pass `resume_after=token` to
  restart a stream from the event after the identified one — surviving process restarts,
  reconnects, and crash recovery.  `max_await_time_ms` controls how long the blocking
  iterator waits before raising `StopIteration` (default 30 s).

- **`AsyncChangeStream`** wraps the synchronous change stream for use in async contexts:
  `async for event in await coll.watch(): ...` polls the oplog without blocking the loop.

## [0.9.7] — 2026-04-09

### Fixed

- All `cargo clippy -- -D warnings` errors resolved across the workspace
  (`smongo-engine`, `smongo-py`, `smongo-node`).
- Pre-commit `cargo-fmt` hook updated to use `--all` for workspace support.
- Pre-existing `ruff` and `mypy` issues in Python sources fixed or suppressed.

### Added

- `WASM-BROWSER-GUIDE.md` — comprehensive guide for the WASM/JS browser
  experience covering secure proxy sync, CSP hardening, and production
  deployment best practices.

## [0.9.6] — 2026-04-09

## [0.9.5] — 2026-04-09

### Added

- **Multi-language binding parity.** Node.js and C bindings now cover the full
  Embedded Tier contract defined in `BINDING-PARITY.md`.
- **Node (`smongo-node`):**
  - `Database.drop()`, `Collection.explainFind()`, `Collection.explainFindOne()`,
    `Collection.rebuildAllIndexes()`.
  - Full `IndexOptions` pass-through (partial filter, collation, text/vector/prefix
    index types) on `createIndex`.
  - `ClientSession` gains `find` / `findOne` with sort/limit/skip/projection,
    `updateOne` / `updateMany` / `deleteMany` / `countDocuments`, and
    `aggregate` within transactions.
- **C (`smongo-c`):**
  - Bulk CRUD: `smongo_insert_many`, `smongo_update_many`, `smongo_delete_many`.
  - Metadata: `smongo_list_collection_names`, `smongo_drop_collection`,
    `smongo_stats`, `smongo_drop`.
  - Index management: `smongo_drop_index`, `smongo_list_indexes`,
    `smongo_rebuild_all_indexes`.
  - Explain: `smongo_explain_find`, `smongo_explain_find_one`.
  - Session: `smongo_session_update_one`, `smongo_session_update_many`,
    `smongo_session_delete_many`, `smongo_session_count`,
    `smongo_session_aggregate`.
- **Engine (`smongo-engine`):** `CollectionView::aggregate` enables session-scoped
  aggregation pipelines for all bindings.
- `BINDING-PARITY.md` tracks method-level coverage across Python/Node/C/WASM
  and explicitly scopes Python-only surfaces.

### Changed

- Node test suite (`index.spec.mjs`) expanded from basic CRUD to full
  embedded-tier coverage.
- C test suite (`test_ffi.c`) expanded from 21 to 38 test cases.
- C binding refactored: extracted `parse_bson_array_doc` helper to DRY pipeline
  and bulk-insert parsing.
- Node binding: renamed internal `_db_ref` to `db_ref` for clarity.

## [0.9.3] — 2026-04-07

Baseline for this changelog file. **smongo** is a PyMongo-style API over **redb** (Rust **smongo-engine**), optional **MongoDB wire protocol**, Atlas **sync**, and related tooling. See **README.md** and **ARCHITECTURE.md** for the current design.
