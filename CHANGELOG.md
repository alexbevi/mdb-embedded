# Changelog

Notable changes to **smongo** are recorded here. Earlier history lives in git.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
