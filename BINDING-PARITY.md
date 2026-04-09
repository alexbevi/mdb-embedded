# Binding Parity Matrix

This document tracks API surface coverage across all smongo bindings relative
to the `smongo-engine` Rust core. It defines the **Embedded Tier** -- the
contract every binding should implement -- and explicitly scopes everything
above that tier as Python-only.

## Embedded Tier (contract for all bindings)

The embedded tier is the `Database`, `Collection`, `CollectionView`
(transaction session), and index/explain surface from `smongo-engine`.

### Database

| Method                   | Python | Node | C    | WASM |
|--------------------------|--------|------|------|------|
| `open`                   | Yes    | Yes  | Yes  | Yes  |
| `close` / drop handle    | Yes    | Yes  | Yes  | --   |
| `drop` (remove DB)       | Yes    | Yes  | Yes  | --   |
| `collection`             | Yes    | Yes  | Yes  | Yes  |
| `listCollectionNames`    | Yes    | Yes  | Yes  | --   |
| `dropCollection`         | Yes    | Yes  | Yes  | --   |
| `stats`                  | Yes    | Yes  | Yes  | --   |
| `startSession`           | Yes    | Yes  | Yes  | --   |
| `reapTtl`                | Yes    | Yes  | Yes  | --   |

### Collection

| Method                     | Python | Node | C    | WASM |
|----------------------------|--------|------|------|------|
| `insertOne`                | Yes    | Yes  | Yes  | Yes  |
| `insertMany`               | Yes    | Yes  | Yes  | --   |
| `findOne`                  | Yes    | Yes  | Yes  | --   |
| `findOne` (with options)   | Yes    | Yes  | Yes  | --   |
| `find`                     | Yes    | Yes  | Yes  | Yes  |
| `find` (with options)      | Yes    | Yes  | Yes  | --   |
| `updateOne`                | Yes    | Yes  | Yes  | --   |
| `updateOne` (with upsert)  | Yes    | Yes  | Yes  | --   |
| `updateMany`               | Yes    | Yes  | Yes  | Yes  |
| `deleteOne`                | Yes    | Yes  | Yes  | --   |
| `deleteMany`               | Yes    | Yes  | Yes  | Yes  |
| `countDocuments`           | Yes    | Yes  | Yes  | Yes  |
| `aggregate`                | Yes    | Yes  | Yes  | --   |
| `explainAggregate`         | Yes    | Yes  | Yes  | --   |
| `explainFind`              | Yes    | Yes  | Yes  | --   |
| `explainFindOne`           | Yes    | Yes  | Yes  | --   |
| `createIndex`              | Yes    | Yes  | Yes  | --   |
| `createIndex` (adv. opts)  | Yes    | Yes  | Yes  | --   |
| `dropIndex`                | Yes    | Yes  | Yes  | --   |
| `listIndexes`              | Yes    | Yes  | Yes  | --   |
| `rebuildAllIndexes`        | Yes    | Yes  | Yes  | --   |
| `reapExpired` (TTL)        | Yes    | Yes  | Yes  | --   |

### TransactionSession (CollectionView)

| Method                     | Python | Node | C    | WASM |
|----------------------------|--------|------|------|------|
| `beginTransaction`         | Yes    | Yes  | Yes  | --   |
| `commitTransaction`        | Yes    | Yes  | Yes  | --   |
| `rollbackTransaction`      | Yes    | Yes  | Yes  | --   |
| `insertOne`                | Yes    | Yes  | Yes  | --   |
| `findOne`                  | Yes    | Yes  | Yes  | --   |
| `find`                     | Yes    | Yes  | Yes  | --   |
| `updateOne`                | Yes    | Yes  | Yes  | --   |
| `updateMany`               | Yes    | Yes  | Yes  | --   |
| `deleteOne`                | Yes    | Yes  | Yes  | --   |
| `deleteMany`               | Yes    | Yes  | Yes  | --   |
| `countDocuments`           | Yes    | Yes  | Yes  | --   |
| `aggregate`                | Yes    | Yes  | Yes  | --   |

## Python-Only Surfaces (explicitly out of scope for Node/C/WASM)

These capabilities are built on top of the embedded tier in `smongo-py` and
the pure-Python `smongo` package. They are not part of the binding contract.

- **Wire protocol server** -- `RustWireServer`, `rs_dispatch`, OP_MSG codec,
  compression, BSON normalization, error response builders, command routing
- **Session & cursor registries** -- `SessionRegistry`, `CursorRegistry`,
  reaper threads, `MsgHeader` parsing
- **Profiler & diagnostics** -- `Profiler`, `OperationTracker`, `TopStats`,
  `OpEntry`, opcounters, `ConnectionCounter`, `ParameterStore`
- **Sync / replication** -- `OplogHub`, `ChangeStream`, `VectorClock`,
  `TombstoneRegistry`, CRDT merges, field-level sync, `to_pymongo`/`from_pymongo`
- **Query compiler exports** -- `CompiledQuery`, `compile_query`, `match_doc`,
  `resolve_expr`, `apply_update` as standalone functions
- **Aggregation stage exports** -- Individual `*_stage` functions (the engine
  runs full pipelines; individual stages are not separately callable)
- **BSON utilities** -- `to_bson`/`from_bson`, `ObjectId` class,
  `ReadWriteLock`, result wrapper classes
- **Schema validation** -- `validate_document`, `ValidationError`
- **Connection context** -- `ConnectionContext`, `FreeMonitoringState`,
  `LastWriteResult`, auth fields
- **Index encoding helpers** -- `sortable_encode`, `invert_encoded`,
  `encode_index_key`, `rs_tokenize`, `rs_hash_value`, `rs_flatten_doc`
- **KV helpers** -- `redb_kv_get/put/remove/scan` (redb-specific)

## Intentionally Omitted (backend-specific or internal)

- `collection_with_oplog` / oplog writer/reader -- requires oplog tables
- `redb_atomic_checkpoint_truncate_oplog` -- redb-specific maintenance
- `storage_stats`, `verify`, `compact` -- may be exposed later as needed
- `find_iter` / `FindCursor` -- language-native iteration patterns differ
- Streaming `aggregate_stream` -- bindings use materialized results

## WASM Notes

The WASM surface (`wasm_bindings.rs`) is intentionally minimal: `insert_one`,
`find`, `count_documents`, `delete_many`, `update_many` over both `MemBackend`
and `OpfsBackend`. OPFS transactions are no-ops at the storage level. Expanding
WASM toward embedded tier parity is tracked separately from this document.
