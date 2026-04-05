# Changelog

All notable changes to smongo will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note**: Entries below describe the state of the codebase at the time of each release. References to Python classes like `LocalCollection`, `StreamingCursor`, etc. in older entries reflect APIs that have since been replaced by their Rust equivalents (`RustLocalCollection`, `RustStreamingCursor`, etc.).

## [0.4.0] - 2026-04-05

### Added — Security & Enterprise Features

- **TLS + SCRAM-SHA-256 authentication**: `WireServer(auth_required=True, tls_cert_file=..., tls_key_file=...)` enables TLS (rustls) and SCRAM auth. The Python `WireServer` auto-delegates to `RustWireServer` when security features are requested.
- **Role-based access control (RBAC)**: `grantRolesToUser` / `revokeRolesFromUser` with built-in roles (`read`, `readWrite`, `dbAdmin`, `root`). Auth gate blocks unauthenticated commands (except handshake).
- **Audit logging**: `configure_audit(path)` writes structured JSON audit events (auth, command execution) via `smongo.audit` logger.
- **Free-threaded Python (3.13t+)**: Module declares `#[pymodule(gil_used = false)]`. Python-valued statics migrated to `PyOnceLock`. `unsafe impl Send/Sync` safety comments updated to reference Rust-native synchronization. CI job for `python3.13t` with `PYTHON_GIL=0`.

### Added — Atlas Compatibility & AI Integration

- **`$meta` expression operator** (`query_expressions.rs`): Supports `{"$meta": "vectorSearchScore"}`, `"textScore"`, `"searchScore"`, and `"indexKey"`. Maps to smongo's internal score fields (`_vectorScore`, `_textScore`, etc.), enabling transparent compatibility with Atlas-targeted pipelines from LangChain, LlamaIndex, and other AI frameworks.
- **`$project` exclusion projection** (`aggregation.rs`): Fixed `$project` stage to correctly handle exclusion projections (e.g. `{"$project": {"embedding": 0}}`). Previously, exclusion specs produced empty documents; now the stage copies all fields and removes only the excluded ones, matching MongoDB server behavior.
- **AI examples** (`examples/ai_examples/`): Four wire-protocol-based examples demonstrating smongo as an invisible drop-in for AI workloads:
  - `01_vector_search_rag.py` -- RAG pipeline with `$vectorSearch` over standard PyMongo
  - `02_chat_memory.py` -- AI chat history store with cross-session search and analytics
  - `03_langchain_rag_chain.py` -- Official `MongoDBAtlasVectorSearch` class with zero custom code
  - `04_crewai_agent_tool.py` -- CrewAI agent tools querying smongo via standard PyMongo

### Fixed

- **`$clusterTime` always present**: `operationTime` and `$clusterTime` are now attached to every wire response, not only when an `lsid` is present. This matches real `mongod` behavior (since 3.6) and fixes compatibility with `mongosh`, MongoDB Compass, and drivers that unconditionally expect these fields.
- `cmd_find` in the Rust wire server now materializes `RustStreamingCursor` into a list before slice operations, fixing TypeError when the wire `find` command is used without a sort.
- `WireServer` now auto-creates a `RustLocalClient` (instead of Python `LocalClient`) when delegating to `RustWireServer`, fixing type mismatch on the Rust dispatch path.
- `smongo.audit` component added to `StructuredJSONFormatter`'s component map, so audit log entries emit `"c": "AUDIT"` instead of `"DEFAULT"`.
- Test isolation fix: `TestUsersInfo.test_returns_users_list` clears the global user store before assertions.

### Changed

- 80 Rust tests (up from 67), 1,017 Python tests (up from ~960). Total: 1,097.
- 50 Rust source files, 54 Python source files. ~24,100 Rust LOC, ~9,000 Python LOC.

## [0.3.0] - 2026-04-04

### Changed — Interop Elimination (Priorities 1-5, 7-8)

- **P1**: Wire `find` performs sort, skip, limit, and projection in Rust; wire `aggregate` calls `aggregate_pipeline` directly. The Python `Cursor` is removed from the wire dispatch path.
- **P2**: All 47 oplog `call_method` sites replaced with typed `RustWtSession` / `RustWtCursor` borrow. `OplogHub` uses `Py<ChangeStream>` instead of `Py<PyAny>`.
- **P3**: Admin WiredTiger operations (metadata walks, statistics, user persistence, checkpoint) use typed borrow. `CachedSystemInfo` caches platform/OS info. `cached_pid()`. `BTreeSet` replaces Python `set()` in `listDatabases`. `list_collection_names()` uses a direct Rust call.
- **P4**: Eight class attributes cached via `OnceLock` macros (`bson.ObjectId`, `bson.Decimal128`, `bson.Regex`, `builtins.int` / `float` / `round`, `datetime.datetime`, `datetime.timezone.utc`). `wire_codec.rs` and `query_expressions.rs` updated.
- **P5**: `bson_helpers::shallow_copy_dict` replaces five `dict.copy()` Python dispatch calls in `local_collection.rs`.
- **P7**: `$jsonSchema` validation ported to Rust (`schema.rs`). `ValidationError` is now a Rust-defined PyO3 exception. `local_collection.rs` calls `crate::schema::validate_document` directly -- zero Python dispatch on validated writes. `smongo_schema` cached module removed. `CachedImports.validation_err` field removed.
- **P8**: BSON boundary normalization moved to raw byte level (`rust/src/raw_bson.rs`). Wire decode now goes directly from BSON bytes to engine-ready `PyDict` in a single pass -- no intermediate `bson::Document` allocation and no `normalize_inbound` walk. Wire encode goes directly from `PyDict` to BSON bytes -- no intermediate `bson::Document` and no `normalize_outbound` walk. `ObjectId::from_raw()` avoids hex encode/decode overhead. All CRUD and aggregate handlers updated; `to_bson`/`from_bson` pyfunctions delegate to raw codec.

### Added

- `cached_attr!` and `cached_nested_attr!` macros in `cached_modules.rs`
- `CachedSystemInfo` struct for process-level platform/OS caching
- `RustWtSession::open_session_typed()` on `RustLocalClient`
- `pub(crate)` typed methods on `RustWtSession` (`create_typed`, `checkpoint_typed`, `close_typed`) and `RustWtCursor` (various)
- `rust/src/schema.rs` -- full `$jsonSchema` validation engine with `ValidationError` exception
- `rust/src/raw_bson.rs` -- single-pass raw BSON byte decoder and encoder with inline engine-type conversion

## [0.2.0] - 2026-04-01

### Changed — Streaming Architecture

The read path has been rearchitected for lazy, on-demand document iteration.
Previously, `find()` materialized every matching document into a Python list
before returning. Now the entire pipeline -- from WiredTiger cursors through
the client-facing `Cursor` -- streams documents lazily. Callers that only need
the first *N* results (e.g. `.limit(10)`, `find_one()`) avoid deserializing
the rest.

- **`StreamingCursor` handles all query plan types** -- PK lookup, index scan,
  `$in` multi-point scan, `$or`-union, and collection scan. Previously only
  PK lookup and collection scan were supported; index-accelerated paths now
  stream one document at a time from WiredTiger with per-doc BSON decode.
- **`Cursor` accepts any `Iterable[Document]`** (generators, `StreamingCursor`,
  lists). Source documents are materialized only when needed: `sort()` forces
  full materialization; `skip()`/`limit()` without `sort()` use
  `itertools.islice` so only the required slice is consumed from the source.
  Results are cached after first resolution.
- **`LocalCollection.find_one(query)`** -- new method that returns the first
  matching document via the streaming path. Only one document is deserialized
  from WiredTiger instead of every match.
- **`LocalCollection.count(query)`** -- new method that counts matching
  documents without building an intermediate list. For empty queries, delegates
  to `count_fast()` (no BSON deserialization at all).
- **`Collection.find()`** (client facade) now wraps a `StreamingCursor` in a
  lazy `Cursor`, replacing the previous eager list.
- **`Collection.find_one()`** delegates to `LocalCollection.find_one()`.
- **`Collection.count_documents()`** delegates to `LocalCollection.count()`.
- **`Collection.aggregate()`** pulls from `find_streaming()` instead of
  `get_all()`.
- **Wire protocol `find`** command uses `find_streaming()` → lazy `Cursor`.
- **Wire protocol `count`** command uses `LocalCollection.count()`.
- **Wire protocol `distinct`** streams instead of materializing.
- **Wire protocol `findAndModify`** (no-sort path) and `update` (replace path)
  use `find_one()` instead of materializing all matches to take `[0]`.

### Added

- `tests/test_streaming.py` -- 59 tests covering every streaming path,
  `find_one`, `count`, lazy `Cursor` with generators, `islice` short-circuit,
  find-vs-streaming parity, and client integration.
- `Cursor` and `StreamingCursor` re-exported from `smongo` package root.
- Streaming performance benchmarks in `tests/performance/`.

## [0.1.0] - 2026-04-01

### Added

- WiredTiger-backed embedded document storage engine
- Full MQL query compiler: `$gt`, `$lt`, `$gte`, `$lte`, `$eq`, `$ne`, `$in`, `$nin`, `$exists`, `$regex`, `$not`, `$all`, `$elemMatch`, `$size`, `$type`, `$or`, `$and`, `$nor`, `$expr`, `$mod`, `$text`, bitwise operators
- Update operators: `$set`, `$inc`, `$push`, `$unset`, `$addToSet`, `$pull`, `$pop`, `$min`, `$max`, `$mul`, `$rename`, `$currentDate`, `$bit`, positional `$`, `$[]`, `$[<identifier>]`, pipeline updates
- Aggregation pipeline with 25+ stages: `$match`, `$group`, `$project`, `$sort`, `$limit`, `$skip`, `$unwind`, `$addFields`/`$set`, `$count`, `$replaceRoot`, `$lookup` (equality + pipeline), `$sample`, `$vectorSearch`, `$facet`, `$out`, `$merge`, `$bucket`, `$bucketAuto`, `$graphLookup`, `$unionWith`, `$unset`, `$redact`, `$sortByCount`, `$setWindowFields`, `$replaceWith`
- Group accumulators: `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`, `$stdDevPop`, `$stdDevSamp`, `$mergeObjects`, `$top`, `$bottom`, `$topN`, `$bottomN`, `$firstN`, `$lastN`
- 60+ expression operators for aggregation and `$expr`
- B-Tree indexes with WiredTiger, unique/sparse/TTL/compound support
- Text indexes with `$text` query operator
- Hashed indexes for equality-only lookups
- Partial indexes with `partialFilterExpression`
- Wildcard indexes (`$**`)
- Heuristic prefix-scoring query planner with `$in` multi-point index scan
- Streaming cursors for lazy document iteration
- Read/write lock for concurrent reader access
- Index-driven TTL reaper (no more full collection scans)
- MongoDB wire protocol (OP_MSG) server with 80+ commands
- Oplog with change streams for real-time mutation tracking
- Bidirectional sync to Atlas via SyncManager
- Flask-based web dashboard with REST API
- `$jsonSchema` document validation
- `smongo.connect()` zero-config quickstart
- Context manager support (`with smongo.connect() as db`)
- PyMongo-compatible exception hierarchy
- `py.typed` marker for PEP 561 compliance
- Docker and docker-compose support
- GitHub Actions CI/CD pipeline
- 955+ tests (at time of release) with pytest
