# Changelog

All notable changes to smongo will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- 960+ tests with pytest
