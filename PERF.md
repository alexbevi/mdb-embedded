# Performance Benchmarks

**27 benchmarks across writes, reads, aggregation, and streaming.**

All benchmarks run against 1,000- or 10,000-document collections backed by WiredTiger on local disk, using pytest-benchmark with GC disabled. Times are per-operation unless noted. Results below are from a single machine (Apple Silicon, Python 3.11); your absolute numbers will differ, but the **relative relationships** between operations are the point.

```bash
make perf   # or: pytest tests/performance/ -m performance -v --benchmark-disable-gc
```

---

## Summary

| Category | Benchmark | Mean | Ops/sec | Dataset |
|---|---|---:|---:|---|
| **Write** | `insert_one` | 26.6 us | 37,600 | single doc |
| **Write** | `insert_many` (1K docs) | 42.0 ms | 24 | 1K batch |
| **Write** | `update_many` (indexed) | 6.5 ms | 155 | 1K docs, 200 matching |
| **Write** | `update_many` (no index) | 8.4 ms | 120 | 1K docs, 200 matching |
| **Write** | `delete_many` (1K docs) | 42.0 ms | 24 | 1K docs |
| **Read** | PK lookup (`_id`) | 11.2 us | 89,300 | 10K docs |
| **Read** | Index scan (equality) | 0.87 ms | 1,150 | 10K docs, ~125 matches |
| **Read** | Compound index range | 0.96 ms | 1,040 | 10K docs |
| **Read** | Collection scan (`$gt`) | 34.9 ms | 29 | 10K docs, ~6,875 matches |
| **Read** | `compile_query` (complex) | 0.6 us | 1,666,000 | in-memory only |
| **Aggregation** | `$match` + `$group` | 40.7 ms | 25 | 10K docs |
| **Aggregation** | `$sort` | 38.4 ms | 26 | 10K docs |
| **Aggregation** | `$lookup` (1K-to-20) | 3.1 ms | 323 | 1K parent, 20 foreign |
| **Aggregation** | `$unwind` | 27.1 ms | 37 | 1K docs x 10 elements |
| **Aggregation** | Full pipeline (4 stages) | 41.3 ms | 24 | 10K docs |
| **Streaming** | `find_one` (indexed) | 1.9 ms | 513 | 10K docs |
| **Streaming** | `count({})` (fast path) | 2.6 ms | 385 | 10K docs |
| **Streaming** | `count` (filtered) | 26.3 ms | 38 | 10K docs |
| **Streaming** | `find({}).limit(10)` | 36.8 us | 27,200 | 10K docs |
| **Streaming** | Index scan (streaming) | 7.0 ms | 143 | 10K docs, ~1K matches |
| **Streaming** | PK lookup (streaming) | 11.7 us | 85,500 | 10K docs |
| **Streaming** | `$in` scan (3 values) | 21.9 ms | 46 | 10K docs, ~3K matches |
| **Streaming** | Collection scan (streaming) | 35.7 ms | 28 | 10K docs |

---

## Write Path

Every write -- insert, update, delete -- is wrapped in a **WiredTiger transaction** that atomically commits data, indexes, and oplog. The cost includes BSON encoding, index key maintenance, and oplog append.

| Benchmark | What it measures | Mean | Rounds |
|---|---|---:|---:|
| `test_insert_one_throughput` | Single-doc insert (BSON encode + WT write + oplog) | 26.6 us | 7,971 |
| `test_insert_many_1000` | Batch insert 1,000 docs (includes delete_many cleanup) | 42.0 ms | 37 |
| `test_update_many_with_index` | Update 200/1K docs via `dept_1` index scan | 6.5 ms | 145 |
| `test_update_many_without_index` | Update 200/1K docs via collection scan | 8.4 ms | 109 |
| `test_delete_many_1000` | Delete all 1K docs (includes re-insert for each round) | 42.0 ms | 22 |

**Key observations**:
- Single inserts sustain ~37K ops/sec -- each one is a full WiredTiger transaction with BSON encode, index maintenance, and oplog append.
- Indexed updates are ~23% faster than unindexed updates because the query planner narrows the candidate set before mutating.
- Batch operations (insert_many, delete_many) are dominated by per-doc WiredTiger cursor writes; the transaction wraps the entire batch.

---

## Read Path (Materialized)

These benchmarks use `Collection.find()` (backed by `RustLocalCollection`) which materializes all matching documents into a Python list. This is the baseline for comparison with the streaming path.

| Benchmark | What it measures | Mean | Rounds |
|---|---|---:|---:|
| `test_pk_lookup_query` | `find({"_id": target})` on 10K docs | 11.2 us | 15,287 |
| `test_index_scan_query` | `find({"age": 30})` with `age_1` index, 10K docs | 0.87 ms | 895 |
| `test_compound_index_range_query` | `find({"city": "city_2", "age": {"$gte": 25, "$lte": 40}})` with compound index | 0.96 ms | 838 |
| `test_collection_scan_query` | `find({"age": {"$gt": 25}})` on 10K docs, no index | 34.9 ms | 28 |
| `test_compile_query_complex` | `compile_query()` with `$and`, `$in`, `$elemMatch`, `$regex` | 0.6 us | 83,626 |

**Key observations**:
- PK lookup is **~3,100x faster** than a collection scan. WiredTiger's B-tree seek is O(log n).
- Index scans are **~40x faster** than collection scans on the same data. The planner computes tight bounds and fetches only matching IDs from the index B-tree.
- Query compilation is essentially free (~0.6 us). The compiled predicate is a closure that avoids re-parsing on each document.

---

## Aggregation Pipeline

All aggregation benchmarks use the client-level `aggregate()` API, which feeds documents through the pipeline engine.

| Benchmark | What it measures | Mean | Rounds |
|---|---|---:|---:|
| `test_match_group_10k` | `$match` + `$group` (by city, `$sum` + `$avg`) on 10K docs | 40.7 ms | 25 |
| `test_sort_10k` | `$sort` on two fields, 10K docs | 38.4 ms | 27 |
| `test_lookup_1k_to_1k` | `$lookup` joining 1K parent docs to 20 foreign docs | 3.1 ms | 289 |
| `test_unwind_arrays` | `$unwind` expanding 1K docs x 10 array elements = 10K output docs | 27.1 ms | 36 |
| `test_full_pipeline_10k` | `$match` → `$group` → `$sort` → `$project` on 10K docs | 41.3 ms | 24 |

**Key observations**:
- The pipeline processes ~250K docs/sec for `$match` + `$group` (10K input docs, hash-based grouping with accumulator updates).
- `$sort` on 10K docs takes ~38ms (Python's `list.sort` with a custom key function).
- `$lookup` is fast when the foreign collection is small (20 docs) -- each join is a hash lookup, not a nested scan.
- A 4-stage pipeline adds negligible overhead beyond its constituent stages -- the pipeline dispatch loop is thin.

---

## Streaming Architecture

The streaming benchmarks measure the lazy read path introduced in v0.2.0. At the `MongoClient` level, `Collection.find()` uses `StreamingCursor` (Python) which delegates to the Rust query planner and BSON decoding. When using `RustLocalClient` directly, `RustStreamingCursor` yields documents one at a time from WiredTiger with zero Python overhead. Both paths apply `skip`/`limit` via `itertools.islice` when no sorting is needed.

### Client-Level API

| Benchmark | What it measures | Mean | Rounds |
|---|---|---:|---:|
| `test_find_one_streaming_vs_full` | `Collection.find_one({"city": "city_3"})` (indexed, 10K docs) | 1.95 ms | 452 |
| `test_count_documents_empty_query` | `Collection.count_documents({})` (fast path, 10K docs) | 2.51 ms | 402 |
| `test_count_documents_filtered` | `Collection.count_documents({"age": {"$gt": 50}})` (10K docs) | 26.2 ms | 38 |
| `test_find_limit_10` | `Collection.find({}).limit(10).to_list()` (10K docs) | 36.8 us | 13,645 |

### Storage-Level Streaming

| Benchmark | What it measures | Mean | Rounds |
|---|---|---:|---:|
| `test_streaming_find_one` | `RustLocalCollection.find_one({"city": "city_3"})` (indexed) | 1.92 ms | 488 |
| `test_streaming_count_empty` | `RustLocalCollection.count({})` (fast path, no BSON decode) | 2.60 ms | 378 |
| `test_streaming_count_filtered` | `RustLocalCollection.count({"age": {"$gt": 50}})` | 26.3 ms | 40 |
| `test_streaming_limit_10` | `find_streaming({}).limit(10)` via Cursor | 36.8 us | 13,209 |
| `test_streaming_index_scan` | `find_streaming({"city": "city_5"})` (1K matches) | 7.0 ms | 142 |
| `test_streaming_pk_lookup` | `find_streaming({"_id": target})` (single doc) | 11.7 us | 12,739 |
| `test_streaming_in_scan` | `find_streaming({"city": {"$in": [...]}})` (3K matches) | 21.9 ms | 47 |
| `test_streaming_collection_scan` | `find_streaming({"age": {"$gt": 25}})` (full scan) | 35.7 ms | 28 |

### Streaming vs. Materialized: Key Comparisons

| Operation | Streaming | Materialized | Speedup | Why |
|---|---:|---:|---:|---|
| **find + limit(10)** on 10K docs | **36.8 us** | 34.9 ms (full scan) | **~950x** | `islice` stops after 10 docs; materialized scans all 10K |
| **PK lookup** | 11.7 us | 11.2 us | ~1x | Both paths do a single B-tree seek; streaming adds negligible generator overhead |
| **Index scan** (1K matches) | 7.0 ms | 0.87 ms (125 matches) | N/A (different cardinality) | Streaming iterates per-doc; materialized batches all IDs then fetches |
| **Collection scan** (6.8K matches) | 35.7 ms | 34.9 ms | ~1x | Full scan is full scan -- streaming doesn't help when you consume everything |
| **find_one** (indexed, 10K docs) | **1.92 ms** | N/A | -- | Only 1 doc deserialized from WiredTiger instead of all matches |
| **count({})** (10K docs) | **2.60 ms** | N/A | -- | `count_fast` uses WiredTiger cursor walk, no BSON decode |

**Key insight**: Streaming shines when you **don't need all results**. `find({}).limit(10)` is ~950x faster than materializing the entire collection. `find_one()` deserializes exactly 1 document. `count({})` never touches BSON at all. When you consume every result (full collection scan, no limit), streaming adds negligible overhead (~2%) compared to the materialized path.

---

## Regression Thresholds

The benchmark suite enforces **ceiling thresholds** -- if any benchmark's mean exceeds its threshold, the test fails. These are intentionally generous to avoid false positives from machine load variance while still catching real regressions.

| Benchmark | Threshold |
|---|---:|
| `test_insert_one_throughput` | 10.0 ms |
| `test_insert_many_1000` | 2,000 ms |
| `test_update_many_without_index` | 1,000 ms |
| `test_update_many_with_index` | 1,000 ms |
| `test_delete_many_1000` | 3,000 ms |
| `test_collection_scan_query` | 1,000 ms |
| `test_pk_lookup_query` | 5.0 ms |
| `test_index_scan_query` | 50.0 ms |
| `test_compound_index_range_query` | 100.0 ms |
| `test_compile_query_complex` | 1.0 ms |
| `test_match_group_10k` | 2,000 ms |
| `test_sort_10k` | 2,000 ms |
| `test_lookup_1k_to_1k` | 2,000 ms |
| `test_unwind_arrays` | 1,000 ms |
| `test_full_pipeline_10k` | 3,000 ms |

Streaming benchmarks do not have thresholds yet -- they will be tightened once baseline variance is established.

---

## Interop Dispatch Overhead

These benchmarks isolate the Rust/Python dispatch path: command parsing, handler lookup, `CursorRegistry` interaction, and BSON normalization. They run against a 200-document collection to keep data overhead low and focus on per-command fixed costs.

| Benchmark | What it measures | Mean | Ops/sec |
|---|---|---:|---:|
| `test_dispatch_findandmodify` | `findAndModify` with `$inc` (dispatch + WT round-trip) | 64 us | 15,600 |
| `test_dispatch_insert_delete_cycle` | `insert_one` + `delete_one` pair | 91 us | 11,000 |
| `test_dispatch_find_simple` | `find` with filter + `limit(10)` | 287 us | 3,490 |
| `test_dispatch_find_all` | `find({})` materializing 200 docs | 361 us | 2,770 |
| `test_dispatch_count` | `count_documents` with filter | 378 us | 2,650 |
| `test_dispatch_aggregate_small` | 3-stage pipeline (`$match` + `$group` + `$sort`) | 457 us | 2,190 |

**Key observations**:
- Single-doc write commands (`findAndModify`, `insert_one`) complete in ~60-90 us end-to-end, including WiredTiger I/O.
- The per-dispatch overhead (command lookup, context access, cursor registration) is <20 us -- dominated by the actual data operation.
- All module lookups, handler resolution, and cursor registration use Rust-native paths (no `py.import()`, no Python method dispatch for `CursorRegistry`).

---

## Running Benchmarks

```bash
# All performance benchmarks
make perf

# Or directly:
pytest tests/performance/ -m performance -v --benchmark-disable-gc

# Save results for comparison:
pytest tests/performance/ -m performance --benchmark-save=baseline

# Compare against saved baseline:
pytest tests/performance/ -m performance --benchmark-compare=baseline
```

---

## Methodology

- **Framework**: [pytest-benchmark](https://pytest-benchmark.readthedocs.io/) with `--benchmark-disable-gc`
- **Timer**: `time.perf_counter` (default, nanosecond resolution)
- **Calibration**: Automatic -- pytest-benchmark calibrates rounds to achieve statistical significance (minimum 5 rounds, minimum 5us per round, up to 1s total)
- **Isolation**: Each benchmark uses a fresh WiredTiger directory via `tmp_path` fixtures. No shared state between benchmarks.
- **Dataset**: Synthetic documents with 7 fields each (string `_id`, string `name`, int `age`, string `city`, string `dept`, array `tags`, int `salary`). Cities cycle through 10 values, departments through 5, ages through 80.
- **Index setup**: Benchmarks that test indexed paths create indexes in the fixture, ensuring index build time is not measured.
- **What's measured**: Only the query/write operation itself. Document generation, index creation, and collection cleanup happen outside the timed region.

---

## Where Python Is Still the Bottleneck

BSON serialization, query compilation, index operations, wire compression, and most CRUD are now handled entirely in Rust via `_smongo_core`. All hot-path module imports are cached (`PyOnceLock` for Python objects, `OnceLock` for Rust-only data), the handler signature is fully typed (`ConnectionContext`), and `CursorRegistry` operations are called directly from Rust. The remaining Python-bound costs are:

1. **Aggregation accumulators** (~40ms for `$group` on 10K): Hash-based grouping with per-doc field access via Python dicts. Rust pipeline stages exist for many operators but the fallback Python path is still used for some complex expressions.
2. **Pipeline materialization**: Each aggregation stage produces a full `list[Document]`. A fully Rust pipeline could use zero-copy iterators between stages.
3. **PyO3 boundary crossings**: Callbacks from Rust into Python (e.g. oplog writes, aggregation accumulator dispatch) add per-call overhead that would disappear with pure-Rust equivalents.
4. **User-facing `Cursor` only on the Python API**: Wire protocol `find` and `aggregate` no longer route through the Python `Cursor` class. For wire `find`, sort, skip, limit, and projection are applied in Rust before batches are returned. Wire `aggregate` calls `aggregate_pipeline` directly. The Python `Cursor` remains only for the user-facing Python API (`Collection.find()` and chained `.sort()` / `.skip()` / `.limit()` / projection there).
5. **Oplog and admin WiredTiger paths**: Oplog and admin/metadata WiredTiger operations are fully typed at the Rust boundary (no Python dispatch for WT cursor operations in those hot paths).

See [ROADMAP.md](ROADMAP.md) (Part 5 — Python wire path) for the roadmap on eliminating the remaining Python-bound stages.

---

## Hardware

Results above were collected on:

```
Platform: macOS (darwin)
Python:   3.11.13
Timer:    time.perf_counter
pytest-benchmark: 5.2.3
```

Your numbers will differ. Run `make perf` on your own machine and use `--benchmark-save` / `--benchmark-compare` to track regressions over time.
