# What's Next

**Three features that turn smongo from a great embedded engine into a universal one.**

smongo already runs 25+ aggregation stages, `$graphLookup`, `$vectorSearch`, full MQL, ACID transactions, and bidirectional Atlas sync -- all on local WiredTiger B-trees. What follows is the roadmap for the three most-requested capabilities that MongoDB users expect from a production-grade engine: **geospatial queries**, **time series collections**, and **graph traversal**. One of them is already done.

---

## Status at a Glance

```
 ✅  $graphLookup          BFS traversal in Rust          SHIPPED
 ✅  $geoNear              Haversine stage (no index)     SHIPPED
 🔲  $near / $nearSphere   Query operators (needs index)  STUBBED
 🔲  $geoWithin            Polygon containment            STUBBED
 🔲  $geoIntersects        Geometry intersection           STUBBED
 🔲  2dsphere index         S2 cell encoding               STUBBED
 🔲  Time series colls      Auto-bucketing engine           PLANNED
```

---

## 1. Geospatial: `$geoNear` (DONE)

**Priority: HIGH | Impact: HIGH | Difficulty: LOW**

```
 Pipeline input
       │
       ▼
 ┌─────────────────────────────────────────┐
 │  $geoNear                               │
 │                                         │
 │  1. Parse query point (GeoJSON / array) │
 │  2. Optional MQL pre-filter (query)     │
 │  3. Extract (lon, lat) per document     │
 │  4. Haversine distance computation      │
 │  5. Filter by minDistance / maxDistance  │
 │  6. Sort nearest-first                  │
 │  7. Apply distanceMultiplier            │
 │  8. Attach distanceField + includeLocs  │
 └─────────────────┬───────────────────────┘
                   │
                   ▼
           Pipeline output (sorted by distance)
```

**What shipped:**

- `smongo/aggregation/geo.py` -- full `$geoNear` aggregation stage
- Haversine great-circle distance (meters) on WGS84 sphere
- GeoJSON Point and legacy `[lon, lat]` coordinate formats
- All MongoDB spec fields: `near`, `distanceField`, `key`, `spherical`, `maxDistance`, `minDistance`, `distanceMultiplier`, `query`, `includeLocs`, `limit`
- Wired into both the Rust aggregation dispatch and the Python `Cursor` pipeline
- 31 tests covering distance math, coordinate extraction, filtering, sorting, limiting, and edge cases

**Usage:**

```python
from smongo import MongoClient

client = MongoClient("local://data")
db = client["myapp"]
restaurants = db["restaurants"]

restaurants.insert_many([
    {"name": "Joe's Pizza", "location": {"type": "Point", "coordinates": [-73.9857, 40.7484]}},
    {"name": "Tartine",     "location": {"type": "Point", "coordinates": [-122.4194, 37.7749]}},
    {"name": "In-N-Out",    "location": {"type": "Point", "coordinates": [-118.2437, 34.0522]}},
])

# Find the 5 nearest restaurants to Times Square
nearest = restaurants.aggregate([
    {"$geoNear": {
        "near": {"type": "Point", "coordinates": [-73.9857, 40.7484]},
        "distanceField": "distance_meters",
        "maxDistance": 5_000_000,
        "limit": 5,
    }},
])
for r in nearest:
    print(f"{r['name']}: {r['distance_meters']:,.0f}m away")
```

**Why no index is needed:** The stage computes Haversine distance for every candidate document, the same way `$vectorSearch` computes cosine/euclidean distance for every vector. This is the correct architecture at collections up to ~100k documents. A `2dsphere` index (below) narrows the candidate set for larger collections but doesn't change the stage's behavior or API.

**Files:**
- `smongo/aggregation/geo.py` -- stage implementation
- `rust/src/aggregation.rs` -- Rust dispatch (delegates to Python)
- `rust/src/cached_modules.rs` -- module cache entry
- `smongo/aggregation/cursor.py` -- Python pipeline dispatch
- `smongo/aggregation/__init__.py` -- public export
- `tests/test_geo.py` -- 31 tests

---

## 2. Geospatial: `2dsphere` Index + Query Operators (NEXT)

**Priority: HIGH | Impact: HIGH | Difficulty: MEDIUM-HIGH**

The `$geoNear` stage is the query interface. A `2dsphere` index is the performance backend. When the collection grows beyond what brute-force scanning handles comfortably, the index narrows the candidate set *before* distance computation.

### How MongoDB does it

MongoDB stores geospatial data in `2dsphere` indexes using **S2 cell coverings** from Google's S2 Geometry library. The key insight: S2 maps every point on the sphere to a 64-bit cell ID at configurable resolution levels (1-30). These cell IDs are regular integers that sort lexicographically in a B-tree -- which is exactly what WiredTiger provides.

```
  GeoJSON Point                 S2 Cell IDs              WiredTiger B-Tree
  ─────────────                 ───────────              ─────────────────
  { type: "Point",    ──►    cell_id_level_16  ──►    key: "cell_id|doc_id"
    coordinates:             cell_id_level_20         value: doc_id
    [-73.98, 40.74] }       cell_id_level_24
                             (covering at multiple levels)
```

### What to build

**Phase A: Index type (`2dsphere`)**

```
 create_index([("location", "2dsphere")])
       │
       ▼
 ┌────────────────────────────────────────────┐
 │  On insert:                                │
 │  1. Extract GeoJSON from document          │
 │  2. Compute S2 cell covering (multi-level) │
 │  3. Write cell_id keys to WT B-tree table  │
 │                                            │
 │  On delete:                                │
 │  1. Recompute covering from stored doc     │
 │  2. Remove cell_id keys from WT table      │
 └────────────────────────────────────────────┘
```

This follows the exact pattern of the existing index types. Compare:

| Index type | Key encoding | WiredTiger key format |
|---|---|---|
| **btree** | `encode_index_key(values, id, dirs)` | `"encoded_value\|doc_id"` |
| **text** | `_tokenize(text)` | `"token\|doc_id"` |
| **hashed** | `_hash_value(val)` | `"hash\|doc_id"` |
| **wildcard** | `_flatten_doc` + `_sortable_encode` | `"path\|encoded_val\|doc_id"` |
| **2dsphere** | S2 cell covering | `"cell_id\|doc_id"` |

The `_insert_geo_entry` / `_delete_geo_entry` / `geo_lookup` methods slot into `IndexManager` alongside the existing text/hashed/wildcard helpers.

**Phase B: Query operators**

| Operator | What it does | Index usage |
|---|---|---|
| `$near` / `$nearSphere` | Find docs nearest to a point | S2 cell range scan, expanding rings |
| `$geoWithin` | Find docs inside a polygon/circle | S2 cell covering of query region |
| `$geoIntersects` | Find docs whose geometry intersects | S2 cell covering + exact post-filter |

**Dependencies:**
- S2 Geometry library: `s2` crate (Rust) or `s2sphere` / `s2geometry` (Python)
- Point-in-polygon: for `$geoWithin` with `$geometry` polygons
- Great-circle math: already implemented (`_haversine` in `geo.py`, reusable)

**Current stubs:**
- `$near` / `$nearSphere` in the Rust query compiler raise `"requires a 2dsphere index (not yet supported); use the $geoNear aggregation stage instead"`
- `$geoWithin` / `$geoIntersects` raise `"planned but not yet implemented; see WHATSNEXT.md"`
- `create_index([("location", "2dsphere")])` raises `"2dsphere indexes are planned; $geoNear aggregation works without an index"`

**Estimated effort:** 3-4 iterations. The S2 cell encoding and expanding-ring search algorithm are the non-trivial parts; the WiredTiger plumbing follows existing patterns mechanically.

---

## 3. Time Series Collections (PLANNED)

**Priority: MEDIUM | Impact: HIGH | Difficulty: HIGH**

MongoDB's time series collections are an optimization for append-heavy, time-ordered workloads (IoT sensors, metrics, logs). Documents sharing the same `metaField` value within a time window are packed into internal "bucket" documents, reducing per-document storage overhead and enabling columnar-style compression.

### How MongoDB does it

```
  Application inserts:
  ─────────────────────
  { ts: ISODate("2026-04-04T10:00:00Z"), sensor: "temp_01", value: 22.5 }
  { ts: ISODate("2026-04-04T10:01:00Z"), sensor: "temp_01", value: 22.7 }
  { ts: ISODate("2026-04-04T10:02:00Z"), sensor: "temp_01", value: 22.6 }

  Internal bucket (hidden system collection):
  ────────────────────────────────────────────
  {
    _id: ObjectId(...),
    meta: "temp_01",
    control: { min: { ts: ..., value: 22.5 }, max: { ts: ..., value: 22.7 }, count: 3 },
    data: {
      ts:    [ISODate("...T10:00"), ISODate("...T10:01"), ISODate("...T10:02")],
      value: [22.5, 22.7, 22.6]
    }
  }
```

### What to build

| Component | Description | Difficulty |
|---|---|---|
| `createCollection` options | `timeseries: {timeField, metaField, granularity}` | Low |
| Bucket creation | Group inserts by meta + time window into internal bucket docs | Medium |
| Insert routing | Transparently route `insert_one`/`insert_many` to bucket writer | Medium |
| Read translation | Transparently un-bucket so queries see individual documents | Medium-High |
| Clustered index | `(meta, timeField)` index for efficient range scans | Medium |
| Bucket compaction | Merge small buckets, respect `bucketMaxSpanSeconds` | High |
| Aggregation hints | Pipeline optimizer can push `$match` on time/meta into bucket scans | High |

### What smongo already provides

The **pattern** works today without engine-level time series support:

- `$bucket` / `$bucketAuto` -- time-window aggregation on any datetime field
- TTL indexes -- automatic document expiry by timestamp
- Compound indexes -- `[("sensor_id", 1), ("timestamp", -1)]` gives efficient time-range scans
- The `examples/patterns/iot_timeseries.py` demo runs 24h of sensor analytics with hourly aggregation, anomaly detection, and facility comparisons using these primitives

The time series collection type adds **storage efficiency** (columnar bucketing, delta compression) and **transparent insert routing** -- it's an optimization layer over capabilities that already work.

**Estimated effort:** High. The auto-bucketing engine and transparent read translation are the most complex parts. A pragmatic incremental path:

1. **Phase 1:** `createCollection` with `timeseries` options (metadata only, no bucketing -- documents stored individually with automatic compound index on `(metaField, timeField)`)
2. **Phase 2:** Internal bucket writer for insert routing (amortizes per-document overhead)
3. **Phase 3:** Transparent un-bucketing on read (queries see individual documents)
4. **Phase 4:** Bucket compaction and pipeline optimizer integration

---

## 4. `$graphLookup` (DONE)

**Priority: HIGH | Impact: HIGH | Difficulty: MEDIUM (was)**

Already shipped in Rust. BFS traversal with all MongoDB-spec fields.

```
 Input docs
      │
      ▼
 ┌────────────────────────────────────────────────────────┐
 │  $graphLookup                                          │
 │                                                        │
 │  For each input doc:                                   │
 │  1. Evaluate startWith expression                      │
 │  2. BFS over foreign collection:                       │
 │     - Hash-index foreign docs by connectToField        │
 │     - Expand frontier: lookup connectFromField values   │
 │     - Track visited set to prevent cycles              │
 │     - Apply restrictSearchWithMatch filter             │
 │     - Respect maxDepth                                 │
 │  3. Attach results array to "as" field                 │
 │  4. Optionally set depthField on each matched doc      │
 └────────────────────────────────────────────────────────┘
```

**What shipped:**
- `rust/src/aggregation_joins.rs` -- full BFS implementation
- All spec fields: `from`, `startWith`, `connectFromField`, `connectToField`, `as`, `maxDepth`, `depthField`, `restrictSearchWithMatch`
- Hash-indexed foreign collection for O(1) lookups per frontier expansion
- Cycle detection via visited set
- Cross-collection support via `collection_getter`

---

## Roadmap Summary

| # | Feature | Status | Impact | Difficulty | Dependencies |
|---|---|---|---|---|---|
| 1 | `$geoNear` aggregation stage | **DONE** | High | Low | None |
| 2 | `$graphLookup` | **DONE** | High | -- | None |
| 3 | `2dsphere` index + `$near` / `$nearSphere` | Stubbed | High | Medium-High | S2 geometry lib |
| 4 | `$geoWithin` / `$geoIntersects` | Stubbed | Medium | High | S2 + polygon math |
| 5 | Time series collections (Phase 1: metadata) | Planned | Medium | Low | None |
| 6 | Time series collections (Phase 2-4: bucketing) | Planned | High | High | Phase 1 |

---

## The Pattern

Every major feature in this roadmap follows the same architecture: **application-level logic on top of WiredTiger B-trees.**

WiredTiger doesn't know about S2 cells, GeoJSON, or time-bucketed columnar storage. It provides ordered key/value tables with ACID transactions and crash recovery. MongoDB builds geospatial, time series, and graph features as **pure application logic** over that primitive -- and so does smongo.

```
  ┌──────────────────────────────────────────────────────┐
  │               Feature Layer                           │
  │  $geoNear · 2dsphere · $graphLookup · time series     │
  ├──────────────────────────────────────────────────────┤
  │               Query / Aggregation Engine              │
  │  MQL compiler · pipeline stages · query planner        │
  ├──────────────────────────────────────────────────────┤
  │               Index Layer                              │
  │  btree · text · hashed · wildcard · (2dsphere)         │
  ├──────────────────────────────────────────────────────┤
  │               WiredTiger                               │
  │  B-Tree tables · ACID transactions · crash recovery    │
  └──────────────────────────────────────────────────────┘
```

The bet: one storage primitive, many feature layers, same query language everywhere.
