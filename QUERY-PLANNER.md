# Query Planner

**How smongo decides the fastest way to answer every query.**

---

## The Problem

A naive embedded database scans every document in a collection for every query. That's O(n) per read and O(n) per write. For 10 documents it doesn't matter. For 10,000 it's sluggish. For 100,000 it's unusable.

MongoDB solves this with a query planner that evaluates candidate indexes and picks the cheapest execution path. smongo does the same thing — backed by **real B-tree indexes** in **`smongo-engine`** (persisted with **redb** on native targets), not in-memory hash maps.

---

## Three Execution Strategies

The planner chooses one of three strategies for every query:

```
┌─────────────────────────────────────────────────────────┐
│                     Query Arrives                         │
│                  {"city": "NYC", "age": {"$gt": 30}}     │
└────────────────────────┬────────────────────────────────┘
                         │
       ┌─────────────┬───┼───────────┬──────────────┐
       ▼             ▼   ▼           ▼              ▼
┌──────────┐  ┌──────────┐  ┌──────────┐   ┌──────────────┐
│ PK       │  │ Index    │  │ Or-Union │   │ Collection   │
│ Lookup   │  │ Scan     │  │ (branch  │   │ Scan         │
│ O(log n) │  │ O(log n) │  │  plans)  │   │ O(n)         │
└──────────┘  └──────────┘  └──────────┘   └──────────────┘
```

### 1. PK Lookup -- O(log n)

When the query includes a direct `_id` equality match like `{"_id": "abc123"}`, the planner bypasses all secondary indexes entirely. The **primary document table** is keyed by `_id`, so this is a single `cursor.search()` — one seek, one document back.

This is the same optimization that makes `findOne({_id: ...})` fast in production MongoDB.

```python
# The planner detects this pattern:
if "_id" in query and not isinstance(query["_id"], dict):
    return QueryPlan("pk_lookup")
```

The key insight: `{"_id": "abc123"}` triggers PK lookup, but `{"_id": {"$in": [...]}}` does not -- operator expressions on `_id` fall through to index scoring.

### 2. Index Scan -- O(log n + k)

When secondary indexes exist, the planner scores each one and picks the best. An index scan uses **`search_near()`** on the index cursor to find the starting position, then walks forward, collecting `_id` values until it exceeds the upper bound. Those IDs are then fetched from the primary table and re-filtered with the full query predicate.

```
Index B-Tree: city_1_age_-1

        search_near("NYC|30")
              │
              ▼
    ... | NYC,25 | NYC,30 | NYC,34 | NYC,42 | SF,28 | ...
                   ^^^^^^^^^^^^^^^^^^^^^^^^
                   cursor walks this range
                   collects _id values
```

### 3. Or-Union -- O(log n + k) per branch

When the query contains a top-level `$or`, the planner plans each branch independently. If **every** branch can be served by an index (or PK lookup), the result is an `or_union` plan that runs each sub-plan and merges the results with deduplication.

```python
coll.explain({"$or": [{"city": "NYC"}, {"city": "SF"}]})

# Returns (given a city_1 index):
{
    "plan": "or_union",
    "subplans": [
        {"plan": "index_scan", "index": "city_1"},
        {"plan": "index_scan", "index": "city_1"}
    ]
}
```

If **any** branch falls back to a collection scan, the entire query falls back to a single collection scan (running the disjunction is no faster than scanning once).

### 4. Collection Scan -- O(n)

The fallback. When no index matches the query, the planner scans every document and applies the compiled MQL predicate.

The planner falls back to collection scan when:
- The query is empty (`{}`)
- No indexes exist on queried fields
- Any `$or` branch can't use an index (the whole query falls back)
- All field conditions use unsupported operators for index acceleration

---

## Index Scoring: Longest Prefix Match

The planner evaluates every index against the query's field conditions using a **longest prefix match** algorithm. It walks the index's key fields in order and accumulates a score:

| Condition | Score | Why |
|---|---|---|
| Equality (`field: value`) | **+2** | Tight bound on both sides -- maximum selectivity |
| Range (`$gt`, `$lt`, `$gte`, `$lte`) | **+1** | Open or half-open bound -- good but less selective |
| `$in` | **+1** | Multi-point scan: one seek per value, then merge results |

The scoring walks the index key fields left to right and **stops at the first non-matchable field**. This mirrors the "index prefix" concept in MongoDB -- a compound index `{a: 1, b: 1, c: 1}` can accelerate `{a: 5, b: 10}` but not `{b: 10, c: 20}`.

```python
for field, direction in idx.keys:
    if field not in field_conditions:
        break  # prefix chain broken

    cond = field_conditions[field]
    if not isinstance(cond, dict):
        score += 2   # equality
    else:
        # range operators and $in score +1
        ...
```

The index with the highest score wins. Ties are broken implicitly by iteration order (first declared index).

### Scoring Examples

Given indexes: `city_1`, `city_1_age_-1`, `age_1`

| Query | Best Index | Score | Plan |
|---|---|---|---|
| `{"city": "NYC"}` | `city_1` | 2 | Index scan |
| `{"city": "NYC", "age": {"$gt": 30}}` | `city_1_age_-1` | 3 (2+1) | Index scan |
| `{"age": {"$gt": 30}}` | `age_1` | 1 | Index scan |
| `{"name": "Alice"}` | (none) | 0 | Collection scan |
| `{"_id": "abc123"}` | (bypassed) | -- | PK lookup |

---

## Bound Computation

Once the winning index is chosen, the planner computes **encoded** lower and upper bound keys for the storage layer. This is where the real complexity lives.

### Ascending Fields

For ascending indexes, the mapping is direct:
- `$gt: 30` → lower bound = `encode(30)`, exclusive
- `$gte: 30` → lower bound = `encode(30)`, inclusive
- `$lt: 50` → upper bound = `encode(50)`, exclusive
- `$lte: 50` → upper bound = `encode(50)`, inclusive

### Descending Fields

Descending indexes require **inverted encoding and swapped bounds**. When a field has direction `-1`, the encoded key is run through a hex digit inversion (`0↔f, 1↔e, ...`), which reverses the lexicographic ordering in the B-Tree. The planner also swaps the lower/upper bounds:

```python
if direction == -1:
    # Descending: invert encoding and swap bounds
    enc_low = _invert_encoded(_sortable_encode(high_val))   # high becomes low
    enc_high = _invert_encoded(_sortable_encode(low_val))    # low becomes high
    lower_segments.append((enc_low, high_inc))
    upper_segments.append((enc_high, low_inc))
```

This ensures that a query like `{"age": {"$gt": 30}}` on an index `{age: -1}` correctly scans from the largest age downward to 30.

### Sentinel Values

When a bound is unbounded (e.g., `$gt: 30` with no upper limit), the planner uses sentinel values:
- Lower sentinel: `""` (empty string, sorts before everything)
- Upper sentinel: `"\xff" * 20` (sorts after everything in the B-Tree)

---

## The Cursor Walk

`execute_index_scan()` translates the computed bounds into **storage cursor** operations:

```
1. Open cursor on the index table
2. search_near(lower_bound_key)
3. If cursor landed before bound, advance with next()
4. Walk forward, collecting _id values from each entry
5. Stop when key > upper_bound_key
6. Close cursor
7. Fetch full documents by _id from the primary table
8. Re-filter with compiled MQL predicate
```

Step 8 is important: the index provides **acceleration**, not **full pushdown**. The index narrows the candidate set, but the MQL predicate is always applied to ensure correctness for conditions the index doesn't fully cover.

---

## Streaming Reads Use the Planner

The **streaming cursor** (Rust-backed on hot paths) — the lazy iterator behind `Collection.find()`, `find_one()`, and `count_documents()` — consults the query planner. Iteration branches on `plan.plan_type`:

| Plan Type | Streaming cursor behavior |
|---|---|
| **`pk_lookup`** | Single `cursor.search()`, yield 0 or 1 doc |
| **`index_scan`** | Walk index B-tree, look up each doc by `_id` one at a time, yield those passing the MQL filter |
| **`$in` scan** | Multi-point seek on the index (one seek per `$in` value), yield per-doc |
| **`or_union`** | Execute each subplan, deduplicate candidate `_id`s, yield per-doc |
| **`collection_scan`** | `cursor.next()` loop with per-doc filter and yield |

Because the streaming cursor *yields* rather than *collects*, callers that stop early (`.limit(10)`, `find_one()`) avoid touching the remaining documents. A **read transaction** (or snapshot) covers the iteration and ends when the cursor is exhausted or dropped.

This means `find({}).limit(10)` on a million-document collection deserializes exactly 10 BSON documents. `find_one()` deserializes exactly 1.

---

## Writes Use the Planner Too

The query planner doesn't just accelerate reads. **Every update and delete operation** routes through `_find_matching_docs()`, which calls the planner to find matching documents. This means:

- `update({"_id": "abc"}, {"$set": {"x": 1}})` → PK lookup (O(log n))
- `update({"city": "NYC"}, {"$set": {"x": 1}})` with `city_1` index → index scan (O(log n + k))
- `delete({"status": "expired"})` with no index → collection scan (O(n))

In production MongoDB, updates by `_id` are always fast because of the primary index. smongo preserves this guarantee.

---

## Lexicographic Key Encoding

Index B-trees compare keys as **byte strings**. To get correct ordering, MongoDB's type-aware comparison semantics must map cleanly onto lexicographic byte ordering. The solution is a type-prefix encoding scheme:

```
Type Prefix Hierarchy:
    "00"              → None (sorts lowest)
    "1" + IEEE 754    → Numbers
    "15" + hex        → ObjectId
    "2" + UTF-8 hex   → Strings
    "30" / "31"       → Boolean false / true
```

### Numbers: IEEE 754 Bit Manipulation

The hardest part. Floating-point binary representation doesn't sort lexicographically because of the sign bit. The encoder performs a bit manipulation that transforms IEEE 754 doubles into lexicographically sortable sequences:

```python
packed = struct.pack(">d", float(value))
b = bytearray(packed)
if b[0] & 0x80:           # negative: invert ALL bits
    b = bytearray(~x & 0xFF for x in b)
else:                       # non-negative: flip only the sign bit
    b[0] ^= 0x80
return "1" + b.hex()
```

After this transformation:
- `-100.0` < `-1.0` < `0.0` < `1.0` < `100.0` in lexicographic order
- Integers and floats interleave correctly
- `NaN`, `Inf`, `-Inf` sort deterministically

### Compound Keys

Multi-field indexes concatenate encoded values with pipe separators, appending the `_id` as a tiebreaker:

```
encoded_field_1 | encoded_field_2 | ... | _id

Example: index {city: 1, age: -1}, doc {_id: "abc", city: "NYC", age: 30}
→ "2[hex(NYC)]|[inverted_encode(30)]|abc"
```

The `_id` suffix ensures uniqueness even when multiple documents share the same indexed field values.

---

## What the Explain Output Looks Like

```python
coll.explain({"city": "NYC", "age": {"$gt": 30}})

# Returns:
{
    "plan": "index_scan",
    "index": "city_1_age_-1",
    "indexBounds": {"lower": "...", "upper": "..."},
    "rejectedPlans": [
        {"plan": "index_scan", "index": "city_1", "score": 2}
    ],
    "filterResidual": ["age"]
}
```

```python
coll.explain({"_id": "abc123"})

# Returns:
{
    "plan": "pk_lookup"
}
```

```python
# With execute=True, returns execution statistics:
coll.explain({"city": "NYC"}, execute=True)

# Returns:
{
    "plan": "index_scan",
    "index": "city_1",
    "executionStats": {
        "nReturned": 42,
        "executionTimeMillis": 3
    }
}
```

```python
coll.explain({"$or": [{"city": "NYC"}, {"city": "SF"}]})

# Returns (given a city_1 index):
{
    "plan": "or_union",
    "subplans": [
        {"plan": "index_scan", "index": "city_1"},
        {"plan": "index_scan", "index": "city_1"}
    ]
}
```

```python
coll.explain({"$or": [{"city": "NYC"}, {"unindexed_field": 42}]})

# Returns (one branch can't use an index → full fallback):
{
    "plan": "collection_scan"
}
```

---

## Design Decisions

**Why re-filter after index scan?** The index narrows the candidate set but doesn't guarantee full predicate satisfaction. A compound index `{city: 1, age: -1}` accelerates `{"city": "NYC", "age": {"$gt": 30}, "status": "active"}`, but the `status` condition isn't in the index. Re-filtering catches these residual conditions.

**Why does `$or` fall back to collection scan when any branch is unindexed?** If even one branch requires a full scan, running the other branches through indexes and then scanning for the remaining branch is no faster than a single scan that evaluates the entire `$or` predicate. The planner takes the simple, correct path: all branches indexed means `or_union`, otherwise `collection_scan`.

**Why score equality at +2 and range at +1?** Equality conditions produce tight two-sided bounds (lower = upper), maximizing selectivity. Range conditions only bound one side, so they filter fewer documents. Doubling the score for equality ensures the planner prefers indexes that match equality conditions first.
