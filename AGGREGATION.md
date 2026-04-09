# Aggregation Pipeline

**27 stages. 17 group accumulators. Vector search. Faceted queries. Materialized views. All running locally in `smongo-engine` (pure Rust) on redb.**

---

## How the Pipeline Works

The aggregation pipeline is a chain of transformation stages. Documents flow in from a collection, pass through each stage in order, and emerge as results. Each stage transforms the document stream -- filtering, reshaping, grouping, sorting, joining, or writing.

```
Input documents
    │
    ▼
┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
│ $match  │──►│ $group  │──►│ $sort   │──►│ $limit  │──► Results
└─────────┘   └─────────┘   └─────────┘   └─────────┘
```

All 27 stages run in **`smongo-engine`** (pure Rust) with a single FFI crossing per pipeline. Streaming stages (`$match`, `$project`, `$limit`, `$skip`, `$unwind`, `$addFields`, `$unset`, `$replaceRoot`, `$redact`) use lazy iterator adapters with constant memory overhead. Blocking stages (`$sort`, `$group`, `$bucket`, `$facet`, `$setWindowFields`) materialize at their boundary only. See [ZERO-FFI-STATUS.md](ZERO-FFI-STATUS.md) for the full architecture.

The Python `smongo/aggregation/` package serves as the reference specification and provides the user-facing `Cursor` class; at runtime, `Collection.aggregate()` delegates to the Rust engine via `aggregate_engine()` / `aggregate_pipeline()`.

---

## The Cursor

Results are delivered through a `Cursor` object that supports PyMongo-style chaining:

```python
cursor = coll.find({"status": "active"})
    .sort([("age", -1)])
    .skip(10)
    .limit(5)
    .projection({"name": 1, "age": 1})
```

Modifiers are **deferred** -- they're recorded on the cursor and applied together when you iterate. This means the order of chaining doesn't matter:

```python
# These produce the same result:
cursor.sort("age", -1).limit(5)
cursor.limit(5).sort("age", -1)
```

Internally, `_resolve()` applies them in the correct order: sort → skip → limit → projection.

**Lazy input**: The cursor accepts any `Iterable[Document]` — including engine-backed iterators that pull documents **one BSON decode at a time**. When `.sort()` is applied, the source is fully materialized (sorting requires the full set). When only `.skip()` and `.limit()` are applied **without** sorting, the cursor uses `itertools.islice` to consume only the required slice from the underlying iterator. This means `find({}).limit(10)` on a large collection deserializes only 10 BSON documents.

The cursor also serves as the aggregation entry point. At runtime, `aggregate()` delegates to the Rust engine (`aggregate_engine()` / `aggregate_pipeline()`), which processes stages with streaming iterators where possible. The Python `Cursor` wrapper receives the final results.

---

## Stage Reference

### $match -- Filter Documents

Compiles the filter into an MQL predicate using `compile_query` and removes non-matching documents. Supports the full query operator set.

```python
{"$match": {"status": "active", "age": {"$gte": 18}}}
```

Place `$match` as early as possible in the pipeline. It reduces the document count for all downstream stages.

### $group -- Aggregate by Key

Groups documents by a key expression and computes accumulators over each group.

```python
{"$group": {
    "_id": "$department",
    "total_salary": {"$sum": "$salary"},
    "avg_age": {"$avg": "$age"},
    "headcount": {"$sum": 1},
    "names": {"$push": "$name"},
    "unique_cities": {"$addToSet": "$city"},
    "highest_paid": {"$max": "$salary"},
    "first_hired": {"$first": "$hire_date"},
}}
```

The `_id` field is the group key. It can be:
- A field reference: `"$department"`
- A compound expression: `{"dept": "$department", "level": "$level"}`
- A literal value: `null` (groups everything into one bucket)

#### Accumulators

| Accumulator | Description |
|---|---|
| `$sum` | Sum of values. `$sum: 1` counts documents. |
| `$avg` | Arithmetic mean (skips null values) |
| `$min` | Minimum value |
| `$max` | Maximum value |
| `$push` | Collect all values into an array |
| `$addToSet` | Collect unique values into an array |
| `$first` / `$last` | First / last value in the group (input order) |
| `$firstN` / `$lastN` | First / last N values in the group |
| `$stdDevPop` / `$stdDevSamp` | Population / sample standard deviation |
| `$mergeObjects` | Merge documents in the group into one |
| `$top` / `$bottom` | Top / bottom document by a sort key |
| `$topN` / `$bottomN` | Top / bottom N documents by a sort key |

The group key is resolved via `resolve_expr`, so it supports the full expression language -- `$cond`, `$concat`, arithmetic, etc.

When the group key is a dict or list, the engine serializes it for use as a hash key and deserializes back in the output, ensuring compound keys work correctly.

### $project -- Reshape Documents

Include, exclude, or compute fields:

```python
{"$project": {
    "name": 1,                              # include
    "internal_id": 0,                        # exclude
    "display": {"$concat": ["$first", " ", "$last"]},  # compute
    "age_bucket": {
        "$cond": {
            "if": {"$gte": ["$age", 18]},
            "then": "adult",
            "else": "minor"
        }
    }
}}
```

Inclusion (`1`/`True`), exclusion (`0`/`False`), and computed expressions can be mixed. Computed fields use `resolve_expr` for the full expression language.

### $sort -- Order Documents

Multi-key stable sort with null-aware ordering:

```python
{"$sort": {"age": -1, "name": 1}}
```

The sort is null-aware: documents with `None` for a sort field sort **after** documents with values (when ascending) or **before** (when descending). Multi-key sorts produce the correct composite ordering. The sort runs in the Rust engine with BSON-aware comparison semantics.

### $limit / $skip -- Result Windowing

```python
{"$skip": 20}
{"$limit": 10}
```

Standard list slicing. `$skip` removes the first N documents. `$limit` keeps only the first N.

### $unwind -- Array Expansion

Deconstructs an array field, producing one output document per array element:

```python
# Input: {"_id": 1, "tags": ["a", "b", "c"]}

{"$unwind": "$tags"}

# Output:
# {"_id": 1, "tags": "a"}
# {"_id": 1, "tags": "b"}
# {"_id": 1, "tags": "c"}
```

Supports the extended form with `preserveNullAndEmptyArrays`:

```python
{"$unwind": {
    "path": "$tags",
    "preserveNullAndEmptyArrays": true
}}
```

When `preserveNullAndEmptyArrays` is `true`:
- Documents with an empty array get a single output with the field set to `null`
- Documents missing the field are preserved
- Documents with a non-array value are preserved

When `false` (default), documents with missing or empty arrays are dropped entirely.

### $addFields / $set -- Inject Computed Fields

Adds new fields to each document without removing existing ones. `$set` is an alias.

```python
{"$addFields": {
    "fullName": {"$concat": ["$firstName", " ", "$lastName"]},
    "ageNextYear": {"$add": ["$age", 1]}
}}
```

The engine copies each document before modification to prevent mutation of upstream documents.

### $count -- Count Documents

Replaces the entire document stream with a single document containing the count:

```python
{"$count": "total"}
# Output: [{"total": 42}]
```

### $replaceRoot -- Promote Sub-Document

Replaces each document with one of its sub-documents:

```python
{"$replaceRoot": {"newRoot": "$address"}}

# Input:  {"_id": 1, "name": "Alice", "address": {"city": "NYC", "zip": "10001"}}
# Output: {"city": "NYC", "zip": "10001"}
```

Documents where `newRoot` resolves to a non-dict value are silently dropped.

### $lookup -- Left Outer Join

Joins documents from another collection:

```python
{"$lookup": {
    "from": "departments",
    "localField": "dept_id",
    "foreignField": "_id",
    "as": "department"
}}
```

The engine builds a hash index on the foreign collection's `foreignField` values for O(1) lookups per document, then performs the join. Cross-collection access uses the `DatabaseContext` provided to the engine — no Python callbacks are needed.

### $sample -- Random Sampling

Returns a random subset of documents:

```python
{"$sample": {"size": 5}}
```

If `size` exceeds the document count, returns all documents.

---

## Advanced Stages

### $vectorSearch -- Semantic Similarity

In-memory vector similarity search with optional pre-filtering:

```python
{"$vectorSearch": {
    "path": "embedding",
    "queryVector": [0.1, 0.2, 0.3, ...],
    "limit": 10,
    "metric": "cosine",
    "filter": {"category": "tech"},
    "scoreField": "_score"
}}
```

| Parameter | Description |
|---|---|
| `path` | Dot-path to the vector field |
| `queryVector` | The query vector (list of floats) |
| `limit` | Maximum results (default 10) |
| `numCandidates` | Pre-limit candidate count for ANN |
| `metric` | `"cosine"` (default) or `"euclidean"` |
| `filter` | Optional MQL pre-filter |
| `scoreField` | Output field name for similarity score (default `"_vectorScore"`) |

**Execution flow**:

1. Apply MQL `filter` (if present) to narrow candidates
2. Extract vectors from the `path` field, skip docs with wrong dimensionality
3. Run similarity search using the configured backend
4. Inject similarity score into each result document

**Two backends**:

- **USearch** (`usearch`): Fast approximate nearest neighbor (ANN) search. Used when installed.
- **NumPy**: Brute-force exact search. The fallback when USearch isn't available.

Cosine similarity computation (NumPy path):

```python
qnorm = np.linalg.norm(query_vec)
vnorm = np.linalg.norm(vectors, axis=1)
scores = (vectors @ query_vec) / (vnorm * qnorm)
order = np.argsort(-scores)[:limit]
```

### $facet -- Parallel Sub-Pipelines

Runs multiple independent pipelines against the same input documents. Returns a single document where each key is a facet name and the value is that sub-pipeline's results.

```python
{"$facet": {
    "by_department": [
        {"$group": {"_id": "$dept", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ],
    "top_earners": [
        {"$sort": {"salary": -1}},
        {"$limit": 5},
        {"$project": {"name": 1, "salary": 1}}
    ],
    "age_stats": [
        {"$group": {"_id": null, "avg": {"$avg": "$age"}, "max": {"$max": "$age"}}}
    ]
}}
```

Output is always a single document:

```json
[{
    "by_department": [{"_id": "Engineering", "count": 42}, ...],
    "top_earners": [{"name": "Alice", "salary": 200000}, ...],
    "age_stats": [{"_id": null, "avg": 34.5, "max": 62}]
}]
```

Each sub-pipeline gets a copy of the input documents, so mutations in one facet don't affect others. Sub-pipelines can use `$lookup` and other collection-aware stages via the engine's `DatabaseContext`.

### $out -- Write Results to Collection

Terminal stage. Replaces the target collection's contents with the pipeline results.

```python
{"$out": "monthly_report"}
```

Execution:

1. Delete all documents from the target collection
2. Insert all pipeline result documents
3. Return the result documents (so they're still available to the caller)

Must be the last stage in the pipeline. Requires a `DatabaseContext` (provided automatically when using `Collection.aggregate()`).

### $merge -- Upsert into Collection

Terminal stage. Upserts pipeline results into a target collection with configurable merge behavior.

```python
{"$merge": {
    "into": "dept_stats",
    "on": "_id",
    "whenMatched": "replace",
    "whenNotMatched": "insert"
}}
```

| Option | Values | Default |
|---|---|---|
| `into` | Collection name (string or `{"coll": ...}`) | Required |
| `on` | Match field(s) for upsert | `"_id"` |
| `whenMatched` | `"replace"`, `"merge"` | `"replace"` |
| `whenNotMatched` | `"insert"` | `"insert"` |

For each pipeline result document:

1. Query the target collection using the `on` field(s)
2. If a match exists:
   - `"replace"`: overwrite all fields except `_id`
   - `"merge"`: `$set` the new fields into the existing document
3. If no match and `whenNotMatched` is `"insert"`: insert the document

This enables materialized views and incremental ETL patterns:

```python
# Build and maintain a summary table
coll.aggregate([
    {"$group": {"_id": "$region", "total": {"$sum": "$revenue"}}},
    {"$merge": {"into": "revenue_by_region", "on": "_id", "whenMatched": "replace"}}
])
```

---

## Projection

The projection system supports three modes, applied both in the Cursor's `.projection()` method and in `$project` stage results:

### Inclusion Mode

When any field is set to `1` or `True`, only those fields appear in the output. `_id` is included by default unless explicitly excluded:

```python
{"name": 1, "age": 1}
# Input:  {"_id": 1, "name": "Alice", "age": 30, "email": "alice@..."}
# Output: {"_id": 1, "name": "Alice", "age": 30}
```

### Exclusion Mode

When fields are set to `0` or `False`, those fields are removed and everything else is kept:

```python
{"password": 0, "internal_notes": 0}
```

### Expression Mode

In `$project`, fields can be set to expression objects:

```python
{"full_name": {"$concat": ["$first", " ", "$last"]}, "age": 1}
```

---

## Expression Engine Integration

Every stage that computes values -- `$group`, `$project`, `$addFields`, `$sort`, `$replaceRoot`, `$unwind`, `$lookup` -- flows through the engine's expression evaluator (Rust, with the Python reference in `query/expressions.py`). This means the full expression language is available everywhere:

- Conditionals: `$cond`, `$ifNull`, `$switch`
- String operations: `$concat`, `$toUpper`, `$toLower`, `$substr`
- Array operations: `$arrayElemAt`, `$size`, `$filter`, `$concatArrays`
- Arithmetic: `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`
- Comparisons: `$eq`, `$ne`, `$gt`, `$lt`, `$gte`, `$lte`
- Boolean: `$and`, `$or`, `$not`
- Type: `$type`, `$literal`

---

## Pipeline Composition Examples

### Analytics: Revenue by Region

```python
coll.aggregate([
    {"$match": {"status": "completed"}},
    {"$group": {"_id": "$region", "revenue": {"$sum": "$total"}}},
    {"$sort": {"revenue": -1}},
    {"$limit": 10}
])
```

### Denormalization: Join and Flatten

```python
coll.aggregate([
    {"$lookup": {
        "from": "departments",
        "localField": "dept_id",
        "foreignField": "_id",
        "as": "dept"
    }},
    {"$unwind": "$dept"},
    {"$addFields": {"department_name": "$dept.name"}},
    {"$project": {"dept": 0}}
])
```

### Semantic Search with Pre-Filter

```python
coll.aggregate([
    {"$vectorSearch": {
        "path": "embedding",
        "queryVector": query_embedding,
        "limit": 5,
        "metric": "cosine",
        "filter": {"published": True},
        "scoreField": "relevance"
    }},
    {"$project": {"title": 1, "summary": 1, "relevance": 1}}
])
```

### Multi-Dimensional Analytics with $facet

```python
coll.aggregate([
    {"$match": {"year": 2024}},
    {"$facet": {
        "by_quarter": [
            {"$group": {"_id": "$quarter", "total": {"$sum": "$amount"}}},
            {"$sort": {"_id": 1}}
        ],
        "by_category": [
            {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
            {"$sort": {"total": -1}},
            {"$limit": 5}
        ],
        "summary": [
            {"$group": {"_id": None, "total": {"$sum": "$amount"}, "count": {"$sum": 1}}}
        ]
    }}
])
```

### Materialized View with $merge

```python
coll.aggregate([
    {"$group": {
        "_id": {"product": "$product_id", "month": "$month"},
        "units_sold": {"$sum": "$quantity"},
        "revenue": {"$sum": "$total"}
    }},
    {"$merge": {
        "into": "product_monthly_stats",
        "on": "_id",
        "whenMatched": "replace",
        "whenNotMatched": "insert"
    }}
])
```

---

## Additional Stages

The following stages are fully implemented and available in the aggregation pipeline.

### $bucket -- Fixed-Range Histograms

Groups documents into buckets defined by boundary values.

```python
coll.aggregate([
    {"$bucket": {
        "groupBy": "$price",
        "boundaries": [0, 25, 50, 100, 200],
        "default": "Other",
        "output": {"count": {"$sum": 1}, "avg_price": {"$avg": "$price"}}
    }}
])
# Returns: [{"_id": 0, "count": 5, "avg_price": 15.0}, {"_id": 25, "count": 3, ...}, ...]
```

### $bucketAuto -- Auto-Computed Histograms

Automatically divides documents into a specified number of equal-range buckets.

```python
coll.aggregate([
    {"$bucketAuto": {
        "groupBy": "$score",
        "buckets": 4,
        "output": {"count": {"$sum": 1}}
    }}
])
```

### $graphLookup -- Recursive Graph Traversal

Performs a recursive search on a collection following a specified edge relationship.

```python
coll.aggregate([
    {"$graphLookup": {
        "from": "employees",
        "startWith": "$manager_id",
        "connectFromField": "manager_id",
        "connectToField": "_id",
        "as": "reporting_chain",
        "maxDepth": 5
    }}
])
```

### $unionWith -- Union Multiple Collections

Combines documents from another collection into the pipeline, similar to SQL `UNION ALL`.

```python
coll.aggregate([
    {"$match": {"status": "active"}},
    {"$unionWith": {
        "coll": "archived_orders",
        "pipeline": [{"$match": {"status": "active"}}]
    }}
])
```

### $redact -- Field-Level Access Control

Restricts document content based on field-level conditions. Each sub-document is evaluated and either kept (`$$DESCEND`), pruned (`$$PRUNE`), or preserved (`$$KEEP`).

```python
coll.aggregate([
    {"$redact": {
        "$cond": {
            "if": {"$eq": ["$access_level", "public"]},
            "then": "$$DESCEND",
            "else": "$$PRUNE"
        }
    }}
])
```

### $sortByCount -- Group and Sort by Frequency

Equivalent to `$group` + `$sort`: groups by a given expression and sorts by count descending.

```python
coll.aggregate([
    {"$sortByCount": "$category"}
])
# Returns: [{"_id": "electronics", "count": 42}, {"_id": "books", "count": 31}, ...]
```

### $setWindowFields -- Window Functions

Applies window functions (running totals, rank, moving averages) over a sorted partition of documents.

```python
coll.aggregate([
    {"$setWindowFields": {
        "partitionBy": "$department",
        "sortBy": {"salary": -1},
        "output": {
            "rank": {"$rank": {}},
            "running_total": {
                "$sum": "$salary",
                "window": {"documents": ["unbounded", "current"]}
            }
        }
    }}
])
```

### $unset -- Remove Fields

Removes one or more fields from documents. Shorthand for `$project` with exclusion.

```python
coll.aggregate([
    {"$unset": ["internal_notes", "debug_info"]}
])
```

### $replaceWith -- Replace Document Root

Alias for `$replaceRoot`. Promotes an expression to become the entire document.

```python
coll.aggregate([
    {"$replaceWith": "$embedded_profile"}
])
```
