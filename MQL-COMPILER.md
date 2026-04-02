# MQL Compiler

**A pure-Python compiler that translates MongoDB query dictionaries into executable predicates, document mutators, and expression evaluators.**

---

## What MQL Is

MQL (MongoDB Query Language) is the JSON-based query language that every MongoDB application uses. It's how you say "find all users in NYC older than 30" or "increment the login count by 1" or "concatenate first and last name into a display name."

The language has three distinct subsystems:

1. **Query predicates** -- match documents: `{"city": "NYC", "age": {"$gt": 30}}`
2. **Update operators** -- mutate documents: `{"$set": {"status": "active"}, "$inc": {"logins": 1}}`
3. **Aggregation expressions** -- compute values: `{"$concat": ["$firstName", " ", "$lastName"]}`

smongo implements all three in the `query/` package. This package is the brain of the engine -- every read, write, aggregation, and sync operation flows through it. The package is split into focused modules: `compiler.py` (query predicates), `update.py` (document mutation), `expressions.py` (aggregation expressions), and `paths.py` (dot-notation traversal).

---

## The Query Compiler

### How It Works

`compile_query(query)` takes a MongoDB query document and returns a Python callable. That callable accepts a single document and returns `True` if it matches.

```python
fn = compile_query({"city": "NYC", "age": {"$gt": 30}})

fn({"city": "NYC", "age": 34})   # True
fn({"city": "NYC", "age": 25})   # False
fn({"city": "SF", "age": 40})    # False
```

Internally, the compiler creates a closure that walks each key-condition pair in the query. For each pair, it resolves the field value using dot-notation path traversal, then evaluates the condition:

- **Literal values** are compared with `==`
- **Operator dictionaries** dispatch to `_eval_op()` for each operator
- **Logical operators** (`$or`, `$and`, `$nor`) recursively compile sub-queries

### Supported Query Operators

#### Comparison

| Operator | Semantics |
|---|---|
| `$gt` | Greater than (null-safe: `None` never matches) |
| `$lt` | Less than (null-safe) |
| `$gte` | Greater than or equal (null-safe) |
| `$lte` | Less than or equal (null-safe) |
| `$eq` | Explicit equality |
| `$ne` | Not equal |
| `$in` | Value is in array (also handles array-to-array matching) |
| `$nin` | Value is not in array |

The null-safety design is deliberate: `{"age": {"$gt": 30}}` should not match a document without an `age` field. The compiler returns `False` when the left-hand value is `None` for all ordering operators.

#### Logical

| Operator | Semantics |
|---|---|
| `$or` | At least one sub-query matches |
| `$and` | All sub-queries match |
| `$nor` | No sub-query matches |
| `$not` | Inverts a nested operator expression |

Logical operators use recursive compilation. `$or` short-circuits on the first match. `$and` short-circuits on the first failure.

```python
# $or compiles each sub-query and tests them
if key == "$or":
    if not any(compile_query(sub)(doc) for sub in condition):
        return False
```

#### Element

| Operator | Semantics |
|---|---|
| `$exists` | Field is present (when `True`) or absent (when `False`) |
| `$type` | BSON type checking via `_TYPE_MAP` |

The `$type` operator maps BSON type names to Python types:

```python
_TYPE_MAP = {
    "double": (float,),
    "string": (str,),
    "object": (dict,),
    "array": (list,),
    "bool": (bool,),
    "int": (int,),
    "long": (int,),
    "null": (type(None),),
    "number": (int, float),
}
```

#### Array

| Operator | Semantics |
|---|---|
| `$all` | Array contains all specified elements |
| `$elemMatch` | At least one array element matches a sub-query |
| `$size` | Array has exactly N elements |

`$elemMatch` recursively compiles its sub-query and tests each array element. For non-dict array elements, it wraps them in `{"value": elem}` to enable operator matching.

`$in` has special array-aware behavior: when the document field is itself an array, it checks if *any* element of the document array is in the `$in` list:

```python
if op == "$in":
    if isinstance(value, list):
        return any(v in cond_val for v in value)
    return value in cond_val
```

#### String

| Operator | Semantics |
|---|---|
| `$regex` | Pattern match with optional flags |
| `$options` | Flags for `$regex`: `i` (case-insensitive), `m` (multiline), `s` (dotall) |

The `$options` operator is handled at the caller level, not inside `_eval_op`. When the compiler sees `$options` alongside `$regex`, it pre-computes the regex flags and passes them to all operator evaluations for that field:

```python
if "$options" in condition:
    opts = condition.get("$options", "")
    if "i" in opts: regex_flags |= re.IGNORECASE
    if "m" in opts: regex_flags |= re.MULTILINE
    if "s" in opts: regex_flags |= re.DOTALL
```

---

## Dot-Notation Path Traversal

Every field access in the query compiler goes through `get_value(doc, key)`. This function handles dot-notation paths, traversing nested documents and arrays:

```python
get_value({"address": {"city": "NYC"}}, "address.city")       # "NYC"
get_value({"scores": [90, 85, 92]}, "scores.1")               # 85
get_value({"users": [{"name": "Alice"}]}, "users.0.name")     # "Alice"
get_value({"x": 1}, "y.z")                                     # None
```

The implementation walks the dot-separated path segments:

```python
def get_value(doc, key):
    parts = key.split(".")
    val = doc
    for p in parts:
        if isinstance(val, dict):
            val = val.get(p)
        elif isinstance(val, list):
            try:
                val = val[int(p)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if val is None:
            return None
    return val
```

The complementary `set_value(doc, key, value)` creates intermediate dictionaries as needed:

```python
set_value({}, "address.city", "NYC")
# Result: {"address": {"city": "NYC"}}
```

And `unset_value(doc, key)` walks to the parent and deletes the final key.

---

## The Update Engine

`apply_update(doc, update_spec)` mutates a document in place according to MongoDB's update operators. It's called by every `update_one`, `update_many`, `find_one_and_update`, and bulk `UpdateOne` operation.

### Supported Update Operators

| Operator | Behavior | Example |
|---|---|---|
| `$set` | Set field value (dot-path) | `{"$set": {"status": "active"}}` |
| `$inc` | Atomic increment | `{"$inc": {"logins": 1}}` |
| `$mul` | Multiply | `{"$mul": {"price": 1.1}}` |
| `$min` | Conditional floor | `{"$min": {"low_score": 50}}` |
| `$max` | Conditional ceiling | `{"$max": {"high_score": 100}}` |
| `$unset` | Remove field | `{"$unset": {"temp_field": ""}}` |
| `$rename` | Atomic rename | `{"$rename": {"old_name": "new_name"}}` |
| `$push` | Append to array | `{"$push": {"tags": "new"}}` |
| `$addToSet` | Set-semantic append | `{"$addToSet": {"tags": "unique"}}` |
| `$pull` | Remove from array | `{"$pull": {"tags": "old"}}` |
| `$pop` | Remove first (-1) or last (1) | `{"$pop": {"queue": 1}}` |
| `$currentDate` | Set to now | `{"$currentDate": {"updatedAt": true}}` |

### Array Operator Details

**`$push` with `$each`**: Supports batch appending.

```python
{"$push": {"scores": {"$each": [90, 85, 92]}}}
```

**`$addToSet` with `$each`**: Only adds elements not already present.

```python
{"$addToSet": {"tags": {"$each": ["python", "mongodb"]}}}
```

**`$pull` with sub-query**: Removes array elements matching a query predicate. The sub-query is compiled using `compile_query`, so you can use any query operator:

```python
{"$pull": {"items": {"status": "expired"}}}
```

**`$currentDate`**: Two modes -- ISO string (default) or Unix timestamp:

```python
{"$currentDate": {"updatedAt": true}}                      # ISO string
{"$currentDate": {"updatedAt": {"$type": "timestamp"}}}    # Unix float
```

### Operator Composition

Multiple operators can be combined in a single update:

```python
apply_update(doc, {
    "$set": {"status": "active"},
    "$inc": {"version": 1},
    "$push": {"log": "activated"},
    "$currentDate": {"updatedAt": True},
})
```

Operators are applied in iteration order. This matches MongoDB's behavior where `$set` and `$inc` on different fields compose cleanly, but conflicting operators on the same field produce undefined results.

---

## The Expression Engine

`resolve_expr(doc, expr)` (in `query/expressions.py`) powers the aggregation framework. It evaluates a rich expression tree that appears in `$project`, `$addFields`, `$group`, and computed fields.

### Field References

```python
resolve_expr(doc, "$fieldName")    # → doc["fieldName"]
resolve_expr(doc, "$$ROOT")        # → entire document
resolve_expr(doc, "$$CURRENT")     # → entire document
resolve_expr(doc, "literal")       # → "literal" (plain string, no $ prefix)
```

### Expression Operators

#### Conditional

| Operator | Description |
|---|---|
| `$cond` | If/then/else (supports both dict and array forms) |
| `$ifNull` | Coalesce to fallback value when null |
| `$switch` | Multi-branch conditional with `branches` and `default` |

#### String

| Operator | Description |
|---|---|
| `$concat` | Concatenate strings (returns null if any input is null) |
| `$toUpper` | Convert to uppercase |
| `$toLower` | Convert to lowercase |
| `$substr` | Substring extraction |
| `$strLenCP` | String length in code points |

#### Array

| Operator | Description |
|---|---|
| `$arrayElemAt` | Access element by index (supports negative indexing) |
| `$size` | Array length |
| `$filter` | Filter array elements with a condition expression |
| `$concatArrays` | Concatenate multiple arrays |
| `$in` | Check if value exists in array |

#### Arithmetic

| Operator | Description |
|---|---|
| `$add` | Sum of values |
| `$subtract` | Difference of two values |
| `$multiply` | Product of values |
| `$divide` | Division (safe: returns null on zero divisor) |
| `$mod` | Modulo (safe: returns null on zero divisor) |
| `$abs` | Absolute value |
| `$ceil` | Ceiling |
| `$floor` | Floor |
| `$round` | Round to N decimal places |

#### Comparison (Expression Form)

| Operator | Description |
|---|---|
| `$eq` | Equal (two-arg array form, returns boolean) |
| `$ne` | Not equal |
| `$gt`, `$lt`, `$gte`, `$lte` | Ordering comparisons (null-safe) |

These are distinct from the query operators -- they take `[expr_a, expr_b]` arrays and return booleans, used inside `$cond` and `$filter` expressions.

#### Boolean

| Operator | Description |
|---|---|
| `$and` | All expressions truthy |
| `$or` | Any expression truthy |
| `$not` | Logical negation |

#### Type

| Operator | Description |
|---|---|
| `$type` | Returns BSON type name as string |
| `$literal` | Return the argument as-is (escapes `$`-prefixed strings) |

### Recursive Resolution

The expression engine is fully recursive. Nested expressions evaluate bottom-up:

```python
resolve_expr(doc, {
    "$cond": {
        "if": {"$gt": ["$age", 18]},
        "then": {"$concat": ["$name", " (adult)"]},
        "else": {"$concat": ["$name", " (minor)"]}
    }
})
```

The `$gt` resolves first, then its boolean result determines which `$concat` branch evaluates.

### Plain Dict Literals

When an expression is a dictionary that doesn't contain exactly one `$`-prefixed key, it's treated as a literal dict with recursive resolution of its values:

```python
resolve_expr(doc, {"city": "$address.city", "country": "$address.country"})
# → {"city": "NYC", "country": "US"}
```

This is how `$group` with compound `_id` works:

```python
{"$group": {"_id": {"city": "$city", "state": "$state"}, ...}}
```

---

## Design Philosophy

**Closure-based compilation**: `compile_query` returns a closure, not an AST. This means the "compilation" step is cheap (just creating the closure), and the matching step is a direct Python function call -- no interpreter overhead, no tree walking at match time.

**Null safety by default**: Ordering operators (`$gt`, `$lt`, etc.) always return `False` when the value is `None`. This prevents `TypeError` exceptions and matches MongoDB's behavior where missing fields don't participate in range comparisons.

**Graceful degradation**: Unknown expression operators return `None` instead of raising exceptions. Unknown update operators raise `NotImplementedError`. This asymmetry is intentional -- expressions in aggregation pipelines should degrade gracefully, while update operators that silently do nothing would corrupt data.
