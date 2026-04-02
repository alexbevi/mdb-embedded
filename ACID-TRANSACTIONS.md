# ACID Transactions

**How smongo guarantees that every write is atomic, consistent, isolated, and durable -- using the same engine that powers production MongoDB.**

---

## The Guarantee

Every write operation in smongo -- insert, update, delete, index create, index drop -- is wrapped in a **WiredTiger transaction**. Within that transaction, the engine modifies the data table, all affected index tables, and the oplog as a single atomic unit. If any step fails, the entire transaction rolls back. No partial writes. No orphaned index entries. No phantom oplog records.

This is the same transactional model that production MongoDB uses internally for single-document writes. The difference is that smongo makes it explicit and visible.

---

## The Write Path

```
Application calls insert_one(doc)
    │
    ▼
┌──────────────────────────────────┐
│  1. Acquire per-collection lock  │  ← ReadWriteLock + threading.Lock
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│  2. Validate document            │  ← $jsonSchema enforcement
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│  3. Begin WiredTiger transaction │  ← session.begin_transaction()
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│  4. Maintain indexes             │  ← add_doc / update_doc / remove_doc
│     (may raise DuplicateKeyError)│     on every index in the collection
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│  5. Write BSON to data table     │  ← cursor[_id] = bson.encode(doc)
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│  6. Bump document version        │  ← monotonic counter for conflict detection
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│  7. Append oplog entry           │  ← timestamped, checksummed, with changed_fields
└──────────────┬───────────────────┘
               ▼
        ┌──────┴──────┐
        │  Success?   │
        └──────┬──────┘
          yes  │  no
          ▼    ▼
  ┌────────┐  ┌──────────────┐
  │ COMMIT │  │   ROLLBACK   │
  │        │  │              │
  │ All    │  │ Data table   │
  │ changes│  │ Index tables │
  │ durable│  │ Oplog        │
  └────────┘  │ ALL reverted │
              └──────────────┘
```

---

## What "Atomic" Means Here

A single `insert_one` call touches up to **N + 2 WiredTiger tables** (1 data table, N index tables, 1 oplog table). All of these writes happen inside one transaction:

```python
def _with_transaction(self, fn):
    self.session.begin_transaction()
    try:
        result = fn()
        self.session.commit_transaction()
        return result
    except Exception:
        try:
            self.session.rollback_transaction()
        except _WTError:
            pass
        raise
```

If a unique index rejects a duplicate key on the third of five indexes, the data table write is rolled back, the first two index entries are rolled back, and the oplog entry is never written. The collection is left in exactly the state it was before the call.

### Concrete Example: Update Rollback

```python
coll.create_index("email", unique=True)
coll.insert_one({"_id": "1", "email": "alice@example.com", "name": "Alice"})
coll.insert_one({"_id": "2", "email": "bob@example.com", "name": "Bob"})

# This update would create a duplicate email:
coll.update({"_id": "2"}, {"$set": {"email": "alice@example.com"}})
# → DuplicateKeyError raised
# → Transaction rolled back
# → Bob's document is unchanged
# → No oplog entry written
# → No index entry modified
```

---

## Thread Safety: Per-Collection Locking

Every `LocalCollection` uses a two-tier locking model: a `ReadWriteLock` that allows concurrent readers while serializing writers, and a `threading.Lock` for session-level cursor operations:

```python
class LocalCollection:
    def __init__(self, ...):
        self._lock = threading.Lock()
        self._rwlock = ReadWriteLock()
        ...

    def insert_one(self, doc, *, _internal=False):
        # ... validation happens outside the lock ...

        with self._rwlock.acquire_write():
            with self._lock:
                self._with_transaction(_do)

        return self.Result(1, [doc["_id"]])
```

The `ReadWriteLock` allows multiple concurrent `find()` calls (readers) to proceed in parallel, while write operations (`insert`, `update`, `delete`, `find_one_and_*`) acquire exclusive access. The inner `threading.Lock` serializes WiredTiger session cursor operations. This is sufficient for the embedded use case where write contention is low.

`LocalDB` also holds a separate lock protecting its `_collections` dictionary, so concurrent `get_collection()` calls don't race.

### What Runs Outside the Lock

Schema validation (`_validate(doc)`) runs **before** acquiring the lock and starting the transaction. This is a deliberate design choice:

1. Validation is a read-only operation -- it doesn't touch WiredTiger
2. Running it outside the lock reduces contention
3. If validation fails, we avoid the overhead of starting and rolling back a transaction

For updates, document matching (`_find_matching_docs(query)`) also runs outside the transaction. The matching phase reads the current state, then the transaction phase applies the mutation. This means a concurrent insert between matching and mutation could theoretically cause the update to miss a newly-inserted document, but this is consistent with MongoDB's behavior for multi-document operations.

---

## WiredTiger Transactions Under the Hood

WiredTiger provides MVCC (Multi-Version Concurrency Control) with snapshot isolation. When `begin_transaction()` is called, the session gets a consistent view of the data at that point in time. Writes are buffered in memory and only become visible to other sessions after `commit_transaction()`.

### What WiredTiger's Transactions Give Us

| Property | How |
|---|---|
| **Atomicity** | `commit_transaction()` makes all writes visible at once; `rollback_transaction()` discards them all |
| **Consistency** | Unique index checks happen within the transaction; violations trigger rollback |
| **Isolation** | Snapshot isolation prevents dirty reads between concurrent sessions |
| **Durability** | WiredTiger's checkpoint mechanism flushes committed data to disk |

### Crash Recovery

WiredTiger maintains a write-ahead log (WAL) and periodic checkpoints. If the process crashes:

1. On restart, `wiredtiger_open()` replays the WAL from the last checkpoint
2. Committed transactions are recovered; uncommitted transactions are discarded
3. The database is consistent without any application-level recovery logic

This is the same crash recovery mechanism that production MongoDB relies on.

---

## BSON Storage

Documents are stored as **native BSON bytes**, not JSON strings. This is a critical design decision that affects both correctness and performance:

```python
def _to_bson(doc):
    """Normalize a document for BSON encoding and return raw bytes."""
    def _normalize(v):
        if isinstance(v, dict):
            return {k: _normalize(val) for k, val in v.items()}
        if isinstance(v, list):
            return [_normalize(item) for item in v]
        if isinstance(v, (str, int, float, bool, type(None), bytes)):
            return v
        return str(v)
    return _bson_encode(_normalize(doc))


def _from_bson(raw):
    """Decode raw BSON bytes into a plain dict."""
    doc = _bson_decode(raw)
    return dict(doc) if isinstance(doc, SON) else doc
```

### Why BSON, Not JSON

| Aspect | BSON | JSON |
|---|---|---|
| **Type fidelity** | int32, int64, double, ObjectId, datetime, binary | Everything is string or number |
| **Encoding speed** | C-optimized codec via PyMongo | Python's json module |
| **Size** | Compact binary format | Text with quotes and escaping |
| **Compatibility** | Exact MongoDB on-disk format | Lossy (no ObjectId, no datetime) |

The WiredTiger table is configured with `value_format=u` (raw bytes) for the data table, so BSON documents are stored without any re-encoding.

---

## Document Versioning

Every document has a monotonic version counter maintained in memory:

```python
def _bump_version(self, doc_id):
    v = self._doc_versions.get(doc_id, 0) + 1
    self._doc_versions[doc_id] = v
    return v
```

The version is written to the oplog with every mutation. The sync layer uses it for conflict detection: when a document has been modified locally and remotely, the version counter helps determine which writes happened in what order.

---

## Index Maintenance Within Transactions

Indexes are maintained transactionally. The `IndexManager` methods are called inside the WiredTiger transaction:

### Insert Path

```python
def _do():
    self.index_mgr.add_doc(doc)            # write to all index tables
    cursor = self.session.open_cursor(...)
    cursor[str(doc["_id"])] = _to_bson(doc) # write to data table
    cursor.close()
    version = self._bump_version(doc["_id"])
    self._oplog_w.log("insert", ...)        # write to oplog table
```

`add_doc` iterates every index and inserts an entry. For unique indexes, it first checks for duplicates via a prefix scan. If a duplicate is found, `DuplicateKeyError` is raised, which propagates up through `_with_transaction`, triggering `rollback_transaction()`.

### Update Path

The update path is more complex because indexes must reflect the change:

```python
for doc in matching:
    old_doc = dict(doc)
    apply_update(doc, update_spec)
    self._validate(doc)
    self.index_mgr.update_doc(old_doc, doc)   # remove old entries, add new
    cursor[str(doc["_id"])] = _to_bson(doc)
```

`update_doc` is smart about which indexes need updating. It only touches indexes where the indexed field values actually changed:

```python
def update_doc(self, old_doc, new_doc):
    for idx in self._indexes.values():
        needs_update = any(
            get_value(old_doc, f) != get_value(new_doc, f) for f in idx.fields
        )
        if needs_update:
            self._delete_entry(idx, old_doc)
            self._insert_entry(idx, new_doc)
```

### Delete Path

```python
for doc in matching:
    self.index_mgr.remove_doc(doc)   # remove from all index tables
    cursor.set_key(str(doc["_id"]))
    cursor.remove()                   # remove from data table
```

---

## The _internal Flag: Echo Prevention

When the sync layer pulls a document from Atlas and writes it locally, it passes `_internal=True`:

```python
local_coll.insert_one(rdoc, _internal=True)
```

Inside the transaction, the `_internal` flag suppresses oplog logging:

```python
if not _internal:
    self._oplog_w.log("insert", doc["_id"], doc, ...)
```

Without this, a pulled document would generate an oplog entry, which the push cycle would send back to Atlas, which would generate a change stream event, which the pull cycle would bring back locally -- an infinite echo loop. The `_internal` flag breaks the cycle at the oplog level.

---

## Schema Validation

When a collection has a `$jsonSchema` validator, every insert and update passes through validation **before** the transaction starts:

```python
def _validate(self, doc):
    if self._validator:
        validate_document(doc, self._validator)
```

If validation fails, a `ValidationError` is raised. For inserts, this happens before the lock is acquired. For updates, validation runs inside the transaction (after `apply_update` modifies the document) to ensure the post-update state is valid.

The validator supports the full `$jsonSchema` spec: type checking, required fields, string/numeric/array constraints, nested schemas, enums, and property count limits.

---

## Bulk Writes

`bulk_write` at the storage level executes each operation sequentially, each in its own transaction. This matches PyMongo's behavior where bulk operations are a convenience wrapper, not a multi-document transaction:

```python
# At the client level:
for op in operations:
    if isinstance(op, InsertOne):
        result = coll.insert_one(op.document)
    elif isinstance(op, UpdateOne):
        result = coll.update_one(op.filter, op.update)
    ...
```

Each individual operation gets its own atomic transaction. The bulk as a whole is not atomic -- if the third operation fails, the first two have already committed. This is consistent with MongoDB's `ordered=True` bulk write semantics.

---

## Summary of Guarantees

| Guarantee | Mechanism |
|---|---|
| **Atomic writes** | WiredTiger transactions span data + indexes + oplog |
| **No partial index updates** | Rollback on any failure reverts all index changes |
| **No orphaned oplog entries** | Oplog write is inside the transaction |
| **Thread safety** | Per-collection `ReadWriteLock` + `threading.Lock` serializes write access while allowing concurrent reads |
| **Crash recovery** | WiredTiger WAL + checkpoints survive process crashes |
| **Type fidelity** | BSON storage preserves MongoDB's type system |
| **Schema enforcement** | `$jsonSchema` validation prevents invalid documents |
| **Echo prevention** | `_internal` flag prevents sync feedback loops |
