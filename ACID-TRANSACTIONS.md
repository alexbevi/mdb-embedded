# ACID Transactions

**How smongo keeps local writes atomic, consistent, and durable on `smongo-engine` backed by [redb](https://github.com/cberner/redb), while matching MongoDB wire-level semantics.**

---

## The Guarantee

Every local write — insert, update, delete, index create, index drop — runs through **`smongo-engine`** so that **data rows, secondary indexes, and the per-collection oplog** are updated in one logical transaction.

On native targets the storage backend is **redb**. The engine’s `StorageSession` implements `begin_transaction` / `commit_transaction` / `rollback_transaction`: while a transaction is open, mutations are buffered; **commit** flushes them inside a single **redb** write transaction. If any step fails, **rollback** discards the buffered work — no half-applied indexes, no orphan oplog entries for that operation.

This is the same *shape* of guarantee you expect from MongoDB for single-document operations: one observable outcome, not a torn update.

---

## The Write Path

```
Application calls insert_one(doc)
    │
    ▼
┌──────────────────────────────────┐
│  1. Validate (when applicable)   │  ← $jsonSchema, type checks
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│  2. Begin engine transaction     │  ← StorageSession::begin_transaction
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│  3. Maintain indexes + data      │  ← engine Collection / index layer
│     (may raise DuplicateKeyError)│
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│  4. Append oplog (unless _internal) │
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
  │ redb   │  │ Pending ops  │
  │ write  │  │ discarded    │
  │ txn    │  │              │
  └────────┘  └──────────────┘
```

---

## What "Atomic" Means Here

A single `insert_one` touches the **document table**, **every relevant index table**, and (unless `_internal=True`) the **oplog**. The engine’s transaction wrapper ensures they succeed or fail together:

```rust
// smongo-engine — illustrative
collection.with_transaction(|| {
    // … indexes, BSON row, oplog …
    Ok(())
})?;
```

If a unique index rejects a duplicate in the middle of the operation, the error propagates, the session rolls back, and the collection matches the pre-call state.

### Example: update rollback

```python
coll.create_index("email", unique=True)
coll.insert_one({"_id": "1", "email": "alice@example.com", "name": "Alice"})
coll.insert_one({"_id": "2", "email": "bob@example.com", "name": "Bob"})

coll.update_one({"_id": "2"}, {"$set": {"email": "alice@example.com"}})
# → DuplicateKeyError
# → No durable change to Bob’s row or indexes
# → No new oplog entry for that failed attempt
```

---

## Concurrency and Python bindings

- **`RedbLocalCollection`** (PyO3) holds a shared **`Arc<Database<RedbBackend>>`** and builds **`smongo_engine::collection::Collection`** views for each operation.
- **Multi-document transactions** use a **`RedbTxnSlot`** on the collection: while a client transaction is active, ordinary engine access is blocked so routing stays unambiguous.
- The wire server multiplexes many TCP connections onto **one** embedded database handle; isolation follows **redb**’s transaction rules plus the engine’s session discipline.

---

## Durability and crash recovery

**redb** is an embedded ACID store. After `commit` succeeds, committed data is durable according to redb’s persistence model (memory-mapped file, copy-on-write pages). The Python embed does not run a separate storage daemon or manage legacy engine lock files.

---

## BSON storage

Documents are stored as **raw BSON bytes** in engine tables — not JSON text. That preserves MongoDB types (ObjectId, DateTime, binary, Decimal128, etc.) and keeps encode/decode aligned with PyMongo and the wire protocol.

---

## Document versioning

Each mutation bumps a per-document version counter carried in the oplog. The sync layer uses **`v`** (and timestamps) for conflict detection when merging local and Atlas changes.

---

## Index maintenance

Index entries are maintained **inside the same engine transaction** as the document row: insert adds index keys for every index; update removes stale keys and inserts new ones for fields that changed; delete removes all keys for that document. A failure anywhere rolls back the whole transaction.

---

## The `_internal` flag: echo prevention

When sync pulls from Atlas it writes with `_internal=True`, which **suppresses oplog logging** for that write. Otherwise pull → oplog → push → remote change stream → pull would loop forever.

---

## Schema validation

`$jsonSchema` runs **before** the costly path where possible so invalid documents fail fast. When validation must see the post-update document, it runs at the appropriate point in the update pipeline inside the engine’s rules for that operation.

---

## Bulk writes

`bulk_write` in local mode runs **one engine transaction per operation** (each insert/update/delete is still individually atomic). If `ordered=True` and an operation fails, earlier ops have already committed — same general model as PyMongo against a server.

---

## Summary

| Guarantee | Mechanism |
|-----------|-----------|
| **Atomic writes** | Engine transaction → single redb `WriteTransaction` on commit |
| **No partial index updates** | Rollback drops buffered ops |
| **No orphaned oplog for failed writes** | Oplog append participates in the same transaction |
| **Thread / connection safety** | Shared DB handle + engine session rules + txn slot for multi-doc txns |
| **Type fidelity** | BSON values in storage |
| **Echo prevention** | `_internal` on sync apply path |
