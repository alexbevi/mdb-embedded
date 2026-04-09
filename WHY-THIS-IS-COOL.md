# Why This Is Cool

---

## 1. It's Not a Mock

Most "embedded MongoDB" solutions are mocks. They intercept PyMongo calls, store documents in Python dictionaries, and approximate query behavior with hand-rolled filtering. They break on edge cases. They don't support real aggregation. They don't have indexes. They don't crash-recover.

smongo embeds **`smongo-engine`** — a Rust implementation of MQL, aggregation, indexes, and sync-oriented oplog — persisted on native targets with **[redb](https://github.com/cberner/redb)** (embedded ACID B-tree). Documents live as **native BSON bytes**. Writes run in **engine transactions** that commit to a single redb write transaction. Indexes are real ordered key structures with planner-driven selection.

When you test against this, you're exercising the same *protocol and query semantics* you use against Atlas, backed by a real on-disk engine — not an in-memory dict.

---

## 2. One Query Language, Everywhere

Most offline-first or edge architectures force you to use a different database and query language locally. SQLite on the device. Postgres in the cloud. Two schemas, two query languages, two sets of bugs.

smongo lets you write MQL once:

```python
users.find({"city": "NYC", "age": {"$gt": 30}})
```

That query runs identically against:

- A **local** `local://` database (redb file on disk)
- A **MongoDB Atlas** cluster in the cloud
- A **hybrid** setup where both exist simultaneously

The `MongoClient` URI is the only thing that changes:

```python
client = MongoClient("local://data")                              # embedded
client = MongoClient("mongodb+srv://...")                          # Atlas
client = MongoClient("local://data", sync="mongodb+srv://...")     # both
```

Same API. Same operators. Same aggregation pipeline. Same index semantics.

---

## 3. Real Drivers Can Connect

The wire protocol server isn't a toy. It speaks OP_MSG (opcode 2013) — the binary protocol MongoDB drivers use. Start the server and connect `mongosh`:

```bash
$ python -m smongo.wire --port 27017
$ mongosh mongodb://localhost:27017
```

Or connect PyMongo, Node, Go, Compass, etc.

The server handles many commands: `find`, `insert`, `update`, `delete`, `aggregate`, `createIndexes`, `listCollections`, `findAndModify`, `getMore`, `killCursors`, and more. It advertises wire version 0–21, 16MB max BSON object size, and 48MB max message size — aligned with production MongoDB expectations.

---

## 4. Bidirectional Sync That Actually Works

Local-first databases are easy until you need to sync. Then you discover conflict resolution, echo prevention, checkpoint persistence, partial failure recovery, selective filtering, and backoff strategies.

smongo's `SyncManager` addresses these:

- **Push**: Tail the local oplog, batch `bulk_write` to Atlas. Checkpoint after each batch. Auto-compact the oplog after successful push.
- **Pull**: MongoDB Change Streams with resume token persistence (preferred), or timestamp-based polling (fallback). Initial full snapshot on first pull.
- **Conflict resolution**: Last-write-wins, local-wins, remote-wins, field-level merge, or a custom callable.
- **Echo prevention**: `_internal=True` on sync-applied writes suppresses oplog entries so you don't loop push ↔ pull.
- **Exponential backoff** and **selective sync filters** (per-collection MQL).

Sync checkpoints and metadata live in **engine tables** (e.g. `table:__sync_checkpoint` naming) so state survives restarts.

---

## 5. The Aggregation Pipeline Is Real

The pipeline engine supports many stages (`$match`, `$group`, `$project`, `$sort`, `$lookup`, `$vectorSearch`, `$facet`, `$out`, `$merge`, …), group accumulators, and a broad expression language — suitable for analytics, denormalized views, and materialized outputs running locally.

---

## 6. Vector Search Runs In-Process

`$vectorSearch` is a first-class aggregation stage. Embeddings live alongside documents; search uses NumPy (exact) or USearch (ANN) without a separate vector service.

---

## 7. ACID Transactions Are Not Optional

Every local write goes through the engine’s transactional API. Data, indexes, and oplog updates for that operation commit or roll back together. See [ACID-TRANSACTIONS.md](ACID-TRANSACTIONS.md).

---

## 8. Lazy Reads -- No Wasted Work

The read path is **streaming** where it matters: `find()` uses cursors that decode BSON as you iterate; `find_one()` stops at the first match; `count_documents` can count without building Python lists of full documents. The same planner that accelerates reads also accelerates targeted updates and deletes.

---

## 9. The Query Planner Accelerates Writes Too

Writes that identify documents by `_id` or by an index use the same planning logic as reads (PK or index seek) instead of always scanning the full collection.

---

## 10. The Oplog Makes Everything Possible

Mutations are append-logged with timestamps, version counters, checksums, and changed-field tracking. That powers **sync push**, **change streams**, **conflict resolution**, and **auditing**. Compaction is available via **`OplogWriter.truncate_*`** and sync’s auto-compact path.

---

## 11. ObjectId Is Spec-Compliant

Proper 12-byte MongoDB ObjectId layout: timestamp, random, counter — `generation_time` matches PyMongo’s behavior.

---

## 12. Schema Validation at the Edge

`$jsonSchema` runs on insert/update so invalid documents fail before durable storage.

---

## 13. Minimal Runtime Dependencies

Local mode needs **PyMongo** (BSON / remote), the **`smongo`** package with the compiled **Rust extension** (PyO3 / maturin), and **redb** pulled in as a **Rust** dependency of the engine — not a separate `pip install` of a second database engine.

```bash
pip install pymongo maturin  # build/install smongo per your project
```

No `mongod` process is required for embedded use. Sync and Atlas are optional.

---

## 14. Broad Test Coverage

The suite spans query compilation, storage, aggregation, sync, wire protocol, auth, schema, and more — plus integration tests where applicable. Run `pytest` and the Rust `cargo test` workflows in CI for the full picture.

---

## 15. Free-Threaded Python Ready

The Rust extension targets CPython 3.13+ free-threading with `#[pymodule(gil_used = false)]`, `PyOnceLock` caches, and documented `Send`/`Sync` invariants. See [BYE-BYE-GIL.md](BYE-BYE-GIL.md).

---

## The Big Picture

smongo brings **MongoDB-shaped** APIs and wire compatibility to an **embedded Rust engine** with a **real on-disk store** (redb), optional **Atlas sync**, and **no mock storage** in the default path. That combination is what makes it cool.
