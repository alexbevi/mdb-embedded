# Sync Notes: Edge Cases, Gaps, and Things to Be Mindful Of

Internal engineering notes on the bidirectional sync layer (`smongo/sync.py`), covering how it works, where the edges are, and what to watch for in production-like deployments.

---

## How Index Sync Works

Indexes sync bidirectionally, but the mechanism is different in each direction.

### Push (local → remote)

`create_index()` and `drop_index()` log oplog entries (`"index_create"`, `"index_drop"`) containing the key spec and options. During push, the sync layer reads those entries and calls PyMongo's `create_index()` / `drop_index()` on the remote collection. This works because both sides interpret `[("city", 1), ("age", -1)]` identically — smongo maintains real secondary indexes locally via **`smongo-engine` + redb**, while PyMongo tells `mongod` to create the same logical index remotely. The definitions are portable.

### Pull (remote → local)

After pulling documents, `_pull_index_defs()` lists remote indexes via `list_indexes()` and creates any that don't exist locally, using `_internal=True` to prevent echo loops.

### What syncs correctly

- Single-field, compound, unique, and sparse indexes in both directions
- Key specs `[(field, direction)]` are the same format on both sides
- Index names are preserved across sync
- `_internal=True` prevents pulled indexes from being re-pushed

---

## Known Gaps and Edge Cases

### 1. Pull-side index drops are not propagated

`_pull_index_defs()` is **additive-only** -- it creates missing indexes but never drops indexes that exist locally but were removed remotely. If someone drops an index on Atlas, the local side will keep it. Push-side drops work fine (they go through the oplog).

**Impact:** Local index bloat over time if remote indexes are frequently dropped.
**Workaround:** Manually drop stale local indexes, or add a reconciliation step that compares local and remote index sets and removes orphans.

### 2. Exotic index options are silently dropped on pull

`_pull_index_defs()` explicitly extracts only `unique` and `sparse` from remote index definitions:

```python
local_coll.create_index(
    [(f, int(d)) for f, d in keys],
    name=name,
    unique=ridx.get("unique", False),
    sparse=ridx.get("sparse", False),
    _internal=True,
)
```

If the remote index has `partialFilterExpression`, `collation`, `weights` (text index), `expireAfterSeconds` (TTL), or `wildcardProjection`, those options will be silently lost during pull.

**Impact:** Pulled indexes may have different behavior than their remote counterparts.
**Fix:** Forward all supported `**kwargs` from the remote index definition.

### 3. Atlas Search indexes are a different universe

Atlas Search indexes (`$search`, `$searchMeta`) are managed by the Atlas Search service, not by `mongod`'s collection indexes. They don't appear in `list_indexes()` and use a completely different creation API (`createSearchIndex`). These cannot sync in either direction.

**Impact:** None unless you expect `$search` to work locally. smongo's `$vectorSearch` is a separate native stage and works independently.

### 4. TTL index semantics differ

When a TTL index is pushed, the `expireAfterSeconds` option flows through as part of the oplog payload `**kwargs`. However, TTL expiry is implemented differently on each side:
- **Remote:** `mongod` has a built-in background thread that checks TTL indexes every 60 seconds.
- **Local:** smongo uses a `TTLReaper` Python thread per collection.

If a document expires locally and gets deleted, that delete is oplog'd and pushed (deleting it remotely too). If a document expires remotely first, the delete comes through change streams or polling. The clocks need to be reasonably in sync for this to behave predictably.

**Impact:** Documents may expire at different times on each side, causing temporary inconsistency.

### 5. Geospatial indexes (`2dsphere`, `2d`) have limited local support

smongo has a `geo.py` module, but geospatial index types behave differently from standard B-tree indexes. The key encoding for geo coordinates is not the same as what `mongod` uses internally. Syncing the index definition is possible, but query behavior (`$near`, `$geoWithin`) may differ.

---

## Sync Checkpoint and Oplog Behavior

### 6. ~~Checkpoint advances even on partial batch failure~~ (FIXED in 0.9.1)

**Fixed.** The checkpoint now advances only to `safe_key` (the last key from a fully successful batch). Partially failed batches leave the checkpoint unchanged, so failed ops are retried on the next sync cycle. `_flush_bulk()` also returns per-op success counts and logs individual `writeErrors`.

### 7. Oplog auto-compact truncates up to `safe_key`

After a successful push, if `oplog_auto_compact` is enabled (default), the sync layer truncates all oplog entries before `safe_key`. This is correct for the push path, but if another consumer is reading the oplog (e.g., a change stream listener or a custom audit trail), those entries will vanish.

**Impact:** Change stream listeners that fell behind may miss events if oplog compaction runs.
**Workaround:** Disable `oplog_auto_compact` if you have other oplog consumers, or use `compact_oplog(keep=N)` manually.

### 8. ~~Checkpoint table is not transactional with the oplog~~ (FIXED in 0.9.2)

**Fixed.** `_atomic_checkpoint_and_compact()` performs checkpoint write and oplog truncation under the engine’s transactional rules so a crash cannot leave “checkpoint advanced but oplog not truncated” in a bad split. The `_ck_lock` serializes checkpoint access for thread safety under concurrent push.

### 8b. Dead-letter queue for failed ops (Added in 0.9.3)

When individual ops within a `bulk_write` fail, they are now captured in `table:__sync_dlq` instead of being logged and forgotten. Each DLQ entry stores the original oplog entry, error code, message, retry count, and next-retry timestamp. A sweep at the start of each push cycle retries eligible entries with exponential backoff. After `max_dlq_retries` (default 5), entries are marked `permanently_failed`. `status()` exposes `dlq_depth` and `dlq_permanent_failures` for monitoring.

---

## Conflict Resolution Subtleties

### 9. LWW is now clock-skew-resilient (Improved in 0.9.4)

**Improved.** When both documents carry `_vclock` metadata, `_lww` now delegates to `_deterministic_lww` -- a tiebreaker that uses the lexicographically highest `node_id` in each document's vector clock instead of `_lastModified`. This means a client with a skewed wall clock cannot steal wins simply by having a future timestamp. The tiebreak cascade is:

1. **Highest `node_id`** in the vclock wins (deterministic, clock-independent).
2. If highest node_ids are equal, fall back to **`_lastModified`** comparison.
3. If timestamps are also equal, fall back to **`_id`** comparison for a total order.

For **legacy documents without `_vclock`**, `_lww` falls back to pure `_lastModified` comparison (backward compatible). The push path injects `_lastModified` from the oplog's `ts`, so local→remote documents always get it. Remote documents inserted directly by other clients must include `_lastModified` for the legacy fallback to be meaningful.

**Impact:** Multi-device fleets with NTP skew now resolve conflicts deterministically. All nodes running the same algorithm on the same two versions will pick the same winner.

### 10. Field-merge requires oplog `changed_fields` tracking

The `field_merge` conflict resolver uses `changed_fields` from the oplog to determine which side modified which fields. This data is populated during `_push()` from the oplog entry. However, `changed_fields` only tracks fields from the most recent update to that document -- if there were multiple local updates to different fields before push, only the last update's fields are recorded in `_local_field_history`.

**Impact:** Field-merge may under-report locally changed fields, causing some local changes to be overwritten by remote values.

### 11. Vector clocks are active in conflict resolution

`VectorClock` is now wired into `_upsert_remote_doc`. On conflict:
1. The local and remote document vector clocks (stored as `_vclock`) are compared
2. If one dominates the other, the dominating version wins automatically (no resolver invoked)
3. If the events are truly concurrent, the configured conflict resolver (LWW, local-wins, etc.) decides
4. The merged clock is ticked for the local `node_id` and stamped back onto the resolved document

**Impact:** Causal ordering is now enforced. Documents carry `_vclock` metadata (a dict of `{node_id: counter}`). This adds a small per-document overhead but enables correct conflict detection across multiple replicas.

### 12. CRDT merge is implemented but opt-in and untested in integration

`_crdt_counter_merge` and `_crdt_set_merge` are implemented, and `_crdt_merge_doc` can merge documents with CRDT-annotated fields. However, the CRDT merge is only invoked when explicitly configured via `crdt_fields` in sync config, and there are no integration tests covering this path.

**Impact:** The feature exists but should be considered experimental.

---

## Change Streams vs. Timestamp Polling

### 13. ~~Change streams require a replica set~~ (FIXED in 0.9.3)

**Fixed.** The testcontainers fixture now runs `mongo:7` as a single-node replica set (`--replSet rs0` + `rs.initiate()`). `docker-compose.yml` and CI are updated to match. Dedicated integration tests exercise the change stream pull path, including delete propagation. The `_pull_via_change_stream()` method now saves the initial resume token even when no events arrive, so events between pull cycles are not lost.

### 14. Timestamp polling misses remote deletes

The polling fallback queries `find({_lastModified: {$gt: last_ts}})`. Deleted documents don't appear in find results. This means remote deletes are **not detected** via the polling path -- only via change streams (which emit `"delete"` events with `documentKey`).

**Impact:** If change streams are unavailable, remote deletes will never propagate to local. This is noted in the integration test:

```python
# Pull delete is change-stream-specific; with polling fallback this may not delete.
assert coll.find_one({"_id": "r3"}) is not None  # still exists locally
```

### 15. Change stream `max_await_time_ms` is fixed at 200ms

The change stream watcher uses `max_await_time_ms=200`, which means it blocks for at most 200ms waiting for events before returning. Combined with `batch_size` events per cycle, this creates a throughput ceiling during high-frequency remote writes. The pull cycle processes at most `batch_size` events (default 100), then waits for the next sync interval.

**Impact:** Under heavy remote write load, pull may lag behind. Increase `batch_size` or decrease `interval_sec` for write-heavy workloads.

---

## Type Conversion Boundary

### 16. `_to_pymongo` / `_from_pymongo` conversion asymmetry

The sync layer converts documents between smongo-native types and PyMongo types using `_to_pymongo` (push) and `_from_pymongo` (pull). These handle `ObjectId`, `datetime`, and other BSON types. However, the conversion is not perfectly symmetric for all types:

- `Decimal128` on remote becomes `float` locally (precision loss)
- `Binary` subtype handling may differ
- `Regex` objects are stored as `{"$regex": ..., "$options": ...}` dicts locally but as native `bson.Regex` on the remote

**Impact:** Round-tripping exotic BSON types through sync may lose fidelity.

### 17. ~~Large documents near the 16MB BSON limit~~ (Improved in 0.9.3)

**Improved.** The oplog now stores document payloads as raw BSON bytes (via the Rust encoder) instead of JSON strings. This roughly halves oplog storage and eliminates the JSON serialization overhead. The push path re-encodes via `_to_pymongo` and `bulk_write`, which has its own 48MB message size limit.

**Impact:** Large documents are better handled, but individual documents should still stay well under 16MB.

---

## Selective Sync Filters

### 18. Push filters now resolve the full document for updates

Push-side filters for `update` operations now fetch the current document from the local collection by `doc_id` and evaluate the filter against the full document -- not the oplog payload (which is an update spec like `{"$set": {...}}`). For `insert` operations, the filter evaluates against the payload directly (which is the full document). For `delete` operations, the filter evaluates against the payload if available.

**Impact:** Selective sync filters now work correctly for all operation types. There is one additional local read per filtered update operation, but this only applies when sync rules or per-collection filters are configured.

### 19. Filter changes don't retroactively sync

If you change a selective sync filter (e.g., widen it to include documents that were previously excluded), documents that were skipped in past push cycles are already past the checkpoint and won't be re-pushed. The oplog entries may also have been compacted.

**Impact:** Changing filters requires a manual full re-sync to catch up previously excluded documents.

---

## Tombstones

### 20. ~~Tombstone registry is in-memory only~~ (FIXED in 0.9.2)

**Fixed.** `TombstoneRegistry` is backed by a durable engine table (`table:__tombstones`, key=doc_id, value=deletion_timestamp). Tombstones survive process restarts. The `mark_deleted()` / `is_tombstoned()` / `expire()` API is unchanged. A threading lock protects concurrent access.

---

## Docker / Compose Considerations

### 21. ~~Standalone `mongod` vs. replica set~~ (FIXED in 0.9.3)

**Fixed.** The `docker-compose.yml` now runs `mongo:7` as a **single-node replica set** (`--replSet rs0` + `rs.initiate()`). Change streams, remote delete detection, and transaction support all work out of the box.

### 22. Local database path persistence

The compose file may use a named volume for the **local redb database path**. Only one process should open a given path at a time; competing containers or hosts must use different paths. If a process exits uncleanly, rely on normal filesystem recovery; avoid running two smongo instances on the same file.

### 23. ~~Clock skew between containers~~ (Mitigated in 0.9.4)

**Mitigated.** LWW conflict resolution now uses `_deterministic_lww` (node-id-based tiebreaker) when both documents carry `_vclock`, so wall-clock skew no longer determines the winner for concurrent edits. NTP synchronization is still recommended for accurate `_lastModified` ordering of non-concurrent changes, but a few seconds of drift will not cause incorrect conflict resolution outcomes.

---

## Schema Rejection Handling (Added in 0.9.4)

### 24. Server-side schema validation failures are now handled

When a push operation fails with MongoDB error code `121` (DocumentValidationFailure), the sync layer now reacts based on the `schema_rejection_strategy` config:

| Strategy | Behavior |
|---|---|
| `"rollback"` (default) | Pull the server's version of the document and overwrite the local copy. If the server has no copy (rejected insert), delete the local document. |
| `"quarantine"` | Leave the local document untouched. The failed op goes to the DLQ as `permanently_failed`. |
| `"ignore"` | Legacy behavior: DLQ the op and move on. |

Schema rejections are always marked `permanently_failed` in the DLQ (no retries -- the same invalid document will always fail). The `status()` dict includes a `schema_rejections` counter.

```python
sync_config = {
    "schema_rejection_strategy": "rollback",  # or "quarantine" or "ignore"
}
```

**Impact:** Documents that pass local validation but fail server-side validation are no longer silently stuck in the DLQ forever. The rollback strategy keeps client and server converged.

---

## Oplog Overflow and Full Resync (Added in 0.9.4)

### 25. Oplog overflow is now detected

If the push checkpoint references an oplog key that no longer exists (because auto-compact or manual `compact_oplog` removed it), the sync layer detects this by comparing the checkpoint against `OplogReader.oldest_key()`. When `checkpoint < oldest_key`, the oplog has been truncated past the checkpoint.

Behavior is controlled by the `overflow_strategy` config:

| Strategy | Behavior |
|---|---|
| `"server_wins"` (default) | Automatically trigger a full resync: reset all checkpoints, drop local data, re-pull everything from the server. |
| `"error"` | Raise `SyncOverflowError` so the application can handle recovery. |

```python
sync_config = {
    "overflow_strategy": "server_wins",  # or "error"
}
```

### 26. `force_full_resync()` is available as a public API

`SyncManager.force_full_resync(db_name, coll_name)` manually triggers a full resync for any registered collection. It:

1. Resets all push/pull checkpoints for the namespace.
2. Drops all local documents in that collection.
3. Re-pulls everything from the remote.
4. Returns `{"ns": ..., "winner": "server", "docs_synced": N}`.

This is the escape hatch for any unrecoverable sync state: oplog corruption, checkpoint drift, or manual "nuke and re-pull" when data has diverged beyond repair.

**Impact:** Oplog overflow no longer silently loses changes. The sync layer either auto-recovers or raises an explicit error, and there is always a manual recovery path.

---

## Testing Gaps

| Area | Current Coverage | Gap |
|---|---|---|
| Push insert/update/delete | Integration tested | Covered |
| Index push (create) | Integration tested | Covered |
| Index pull (create) | Integration tested | Covered |
| Index pull (drop) | Not tested | Not implemented |
| Conflict: LWW | Implicitly tested | Covered |
| Conflict: LWW clock-skew resilience | Unit + integration tested (0.9.4) | Covered |
| Conflict: local_wins | Integration tested | Covered |
| Conflict: remote_wins | Integration tested | Covered |
| Conflict: field_merge | Not integration tested | Unit only |
| Conflict: custom callable | Not tested | Gap |
| CRDT merge | Not tested | Gap |
| Vector clocks | Unit + integration tested | Covered |
| Multi-client convergence | Unit + integration tested (0.9.4) | Covered |
| Change stream pull | Integration tested (0.9.3) | Covered |
| Timestamp polling pull | Integration tested | Covered |
| Remote delete via change stream | Integration tested (0.9.3) | Covered |
| Remote delete via polling | Known non-functional | Documented |
| MQL sync rules (global) | Unit + integration tested | Covered |
| Device-scoped sync | Integration tested | Covered |
| Time-windowed sync | Integration tested | Covered |
| Variable substitution | Unit tested | Covered |
| Push filter for updates | Unit tested | Covered |
| Node ID in oplog | Integration tested | Covered |
| Per-collection sync_filter | Unit tested | Covered |
| Schema rejection handling | Unit + integration tested (0.9.4) | Covered |
| Oplog overflow detection | Unit + integration tested (0.9.4) | Covered |
| Full resync (force_full_resync) | Unit + integration tested (0.9.4) | Covered |
| Oplog auto-compact | Not directly tested | Exercised implicitly |
| Exponential backoff | Not tested | Gap |
| Tombstone expiry | Unit tested (0.9.2) | Covered |
| Tombstone persistence | Unit tested (0.9.2) | Covered |
| Large batch push (>batch_size) | Not tested | Gap |
| Crash recovery / checkpoint | Unit tested (0.9.2, rollback path) | Partial |
| Concurrent namespace push | Unit tested (0.9.2, barrier) | Covered |
| Resumable initial snapshot | Unit tested (0.9.2) | Covered |
| Dead-letter queue | Unit tested (0.9.3) | Covered |
| BSON oplog encoding | Unit tested (0.9.3) | Covered |

---

## MQL Sync Rules and Variable Substitution

### How it works

Sync rules are standard MQL query dicts passed via `sync_config["sync_rules"]`. They control which documents are pushed and pulled -- the same query language you use for `find()` and `aggregate()`.

**Variable substitution** replaces `$$NAME` strings with values from a context dict. This happens at the start of each sync cycle, so time-based variables like `$$NOW` are always fresh. The substitution is a pure-Python deep walk -- the Rust query engine is unchanged.

Built-in variables:

| Variable | Value | Description |
|---|---|---|
| `$$NOW` | `time.time()` | Epoch seconds (float), matches `_lastModified` |
| `$$NODE_ID` | `sync_config["node_id"]` | Device/replica identity |

User-defined variables via `sync_config["variables"]`:

```python
sync_config = {
    "sync_rules": {"region": "$$REGION", "active": True},
    "variables": {"REGION": "us-east-1"},
}
```

Variables that start with `$$` but have no matching context key are left as-is (so `$$ROOT` and `$$CURRENT` in `$expr` still work).

### Where rules are applied

- **Push (local → Atlas):** each oplog entry is checked against the global sync filter and per-collection filter. For `insert` ops, the payload (full document) is evaluated. For `update` ops, the **current full document** is fetched by `doc_id` and evaluated (not the update spec). For `delete` ops, the payload is evaluated if available.
- **Pull (Atlas → local):** each remote document is checked against the global sync filter and per-collection filter before being upserted locally. This applies to both the timestamp-polling path and the change-stream path.

### Per-collection filters

Per-collection filters are specified via `collections` as a dict:

```python
sync_config = {
    "collections": {
        "iot.readings": {"device_id": "$$NODE_ID"},
        "iot.config": {},
    },
    "node_id": "sensor-042",
}
```

These also support `$$` variable substitution and are recompiled each sync cycle.

The `register_collection` method also accepts an optional `sync_filter` kwarg for programmatic registration:

```python
mgr.register_collection("iot", "readings", local_coll, sync_filter={"device_id": "$$NODE_ID"})
```

---

## Edge Fleet Sync Pattern

### Architecture

Multiple edge devices (IoT sensors, mobile apps, point-of-sale terminals) each run their own smongo engine. A central MongoDB Atlas cluster aggregates data from the entire fleet. Each device uses MQL sync rules scoped to its own `node_id`:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  device-001  │     │  device-002  │     │  device-003  │
│ node_id=001  │     │ node_id=002  │     │ node_id=003  │
│ sync_rules:  │     │ sync_rules:  │     │ sync_rules:  │
│ device_id=   │     │ device_id=   │     │ device_id=   │
│   $$NODE_ID  │     │   $$NODE_ID  │     │   $$NODE_ID  │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │  push: own data     │                     │
       │  pull: own data     │                     │
       └─────────┬───────────┴─────────────┬───────┘
                 ▼                         ▼
         ┌───────────────────────────────────┐
         │        MongoDB Atlas (central)     │
         │  All devices' data aggregated      │
         │  Fleet-wide analytics via MQL      │
         └───────────────────────────────────┘
```

### Configuration

```python
client = MongoClient(f"local://{device_data_dir}", sync=ATLAS_URI, sync_config={
    "node_id": device_serial_number,
    "sync_rules": {"device_id": "$$NODE_ID"},
})
```

### Node provenance in oplog

When a `SyncManager` registers a collection, it stamps the configured `node_id` on the collection's `OplogWriter`. Subsequent oplog entries include a `"node_id"` field, enabling audit trails and debugging across the fleet.

### Vector clocks for multi-device conflict resolution

Documents carry a `_vclock` field (dict of `{node_id: counter}`). When two devices modify the same document:
- If one version's clock **dominates** the other (every counter >=, at least one >), it wins automatically
- If the clocks are **concurrent** (neither dominates), the configured conflict resolver decides
- The merged clock is ticked for the resolving node and stamped on the winner

This provides correct causal ordering without requiring synchronized wall clocks.

### Example

See [`examples/patterns/edge_fleet_sync.py`](examples/patterns/edge_fleet_sync.py) for a complete working example with three simulated edge devices, device-scoped sync, and time-windowed sync.

---

## Recommendations

1. ~~**Enable replica set in docker-compose**~~ -- done in 0.9.3.
2. **Implement pull-side index drop reconciliation** to prevent local index bloat.
3. **Forward all index options on pull**, not just `unique` and `sparse`.
4. ~~**Fix checkpoint advancement on partial failure**~~ -- done in 0.9.1.
5. ~~**Add integration tests for field_merge and change stream pull.**~~ -- change stream pull done in 0.9.3; field_merge still unit-only.
6. ~~**Persist tombstones to durable storage**~~ -- done in 0.9.2.
7. ~~**Document the `_lastModified` requirement**~~ -- LWW is now clock-skew-resilient via `_deterministic_lww` (0.9.4).
8. ~~**Handle server-side schema validation failures**~~ -- done in 0.9.4; rollback, quarantine, and ignore strategies.
9. ~~**Detect and recover from oplog overflow**~~ -- done in 0.9.4; `force_full_resync()` + `overflow_strategy` config.
10. ~~**Add multi-client convergence tests**~~ -- done in 0.9.4; unit and integration tests for 2+ clients.
