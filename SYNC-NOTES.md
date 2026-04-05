# Sync Notes: Edge Cases, Gaps, and Things to Be Mindful Of

Internal engineering notes on the bidirectional sync layer (`smongo/sync.py`), covering how it works, where the edges are, and what to watch for in production-like deployments.

---

## How Index Sync Works

Indexes sync bidirectionally, but the mechanism is different in each direction.

### Push (local → remote)

`create_index()` and `drop_index()` log oplog entries (`"index_create"`, `"index_drop"`) containing the key spec and options. During push, the sync layer reads those entries and calls PyMongo's `create_index()` / `drop_index()` on the remote collection. This works because both sides interpret `[("city", 1), ("age", -1)]` identically -- smongo creates a WiredTiger B-tree table locally, PyMongo tells `mongod` to create one remotely. The logical definitions are portable.

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

Atlas Search indexes (`$search`, `$searchMeta`) are managed by the Atlas Search service, not by `mongod`'s WiredTiger. They don't appear in `list_indexes()` and use a completely different creation API (`createSearchIndex`). These cannot sync in either direction.

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

### 6. Checkpoint advances even on partial batch failure

When a bulk_write partially fails (some ops succeed, some don't), `_flush_bulk` returns `False` and `safe_key` is not updated -- but `last_key` still advances. The checkpoint (`push:{ns}`) is set to `last_key` at the end of the loop regardless:

```python
if last_key:
    self._set_checkpoint(f"push:{ns}", last_key)
```

This means: if a batch partially fails and the process restarts, the failed ops within that batch will **not** be retried because the checkpoint has moved past them. The warning is logged but the entries are effectively skipped.

**Impact:** Silent data loss on partial bulk_write failures during push.
**Mitigation:** Set `oplog_auto_compact: False` during development so you can inspect the oplog. Monitor the `errors` counter in `status()`.

### 7. Oplog auto-compact truncates up to `safe_key`

After a successful push, if `oplog_auto_compact` is enabled (default), the sync layer truncates all oplog entries before `safe_key`. This is correct for the push path, but if another consumer is reading the oplog (e.g., a change stream listener or a custom audit trail), those entries will vanish.

**Impact:** Change stream listeners that fell behind may miss events if oplog compaction runs.
**Workaround:** Disable `oplog_auto_compact` if you have other oplog consumers, or use `compact_oplog(keep=N)` manually.

### 8. Checkpoint table is not transactional with the oplog

The sync checkpoint (`table:__sync_checkpoint`) is written via a separate WiredTiger session from the oplog. If the process crashes between pushing ops and updating the checkpoint, some ops may be re-pushed on restart (duplicates). The push uses `upsert=True` for updates and `ordered=False` for bulk writes, so duplicates are tolerable for updates/deletes but will fail for inserts (duplicate key on remote).

**Impact:** Rare insert duplication errors on crash recovery. The `BulkWriteError` is caught and logged but the entries are considered pushed.

---

## Conflict Resolution Subtleties

### 9. `_lastModified` must be present for LWW to work

The default LWW resolver compares `_lastModified` timestamps:

```python
def _lww(local_doc, remote_doc):
    local_ts = (local_doc or {}).get("_lastModified", 0)
    remote_ts = (remote_doc or {}).get("_lastModified", 0)
    return remote_doc if remote_ts >= local_ts else local_doc
```

If documents are inserted without `_lastModified` (e.g., directly via the wire protocol or a PyMongo client), the timestamp defaults to `0` and the remote always wins (tie goes to remote). The push path injects `_lastModified` from the oplog's `ts`, so local→remote documents get it. But documents inserted directly on the remote side by other clients must include `_lastModified` for LWW to be meaningful.

**Impact:** Remote documents without `_lastModified` will always lose to local documents that have it, or both default to `0` and remote wins by tie.

### 10. Field-merge requires oplog `changed_fields` tracking

The `field_merge` conflict resolver uses `changed_fields` from the oplog to determine which side modified which fields. This data is populated during `_push()` from the oplog entry. However, `changed_fields` only tracks fields from the most recent update to that document -- if there were multiple local updates to different fields before push, only the last update's fields are recorded in `_local_field_history`.

**Impact:** Field-merge may under-report locally changed fields, causing some local changes to be overwritten by remote values.

### 11. Vector clocks are initialized but not fully wired

`VectorClock` is implemented and `self._vector_clocks` is initialized in `__init__`, but vector clocks are not actually stamped on documents during push/pull or consulted during conflict resolution. The infrastructure is there but the integration is incomplete.

**Impact:** None currently -- the feature is dormant. If you're counting on causal ordering beyond LWW timestamps, it's not active yet.

### 12. CRDT merge is implemented but opt-in and untested in integration

`_crdt_counter_merge` and `_crdt_set_merge` are implemented, and `_crdt_merge_doc` can merge documents with CRDT-annotated fields. However, the CRDT merge is only invoked when explicitly configured via `crdt_fields` in sync config, and there are no integration tests covering this path.

**Impact:** The feature exists but should be considered experimental.

---

## Change Streams vs. Timestamp Polling

### 13. Change streams require a replica set

MongoDB change streams require a replica set (or sharded cluster). A standalone `mongod` does not support them. The current `docker-compose.yml` uses standalone `mongo:7`, so the integration test fixture explicitly sets `use_change_stream_pull: False`.

**Impact:** The preferred pull mechanism (change streams with resume tokens) is never exercised in CI. Only the timestamp polling fallback is tested.
**Fix:** Initialize `mongo:7` as a single-node replica set in docker-compose:

```yaml
mongo:
  image: mongo:7
  command: mongod --replSet rs0 --bind_ip_all
```

With a healthcheck that calls `rs.initiate()` on first boot.

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

### 17. Large documents near the 16MB BSON limit

The oplog stores the full document payload as a JSON string (not BSON). Documents near the 16MB BSON limit may exceed WiredTiger's default value size or cause JSON serialization overhead. The push path re-encodes via `_to_pymongo` and `bulk_write`, which has its own 48MB message size limit.

**Impact:** Extremely large documents may fail to sync. Keep individual documents well under 16MB.

---

## Selective Sync Filters

### 18. Filters are evaluated against the payload, not the live document

Push-side namespace filters (`ns_filter`) are evaluated against the oplog entry's `payload`, not the current state of the document. For updates, the payload is the update spec (e.g., `{"$set": {"status": "active"}}`), not the full document. A filter like `{"status": "active"}` won't match an update spec because the top-level key is `"$set"`, not `"status"`.

**Impact:** Selective sync filters work reliably for insert payloads (full documents) but may not filter updates correctly because the payload shape differs.
**Workaround:** Use broad filters or apply filtering at the collection level rather than per-document.

### 19. Filter changes don't retroactively sync

If you change a selective sync filter (e.g., widen it to include documents that were previously excluded), documents that were skipped in past push cycles are already past the checkpoint and won't be re-pushed. The oplog entries may also have been compacted.

**Impact:** Changing filters requires a manual full re-sync to catch up previously excluded documents.

---

## Tombstones

### 20. Tombstone registry is in-memory only

`TombstoneRegistry` tracks deleted document IDs with timestamps for expiry, but the registry lives in memory and is not persisted to WiredTiger. If the process restarts, all tombstone state is lost. This means a document deleted locally, pushed to remote, and then pulled back before the push checkpoint was written could be re-inserted locally.

**Impact:** Edge case on crash recovery -- deleted documents could reappear.
**Mitigation:** The `_internal=True` flag on pulled writes prevents oplog echo, but the tombstone gap exists for the narrow window between delete-push and checkpoint-write.

---

## Docker / Compose Considerations

### 21. Standalone `mongod` vs. replica set

The existing `docker-compose.yml` runs `mongo:7` standalone. This means:
- No change streams (pull falls back to timestamp polling)
- No remote delete detection (see #14)
- No transaction support on the remote side

For a realistic sync demo, use a single-node replica set.

### 22. WiredTiger data directory persistence

The compose file uses a named volume (`wt_data`) for the WiredTiger data directory. WiredTiger lock files (`WiredTiger.lock`) will prevent multiple smongo containers from opening the same directory. If a container crashes without clean shutdown, the lock file may need manual removal.

### 23. Clock skew between containers

LWW conflict resolution depends on timestamps. Docker containers share the host clock, so clock skew is not usually an issue. But if running across multiple hosts (e.g., in a multi-node conflict resolution demo), NTP synchronization matters. A few seconds of drift can cause unexpected conflict resolution outcomes.

---

## Testing Gaps

| Area | Current Coverage | Gap |
|---|---|---|
| Push insert/update/delete | Integration tested | Covered |
| Index push (create) | Integration tested | Covered |
| Index pull (create) | Integration tested | Covered |
| Index pull (drop) | Not tested | Not implemented |
| Conflict: LWW | Implicitly tested | Covered |
| Conflict: local_wins | Integration tested | Covered |
| Conflict: remote_wins | Integration tested | Covered |
| Conflict: field_merge | Not integration tested | Unit only |
| Conflict: custom callable | Not tested | Gap |
| CRDT merge | Not tested | Gap |
| Vector clocks | Not tested | Dormant feature |
| Change stream pull | Not tested (disabled in CI) | Major gap |
| Timestamp polling pull | Integration tested | Covered |
| Remote delete via polling | Known non-functional | Documented |
| Selective sync filters | Not integration tested | Gap |
| Oplog auto-compact | Not directly tested | Exercised implicitly |
| Exponential backoff | Not tested | Gap |
| Tombstone expiry | Not tested | Gap |
| Large batch push (>batch_size) | Not tested | Gap |
| Crash recovery / checkpoint | Not tested | Gap |

---

## Recommendations

1. **Enable replica set in docker-compose** to test the change stream pull path in CI.
2. **Implement pull-side index drop reconciliation** to prevent local index bloat.
3. **Forward all index options on pull**, not just `unique` and `sparse`.
4. **Fix checkpoint advancement on partial failure** -- only advance to `safe_key`, not `last_key`.
5. **Add integration tests for field_merge, selective filters, and change stream pull.**
6. **Persist tombstones to WiredTiger** for crash-safe delete tracking.
7. **Wire up vector clocks** or remove the dormant code to avoid confusion.
8. **Document the `_lastModified` requirement** for remote documents participating in LWW.
