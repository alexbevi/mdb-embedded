# UPNEXT4SYNC -- A Path to Enterprise-Grade Sync

The sync layer already works: oplog-driven bidirectional sync, MQL-native rules with variable substitution, vector clocks, configurable conflict resolution, and device-scoped filtering. This document lays out the path from "works" to "production-ready at fleet scale" -- organized into tiers so each layer builds on the last.

```
Where we are                 Where we're going
─────────────────            ──────────────────────────────────
Crash-safe sync              Tier 0  Stop losing data        ✓
  with concurrent push  ──►  Tier 1  Survive crashes         ✓
                             Tier 2  Scale                   (2.1, 2.2, 2.3 done)
                             Tier 3  Parity with Device Sync (3.4 phase 1 done)
                             Tier 4  Beyond Device Sync
```

---

## Tier 0 -- Stop Losing Data

*Status: DONE*

Low-effort, high-value fixes to the three worst data integrity issues.

### 0.1 Checkpoint only advances on success

**File:** `smongo/sync.py` `_push()`

The push loop previously advanced the checkpoint to `last_key` regardless of whether the batch succeeded. If a `bulk_write` partially failed and the process restarted, the failed ops were silently skipped. Fixed: checkpoint now advances to `safe_key` only -- the last key from a fully successful batch.

### 0.2 Oplog seek via `search_near()`

**File:** `smongo/oplog.py` `OplogReader.read_from()`

`read_from()` previously iterated every oplog entry from the beginning to find the checkpoint key -- O(n) per sync cycle. Replaced with WiredTiger `search_near()` to seek directly to the checkpoint position. Now O(log n + k) where k is the number of new entries since checkpoint.

### 0.3 Per-op failure visibility in `_flush_bulk()`

**File:** `smongo/sync.py` `_flush_bulk()`

Previously logged the raw `BulkWriteError.details` blob and returned a boolean. Now:
- Extracts individual `writeErrors` and logs each with op index, error code, and message
- Returns the count of successfully written ops (not just pass/fail)
- Callers credit partial successes to `_pushed_count`

---

## Tier 1 -- Survive Crashes

*Status: DONE*

Crash-safe sync state, no ghost documents.

### 1.1 Transactional checkpoint + oplog compaction

**File:** `smongo/sync.py` `_atomic_checkpoint_and_compact()`

The checkpoint write and oplog truncation now execute inside a single WiredTiger transaction (`begin_transaction` / `commit_transaction`). A crash between the two triggers a rollback, so the checkpoint is never advanced past ops that weren't compacted. The new `_ck_lock` serializes checkpoint-session access for thread safety under concurrent push.

### 1.2 Persistent tombstones

**File:** `smongo/sync.py` `TombstoneRegistry`

`TombstoneRegistry` now accepts an optional WiredTiger `session` and `uri`. When provided (always in `SyncManager.__init__`), tombstones are stored in `table:__tombstones` (key=doc_id, value=deletion_timestamp). The `mark_deleted()` / `is_tombstoned()` / `expire()` API is unchanged. Without a session, the registry falls back to an in-memory dict for unit tests.

### 1.3 Resumable initial snapshot

**File:** `smongo/sync.py` `_pull_via_change_stream()`

The initial `find({})` snapshot is now paginated using `_id`-based cursor pagination with `sort("_id", 1).limit(page_size)`. After each page, the last `_id` is checkpointed to `pull_cs_page:{ns}`. On restart, the snapshot resumes from the checkpoint with `find({"_id": {"$gt": last_id}})`. Page size defaults to `batch_size`.

---

## Tier 2 -- Scale

*Effort: ~3-5 days each. Unlocks: high-throughput sync for large fleets.*

### 2.1 BSON oplog encoding -- DONE

**Files:** `smongo/oplog.py` `OplogWriter.log()`, `OplogReader`, `smongo/storage/collection.py`

Oplog entries are now stored as raw BSON bytes via the Rust encoder (`to_bson`/`from_bson` from `_smongo_core`). The oplog table format changed from `value_format=S` to `value_format=u`. Existing tables are auto-migrated (drop + recreate) on first startup. This cuts oplog storage roughly in half and removes Python JSON serialization from the hot path.

### 2.2 Concurrent namespace push -- DONE

**Files:** `smongo/sync.py` `_push()`, `_push_namespace()`

Push body extracted into `_push_namespace()` and dispatched via `ThreadPoolExecutor` when `push_concurrency > 1` and multiple namespaces are tracked. Per-namespace checkpointing was already in place. New `push_concurrency` config option (default: 4). Single-namespace syncs remain sequential to avoid thread-pool overhead.

### 2.3 Dead-letter queue for failed ops -- DONE

**Files:** `smongo/sync.py` `_flush_bulk()`, `_dlq_enqueue()`, `_sweep_dlq()`, `table:__sync_dlq`

Failed ops from `bulk_write` are now captured in `table:__sync_dlq` with the original oplog entry, error code, retry count, and next-retry timestamp. `_sweep_dlq()` runs at the start of each push cycle and retries eligible entries with exponential backoff (configurable via `dlq_backoff_base_sec` and `max_dlq_retries`). After exhausting retries, entries are marked `permanently_failed`. `status()` exposes `dlq_depth` and `dlq_permanent_failures`.

### 2.4 Backpressure and rate limiting

**Files:** `smongo/sync.py` `_push()`, `_pull()`

**Problem:** No throttling when the remote is under load. Backoff only kicks in on errors.

**Fix:** Add adaptive rate limiting:
- Track remote response latency per batch
- If latency exceeds a threshold, reduce batch size and increase interval
- Add a `max_ops_per_sec` config option for hard rate limits
- Expose current throughput in `status()`

---

## Tier 3 -- Parity with Device Sync

*Effort: ~1-2 weeks each. Unlocks: feature parity with what MongoDB deprecated.*

### 3.1 Server-side sync rule enforcement

**Problem:** MQL sync rules are currently self-enforced by the client. A malicious or buggy client can ignore them and sync anything.

**Fix:** Add a server-side validation layer. Options (in order of increasing complexity):
1. **Atlas Trigger:** An Atlas trigger that validates inbound documents against a rules collection and rejects non-conforming writes
2. **Sync proxy:** A lightweight service that sits between smongo and Atlas, validates sync payloads against MQL rules, and forwards only compliant writes
3. **Wire protocol middleware:** A handler in smongo's wire server that enforces rules on inbound sync traffic from other nodes

### 3.2 Schema evolution during sync

**Problem:** If the local schema changes (new fields, renamed fields), there's no negotiation with the remote. Documents arrive with different shapes.

**Fix:**
- Add a `_schemaVersion` field to synced documents
- Support additive migrations: new fields with defaults, field renames via alias
- Reject breaking changes (field type changes, removed required fields) during sync
- Store migration definitions in a `table:__schema_migrations` table
- Apply migrations on pull when `_schemaVersion` differs

### 3.3 Partial document sync (field projection)

**Problem:** smongo syncs full documents. Edge devices may only need a subset of fields.

**Fix:** Add a `sync_projection` option to sync rules:
```python
sync_config = {
    "sync_rules": {"device_id": "$$NODE_ID"},
    "sync_projection": {"readings": 1, "device_id": 1, "status": 1},
}
```
- Push: project locally before sending to remote (strip excluded fields)
- Pull: project remotely in the find/change-stream query
- Field-merge conflict resolution must be projection-aware

### 3.4 Sync progress API -- DONE (phase 1)

`status()` now returns:
- Per-collection sync state via `collections` dict (`last_push_ts`, `last_pull_ts`, `last_push_count`, `last_pull_count`)
- Throughput metric: `throughput_ops_sec` (ops/sec for the last sync cycle)
- Cycle timing: `last_cycle_duration_sec`

**Remaining (phase 2):** Initial snapshot progress, p50/p95/p99 latency, structured event log.

---

## Tier 4 -- Beyond Device Sync

*Effort: multi-week projects. Unlocks: capabilities Device Sync never had.*

### 4.1 Pluggable sync transports

**Problem:** Sync is hardcoded to PyMongo `bulk_write` / change streams over TCP. Constrained IoT devices may need MQTT; firewalled environments may need HTTP; high-throughput pipelines may want gRPC.

**Fix:** Abstract the sync transport behind a protocol interface:

```
┌──────────────────────────────────────────┐
│              SyncManager                  │
│  (oplog tailing, conflict resolution,    │
│   checkpoint, filtering -- unchanged)    │
└────────────────┬─────────────────────────┘
                 │  SyncTransport interface
         ┌───────┼───────┬──────────┐
         ▼       ▼       ▼          ▼
     PyMongo   gRPC    MQTT     HTTP/REST
    (current) (proto) (broker)  (webhook)
```

Each transport implements `push_batch(ops)`, `pull_changes(since)`, and `pull_initial_snapshot()`. The existing PyMongo transport becomes the default. New transports are pluggable via `sync_config["transport"]`.

### 4.2 Sync-aware aggregation

**Problem:** Aggregation pipelines run against local data only. No way to transparently query across local + remote.

**Fix:** Add a `$remoteLookup` stage (or extend `$lookup`) that fetches data from the remote Atlas cluster during a local pipeline execution. Use case: local IoT readings joined with remote reference data (device metadata, fleet configuration).

### 4.3 Fleet admin API

**Problem:** No way to remotely inspect or control sync state across a fleet of devices.

**Fix:** Add a fleet management protocol over the wire server:
- Central control plane queries each device's `sync.status()` via wire protocol commands
- Remote commands: trigger sync, pause/resume, update sync rules, rotate credentials
- Fleet dashboard: aggregate sync health across all devices (total lag, error rates, throughput)
- Device registration: new devices self-register with the control plane on first sync

### 4.4 Observability (OpenTelemetry / Prometheus)

**Problem:** `status()` is a Python dict. No structured metrics export, no alerting hooks, no distributed tracing.

**Fix:**
- Export sync metrics via OpenTelemetry SDK: `sync.push.ops`, `sync.pull.ops`, `sync.conflicts`, `sync.errors`, `sync.lag_seconds` (gauges and counters)
- Optional Prometheus `/metrics` endpoint on the wire server
- Span tracing for each sync cycle (push span, pull span, conflict resolution span)
- Structured JSON logging with correlation IDs linking sync events to specific ops

### 4.5 Encryption at rest

**Problem:** Oplog, checkpoint, and tombstone tables are plaintext in WiredTiger.

**Fix:** Enable WiredTiger's built-in encryption-at-rest via `WT_CONNECTION::open` config:
```
encryption=(name=rotn,keyid=...)
```
Or integrate with a KMS (AWS KMS, HashiCorp Vault) for key management. Add a `encryption` section to the smongo connection config that passes through to WiredTiger.

---

## Testing Milestones

Each tier should include test coverage before moving to the next:

| Tier | Key tests to add |
|------|-----------------|
| 0 | (Already exercised by existing unit + integration suite) |
| 1 | ~~Crash recovery simulation~~ (unit tested: rollback on truncation failure) |
| 1 | ~~Tombstone persistence across process restart~~ (unit tested) |
| 1 | ~~Interrupted initial snapshot resume~~ (unit tested) |
| 2 | ~~Concurrent push correctness under contention~~ (unit tested with barrier) |
| 2 | ~~DLQ retry lifecycle (fail, enqueue, retry, succeed or exhaust)~~ (unit tested) |
| 2 | ~~BSON oplog round-trip~~ (unit tested) |
| 2 | ~~Change stream pull snapshot + delete~~ (integration tested) |
| 2 | Rate limiting behavior under simulated remote latency |
| 3 | Server-side rule rejection (bad document blocked on Atlas side) |
| 3 | Schema migration on pull (v1 doc pulled into v2 local schema) |
| 3 | Partial document sync round-trip fidelity |
| 4 | Transport abstraction: run full sync suite over each transport |
| 4 | Fleet admin commands over wire protocol |
| 4 | OTel metrics emission and Prometheus scrape |

---

## The Vision

```
Today:  "SQLite for the MongoDB world" -- a local-first document engine
        that speaks MQL and syncs to Atlas.

Next:   The sync layer becomes the product. Every MongoDB deployment
        gets an embedded local tier -- edge, mobile, IoT, AI -- that
        syncs bidirectionally with zero config, zero managed services,
        and the same query language everywhere.

        No separate SDK. No proprietary protocol. No managed backend
        to sunset. Just the same engine, the same MQL, the same wire
        protocol, from the edge to the cloud.
```
