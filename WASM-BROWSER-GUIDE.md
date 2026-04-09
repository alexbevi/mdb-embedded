# smongo WASM/JS Browser Guide

Production best practices for running smongo as a **local-first embedded database
inside the browser** -- from first `npm install` through secure Atlas sync via a
proxy relay.

> **Version:** 0.9.8 | **Reference runtime:** Chromium 120+ (secure context)

---

## Table of contents

1. [Quick start](#quick-start)
2. [Storage modes](#storage-modes)
3. [Full API cheat-sheet](#full-api-cheat-sheet)
4. [Secure sync proxy](#secure-sync-proxy)
5. [Content Security Policy (CSP)](#content-security-policy-csp)
6. [Multi-tab coordination](#multi-tab-coordination)
7. [Error handling](#error-handling)
8. [Performance](#performance)
9. [Deployment checklist](#deployment-checklist)
10. [Platform support matrix](#platform-support-matrix)
11. [FAQ](#faq)

---

## Quick start

### 1. Build the WASM bundle

```bash
# From the repo root
make build-wasm
# Produces: rust/smongo-engine/wasm/pkg/
```

### 2. Copy artifacts into your project

```
your-app/
  public/
    smongo/
      pkg/                        # wasm-pack output
        smongo_engine.js
        smongo_engine_bg.wasm
        smongo_engine.d.ts
      smongo-browser.js           # canonical entry point
      wrapper.js                  # memory helpers
      opfs-wrapper.js             # OPFS + multi-tab
      opfs-worker.js              # dedicated worker
      smongo-browser.d.ts         # TypeScript types
      opfs-wrapper.d.ts
      wrapper.d.ts
```

### 3. Import and use

```javascript
import {
  initSmongo,
  Database,
  initOpfsDatabase,
  closeOpfsDatabase,
} from './smongo/smongo-browser.js';

// In-memory (instant, ephemeral)
await initSmongo();
const db = new Database('scratch');
const users = db.collection('users');
users.insertOne({ name: 'Alice', age: 30 });
console.log(users.find({ age: { $gte: 18 } }));

// OPFS (persistent across reloads, Chromium only)
const pdb = await initOpfsDatabase('myapp', ['users', 'sessions']);
const pcoll = pdb.collection('users');
await pcoll.insertOne({ name: 'Bob', age: 25 });
console.log(await pcoll.find({}));
```

---

## Storage modes

Ship **one WASM binary**; pick the storage mode at runtime:

| Mode | Persistence | Thread | Init |
|------|------------|--------|------|
| **Memory** | None (tab lifetime) | Main thread, sync | `await initSmongo(); new Database(name)` |
| **OPFS** | Survives reloads | Dedicated worker, async | `await initOpfsDatabase(name, collections)` |

**Memory** is ideal for scratch pads, benchmarks, SSR islands, tests, and
runtimes without OPFS (Firefox, Safari, Cloudflare Workers).

**OPFS** is ideal for production local-first apps on Chromium-class browsers
where data must survive navigation and reloads.

Both modes share the same MongoDB-compatible query language, aggregation
pipeline, index engine, and BSON serialization.

---

## Full API cheat-sheet

### Memory (synchronous, main thread)

```javascript
import { initSmongo, Database } from './smongo-browser.js';
await initSmongo();

const db    = new Database('mydb');
const coll  = db.collection('items');

// CRUD
coll.insertOne({ x: 1 });
coll.insertMany([{ x: 2 }, { x: 3 }]);
coll.findOne({ x: 1 });
coll.find({ x: { $gte: 2 } });
coll.findWithOptions({ x: { $gte: 1 } }, { limit: 10, sort: { x: -1 }, projection: { x: 1 } });
coll.countDocuments({ x: { $gte: 2 } });
coll.updateOne({ x: 1 }, { $set: { x: 10 } });
coll.updateMany({}, { $inc: { x: 1 } });
coll.deleteOne({ x: 10 });
coll.deleteMany({ x: { $lt: 0 } });

// Aggregation
coll.aggregate([
  { $match: { x: { $gte: 1 } } },
  { $group: { _id: null, total: { $sum: '$x' } } },
]);

// Indexes
coll.createIndex({ x: 1 }, { unique: true });
coll.listIndexes();
coll.dropIndex('x_1');

// Database
db.listCollectionNames();
db.dropCollection('items');
db.stats();
```

### OPFS (asynchronous, worker-backed)

```javascript
import {
  initOpfsDatabase,
  closeOpfsDatabase,
  reconnectOpfsDatabase,
  wipeOpfsDatabaseDirectory,
} from './smongo-browser.js';

const db   = await initOpfsDatabase('myapp', ['users']);
const coll = db.collection('users');

// Same operations, but every call returns a Promise
await coll.insertOne({ name: 'Alice' });
const docs = await coll.find({ name: 'Alice' });
await coll.aggregate([{ $count: 'total' }]);

// Lifecycle
await closeOpfsDatabase('myapp');                  // release lock, keep files
await wipeOpfsDatabaseDirectory('myapp');           // delete OPFS files
await reconnectOpfsDatabase('myapp', ['users']);    // re-acquire or become client
```

---

## Secure sync proxy

The WASM engine runs entirely client-side. It **cannot** open a direct TCP
connection to MongoDB Atlas (or any MongoDB deployment). To synchronize local
data with a remote cluster, route traffic through a thin server-side relay.

### Architecture

```
Browser                          Your Server                     Atlas
+-----------------------+        +-------------------+        +--------+
| smongo-browser (WASM) |  HTTPS | Sync Proxy        |  TCP   | MongoDB|
|   OPFS / Memory       | -----> | Auth + Rate-limit | -----> | Atlas  |
|   Local reads & writes|  fetch | Validate + Forward|        |        |
+-----------------------+        +-------------------+        +--------+
```

The browser never holds a connection string, credentials, or direct network
access to the database. The proxy is the trust boundary.

### Why a proxy?

1. **Credentials stay server-side.** Atlas connection strings, API keys, and
   x.509 certs never reach `localStorage`, `IndexedDB`, or JS memory.
2. **Validation.** The proxy can reject malformed ops, enforce schema, and
   apply rate limits before they touch the cluster.
3. **Bandwidth control.** Delta sync, batching, and compression happen at the
   proxy so the browser sends minimal payloads over mobile networks.
4. **Audit trail.** The proxy can log every sync operation for compliance.

### Reference proxy (Express / Node)

```javascript
// sync-proxy.mjs
import express from 'express';
import { MongoClient } from 'mongodb';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';

const app = express();
app.use(helmet());
app.use(express.json({ limit: '1mb' }));

// Rate limit: 100 sync requests per 15-minute window per IP
app.use('/api/sync', rateLimit({ windowMs: 15 * 60_000, max: 100 }));

const client = new MongoClient(process.env.ATLAS_URI);
await client.connect();

// POST /api/sync/push  — browser pushes local changes
app.post('/api/sync/push', authenticate, async (req, res) => {
  const { collection, ops } = req.body;

  // Validate: collection name, op shape, payload size
  if (!isValidCollectionName(collection)) return res.status(400).json({ error: 'bad collection' });
  if (!Array.isArray(ops) || ops.length > 500) return res.status(400).json({ error: 'bad ops' });

  const db = client.db(req.user.database);
  const coll = db.collection(collection);
  const bulkOps = ops.map(toBulkWriteOp);         // map client ops to MongoDB bulk ops
  const result = await coll.bulkWrite(bulkOps);
  res.json({ matched: result.matchedCount, modified: result.modifiedCount });
});

// GET /api/sync/pull?collection=X&since=<ISO>  — browser pulls remote changes
app.get('/api/sync/pull', authenticate, async (req, res) => {
  const { collection, since } = req.query;
  if (!isValidCollectionName(collection)) return res.status(400).json({ error: 'bad collection' });

  const db = client.db(req.user.database);
  const coll = db.collection(collection);
  const docs = await coll
    .find({ _updatedAt: { $gt: new Date(since) } })
    .sort({ _updatedAt: 1 })
    .limit(1000)
    .toArray();
  res.json({ docs, checkpoint: new Date().toISOString() });
});

app.listen(3001, () => console.log('Sync proxy on :3001'));
```

### Browser-side sync loop

```javascript
import { initOpfsDatabase } from './smongo-browser.js';

const db = await initOpfsDatabase('myapp', ['tasks']);
const tasks = db.collection('tasks');

async function syncPull() {
  const checkpoint = localStorage.getItem('sync:tasks:checkpoint') ?? '1970-01-01T00:00:00Z';
  const res = await fetch(`/api/sync/pull?collection=tasks&since=${checkpoint}`, {
    headers: { Authorization: `Bearer ${getToken()}` },
  });
  const { docs, checkpoint: newCp } = await res.json();

  for (const doc of docs) {
    const existing = await tasks.findOne({ _id: doc._id });
    if (existing) {
      await tasks.updateOne({ _id: doc._id }, { $set: doc });
    } else {
      await tasks.insertOne(doc);
    }
  }
  localStorage.setItem('sync:tasks:checkpoint', newCp);
}

async function syncPush() {
  // Collect local pending changes (track with a _dirty flag or oplog)
  const pending = await tasks.find({ _dirty: true });
  if (pending.length === 0) return;

  const res = await fetch('/api/sync/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${getToken()}`,
    },
    body: JSON.stringify({
      collection: 'tasks',
      ops: pending.map((doc) => ({ op: 'upsert', filter: { _id: doc._id }, doc })),
    }),
  });

  if (res.ok) {
    await tasks.updateMany({ _dirty: true }, { $unset: { _dirty: '' } });
  }
}

// Periodic sync (adjust interval for your use case)
setInterval(async () => {
  try { await syncPull(); } catch (e) { console.warn('pull failed', e); }
  try { await syncPush(); } catch (e) { console.warn('push failed', e); }
}, 30_000);

// Also sync on visibility change (tab becomes active)
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    syncPull().catch(console.warn);
  }
});
```

### Proxy hardening checklist

- [ ] **Authentication.** Every request carries a JWT or session token; the proxy
      verifies before touching MongoDB.
- [ ] **Authorization.** Per-user or per-tenant database/collection scoping --
      never let the browser choose an arbitrary namespace.
- [ ] **Input validation.** Reject unknown fields, over-size documents, deeply
      nested objects, and `$`-prefixed keys that could inject MongoDB operators.
- [ ] **Rate limiting.** Per-IP and per-user limits (e.g. 100 ops / 15 min).
- [ ] **TLS everywhere.** HTTPS between browser and proxy; TLS between proxy and
      Atlas (Atlas enforces this by default).
- [ ] **CORS.** Lock `Access-Control-Allow-Origin` to your app's exact origin.
- [ ] **Payload limits.** `express.json({ limit: '1mb' })` or equivalent.
- [ ] **Idempotency.** Use `_id`-based upserts so retries are safe.
- [ ] **Conflict resolution.** Choose a strategy: last-write-wins, field-level
      merge, or CRDT (smongo's Python `sync` module implements these; port the
      logic to your proxy if needed).

---

## Content Security Policy (CSP)

WASM requires `'wasm-eval'` (or `'wasm-unsafe-eval'` on older engines). A
production-grade CSP for smongo-browser:

```
Content-Security-Policy:
  default-src 'self';
  script-src  'self' 'wasm-unsafe-eval';
  worker-src  'self';
  connect-src 'self' https://your-sync-proxy.example.com;
  style-src   'self' 'unsafe-inline';
  img-src     'self' data:;
```

| Directive | Why |
|-----------|-----|
| `script-src 'wasm-unsafe-eval'` | Required for `WebAssembly.instantiateStreaming` |
| `worker-src 'self'` | OPFS dedicated worker loads from same origin |
| `connect-src` | Sync proxy fetch; lock to your API domain |

### Cross-Origin headers for OPFS + SharedArrayBuffer

If you use `SharedArrayBuffer` (e.g. for future multi-threaded WASM), you also
need:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

For OPFS-only (no SharedArrayBuffer), these are not required but recommended for
defense in depth.

### nginx snippet

```nginx
server {
    listen 443 ssl http2;
    server_name app.example.com;

    # WASM MIME type (critical for streaming compilation)
    types { application/wasm wasm; }

    # Security headers
    add_header Content-Security-Policy
        "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; worker-src 'self'; connect-src 'self' https://api.example.com"
        always;
    add_header Cross-Origin-Opener-Policy  same-origin always;
    add_header Cross-Origin-Embedder-Policy require-corp always;
    add_header X-Content-Type-Options nosniff always;

    location /smongo/ {
        root /var/www/app/public;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
```

---

## Multi-tab coordination

OPFS sync access handles are **exclusive per file across all tabs** (browser
constraint, not smongo). The OPFS layer handles this transparently:

```
Tab 1: initOpfsDatabase('myapp', ['users'])
  → acquires Web Lock "smongo-opfs-myapp"
  → spawns dedicated worker with OPFS handles
  → becomes OWNER, starts RPC server

Tab 2: initOpfsDatabase('myapp', ['users'])
  → Web Lock busy → pings Tab 1 via BroadcastChannel
  → becomes RPC CLIENT (same API, higher latency)

Tab 1 closes:
  → lock released, RPC clients get OPFS_OWNER_LOST
  → Tab 2 calls reconnectOpfsDatabase() → becomes new OWNER
```

### Recommended reconnect pattern

```javascript
import {
  initOpfsDatabase,
  reconnectOpfsDatabase,
  isOpfsError,
  OPFS_ERROR_CODES,
} from './smongo-browser.js';

let db = await initOpfsDatabase('myapp', ['users']);

// Handle owner loss (from any async operation)
async function safeOp(fn) {
  try {
    return await fn();
  } catch (e) {
    if (isOpfsError(e) && (
      e.code === OPFS_ERROR_CODES.OWNER_LOST ||
      e.code === OPFS_ERROR_CODES.OWNER_UNAVAILABLE ||
      e.code === OPFS_ERROR_CODES.RPC_TIMEOUT
    )) {
      db = await reconnectOpfsDatabase('myapp', ['users']);
      return await fn();  // retry once
    }
    throw e;
  }
}

// Usage
const docs = await safeOp(() => db.collection('users').find({}));
```

### bfcache recovery

```javascript
window.addEventListener('pageshow', (e) => {
  if (e.persisted) {
    reconnectOpfsDatabase('myapp', ['users']).then((newDb) => {
      db = newDb;
    });
  }
});
```

---

## Error handling

All OPFS errors are instances of `OpfsError` with a stable `.code`:

```javascript
import { isOpfsError, OPFS_ERROR_CODES } from './smongo-browser.js';

try {
  await coll.insertOne(doc);
} catch (e) {
  if (isOpfsError(e)) {
    switch (e.code) {
      case OPFS_ERROR_CODES.OWNER_LOST:
      case OPFS_ERROR_CODES.RPC_TIMEOUT:
        // reconnect and retry
        break;
      case OPFS_ERROR_CODES.INVALID_PAYLOAD:
        // fix the document
        break;
      default:
        // log e.code for telemetry (not e.message)
        break;
    }
  }
}
```

For the full error code table, see
[PERSISTENCE-AND-LIFECYCLE.md](rust/smongo-engine/wasm/PERSISTENCE-AND-LIFECYCLE.md#structured-errors-and-recovery-cookbook).

---

## Performance

### Tips

- **Indexes matter.** `createIndex({ email: 1 }, { unique: true })` before
  querying -- the WASM engine uses B-tree indexes just like the native build.
- **Batch writes.** `insertMany` is significantly faster than N x `insertOne`.
- **Limit result sets.** Use `findWithOptions` with `limit` and `projection` to
  avoid deserializing large documents on the main thread.
- **OPFS vs Memory.** OPFS adds ~1-3ms per operation (worker postMessage round
  trip). For write-heavy hot paths, consider batching into fewer calls.
- **WASM binary size.** The release build is ~1.5-2 MB gzipped. Use
  `wasm-opt -Oz` (included in `make build-wasm`) and serve with
  `Content-Encoding: gzip` or `br`.

### Benchmarking

Open `demo/benchmark.html` to run in-memory throughput tests in your target
browser, or use the Playwright harness:

```bash
cd rust/smongo-engine/wasm
npm run test:e2e
```

---

## Deployment checklist

```
Pre-flight
  [ ] Built with `make build-wasm` (release + wasm-opt)
  [ ] WASM file served with `Content-Type: application/wasm`
  [ ] CSP allows 'wasm-unsafe-eval' and 'self' for worker-src
  [ ] Sync proxy deployed behind HTTPS with auth + rate-limiting
  [ ] CORS locked to your app's origin
  [ ] Tested on target browsers (Chromium for OPFS; memory fallback elsewhere)

Runtime
  [ ] initOpfsDatabase called once per dbName per tab
  [ ] reconnectOpfsDatabase wired to OPFS_OWNER_LOST handler
  [ ] pageshow listener handles bfcache restore
  [ ] Sync loop uses exponential backoff on failure
  [ ] Error telemetry emits e.code, not e.message
```

---

## Platform support matrix

| Runtime | Memory | OPFS | Multi-tab RPC | Notes |
|---------|--------|------|---------------|-------|
| Chrome / Edge 120+ | Yes | Yes | Yes (Web Locks) | Reference platform |
| Chrome / Edge 102-119 | Yes | Yes | Partial | Verify `FileSystemSyncAccessHandle` in worker |
| Firefox 111+ | Yes | No | No | OPFS sync handles not available in workers |
| Safari 17+ | Yes | No | No | OPFS sync handles not available in workers |
| Electron (Chromium) | Yes | Yes | Yes | Match Chrome version |
| Cloudflare Workers | Yes | No | N/A | In-memory only; no OPFS |
| Node.js | Use `@smongo/embedded` instead | -- | -- | Native N-API binding (redb) |

---

## FAQ

### Can the browser connect directly to Atlas?

No. Browsers cannot open raw TCP sockets. You need a sync proxy (HTTPS relay)
between the browser and MongoDB. See [Secure sync proxy](#secure-sync-proxy).

### What happens when the user is offline?

All reads and writes hit the local WASM database (memory or OPFS). When
connectivity returns, your sync loop pushes pending changes and pulls remote
updates. The database is fully functional offline.

### Can I use this with React / Vue / Svelte?

Yes. The WASM engine is framework-agnostic. Import `smongo-browser.js` in your
app entry point, initialize once, and pass the database handle through context
or a store. Example with React:

```javascript
// db.js
import { initOpfsDatabase } from './smongo/smongo-browser.js';
export const dbPromise = initOpfsDatabase('myapp', ['todos']);

// App.jsx
import { dbPromise } from './db.js';
const db = await dbPromise;
const todos = db.collection('todos');
// ... use in components
```

### How big can the OPFS database get?

OPFS quota is origin-scoped and varies by browser. Chromium typically allows up
to 60% of the disk (shared with other storage APIs). Check available quota with:

```javascript
const { usage, quota } = await navigator.storage.estimate();
console.log(`Using ${usage} of ${quota} bytes`);
```

### Is the data encrypted at rest?

OPFS files are stored by the browser and are **not encrypted by default**. For
sensitive data, encrypt documents before inserting:

```javascript
const encrypted = await encrypt(JSON.stringify(doc));
await coll.insertOne({ _id: doc._id, _enc: encrypted });
```

Or use the Web Crypto API with per-user keys derived from a passphrase.

### How do I migrate from IndexedDB to smongo OPFS?

Read your IndexedDB data, transform to plain objects, and bulk-insert:

```javascript
const idbData = await readFromIndexedDB();  // your migration function
const coll = opfsDb.collection('migrated');
await coll.insertMany(idbData);
```

---

## Further reading

- [PERSISTENCE-AND-LIFECYCLE.md](rust/smongo-engine/wasm/PERSISTENCE-AND-LIFECYCLE.md) --
  storage lifecycle, recovery APIs, error code table
- [OPFS-ARCHITECTURE.md](rust/smongo-engine/wasm/OPFS-ARCHITECTURE.md) --
  constraints, multi-tab design rationale
- [wasm/README.md](rust/smongo-engine/wasm/README.md) -- build, demos, full API
  reference
- [ARCHITECTURE.md](ARCHITECTURE.md) -- overall project architecture
- [BINDING-PARITY.md](BINDING-PARITY.md) -- API coverage across
  Python/Node/C/WASM
