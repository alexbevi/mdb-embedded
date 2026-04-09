# smongo WASM browser demos

Browser tests for the `wasm-pack` bundle (`pkg/`). ES modules require **HTTP** (use `npm run serve` or Docker below).

## Choosing memory vs OPFS

Ship **one WASM binary**; pick storage by import and API:

| Goal | API | Module |
|------|-----|--------|
| Fast, ephemeral (benchmarks, SSR islands, tests) | `initSmongo()` → `new Database(name)` | Prefer [`smongo-browser.js`](smongo-browser.js) |
| Survives reloads (Chromium-class browsers, secure context) | `initOpfsDatabase(dbName, collections)` | Same — [`smongo-browser.js`](smongo-browser.js) |

**`smongo-browser.js` is the supported application entry:** it re-exports memory helpers from `wrapper.js` and persistence from `opfs-wrapper.js`. Multi-tab behavior, lifecycle, structured errors, and recovery are documented in [**PERSISTENCE-AND-LIFECYCLE.md**](PERSISTENCE-AND-LIFECYCLE.md).

## Build

From `rust/smongo-engine`:

```bash
wasm-pack build --target web --out-dir wasm/pkg --release
```

## Run locally

```bash
cd rust/smongo-engine/wasm
npm install
npm run serve
```

Open **http://127.0.0.1:8080/** (redirects to `demo/`).

## Run with Docker

From `rust/smongo-engine/wasm`:

```bash
# Build wasm first (see above), then:
docker compose up
```

Open **http://localhost:8080/demo/**.

## Demo pages (`demo/`)

| Page | Purpose |
|------|---------|
| `demo/index.html` | Hub |
| `demo/memory-crud.html` | In-memory CRUD via `smongo-browser.js` |
| `demo/benchmark.html` | MemBackend throughput (`smongo-browser.js`) |
| `demo/opfs-persistence.html` | OPFS write / read / **wipe** |
| `demo/opfs-multitab-shared.html` | Multi-tab RPC: owner + client tabs share one OPFS worker |
| `demo/opfs-multitab-handoff.html` | **Close database** releases lock for another tab |

OPFS flows use the **dedicated worker** (`opfs-worker.js`, loaded by `opfs-wrapper.js`). Demos import **`smongo-browser.js`**. Use **Chromium**; sync access handles require a worker.

**Multi-tab:** With the Web Locks API, the first tab to open a `dbName` becomes the **owner** (holds the lock + worker). Other tabs become **RPC clients** on `BroadcastChannel('smongo-opfs-rpc-' + dbName)` and forward operations to the owner. Without Web Locks, only single-tab owner mode is used.

### JavaScript API (canonical)

```javascript
import {
  initSmongo,
  Database,
  initOpfsDatabase,
  closeOpfsDatabase,
  wipeOpfsDatabaseDirectory,
  reconnectOpfsDatabase,
  assertValidDbName,
  OpfsError,
  OPFS_ERROR_CODES,
  OPFS_RPC_LIMITS,
  isOpfsError,
  configureOpfsDebug,
} from './smongo-browser.js';

const db = await initOpfsDatabase('myDb', ['collectionA']);
await db.collection('collectionA').insertOne({ x: 1 });

// Release lock + worker without deleting files (another tab can become owner)
await closeOpfsDatabase('myDb');

// Close handles then delete OPFS directory (owner or client via RPC)
await wipeOpfsDatabaseDirectory('myDb');

// After owner tab closed or RPC failures: full re-handshake (owner closes worker+lock first)
await reconnectOpfsDatabase('myDb', ['collectionA']);
```

### Enterprise / production notes

- **Trust boundary:** `BroadcastChannel` is same-origin only; hostile same-origin iframes can still post messages. This layer enforces allow-listed ops, plain-object RPC envelopes, payload weight/depth caps, and per-database in-flight RPC limits. Isolate sensitive apps on a dedicated origin and use CSP / COOP+COEP where appropriate.
- **Structured errors:** Operations throw `OpfsError` with a stable `code` matching `OPFS_ERROR_CODES`. When the engine wraps an underlying failure, `cause` is set; RPC responses may include `errorCode` for the same strings. Use `isOpfsError(e)` and switch on `e.code` for retries, circuit-breaking, or telemetry.
- **Limits:** See `OPFS_RPC_LIMITS` (payload weight, nesting, client RPC concurrency, ping/backoff, **worker message timeout**). Hung worker replies no longer block the main thread indefinitely.
- **Multi-database:** In-flight RPC waiters are **scoped per `dbName`**; losing one owner does not reject pending calls for another database in the same tab.
- **Debug:** `configureOpfsDebug({ enabled: true })` turns on verbose internal logging (default off).

## Automation

```bash
npm run test:e2e
```

Playwright serves this directory and loads `tests/opfs-multitab-harness.html`.

## Layout

- **`smongo-browser.js`** (+ **`smongo-browser.d.ts`**) — **canonical app entry**; memory + OPFS
- `wrapper.js` / `wrapper.d.ts` — BSON + `WasmDatabase` (in-memory only)
- `opfs-wrapper.js` / `opfs-wrapper.d.ts` / `opfs-worker.js` — OPFS + `WasmOpfsDatabase`
- `pkg/` — `wasm-pack` output (not committed in some setups)
- `docker-compose.yml` — static `nginx:alpine` on port 8080

See `../../ROADMAP.md` (Part 2 — WASM) for WASM planning notes.
