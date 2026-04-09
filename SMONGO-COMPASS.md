# MongoDB Compass + smongo

**The same Compass you use in production, pointing at the same engine that runs on the edge.**

---

## The Story

MongoDB runs in the cloud. It powers your Atlas cluster, your replica sets, your sharded deployments. The tools you use to work with it — Compass, mongosh, PyMongo, the Node.js driver — all speak the same binary protocol over TCP. That protocol is the lingua franca of the MongoDB ecosystem.

smongo brings the MongoDB experience to the edge: **`smongo-engine` + redb** for embedded storage, the full MQL query language, a rich aggregation pipeline, B-tree indexes, and an oplog that syncs bidirectionally with Atlas. An engine without connectivity is a silo — the wire protocol server breaks that boundary by letting standard MongoDB tools connect over TCP, as if the embed were a `mongod`.

Compass is the proof. When it connects to smongo and renders databases, collections, documents, indexes, and aggregation results with no special configuration, no adapter, no translation layer — that's the confirmation that the edge engine speaks the same language as the cloud.

```
    ┌─────────────────────────────────────────────────────────────────┐
    │                    THE SAME EVERYWHERE                           │
    │                                                                 │
    │  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
    │  │ MongoDB  │    │  mongosh │    │  PyMongo │    │ Node.js  │  │
    │  │ Compass  │    │          │    │  Driver  │    │ Driver   │  │
    │  └────┬─────┘    └────┬─────┘    └────┬─────┘    └────┬─────┘  │
    │       │               │               │               │        │
    │       └───────────────┴───────┬───────┴───────────────┘        │
    │                               │                                 │
    │                        MongoDB Wire Protocol                    │
    │                        OP_MSG over TCP                          │
    │                               │                                 │
    │              ┌────────────────┼────────────────┐                │
    │              ▼                                 ▼                │
    │     ┌────────────────┐               ┌────────────────┐        │
    │     │   Atlas / mongod│               │  smongo Wire   │        │
    │     │   (cloud)       │◄── sync ──►  │  Server (edge) │        │
    │     │                │               │                │        │
    │     │  Wire v0–21    │               │  Wire v0–21   │        │
    │     │  OP_MSG        │               │  OP_MSG       │        │
    │     │  80+ commands  │               │  80+ commands │        │
    │     └────────────────┘               └───────┬────────┘        │
    │                                              │                 │
    │                                       ┌──────┴───────┐         │
    │                                       │ smongo-engine│         │
    │                                       │ + redb file  │         │
    │                                       └──────────────┘         │
    └─────────────────────────────────────────────────────────────────┘
```

One protocol. One query language. One set of tools. Cloud and edge.

---

## Quick Start: Docker Compose (Compass-Ready)

The fastest path. One command gives you the web dashboard, the wire server, and a real MongoDB for sync — with Compass connectivity out of the box.

```bash
docker compose up --build
```

Then open Compass and connect:

```
mongodb://localhost:27018
```

Sample data (10 employees, 5 indexes) is auto-seeded. Sync to the real MongoDB container is already running.

### What's Running

```
Your Machine
│
├── localhost:5000   ──►  Web dashboard (Flask GUI)
├── localhost:27017  ──►  Real MongoDB 7 container (sync target, stands in for Atlas)
└── localhost:27018  ──►  smongo wire server (Compass connects here)
                          │
                          ├── redb-backed engine on disk
                          ├── Bidirectional sync to :27017
                          └── Same data the web dashboard shows
```

| Port | Service | Compass? | Purpose |
|---|---|---|---|
| `5000` | Web dashboard | No (HTTP) | Browser-based GUI for the engine |
| `27017` | Real MongoDB container | Yes | Sync target (stands in for Atlas) |
| **`27018`** | **smongo wire server** | **Yes** | **The embedded engine — connect here** |

You can connect Compass to both `27017` and `27018` simultaneously to compare the real MongoDB and the embedded engine side by side. After sync completes, they show the same data.

### How It Works Inside the Container

The `docker-compose.yml` sets `WIRE_PORT=27018` and `WIRE_HOST=0.0.0.0`. When `web_app.py` starts, it launches the wire server **in the same process** as Flask, sharing the same **`RedbClient` / `RedbLocalClient`** via the `local_client` parameter. No second process, no duplicate open of the database path, no duplicate data.

```yaml
services:
  app:
    ports:
      - "5000:5000"     # web dashboard
      - "27018:27018"   # wire protocol (Compass connects here)
    environment:
      WIRE_PORT: "27018"
      WIRE_HOST: "0.0.0.0"
```

If the wire server fails to start (port conflict, etc.), the web dashboard continues running normally — the failure is logged but doesn't crash Flask.

### Verifying the Connection

After `docker compose up`, look for this line in the logs:

```
app-1  | Wire server listening on 0.0.0.0:27018 (Compass-ready)
```

If it's missing, check that `WIRE_PORT` is set in the `environment` block.

---

## Quick Start: Standalone (No Docker)

### Start the wire server

```bash
python -m smongo.wire --port 27017
```

Output:

```
smongo wire server on 127.0.0.1:27017  (db path: e.g. `./local_data`)  -- small but mighty
```

### Connect from Compass

```
mongodb://localhost:27017
```

No auth. No TLS. No replica set config. (The Rust `RustWireServer` supports SCRAM-SHA-256 and TLS if needed.)

### CLI Flags

| Flag | Default | Description |
|---|---|---|
| `--port` | `27017` | TCP port to listen on |
| `--host` | `127.0.0.1` | Bind address (`0.0.0.0` for external access) |
| `--db-path` | `local_data` | Directory / file path for the embedded redb database |
| `-v` / `--verbose` | off | Debug-level logging |

### Alternative: Installed Entry Point

```bash
pip install -e .
smongo-wire --port 27017
```

### Alternative: Python API

```python
from smongo.wire import WireServer

with WireServer("./my_data", port=27017) as srv:
    input("Press Enter to stop...")
```

### Alternative: Wire Server + Atlas Sync

```python
from smongo.wire import WireServer

with WireServer(
    db_path="./my_data",
    port=27017,
    sync="mongodb+srv://user:pass@cluster.mongodb.net",
) as srv:
    input("Wire + sync running. Enter to stop...")
```

Compass browses the local engine while mutations flow to and from Atlas in the background.

---

## What Compass Sees

When Compass connects, smongo advertises itself as a MongoDB 7.0-compatible server. Compass enables its full modern feature set.

### The Handshake

```
Compass                                     smongo
  │                                           │
  │  OP_QUERY: {isMaster: 1}                 │
  │ ─────────────────────────────────────►    │
  │                                           │
  │  OP_REPLY: {                              │
  │    ismaster: true,                        │
  │    isWritablePrimary: true,               │
  │    maxWireVersion: 21,                    │
  │    maxBsonObjectSize: 16777216,           │
  │    version: "7.0.0-smongo",              │
  │    modules: ["embedded", "redb"],   │
  │    ok: 1.0                                │
  │  }                                        │
  │ ◄─────────────────────────────────────    │
  │                                           │
  │  OP_MSG: {listDatabases: 1}               │
  │ ─────────────────────────────────────►    │
  │                                           │
  │  OP_MSG: {databases: [...], ok: 1.0}      │
  │ ◄─────────────────────────────────────    │
  │                                           │
  │  (Compass renders the database sidebar)   │
```

### Feature Map

Every major Compass feature maps to wire protocol commands that smongo handles:

| Compass Feature | Wire Command(s) | Notes |
|---|---|---|
| Database sidebar | `listDatabases` | Size-on-disk stats from the engine |
| Collection list | `listCollections` | Types, UUIDs, options |
| Browse documents | `find` + `getMore` | Paginated, proper BSON types (ObjectId, Date, etc.) |
| Insert document | `insert` | Persisted to redb with oplog entry |
| Edit document | `findAndModify` / `update` | In-place field edits |
| Delete document | `delete` | With oplog entry for sync |
| Query filter bar | `find` with `filter` | Full MQL: `$gt`, `$in`, `$regex`, `$elemMatch`, ... |
| Sort / Project | `find` with `sort` / `projection` | Multi-key sort, field inclusion/exclusion |
| Aggregation builder | `aggregate` | 25+ stages including `$lookup`, `$facet`, `$vectorSearch` |
| Indexes tab | `listIndexes` / `createIndexes` / `dropIndexes` | B-tree indexes with unique, sparse, TTL |
| Explain plan | `explain` | INDEX SCAN / PK LOOKUP / COLL SCAN with scoring |
| Schema analysis | `find` (sampling) | Compass samples documents to infer field types |
| Server stats | `serverStatus` / `hostInfo` / `buildInfo` | `storageEngine` (`redb`), uptime, memory, connections |
| mongosh shell | Any command | Full 80+ command set |

---

## Connection Settings

### Connection Strings

```
mongodb://localhost:27017                         # standalone wire server
mongodb://localhost:27018                         # Docker Compose (default)
mongodb://localhost:27017/myapp                   # with default database
mongodb://localhost:27017/?directConnection=true  # explicit direct mode
```

### Compass GUI Settings

| Setting | Value | Reason |
|---|---|---|
| **Authentication** | None (default Python `WireServer`) or SCRAM-SHA-256 (`RustWireServer`) | Default mode has no auth; Rust wire server supports SCRAM |
| **TLS/SSL** | Off (default) or On (`RustWireServer` with cert/key) | Default mode has no TLS; Rust wire server supports rustls |
| **Direct Connection** | On | Standalone server, not a replica set |
| **Read Preference** | Primary | Single node — only primary exists |

---

## Edge Cases and Troubleshooting

### Authentication Failed

**Symptom**: Error immediately after connecting.

**Cause**: Credentials in the URI (`mongodb://user:pass@localhost`) or saved Compass auth settings when connecting to the default Python `WireServer` (which has no auth).

The default Python `WireServer` returns error code 18 (`AuthenticationFailed`) with a clear message:

> *"smongo embedded mode does not support authentication. Connect without credentials (remove username/password from your URI)."*

**Fix**: Remove credentials from the URI and set Authentication to **None** in Compass. Alternatively, use the Rust `RustWireServer` which supports SCRAM-SHA-256 authentication.

---

### Connection Refused

**Cause & fix** — check in order:

| Check | Verify | Fix |
|---|---|---|
| Server not running | No startup message in terminal | Start the wire server |
| Port conflict | `lsof -i :27017` shows another process | Use `--port 27018` or stop the other process |
| Bind address | Server says `127.0.0.1`, connecting from another machine | Use `--host 0.0.0.0` |
| Firewall | macOS "accept incoming connections" dialog | Allow the connection |

---

### Port Already in Use

**Symptom**: `OSError: [Errno 48] Address already in use`

Another MongoDB (Docker, Homebrew, previous smongo) is on that port.

```bash
# Use a different port
python -m smongo.wire --port 27018

# Or stop the conflicting service
brew services stop mongodb-community
docker stop <container_id>

# Or find and kill
lsof -i :27017
kill <PID>
```

---

### Empty Databases

**Symptom**: Connection works but no databases appear.

**Cause**: The `--db-path` points to an empty or different directory than where your app writes.

```bash
# Match the path your app uses:
python -m smongo.wire --db-path ./my_data --port 27017
```

---

### "Unknown" Server Type or "Not Primary"

**Cause**: Some Compass versions expect a replica set.

**Fix**: Add `directConnection=true`:

```
mongodb://localhost:27017/?directConnection=true
```

---

### Idle Connection Drops (5 Minutes)

The wire server closes sockets after 300 seconds of inactivity. Compass automatically reconnects on the next interaction. This is a server-side constant (`CONNECTION_TIMEOUT_SEC` in `smongo/wire/server.py`).

---

### Too Many Connections

smongo limits to 1,024 concurrent connections. Compass opens 2–5 per window. Configurable via the Python API:

```python
server = WireServer(db_path="./data", port=27017, max_connections=2048)
```

---

### Unsupported Aggregation Stage

smongo supports 25+ stages but not the entire MongoDB catalog. `$search` (Atlas Search) and `$changeStream` are not available.

**Supported stages** (all work in the Compass aggregation builder):

`$match` · `$group` · `$project` · `$sort` · `$limit` · `$skip` · `$unwind` · `$lookup` · `$graphLookup` · `$unionWith` · `$addFields` / `$set` · `$count` · `$replaceRoot` / `$replaceWith` · `$sample` · `$bucket` · `$bucketAuto` · `$sortByCount` · `$redact` · `$setWindowFields` · `$unset` · `$vectorSearch` · `$facet` · `$out` · `$merge`

---

## Multiple Apps, One Machine

The embedded engine opens a **single redb database** at a path you choose. **Only one process** should own that path at a time — a second opener (another wire server or another embed in a different process) will conflict. TCP ports are separate: many Compass / PyMongo clients can share **one** wire server process.

### The Rule

```
Process A opens local://./my_data   →  ✅  owns the redb file
Process B opens the same path       →  ❌  conflict (second process)
```

Use **one wire server** (or one Python process with a shared `RedbClient`) per database path.

### Every Scenario

```
Scenario                                         Result
─────────────────────────────────────────────────────────────────────
Two wire servers, same db-path, same port        ❌ Port conflict
Two wire servers, same db-path, different ports  ❌ Second open of same path
Two wire servers, diff db-path, same port        ❌ Port conflict
Two wire servers, diff db-path, different ports  ✅ Independent databases
Wire server + embedded client, same db-path      ❌ Two owners (unless same process — see below)
Multiple Compass windows → one wire server       ✅ Each gets its own TCP connection
Compass + PyMongo → one wire server              ✅ Both connect over TCP
Web dashboard + wire server in same process      ✅ Shared RedbClient / RedbLocalClient
```

### The Safe Pattern

Route everything through one wire server:

```
┌────────────┐  ┌────────────┐  ┌────────────┐
│  Compass   │  │  PyMongo   │  │  mongosh   │
└─────┬──────┘  └─────┬──────┘  └─────┬──────┘
      │  TCP           │  TCP          │  TCP
      └────────────────┼───────────────┘
                       │
          ┌────────────┴───────────┐
          │  Wire Server :27017    │
          │  Up to 1,024 clients   │
          └────────────┬───────────┘
                       │
          ┌────────────┴───────────┐
          │  redb ./my_data        │
          │  One process owner     │
          │  Many TCP sessions OK  │
          └────────────────────────┘
```

Your app connects over TCP with standard PyMongo:

```python
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017")
```

Avoid a **second process** also opening `local://my_data` while the wire server holds that path:

```python
from smongo import MongoClient
client = MongoClient("local://my_data")  # ❌ if another process already has it
```

### Single-Process Sharing

If your app and the wire server live in the **same Python process**, pass **`MongoClient(...).get_local_client()`** into `WireServer` so there is a single engine handle. This is how `web_app.py` works: Flask and the wire server share one **`RedbClient`**.

```python
from smongo import MongoClient
from smongo.wire import WireServer

mc = MongoClient("local://my_data")
server = WireServer(
    port=27017,
    local_client=mc.get_local_client(),
)
server.start()

db = mc["myapp"]
db["users"].insert_one({"name": "Alice"})
```

---

## The Edge-to-Cloud Story

This is what the architecture enables end-to-end:

```
    ┌────────────────────────────────────────────────────────────┐
    │                       THE EDGE                              │
    │                                                            │
    │   Your app writes to the local redb-backed engine.         │
    │   Queries execute against local B-tree indexes.            │
    │   Aggregation pipelines run in-process.                    │
    │   Vector search runs against local embeddings.             │
    │   Everything works offline.                                │
    │                                                            │
    │   ┌─────────────────────────────────────────────────────┐  │
    │   │  smongo embedded engine                              │  │
    │   │  redb storage · MQL compiler · Query planner         │  │
    │   │  25+ agg stages · B-tree indexes · $vectorSearch    │  │
    │   │  Oplog · Schema validation · Change streams          │  │
    │   └──────────────────────┬──────────────────────────────┘  │
    │                          │                                  │
    │                    Wire protocol                            │
    │                    (:27017 or :27018)                       │
    │                          │                                  │
    │   ┌──────────┐    ┌─────┴──────┐    ┌──────────┐          │
    │   │ Compass  │    │  Your app  │    │ mongosh  │          │
    │   │ (browse) │    │  (PyMongo) │    │ (debug)  │          │
    │   └──────────┘    └────────────┘    └──────────┘          │
    └────────────────────────────┬───────────────────────────────┘
                                 │
                           SyncManager
                        push (oplog tail)
                        pull (change streams)
                        conflict resolution
                                 │
    ┌────────────────────────────┴───────────────────────────────┐
    │                       THE CLOUD                             │
    │                                                            │
    │   ┌──────────────────────────────────────────────────────┐ │
    │   │  MongoDB Atlas                                        │ │
    │   │  Replica sets · Sharding · Atlas Search               │ │
    │   │  The authoritative copy when you need it              │ │
    │   └──────────────────────────────────────────────────────┘ │
    │                                                            │
    │   Connect Compass to Atlas too — same tool, same UX.      │
    │   Compare edge and cloud side by side.                     │
    └────────────────────────────────────────────────────────────┘
```

**Write locally.** Your app talks to the embedded **redb** database file. Queries are fast because they're local. The query planner picks indexes. The MQL compiler handles the full grammar. There's no network round-trip.

**Browse with Compass.** The wire server exposes the local engine as a standard `mongod`. Compass connects, discovers databases, and renders the full GUI. You can explain queries, build aggregation pipelines, create indexes, and inspect documents — all against the local engine.

**Sync to the cloud.** The `SyncManager` tails the local oplog and pushes mutations to Atlas. Change streams (or timestamp polling) pull remote changes back. Conflict resolution is configurable: last-write-wins, local-wins, remote-wins, field-level merge, or a custom callable.

**Compare both.** Open two Compass windows — one to the local engine, one to Atlas. After sync, they show the same data. The edge and the cloud, unified by the same protocol, the same query language, and the same tools.

---

## Appendix

### A. Full Handshake Response

The exact document returned for `hello` / `isMaster`:

```json
{
  "ismaster": true,
  "isWritablePrimary": true,
  "topologyVersion": {"processId": "<ObjectId>", "counter": 0},
  "maxBsonObjectSize": 16777216,
  "maxMessageSizeBytes": 50331648,
  "maxWriteBatchSize": 100000,
  "localTime": "<ISODate>",
  "logicalSessionTimeoutMinutes": 30,
  "connectionId": 1,
  "minWireVersion": 0,
  "maxWireVersion": 21,
  "readOnly": false,
  "ok": 1.0
}
```

| Field | Value | Meaning |
|---|---|---|
| `ismaster` | `true` | Accepts writes |
| `maxWireVersion` | `21` | MongoDB 7.0+ protocol |
| `maxBsonObjectSize` | 16 MB | Max single document |
| `maxMessageSizeBytes` | 48 MB | Max wire message |
| `maxWriteBatchSize` | 100,000 | Max batch insert size |
| `logicalSessionTimeoutMinutes` | 30 | Session idle expiry |

### B. `buildInfo` Response

```json
{
  "version": "7.0.0-smongo",
  "gitVersion": "<commit hash>",
  "versionArray": [7, 0, 0, 0],
  "bits": 64,
  "modules": ["embedded", "redb"],
  "javascriptEngine": "none",
  "ok": 1.0
}
```

Compass sees `7.0.0` and enables all modern features.

### C. Wire Protocol Opcodes

| Opcode | Name | Support |
|---|---|---|
| 2013 | OP_MSG | Full — all modern driver traffic |
| 2012 | OP_COMPRESSED | Full — zlib built-in; snappy/zstd if installed |
| 2004 | OP_QUERY | Legacy handshake only (`isMaster`, `hello`) |
| 1 | OP_REPLY | Outbound replies to OP_QUERY |

### D. All 80+ Commands

**Handshake & Discovery**
`hello` · `ismaster` / `isMaster` · `ping` · `buildInfo` · `hostInfo` · `getLog` · `getCmdLineOpts` · `whatsmyuri` · `connectionStatus` · `getFreeMonitoringStatus`

**CRUD**
`find` · `insert` · `update` · `delete` · `count` · `distinct` · `findAndModify` · `getMore` · `killCursors` · `bulkWrite` · `getLastError` · `estimatedDocumentCount` · `dataSize`

**Indexes**
`listIndexes` · `createIndexes` · `dropIndexes` · `reIndex`

**Aggregation**
`aggregate` · `mapReduce`

**Admin**
`listDatabases` · `listCollections` · `create` · `drop` · `dropDatabase` · `explain` · `collMod` · `renameCollection` · `compact` · `collStats` · `dbStats` · `validate` · `serverStatus` · `fsync` · `getnonce` · `getParameter` · `setParameter`

**Sessions & Transactions**
`startSession` · `endSessions` · `refreshSessions` · `killSessions` · `killAllSessions` · `startTransaction` · `abortTransaction` · `commitTransaction`

**Users (stub)**
`usersInfo` · `rolesInfo` · `createUser` · `dropUser` · `updateUser`

**Diagnostic**
`currentOp` · `killOp` · `connPoolStats` · `features` · `logRotate` · `top` · `profile` · `setProfilingLevel` · `system.profile` · `shardingState` · `replSetGetConfig` · `replSetGetStatus` · `setFreeMonitoring` · `lockInfo` · `listCommands`

**Auth (graceful rejection)**
`saslStart` · `saslContinue` · `logout`

**Sync**
`client.sync`

### E. Wire Compression

Negotiated during the handshake. Transparent to Compass — no configuration needed.

| Compressor | Availability | Install |
|---|---|---|
| **zlib** | Always | Python stdlib |
| **snappy** | Optional | `pip install python-snappy` |
| **zstd** | Optional | `pip install zstandard` |

### F. Error Codes

| Code | Name | Trigger |
|---|---|---|
| 18 | `AuthenticationFailed` | Credentials in the URI |
| 59 | `CommandNotFound` | Unsupported command |
| 11000 | `DuplicateKey` | Duplicate `_id` or unique index violation |
| 121 | `DocumentValidationFailure` | `$jsonSchema` violation |
| 73 | `InvalidNamespace` | Illegal database or collection name |
| 43 | `CursorNotFound` | Expired or killed cursor |
| 96 | `OperationFailed` | Max connections exceeded |
| 1 | `InternalError` | Engine error (run with `-v` for details) |

### G. Server Limits

| Limit | Value | Configurable |
|---|---|---|
| Max connections | 1,024 | `WireServer(max_connections=N)` |
| Connection idle timeout | 300 s | Code constant |
| Max BSON document | 16 MB | Protocol standard |
| Max wire message | 48 MB | Protocol standard |
| Max write batch | 100,000 | Protocol standard |
| Cursor idle timeout | 600 s | Registry default |
| Max open cursors | 10,000 | Registry default |
| Session timeout | 30 min | Protocol standard |
| Max DB name length | 64 chars | MongoDB spec |
| Max collection name | 120 chars | MongoDB spec |

### H. smongo vs. `mongod`

| Capability | smongo | `mongod` |
|---|---|---|
| Wire protocol (OP_MSG) | Yes | Yes |
| OP_COMPRESSED | Yes (zlib/snappy/zstd) | Yes |
| Authentication (SCRAM) | Yes (`RustWireServer`); No (default Python `WireServer`) | Yes |
| TLS/SSL | Yes (`RustWireServer` via rustls); No (default Python `WireServer`) | Yes |
| Replica set | No (standalone) | Yes |
| Sharding | No | Yes |
| Change streams (wire) | No | Yes |
| Transactions (wire) | Yes (single-node) | Yes |
| Aggregation pipeline | 25+ stages | 30+ stages |
| `$search` (Atlas Search) | No | Atlas only |
| `$vectorSearch` | Yes (in-memory) | Atlas only |
| Wire version | 21 | 21 |
| Compass compatible | Yes | Yes |

### I. Single-owner database path

redb uses a memory-mapped file with **single-writer** semantics at the library level. Practically: **one `Database` / one process** should own a given path. Run **one** embedded Python process (or one wire server) per path; scale read concurrency by connecting many TCP clients to that server — not by opening the same file in two processes.

### J. Compass mongosh Commands

Open the built-in shell (bottom bar in Compass) and run these against smongo:

```javascript
db.serverStatus()                                    // storageEngine (redb), uptime, memory
db.adminCommand({ listDatabases: 1 })                // all databases with sizes
db.users.find({ age: { $gt: 30 } }).explain()        // query plan (INDEX SCAN / COLL SCAN)
db.adminCommand({ "client.sync": 1 })                // sync status (if sync is enabled)
db.adminCommand({ fsync: 1 })                        // flush / checkpoint (engine implementation)
db.users.stats()                                     // collection storage stats
db.serverStatus().connections                         // active connection count
db.adminCommand({ "system.profile": 1 })             // profiled slow operations

db.orders.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```
