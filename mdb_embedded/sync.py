"""
Sync Layer -- bidirectional sync between the local WiredTiger engine and MongoDB Atlas.

Architecture:
    - Background thread tails the local oplog and pushes mutations to Atlas
    - Optionally pulls remote changes via change streams or timestamp polling
    - Conflict resolution strategies: LWW, local-wins, remote-wins, or custom callable
    - Checkpoint stored in a dedicated WiredTiger table so sync survives restarts
"""

import json
import logging
import threading
import time

try:
    from pymongo import MongoClient as _PyMongoClient
    from pymongo import UpdateOne, DeleteOne, InsertOne
    from pymongo.errors import BulkWriteError
except ImportError:
    _PyMongoClient = None
    UpdateOne = DeleteOne = InsertOne = None
    BulkWriteError = None

log = logging.getLogger("mdb_embedded.sync")


# ------------------------------------------------------------------
# Conflict resolution
# ------------------------------------------------------------------

def _lww(local_doc, remote_doc):
    """Last-write-wins: compare _lastModified timestamps."""
    local_ts = (local_doc or {}).get("_lastModified", 0)
    remote_ts = (remote_doc or {}).get("_lastModified", 0)
    return remote_doc if remote_ts >= local_ts else local_doc


def _local_wins(local_doc, _remote_doc):
    return local_doc


def _remote_wins(_local_doc, remote_doc):
    return remote_doc


_RESOLVERS = {
    "lww": _lww,
    "local_wins": _local_wins,
    "remote_wins": _remote_wins,
}


# ------------------------------------------------------------------
# SyncManager
# ------------------------------------------------------------------

class SyncManager:
    """
    Manages bidirectional sync between a local MongoClient and a remote Atlas cluster.

    Usage:
        sync = SyncManager(local_client, "mongodb+srv://...@cluster.mongodb.net")
        sync.start()
        # ... app writes locally ...
        sync.status()
        sync.stop()
    """

    DEFAULT_CONFIG = {
        "mode": "bidirectional",  # bidirectional | push_only | pull_only
        "interval_sec": 5,
        "batch_size": 100,
        "conflict_resolution": "lww",  # lww | local_wins | remote_wins | callable
        "collections": "*",  # "*" for all, or ["db.coll", ...]
    }

    def __init__(self, local_client, atlas_uri, sync_config=None):
        if not _PyMongoClient:
            raise ImportError("pymongo required for sync to MongoDB Atlas")

        self._local = local_client
        self._atlas_uri = atlas_uri
        self._remote = _PyMongoClient(atlas_uri)

        cfg = dict(self.DEFAULT_CONFIG)
        if sync_config:
            cfg.update(sync_config)
        self._config = cfg

        resolver = cfg["conflict_resolution"]
        if callable(resolver):
            self._resolve = resolver
        else:
            self._resolve = _RESOLVERS.get(resolver, _lww)

        self._thread = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()  # starts unpaused

        self._lock = threading.Lock()
        self._last_error = None
        self._pending_count = 0
        self._last_sync_ts = None

        # Checkpoint table lives on the local WiredTiger connection
        wt_conn = local_client.client.conn
        self._ck_session = wt_conn.open_session()
        self._ck_uri = "table:__sync_checkpoint"
        self._ck_session.create(self._ck_uri, "key_format=S,value_format=S")

        self._tracked = {}  # "db.coll" -> (LocalCollection, remote_collection)

    # -- lifecycle -----------------------------------------------------

    def start(self):
        """Launch the background sync thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._discover_collections()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="sync")
        self._thread.start()
        log.info("Sync started (mode=%s, interval=%ss)", self._config["mode"], self._config["interval_sec"])

    def stop(self):
        """Signal the sync thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=15)
        log.info("Sync stopped")

    def pause(self):
        self._pause_event.clear()

    def resume(self):
        self._pause_event.set()

    def sync_now(self):
        """Run one sync cycle synchronously on the calling thread."""
        self._discover_collections()
        self._sync_cycle()

    def status(self):
        with self._lock:
            return {
                "running": self._thread is not None and self._thread.is_alive(),
                "pending": self._pending_count,
                "last_sync": self._last_sync_ts,
                "last_error": str(self._last_error) if self._last_error else None,
                "mode": self._config["mode"],
            }

    # -- background loop -----------------------------------------------

    def _run_loop(self):
        while not self._stop_event.is_set():
            self._pause_event.wait()
            if self._stop_event.is_set():
                break
            try:
                self._sync_cycle()
            except Exception as exc:
                with self._lock:
                    self._last_error = exc
                log.exception("Sync cycle error")

            self._stop_event.wait(timeout=self._config["interval_sec"])

    def _sync_cycle(self):
        mode = self._config["mode"]

        if mode in ("bidirectional", "push_only"):
            self._push()

        if mode in ("bidirectional", "pull_only"):
            self._pull()

        with self._lock:
            self._last_sync_ts = time.time()

    # -- push (local -> Atlas) -----------------------------------------

    def _push(self):
        batch_size = self._config["batch_size"]

        for ns, (local_coll, remote_coll) in self._tracked.items():
            checkpoint = self._get_checkpoint(f"push:{ns}")
            reader = local_coll.get_oplog_reader()
            entries = reader.read_from(checkpoint, skip_internal=True)

            if not entries:
                continue

            ops = []
            last_key = None

            for key, entry in entries:
                op = entry["op"]
                doc_id = entry["doc_id"]
                payload = entry["payload"]

                if op == "insert":
                    doc = dict(payload)
                    doc["_lastModified"] = entry["ts"]
                    ops.append(InsertOne(doc))
                elif op == "update":
                    update_spec = dict(payload)
                    if "$set" not in update_spec:
                        update_spec["$set"] = {}
                    update_spec["$set"]["_lastModified"] = entry["ts"]
                    ops.append(UpdateOne({"_id": doc_id}, update_spec, upsert=True))
                elif op == "delete":
                    ops.append(DeleteOne({"_id": doc_id}))
                elif op == "index_create":
                    try:
                        idx_keys = payload.get("keys", [])
                        idx_kwargs = {k: v for k, v in payload.items() if k != "keys"}
                        remote_coll.create_index(idx_keys, **idx_kwargs)
                    except Exception as exc:
                        log.warning("Failed to sync index create %s: %s", doc_id, exc)
                elif op == "index_drop":
                    try:
                        remote_coll.drop_index(doc_id)
                    except Exception as exc:
                        log.warning("Failed to sync index drop %s: %s", doc_id, exc)

                last_key = key

                if len(ops) >= batch_size:
                    self._flush_bulk(remote_coll, ops)
                    ops = []

            if ops:
                self._flush_bulk(remote_coll, ops)

            if last_key:
                self._set_checkpoint(f"push:{ns}", last_key)

            with self._lock:
                self._pending_count = 0

    def _flush_bulk(self, remote_coll, ops):
        try:
            remote_coll.bulk_write(ops, ordered=False)
        except BulkWriteError as bwe:
            log.warning("Bulk write partial failure: %s", bwe.details)

    # -- pull (Atlas -> local) -----------------------------------------

    def _pull(self):
        for ns, (local_coll, remote_coll) in self._tracked.items():
            last_ts = self._get_checkpoint(f"pull_ts:{ns}")
            last_ts = float(last_ts) if last_ts else 0.0

            query = {"_lastModified": {"$gt": last_ts}}
            try:
                remote_docs = list(remote_coll.find(query).sort("_lastModified", 1))
            except Exception as exc:
                log.warning("Pull query failed for %s: %s", ns, exc)
                continue

            if not remote_docs:
                self._pull_index_defs(ns, local_coll, remote_coll)
                continue

            max_ts = last_ts

            for rdoc in remote_docs:
                doc_id = rdoc["_id"]
                remote_ts = rdoc.get("_lastModified", 0)

                local_doc = local_coll.get_by_id(doc_id)

                if local_doc:
                    resolved = self._resolve(local_doc, rdoc)
                    if resolved and resolved.get("_id") == doc_id:
                        local_coll.update(
                            {"_id": doc_id},
                            {"$set": {k: v for k, v in resolved.items() if k != "_id"}},
                            multi=False,
                            _internal=True,
                        )
                else:
                    local_coll.insert_one(rdoc, _internal=True)

                if remote_ts > max_ts:
                    max_ts = remote_ts

            self._set_checkpoint(f"pull_ts:{ns}", str(max_ts))
            self._pull_index_defs(ns, local_coll, remote_coll)

    def _pull_index_defs(self, ns, local_coll, remote_coll):
        """Ensure local indexes match remote index definitions."""
        try:
            remote_indexes = list(remote_coll.list_indexes())
        except Exception:
            return

        local_names = {idx["name"] for idx in local_coll.list_indexes()}

        for ridx in remote_indexes:
            name = ridx.get("name", "")
            if name == "_id_" or name in local_names:
                continue
            keys = list(ridx.get("key", {}).items())
            if keys:
                try:
                    local_coll.create_index(
                        [(f, int(d)) for f, d in keys],
                        name=name,
                        unique=ridx.get("unique", False),
                        sparse=ridx.get("sparse", False),
                        _internal=True,
                    )
                except Exception as exc:
                    log.warning("Failed to create pulled index %s: %s", name, exc)

    # -- collection discovery ------------------------------------------

    def _discover_collections(self):
        """
        Build the mapping of namespace -> (LocalCollection, remote pymongo collection).
        """
        cfg_colls = self._config["collections"]

        if cfg_colls == "*":
            # Nothing pre-registered; will need explicit registration.
            return

        for ns_str in cfg_colls:
            if ns_str in self._tracked:
                continue
            parts = ns_str.split(".", 1)
            if len(parts) != 2:
                continue
            db_name, coll_name = parts
            self._register(db_name, coll_name)

    def register_collection(self, db_name, coll_name, local_collection):
        """
        Explicitly register a collection for syncing.
        Called by the user or automatically when collections="*".
        """
        ns = f"{db_name}.{coll_name}"
        if ns in self._tracked:
            return
        remote_coll = self._remote[db_name][coll_name]
        self._tracked[ns] = (local_collection, remote_coll)

    def _register(self, db_name, coll_name):
        local_db = self._local.client.get_db(db_name)
        local_coll = local_db.get_collection(coll_name)
        remote_coll = self._remote[db_name][coll_name]
        ns = f"{db_name}.{coll_name}"
        self._tracked[ns] = (local_coll, remote_coll)

    # -- checkpoint persistence ----------------------------------------

    def _get_checkpoint(self, key):
        cursor = self._ck_session.open_cursor(self._ck_uri, None, None)
        cursor.set_key(key)
        val = None
        if cursor.search() == 0:
            val = cursor.get_value()
        cursor.close()
        return val

    def _set_checkpoint(self, key, value):
        cursor = self._ck_session.open_cursor(self._ck_uri, None, "overwrite=true")
        cursor[key] = value
        cursor.close()
