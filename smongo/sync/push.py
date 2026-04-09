"""Push mixin: local-to-Atlas replication path."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .._types import Document, Predicate
from .conflict import SyncOverflowError, _is_commutative_op

try:
    from pymongo import DeleteOne, InsertOne, UpdateOne
    from pymongo.errors import BulkWriteError, PyMongoError
except ImportError:
    UpdateOne = DeleteOne = InsertOne = None  # type: ignore[misc, assignment]

    class BulkWriteError(Exception):  # type: ignore[no-redef]
        pass

    class PyMongoError(Exception):  # type: ignore[no-redef]
        pass


from smongo._smongo_core import to_pymongo as _to_pymongo

log = logging.getLogger("smongo.sync")


class _PushMixin:
    """Mixin providing push (local -> Atlas) replication for the SyncManager."""

    def _push(self) -> None:
        self._sweep_dlq()

        batch_size: int = self._config["batch_size"]
        concurrency = int(self._config.get("push_concurrency", 4))

        namespaces = list(self._tracked.items())
        if not namespaces:
            return

        if concurrency <= 1 or len(namespaces) <= 1:
            for ns, (local_coll, remote_coll, ns_filter) in namespaces:
                self._push_namespace(ns, local_coll, remote_coll, ns_filter, batch_size)
        else:
            with ThreadPoolExecutor(max_workers=min(concurrency, len(namespaces))) as pool:
                futures = {
                    pool.submit(self._push_namespace, ns, lc, rc, nf, batch_size): ns
                    for ns, (lc, rc, nf) in namespaces
                }
                for fut in as_completed(futures):
                    ns_name = futures[fut]
                    try:
                        fut.result()
                    except Exception as exc:
                        log.warning("Push failed for namespace %s: %s", ns_name, exc)
                        with self._lock:
                            self._error_count += 1

        with self._lock:
            self._pending_count = 0

    def _push_namespace(
        self,
        ns: str,
        local_coll: Any,
        remote_coll: Any,
        ns_filter: Predicate | None,
        batch_size: int,
    ) -> None:
        """Push pending oplog entries for a single namespace to the remote."""
        checkpoint = self._get_checkpoint(f"push:{ns}")
        reader = local_coll.get_oplog_reader()

        if checkpoint is not None:
            try:
                oldest = reader.oldest_key()
            except Exception:
                oldest = None
            if oldest is not None and checkpoint < oldest:
                log.error(
                    "Oplog overflow for %s: checkpoint %s is older than oldest key %s",
                    ns,
                    checkpoint,
                    oldest,
                )
                self._handle_oplog_overflow(ns, local_coll, remote_coll)
                return

        entries = reader.read_from(checkpoint, skip_internal=True)

        if not entries:
            self._push_index_defs(ns, local_coll, remote_coll)
            return

        ops: list[Any] = []
        op_entries: list[Document] = []
        last_key: str | None = None
        safe_key: str | None = None
        batch_start_key: str | None = None
        ns_pushed = 0

        for key, entry in entries:
            op = entry["op"]
            doc_id = entry["doc_id"]
            payload = entry.get("payload")
            changed_fields = entry.get("changed_fields") or []

            has_filter = ns_filter or self._active_sync_filter
            if has_filter and payload and op in ("insert", "update", "delete"):
                filter_doc = payload
                if op == "update":
                    try:
                        filter_doc = local_coll.get_by_id(doc_id) or payload
                    except (KeyError, TypeError, RuntimeError):
                        filter_doc = payload
                if ns_filter:
                    try:
                        if not ns_filter(filter_doc):
                            last_key = key
                            if not ops:
                                safe_key = key
                            continue
                    except (KeyError, TypeError):
                        pass
                if not self._doc_passes_sync_filter(filter_doc):
                    last_key = key
                    if not ops:
                        safe_key = key
                    continue

            if op == "insert":
                doc = _to_pymongo(dict(payload))
                doc["_lastModified"] = entry["ts"]
                ops.append(InsertOne(doc))
                op_entries.append(entry)
            elif op == "update":
                update_spec = _to_pymongo(dict(payload))
                if "$set" not in update_spec:
                    update_spec["$set"] = {}
                update_spec["$set"]["_lastModified"] = entry["ts"]
                ops.append(UpdateOne({"_id": _to_pymongo(doc_id)}, update_spec, upsert=True))
                op_entries.append(entry)
                field_key = (ns, str(doc_id))
                self._local_field_history[field_key] = set(changed_fields)
                cum = self._cumulative_field_history.get(field_key, set())
                cum.update(changed_fields)
                self._cumulative_field_history[field_key] = cum
                if _is_commutative_op(payload):
                    self._local_update_specs[field_key] = dict(payload)
            elif op == "delete":
                ops.append(DeleteOne({"_id": _to_pymongo(doc_id)}))
                op_entries.append(entry)
                self._tombstones.mark_deleted(doc_id)
            elif op == "index_create":
                try:
                    idx_keys = payload.get("keys", [])
                    idx_kwargs = {k: v for k, v in payload.items() if k != "keys"}
                    remote_coll.create_index(idx_keys, **idx_kwargs)
                except PyMongoError as exc:
                    log.warning("Failed to sync index create %s: %s", doc_id, exc)
            elif op == "index_drop":
                try:
                    remote_coll.drop_index(doc_id)
                except PyMongoError as exc:
                    log.warning("Failed to sync index drop %s: %s", doc_id, exc)

            last_key = key
            if batch_start_key is None and ops:
                batch_start_key = key

            if len(ops) >= batch_size:
                n_ok = self._flush_bulk(
                    remote_coll, ops, ns=ns, op_entries=op_entries, local_coll=local_coll
                )
                if n_ok > 0:
                    safe_key = key
                    with self._lock:
                        self._pushed_count += n_ok
                    ns_pushed += n_ok
                if n_ok < len(ops):
                    log.warning("Batch failed for %s; entries retained in oplog for retry", ns)
                ops = []
                op_entries = []
                batch_start_key = None

        if ops:
            n_ok = self._flush_bulk(
                remote_coll, ops, ns=ns, op_entries=op_entries, local_coll=local_coll
            )
            if n_ok > 0:
                safe_key = last_key
                with self._lock:
                    self._pushed_count += n_ok
                ns_pushed += n_ok
            if n_ok < len(ops):
                log.warning("Final batch failed for %s; entries retained in oplog for retry", ns)

        if safe_key:
            if self._config.get("oplog_auto_compact", True):
                self._atomic_checkpoint_and_compact(ns, safe_key, local_coll._oplog_w.oplog_uri)
            else:
                self._set_checkpoint(f"push:{ns}", safe_key)

        self._push_index_defs(ns, local_coll, remote_coll)

        stats = self._ensure_ns_stats(ns)
        stats["last_push_ts"] = time.time()
        stats["last_push_count"] = ns_pushed

    @staticmethod
    def _extract_index_options(idx: dict[str, Any]) -> dict[str, Any]:
        """Build a kwargs dict from an index definition, forwarding all known options."""
        kwargs: dict[str, Any] = {}
        name = idx.get("name", "")
        if name:
            kwargs["name"] = name
        if idx.get("unique"):
            kwargs["unique"] = True
        if idx.get("sparse"):
            kwargs["sparse"] = True
        if idx.get("background"):
            kwargs["background"] = True
        eas = idx.get("expireAfterSeconds") or idx.get("expire_after_seconds")
        if eas is not None:
            kwargs["expireAfterSeconds"] = int(eas)
        pfe = idx.get("partialFilterExpression")
        if pfe is not None:
            kwargs["partialFilterExpression"] = pfe
        collation = idx.get("collation")
        if collation is not None:
            kwargs["collation"] = collation
        idx_type = idx.get("type") or idx.get("index_type")
        if idx_type is not None:
            kwargs["type"] = idx_type
        weights = idx.get("weights")
        if weights is not None:
            kwargs["weights"] = weights
        vs = idx.get("vectorSearchOptions") or idx.get("vector_options")
        if vs is not None:
            kwargs["vectorSearchOptions"] = vs
        prefix_len = idx.get("prefixLength")
        if prefix_len is not None:
            kwargs["prefixLength"] = prefix_len
        return kwargs

    def _push_index_defs(self, ns: str, local_coll: Any, remote_coll: Any) -> None:
        """Push local index definitions to remote and drop remote indexes removed locally."""
        try:
            remote_indexes = list(remote_coll.list_indexes())
        except (PyMongoError, AttributeError, TypeError, RuntimeError) as exc:
            log.debug("Failed to list remote indexes for %s: %s", ns, exc)
            return

        local_indexes = local_coll.list_indexes()

        local_hash = self._compute_index_hash(local_indexes)
        remote_hash = self._compute_index_hash(remote_indexes)
        cache_key = f"push:{ns}"
        if local_hash == remote_hash and self._index_hash_cache.get(cache_key) == local_hash:
            return
        self._index_hash_cache[cache_key] = local_hash

        local_names = {idx.get("name", "") for idx in local_indexes}
        remote_names = {idx.get("name", "") for idx in remote_indexes}

        for idx in local_indexes:
            name = idx.get("name", "")
            if name == "_id_" or name in remote_names:
                continue
            keys = list(idx.get("keys", idx.get("key", {})).items())
            if keys:
                try:
                    kwargs = self._extract_index_options(idx)
                    remote_coll.create_index(keys, **kwargs)
                except (PyMongoError, AttributeError, TypeError, RuntimeError) as exc:
                    log.warning("Failed to push index %s on %s: %s", name, ns, exc)

        for ridx in remote_indexes:
            name = ridx.get("name", "")
            if name == "_id_" or name in local_names:
                continue
            try:
                remote_coll.drop_index(name)
                log.info("Dropped remote index %s on %s (removed locally)", name, ns)
            except (PyMongoError, AttributeError, TypeError, RuntimeError) as exc:
                log.warning("Failed to drop remote index %s on %s: %s", name, ns, exc)

    def _atomic_checkpoint_and_compact(self, ns: str, safe_key: str, oplog_uri: str) -> None:
        """Atomically update the push checkpoint and truncate the oplog (redb single transaction)."""
        with self._ck_lock:
            self._rust.sync_atomic_checkpoint_truncate(
                self._ck_uri,
                f"push:{ns}",
                safe_key,
                oplog_uri,
                safe_key,
            )

    _SCHEMA_VALIDATION_ERROR_CODE = 121

    def _flush_bulk(
        self,
        remote_coll: Any,
        ops: list[Any],
        *,
        ns: str = "",
        op_entries: list[Document] | None = None,
        local_coll: Any = None,
    ) -> int:
        """Flush a batch of operations to remote.

        Returns the number of successfully written ops (``len(ops)`` on full
        success, 0..n on partial failure, ``-1`` on total failure).
        Failed ops are enqueued into the dead-letter queue when *op_entries*
        is provided.  Schema validation failures (code 121) are marked as
        permanently failed and optionally rolled back locally.
        """
        try:
            remote_coll.bulk_write(ops, ordered=False)
            return len(ops)
        except BulkWriteError as bwe:
            details = bwe.details or {}
            write_errors = details.get("writeErrors", [])
            n_failed = len(write_errors)
            n_ok = len(ops) - n_failed
            for err in write_errors:
                idx = err.get("index")
                code = err.get("code")
                errmsg = err.get("errmsg", "")
                log.warning(
                    "Sync bulk_write error: op_index=%s code=%s msg=%s",
                    idx,
                    code,
                    errmsg,
                )
                if op_entries and idx is not None and idx < len(op_entries):
                    is_schema = code == self._SCHEMA_VALIDATION_ERROR_CODE
                    self._dlq_enqueue(
                        ns,
                        op_entries[idx],
                        code,
                        errmsg,
                        permanently_failed=is_schema,
                    )
                    if is_schema:
                        self._handle_schema_rejection(
                            ns, op_entries[idx], remote_coll, local_coll, errmsg
                        )
            log.warning("Bulk write partial failure: %d/%d ops succeeded", n_ok, len(ops))
            return n_ok

    def _handle_schema_rejection(
        self,
        ns: str,
        entry: Document,
        remote_coll: Any,
        local_coll: Any,
        errmsg: str,
    ) -> None:
        """React to a server-side schema validation failure (error code 121).

        Depending on ``schema_rejection_strategy`` config:
        - ``"rollback"``: overwrite the local doc with the server version (or
          delete if the server has no copy).
        - ``"quarantine"``: leave the local doc untouched (DLQ-only).
        - ``"ignore"``: legacy behaviour, DLQ and move on.
        """
        strategy = self._config.get("schema_rejection_strategy", "rollback")
        doc_id = entry.get("doc_id")

        with self._lock:
            self._schema_rejection_count += 1

        if strategy in ("quarantine", "ignore"):
            log.warning(
                "Schema rejection (%s) for %s _id=%s: %s",
                strategy,
                ns,
                doc_id,
                errmsg,
            )
            return

        if local_coll is None or doc_id is None:
            return

        try:
            from smongo._smongo_core import from_pymongo as _from_pymongo

            remote_doc = remote_coll.find_one({"_id": _to_pymongo(doc_id)})
            if remote_doc is not None:
                remote_doc = _from_pymongo(remote_doc)
                update_fields = {k: v for k, v in remote_doc.items() if k != "_id"}
                local_coll.update(
                    {"_id": doc_id}, {"$set": update_fields}, multi=False, _internal=True
                )
                log.warning(
                    "Schema rejection rollback: overwrote local %s _id=%s with server version",
                    ns,
                    doc_id,
                )
            else:
                local_coll.delete({"_id": doc_id}, multi=False, _internal=True)
                log.warning(
                    "Schema rejection rollback: deleted local %s _id=%s (no server version)",
                    ns,
                    doc_id,
                )
        except Exception as exc:
            log.warning("Schema rejection rollback failed for %s _id=%s: %s", ns, doc_id, exc)

    def _handle_oplog_overflow(self, ns: str, local_coll: Any, remote_coll: Any) -> None:
        """Handle the case where the oplog has been truncated past the push checkpoint."""
        strategy = self._config.get("overflow_strategy", "server_wins")
        if strategy == "error":
            raise SyncOverflowError(
                f"Oplog overflow for {ns}: checkpoint references truncated entries. "
                "Set overflow_strategy='server_wins' to auto-recover."
            )
        self._force_full_resync(ns, local_coll, remote_coll, winner="server")
