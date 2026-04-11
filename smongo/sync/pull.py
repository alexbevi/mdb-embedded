"""Pull mixin: Atlas-to-local replication path with conflict resolution."""

from __future__ import annotations

import json
import logging
from typing import Any

from .._types import Document, Predicate
from .conflict import (
    VectorClock,
    _apply_commutative_to_doc,
    _crdt_merge_doc,
    _diff_fields,
    _field_merge,
    _is_commutative_op,
)

try:
    from pymongo.errors import PyMongoError
except ImportError:

    class PyMongoError(Exception):  # type: ignore[no-redef]
        pass


from smongo._smongo_core import ejson_default as _ejson_default
from smongo._smongo_core import ejson_object_hook as _ejson_object_hook
from smongo._smongo_core import from_pymongo as _from_pymongo

from ..index import DuplicateKeyError

log = logging.getLogger("smongo.sync")


class _PullMixin:
    """Mixin providing pull (Atlas -> local) replication for the SyncManager."""

    _SYNC_META_FIELDS = frozenset({"_lastModified"})
    _VCLOCK_FIELD = "_vclock"

    def _pull(self) -> None:
        for ns, (local_coll, remote_coll, ns_filter) in self._tracked.items():
            with self._lock:
                pulled_before = self._pulled_count
            try:
                if self._config.get("use_change_stream_pull", True):
                    used_stream = self._pull_via_change_stream(
                        ns, local_coll, remote_coll, ns_filter
                    )
                    if used_stream:
                        self._pull_index_defs(ns, local_coll, remote_coll)
                        continue

                last_ts_str = self._get_checkpoint(f"pull_ts:{ns}")
                last_ts = float(last_ts_str) if last_ts_str else 0.0

                query: dict[str, Any] = {"_lastModified": {"$gt": last_ts}}
                try:
                    remote_docs: list[Document] = list(
                        remote_coll.find(query).sort("_lastModified", 1)
                    )
                except PyMongoError as exc:
                    log.warning("Pull query failed for %s: %s", ns, exc)
                    continue

                if not remote_docs:
                    self._detect_remote_deletes(ns, local_coll, remote_coll)
                    self._pull_index_defs(ns, local_coll, remote_coll)
                    continue

                max_ts = last_ts

                for rdoc in remote_docs:
                    if ns_filter:
                        try:
                            if not ns_filter(rdoc):
                                continue
                        except (KeyError, TypeError):
                            pass
                    if not self._doc_passes_sync_filter(rdoc):
                        continue
                    remote_ts = rdoc.get("_lastModified", 0)
                    self._upsert_remote_doc(ns, local_coll, rdoc)
                    with self._lock:
                        self._pulled_count += 1

                    if remote_ts > max_ts:
                        max_ts = remote_ts

                self._set_checkpoint(f"pull_ts:{ns}", str(max_ts))
                self._detect_remote_deletes(ns, local_coll, remote_coll)
                self._pull_index_defs(ns, local_coll, remote_coll)
            finally:
                self._record_ns_pull(ns, pulled_before)

    def _detect_remote_deletes(self, ns: str, local_coll: Any, remote_coll: Any) -> None:
        """Detect documents deleted on remote by comparing local IDs against the remote.

        Queries the remote in batches and removes any local documents whose ``_id``
        is absent from the remote. Throttled by ``delete_detection_interval_cycles``
        and ``delete_detection_enabled`` config options.
        """
        if not self._config.get("delete_detection_enabled", True):
            return
        interval = int(self._config.get("delete_detection_interval_cycles", 5))
        if interval > 1 and self._cycle_count % interval != 0:
            return

        try:
            local_ids: list[Any] = [
                doc["_id"] for doc in local_coll.find({}, projection={"_id": 1})
            ]
        except Exception as exc:
            log.warning("Failed to load local IDs for delete detection in %s: %s", ns, exc)
            return
        if not local_ids:
            return

        batch_size = int(self._config.get("delete_detection_batch_size", 1000))
        remote_ids: set[Any] = set()

        for i in range(0, len(local_ids), batch_size):
            batch = local_ids[i : i + batch_size]
            try:
                remote_id_docs: list[Document] = list(
                    remote_coll.find(
                        {"_id": {"$in": batch}},
                        projection={"_id": 1},
                    )
                )
                remote_ids.update(d["_id"] for d in remote_id_docs)
            except (PyMongoError, AttributeError, TypeError) as exc:
                log.debug("Remote delete detection query failed for %s: %s", ns, exc)
                return

        deleted_ids = set(local_ids) - remote_ids
        for doc_id in deleted_ids:
            if self._tombstones.is_tombstoned(doc_id):
                continue
            try:
                local_coll.delete({"_id": doc_id}, multi=False, _internal=True)
                log.debug("Detected remote delete for %s _id=%s", ns, doc_id)
            except Exception as exc:
                log.warning("Failed to apply remote delete for %s _id=%s: %s", ns, doc_id, exc)

    def _upsert_remote_doc(
        self,
        ns: str,
        local_coll: Any,
        rdoc: Document,
        remote_changed: set[str] | None = None,
    ) -> None:
        rdoc = _from_pymongo(rdoc)
        doc_id = rdoc["_id"]

        if self._tombstones.is_tombstoned(doc_id):
            return

        if self._config.get("validate_on_pull") and hasattr(local_coll, "_validator"):
            validator = getattr(local_coll, "_validator", None)
            if validator:
                try:
                    from smongo.storage.redb_engine import validate_document

                    validate_document(rdoc, validator)
                except Exception as exc:
                    log.warning(
                        "Schema validation failed on pull for %s _id=%s: %s", ns, doc_id, exc
                    )
                    self._dlq_enqueue(
                        ns,
                        {"op": "pull_validation_fail", "doc_id": doc_id, "payload": rdoc},
                        "SCHEMA_VALIDATION",
                        str(exc),
                    )
                    return

        local_doc = local_coll.get_by_id(doc_id)
        if local_doc:
            real_diff = (
                _diff_fields(local_doc, rdoc) - self._SYNC_META_FIELDS - {self._VCLOCK_FIELD}
            )
            if not real_diff:
                if rdoc.get("_lastModified") != local_doc.get("_lastModified"):
                    local_coll.update(
                        {"_id": doc_id},
                        {"$set": {"_lastModified": rdoc["_lastModified"]}},
                        multi=False,
                        _internal=True,
                    )
                return

            with self._lock:
                self._conflict_count += 1

            local_vc = VectorClock.from_dict(local_doc.get(self._VCLOCK_FIELD))
            remote_vc = VectorClock.from_dict(rdoc.get(self._VCLOCK_FIELD))

            remote_has_clock = bool(rdoc.get(self._VCLOCK_FIELD))
            resolved: Document | None = None
            resolution_strategy = self._resolver_name

            if remote_has_clock and remote_vc.dominates(local_vc):
                resolved = rdoc
                resolution_strategy = "remote_vc_dominates"
            elif remote_has_clock and local_vc.dominates(remote_vc):
                resolved = local_doc
                resolution_strategy = "local_vc_dominates"
            elif self._crdt_fields and self._resolver_name in ("lww", "field_merge"):
                resolved = _crdt_merge_doc(local_doc, rdoc, self._crdt_fields)
                resolution_strategy = "crdt_merge"

            if resolved is None:
                field_key = (ns, str(doc_id))
                local_spec = self._local_update_specs.get(field_key)
                if local_spec and _is_commutative_op(local_spec):
                    resolved = _apply_commutative_to_doc(rdoc, local_spec)
                    resolution_strategy = "operational_transform"

            if resolved is None:
                if self._resolver_name == "field_merge":
                    field_key = (ns, str(doc_id))
                    local_changed = self._cumulative_field_history.get(
                        field_key, self._local_field_history.get(field_key, set())
                    )
                    if remote_changed is None:
                        remote_changed = real_diff
                    resolved = _field_merge(
                        local_doc,
                        rdoc,
                        local_changed=local_changed,
                        remote_changed=remote_changed,
                    )
                    resolution_strategy = "field_merge"
                else:
                    resolved = self._resolve(local_doc, rdoc)
                    resolution_strategy = self._resolver_name

            self._log_conflict(
                ns=ns,
                doc_id=doc_id,
                strategy=resolution_strategy,
                local_vc=local_vc,
                remote_vc=remote_vc,
                diff_fields=real_diff,
            )

            merged_vc = VectorClock.from_dict(local_vc.to_dict())
            merged_vc.merge(remote_vc).tick(self._node_id)
            if resolved and resolved.get("_id") == doc_id:
                update_fields = {k: v for k, v in resolved.items() if k != "_id"}
                update_fields[self._VCLOCK_FIELD] = merged_vc.to_dict()
                local_coll.update(
                    {"_id": doc_id},
                    {"$set": update_fields},
                    multi=False,
                    _internal=True,
                )
                self._vector_clocks[str(doc_id)] = merged_vc
        else:
            vc = VectorClock.from_dict(rdoc.get(self._VCLOCK_FIELD))
            vc.tick(self._node_id)
            rdoc[self._VCLOCK_FIELD] = vc.to_dict()
            self._vector_clocks[str(doc_id)] = vc
            local_coll.insert_one(rdoc, _internal=True)

    def _pull_via_change_stream(
        self, ns: str, local_coll: Any, remote_coll: Any, ns_filter: Predicate | None = None
    ) -> bool:
        """
        Pull remote changes using MongoDB Change Streams with resume token checkpointing.
        Returns True when stream path is used; False when falling back to polling.
        """
        init_key = f"pull_cs_init:{ns}"
        if not self._get_checkpoint(init_key):
            page_key = f"pull_cs_page:{ns}"
            page_size = int(self._config.get("batch_size", 100))
            last_id_raw = self._get_checkpoint(page_key)
            last_id: Any = (
                json.loads(last_id_raw, object_hook=_ejson_object_hook) if last_id_raw else None
            )
            try:
                while True:
                    find_q: dict[str, Any] = (
                        {"_id": {"$gt": last_id}} if last_id is not None else {}
                    )
                    page = list(remote_coll.find(find_q).sort("_id", 1).limit(page_size))
                    for rdoc in page:
                        raw_id = rdoc.get("_id")
                        if ns_filter:
                            try:
                                if not ns_filter(rdoc):
                                    continue
                            except (KeyError, TypeError):
                                pass
                        if not self._doc_passes_sync_filter(rdoc):
                            continue
                        self._upsert_remote_doc(ns, local_coll, rdoc)
                        last_id = raw_id
                    if page and last_id is not None:
                        self._set_checkpoint(
                            page_key,
                            json.dumps(last_id, default=_ejson_default),
                        )
                    if len(page) < page_size:
                        break
                self._set_checkpoint(init_key, "1")
            except PyMongoError as exc:
                log.warning("Initial change-stream snapshot failed for %s: %s", ns, exc)
                return False

        token_key = f"pull_cs_token:{ns}"
        token_raw = self._get_checkpoint(token_key)
        resume_token: dict[str, Any] | None = None
        if token_raw:
            try:
                resume_token = json.loads(token_raw)
            except (json.JSONDecodeError, ValueError):
                log.warning("Corrupt change-stream resume token for %s; discarding", ns)
                self._remove_checkpoint(token_key)

        try:
            watch_kwargs: dict[str, Any] = {
                "full_document": "updateLookup",
                "max_await_time_ms": 200,
            }
            if resume_token:
                watch_kwargs["resume_after"] = resume_token
            with remote_coll.watch([], **watch_kwargs) as stream:
                max_events = int(self._config.get("batch_size", 100))
                processed = 0
                while processed < max_events:
                    change = stream.try_next()
                    if not change:
                        break
                    op = change.get("operationType")
                    doc_id = (change.get("documentKey") or {}).get("_id")
                    if op in ("insert", "replace", "update"):
                        full_doc = change.get("fullDocument")
                        if full_doc:
                            if ns_filter:
                                try:
                                    if not ns_filter(full_doc):
                                        processed += 1
                                        continue
                                except (KeyError, TypeError):
                                    pass
                            if not self._doc_passes_sync_filter(full_doc):
                                processed += 1
                                continue
                            rc: set[str] | None = None
                            if op == "update":
                                ud = change.get("updateDescription") or {}
                                updated = set(ud.get("updatedFields", {}).keys())
                                removed = set(ud.get("removedFields", []))
                                rc = updated | removed if (updated or removed) else None
                            self._upsert_remote_doc(ns, local_coll, full_doc, remote_changed=rc)
                            with self._lock:
                                self._pulled_count += 1
                    elif op == "delete" and doc_id is not None:
                        local_coll.delete(
                            {"_id": _from_pymongo(doc_id)}, multi=False, _internal=True
                        )
                        self._tombstones.mark_deleted(doc_id)

                    token = change.get("_id")
                    if token is not None:
                        self._set_checkpoint(token_key, json.dumps(token, default=str))
                    processed += 1
                if processed == 0:
                    initial_token = getattr(stream, "resume_token", None)
                    if initial_token is not None:
                        self._set_checkpoint(token_key, json.dumps(initial_token, default=str))
            return True
        except PyMongoError as exc:
            log.warning("Change-stream pull unavailable for %s, falling back: %s", ns, exc)
            return False

    def _pull_index_defs(self, ns: str, local_coll: Any, remote_coll: Any) -> None:
        """Ensure local indexes match remote index definitions (add missing, drop stale)."""
        try:
            remote_indexes: list[dict[str, Any]] = list(remote_coll.list_indexes())
        except PyMongoError as exc:
            log.debug("Failed to list remote indexes for %s: %s", ns, exc)
            return

        local_indexes = local_coll.list_indexes()

        local_hash = self._compute_index_hash(local_indexes)
        remote_hash = self._compute_index_hash(remote_indexes)
        cache_key = f"pull:{ns}"
        if local_hash == remote_hash and self._index_hash_cache.get(cache_key) == remote_hash:
            return
        self._index_hash_cache[cache_key] = remote_hash

        local_names = {idx.get("name", "") for idx in local_indexes}
        remote_names = {ridx.get("name", "") for ridx in remote_indexes}

        for ridx in remote_indexes:
            name = ridx.get("name", "")
            if name == "_id_" or name in local_names:
                continue
            keys = list(ridx.get("key", ridx.get("keys", {})).items())
            if keys:
                try:
                    kwargs = self._extract_index_options(ridx)
                    kwargs["_internal"] = True
                    local_coll.create_index(
                        [(f, int(d)) for f, d in keys],
                        **kwargs,
                    )
                except (DuplicateKeyError, ValueError, KeyError, RuntimeError) as exc:
                    log.warning("Failed to create pulled index %s: %s", name, exc)
                except Exception as exc:
                    log.warning("Failed to create pulled index %s: %s", name, exc)

        for lidx in local_indexes:
            name = lidx.get("name", "")
            if name == "_id_" or name in remote_names:
                continue
            try:
                local_coll.drop_index(name)
                log.info("Dropped local index %s on %s (removed on remote)", name, ns)
            except Exception as exc:
                log.warning("Failed to drop local index %s on %s: %s", name, ns, exc)
