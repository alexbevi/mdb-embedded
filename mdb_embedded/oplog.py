"""
Oplog (Operations Log) -- records every mutation for sync and audit.

Enhanced format includes namespace, doc versioning, and an internal flag
to prevent echo loops during bidirectional sync.
"""

import json
import time
import uuid
import hashlib


def _doc_checksum(doc):
    """Stable hash of a document for integrity verification."""
    if doc is None:
        return None
    raw = json.dumps(doc, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class OplogWriter:
    """Appends structured operations to a WiredTiger oplog table."""

    def __init__(self, session, oplog_uri, namespace):
        self.session = session
        self.oplog_uri = oplog_uri
        self.namespace = namespace

    def log(self, op, doc_id, payload, *, version=None, internal=False):
        """
        Write an oplog entry.

        Args:
            op: Operation type (insert, update, delete, index_create, index_drop)
            doc_id: The _id of the affected document (or index name for index ops)
            payload: The document or update spec
            version: Incrementing doc version for conflict detection
            internal: If True, sync layer should skip this entry (echo prevention)
        """
        oplog_key = f"{time.time_ns():020d}-{uuid.uuid4()}"
        log_entry = {
            "ts": time.time(),
            "ns": self.namespace,
            "op": op,
            "doc_id": doc_id,
            "payload": payload,
            "v": version,
            "checksum": _doc_checksum(payload) if op != "delete" else None,
            "internal": internal,
        }

        cursor = self.session.open_cursor(self.oplog_uri, None, "overwrite=true")
        cursor[oplog_key] = json.dumps(log_entry, default=str)
        cursor.close()
        return oplog_key


class OplogReader:
    """Reads oplog entries, optionally from a checkpoint forward."""

    def __init__(self, session, oplog_uri):
        self.session = session
        self.oplog_uri = oplog_uri

    def read_all(self):
        """Return all oplog entries in chronological order."""
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        logs = []
        while cursor.next() == 0:
            logs.append(json.loads(cursor.get_value()))
        cursor.close()
        return logs

    def read_from(self, checkpoint_key=None, *, skip_internal=True):
        """
        Read oplog entries after the given checkpoint key.
        Returns list of (key, entry) tuples.
        """
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        entries = []
        past_checkpoint = checkpoint_key is None

        while cursor.next() == 0:
            key = cursor.get_key()
            if not past_checkpoint:
                if key == checkpoint_key:
                    past_checkpoint = True
                continue

            entry = json.loads(cursor.get_value())
            if skip_internal and entry.get("internal"):
                continue
            entries.append((key, entry))

        cursor.close()
        return entries

    def latest_key(self):
        """Return the key of the most recent oplog entry, or None."""
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        last_key = None
        while cursor.next() == 0:
            last_key = cursor.get_key()
        cursor.close()
        return last_key
