"""
Sync Layer -- bidirectional sync between the local embedded engine and MongoDB Atlas.

Architecture:
    - Background thread tails the local oplog and pushes mutations to Atlas
    - Optionally pulls remote changes via change streams or timestamp polling
    - Conflict resolution strategies: LWW, local-wins, remote-wins, or custom callable
    - Checkpoints, DLQ, and tombstones: redb KV tables via the Rust ``RedbLocalClient``
    - Exponential backoff on consecutive errors, per-collection selective filters
    - MQL-native sync rules with $$NOW, $$NODE_ID, and user-defined variable substitution
"""

from .conflict import (
    VectorClock,
    _RESOLVERS,
    _apply_commutative_to_doc,
    _crdt_counter_merge,
    _crdt_merge_doc,
    _crdt_set_merge,
    _diff_fields,
    _field_merge,
    _is_commutative_op,
    _local_wins,
    _lww,
    _merge_commutative_ops,
    _remote_wins,
    _resolve_variables,
)
from .manager import SyncManager
from .tombstone import DEFAULT_TOMBSTONE_TTL_SEC, TombstoneRegistry

try:
    from pymongo.errors import BulkWriteError, PyMongoError
except ImportError:

    class BulkWriteError(Exception):  # type: ignore[no-redef]
        pass

    class PyMongoError(Exception):  # type: ignore[no-redef]
        pass


__all__ = [
    "BulkWriteError",
    "DEFAULT_TOMBSTONE_TTL_SEC",
    "PyMongoError",
    "SyncManager",
    "TombstoneRegistry",
    "VectorClock",
    "_RESOLVERS",
    "_apply_commutative_to_doc",
    "_crdt_counter_merge",
    "_crdt_merge_doc",
    "_crdt_set_merge",
    "_diff_fields",
    "_field_merge",
    "_is_commutative_op",
    "_local_wins",
    "_lww",
    "_merge_commutative_ops",
    "_remote_wins",
    "_resolve_variables",
]
