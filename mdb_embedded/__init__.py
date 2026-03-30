"""
mdb_embedded -- A local-first MongoDB emulator powered by WiredTiger.

Drop-in PyMongo-compatible API with:
    - WiredTiger B-Tree storage (same engine family as MongoDB)
    - B-Tree backed indexes with a query planner
    - Bidirectional sync to MongoDB Atlas
    - Full MQL query and aggregation pipeline support
"""

from .client import MongoClient, Database, Collection
from .sync import SyncManager
from .index import DuplicateKeyError

__all__ = [
    "MongoClient",
    "Database",
    "Collection",
    "SyncManager",
    "DuplicateKeyError",
]
