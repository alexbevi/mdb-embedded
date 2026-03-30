"""
Connection Layer -- the magic switch between remote MongoDB and local WiredTiger.

MongoClient("mongodb://...")   -> real PyMongo
MongoClient("local://./path")  -> embedded WiredTiger engine
"""

try:
    from pymongo import MongoClient as _PyMongoClient
except ImportError:
    _PyMongoClient = None

from .storage import LocalClient, LocalCollection
from .aggregation import Cursor
from .query import compile_query


class MongoClient:
    """
    Drop-in client that routes to either real MongoDB or the local engine.
    The URI string dictates which backend is used.
    """

    def __init__(self, uri="local://local_wt_data"):
        self.uri = uri

        if uri.startswith(("mongodb://", "mongodb+srv://")):
            if not _PyMongoClient:
                raise ImportError("pymongo required for MongoDB connections")
            self.mode = "remote"
            self.client = _PyMongoClient(uri)
        else:
            self.mode = "local"
            db_path = uri.split("://")[1] if "://" in uri else uri
            db_path = db_path or "local_wt_data"
            self.client = LocalClient(db_path)

    def __getitem__(self, db_name):
        if self.mode == "remote":
            return Database(self.client[db_name], self.mode)
        return Database(self.client.get_db(db_name), self.mode)

    def get_local_client(self):
        """Return the underlying LocalClient (only available in local mode)."""
        if self.mode != "local":
            raise RuntimeError("get_local_client() only available in local mode")
        return self.client


class Database:
    def __init__(self, db, mode):
        self.db = db
        self.mode = mode
        self._collections = {}

    def __getitem__(self, name):
        if name in self._collections:
            return self._collections[name]

        if self.mode == "remote":
            coll = Collection(self.db[name], self.mode)
        else:
            coll = Collection(self.db.get_collection(name), self.mode)

        self._collections[name] = coll
        return coll

    def list_collection_names(self):
        if self.mode == "remote":
            return self.db.list_collection_names()
        return list(self._collections.keys())


class Collection:
    """Unified collection API -- delegates to either PyMongo or LocalCollection."""

    def __init__(self, backend, mode):
        self.backend = backend
        self.mode = mode

    # -- reads ---------------------------------------------------------

    def find(self, query=None):
        query = query or {}
        if self.mode == "remote":
            return self.backend.find(query)
        docs = self.backend.find(query)
        return Cursor(docs)

    def find_one(self, query=None):
        query = query or {}
        if self.mode == "remote":
            return self.backend.find_one(query)
        docs = self.backend.find(query)
        return docs[0] if docs else None

    def aggregate(self, pipeline):
        if self.mode == "remote":
            return list(self.backend.aggregate(pipeline))
        docs = self.backend.get_all()
        return Cursor(docs).aggregate(pipeline)

    def count_documents(self, query=None):
        query = query or {}
        if self.mode == "remote":
            return self.backend.count_documents(query)
        return len(self.backend.find(query))

    def explain(self, query=None):
        """Return the query plan (local mode only)."""
        if self.mode == "remote":
            return {"plan": "remote"}
        return self.backend.explain(query or {})

    # -- writes --------------------------------------------------------

    def insert_one(self, doc):
        if self.mode == "remote":
            return self.backend.insert_one(doc)
        return self.backend.insert_one(doc)

    def insert_many(self, docs):
        if self.mode == "remote":
            return self.backend.insert_many(docs)
        return self.backend.insert_many(docs)

    def update_one(self, query, update):
        if self.mode == "remote":
            return self.backend.update_one(query, update)
        return self.backend.update(query, update, multi=False)

    def update_many(self, query, update):
        if self.mode == "remote":
            return self.backend.update_many(query, update)
        return self.backend.update(query, update, multi=True)

    def delete_one(self, query):
        if self.mode == "remote":
            return self.backend.delete_one(query)
        return self.backend.delete(query, multi=False)

    def delete_many(self, query):
        if self.mode == "remote":
            return self.backend.delete_many(query)
        return self.backend.delete(query, multi=True)

    # -- indexes -------------------------------------------------------

    def create_index(self, keys, **kwargs):
        if self.mode == "remote":
            return self.backend.create_index(keys, **kwargs)
        return self.backend.create_index(keys, **kwargs)

    def drop_index(self, name):
        if self.mode == "remote":
            return self.backend.drop_index(name)
        return self.backend.drop_index(name)

    def list_indexes(self):
        if self.mode == "remote":
            return list(self.backend.list_indexes())
        return self.backend.list_indexes()

    # -- oplog ---------------------------------------------------------

    def get_oplog(self):
        if self.mode == "local":
            return self.backend.get_oplog()
        return []

    def get_local_collection(self) -> LocalCollection:
        """Return the underlying LocalCollection (local mode only)."""
        if self.mode != "local":
            raise RuntimeError("get_local_collection() only available in local mode")
        return self.backend
