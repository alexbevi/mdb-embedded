"""
Local Embedded Engine -- WiredTiger storage layer.

Each collection is a WiredTiger B-Tree table (key=_id, value=JSON document).
Write operations maintain indexes and log to the oplog.
"""

import json
import os
import uuid

try:
    import wiredtiger as wt
except ImportError:
    wt = None

from .query import compile_query, apply_update, get_value
from .aggregation import Cursor
from .index import IndexManager, QueryPlanner, DuplicateKeyError
from .oplog import OplogWriter, OplogReader


class LocalClient:
    """Top-level WiredTiger connection manager."""

    def __init__(self, db_path):
        if not wt:
            raise ImportError("wiredtiger required for local embedded mode")
        os.makedirs(db_path, exist_ok=True)
        self.conn = wt.wiredtiger_open(db_path, "create")

    def get_db(self, name):
        return LocalDB(self.conn, name)


class LocalDB:
    def __init__(self, conn, name):
        self.conn = conn
        self.name = name

    def get_collection(self, name):
        return LocalCollection(self.conn, self.name, name)


class LocalCollection:
    """
    A single collection backed by WiredTiger, with index support and oplog.

    Write paths:
        1. Mutate the data table
        2. Maintain every index via IndexManager
        3. Append to the oplog via OplogWriter
    """

    def __init__(self, conn, db_name, name):
        self.conn = conn
        self.db_name = db_name
        self.name = name
        self.namespace = f"{db_name}.{name}"

        self.table_uri = f"table:{db_name}_{name}"
        self.oplog_uri = f"table:__oplog_{db_name}_{name}"

        self.session = conn.open_session()

        self.session.create(self.table_uri, "key_format=S,value_format=S")
        self.session.create(self.oplog_uri, "key_format=S,value_format=S")

        self._oplog_w = OplogWriter(self.session, self.oplog_uri, self.namespace)
        self._oplog_r = OplogReader(self.session, self.oplog_uri)

        self.index_mgr = IndexManager(self.session, db_name, name)
        self.planner = QueryPlanner(self.index_mgr)

        self._doc_versions = {}  # _id -> version counter

    # -- Result wrapper ------------------------------------------------

    class Result:
        def __init__(self, count, ids=None):
            self.modified_count = count
            self.deleted_count = count
            self.inserted_ids = ids or []

    # -- reads ---------------------------------------------------------

    def get_all(self):
        cursor = self.session.open_cursor(self.table_uri, None, None)
        docs = []
        while cursor.next() == 0:
            docs.append(json.loads(cursor.get_value()))
        cursor.close()
        return docs

    def get_by_id(self, doc_id):
        """O(log n) primary-key lookup via WiredTiger."""
        cursor = self.session.open_cursor(self.table_uri, None, None)
        cursor.set_key(str(doc_id))
        if cursor.search() == 0:
            doc = json.loads(cursor.get_value())
            cursor.close()
            return doc
        cursor.close()
        return None

    def get_by_ids(self, doc_ids):
        """Batch primary-key lookup."""
        cursor = self.session.open_cursor(self.table_uri, None, None)
        docs = []
        for did in doc_ids:
            cursor.set_key(str(did))
            if cursor.search() == 0:
                docs.append(json.loads(cursor.get_value()))
        cursor.close()
        return docs

    def find(self, query):
        """Execute a find using the query planner."""
        plan = self.planner.plan(query)

        if plan.plan_type == "pk_lookup":
            doc = self.get_by_id(query["_id"])
            remaining = {k: v for k, v in query.items() if k != "_id"}
            if doc and remaining:
                fn = compile_query(remaining)
                return [doc] if fn(doc) else []
            return [doc] if doc else []

        if plan.plan_type == "index_scan":
            ids = self.planner.execute_index_scan(plan, self.session, self.table_uri)
            docs = self.get_by_ids(ids)
            fn = compile_query(query)
            return [d for d in docs if fn(d)]

        docs = self.get_all()
        fn = compile_query(query)
        return [d for d in docs if fn(d)]

    def explain(self, query):
        """Return the query execution plan without running it."""
        return self.planner.plan(query or {}).to_dict()

    # -- writes --------------------------------------------------------

    def insert_one(self, doc, *, _internal=False):
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = str(uuid.uuid4())

        self.index_mgr.add_doc(doc)

        cursor = self.session.open_cursor(self.table_uri, None, "overwrite=true")
        cursor[str(doc["_id"])] = json.dumps(doc, default=str)
        cursor.close()

        version = self._bump_version(doc["_id"])
        if not _internal:
            self._oplog_w.log("insert", doc["_id"], doc, version=version)

        return self.Result(1, [doc["_id"]])

    def insert_many(self, docs, *, _internal=False):
        ids = []
        cursor = self.session.open_cursor(self.table_uri, None, "overwrite=true")

        for doc in docs:
            doc = dict(doc)
            if "_id" not in doc:
                doc["_id"] = str(uuid.uuid4())

            self.index_mgr.add_doc(doc)
            cursor[str(doc["_id"])] = json.dumps(doc, default=str)

            version = self._bump_version(doc["_id"])
            if not _internal:
                self._oplog_w.log("insert", doc["_id"], doc, version=version)
            ids.append(doc["_id"])

        cursor.close()
        return self.Result(len(ids), ids)

    def update(self, query, update_spec, multi=True, *, _internal=False):
        fn = compile_query(query)
        docs = self.get_all()
        modified = 0

        cursor = self.session.open_cursor(self.table_uri, None, "overwrite=true")

        for doc in docs:
            if fn(doc):
                old_doc = dict(doc)
                apply_update(doc, update_spec)
                self.index_mgr.update_doc(old_doc, doc)
                cursor[str(doc["_id"])] = json.dumps(doc, default=str)

                version = self._bump_version(doc["_id"])
                if not _internal:
                    self._oplog_w.log("update", doc["_id"], update_spec, version=version)

                modified += 1
                if not multi:
                    break

        cursor.close()
        return self.Result(modified)

    def delete(self, query, multi=True, *, _internal=False):
        fn = compile_query(query)
        docs = self.get_all()
        deleted = 0

        cursor = self.session.open_cursor(self.table_uri, None, "overwrite=true")

        for doc in docs:
            if fn(doc):
                self.index_mgr.remove_doc(doc)
                cursor.set_key(str(doc["_id"]))
                cursor.remove()

                version = self._bump_version(doc["_id"])
                if not _internal:
                    self._oplog_w.log("delete", doc["_id"], None, version=version)

                deleted += 1
                if not multi:
                    break

        cursor.close()
        return self.Result(deleted)

    # -- index management ----------------------------------------------

    def create_index(self, keys, *, _internal=False, **kwargs):
        name = self.index_mgr.create_index(keys, **kwargs)
        self.index_mgr.rebuild_index(name, self.get_all())
        if not _internal:
            self._oplog_w.log(
                "index_create", name,
                {"keys": keys if isinstance(keys, list) else [(keys, 1)], **kwargs},
            )
        return name

    def drop_index(self, name, *, _internal=False):
        self.index_mgr.drop_index(name)
        if not _internal:
            self._oplog_w.log("index_drop", name, None)

    def list_indexes(self):
        return self.index_mgr.list_indexes()

    # -- oplog ---------------------------------------------------------

    def get_oplog(self):
        return self._oplog_r.read_all()

    def get_oplog_reader(self):
        return self._oplog_r

    # -- internal helpers ----------------------------------------------

    def _bump_version(self, doc_id):
        v = self._doc_versions.get(doc_id, 0) + 1
        self._doc_versions[doc_id] = v
        return v
