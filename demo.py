import os
import uuid
import time
import json
from collections import defaultdict

try:
    from pymongo import MongoClient as _PyMongoClient
except ImportError:
    _PyMongoClient = None

try:
    import wiredtiger as wt
except ImportError:
    wt = None


# ==========================================
# Connection Layer (THE MAGIC SWITCH)
# ==========================================

class MongoClient:
    """
    Drop-in style client:
    - mongodb://...   -> real MongoDB
    - local://./path  -> embedded WiredTiger engine
    """

    def __init__(self, uri="local://local_wt_data"):
        self.uri = uri

        if uri.startswith("mongodb://"):
            if not _PyMongoClient:
                raise ImportError("pymongo required for MongoDB connections")
            self.mode = "remote"
            self.client = _PyMongoClient(uri)
        else:
            if not wt:
                raise ImportError("wiredtiger required for local embedded mode")
            self.mode = "local"
            
            # Extract DB path from local:// path (default to local_wt_data dir)
            db_path = uri.split("://")[1] or "local_wt_data"
            self.client = LocalClient(db_path)

    def __getitem__(self, db_name):
        if self.mode == "remote":
            return Database(self.client[db_name], self.mode)
        return Database(self.client.get_db(db_name), self.mode)


class Database:
    def __init__(self, db, mode):
        self.db = db
        self.mode = mode

    def __getitem__(self, name):
        if self.mode == "remote":
            return Collection(self.db[name], self.mode)
        return Collection(self.db.get_collection(name), self.mode)


# ==========================================
# Collection Abstraction
# ==========================================

class Collection:
    def __init__(self, backend, mode):
        self.backend = backend
        self.mode = mode

    def insert_one(self, doc):
        if self.mode == "remote": return self.backend.insert_one(doc)
        return self.backend.insert_one(doc)

    def insert_many(self, docs):
        if self.mode == "remote": return self.backend.insert_many(docs)
        return self.backend.insert_many(docs)

    def find(self, query=None):
        query = query or {}
        if self.mode == "remote": return self.backend.find(query)
        return Cursor(self.backend.get_all()).find(query)

    def find_one(self, query=None):
        query = query or {}
        if self.mode == "remote": return self.backend.find_one(query)
        docs = Cursor(self.backend.get_all()).find(query).to_list()
        return docs[0] if docs else None

    def aggregate(self, pipeline):
        if self.mode == "remote": return list(self.backend.aggregate(pipeline))
        return Cursor(self.backend.get_all()).aggregate(pipeline)

    def update_one(self, query, update):
        if self.mode == "remote": return self.backend.update_one(query, update)
        return self.backend.update(query, update, multi=False)

    def update_many(self, query, update):
        if self.mode == "remote": return self.backend.update_many(query, update)
        return self.backend.update(query, update, multi=True)

    def delete_one(self, query):
        if self.mode == "remote": return self.backend.delete_one(query)
        return self.backend.delete(query, multi=False)

    def delete_many(self, query):
        if self.mode == "remote": return self.backend.delete_many(query)
        return self.backend.delete(query, multi=True)

    def count_documents(self, query):
        if self.mode == "remote": return self.backend.count_documents(query)
        return len(self.find(query).to_list())

    def get_oplog(self):
        if self.mode == "local": return self.backend.get_oplog()
        return []


# ==========================================
# Local Embedded Engine & WiredTiger Storage
# ==========================================

class LocalClient:
    def __init__(self, db_path):
        # WiredTiger requires the directory to exist
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
    def __init__(self, conn, db_name, name):
        self.conn = conn
        self.db_name = db_name
        self.name = name
        
        self.table_uri = f"table:{self.db_name}_{self.name}"
        self.oplog_uri = f"table:__oplog_{self.db_name}_{self.name}"
        
        self.session = self.conn.open_session()
        
        # Initialize tables: Key = String (_id), Value = String (JSON doc)
        self.session.create(self.table_uri, "key_format=S,value_format=S")
        self.session.create(self.oplog_uri, "key_format=S,value_format=S")

    def _log_op(self, op, doc_id, payload):
        # Use nano-time + UUID to ensure oplog is chronologically sortable by key
        oplog_key = f"{time.time_ns()}-{uuid.uuid4()}"
        log_entry = {
            "ts": time.time(),
            "op": op,
            "doc_id": doc_id,
            "payload": payload
        }
        
        cursor = self.session.open_cursor(self.oplog_uri, None, "overwrite=true")
        cursor[oplog_key] = json.dumps(log_entry)
        cursor.close()

    def get_all(self):
        cursor = self.session.open_cursor(self.table_uri, None, None)
        docs = []
        # Iterate over the WiredTiger B-Tree
        while cursor.next() == 0:
            val = cursor.get_value()
            docs.append(json.loads(val))
        cursor.close()
        return docs

    class Result:
        def __init__(self, count, ids=None):
            self.modified_count = count
            self.deleted_count = count
            self.inserted_ids = ids or []

    def insert_one(self, doc):
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = str(uuid.uuid4())
            
        cursor = self.session.open_cursor(self.table_uri, None, "overwrite=true")
        cursor[doc["_id"]] = json.dumps(doc)
        cursor.close()
        
        self._log_op("insert", doc["_id"], doc)
        return self.Result(1, [doc["_id"]])

    def insert_many(self, docs):
        ids = []
        cursor = self.session.open_cursor(self.table_uri, None, "overwrite=true")
        
        for doc in docs:
            doc = dict(doc)
            if "_id" not in doc: 
                doc["_id"] = str(uuid.uuid4())
            cursor[doc["_id"]] = json.dumps(doc)
            self._log_op("insert", doc["_id"], doc)
            ids.append(doc["_id"])
            
        cursor.close()
        return self.Result(len(ids), ids)

    def update(self, query, update_spec, multi=True):
        fn = compile_query(query)
        docs = self.get_all()
        modified = 0

        cursor = self.session.open_cursor(self.table_uri, None, "overwrite=true")
        
        for doc in docs:
            if fn(doc):
                apply_update(doc, update_spec)
                cursor[doc["_id"]] = json.dumps(doc)
                self._log_op("update", doc["_id"], update_spec)
                modified += 1
                if not multi:
                    break
                    
        cursor.close()
        return self.Result(modified)

    def delete(self, query, multi=True):
        fn = compile_query(query)
        docs = self.get_all()
        deleted = 0

        cursor = self.session.open_cursor(self.table_uri, None, "overwrite=true")
        
        for doc in docs:
            if fn(doc):
                cursor.set_key(doc["_id"])
                cursor.remove()
                self._log_op("delete", doc["_id"], None)
                deleted += 1
                if not multi:
                    break
                    
        cursor.close()
        return self.Result(deleted)

    def get_oplog(self):
        cursor = self.session.open_cursor(self.oplog_uri, None, None)
        logs = []
        # Keys are chronologically sortable by design
        while cursor.next() == 0:
            logs.append(json.loads(cursor.get_value()))
        cursor.close()
        return logs


# ==========================================
# Cursor + Execution Engine
# ==========================================

class Cursor:
    def __init__(self, docs):
        self.docs = docs

    def find(self, query):
        fn = compile_query(query)
        return Cursor([d for d in self.docs if fn(d)])

    def aggregate(self, pipeline):
        docs = self.docs

        for stage in pipeline:
            op, spec = list(stage.items())[0]

            if op == "$match":
                fn = compile_query(spec)
                docs = [d for d in docs if fn(d)]
            elif op == "$group": docs = group_stage(docs, spec)
            elif op == "$project": docs = project_stage(docs, spec)
            elif op == "$sort": docs = sort_stage(docs, spec)
            elif op == "$limit": docs = docs[:spec]
            elif op == "$skip": docs = docs[spec:]
            elif op == "$unwind": docs = unwind_stage(docs, spec)
            else: raise NotImplementedError(f"{op} not supported")

        return docs

    def to_list(self): return self.docs
    def __iter__(self): return iter(self.docs)


# ==========================================
# MQL Compiler (Queries & Updates)
# ==========================================

def compile_query(query):
    def match(doc):
        for key, condition in query.items():
            if key == "$or":
                if not any(compile_query(sub)(doc) for sub in condition): return False
                continue
            if key == "$and":
                if not all(compile_query(sub)(doc) for sub in condition): return False
                continue

            value = get_value(doc, key)

            if isinstance(condition, dict):
                for op, cond_val in condition.items():
                    if op == "$gt" and not (value is not None and value > cond_val): return False
                    elif op == "$lt" and not (value is not None and value < cond_val): return False
                    elif op == "$gte" and not (value is not None and value >= cond_val): return False
                    elif op == "$lte" and not (value is not None and value <= cond_val): return False
                    elif op == "$eq" and not (value == cond_val): return False
                    elif op == "$in" and not (value in cond_val): return False
                    elif op == "$ne" and not (value != cond_val): return False
                    elif op == "$exists": 
                        exists = value is not None
                        if cond_val and not exists: return False
                        if not cond_val and exists: return False
            else:
                if value != condition:
                    return False

        return True

    return match

def apply_update(doc, update):
    for op, fields in update.items():
        if op == "$set":
            for k, v in fields.items(): set_value(doc, k, v)
        elif op == "$inc":
            for k, v in fields.items():
                current = get_value(doc, k) or 0
                set_value(doc, k, current + v)
        elif op == "$push":
            for k, v in fields.items():
                arr = get_value(doc, k) or []
                if not isinstance(arr, list): arr = [arr]
                arr.append(v)
                set_value(doc, k, arr)
        elif op == "$unset":
            for k in fields.keys(): unset_value(doc, k)
        else:
            raise NotImplementedError(f"Update operator {op} not supported")

def get_value(doc, key):
    parts = key.split(".")
    val = doc
    for p in parts:
        if not isinstance(val, dict): return None
        val = val.get(p)
        if val is None: return None
    return val

def set_value(doc, key, value):
    parts = key.split(".")
    d = doc
    for p in parts[:-1]:
        if p not in d or not isinstance(d[p], dict): d[p] = {}
        d = d[p]
    d[parts[-1]] = value

def unset_value(doc, key):
    parts = key.split(".")
    d = doc
    for p in parts[:-1]:
        if p not in d or not isinstance(d[p], dict): return
        d = d[p]
    if parts[-1] in d:
        del d[parts[-1]]


# ==========================================
# Aggregation Stages
# ==========================================

def group_stage(docs, spec):
    grouped = defaultdict(list)
    for doc in docs:
        key = resolve_expr(doc, spec["_id"])
        if isinstance(key, (dict, list)): key = json.dumps(key, sort_keys=True)
        grouped[key].append(doc)

    results = []
    for key, group_docs in grouped.items():
        try: key = json.loads(key) 
        except (TypeError, json.JSONDecodeError): pass

        out = {"_id": key}
        for field, expr in spec.items():
            if field == "_id": continue
            op, val = list(expr.items())[0]
            if op == "$sum":
                if val == 1: out[field] = len(group_docs)
                else: out[field] = sum(resolve_expr(d, val) or 0 for d in group_docs)
            elif op == "$push":
                out[field] = [resolve_expr(d, val) for d in group_docs]

        results.append(out)
    return results

def project_stage(docs, spec):
    out = []
    for doc in docs:
        new_doc = {}
        for field, expr in spec.items():
            if expr == 1: new_doc[field] = get_value(doc, field)
            elif expr == 0: continue 
            else: new_doc[field] = resolve_expr(doc, expr)
        out.append(new_doc)
    return out

def sort_stage(docs, spec):
    for field, direction in reversed(list(spec.items())):
        docs = sorted(
            docs,
            key=lambda d: (get_value(d, field) is not None, get_value(d, field)),
            reverse=(direction == -1)
        )
    return docs

def unwind_stage(docs, spec):
    out = []
    path = spec[1:] if spec.startswith("$") else spec
    for doc in docs:
        val = get_value(doc, path)
        if isinstance(val, list):
            for item in val:
                new_doc = doc.copy()
                set_value(new_doc, path, item)
                out.append(new_doc)
        elif val is not None:
            out.append(doc)
    return out

def resolve_expr(doc, expr):
    if isinstance(expr, str) and expr.startswith("$"):
        return get_value(doc, expr[1:])
    return expr


# ==========================================
# DEMO USAGE
# ==========================================

if __name__ == "__main__":
    # SWITCH HERE:
    # uri = "mongodb://localhost:27017"
    uri = "local://local_wt_data" # Creates a directory powered by WiredTiger!

    client = MongoClient(uri)
    db = client["test_db"]
    users = db["users"]

    # Clear previous runs if testing locally
    if client.mode == "local":
        users.delete_many({})

    print("--- 1. INSERTING DATA (WiredTiger Engine) ---")
    users.insert_many([
        {"name": "Alice", "age": 34, "city": "NYC", "tags": ["python"]},
        {"name": "Bob", "age": 28, "city": "SF", "tags": ["js", "react"]},
        {"name": "Charlie", "age": 40, "city": "NYC", "tags": ["python", "go"]},
    ])
    print(f"Total Users: {users.count_documents({})}")

    print("\n--- 2. COMPLEX FIND ($or / $and) ---")
    query = {"$or": [{"age": {"$lt": 30}}, {"city": "NYC"}]}
    for doc in users.find(query):
        print(f"{doc['name']} - {doc['age']} - {doc['city']}")

    print("\n--- 3. UPDATING ($push and $unset) ---")
    users.update_one({"name": "Alice"}, {"$push": {"tags": "rust"}, "$unset": {"age": ""}})
    print(users.find_one({"name": "Alice"}))

    print("\n--- 4. AGGREGATION ($unwind and $group) ---")
    pipeline = [
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for r in users.aggregate(pipeline):
        print(r)
        
    print("\n--- 5. SYNC LOG (OPLOG via WT) ---")
    if client.mode == "local":
        oplog = users.get_oplog()
        print(f"Recorded {len(oplog)} mutations in the WiredTiger oplog table.")
        print("Latest Operation:", oplog[-1])
