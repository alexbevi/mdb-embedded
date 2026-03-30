# mdb-embedded

# Building a Local-First MongoDB Emulator in Python (with WiredTiger)

If you’ve ever built a Python application heavily reliant on MongoDB, you know the friction of local development. You either have to spin up a Docker container, maintain a local MongoDB daemon, or rely on heavy mocking libraries like `mongomock` that don't always support the complex aggregation pipelines you need. 

But what if you could have a truly **local-first** database that uses the exact same `PyMongo` API, supports complex Aggregation Pipelines, and persists to disk using **WiredTiger**—the exact same storage engine that powers MongoDB under the hood?

We just built exactly that. Let's break down the architecture of a drop-in MongoDB emulator that fits into a single Python file.

---

## 1. The Magic Switch: The Connection Layer

The core architectural goal was zero code changes for the consumer. You shouldn't have to write `if env == "local"` everywhere in your app. 

We achieved this by building a proxy `MongoClient` class. The URI string dictates the execution engine:

```python
# Remote Mode (Standard PyMongo)
client = MongoClient("mongodb://user:pass@cluster.mongodb.net")

# Local Embedded Mode (Our Engine)
client = MongoClient("local://my_local_wt_data")
```

If it sees `mongodb://`, it instantiates a standard `PyMongo` client. If it sees `local://`, it drops into our embedded engine. Both return wrapper objects (`Database` and `Collection`) that expose the exact same API: `.find()`, `.insert_many()`, `.aggregate()`, etc.

---

## 2. Using MongoDB’s Actual Brain: WiredTiger

This is where the project shifts from a "toy mock" to a highly capable embedded database. We utilized the Python bindings for **WiredTiger**.

WiredTiger is an extensible, high-performance, NoSQL, open-source storage engine that MongoDB acquired in 2014 and uses as its default engine. By using WiredTiger locally, we inherit document-level concurrency, intense write optimization, and highly efficient B-Tree storage.

When our `LocalCollection` initializes, it opens a WiredTiger session and creates a table. 

```python
self.table_uri = f"table:{self.db_name}_{self.name}"
# Key = String (_id), Value = String (JSON doc)
self.session.create(self.table_uri, "key_format=S,value_format=S")
```

Whenever you run `.insert_one()` or `.update_many()`, the engine simply serializes the document to JSON and stores it natively in the WiredTiger B-Tree.

---

## 3. The Pure Python MQL Compiler

MongoDB Query Language (MQL) is famously represented as JSON/Dictionaries. To make our local engine work, we had to build a compiler that translates dictionaries like `{"age": {"$gt": 30}}` into executable Python functions.

We built a recursive `compile_query` function that evaluates documents on the fly:

```python
def compile_query(query):
    def match(doc):
        for key, condition in query.items():
            # Support for logical operators
            if key == "$or":
                if not any(compile_query(sub)(doc) for sub in condition): return False
                continue
            
            value = get_value(doc, key) # Supports "user.stats.logins" dot-notation
            
            # Support for comparison operators
            if isinstance(condition, dict):
                for op, cond_val in condition.items():
                    if op == "$gt" and not (value > cond_val): return False
                    if op == "$in" and not (value in cond_val): return False
            else:
                if value != condition: return False
        return True
    return match
```

When you call `collection.find(query)`, the engine fetches the documents from WiredTiger, compiles your query into a `match` function, and filters the results in memory. 

---

## 4. Aggregation Pipelines on the Edge

One of the hardest things to replicate outside of MongoDB is the Aggregation Pipeline. However, because an aggregation pipeline is just a series of sequential data transformations, we built a local `Cursor` that applies these stages iteratively.

```python
def aggregate(self, pipeline):
    docs = self.docs
    for stage in pipeline:
        op, spec = list(stage.items())[0]

        if op == "$match":
            fn = compile_query(spec)
            docs = [d for d in docs if fn(d)]
        elif op == "$group": 
            docs = group_stage(docs, spec)
        elif op == "$unwind": 
            docs = unwind_stage(docs, spec)
        elif op == "$sort": 
            docs = sort_stage(docs, spec)
            
    return docs
```

You can now test complex `$group` and `$unwind` analytics queries locally without a network round-trip.

---

## 5. The Sync Log (Oplog)

A local-first database isn't very useful if that data remains trapped on the edge forever. To bridge the gap between local execution and remote synchronization, we implemented an **Oplog** (Operations Log).

Every single time a document is inserted, updated, or deleted, the `LocalCollection` writes a chronological event to a dedicated `__oplog_` WiredTiger table.

```python
def _log_op(self, op, doc_id, payload):
    oplog_key = f"{time.time_ns()}-{uuid.uuid4()}"
    log_entry = {
        "ts": time.time(),
        "op": op,
        "doc_id": doc_id,
        "payload": payload
    }
    
    cursor = self.session.open_cursor(self.oplog_uri, None, "overwrite=true")
    cursor[oplog_key] = json.dumps(log_entry)
```

**Why is this a big deal?** Because this lays the exact foundation needed for a background **Sync Worker**. A separate thread can wake up, read the `_oplog` table, push those mutations to your central MongoDB Atlas cluster, and clear the local log. You get immediate UI updates locally, and eventual consistency globally.

---

## Conclusion

By combining the PyMongo API structure, a custom Python MQL compiler, and the raw power of the WiredTiger storage engine, we've created an incredibly potent tool for developers. 

Whether you are building unit tests that need persistent state, creating an edge-compute application, or prototyping a local-first application architecture, you no longer have to compromise on your database syntax.
