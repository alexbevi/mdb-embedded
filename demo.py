#!/usr/bin/env python3
"""
smongo -- Small MongoDB, big ambitions. One query language to rule them all.
"""

import os
import time

from smongo import DeleteOne, InsertOne, MongoClient, UpdateOne, ValidationError


def banner(n, title):
    print(f"\n{'─' * 60}")
    print(f"  {n}. {title}")
    print(f"{'─' * 60}\n")


def main():
    print()
    print("  ╔══════════════════════════════════════════════════╗")
    print("  ║   smongo  ·  Small MongoDB, big ambitions        ║")
    print("  ║   redb + Rust engine  ·  MQL  ·  Zero network   ║")
    print("  ╚══════════════════════════════════════════════════╝")

    sync_uri = os.environ.get("MDB_SYNC_URI")
    client = (
        MongoClient("local://demo_redb_data", sync=sync_uri)
        if sync_uri
        else MongoClient("local://demo_redb_data")
    )
    db = client["demo"]
    try:
        users = db.create_collection(
            "users",
            validator={
                "$jsonSchema": {
                    "required": ["name", "age", "dept"],
                    "properties": {
                        "name": {"type": "string", "minLength": 2},
                        "age": {"type": "int", "minimum": 18},
                        "dept": {"type": "string"},
                    },
                }
            },
        )
    except (RuntimeError, ValueError, OSError):
        users = db["users"]
    users.delete_many({})

    # ── 1. Indexes ────────────────────────────────────────────
    banner(1, "B-TREE INDEXES")

    users.create_index([("age", 1)])
    users.create_index([("city", 1), ("age", -1)])
    users.create_index("name", unique=True)
    users.create_index([("dept", 1)])
    users.create_index([("salary", -1)])
    users.create_index([("expiresAt", 1)], expireAfterSeconds=5)

    index_count = 0
    for idx in users.list_indexes():
        flag = "  UNIQUE" if idx["unique"] else ""
        print(f"    {idx['name']:25s} keys={idx['keys']}{flag}")
        index_count += 1

    print(f"\n    {index_count} indexes created on the embedded engine")

    # ── 2. Insert ─────────────────────────────────────────────
    banner(2, "INSERT  (same API as PyMongo)")

    docs = [
        {
            "name": "Alice",
            "age": 34,
            "city": "NYC",
            "dept": "engineering",
            "salary": 145000,
            "tags": ["python", "mongodb"],
        },
        {
            "name": "Bob",
            "age": 28,
            "city": "SF",
            "dept": "engineering",
            "salary": 128000,
            "tags": ["js", "react"],
        },
        {
            "name": "Charlie",
            "age": 40,
            "city": "NYC",
            "dept": "management",
            "salary": 175000,
            "tags": ["python", "go"],
        },
        {
            "name": "Diana",
            "age": 25,
            "city": "LA",
            "dept": "design",
            "salary": 98000,
            "tags": ["rust", "figma"],
        },
        {
            "name": "Eve",
            "age": 31,
            "city": "SF",
            "dept": "engineering",
            "salary": 155000,
            "tags": ["python", "ml"],
        },
        {
            "name": "Frank",
            "age": 36,
            "city": "CHI",
            "dept": "engineering",
            "salary": 140000,
            "tags": ["go", "k8s"],
        },
        {
            "name": "Grace",
            "age": 29,
            "city": "NYC",
            "dept": "data",
            "salary": 135000,
            "tags": ["python", "spark"],
        },
        {
            "name": "Hank",
            "age": 45,
            "city": "SF",
            "dept": "management",
            "salary": 190000,
            "tags": ["strategy"],
        },
        {
            "name": "Ivy",
            "age": 27,
            "city": "LA",
            "dept": "design",
            "salary": 105000,
            "tags": ["figma", "css"],
        },
        {
            "name": "Jack",
            "age": 33,
            "city": "NYC",
            "dept": "engineering",
            "salary": 142000,
            "tags": ["java", "spring"],
        },
    ]
    users.insert_many(docs)
    print(f"    Inserted {users.count_documents({})} documents into local storage\n")
    try:
        users.insert_one({"name": "X", "age": 14, "dept": "intern"})
    except ValidationError as exc:
        print(f"    Schema validation blocked bad doc -> {exc}")

    for d in docs[:3]:
        print(
            f"    {d['name']:10s}  age={d['age']}  city={d['city']}  dept={d['dept']}  ${d['salary']:,}"
        )
    print(f"    ... and {len(docs) - 3} more")

    # ── 3. Query planner and cursor chaining ───────────────────
    banner(3, "QUERY PLANNER  (explain)")

    queries = [
        {"age": {"$gt": 30}},
        {"city": "NYC", "age": {"$lte": 35}},
        {"name": "Alice"},
        {"dept": "engineering"},
        {"salary": {"$gte": 150000}},
        {"$or": [{"city": "SF"}, {"city": "LA"}]},
        {},
    ]
    for q in queries:
        plan = users.explain(q)
        tag = plan.get("index", plan["plan"].upper())
        print(f"    {q!s:50s}  ->  {plan['plan']:14s} ({tag})")

    print("\n    cursor chaining (.sort().skip().limit().projection()):")
    for doc in (
        users.find({"dept": "engineering"})
        .sort("salary", -1)
        .skip(1)
        .limit(2)
        .projection({"name": 1, "salary": 1, "_id": 0})
    ):
        print(f"      {doc}")

    # ── 4. Indexed finds ──────────────────────────────────────
    banner(4, "FIND  (indexed queries)")

    print("    age > 35  (index: age_1):")
    for doc in users.find({"age": {"$gt": 35}}):
        print(f"      {doc['name']:10s}  age={doc['age']}")

    print("\n    city=NYC AND dept=engineering:")
    for doc in users.find({"city": "NYC", "dept": "engineering"}):
        print(f"      {doc['name']:10s}  salary=${doc['salary']:,}")

    print("\n    salary >= $150,000  (index: salary_-1):")
    for doc in users.find({"salary": {"$gte": 150000}}):
        print(f"      {doc['name']:10s}  ${doc['salary']:,}  {doc['dept']}")

    # ── 5. Updates ────────────────────────────────────────────
    banner(5, "UPDATE OPERATORS  (extended set)")

    users.update_one({"name": "Alice"}, {"$push": {"tags": "rust"}, "$inc": {"salary": 10000}})
    alice = users.find_one({"name": "Alice"})
    print(f"    Alice after update:  salary=${alice['salary']:,}  tags={alice['tags']}")

    users.update_many({"dept": "engineering"}, {"$inc": {"salary": 5000}})
    users.update_one({"name": "Bob"}, {"$addToSet": {"tags": {"$each": ["python", "react"]}}})
    users.update_one(
        {"name": "Diana"}, {"$rename": {"city": "location"}, "$currentDate": {"updatedAt": True}}
    )
    users.update_one({"name": "Eve"}, {"$mul": {"salary": 1.05}, "$max": {"salary": 200000}})
    print("\n    All engineers got a $5k raise:")
    for doc in users.find({"dept": "engineering"}):
        print(f"      {doc['name']:10s}  ${doc['salary']:,}")

    # ── 6. Bulk write ─────────────────────────────────────────
    banner(6, "BULK WRITE  (ordered + error tracking)")

    result = users.bulk_write(
        [
            InsertOne(
                {
                    "name": "Zara",
                    "age": 26,
                    "city": "BOS",
                    "dept": "data",
                    "salary": 115000,
                    "tags": ["python"],
                }
            ),
            UpdateOne({"name": "Bob"}, {"$inc": {"salary": 3000}}),
            DeleteOne({"name": "Zara"}),
        ]
    )
    print(
        f"    inserted={result.inserted_count}  modified={result.modified_count}  deleted={result.deleted_count}"
    )
    print(f"    write_errors={result.write_errors}")

    # ── 7. Aggregation ────────────────────────────────────────
    banner(7, "AGGREGATION PIPELINE  (18 stages)")

    print("    Average salary by department:")
    for r in users.aggregate(
        [
            {"$group": {"_id": "$dept", "avg_salary": {"$avg": "$salary"}, "count": {"$sum": 1}}},
            {"$sort": {"avg_salary": -1}},
        ]
    ):
        print(f"      {r['_id']:15s}  avg=${r['avg_salary']:,.0f}  ({r['count']} people)")

    print("\n    $facet -- parallel sub-pipelines:")
    facet_result = users.aggregate(
        [
            {
                "$facet": {
                    "by_dept": [
                        {"$group": {"_id": "$dept", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                    ],
                    "top_3": [
                        {"$sort": {"salary": -1}},
                        {"$limit": 3},
                        {"$project": {"name": 1, "salary": 1, "_id": 0}},
                    ],
                }
            },
        ]
    )
    for r in facet_result:
        print(f"      by_dept: {r['by_dept']}")
        print(f"      top_3:   {r['top_3']}")

    departments = db["departments"]
    departments.delete_many({})
    departments.insert_many(
        [
            {"name": "engineering", "head": "Charlie"},
            {"name": "design", "head": "Ivy"},
            {"name": "management", "head": "Hank"},
            {"name": "data", "head": "Grace"},
        ]
    )
    print("\n    $lookup join users -> departments:")
    for r in users.aggregate(
        [
            {
                "$lookup": {
                    "from": "departments",
                    "localField": "dept",
                    "foreignField": "name",
                    "as": "deptInfo",
                }
            },
            {"$limit": 2},
        ]
    ):
        print(f"      {r['name']} deptInfo={len(r.get('deptInfo', []))}")

    print("\n    Tag popularity (unwind + group):")
    for r in users.aggregate(
        [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 6},
        ]
    ):
        print(f"      {r['_id']:12s}  {r['count']}")

    print("\n    $vectorSearch (cosine similarity):")
    for i, doc in enumerate(users.find({})):
        emb = [float((i % 3) == 0), float((i % 3) == 1), float((i % 3) == 2)]
        users.update_one({"_id": doc["_id"]}, {"$set": {"embedding": emb}})

    for r in users.aggregate(
        [
            {
                "$vectorSearch": {
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "limit": 3,
                    "metric": "cosine",
                }
            }
        ]
    ):
        print(f"      {r['name']:10s}  score={r['_vectorScore']:.4f}")

    # ── 8. Change stream + oplog ──────────────────────────────
    banner(8, "CHANGE STREAM + OPLOG")

    stream = users.watch()
    users.insert_one(
        {
            "name": "TTL user",
            "age": 29,
            "city": "NYC",
            "dept": "engineering",
            "salary": 100000,
            "expiresAt": time.time() - 10,
        }
    )
    event = stream.try_next()
    if event:
        print(f"    change stream event: {event['operationType']} for {event['documentKey']}")
    stream.close()

    oplog = users.get_oplog()
    ops = {}
    for e in oplog:
        ops[e["op"]] = ops.get(e["op"], 0) + 1

    print(f"    {len(oplog)} operations recorded in the oplog:\n")
    for op, count in sorted(ops.items()):
        print(f"      {op:18s}  {count}")

    # ── 9. Sync (when hybrid mode is active) ──────────────────
    if client.sync:
        banner(9, "SYNC  (bidirectional to MongoDB)")
        client.sync.push()
        status = client.sync.status()
        print(
            f"    state={status['state']}  pushed={status['pushed']}  pulled={status['pulled']}  conflicts={status['conflicts']}"
        )
        client.sync.pull()
        status = client.sync.status()
        print(f"    after pull: pushed={status['pushed']}  pulled={status['pulled']}")

    # ── Done ──────────────────────────────────────────────────
    print(f"\n{'─' * 60}")
    print("  Small MongoDB. Big ambitions. Zero compromises.")
    print()
    print("  This entire demo ran against the embedded redb engine.")
    print("  Change the URI to mongodb+srv:// and every line above")
    print("  runs against Atlas instead. Zero code changes.")
    if sync_uri:
        print("  Hybrid mode enabled: local CRUD + automatic Atlas sync.")
    print()
    print("  Start the web dashboard:  docker compose up --build")
    print("  Open:                     http://localhost:5000")
    print(f"{'─' * 60}\n")


if __name__ == "__main__":
    main()
