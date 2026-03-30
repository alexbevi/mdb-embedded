#!/usr/bin/env python3
"""
mdb-embedded  --  One Query Language, Every Environment
========================================================
Run this standalone (no Docker, no network) to see the full embedded engine:
    python demo.py

Same MQL you write against Atlas works here against a local WiredTiger B-Tree.
"""

from mdb_embedded import MongoClient


def banner(n, title):
    print(f"\n{'─' * 60}")
    print(f"  {n}. {title}")
    print(f"{'─' * 60}\n")


def main():
    print()
    print("  ╔══════════════════════════════════════════════════╗")
    print("  ║   mdb-embedded  ·  local-first MongoDB engine   ║")
    print("  ║   WiredTiger B-Trees  ·  MQL  ·  Zero network   ║")
    print("  ╚══════════════════════════════════════════════════╝")

    # The only line that changes between local and Atlas
    client = MongoClient("local://demo_wt_data")
    db = client["demo"]
    users = db["users"]
    users.delete_many({})

    # ── 1. Indexes ────────────────────────────────────────────
    banner(1, "B-TREE INDEXES")

    users.create_index([("age", 1)])
    users.create_index([("city", 1), ("age", -1)])
    users.create_index("name", unique=True)
    users.create_index([("dept", 1)])
    users.create_index([("salary", -1)])

    for idx in users.list_indexes():
        flag = "  UNIQUE" if idx["unique"] else ""
        print(f"    {idx['name']:25s} keys={idx['keys']}{flag}")

    print(f"\n    5 indexes created on WiredTiger B-Trees")

    # ── 2. Insert ─────────────────────────────────────────────
    banner(2, "INSERT  (same API as PyMongo)")

    docs = [
        {"name": "Alice",   "age": 34, "city": "NYC", "dept": "engineering", "salary": 145000, "tags": ["python", "mongodb"]},
        {"name": "Bob",     "age": 28, "city": "SF",  "dept": "engineering", "salary": 128000, "tags": ["js", "react"]},
        {"name": "Charlie", "age": 40, "city": "NYC", "dept": "management",  "salary": 175000, "tags": ["python", "go"]},
        {"name": "Diana",   "age": 25, "city": "LA",  "dept": "design",      "salary": 98000,  "tags": ["rust", "figma"]},
        {"name": "Eve",     "age": 31, "city": "SF",  "dept": "engineering", "salary": 155000, "tags": ["python", "ml"]},
        {"name": "Frank",   "age": 36, "city": "CHI", "dept": "engineering", "salary": 140000, "tags": ["go", "k8s"]},
        {"name": "Grace",   "age": 29, "city": "NYC", "dept": "data",        "salary": 135000, "tags": ["python", "spark"]},
        {"name": "Hank",    "age": 45, "city": "SF",  "dept": "management",  "salary": 190000, "tags": ["strategy"]},
        {"name": "Ivy",     "age": 27, "city": "LA",  "dept": "design",      "salary": 105000, "tags": ["figma", "css"]},
        {"name": "Jack",    "age": 33, "city": "NYC", "dept": "engineering", "salary": 142000, "tags": ["java", "spring"]},
    ]
    users.insert_many(docs)
    print(f"    Inserted {users.count_documents({})} documents into WiredTiger\n")

    for d in docs[:3]:
        print(f"    {d['name']:10s}  age={d['age']}  city={d['city']}  dept={d['dept']}  ${d['salary']:,}")
    print(f"    ... and {len(docs) - 3} more")

    # ── 3. Query planner ──────────────────────────────────────
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
        print(f"    {str(q):50s}  →  {plan['plan']:14s} ({tag})")

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
    banner(5, "UPDATE OPERATORS  ($set, $inc, $push, $unset)")

    users.update_one({"name": "Alice"}, {"$push": {"tags": "rust"}, "$inc": {"salary": 10000}})
    alice = users.find_one({"name": "Alice"})
    print(f"    Alice after update:  salary=${alice['salary']:,}  tags={alice['tags']}")

    users.update_many({"dept": "engineering"}, {"$inc": {"salary": 5000}})
    print(f"\n    All engineers got a $5k raise:")
    for doc in users.find({"dept": "engineering"}):
        print(f"      {doc['name']:10s}  ${doc['salary']:,}")

    # ── 6. Aggregation ────────────────────────────────────────
    banner(6, "AGGREGATION PIPELINE  (local, zero network)")

    print("    Average salary by department:")
    for r in users.aggregate([
        {"$group": {"_id": "$dept", "avg_salary": {"$avg": "$salary"}, "count": {"$sum": 1}}},
        {"$sort": {"avg_salary": -1}},
    ]):
        print(f"      {r['_id']:15s}  avg=${r['avg_salary']:,.0f}  ({r['count']} people)")

    print("\n    Tag popularity (unwind + group):")
    for r in users.aggregate([
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 6},
    ]):
        print(f"      {r['_id']:12s}  {r['count']}")

    print("\n    Top 3 earners:")
    for r in users.aggregate([
        {"$sort": {"salary": -1}},
        {"$limit": 3},
        {"$project": {"name": 1, "salary": 1, "dept": 1}},
    ]):
        print(f"      {r['name']:10s}  ${r['salary']:,}  ({r['dept']})")

    # ── 7. Oplog ──────────────────────────────────────────────
    banner(7, "OPLOG  (sync foundation)")

    oplog = users.get_oplog()
    ops = {}
    for e in oplog:
        ops[e["op"]] = ops.get(e["op"], 0) + 1

    print(f"    {len(oplog)} operations recorded in WiredTiger oplog:\n")
    for op, count in sorted(ops.items()):
        print(f"      {op:18s}  {count}")

    # ── Done ──────────────────────────────────────────────────
    print(f"\n{'─' * 60}")
    print("  One query language to rule them all.")
    print()
    print("  This entire demo ran against a local WiredTiger B-Tree.")
    print("  Change the URI to mongodb+srv:// and every line above")
    print("  runs against Atlas instead. Zero code changes.")
    print()
    print("  Start the web dashboard:  docker compose up --build")
    print("  Open:                     http://localhost:5000")
    print(f"{'─' * 60}\n")


if __name__ == "__main__":
    main()
