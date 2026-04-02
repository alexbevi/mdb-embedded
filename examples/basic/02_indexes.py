#!/usr/bin/env python3
"""
02_indexes.py -- B-tree indexes and the query planner.

Shows how creating indexes changes query execution from collection scans
to index scans, how unique indexes enforce constraints, and how the
explain output reveals the query planner's decisions.

Run:
    python examples/basic/02_indexes.py
"""

import shutil
import tempfile

from smongo import DuplicateKeyError, MongoClient


EMPLOYEES = [
    {"name": "Alice",   "age": 34, "city": "NYC", "dept": "engineering", "salary": 145000},
    {"name": "Bob",     "age": 28, "city": "SF",  "dept": "engineering", "salary": 128000},
    {"name": "Charlie", "age": 40, "city": "NYC", "dept": "management",  "salary": 175000},
    {"name": "Diana",   "age": 25, "city": "LA",  "dept": "design",      "salary": 98000},
    {"name": "Eve",     "age": 31, "city": "SF",  "dept": "engineering", "salary": 155000},
    {"name": "Frank",   "age": 36, "city": "CHI", "dept": "engineering", "salary": 140000},
    {"name": "Grace",   "age": 29, "city": "NYC", "dept": "data",        "salary": 135000},
    {"name": "Hank",    "age": 45, "city": "SF",  "dept": "management",  "salary": 190000},
    {"name": "Ivy",     "age": 27, "city": "LA",  "dept": "design",      "salary": 105000},
    {"name": "Jack",    "age": 33, "city": "NYC", "dept": "engineering", "salary": 142000},
]


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_idx_")
    client = MongoClient(f"local://{db_path}")
    db = client["company"]
    emp = db["employees"]

    try:
        emp.insert_many(EMPLOYEES)
        _run(emp)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def _run(emp) -> None:
    # ── Before indexes: everything is a collection scan ───────
    print("── explain BEFORE indexes ──")
    for query in [{"age": {"$gt": 35}}, {"city": "NYC"}, {"salary": {"$gte": 150000}}]:
        plan = emp.explain(query)
        print(f"  {str(query):45s}  {plan['plan']}")

    # ── Create indexes ────────────────────────────────────────
    print()
    print("── create indexes ──")

    emp.create_index([("age", 1)])
    print("  created: age_1  (single ascending)")

    emp.create_index([("city", 1), ("age", -1)])
    print("  created: city_1_age_-1  (compound)")

    emp.create_index("name", unique=True)
    print("  created: name_1  (unique)")

    emp.create_index([("salary", -1)])
    print("  created: salary_-1  (descending)")

    # ── List all indexes ──────────────────────────────────────
    print()
    print("── list_indexes ──")
    for idx in emp.list_indexes():
        unique_flag = "  UNIQUE" if idx["unique"] else ""
        print(f"  {idx['name']:25s}  keys={idx['keys']}{unique_flag}")

    # ── After indexes: query planner picks index scans ────────
    print()
    print("── explain AFTER indexes ──")
    queries = [
        {"age": {"$gt": 35}},
        {"city": "NYC", "age": {"$lte": 35}},
        {"salary": {"$gte": 150000}},
        {"name": "Alice"},
    ]
    for query in queries:
        plan = emp.explain(query)
        tag = plan.get("index", plan["plan"].upper())
        print(f"  {str(query):45s}  {plan['plan']:14s}  ({tag})")

    # ── Indexed queries ───────────────────────────────────────
    print()
    print("── indexed query: city=NYC, sorted by age descending ──")
    for doc in emp.find({"city": "NYC"}).sort("age", -1):
        print(f"  {doc['name']:10s}  age={doc['age']}")

    print()
    print("── cursor chaining: skip(2).limit(3).sort(salary, -1) ──")
    for doc in emp.find({}).sort("salary", -1).skip(2).limit(3).projection({"name": 1, "salary": 1, "_id": 0}):
        print(f"  {doc['name']:10s}  ${doc['salary']:,}")

    # ── Unique index constraint ───────────────────────────────
    print()
    print("── unique index: duplicate name ──")
    try:
        emp.insert_one({"name": "Alice", "age": 99, "city": "BOS", "dept": "test", "salary": 0})
        print("  ERROR: should have raised DuplicateKeyError")
    except DuplicateKeyError as exc:
        print(f"  Caught DuplicateKeyError: {exc}")

    print()


if __name__ == "__main__":
    main()
