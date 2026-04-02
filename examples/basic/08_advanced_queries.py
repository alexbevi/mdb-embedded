#!/usr/bin/env python3
"""
08_advanced_queries.py -- The full power of MQL in smongo.

Demonstrates complex query patterns: $or/$and/$nor, $regex, $elemMatch,
$in/$nin, $exists, $type, dot-notation into nested documents and arrays,
$not, and combining operators with indexes.

Run:
    python examples/basic/08_advanced_queries.py
"""

import shutil
import tempfile

from smongo import MongoClient

PEOPLE = [
    {
        "name": "Alice Chen",
        "age": 34,
        "email": "alice@example.com",
        "address": {"city": "New York", "state": "NY", "zip": "10001"},
        "skills": ["python", "go", "kubernetes"],
        "projects": [
            {"name": "Atlas", "role": "lead", "months": 18},
            {"name": "Beacon", "role": "contributor", "months": 6},
        ],
        "active": True,
    },
    {
        "name": "Bob Martinez",
        "age": 28,
        "email": "bob@company.io",
        "address": {"city": "San Francisco", "state": "CA", "zip": "94105"},
        "skills": ["javascript", "react", "node"],
        "projects": [
            {"name": "Dashboard", "role": "lead", "months": 12},
        ],
        "active": True,
    },
    {
        "name": "Charlie Kim",
        "age": 45,
        "email": "charlie.kim@org.net",
        "address": {"city": "Chicago", "state": "IL", "zip": "60601"},
        "skills": ["java", "python", "sql"],
        "projects": [
            {"name": "Atlas", "role": "contributor", "months": 8},
            {"name": "Core", "role": "lead", "months": 24},
        ],
        "active": False,
        "notes": "On sabbatical until Q3",
    },
    {
        "name": "Diana Okafor",
        "age": 31,
        "email": "diana@startup.co",
        "address": {"city": "Austin", "state": "TX", "zip": "73301"},
        "skills": ["rust", "python", "wasm"],
        "projects": [],
        "active": True,
    },
    {
        "name": "Eve Petrov",
        "age": 38,
        "email": "eve.petrov@research.edu",
        "address": {"city": "Boston", "state": "MA", "zip": "02101"},
        "skills": ["python", "r", "tensorflow"],
        "projects": [
            {"name": "ML Pipeline", "role": "lead", "months": 15},
            {"name": "Atlas", "role": "advisor", "months": 3},
        ],
        "active": True,
        "notes": "Part-time, also teaches at MIT",
    },
]


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_queries_")
    client = MongoClient(f"local://{db_path}")
    db = client["hr"]
    people = db["people"]

    try:
        people.insert_many(PEOPLE)
        people.create_index([("age", 1)])
        people.create_index([("address.state", 1)])
        people.create_index([("skills", 1)])
        _run(people)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def _show(label: str, cursor) -> None:
    results = list(cursor)
    print(f"\n── {label} ({len(results)} results) ──")
    for d in results:
        print(f"  {d['name']:20s}  age={d['age']}  state={d['address']['state']}")


def _run(people) -> None:
    # ── $or: NY or CA residents ───────────────────────────────
    _show(
        "$or: lives in NY or CA",
        people.find({"$or": [{"address.state": "NY"}, {"address.state": "CA"}]}),
    )

    # ── $and + range: active and age 30-40 ────────────────────
    _show(
        "$and: active AND age 30-40",
        people.find({"$and": [{"active": True}, {"age": {"$gte": 30, "$lte": 40}}]}),
    )

    # ── $nor: neither inactive nor from TX ────────────────────
    _show(
        "$nor: not inactive, not from TX",
        people.find({"$nor": [{"active": False}, {"address.state": "TX"}]}),
    )

    # ── $regex: email from .edu domains ───────────────────────
    print("\n── $regex: .edu email addresses ──")
    for d in people.find({"email": {"$regex": r"\.edu$"}}):
        print(f"  {d['name']:20s}  {d['email']}")

    # ── $regex with $options (case-insensitive) ───────────────
    print("\n── $regex (case-insensitive): name contains 'kim' ──")
    for d in people.find({"name": {"$regex": "kim", "$options": "i"}}):
        print(f"  {d['name']}")

    # ── $in: state is NY, CA, or TX ──────────────────────────
    _show(
        "$in: lives in NY, CA, or TX",
        people.find({"address.state": {"$in": ["NY", "CA", "TX"]}}),
    )

    # ── $nin: state is NOT IL or MA ───────────────────────────
    _show(
        "$nin: doesn't live in IL or MA",
        people.find({"address.state": {"$nin": ["IL", "MA"]}}),
    )

    # ── $all: knows BOTH python and go ────────────────────────
    print("\n── $all: knows both python AND go ──")
    for d in people.find({"skills": {"$all": ["python", "go"]}}):
        print(f"  {d['name']:20s}  skills={d['skills']}")

    # ── $size: exactly 3 skills ───────────────────────────────
    print("\n── $size: exactly 3 skills ──")
    for d in people.find({"skills": {"$size": 3}}):
        print(f"  {d['name']:20s}  skills={d['skills']}")

    # ── $elemMatch: has a project where they were lead for 12+ months ──
    print("\n── $elemMatch: led a project for 12+ months ──")
    for d in people.find({"projects": {"$elemMatch": {"role": "lead", "months": {"$gte": 12}}}}):
        leads = [p for p in d["projects"] if p["role"] == "lead" and p["months"] >= 12]
        for p in leads:
            print(f"  {d['name']:20s}  project={p['name']}  months={p['months']}")

    # ── $exists: has a 'notes' field ──────────────────────────
    print("\n── $exists: has 'notes' field ──")
    for d in people.find({"notes": {"$exists": True}}):
        print(f"  {d['name']:20s}  notes={d['notes']}")

    # ── $not: age is NOT greater than 35 ──────────────────────
    _show(
        "$not: age is NOT > 35",
        people.find({"age": {"$not": {"$gt": 35}}}),
    )

    # ── Dot-notation: nested field query ──────────────────────
    print("\n── dot-notation: address.city = 'Chicago' ──")
    for d in people.find({"address.city": "Chicago"}):
        print(f"  {d['name']:20s}  {d['address']['city']}, {d['address']['state']}")

    # ── Combining operators: complex compound query ─────────
    print("\n── complex: active, age>30, led a project for 10+ months ──")
    for d in people.find(
        {
            "active": True,
            "age": {"$gt": 30},
            "projects": {"$elemMatch": {"role": "lead", "months": {"$gte": 10}}},
        }
    ):
        lead_projects = [
            p["name"] for p in d["projects"] if p["role"] == "lead" and p["months"] >= 10
        ]
        print(f"  {d['name']:20s}  age={d['age']}  leads={lead_projects}")

    # ── Dot-notation into nested docs ─────────────────────────
    print("\n── dot-notation: address.zip starts with '1' (NYC area) ──")
    for d in people.find({"address.zip": {"$regex": "^1"}}):
        print(
            f"  {d['name']:20s}  {d['address']['city']}, {d['address']['state']} {d['address']['zip']}"
        )

    print()


if __name__ == "__main__":
    main()
