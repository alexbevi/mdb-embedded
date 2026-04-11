#!/usr/bin/env python3
"""
06_compass_demo.py -- Start smongo and connect from anywhere.

Seeds a rich dataset (employees, departments, vector embeddings), creates
proper indexes (B-tree, unique, vector search), starts the wire protocol
server, runs self-demo queries to prove everything works, then keeps the
server running so you can explore from MongoDB Compass, mongosh, PyMongo,
or any MongoDB driver.

The server speaks the real MongoDB binary protocol (OP_MSG, wire v21).
Clients see a standard mongod -- smongo is completely invisible.

Run:
    python examples/ai_examples/06_compass_demo.py

Then connect from any of these:

    # MongoDB Compass (GUI)
    #   Connection string: mongodb://localhost:27018
    #   Click "Connect" -- browse databases, collections, run aggregations.

    # mongosh (shell)
    mongosh mongodb://localhost:27018

    # PyMongo (Python)
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27018", directConnection=True)

    # Node.js driver
    const { MongoClient } = require("mongodb");
    const client = new MongoClient("mongodb://localhost:27018", { directConnection: true });

    # Any tool that speaks the MongoDB wire protocol will work.
"""

import os
import signal
import sys
import tempfile
import time

import numpy as np

from smongo import MongoClient as SmongoClient
from smongo import WireServer

PORT = 27018
DB_PATH = os.path.join(tempfile.gettempdir(), "smongo_compass_demo")


def separator(char="─", width=64):
    return char * width


def seed_data(db_path: str) -> SmongoClient:
    """Seed a rich dataset for Compass exploration. Returns the open client."""
    native = SmongoClient(f"local://{db_path}")
    db = native["company"]

    # ── Employees ──────────────────────────────────────────────
    employees = db["employees"]
    employees.delete_many({})
    employees.insert_many(
        [
            {
                "name": "Alice Chen",
                "age": 34,
                "city": "New York",
                "dept": "engineering",
                "salary": 145000,
                "skills": ["Python", "Rust", "MongoDB"],
                "level": "senior",
                "joined": "2021-03-15",
            },
            {
                "name": "Bob Martinez",
                "age": 28,
                "city": "San Francisco",
                "dept": "engineering",
                "salary": 128000,
                "skills": ["JavaScript", "React", "Node.js"],
                "level": "mid",
                "joined": "2022-06-01",
            },
            {
                "name": "Charlie Park",
                "age": 40,
                "city": "New York",
                "dept": "management",
                "salary": 175000,
                "skills": ["Leadership", "Strategy"],
                "level": "director",
                "joined": "2019-01-10",
            },
            {
                "name": "Diana Okafor",
                "age": 25,
                "city": "Los Angeles",
                "dept": "design",
                "salary": 98000,
                "skills": ["Figma", "CSS", "User Research"],
                "level": "junior",
                "joined": "2023-09-20",
            },
            {
                "name": "Eve Johnson",
                "age": 31,
                "city": "San Francisco",
                "dept": "engineering",
                "salary": 155000,
                "skills": ["Python", "Machine Learning", "PyTorch"],
                "level": "senior",
                "joined": "2020-11-05",
            },
            {
                "name": "Frank Kim",
                "age": 36,
                "city": "Chicago",
                "dept": "engineering",
                "salary": 140000,
                "skills": ["Go", "Kubernetes", "gRPC"],
                "level": "senior",
                "joined": "2021-08-12",
            },
            {
                "name": "Grace Liu",
                "age": 29,
                "city": "New York",
                "dept": "data",
                "salary": 135000,
                "skills": ["Python", "SQL", "Spark"],
                "level": "mid",
                "joined": "2022-02-28",
            },
            {
                "name": "Hank Williams",
                "age": 45,
                "city": "San Francisco",
                "dept": "management",
                "salary": 190000,
                "skills": ["Leadership", "Finance", "M&A"],
                "level": "vp",
                "joined": "2018-04-01",
            },
            {
                "name": "Ivy Patel",
                "age": 27,
                "city": "Los Angeles",
                "dept": "design",
                "salary": 105000,
                "skills": ["Figma", "HTML", "Prototyping"],
                "level": "mid",
                "joined": "2022-07-15",
            },
            {
                "name": "Jack Torres",
                "age": 33,
                "city": "New York",
                "dept": "engineering",
                "salary": 142000,
                "skills": ["Java", "Spring", "PostgreSQL"],
                "level": "senior",
                "joined": "2021-01-20",
            },
            {
                "name": "Karen Singh",
                "age": 30,
                "city": "Chicago",
                "dept": "data",
                "salary": 130000,
                "skills": ["Python", "TensorFlow", "SQL"],
                "level": "mid",
                "joined": "2022-10-01",
            },
            {
                "name": "Leo Nakamura",
                "age": 38,
                "city": "San Francisco",
                "dept": "engineering",
                "salary": 165000,
                "skills": ["Rust", "C++", "Systems"],
                "level": "staff",
                "joined": "2019-06-15",
            },
        ]
    )
    employees.create_index([("dept", 1)])
    employees.create_index([("city", 1), ("salary", -1)])
    employees.create_index([("level", 1)])
    employees.create_index("name", unique=True)
    print(f"   employees    : {employees.count_documents({}):2d} docs, 4 indexes")

    # ── Departments ────────────────────────────────────────────
    departments = db["departments"]
    departments.delete_many({})
    departments.insert_many(
        [
            {
                "_id": "engineering",
                "label": "Engineering",
                "budget": 2_500_000,
                "head": "Charlie Park",
            },
            {
                "_id": "design",
                "label": "Design",
                "budget": 800_000,
                "head": "Ivy Patel",
            },
            {
                "_id": "data",
                "label": "Data Science",
                "budget": 1_200_000,
                "head": "Grace Liu",
            },
            {
                "_id": "management",
                "label": "Management",
                "budget": 500_000,
                "head": "Hank Williams",
            },
        ]
    )
    print(f"   departments  : {departments.count_documents({}):2d} docs")

    # ── Knowledge base with vector embeddings ─────────────────
    knowledge = db["knowledge_base"]
    knowledge.delete_many({})

    texts = [
        "smongo is an embedded MongoDB engine built on redb with a "
        "PyMongo-compatible API. No server, no Docker — import and go.",
        "The wire protocol server lets Compass, mongosh, and any MongoDB "
        "driver connect over TCP. Clients see a standard mongod.",
        "Vector search uses a vendored HNSW index for approximate "
        "nearest-neighbor search with cosine/euclidean/dotProduct scoring.",
        "ACID transactions use snapshot isolation via the MVCC storage "
        "layer across multiple collections.",
        "The aggregation pipeline supports 25+ stages including $lookup "
        "joins, $graphLookup, $facet, and $setWindowFields.",
        "Atlas sync pushes local writes to MongoDB Atlas and pulls remote "
        "changes with per-document vector clocks.",
        "The query planner auto-selects B-tree indexes with heuristic "
        "prefix scoring. Compound, unique, TTL, text, and wildcard "
        "indexes are supported.",
        "The Rust core eliminates ~50 Python method dispatches per "
        "command. Single-doc operations run ~2x faster than pymongo.",
    ]

    np.random.seed(42)
    dim = 64
    knowledge.insert_many(
        [
            {
                "text": t,
                "embedding": (
                    np.random.rand(dim).astype(np.float32) / np.linalg.norm(np.random.rand(dim))
                ).tolist(),
                "chunk_id": i,
                "topic": [
                    "engine",
                    "wire",
                    "vector",
                    "transactions",
                    "aggregation",
                    "sync",
                    "indexes",
                    "perf",
                ][i],
            }
            for i, t in enumerate(texts)
        ]
    )

    knowledge.create_index(
        {"embedding": "vectorSearch"},
        vectorSearchOptions={"dimensions": dim, "metric": "cosine"},
        name="default",
        type="vectorSearch",
    )
    print(
        f"   knowledge_base: {knowledge.count_documents({}):2d} docs, "
        f"{dim}-dim embeddings + vector index"
    )

    return native


def run_self_demo(port: int) -> None:
    """Run demo queries over the wire to prove everything works."""
    from pymongo import MongoClient as PyMongoClient

    client = PyMongoClient(
        f"mongodb://localhost:{port}",
        serverSelectionTimeoutMS=5000,
        directConnection=True,
    )
    db = client["company"]

    # ── Query 1: find + sort ──────────────────────────────────
    print(f"\n   {separator()}")
    print("   QUERY 1: Engineering team by salary (desc)")
    print(f"   {separator()}\n")

    t0 = time.time()
    engineers = list(
        db.employees.find(
            {"dept": "engineering"},
            {"name": 1, "salary": 1, "level": 1, "_id": 0},
        ).sort("salary", -1)
    )
    ms = (time.time() - t0) * 1000

    for emp in engineers:
        bar = "█" * (emp["salary"] // 10000)
        print(f"     {emp['name']:18s}  ${emp['salary']:>7,}  " f"{emp['level']:6s}  {bar}")
    print(f"     ({ms:.0f}ms, {len(engineers)} results)\n")

    # ── Query 2: aggregation — avg salary by dept ─────────────
    print(f"   {separator()}")
    print("   QUERY 2: Average salary by department")
    print(f"   {separator()}\n")

    t0 = time.time()
    pipeline = [
        {
            "$group": {
                "_id": "$dept",
                "avg_salary": {"$avg": "$salary"},
                "headcount": {"$sum": 1},
                "top_salary": {"$max": "$salary"},
            }
        },
        {"$sort": {"avg_salary": -1}},
    ]
    results = list(db.employees.aggregate(pipeline))
    ms = (time.time() - t0) * 1000

    print(f"     {'Dept':14s}  {'Avg':>10s}  {'Top':>10s}  {'HC':>3s}")
    print(f"     {'─'*14}  {'─'*10}  {'─'*10}  {'─'*3}")
    for r in results:
        print(
            f"     {r['_id']:14s}  ${r['avg_salary']:>9,.0f}  "
            f"${r['top_salary']:>9,}  {r['headcount']:3d}"
        )
    print(f"     ({ms:.0f}ms)\n")

    # ── Query 3: $lookup join ─────────────────────────────────
    print(f"   {separator()}")
    print("   QUERY 3: $lookup — Employees with department details")
    print(f"   {separator()}\n")

    t0 = time.time()
    joined = list(
        db.employees.aggregate(
            [
                {"$match": {"level": {"$in": ["staff", "vp", "director"]}}},
                {
                    "$lookup": {
                        "from": "departments",
                        "localField": "dept",
                        "foreignField": "_id",
                        "as": "dept_info",
                    }
                },
                {"$unwind": "$dept_info"},
                {
                    "$project": {
                        "name": 1,
                        "level": 1,
                        "department": "$dept_info.label",
                        "dept_budget": "$dept_info.budget",
                        "_id": 0,
                    }
                },
                {"$sort": {"dept_budget": -1}},
            ]
        )
    )
    ms = (time.time() - t0) * 1000

    for j in joined:
        print(
            f"     {j['name']:18s}  {j['level']:8s}  "
            f"{j['department']:14s}  ${j['dept_budget']:>10,}"
        )
    print(f"     ({ms:.0f}ms)\n")

    # ── Query 4: $vectorSearch ────────────────────────────────
    print(f"   {separator()}")
    print("   QUERY 4: $vectorSearch — semantic similarity")
    print(f"   {separator()}\n")

    np.random.seed(99)
    query_vec = np.random.rand(64).astype(np.float32)
    query_vec = (query_vec / np.linalg.norm(query_vec)).tolist()

    t0 = time.time()
    vec_results = list(
        db.knowledge_base.aggregate(
            [
                {
                    "$vectorSearch": {
                        "path": "embedding",
                        "queryVector": query_vec,
                        "limit": 4,
                        "numCandidates": 8,
                        "index": "default",
                    }
                },
                {"$set": {"score": {"$meta": "vectorSearchScore"}}},
                {
                    "$project": {
                        "text": 1,
                        "score": 1,
                        "topic": 1,
                        "_id": 0,
                    }
                },
            ]
        )
    )
    ms = (time.time() - t0) * 1000

    for rank, r in enumerate(vec_results, 1):
        score = r.get("score", 0)
        bar = "█" * int(score * 25)
        topic = r.get("topic", "?")
        snippet = r["text"][:60]
        print(f"     {rank}. [{score:.4f}] {bar}")
        print(f"        ({topic}) {snippet}...")
    print(f"     ({ms:.0f}ms)\n")

    # ── Query 5: $facet — multiple analytics in one pass ──────
    print(f"   {separator()}")
    print("   QUERY 5: $facet — city + level breakdowns in one pass")
    print(f"   {separator()}\n")

    t0 = time.time()
    facets = next(
        db.employees.aggregate(
            [
                {
                    "$facet": {
                        "by_city": [
                            {
                                "$group": {
                                    "_id": "$city",
                                    "count": {"$sum": 1},
                                }
                            },
                            {"$sort": {"count": -1}},
                        ],
                        "by_level": [
                            {
                                "$group": {
                                    "_id": "$level",
                                    "count": {"$sum": 1},
                                    "avg_salary": {"$avg": "$salary"},
                                }
                            },
                            {"$sort": {"avg_salary": -1}},
                        ],
                    }
                }
            ]
        )
    )
    ms = (time.time() - t0) * 1000

    print("     By City:")
    for r in facets["by_city"]:
        bar = "█" * r["count"]
        print(f"       {r['_id']:16s}  {r['count']:2d}  {bar}")

    print("\n     By Level:")
    for r in facets["by_level"]:
        bar = "█" * r["count"]
        print(f"       {r['_id']:12s}  {r['count']:2d}  " f"avg ${r['avg_salary']:>9,.0f}  {bar}")
    print(f"     ({ms:.0f}ms)\n")

    client.close()


def main() -> None:
    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║   smongo Compass Demo — local MongoDB, real tools           ║")
    print("║   Wire protocol v21 · OP_MSG · Fully compatible             ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")

    # ── 1. Seed data ───────────────────────────────────────────
    print("1. Seeding dataset...")
    os.makedirs(DB_PATH, exist_ok=True)
    native = seed_data(DB_PATH)

    # ── 2. Start the wire server ───────────────────────────────
    print(f"\n2. Starting wire protocol server on port {PORT}...")

    server = WireServer(DB_PATH, port=PORT, local_client=native.get_local_client())
    server.start()
    time.sleep(0.3)

    # ── 3. Self-demo queries ───────────────────────────────────
    print("\n3. Running self-demo queries over the wire...")

    run_self_demo(PORT)

    # ── Connection banner ─────────────────────────────────────
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║                                                              ║")
    print(f"║   Server running on mongodb://localhost:{PORT}               ║")
    print("║                                                              ║")
    print("║   COMPASS:                                                   ║")
    print(f"║     Connection string: mongodb://localhost:{PORT}            ║")
    print("║     Click Connect → browse databases, collections, indexes   ║")
    print("║                                                              ║")
    print("║   MONGOSH:                                                   ║")
    print(f"║     mongosh mongodb://localhost:{PORT}                       ║")
    print("║                                                              ║")
    print("║   PYMONGO:                                                   ║")
    print(f'║     MongoClient("mongodb://localhost:{PORT}")                ║')
    print("║                                                              ║")
    print("║   Press Ctrl+C to stop the server.                           ║")
    print("║                                                              ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")

    # ── Try-it-yourself hints ──────────────────────────────────
    print("Things to try:\n")
    print("  mongosh:")
    print(f"    mongosh mongodb://localhost:{PORT}")
    print("    use company")
    print("    db.employees.find({dept: 'engineering'}).sort({salary: -1})")
    print("    db.employees.aggregate([")
    print("      {$group: {_id: '$dept', avg: {$avg: '$salary'}, count: {$sum: 1}}},")
    print("      {$sort: {avg: -1}}")
    print("    ])")
    print("    db.knowledge_base.aggregate([{$vectorSearch: {")
    print("      path: 'embedding', queryVector: Array(64).fill(0.1),")
    print("      limit: 3, index: 'default'")
    print("    }}, {$set: {score: {$meta: 'vectorSearchScore'}}}])\n")

    print("  Compass aggregation builder:")
    print("    1. Open company.employees")
    print("    2. Click 'Aggregations' tab")
    print("    3. Add $group stage: { _id: '$city', count: { $sum: 1 } }")
    print("    4. Add $sort stage: { count: -1 }")
    print("    5. See results update live\n")

    print(separator("─"))
    print("  Waiting for connections... (Ctrl+C to stop)\n")

    # ── Keep running until Ctrl+C ──────────────────────────────
    def handle_shutdown(sig, frame):
        print("\n\nShutting down...")
        server.stop()
        native.close()
        print("Server stopped. Data preserved at:", DB_PATH)
        print("Restart anytime:  " "python examples/ai_examples/06_compass_demo.py\n")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handle_shutdown(None, None)


if __name__ == "__main__":
    main()
