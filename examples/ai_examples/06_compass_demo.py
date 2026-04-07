#!/usr/bin/env python3
"""
06_compass_demo.py -- Start smongo and connect from anywhere.

Seeds a rich dataset (employees, departments, vector embeddings), starts
the wire protocol server, and keeps it running so you can explore the
data from MongoDB Compass, mongosh, PyMongo, or any MongoDB driver.

The server speaks the real MongoDB binary protocol (OP_MSG, wire v21).
Clients see a standard mongod -- smongo is completely invisible.

Run:
    python examples/ai_examples/06_compass_demo.py

Then connect from any of these:

    # MongoDB Compass (GUI)
    #   Connection string: mongodb://localhost:27017
    #   Click "Connect" -- browse databases, collections, run aggregations.

    # mongosh (shell)
    mongosh mongodb://localhost:27017

    # PyMongo (Python)
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017", directConnection=True)

    # Node.js driver
    const { MongoClient } = require("mongodb");
    const client = new MongoClient("mongodb://localhost:27017", { directConnection: true });

    # Rust driver
    let client = Client::with_uri_str("mongodb://localhost:27017").await?;

    # Go driver
    client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))

    # Java driver
    MongoClient client = MongoClients.create("mongodb://localhost:27017");

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

PORT = 27017
DB_PATH = os.path.join(tempfile.gettempdir(), "smongo_compass_demo")


def seed_data(db_path: str) -> None:
    """Seed a rich dataset for Compass exploration."""
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
            },
            {
                "name": "Bob Martinez",
                "age": 28,
                "city": "San Francisco",
                "dept": "engineering",
                "salary": 128000,
                "skills": ["JavaScript", "React", "Node.js"],
                "level": "mid",
            },
            {
                "name": "Charlie Park",
                "age": 40,
                "city": "New York",
                "dept": "management",
                "salary": 175000,
                "skills": ["Leadership", "Strategy"],
                "level": "director",
            },
            {
                "name": "Diana Okafor",
                "age": 25,
                "city": "Los Angeles",
                "dept": "design",
                "salary": 98000,
                "skills": ["Figma", "CSS", "User Research"],
                "level": "junior",
            },
            {
                "name": "Eve Johnson",
                "age": 31,
                "city": "San Francisco",
                "dept": "engineering",
                "salary": 155000,
                "skills": ["Python", "Machine Learning", "PyTorch"],
                "level": "senior",
            },
            {
                "name": "Frank Kim",
                "age": 36,
                "city": "Chicago",
                "dept": "engineering",
                "salary": 140000,
                "skills": ["Go", "Kubernetes", "gRPC"],
                "level": "senior",
            },
            {
                "name": "Grace Liu",
                "age": 29,
                "city": "New York",
                "dept": "data",
                "salary": 135000,
                "skills": ["Python", "SQL", "Spark"],
                "level": "mid",
            },
            {
                "name": "Hank Williams",
                "age": 45,
                "city": "San Francisco",
                "dept": "management",
                "salary": 190000,
                "skills": ["Leadership", "Finance", "M&A"],
                "level": "vp",
            },
            {
                "name": "Ivy Patel",
                "age": 27,
                "city": "Los Angeles",
                "dept": "design",
                "salary": 105000,
                "skills": ["Figma", "HTML", "Prototyping"],
                "level": "mid",
            },
            {
                "name": "Jack Torres",
                "age": 33,
                "city": "New York",
                "dept": "engineering",
                "salary": 142000,
                "skills": ["Java", "Spring", "PostgreSQL"],
                "level": "senior",
            },
            {
                "name": "Karen Singh",
                "age": 30,
                "city": "Chicago",
                "dept": "data",
                "salary": 130000,
                "skills": ["Python", "TensorFlow", "SQL"],
                "level": "mid",
            },
            {
                "name": "Leo Nakamura",
                "age": 38,
                "city": "San Francisco",
                "dept": "engineering",
                "salary": 165000,
                "skills": ["Rust", "C++", "Systems"],
                "level": "staff",
            },
        ]
    )
    employees.create_index([("dept", 1)])
    employees.create_index([("city", 1), ("salary", -1)])
    employees.create_index([("level", 1)])
    employees.create_index("name", unique=True)
    print(f"   employees: {employees.count_documents({})} docs, 4 indexes")

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
            {"_id": "design", "label": "Design", "budget": 800_000, "head": "Ivy Patel"},
            {"_id": "data", "label": "Data Science", "budget": 1_200_000, "head": "Grace Liu"},
            {
                "_id": "management",
                "label": "Management",
                "budget": 500_000,
                "head": "Hank Williams",
            },
        ]
    )
    print(f"   departments: {departments.count_documents({})} docs")

    # ── Knowledge base with vector embeddings ──────────────────
    # (so you can run $vectorSearch from Compass's aggregation builder)
    knowledge = db["knowledge_base"]
    knowledge.delete_many({})

    texts = [
        "smongo is an embedded MongoDB engine built on WiredTiger B-trees.",
        "The wire protocol lets Compass, mongosh, and any driver connect.",
        "Vector search runs cosine similarity in-memory with NumPy.",
        "ACID transactions use snapshot isolation across collections.",
        "The aggregation pipeline supports 25+ stages including $lookup.",
        "Atlas sync pushes writes to the cloud with conflict resolution.",
    ]

    np.random.seed(42)
    knowledge.insert_many(
        [
            {
                "text": t,
                "embedding": (np.random.rand(32).astype(np.float32) / 3).tolist(),
                "chunk_id": i,
            }
            for i, t in enumerate(texts)
        ]
    )
    print(f"   knowledge_base: {knowledge.count_documents({})} docs with 32-dim embeddings")

    native.close()


def main() -> None:
    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║   smongo Compass Demo -- local MongoDB, real tools          ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")

    # ── 1. Seed data ───────────────────────────────────────────
    print("1. Seeding dataset...")
    os.makedirs(DB_PATH, exist_ok=True)
    seed_data(DB_PATH)

    # ── 2. Start the wire server ───────────────────────────────
    print(f"\n2. Starting wire protocol server on port {PORT}...\n")

    server = WireServer(DB_PATH, port=PORT)
    server.start()
    time.sleep(0.3)

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║                                                              ║")
    print(f"║   Server running on mongodb://localhost:{PORT}               ║")
    print("║                                                              ║")
    print("║   Connect with any of these:                                 ║")
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
    print("      path: 'embedding', queryVector: Array(32).fill(0.1),")
    print("      limit: 3, metric: 'cosine'")
    print("    }}])\n")

    print("  Compass aggregation builder:")
    print("    1. Open company.employees")
    print("    2. Click 'Aggregations' tab")
    print("    3. Add $group stage: { _id: '$city', count: { $sum: 1 } }")
    print("    4. Add $sort stage: { count: -1 }")
    print("    5. See results update live\n")

    print("  Any MongoDB driver in any language:")
    print(f"    Just connect to mongodb://localhost:{PORT}")
    print("    The full MQL query language and aggregation framework work.\n")

    print("─" * 64)
    print("  Waiting for connections... (Ctrl+C to stop)\n")

    # ── 3. Keep running until Ctrl+C ───────────────────────────
    def handle_shutdown(sig, frame):
        print("\n\nShutting down...")
        server.stop()
        print("Server stopped. Data preserved at:", DB_PATH)
        print("Restart anytime:  python examples/ai_examples/06_compass_demo.py\n")
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
