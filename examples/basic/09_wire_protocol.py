#!/usr/bin/env python3
"""
09_wire_protocol.py -- The real MongoDB wire protocol, locally.

Starts smongo's embedded wire server on a local port, then connects to
it with the *real* PyMongo driver over TCP.  PyMongo has no idea it's
talking to an embedded engine -- it sees a standard mongod speaking
OP_MSG with wire version 21.

The data is seeded via the native smongo client and then queried via
the standard PyMongo driver through the wire protocol.

Requires: pymongo  (pip install pymongo)

Run:
    python examples/basic/09_wire_protocol.py
"""

import os
import shutil
import sys
import tempfile
import time

from smongo import MongoClient as SmongoClient
from smongo import WireServer

PORT = 27018


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_wire_")

    _run(db_path)
    shutil.rmtree(db_path, ignore_errors=True)
    sys.stdout.flush()
    os._exit(0)


def _run(db_path: str) -> None:
    # ── Seed data via the smongo native client ─────────────────
    print("── seeding data via native smongo client ──")
    native = SmongoClient(f"local://{db_path}")
    employees = native["demo"]["employees"]
    employees.insert_many(
        [
            {"name": "Alice", "age": 34, "city": "NYC", "dept": "engineering", "salary": 145000},
            {"name": "Bob", "age": 28, "city": "SF", "dept": "engineering", "salary": 128000},
            {"name": "Charlie", "age": 40, "city": "NYC", "dept": "management", "salary": 175000},
            {"name": "Diana", "age": 25, "city": "LA", "dept": "design", "salary": 98000},
            {"name": "Eve", "age": 31, "city": "SF", "dept": "engineering", "salary": 155000},
            {"name": "Frank", "age": 36, "city": "CHI", "dept": "engineering", "salary": 140000},
        ]
    )
    employees.create_index([("city", 1)])
    employees.create_index([("dept", 1), ("salary", -1)])
    print(f"  {employees.count_documents({})} employees seeded with 2 indexes\n")

    # ── Start the embedded wire server ─────────────────────────
    print("── starting wire protocol server ──")
    print(f"  listening on localhost:{PORT}")
    print("  (Compass / mongosh can connect to mongodb://localhost:27018)\n")

    with WireServer(db_path, port=PORT, local_client=native.get_local_client()) as _srv:
        time.sleep(0.3)

        # ── Connect with the real PyMongo driver ───────────────
        from pymongo import MongoClient as PyMongoClient

        client = PyMongoClient(
            f"mongodb://localhost:{PORT}",
            serverSelectionTimeoutMS=5000,
            directConnection=True,
        )
        pymongo_coll = client["demo"]["employees"]

        # ── Count documents ────────────────────────────────────
        print("── PyMongo: count_documents ──")
        count = pymongo_coll.count_documents({})
        print(f"  {count} employees visible via wire protocol\n")

        # ── find with filter ───────────────────────────────────
        print("── PyMongo: find({city: 'NYC'}) ──")
        for doc in pymongo_coll.find({"city": "NYC"}):
            print(f"  {doc['name']:10s}  {doc['dept']:12s}  ${doc['salary']:,}")

        # ── find with sort + limit ─────────────────────────────
        print()
        print("── PyMongo: find().sort(salary, -1).limit(3) ──")
        for doc in pymongo_coll.find({}).sort("salary", -1).limit(3):
            print(f"  {doc['name']:10s}  ${doc['salary']:,}")

        # ── find_one ───────────────────────────────────────────
        print()
        print("── PyMongo: find_one({name: 'Eve'}) ──")
        doc = pymongo_coll.find_one({"name": "Eve"})
        print(f"  found: {doc['name']}, age {doc['age']}, {doc['city']}")

        client.close()

    native.close()
    print()
    print("Every query above went through the real MongoDB binary protocol (OP_MSG).")
    print(f"Connect Compass or mongosh to mongodb://localhost:{PORT} while the server runs.")


if __name__ == "__main__":
    main()
