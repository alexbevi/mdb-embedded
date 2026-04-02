#!/usr/bin/env python3
"""
05_schema_validation.py -- $jsonSchema enforcement at the edge.

Shows how smongo validates documents on insert and update using MongoDB's
$jsonSchema syntax. Invalid documents are rejected before they touch
WiredTiger, so the data on disk is always clean.

Run:
    python examples/basic/05_schema_validation.py
"""

import shutil
import tempfile

from smongo import MongoClient, ValidationError


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_schema_")
    client = MongoClient(f"local://{db_path}")
    db = client["app"]

    try:
        _run(db)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def _run(db) -> None:
    # ── Create a collection with a strict schema ──────────────
    print("── creating 'users' with $jsonSchema validator ──")
    users = db.create_collection("users", validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["name", "email", "age"],
            "properties": {
                "name": {
                    "bsonType": "string",
                    "minLength": 1,
                    "maxLength": 100,
                },
                "email": {
                    "bsonType": "string",
                    "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
                },
                "age": {
                    "bsonType": "int",
                    "minimum": 0,
                    "maximum": 150,
                },
                "role": {
                    "enum": ["admin", "editor", "viewer"],
                },
            },
            "additionalProperties": False,
        }
    })
    print("  schema requires: name (string), email (valid pattern), age (0-150)")
    print("  allowed roles: admin, editor, viewer")
    print("  additional properties: forbidden\n")

    # ── Valid inserts ─────────────────────────────────────────
    print("── inserting valid documents ──")
    users.insert_one({"name": "Alice", "email": "alice@example.com", "age": 34, "role": "admin"})
    users.insert_one({"name": "Bob", "email": "bob@company.io", "age": 28, "role": "editor"})
    users.insert_one({"name": "Charlie", "email": "charlie@dev.org", "age": 42})
    print(f"  inserted 3 valid users\n")

    # ── Invalid: missing required field ───────────────────────
    print("── rejection: missing required field 'email' ──")
    try:
        users.insert_one({"name": "Diana", "age": 25})
    except ValidationError as e:
        print(f"  REJECTED: {e}\n")

    # ── Invalid: email doesn't match pattern ──────────────────
    print("── rejection: invalid email format ──")
    try:
        users.insert_one({"name": "Eve", "email": "not-an-email", "age": 30})
    except ValidationError as e:
        print(f"  REJECTED: {e}\n")

    # ── Invalid: age out of range ─────────────────────────────
    print("── rejection: age out of range ──")
    try:
        users.insert_one({"name": "Frank", "email": "frank@test.com", "age": -5})
    except ValidationError as e:
        print(f"  REJECTED: {e}\n")

    # ── Invalid: unknown field (additionalProperties: false) ──
    print("── rejection: unexpected field 'phone' ──")
    try:
        users.insert_one({"name": "Grace", "email": "grace@test.com", "age": 29, "phone": "555-0123"})
    except ValidationError as e:
        print(f"  REJECTED: {e}\n")

    # ── Invalid: bad enum value ───────────────────────────────
    print("── rejection: role not in enum ──")
    try:
        users.insert_one({"name": "Hank", "email": "hank@test.com", "age": 45, "role": "superadmin"})
    except ValidationError as e:
        print(f"  REJECTED: {e}\n")

    # ── Updates are also validated ────────────────────────────
    print("── rejection on update: setting age to string ──")
    try:
        users.update_one({"name": "Alice"}, {"$set": {"age": "thirty-four"}})
    except ValidationError as e:
        print(f"  REJECTED: {e}\n")

    # ── Collection is unchanged -- only valid data on disk ────
    print("── final state (only valid docs made it through) ──")
    for doc in users.find({}).sort("name", 1):
        role = doc.get("role", "(none)")
        print(f"  {doc['name']:10s}  {doc['email']:25s}  age={doc['age']}  role={role}")

    print()


if __name__ == "__main__":
    main()
