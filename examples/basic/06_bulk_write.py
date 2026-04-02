#!/usr/bin/env python3
"""
06_bulk_write.py -- Batch operations in a single call.

Demonstrates bulk_write with InsertOne, UpdateOne, UpdateMany, DeleteOne,
DeleteMany, and ReplaceOne. All operations execute in sequence (or
unordered) and return aggregate counts.

Run:
    python examples/basic/06_bulk_write.py
"""

import shutil
import tempfile

from smongo import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    MongoClient,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_bulk_")
    client = MongoClient(f"local://{db_path}")
    db = client["warehouse"]
    products = db["products"]

    try:
        _run(products)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def _run(products) -> None:
    # ── Seed some products ────────────────────────────────────
    print("── seeding products ──")
    products.insert_many(
        [
            {
                "_id": "SKU001",
                "name": "Widget A",
                "price": 9.99,
                "stock": 100,
                "category": "widgets",
            },
            {
                "_id": "SKU002",
                "name": "Widget B",
                "price": 14.99,
                "stock": 50,
                "category": "widgets",
            },
            {
                "_id": "SKU003",
                "name": "Gadget X",
                "price": 29.99,
                "stock": 30,
                "category": "gadgets",
            },
            {
                "_id": "SKU004",
                "name": "Gadget Y",
                "price": 49.99,
                "stock": 10,
                "category": "gadgets",
            },
            {"_id": "SKU005", "name": "Doohickey", "price": 4.99, "stock": 200, "category": "misc"},
        ]
    )
    print(f"  {products.count_documents({})} products in inventory\n")

    # ── bulk_write: mixed operations ──────────────────────────
    print("── bulk_write: 6 operations in one call ──")
    result = products.bulk_write(
        [
            InsertOne(
                {
                    "_id": "SKU006",
                    "name": "Thingamajig",
                    "price": 7.50,
                    "stock": 75,
                    "category": "misc",
                }
            ),
            UpdateOne({"_id": "SKU001"}, {"$inc": {"stock": -20}, "$set": {"on_sale": True}}),
            UpdateMany({"category": "gadgets"}, {"$inc": {"price": -5.0}}),
            ReplaceOne(
                {"_id": "SKU005"},
                {
                    "_id": "SKU005",
                    "name": "Super Doohickey",
                    "price": 6.99,
                    "stock": 180,
                    "category": "misc",
                    "upgraded": True,
                },
            ),
            DeleteOne({"_id": "SKU002"}),
            DeleteMany({"stock": {"$lte": 10}}),
        ]
    )

    print(f"  inserted:  {result.inserted_count}")
    print(f"  matched:   {result.matched_count}")
    print(f"  modified:  {result.modified_count}")
    print(f"  deleted:   {result.deleted_count}")

    # ── Show final state ──────────────────────────────────────
    print()
    print("── inventory after bulk_write ──")
    for doc in products.find({}).sort("_id", 1):
        flags = []
        if doc.get("on_sale"):
            flags.append("SALE")
        if doc.get("upgraded"):
            flags.append("UPGRADED")
        flag_str = f"  [{', '.join(flags)}]" if flags else ""
        print(
            f"  {doc['_id']}  {doc['name']:20s}  ${doc['price']:>6.2f}  stock={doc['stock']:>3}{flag_str}"
        )

    print()


if __name__ == "__main__":
    main()
