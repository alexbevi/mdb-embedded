#!/usr/bin/env python3
"""
01_crud.py -- First contact with smongo.

Demonstrates the core document lifecycle: insert, find, update, delete.
Uses a small book-shop dataset so the output is easy to follow.

Run:
    python examples/basic/01_crud.py
"""

import shutil
import tempfile

from smongo import MongoClient


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_crud_")
    client = MongoClient(f"local://{db_path}")
    db = client["bookshop"]
    books = db["books"]

    try:
        _run(books)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def _run(books) -> None:
    # ── Insert ────────────────────────────────────────────────
    print("── insert_one / insert_many ──")

    books.insert_one(
        {
            "title": "The Pragmatic Programmer",
            "author": "Hunt & Thomas",
            "year": 1999,
            "price": 42.0,
            "tags": ["software"],
        }
    )

    books.insert_many(
        [
            {
                "title": "Designing Data-Intensive Applications",
                "author": "Martin Kleppmann",
                "year": 2017,
                "price": 38.0,
                "tags": ["databases", "distributed"],
            },
            {
                "title": "Clean Code",
                "author": "Robert C. Martin",
                "year": 2008,
                "price": 35.0,
                "tags": ["software"],
            },
            {
                "title": "MongoDB: The Definitive Guide",
                "author": "Shannon Bradshaw",
                "year": 2019,
                "price": 45.0,
                "tags": ["databases", "mongodb"],
            },
            {
                "title": "Python Crash Course",
                "author": "Eric Matthes",
                "year": 2019,
                "price": 30.0,
                "tags": ["python"],
            },
        ]
    )

    print(f"  {books.count_documents({})} books in the collection\n")

    # ── Find ──────────────────────────────────────────────────
    print("── find_one ──")
    doc = books.find_one({"author": "Martin Kleppmann"})
    print(f"  {doc['title']} ({doc['year']})\n")

    print("── find with filter ──")
    for doc in books.find({"year": {"$gte": 2017}}):
        print(f"  {doc['title']:45s}  {doc['year']}")

    print()
    print("── cursor chaining: sort + limit + projection ──")
    for doc in (
        books.find({}).sort("price", -1).limit(3).projection({"title": 1, "price": 1, "_id": 0})
    ):
        print(f"  ${doc['price']:.0f}  {doc['title']}")

    # ── Update ────────────────────────────────────────────────
    print()
    print("── update_one ($set, $inc, $push) ──")

    books.update_one(
        {"title": "Clean Code"},
        {"$set": {"edition": 2}, "$inc": {"price": 5.0}, "$push": {"tags": "classic"}},
    )
    updated = books.find_one({"title": "Clean Code"})
    print(
        f"  Clean Code: price=${updated['price']:.0f}, edition={updated['edition']}, tags={updated['tags']}"
    )

    print()
    print("── update_many ──")
    result = books.update_many({"year": {"$lt": 2010}}, {"$set": {"vintage": True}})
    print(f"  Marked {result.modified_count} pre-2010 books as vintage")

    print()
    print("── find_one_and_update (atomic) ──")
    before = books.find_one_and_update(
        {"title": "Python Crash Course"},
        {"$inc": {"price": -5.0}},
        return_document="after",
    )
    print(f"  Python Crash Course new price: ${before['price']:.0f}")

    # ── Delete ────────────────────────────────────────────────
    print()
    print("── delete_one / delete_many ──")

    books.delete_one({"title": "Python Crash Course"})
    print(f"  After delete_one: {books.count_documents({})} books")

    result = books.delete_many({"vintage": True})
    print(f"  Deleted {result.deleted_count} vintage books, {books.count_documents({})} remain")

    # ── Final state ───────────────────────────────────────────
    print()
    print("── remaining books ──")
    for doc in books.find({}).sort("title", 1):
        print(f"  {doc['title']}")

    print()


if __name__ == "__main__":
    main()
