#!/usr/bin/env python3
"""
ecommerce.py -- E-commerce data model with smongo.

Models a small online store: products, customers, and orders. Then runs
analytics pipelines you'd actually use: revenue by category, customer
lifetime value, top sellers, and a product recommendation facet.

Run:
    python examples/patterns/ecommerce.py
"""

import shutil
import tempfile
from datetime import UTC, datetime, timedelta

from smongo import MongoClient


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_ecom_")
    client = MongoClient(f"local://{db_path}")
    db = client["shopify_lite"]

    try:
        seed(db)
        analytics(db)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def seed(db) -> None:
    products = db["products"]
    customers = db["customers"]
    orders = db["orders"]

    print("── seeding store data ──")

    products.insert_many(
        [
            {
                "_id": "P1",
                "name": "Wireless Earbuds",
                "category": "electronics",
                "price": 79.99,
                "rating": 4.5,
                "stock": 200,
            },
            {
                "_id": "P2",
                "name": "USB-C Hub",
                "category": "electronics",
                "price": 45.00,
                "rating": 4.2,
                "stock": 150,
            },
            {
                "_id": "P3",
                "name": "Standing Desk",
                "category": "furniture",
                "price": 499.00,
                "rating": 4.8,
                "stock": 30,
            },
            {
                "_id": "P4",
                "name": "Mechanical Keyboard",
                "category": "electronics",
                "price": 129.00,
                "rating": 4.7,
                "stock": 80,
            },
            {
                "_id": "P5",
                "name": "Desk Lamp",
                "category": "furniture",
                "price": 39.99,
                "rating": 4.0,
                "stock": 120,
            },
            {
                "_id": "P6",
                "name": "Notebook (3-pack)",
                "category": "office",
                "price": 12.99,
                "rating": 4.3,
                "stock": 500,
            },
            {
                "_id": "P7",
                "name": "Ergonomic Mouse",
                "category": "electronics",
                "price": 69.00,
                "rating": 4.4,
                "stock": 90,
            },
            {
                "_id": "P8",
                "name": "Monitor Arm",
                "category": "furniture",
                "price": 89.00,
                "rating": 4.6,
                "stock": 60,
            },
        ]
    )

    customers.insert_many(
        [
            {
                "_id": "C1",
                "name": "Alice",
                "email": "alice@mail.com",
                "tier": "gold",
                "joined": "2024-01-15",
            },
            {
                "_id": "C2",
                "name": "Bob",
                "email": "bob@mail.com",
                "tier": "silver",
                "joined": "2024-06-01",
            },
            {
                "_id": "C3",
                "name": "Charlie",
                "email": "charlie@mail.com",
                "tier": "gold",
                "joined": "2023-11-20",
            },
            {
                "_id": "C4",
                "name": "Diana",
                "email": "diana@mail.com",
                "tier": "bronze",
                "joined": "2025-02-10",
            },
        ]
    )

    now = datetime.now(UTC)
    orders.insert_many(
        [
            {
                "customer": "C1",
                "items": [
                    {"product": "P1", "qty": 1, "price": 79.99},
                    {"product": "P6", "qty": 2, "price": 12.99},
                ],
                "total": 105.97,
                "status": "delivered",
                "date": (now - timedelta(days=30)).isoformat(),
            },
            {
                "customer": "C1",
                "items": [{"product": "P3", "qty": 1, "price": 499.00}],
                "total": 499.00,
                "status": "delivered",
                "date": (now - timedelta(days=10)).isoformat(),
            },
            {
                "customer": "C2",
                "items": [
                    {"product": "P4", "qty": 1, "price": 129.00},
                    {"product": "P2", "qty": 1, "price": 45.00},
                ],
                "total": 174.00,
                "status": "shipped",
                "date": (now - timedelta(days=3)).isoformat(),
            },
            {
                "customer": "C3",
                "items": [
                    {"product": "P1", "qty": 2, "price": 79.99},
                    {"product": "P7", "qty": 1, "price": 69.00},
                ],
                "total": 228.98,
                "status": "delivered",
                "date": (now - timedelta(days=45)).isoformat(),
            },
            {
                "customer": "C3",
                "items": [{"product": "P4", "qty": 1, "price": 129.00}],
                "total": 129.00,
                "status": "delivered",
                "date": (now - timedelta(days=20)).isoformat(),
            },
            {
                "customer": "C3",
                "items": [
                    {"product": "P5", "qty": 1, "price": 39.99},
                    {"product": "P8", "qty": 1, "price": 89.00},
                ],
                "total": 128.99,
                "status": "pending",
                "date": now.isoformat(),
            },
            {
                "customer": "C4",
                "items": [{"product": "P6", "qty": 5, "price": 12.99}],
                "total": 64.95,
                "status": "delivered",
                "date": (now - timedelta(days=7)).isoformat(),
            },
        ]
    )

    products.create_index([("category", 1)])
    products.create_index([("rating", -1)])
    orders.create_index([("customer", 1)])
    orders.create_index([("status", 1)])

    print(
        f"  {products.count_documents({})} products, {customers.count_documents({})} customers, {orders.count_documents({})} orders\n"
    )


def analytics(db) -> None:
    orders = db["orders"]

    # ── Revenue by category ───────────────────────────────────
    print("── revenue by category ──")
    results = orders.aggregate(
        [
            {"$unwind": "$items"},
            {
                "$lookup": {
                    "from": "products",
                    "localField": "items.product",
                    "foreignField": "_id",
                    "as": "product_info",
                }
            },
            {"$unwind": "$product_info"},
            {
                "$group": {
                    "_id": "$product_info.category",
                    "revenue": {"$sum": {"$multiply": ["$items.qty", "$items.price"]}},
                    "units_sold": {"$sum": "$items.qty"},
                }
            },
            {"$sort": {"revenue": -1}},
        ]
    )
    for r in results:
        print(f"  {r['_id']:15s}  revenue=${r['revenue']:>8,.2f}  units={r['units_sold']}")

    # ── Customer lifetime value ───────────────────────────────
    print("\n── customer lifetime value (total spend) ──")
    results = orders.aggregate(
        [
            {
                "$group": {
                    "_id": "$customer",
                    "total_spend": {"$sum": "$total"},
                    "order_count": {"$sum": 1},
                    "avg_order": {"$avg": "$total"},
                }
            },
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "info",
                }
            },
            {"$unwind": "$info"},
            {
                "$project": {
                    "name": "$info.name",
                    "tier": "$info.tier",
                    "total_spend": 1,
                    "order_count": 1,
                    "avg_order": {"$round": ["$avg_order", 2]},
                }
            },
            {"$sort": {"total_spend": -1}},
        ]
    )
    for r in results:
        print(
            f"  {r['name']:10s}  [{r['tier']:6s}]  spend=${r['total_spend']:>8,.2f}  "
            f"orders={r['order_count']}  avg=${r['avg_order']:,.2f}"
        )

    # ── Top selling products ──────────────────────────────────
    print("\n── top 5 products by units sold ──")
    results = orders.aggregate(
        [
            {"$unwind": "$items"},
            {
                "$group": {
                    "_id": "$items.product",
                    "total_qty": {"$sum": "$items.qty"},
                    "total_revenue": {"$sum": {"$multiply": ["$items.qty", "$items.price"]}},
                }
            },
            {
                "$lookup": {
                    "from": "products",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "product",
                }
            },
            {"$unwind": "$product"},
            {
                "$project": {
                    "name": "$product.name",
                    "total_qty": 1,
                    "total_revenue": 1,
                }
            },
            {"$sort": {"total_qty": -1}},
            {"$limit": 5},
        ]
    )
    for r in results:
        print(f"  {r['name']:25s}  qty={r['total_qty']}  revenue=${r['total_revenue']:,.2f}")

    # ── $facet: dashboard summary ─────────────────────────────
    print("\n── $facet: store dashboard ──")
    results = orders.aggregate(
        [
            {
                "$facet": {
                    "order_status": [
                        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                    ],
                    "revenue_total": [
                        {
                            "$group": {
                                "_id": None,
                                "total": {"$sum": "$total"},
                                "orders": {"$sum": 1},
                            }
                        },
                    ],
                    "biggest_order": [
                        {"$sort": {"total": -1}},
                        {"$limit": 1},
                        {"$project": {"customer": 1, "total": 1, "_id": 0}},
                    ],
                }
            },
        ]
    )
    dash = results[0]

    print("  Order status:")
    for s in dash["order_status"]:
        print(f"    {s['_id']:12s}  {s['count']} orders")

    rev = dash["revenue_total"][0]
    print(f"  Total revenue: ${rev['total']:,.2f} across {rev['orders']} orders")

    big = dash["biggest_order"][0]
    print(f"  Biggest order: ${big['total']:,.2f} by customer {big['customer']}")

    print()


if __name__ == "__main__":
    main()
