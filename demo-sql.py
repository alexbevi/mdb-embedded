#!/usr/bin/env python3
"""
SQLite vs smongo  --  head-to-head performance benchmark.

Runs identical logical operations on both engines and prints a
side-by-side timing table at the end.

Usage:
    python demo-sql.py              # default 50 000 docs/rows
    python demo-sql.py --rows 200000
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import random
import shutil
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass

from smongo import MongoClient

# ── Helpers ──────────────────────────────────────────────────────────

DEPTS = ["engineering", "design", "management", "data", "marketing", "sales", "support"]
CITIES = ["NYC", "SF", "LA", "CHI", "SEA", "BOS", "AUS", "DEN"]
TAGS = ["python", "rust", "go", "js", "react", "ml", "k8s", "figma", "spark", "java"]

random.seed(42)


def _rand_name(i: int) -> str:
    return f"user_{i:07d}"


def _rand_doc(i: int) -> dict:
    return {
        "name": _rand_name(i),
        "age": random.randint(18, 65),
        "city": random.choice(CITIES),
        "dept": random.choice(DEPTS),
        "salary": random.randint(60_000, 220_000),
        "tags": random.sample(TAGS, k=random.randint(1, 4)),
    }


def _generate_dataset(n: int) -> list[dict]:
    return [_rand_doc(i) for i in range(n)]


def _rand_nested_doc(i: int) -> dict:
    """Document with nested objects and arrays -- showcases document-native ops."""
    return {
        "name": _rand_name(i),
        "profile": {
            "city": random.choice(CITIES),
            "score": round(random.uniform(0.0, 1.0), 3),
            "level": random.randint(1, 10),
        },
        "tags": random.sample(TAGS, k=random.randint(1, 4)),
        "items": [
            {
                "sku": f"SKU{j}",
                "qty": random.randint(0, 50),
                "price": round(random.uniform(5, 500), 2),
            }
            for j in random.sample(range(100), k=random.randint(1, 5))
        ],
    }


def _generate_nested_dataset(n: int) -> list[dict]:
    return [_rand_nested_doc(i) for i in range(n)]


@dataclass
class Result:
    label: str
    sqlite_ms: float = 0.0
    smongo_ms: float = 0.0


@contextmanager
def _timer():
    """Yield a callable that returns elapsed ms when called."""
    gc.disable()
    t0 = time.perf_counter()
    elapsed = lambda: (time.perf_counter() - t0) * 1000
    try:
        yield elapsed
    finally:
        gc.enable()


# ── SQLite harness ───────────────────────────────────────────────────


class SQLiteBench:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")

    def close(self):
        self.conn.close()

    # -- setup --
    def create_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id    INTEGER PRIMARY KEY,
                name  TEXT NOT NULL,
                age   INTEGER NOT NULL,
                city  TEXT NOT NULL,
                dept  TEXT NOT NULL,
                salary INTEGER NOT NULL,
                tags  TEXT NOT NULL
            )
        """)
        self.conn.commit()

    def create_indexes(self):
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_age ON users(age)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_city_age ON users(city, age)")
        self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON users(name)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_dept ON users(dept)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_salary ON users(salary)")
        self.conn.commit()

    # -- writes --
    def insert_many(self, docs: list[dict]):
        rows = [
            (d["name"], d["age"], d["city"], d["dept"], d["salary"], ",".join(d["tags"]))
            for d in docs
        ]
        self.conn.executemany(
            "INSERT INTO users (name, age, city, dept, salary, tags) VALUES (?,?,?,?,?,?)",
            rows,
        )
        self.conn.commit()

    def update_one(self, name: str, salary_inc: int):
        self.conn.execute(
            "UPDATE users SET salary = salary + ? WHERE name = ?",
            (salary_inc, name),
        )
        self.conn.commit()

    def update_many_dept(self, dept: str, salary_inc: int):
        self.conn.execute(
            "UPDATE users SET salary = salary + ? WHERE dept = ?",
            (salary_inc, dept),
        )
        self.conn.commit()

    def delete_range(self, min_age: int, max_age: int):
        self.conn.execute("DELETE FROM users WHERE age BETWEEN ? AND ?", (min_age, max_age))
        self.conn.commit()

    # -- reads --
    def point_lookup(self, name: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM users WHERE name = ?", (name,)).fetchone()
        return row

    def range_query(self, min_age: int):
        return self.conn.execute("SELECT * FROM users WHERE age > ?", (min_age,)).fetchall()

    def multi_condition(self, city: str, min_salary: int):
        return self.conn.execute(
            "SELECT * FROM users WHERE city = ? AND salary >= ?",
            (city, min_salary),
        ).fetchall()

    def group_avg_salary(self):
        return self.conn.execute(
            "SELECT dept, AVG(salary) AS avg_sal, COUNT(*) AS cnt "
            "FROM users GROUP BY dept ORDER BY avg_sal DESC"
        ).fetchall()

    def top_n_by_dept(self, n: int = 3):
        return self.conn.execute(
            "SELECT dept, name, salary FROM users u1 "
            "WHERE (SELECT COUNT(*) FROM users u2 "
            "       WHERE u2.dept = u1.dept AND u2.salary > u1.salary) < ? "
            "ORDER BY dept, salary DESC",
            (n,),
        ).fetchall()

    def count_by_city(self):
        return self.conn.execute(
            "SELECT city, COUNT(*) FROM users GROUP BY city ORDER BY COUNT(*) DESC"
        ).fetchall()

    def salary_percentiles(self):
        return self.conn.execute(
            "SELECT dept, MIN(salary), MAX(salary), AVG(salary) FROM users GROUP BY dept"
        ).fetchall()

    # -- document-native benchmarks (JSON blob column) --
    def create_docs_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS docs (
                id   INTEGER PRIMARY KEY,
                data TEXT NOT NULL
            )
        """)
        self.conn.commit()

    def insert_docs(self, docs: list[dict]):
        rows = [(json.dumps(d),) for d in docs]
        self.conn.executemany("INSERT INTO docs (data) VALUES (?)", rows)
        self.conn.commit()

    def nested_query(self, city: str, min_score: float):
        return self.conn.execute(
            "SELECT data FROM docs "
            "WHERE json_extract(data, '$.profile.city') = ? "
            "  AND json_extract(data, '$.profile.score') >= ?",
            (city, min_score),
        ).fetchall()

    def array_contains_all(self, tag1: str, tag2: str):
        return self.conn.execute(
            "SELECT data FROM docs WHERE "
            "EXISTS (SELECT 1 FROM json_each(json_extract(data, '$.tags')) WHERE value = ?) AND "
            "EXISTS (SELECT 1 FROM json_each(json_extract(data, '$.tags')) WHERE value = ?)",
            (tag1, tag2),
        ).fetchall()

    def array_push(self, name: str, new_tag: str):
        row = self.conn.execute(
            "SELECT id, data FROM docs WHERE json_extract(data, '$.name') = ?", (name,)
        ).fetchone()
        if row:
            doc = json.loads(row[1])
            doc["tags"].append(new_tag)
            self.conn.execute("UPDATE docs SET data = ? WHERE id = ?", (json.dumps(doc), row[0]))
            self.conn.commit()

    def unwind_group_tags(self):
        return self.conn.execute(
            "SELECT j.value AS tag, COUNT(*) AS cnt "
            "FROM docs, json_each(json_extract(docs.data, '$.tags')) AS j "
            "GROUP BY j.value ORDER BY cnt DESC"
        ).fetchall()

    def facet_query(self):
        r1 = self.conn.execute(
            "SELECT json_extract(data, '$.profile.city') AS city, COUNT(*) AS cnt "
            "FROM docs GROUP BY city ORDER BY cnt DESC"
        ).fetchall()
        r2 = self.conn.execute(
            "SELECT json_extract(data, '$.profile.city') AS city, "
            "       AVG(json_extract(data, '$.profile.score')) AS avg_score "
            "FROM docs GROUP BY city ORDER BY avg_score DESC LIMIT 3"
        ).fetchall()
        return r1, r2

    def deep_nested_query(self, min_price: float):
        """Query filtering on deeply nested array element fields."""
        return self.conn.execute(
            "SELECT data FROM docs WHERE EXISTS ("
            "  SELECT 1 FROM json_each(json_extract(data, '$.items')) AS item"
            "  WHERE json_extract(item.value, '$.price') > ?"
            "    AND json_extract(item.value, '$.qty') > 10"
            ")",
            (min_price,),
        ).fetchall()

    def multi_stage_agg(self):
        """5-stage aggregation: filter → unwind → group → sort → limit."""
        rows = self.conn.execute(
            "SELECT tag, SUM(qty) AS total_qty, COUNT(*) AS cnt FROM ("
            "  SELECT j_tag.value AS tag,"
            "         json_extract(j_item.value, '$.qty') AS qty"
            "  FROM docs,"
            "       json_each(json_extract(data, '$.tags')) AS j_tag,"
            "       json_each(json_extract(data, '$.items')) AS j_item"
            "  WHERE json_extract(data, '$.profile.score') >= 0.5"
            ") GROUP BY tag ORDER BY total_qty DESC LIMIT 5"
        ).fetchall()
        return rows

    def schema_flex_query(self):
        """Query documents where a field may or may not exist."""
        r1 = self.conn.execute(
            "SELECT COUNT(*) FROM docs WHERE json_extract(data, '$.profile.level') >= 5"
        ).fetchone()
        r2 = self.conn.execute(
            "SELECT COUNT(*) FROM docs WHERE json_type(json_extract(data, '$.items')) = 'array'"
            " AND json_array_length(json_extract(data, '$.items')) >= 3"
        ).fetchone()
        return r1, r2

    def computed_field_agg(self):
        """Aggregation with computed fields: revenue = sum(price * qty) per city."""
        return self.conn.execute(
            "SELECT json_extract(data, '$.profile.city') AS city,"
            "       SUM(json_extract(i.value, '$.price') * json_extract(i.value, '$.qty')) AS revenue"
            " FROM docs, json_each(json_extract(data, '$.items')) AS i"
            " GROUP BY city ORDER BY revenue DESC"
        ).fetchall()

    def top_items_per_city(self, n: int = 3):
        """Top-N most expensive items per city (correlated subquery on JSON)."""
        return self.conn.execute(
            "SELECT json_extract(d1.data, '$.profile.city') AS city,"
            "       json_extract(i1.value, '$.sku') AS sku,"
            "       json_extract(i1.value, '$.price') AS price"
            " FROM docs d1, json_each(json_extract(d1.data, '$.items')) AS i1"
            " WHERE ("
            "   SELECT COUNT(DISTINCT json_extract(i2.value, '$.price'))"
            "   FROM docs d2, json_each(json_extract(d2.data, '$.items')) AS i2"
            "   WHERE json_extract(d2.data, '$.profile.city') = json_extract(d1.data, '$.profile.city')"
            "     AND json_extract(i2.value, '$.price') > json_extract(i1.value, '$.price')"
            " ) < ?"
            " ORDER BY city, price DESC",
            (n,),
        ).fetchall()

    def multi_group_rollup(self):
        """Group-by two JSON fields and aggregate (simulates $group with compound _id)."""
        return self.conn.execute(
            "SELECT json_extract(data, '$.profile.city') AS city,"
            "       json_extract(data, '$.profile.level') AS lvl,"
            "       COUNT(*) AS cnt,"
            "       AVG(json_extract(data, '$.profile.score')) AS avg_score"
            " FROM docs"
            " GROUP BY city, lvl"
            " ORDER BY cnt DESC"
        ).fetchall()


# ── smongo harness ───────────────────────────────────────────────────


class SmongoBench:
    def __init__(self, path: str):
        self.client = MongoClient(f"local://{path}")
        self.db = self.client["bench"]
        try:
            self.db.drop_collection("users")
        except Exception:
            pass
        self.coll = self.db["users"]

    def close(self):
        self.client.close()

    def create_indexes(self):
        self.coll.create_index([("age", 1)])
        self.coll.create_index([("city", 1), ("age", -1)])
        self.coll.create_index("name", unique=True)
        self.coll.create_index([("dept", 1)])
        self.coll.create_index([("salary", -1)])

    def insert_many(self, docs: list[dict]):
        self.coll.insert_many(docs)

    def update_one(self, name: str, salary_inc: int):
        self.coll.update_one({"name": name}, {"$inc": {"salary": salary_inc}})

    def update_many_dept(self, dept: str, salary_inc: int):
        self.coll.update_many({"dept": dept}, {"$inc": {"salary": salary_inc}})

    def delete_range(self, min_age: int, max_age: int):
        self.coll.delete_many({"age": {"$gte": min_age, "$lte": max_age}})

    def point_lookup(self, name: str):
        return self.coll.find_one({"name": name})

    def range_query(self, min_age: int):
        return self.coll.find({"age": {"$gt": min_age}}).to_list()

    def multi_condition(self, city: str, min_salary: int):
        return self.coll.find({"city": city, "salary": {"$gte": min_salary}}).to_list()

    def group_avg_salary(self):
        return list(
            self.coll.aggregate(
                [
                    {
                        "$group": {
                            "_id": "$dept",
                            "avg_sal": {"$avg": "$salary"},
                            "cnt": {"$sum": 1},
                        }
                    },
                    {"$sort": {"avg_sal": -1}},
                ]
            )
        )

    def top_n_by_dept(self, n: int = 3):
        return list(
            self.coll.aggregate(
                [
                    {"$sort": {"salary": -1}},
                    {
                        "$group": {
                            "_id": "$dept",
                            "top": {"$push": {"name": "$name", "salary": "$salary"}},
                        }
                    },
                    {"$project": {"top": {"$slice": ["$top", n]}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )

    def count_by_city(self):
        return list(
            self.coll.aggregate(
                [
                    {"$group": {"_id": "$city", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                ]
            )
        )

    def salary_percentiles(self):
        return list(
            self.coll.aggregate(
                [
                    {
                        "$group": {
                            "_id": "$dept",
                            "min_sal": {"$min": "$salary"},
                            "max_sal": {"$max": "$salary"},
                            "avg_sal": {"$avg": "$salary"},
                        }
                    },
                ]
            )
        )

    # -- document-native benchmarks --
    def create_docs_collection(self):
        try:
            self.db.drop_collection("docs")
        except Exception:
            pass
        self.docs = self.db["docs"]

    def insert_docs(self, docs: list[dict]):
        self.docs.insert_many(docs)

    def nested_query(self, city: str, min_score: float):
        return self.docs.find(
            {"profile.city": city, "profile.score": {"$gte": min_score}}
        ).to_list()

    def array_contains_all(self, tag1: str, tag2: str):
        return self.docs.find({"tags": {"$all": [tag1, tag2]}}).to_list()

    def array_push(self, name: str, new_tag: str):
        self.docs.update_one({"name": name}, {"$push": {"tags": new_tag}})

    def unwind_group_tags(self):
        return list(
            self.docs.aggregate(
                [
                    {"$unwind": "$tags"},
                    {"$group": {"_id": "$tags", "cnt": {"$sum": 1}}},
                    {"$sort": {"cnt": -1}},
                ]
            )
        )

    def facet_query(self):
        return list(
            self.docs.aggregate(
                [
                    {
                        "$facet": {
                            "by_city": [
                                {"$group": {"_id": "$profile.city", "cnt": {"$sum": 1}}},
                                {"$sort": {"cnt": -1}},
                            ],
                            "top_scores": [
                                {
                                    "$group": {
                                        "_id": "$profile.city",
                                        "avg_score": {"$avg": "$profile.score"},
                                    }
                                },
                                {"$sort": {"avg_score": -1}},
                                {"$limit": 3},
                            ],
                        }
                    }
                ]
            )
        )

    def deep_nested_query(self, min_price: float):
        """Query filtering on deeply nested array element fields."""
        return self.docs.find(
            {"items": {"$elemMatch": {"price": {"$gt": min_price}, "qty": {"$gt": 10}}}}
        ).to_list()

    def multi_stage_agg(self):
        """5-stage aggregation: filter → unwind items+tags → group → sort → limit."""
        return list(
            self.docs.aggregate(
                [
                    {"$match": {"profile.score": {"$gte": 0.5}}},
                    {"$unwind": "$items"},
                    {"$unwind": "$tags"},
                    {
                        "$group": {
                            "_id": "$tags",
                            "total_qty": {"$sum": "$items.qty"},
                            "cnt": {"$sum": 1},
                        }
                    },
                    {"$sort": {"total_qty": -1}},
                    {"$limit": 5},
                ]
            )
        )

    def schema_flex_query(self):
        """Query documents where a field may or may not exist."""
        r1 = self.docs.count_documents({"profile.level": {"$gte": 5}})
        r2 = self.docs.count_documents({"$expr": {"$gte": [{"$size": "$items"}, 3]}})
        return r1, r2

    def computed_field_agg(self):
        """Aggregation with computed fields: revenue = sum(price * qty) per city."""
        return list(
            self.docs.aggregate(
                [
                    {"$unwind": "$items"},
                    {
                        "$group": {
                            "_id": "$profile.city",
                            "revenue": {"$sum": {"$multiply": ["$items.price", "$items.qty"]}},
                        }
                    },
                    {"$sort": {"revenue": -1}},
                ]
            )
        )

    def top_items_per_city(self, n: int = 3):
        """Top-N most expensive items per city (pipeline: unwind+sort+group+slice)."""
        return list(
            self.docs.aggregate(
                [
                    {"$unwind": "$items"},
                    {"$sort": {"items.price": -1}},
                    {
                        "$group": {
                            "_id": "$profile.city",
                            "top_items": {
                                "$push": {
                                    "sku": "$items.sku",
                                    "price": "$items.price",
                                }
                            },
                        }
                    },
                    {"$project": {"top_items": {"$slice": ["$top_items", n]}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )

    def multi_group_rollup(self):
        """Group-by two nested fields and aggregate."""
        return list(
            self.docs.aggregate(
                [
                    {
                        "$group": {
                            "_id": {"city": "$profile.city", "level": "$profile.level"},
                            "cnt": {"$sum": 1},
                            "avg_score": {"$avg": "$profile.score"},
                        }
                    },
                    {"$sort": {"cnt": -1}},
                ]
            )
        )


# ── Benchmark runner ─────────────────────────────────────────────────


def run_benchmarks(n: int) -> list[Result]:
    data = _generate_dataset(n)
    results: list[Result] = []

    sqlite_path = "/tmp/_bench_sqlite.db"
    smongo_path = "/tmp/_bench_smongo_redb"
    for p in (sqlite_path, smongo_path):
        if os.path.isdir(p):
            shutil.rmtree(p)
        elif os.path.isfile(p):
            os.remove(p)

    sq = SQLiteBench(sqlite_path)
    sm = SmongoBench(smongo_path)

    def bench(label: str, sqlite_fn, smongo_fn) -> Result:
        r = Result(label)
        with _timer() as elapsed:
            sqlite_fn()
            r.sqlite_ms = elapsed()
        with _timer() as elapsed:
            smongo_fn()
            r.smongo_ms = elapsed()
        results.append(r)
        return r

    # 1. Schema / table creation
    bench("Create table / collection", sq.create_table, lambda: None)

    # 2. Bulk insert
    bench("Bulk insert", lambda: sq.insert_many(data), lambda: sm.insert_many(data))

    # 3. Create indexes (post-insert)
    bench("Create indexes (5)", sq.create_indexes, sm.create_indexes)

    # 4. Point lookup (unique index)
    target = _rand_name(n // 2)
    bench(
        "Point lookup (unique idx)",
        lambda: sq.point_lookup(target),
        lambda: sm.point_lookup(target),
    )

    # 5. Range query
    bench("Range query (age > 40)", lambda: sq.range_query(40), lambda: sm.range_query(40))

    # 6. Multi-condition filter
    bench(
        "Multi-cond (city + salary)",
        lambda: sq.multi_condition("NYC", 150_000),
        lambda: sm.multi_condition("NYC", 150_000),
    )

    # 7. GROUP BY / $group avg salary
    bench("Agg: avg salary by dept", sq.group_avg_salary, sm.group_avg_salary)

    # 8. Count by city
    bench("Agg: count by city", sq.count_by_city, sm.count_by_city)

    # 9. Salary stats per dept
    bench("Agg: salary min/max/avg", sq.salary_percentiles, sm.salary_percentiles)

    # 10. Top-N per department
    bench("Agg: top-3 earners/dept", lambda: sq.top_n_by_dept(3), lambda: sm.top_n_by_dept(3))

    # 11. Update single doc
    bench(
        "Update one (inc salary)",
        lambda: sq.update_one(target, 10_000),
        lambda: sm.update_one(target, 10_000),
    )

    # 12. Update many
    bench(
        "Update many (dept raise)",
        lambda: sq.update_many_dept("engineering", 5_000),
        lambda: sm.update_many_dept("engineering", 5_000),
    )

    # 13. Delete range
    bench(
        "Delete range (age 18-22)",
        lambda: sq.delete_range(18, 22),
        lambda: sm.delete_range(18, 22),
    )

    # ── Document-native benchmarks ───────────────────────────────
    results.append(Result("─── Document-Native ───"))

    nested_data = _generate_nested_dataset(n)

    bench("Setup doc collection", sq.create_docs_table, sm.create_docs_collection)
    bench(
        "Insert nested docs",
        lambda: sq.insert_docs(nested_data),
        lambda: sm.insert_docs(nested_data),
    )

    bench(
        "Nested field query",
        lambda: sq.nested_query("NYC", 0.7),
        lambda: sm.nested_query("NYC", 0.7),
    )

    bench(
        "Array $all / contains",
        lambda: sq.array_contains_all("python", "ml"),
        lambda: sm.array_contains_all("python", "ml"),
    )

    target_nested = _rand_name(n // 3)
    bench(
        "Array $push / append",
        lambda: sq.array_push(target_nested, "newskill"),
        lambda: sm.array_push(target_nested, "newskill"),
    )

    bench(
        "$unwind + $group tags",
        sq.unwind_group_tags,
        sm.unwind_group_tags,
    )

    bench("$facet sub-pipelines", sq.facet_query, sm.facet_query)

    bench(
        "Deep nested array query",
        lambda: sq.deep_nested_query(200.0),
        lambda: sm.deep_nested_query(200.0),
    )

    bench(
        "Multi-stage pipeline (5)",
        sq.multi_stage_agg,
        sm.multi_stage_agg,
    )

    bench("Schema-flex queries", sq.schema_flex_query, sm.schema_flex_query)

    bench(
        "Computed field agg",
        sq.computed_field_agg,
        sm.computed_field_agg,
    )

    bench(
        "Top-N items/city (subQ)",
        lambda: sq.top_items_per_city(3),
        lambda: sm.top_items_per_city(3),
    )

    bench(
        "Compound $group rollup",
        sq.multi_group_rollup,
        sm.multi_group_rollup,
    )

    sq.close()
    sm.close()

    for p in (sqlite_path, smongo_path):
        if os.path.isdir(p):
            shutil.rmtree(p)
        elif os.path.isfile(p):
            os.remove(p)

    return results


# ── Pretty printer ───────────────────────────────────────────────────


def _bar(sqlite_ms: float, smongo_ms: float, width: int = 20) -> tuple[str, str]:
    """Return a mini bar and winner tag."""
    if sqlite_ms == 0 and smongo_ms == 0:
        return " " * width, "  --"
    mx = max(sqlite_ms, smongo_ms, 0.001)
    sq_bar = int(round(sqlite_ms / mx * width))
    sm_bar = int(round(smongo_ms / mx * width))
    sq_str = "█" * sq_bar + "░" * (width - sq_bar)
    sm_str = "█" * sm_bar + "░" * (width - sm_bar)

    if sqlite_ms < smongo_ms * 0.95:
        winner = "SQLite"
    elif smongo_ms < sqlite_ms * 0.95:
        winner = "smongo"
    else:
        winner = "  TIE"
    return sq_str, sm_str, winner


def print_results(results: list[Result], n: int):
    W = 74
    print()
    print("  ╔══════════════════════════════════════════════════════════════════════════╗")
    print("  ║        SQLite  vs  smongo   ·   Head-to-Head Benchmark                  ║")
    print("  ╚══════════════════════════════════════════════════════════════════════════╝")
    print(f"\n  Dataset: {n:,} documents / rows\n")

    hdr = f"  {'Operation':<28s}  {'SQLite':>10s}  {'smongo':>10s}  {'Δ':>7s}  {'Winner':>7s}"
    print(hdr)
    print(f"  {'─' * (len(hdr) - 2)}")

    sqlite_wins = 0
    smongo_wins = 0

    for r in results:
        if r.label.startswith("───"):
            print(f"  {r.label}")
            continue

        if r.sqlite_ms == 0 and r.smongo_ms == 0:
            delta = ""
            winner = "--"
        elif r.sqlite_ms <= 0.001:
            delta = ""
            winner = "SQLite"
            sqlite_wins += 1
        elif r.smongo_ms <= 0.001:
            delta = ""
            winner = "smongo"
            smongo_wins += 1
        else:
            ratio = r.smongo_ms / r.sqlite_ms
            if ratio > 1:
                delta = f"{ratio:.1f}x"
            else:
                delta = f"{1/ratio:.1f}x"

            if r.sqlite_ms < r.smongo_ms * 0.95:
                winner = "SQLite"
                sqlite_wins += 1
            elif r.smongo_ms < r.sqlite_ms * 0.95:
                winner = "smongo"
                smongo_wins += 1
            else:
                winner = "TIE"

        print(
            f"  {r.label:<28s}  {r.sqlite_ms:>8.2f}ms  {r.smongo_ms:>8.2f}ms  {delta:>7s}  {winner:>7s}"
        )

    print(f"  {'─' * (len(hdr) - 2)}")

    scored = [r for r in results if not r.label.startswith("───")]
    total_sq = sum(r.sqlite_ms for r in scored)
    total_sm = sum(r.smongo_ms for r in scored)
    overall = "SQLite" if total_sq < total_sm else "smongo"
    print(f"  {'TOTAL':<28s}  {total_sq:>8.2f}ms  {total_sm:>8.2f}ms  " f"{'':>7s}  {overall:>7s}")

    print(f"\n  Wins  ─  SQLite: {sqlite_wins}   smongo: {smongo_wins}")

    print(f"\n  {'─' * W}")
    print("  Notes:")
    print("  • SQLite uses WAL mode + NORMAL sync (fast, common config).")
    print("  • smongo uses the embedded redb engine (default durable mode).")
    print("  • Both databases stored in /tmp (cleaned up after run).")
    print("  • Times include commit / flush overhead.")
    print(f"  {'─' * W}\n")


# ── Main ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="SQLite vs smongo benchmark")
    parser.add_argument("--rows", type=int, default=50_000, help="number of documents/rows")
    args = parser.parse_args()

    print(f"\n  Generating {args.rows:,} random records …")
    results = run_benchmarks(args.rows)
    print_results(results, args.rows)


if __name__ == "__main__":
    main()
