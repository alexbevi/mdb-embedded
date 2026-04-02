#!/usr/bin/env python3
"""
03_aggregation.py -- Aggregation pipelines with smongo.

Builds a small employee/department dataset and runs progressively
richer pipelines: $match + $group, $sort, $project, $unwind, $lookup,
$addFields, and $facet.

Run:
    python examples/basic/03_aggregation.py
"""

import shutil
import tempfile

from smongo import MongoClient


DEPARTMENTS = [
    {"_id": "engineering", "label": "Engineering",  "budget": 2_000_000},
    {"_id": "design",      "label": "Design",       "budget": 800_000},
    {"_id": "data",        "label": "Data Science",  "budget": 1_200_000},
    {"_id": "management",  "label": "Management",   "budget": 500_000},
]

EMPLOYEES = [
    {"name": "Alice",   "dept": "engineering", "salary": 145000, "skills": ["python", "go"]},
    {"name": "Bob",     "dept": "engineering", "salary": 128000, "skills": ["java", "python"]},
    {"name": "Charlie", "dept": "management",  "salary": 175000, "skills": ["leadership"]},
    {"name": "Diana",   "dept": "design",      "salary": 98000,  "skills": ["figma", "css"]},
    {"name": "Eve",     "dept": "engineering", "salary": 155000, "skills": ["rust", "python"]},
    {"name": "Frank",   "dept": "engineering", "salary": 140000, "skills": ["go", "java"]},
    {"name": "Grace",   "dept": "data",        "salary": 135000, "skills": ["python", "sql"]},
    {"name": "Hank",    "dept": "management",  "salary": 190000, "skills": ["leadership", "finance"]},
    {"name": "Ivy",     "dept": "design",      "salary": 105000, "skills": ["figma", "html"]},
    {"name": "Jack",    "dept": "engineering", "salary": 142000, "skills": ["python", "go", "rust"]},
]


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_agg_")
    client = MongoClient(f"local://{db_path}")
    db = client["analytics"]
    employees = db["employees"]
    departments = db["departments"]

    try:
        employees.insert_many(EMPLOYEES)
        departments.insert_many(DEPARTMENTS)
        _run(employees, departments)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def _run(employees, departments) -> None:
    # ── $match + $group: headcount and salary stats per department ─
    print("── $match + $group: salary stats per department ──")
    results = employees.aggregate([
        {"$group": {
            "_id": "$dept",
            "count":      {"$sum": 1},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"},
            "min_salary": {"$min": "$salary"},
        }},
        {"$sort": {"avg_salary": -1}},
    ])
    for r in results:
        print(f"  {r['_id']:15s}  n={r['count']}  avg=${r['avg_salary']:,.0f}  "
              f"range=${r['min_salary']:,.0f}–${r['max_salary']:,.0f}")

    # ── $match + $group + $sort + $limit: top 2 highest-paying depts ─
    print()
    print("── top 2 departments by average salary ──")
    results = employees.aggregate([
        {"$group": {"_id": "$dept", "avg": {"$avg": "$salary"}}},
        {"$sort": {"avg": -1}},
        {"$limit": 2},
    ])
    for r in results:
        print(f"  {r['_id']:15s}  ${r['avg']:,.0f}")

    # ── $project: computed fields ─────────────────────────────
    print()
    print("── $project: annual bonus (10% of salary) ──")
    results = employees.aggregate([
        {"$match": {"dept": "engineering"}},
        {"$project": {
            "name": 1,
            "salary": 1,
            "bonus": {"$multiply": ["$salary", 0.10]},
            "_id": 0,
        }},
        {"$sort": {"salary": -1}},
    ])
    for r in results:
        print(f"  {r['name']:10s}  salary=${r['salary']:,.0f}  bonus=${r['bonus']:,.0f}")

    # ── $unwind: expand skills array ──────────────────────────
    print()
    print("── $unwind + $group: most popular skills ──")
    results = employees.aggregate([
        {"$unwind": "$skills"},
        {"$group": {"_id": "$skills", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5},
    ])
    for r in results:
        print(f"  {r['_id']:12s}  used by {r['count']} employees")

    # ── $lookup: join employees with departments ──────────────
    print()
    print("── $lookup: employees with department details ──")
    results = employees.aggregate([
        {"$match": {"dept": "engineering"}},
        {"$lookup": {
            "from": "departments",
            "localField": "dept",
            "foreignField": "_id",
            "as": "dept_info",
        }},
        {"$sort": {"name": 1}},
        {"$limit": 3},
    ])
    for r in results:
        info = r["dept_info"][0] if r.get("dept_info") else {}
        dept_label = info.get("label", "N/A")
        budget = info.get("budget", 0)
        print(f"  {r['name']:10s}  ${r['salary']:>8,}  dept={dept_label}  budget=${budget:,}")

    # ── $addFields / $set: salary band ────────────────────────
    print()
    print("── $addFields: salary band classification ──")
    results = employees.aggregate([
        {"$addFields": {
            "band": {
                "$cond": {
                    "if": {"$gte": ["$salary", 150000]},
                    "then": "senior",
                    "else": {"$cond": {
                        "if": {"$gte": ["$salary", 120000]},
                        "then": "mid",
                        "else": "junior",
                    }},
                },
            },
        }},
        {"$project": {"name": 1, "salary": 1, "band": 1, "_id": 0}},
        {"$sort": {"salary": -1}},
    ])
    for r in results:
        print(f"  {r['name']:10s}  ${r['salary']:>8,}  [{r['band']}]")

    # ── $facet: parallel sub-pipelines ────────────────────────
    print()
    print("── $facet: department summary + top earners (parallel) ──")
    results = employees.aggregate([
        {"$facet": {
            "by_dept": [
                {"$group": {"_id": "$dept", "headcount": {"$sum": 1}}},
                {"$sort": {"headcount": -1}},
            ],
            "top_earners": [
                {"$sort": {"salary": -1}},
                {"$limit": 3},
                {"$project": {"name": 1, "salary": 1, "_id": 0}},
            ],
        }},
    ])
    facet = results[0]

    print("  Department headcounts:")
    for r in facet["by_dept"]:
        print(f"    {r['_id']:15s}  {r['headcount']} people")

    print("  Top 3 earners:")
    for r in facet["top_earners"]:
        print(f"    {r['name']:15s}  ${r['salary']:,}")

    print()


if __name__ == "__main__":
    main()
