#!/usr/bin/env python3
"""
content_cms.py -- Content management system with smongo.

Models a blog/CMS with articles, authors, and tags. Demonstrates
document relationships, tag-based faceted navigation, content search
with $regex, reading-time computation, and editorial analytics.

Run:
    python examples/patterns/content_cms.py
"""

import shutil
import tempfile

from smongo import MongoClient


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_cms_")
    client = MongoClient(f"local://{db_path}")
    db = client["blog"]

    try:
        seed(db)
        queries(db)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def seed(db) -> None:
    authors = db["authors"]
    articles = db["articles"]

    print("── seeding blog data ──")

    authors.insert_many([
        {"_id": "auth_1", "name": "Sarah Chen",     "bio": "Staff engineer writing about distributed systems", "twitter": "@sarahchen"},
        {"_id": "auth_2", "name": "Marcus Johnson",  "bio": "DevRel lead, conference speaker", "twitter": "@marcusj"},
        {"_id": "auth_3", "name": "Priya Gupta",     "bio": "Database internals and storage engines", "twitter": "@priyaDB"},
    ])

    articles.insert_many([
        {
            "title": "Understanding B-Tree Indexes",
            "slug": "understanding-b-tree-indexes",
            "author_id": "auth_3",
            "status": "published",
            "tags": ["databases", "indexes", "performance", "internals"],
            "word_count": 2800,
            "views": 15200,
            "likes": 342,
            "body": "B-Tree indexes are the backbone of modern databases. Every time you run a query...",
            "meta": {"reading_time_min": 11, "difficulty": "intermediate"},
        },
        {
            "title": "Building Offline-First Apps with Local Databases",
            "slug": "offline-first-local-databases",
            "author_id": "auth_1",
            "status": "published",
            "tags": ["architecture", "offline-first", "mobile", "sync"],
            "word_count": 3500,
            "views": 8900,
            "likes": 278,
            "body": "The best apps work everywhere -- with or without a network connection...",
            "meta": {"reading_time_min": 14, "difficulty": "advanced"},
        },
        {
            "title": "Getting Started with MongoDB Aggregation Pipelines",
            "slug": "mongodb-aggregation-getting-started",
            "author_id": "auth_2",
            "status": "published",
            "tags": ["mongodb", "aggregation", "tutorial", "databases"],
            "word_count": 2200,
            "views": 22400,
            "likes": 567,
            "body": "Aggregation pipelines are one of MongoDB's most powerful features...",
            "meta": {"reading_time_min": 9, "difficulty": "beginner"},
        },
        {
            "title": "Write-Ahead Logging and Crash Recovery",
            "slug": "wal-crash-recovery",
            "author_id": "auth_3",
            "status": "published",
            "tags": ["databases", "internals", "reliability", "storage"],
            "word_count": 4100,
            "views": 6700,
            "likes": 198,
            "body": "When your database process crashes mid-write, what happens to your data?...",
            "meta": {"reading_time_min": 16, "difficulty": "advanced"},
        },
        {
            "title": "The Real Cost of Network Round-Trips",
            "slug": "network-round-trip-cost",
            "author_id": "auth_1",
            "status": "published",
            "tags": ["performance", "architecture", "networking"],
            "word_count": 1800,
            "views": 12300,
            "likes": 412,
            "body": "Every network call has a cost. In a microservices world, those costs multiply...",
            "meta": {"reading_time_min": 7, "difficulty": "intermediate"},
        },
        {
            "title": "Vector Search: From Theory to Practice",
            "slug": "vector-search-theory-practice",
            "author_id": "auth_2",
            "status": "published",
            "tags": ["ai", "vector-search", "databases", "embeddings"],
            "word_count": 3200,
            "views": 18500,
            "likes": 489,
            "body": "Semantic search changes how users find content. Instead of keyword matching...",
            "meta": {"reading_time_min": 13, "difficulty": "intermediate"},
        },
        {
            "title": "Conflict Resolution in Distributed Systems",
            "slug": "conflict-resolution-distributed",
            "author_id": "auth_1",
            "status": "draft",
            "tags": ["distributed-systems", "sync", "architecture"],
            "word_count": 1200,
            "views": 0,
            "likes": 0,
            "body": "When two nodes modify the same document at the same time...",
            "meta": {"reading_time_min": 5, "difficulty": "advanced"},
        },
        {
            "title": "Index Selectivity: Why Some Indexes Don't Help",
            "slug": "index-selectivity",
            "author_id": "auth_3",
            "status": "published",
            "tags": ["databases", "indexes", "performance", "query-planning"],
            "word_count": 2600,
            "views": 9100,
            "likes": 256,
            "body": "Not all indexes improve query performance. A low-selectivity index...",
            "meta": {"reading_time_min": 10, "difficulty": "intermediate"},
        },
    ])

    articles.create_index([("tags", 1)])
    articles.create_index([("author_id", 1)])
    articles.create_index([("status", 1)])
    articles.create_index([("views", -1)])
    authors.create_index("name", unique=True)

    print(f"  {authors.count_documents({})} authors, {articles.count_documents({})} articles\n")


def queries(db) -> None:
    articles = db["articles"]

    # ── Tag-based faceted navigation ──────────────────────────
    print("── faceted navigation: articles per tag ──")
    results = articles.aggregate([
        {"$match": {"status": "published"}},
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ])
    for r in results:
        bar = "#" * r["count"]
        print(f"  {r['_id']:25s}  {r['count']}  {bar}")

    # ── Content search with $regex ────────────────────────────
    print("\n── search: titles containing 'index' (case-insensitive) ──")
    for d in articles.find({"title": {"$regex": "index", "$options": "i"}, "status": "published"}):
        print(f"  {d['title']}")
        print(f"    {d['views']:,} views  {d['likes']} likes  {d['meta']['reading_time_min']}min read")

    # ── Author productivity: articles + total views ───────────
    print("\n── author leaderboard ──")
    results = articles.aggregate([
        {"$match": {"status": "published"}},
        {"$group": {
            "_id": "$author_id",
            "articles": {"$sum": 1},
            "total_views": {"$sum": "$views"},
            "total_likes": {"$sum": "$likes"},
            "avg_word_count": {"$avg": "$word_count"},
        }},
        {"$lookup": {
            "from": "authors",
            "localField": "_id",
            "foreignField": "_id",
            "as": "author",
        }},
        {"$unwind": "$author"},
        {"$project": {
            "name": "$author.name",
            "articles": 1,
            "total_views": 1,
            "total_likes": 1,
            "avg_word_count": {"$round": ["$avg_word_count", 0]},
        }},
        {"$sort": {"total_views": -1}},
    ])
    for r in results:
        print(f"  {r['name']:20s}  {r['articles']} articles  "
              f"{r['total_views']:>6,} views  {r['total_likes']:>4} likes  "
              f"avg {r['avg_word_count']:.0f} words")

    # ── Reading difficulty distribution ────────────────────────
    print("\n── content mix by difficulty ──")
    results = articles.aggregate([
        {"$match": {"status": "published"}},
        {"$group": {
            "_id": "$meta.difficulty",
            "count": {"$sum": 1},
            "avg_views": {"$avg": "$views"},
            "total_likes": {"$sum": "$likes"},
        }},
        {"$sort": {"avg_views": -1}},
    ])
    for r in results:
        print(f"  [{r['_id']:15s}]  {r['count']} articles  "
              f"avg views={r['avg_views']:>8,.0f}  likes={r['total_likes']}")

    # ── $facet: editorial dashboard ───────────────────────────
    print("\n── $facet: editorial dashboard ──")
    results = articles.aggregate([
        {"$facet": {
            "top_articles": [
                {"$match": {"status": "published"}},
                {"$sort": {"views": -1}},
                {"$limit": 3},
                {"$project": {"title": 1, "views": 1, "likes": 1, "_id": 0}},
            ],
            "draft_count": [
                {"$match": {"status": "draft"}},
                {"$count": "total"},
            ],
            "content_stats": [
                {"$match": {"status": "published"}},
                {"$group": {
                    "_id": None,
                    "total_articles": {"$sum": 1},
                    "total_words": {"$sum": "$word_count"},
                    "avg_reading_time": {"$avg": "$meta.reading_time_min"},
                }},
            ],
        }},
    ])
    dash = results[0]

    print("  Top 3 articles:")
    for a in dash["top_articles"]:
        print(f"    {a['views']:>6,} views  {a['likes']:>4} likes  {a['title']}")

    drafts = dash["draft_count"][0]["total"] if dash["draft_count"] else 0
    print(f"  Drafts in progress: {drafts}")

    stats = dash["content_stats"][0]
    print(f"  Published: {stats['total_articles']} articles, {stats['total_words']:,} total words, "
          f"avg {stats['avg_reading_time']:.0f}min read time")

    # ── Related articles: find by shared tags ─────────────────
    print("\n── related articles: shares tags with 'B-Tree Indexes' ──")
    source = articles.find_one({"slug": "understanding-b-tree-indexes"})
    if source:
        source_tags = source["tags"]
        related = list(articles.find({
            "tags": {"$in": source_tags},
            "slug": {"$ne": source["slug"]},
            "status": "published",
        }))
        for d in sorted(related, key=lambda x: x["views"], reverse=True):
            shared = set(d["tags"]) & set(source_tags)
            print(f"  {d['title']}")
            print(f"    shared tags: {', '.join(sorted(shared))}  |  {d['views']:,} views")

    print()


if __name__ == "__main__":
    main()
