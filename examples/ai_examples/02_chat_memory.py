#!/usr/bin/env python3
"""
02_chat_memory.py -- AI chat memory over the wire protocol.

Starts smongo's embedded wire server, then uses standard PyMongo to store
and retrieve multi-session chat history with indexes, TTL expiry, full-text
search, and analytics.  Any LangChain MongoDBChatMessageHistory or custom
agent memory backed by PyMongo works unchanged.

Requirements:
    pip install pymongo smongo

Run:
    python examples/ai_examples/02_chat_memory.py
"""

import os
import shutil
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta

from smongo import WireServer

PORT = 27022


def separator(char="─", width=60):
    return char * width


def ts(offset_s: float = 0) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=offset_s)


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_chat_wire_")

    print("╔══════════════════════════════════════════════════════════╗")
    print("║   AI Chat Memory — smongo wire protocol + PyMongo       ║")
    print("╚══════════════════════════════════════════════════════════╝\n")

    # ── 1. Start wire server ───────────────────────────────────
    print(f"1. Starting wire protocol server on port {PORT}...")

    with WireServer(db_path, port=PORT) as _srv:
        time.sleep(0.3)

        from pymongo import MongoClient as PyMongoClient

        client = PyMongoClient(
            f"mongodb://localhost:{PORT}",
            serverSelectionTimeoutMS=5000,
            directConnection=True,
        )
        db = client["ai_agent"]
        messages = db["messages"]
        sessions = db["sessions"]

        # ── 2. Create indexes for production patterns ────────
        print("2. Creating indexes (session lookup, TTL, text search)...")
        messages.create_index([("session_id", 1), ("timestamp", 1)])
        messages.create_index("timestamp", expireAfterSeconds=86400 * 7)
        sessions.create_index("session_id", unique=True)
        print("   3 indexes created\n")

        # ── 3. Multi-turn conversation — session 1 ───────────
        print(f"   {separator()}")
        print("   SESSION 1: Getting Started with smongo")
        print(f"   {separator()}\n")

        s1 = "sess_001"
        turns_1 = [
            ("user", "What is smongo?"),
            (
                "assistant",
                "smongo is a local-first embedded MongoDB engine built on "
                "redb and Rust. No server, no Docker — just import and go.",
            ),
            ("user", "Does it support vector search?"),
            (
                "assistant",
                "Yes — $vectorSearch runs a vendored HNSW index for "
                "approximate nearest-neighbor search, with Atlas-compatible "
                "cosine/euclidean/dotProduct scoring.",
            ),
            ("user", "Can I use it with LangChain?"),
            (
                "assistant",
                "Absolutely. Start the wire server and LangChain's "
                "MongoDBAtlasVectorSearch class connects via standard "
                "PyMongo — zero custom code.",
            ),
            ("user", "How do I sync to the cloud?"),
            (
                "assistant",
                "Pass a sync URI and call client.sync.push()/pull(). "
                "Per-document vector clocks handle causal ordering.",
            ),
        ]

        docs_1 = [
            {
                "session_id": s1,
                "role": role,
                "content": content,
                "timestamp": ts(i),
                "tokens": len(content.split()),
            }
            for i, (role, content) in enumerate(turns_1)
        ]
        messages.insert_many(docs_1)
        sessions.insert_one(
            {
                "session_id": s1,
                "user": "fabian",
                "started_at": ts(0),
                "message_count": len(turns_1),
                "tags": ["smongo", "getting-started", "vector-search"],
            }
        )

        for role, content in turns_1:
            tag = "  YOU" if role == "user" else "  BOT"
            print(f"   {tag}: {content[:78]}")
        print(f"\n   Stored {len(turns_1)} messages\n")

        # ── 4. Multi-turn conversation — session 2 ───────────
        print(f"   {separator()}")
        print("   SESSION 2: Performance Deep-Dive")
        print(f"   {separator()}\n")

        s2 = "sess_002"
        turns_2 = [
            ("user", "How fast is smongo compared to pymongo?"),
            (
                "assistant",
                "The Rust core eliminates ~50 Python method dispatches per "
                "command. Single-doc ops are ~2x faster.",
            ),
            ("user", "What about aggregation performance?"),
            (
                "assistant",
                "25+ pipeline stages run in compiled Rust. The query planner "
                "auto-selects B-tree indexes, and $graphLookup uses hash-"
                "indexed foreign fields for O(1) BFS expansion.",
            ),
            ("user", "Any benchmarks for vector search?"),
            (
                "assistant",
                "The vendored HNSW handles 500K+ vectors with diversified "
                "neighbor selection and SIMD-friendly distance functions. "
                "Flat index available for multi-tenant workloads.",
            ),
        ]

        docs_2 = [
            {
                "session_id": s2,
                "role": role,
                "content": content,
                "timestamp": ts(100 + i),
                "tokens": len(content.split()),
            }
            for i, (role, content) in enumerate(turns_2)
        ]
        messages.insert_many(docs_2)
        sessions.insert_one(
            {
                "session_id": s2,
                "user": "fabian",
                "started_at": ts(100),
                "message_count": len(turns_2),
                "tags": ["smongo", "performance", "benchmarks"],
            }
        )

        for role, content in turns_2:
            tag = "  YOU" if role == "user" else "  BOT"
            print(f"   {tag}: {content[:78]}")
        print(f"\n   Stored {len(turns_2)} messages\n")

        total = messages.count_documents({})
        print(f"   Total messages across all sessions: {total}\n")

        # ── 5. Context window — last N messages ──────────────
        print(f"   {separator()}")
        print("   CONTEXT WINDOW (last 4 messages from session 1)")
        print(f"   {separator()}\n")

        recent = list(
            messages.find(
                {"session_id": s1},
                {"role": 1, "content": 1, "_id": 0},
            )
            .sort("timestamp", -1)
            .limit(4)
        )
        recent.reverse()

        for m in recent:
            tag = "  YOU" if m["role"] == "user" else "  BOT"
            print(f"   {tag}: {m['content'][:78]}")
        print()

        # ── 6. Cross-session search ──────────────────────────
        print(f"   {separator()}")
        print('   CROSS-SESSION SEARCH: "vector"')
        print(f"   {separator()}\n")

        for m in messages.find({"content": {"$regex": "vector", "$options": "i"}}):
            print(f"   [{m['session_id']}] {m['role']:9s}  " f"{m['content'][:65]}...")
        print()

        # ── 7. Analytics via aggregation ─────────────────────
        print(f"   {separator()}")
        print("   ANALYTICS (aggregation pipeline)")
        print(f"   {separator()}\n")

        print("   Messages per role:")
        for r in messages.aggregate(
            [
                {"$group": {"_id": "$role", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]
        ):
            bar = "█" * r["count"]
            print(f"     {r['_id']:12s}  {r['count']:2d}  {bar}")

        print("\n   Messages per session:")
        for r in messages.aggregate(
            [
                {
                    "$group": {
                        "_id": "$session_id",
                        "count": {"$sum": 1},
                        "total_tokens": {"$sum": "$tokens"},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        ):
            print(f"     {r['_id']:12s}  {r['count']:2d} msgs  " f"{r['total_tokens']:4d} tokens")

        print("\n   Average tokens per message:")
        for r in messages.aggregate(
            [
                {
                    "$group": {
                        "_id": "$role",
                        "avg_tokens": {"$avg": "$tokens"},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        ):
            print(f"     {r['_id']:12s}  {r['avg_tokens']:.1f} tokens/msg")

        # ── 8. Assemble LLM prompt from history ──────────────
        print(f"\n   {separator()}")
        print("   LLM PROMPT ASSEMBLY")
        print(f"   {separator()}\n")

        system_msg = (
            "You are a helpful assistant that answers questions about "
            "smongo. Use conversation history for context."
        )
        history = list(
            messages.find(
                {"session_id": s1},
                {"role": 1, "content": 1, "_id": 0},
            ).sort("timestamp", 1)
        )

        llm_messages = [{"role": "system", "content": system_msg}]
        llm_messages.extend(history)

        print(f"   Prompt: {len(llm_messages)} messages " f"(1 system + {len(history)} history)")
        print("   Ready to send to OpenAI / Anthropic / Ollama / " "any LLM API\n")

        for m in llm_messages[:5]:
            tag = m["role"].upper()
            print(f"   [{tag:9s}] {m['content'][:65]}...")
        if len(llm_messages) > 5:
            print(f"   ... +{len(llm_messages) - 5} more messages")

        # ── Summary ──────────────────────────────────────────
        print(f"\n   {separator('═')}")
        print("   Every operation used standard PyMongo over the wire.")
        print("   LangChain's MongoDBChatMessageHistory works the same way.")
        print("   Indexes, TTL, aggregation, regex search — all native.")
        print(f"   {separator('═')}\n")

        client.close()

    shutil.rmtree(db_path, ignore_errors=True)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
