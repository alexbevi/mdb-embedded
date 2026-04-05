#!/usr/bin/env python3
"""
02_chat_memory.py -- AI chat memory over the wire protocol.

Starts smongo's embedded wire server, then uses standard PyMongo to
store and retrieve chat history. Any LangChain MongoDBChatMessageHistory
or custom agent memory that uses PyMongo will work unchanged.

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

from smongo import WireServer

PORT = 27022


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_chat_wire_")

    # ── 1. Start wire server ───────────────────────────────────
    print("── AI Chat Memory over the Wire Protocol ──\n")
    print(f"1. Starting wire protocol server on port {PORT}...")

    with WireServer(db_path, port=PORT) as _srv:
        time.sleep(0.3)

        from pymongo import MongoClient as PyMongoClient

        client = PyMongoClient(
            f"mongodb://localhost:{PORT}",
            serverSelectionTimeoutMS=5000,
            directConnection=True,
        )
        messages = client["ai_agent"]["messages"]
        sessions = client["ai_agent"]["sessions"]

        # ── 2. Record a multi-turn conversation ────────────────
        print("2. Recording a conversation via standard PyMongo...\n")

        session_id = "sess_001"
        now = time.time()

        conversation = [
            {"role": "user",      "content": "What is smongo?",                                                              "ts": now},
            {"role": "assistant", "content": "smongo is a local-first embedded MongoDB engine built on WiredTiger.",          "ts": now + 1},
            {"role": "user",      "content": "Does it support transactions?",                                                "ts": now + 2},
            {"role": "assistant", "content": "Yes -- full ACID transactions with snapshot isolation.",                        "ts": now + 3},
            {"role": "user",      "content": "Can I use it for vector search?",                                              "ts": now + 4},
            {"role": "assistant", "content": "Absolutely. $vectorSearch runs cosine/euclidean similarity in-memory.",         "ts": now + 5},
            {"role": "user",      "content": "How do I sync to the cloud?",                                                  "ts": now + 6},
            {"role": "assistant", "content": "Pass a sync URI to MongoClient and call client.sync.push()/pull().",           "ts": now + 7},
        ]

        messages.insert_many([
            {"session_id": session_id, "role": m["role"], "content": m["content"], "timestamp": m["ts"]}
            for m in conversation
        ])

        sessions.insert_one({
            "session_id": session_id,
            "user": "fabian",
            "started_at": now,
            "message_count": len(conversation),
            "tags": ["smongo", "getting-started"],
        })

        print(f"   Stored {len(conversation)} messages in session {session_id}")

        # ── 3. Retrieve last N messages (context window) ───────
        print("\n3. Last 4 messages (context window for next LLM call):\n")

        recent = list(
            messages.find(
                {"session_id": session_id},
                {"role": 1, "content": 1, "_id": 0},
            ).sort("timestamp", -1).limit(4)
        )
        recent.reverse()

        for m in recent:
            prefix = "USER:" if m["role"] == "user" else "ASST:"
            print(f"   {prefix:6s} {m['content']}")

        # ── 4. Second session ──────────────────────────────────
        print("\n4. Adding a second conversation...")

        session_id_2 = "sess_002"
        messages.insert_many([
            {"session_id": session_id_2, "role": "user",      "content": "How fast is smongo?",                                       "timestamp": now + 100},
            {"session_id": session_id_2, "role": "assistant", "content": "Benchmarks show ~2x faster than pymongo for single-doc ops.", "timestamp": now + 101},
            {"session_id": session_id_2, "role": "user",      "content": "What about aggregation?",                                    "timestamp": now + 102},
            {"session_id": session_id_2, "role": "assistant", "content": "25+ pipeline stages run in Rust with spill-to-disk.",         "timestamp": now + 103},
        ])

        sessions.insert_one({
            "session_id": session_id_2,
            "user": "fabian",
            "started_at": now + 100,
            "message_count": 4,
            "tags": ["smongo", "performance"],
        })

        print(f"   Total messages across all sessions: {messages.count_documents({})}")

        # ── 5. Cross-session search ────────────────────────────
        print("\n5. Search all conversations for 'vector':\n")

        for m in messages.find({"content": {"$regex": "vector", "$options": "i"}}):
            print(f"   [{m['session_id']}] {m['role']}: {m['content'][:70]}")

        # ── 6. Analytics via aggregation ───────────────────────
        print("\n6. Chat analytics (aggregation pipeline):\n")

        print("   Messages per role:")
        for r in messages.aggregate([
            {"$group": {"_id": "$role", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]):
            print(f"     {r['_id']:12s}  {r['count']} messages")

        print("\n   Messages per session:")
        for r in messages.aggregate([
            {"$group": {"_id": "$session_id", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]):
            print(f"     {r['_id']:12s}  {r['count']} messages")

        # ── 7. Assemble LLM prompt from history ────────────────
        print("\n7. Assembling LLM prompt from chat history:\n")

        system_msg = "You are a helpful assistant that answers questions about smongo."
        history = list(
            messages.find(
                {"session_id": session_id},
                {"role": 1, "content": 1, "_id": 0},
            ).sort("timestamp", 1)
        )

        llm_messages = [{"role": "system", "content": system_msg}] + history

        print(f"   Prompt: {len(llm_messages)} messages (1 system + {len(history)} history)")
        print("   Ready to send to OpenAI / Anthropic / Ollama / any LLM API\n")

        for m in llm_messages[:4]:
            tag = m["role"].upper()
            print(f"   [{tag:9s}] {m['content'][:65]}...")
        print(f"   ... +{len(llm_messages) - 4} more messages")

        print("\n   Every operation above used standard PyMongo over the wire.")
        print("   LangChain's MongoDBChatMessageHistory works the same way.\n")

        client.close()

    shutil.rmtree(db_path, ignore_errors=True)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
