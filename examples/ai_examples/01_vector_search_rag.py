#!/usr/bin/env python3
"""
01_vector_search_rag.py -- Local RAG pipeline over the wire protocol.

Starts smongo's embedded wire server, then runs the entire RAG workflow
through standard PyMongo. The $vectorSearch aggregation stage executes
inside the embedded engine -- PyMongo thinks it's talking to MongoDB Atlas.

Requirements:
    pip install pymongo numpy smongo

Run:
    python examples/ai_examples/01_vector_search_rag.py
"""

import os
import shutil
import sys
import tempfile
import time
from collections import Counter

import numpy as np

from smongo import MongoClient as SmongoClient
from smongo import WireServer

PORT = 27019


# ---------------------------------------------------------------------------
# Tiny TF-IDF vectorizer (no external model needed)
# In production, swap for OpenAI / HuggingFace / Cohere embeddings.
# ---------------------------------------------------------------------------
class TinyVectorizer:
    """Bag-of-words vectorizer that produces normalized float vectors."""

    def __init__(self):
        self.vocab: dict[str, int] = {}

    def fit(self, texts: list[str]) -> "TinyVectorizer":
        counts: Counter[str] = Counter()
        for t in texts:
            counts.update(set(t.lower().split()))
        self.vocab = {w: i for i, (w, _) in enumerate(counts.most_common())}
        return self

    def embed(self, text: str) -> list[float]:
        vec = np.zeros(len(self.vocab), dtype=np.float32)
        for w in text.lower().split():
            if w in self.vocab:
                vec[self.vocab[w]] = 1.0
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec /= norm
        return vec.tolist()


# ---------------------------------------------------------------------------
# Knowledge base
# ---------------------------------------------------------------------------
KNOWLEDGE = [
    "WiredTiger is a high-performance storage engine that uses B-tree indexes and supports MVCC concurrency.",
    "smongo supports full ACID transactions with snapshot isolation across multiple collections.",
    "The $vectorSearch aggregation stage performs in-memory cosine similarity search with no external database.",
    "Change streams let you watch a collection for real-time insert, update, and delete events.",
    "The query planner automatically selects B-tree indexes, falling back to collection scan when no index fits.",
    "smongo runs entirely in-process -- no server, no network, no Docker. Just import and go.",
    "The aggregation pipeline supports 25+ stages including $lookup joins, $facet, and $graphLookup.",
    "Atlas sync pushes local writes to MongoDB Atlas and pulls remote changes with conflict resolution.",
    "Schema validation uses $jsonSchema to enforce document structure at write time.",
    "Bulk write operations batch inserts, updates, and deletes into a single atomic call.",
]


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_rag_wire_")

    # ── 1. Seed via native smongo ──────────────────────────────
    print("── RAG Pipeline over the Wire Protocol ──\n")
    print("1. Building knowledge base via native smongo client...")

    vectorizer = TinyVectorizer().fit(KNOWLEDGE)
    print(f"   Vocabulary size: {len(vectorizer.vocab)} terms")

    native = SmongoClient(f"local://{db_path}")
    coll = native["rag_demo"]["knowledge_base"]
    coll.insert_many(
        [
            {"text": text, "embedding": vectorizer.embed(text), "source": f"chunk_{i}"}
            for i, text in enumerate(KNOWLEDGE)
        ]
    )
    print(f"   Inserted {coll.count_documents({})} documents with embeddings")
    native.close()

    # ── 2. Start wire server ───────────────────────────────────
    print(f"\n2. Starting wire protocol server on port {PORT}...")

    with WireServer(db_path, port=PORT) as _srv:
        time.sleep(0.3)

        # ── 3. Connect with STANDARD PyMongo ───────────────────
        from pymongo import MongoClient as PyMongoClient

        client = PyMongoClient(
            f"mongodb://localhost:{PORT}",
            serverSelectionTimeoutMS=5000,
            directConnection=True,
        )
        kb = client["rag_demo"]["knowledge_base"]
        print(f"   PyMongo connected -- sees {kb.count_documents({})} documents\n")

        # ── 4. Semantic search via standard pymongo.aggregate ──
        print("3. Semantic search: 'How does smongo handle queries?'\n")

        query_vec = vectorizer.embed("How does smongo handle queries?")
        results = list(
            kb.aggregate(
                [
                    {
                        "$vectorSearch": {
                            "path": "embedding",
                            "queryVector": query_vec,
                            "limit": 3,
                            "metric": "cosine",
                        }
                    },
                    {"$project": {"text": 1, "_vectorScore": 1, "source": 1, "_id": 0}},
                ]
            )
        )

        for r in results:
            print(f"   [{r['_vectorScore']:.4f}] {r['text'][:80]}...")

        # ── 5. Filtered vector search ──────────────────────────
        print("\n4. Filtered search (only chunks 0-4): 'transactions and isolation'\n")

        results = list(
            kb.aggregate(
                [
                    {
                        "$vectorSearch": {
                            "path": "embedding",
                            "queryVector": vectorizer.embed("transactions and isolation"),
                            "limit": 2,
                            "metric": "cosine",
                            "filter": {"source": {"$in": [f"chunk_{i}" for i in range(5)]}},
                        }
                    },
                    {"$project": {"text": 1, "_vectorScore": 1, "source": 1, "_id": 0}},
                ]
            )
        )

        for r in results:
            print(f"   [{r['_vectorScore']:.4f}] ({r['source']}) {r['text'][:70]}...")

        # ── 6. RAG prompt assembly ─────────────────────────────
        print("\n5. Assembling RAG prompt...\n")

        user_question = "What makes smongo different from a regular MongoDB?"
        q_vec = vectorizer.embed(user_question)

        context_docs = list(
            kb.aggregate(
                [
                    {
                        "$vectorSearch": {
                            "path": "embedding",
                            "queryVector": q_vec,
                            "limit": 3,
                            "metric": "cosine",
                        }
                    },
                    {"$project": {"text": 1, "_id": 0}},
                ]
            )
        )

        context_block = "\n".join(f"  - {d['text']}" for d in context_docs)
        prompt = (
            f"Answer the user's question using ONLY the context below.\n\n"
            f"Context:\n{context_block}\n\n"
            f"Question: {user_question}\n"
            f"Answer:"
        )

        print(f"   Question: {user_question}\n")
        print("   Assembled prompt (ready for any LLM):\n")
        for line in prompt.split("\n"):
            print(f"     {line}")

        print("\n   Every query above used standard PyMongo .aggregate().")
        print("   smongo executed $vectorSearch transparently over the wire.\n")

        client.close()

    shutil.rmtree(db_path, ignore_errors=True)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
