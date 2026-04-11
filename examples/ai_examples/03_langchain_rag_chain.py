#!/usr/bin/env python3
"""
03_langchain_rag_chain.py -- Official LangChain MongoDBAtlasVectorSearch, locally.

Starts smongo's embedded wire server, then uses the OFFICIAL LangChain
MongoDBAtlasVectorSearch class with a standard PyMongo connection.
LangChain has zero idea it's not talking to Atlas.

Demonstrates:
  - similarity_search_with_score (scored retrieval with visual bars)
  - Metadata filtering via pre_filter
  - add_documents (LangChain manages inserts + embeddings)
  - as_retriever for RAG chain assembly
  - Full RAG prompt construction

No custom vectorstore, no wrapper, no adapter -- just a connection string.

Requirements:
    pip install langchain-mongodb langchain-core pymongo numpy smongo

Run:
    python examples/ai_examples/03_langchain_rag_chain.py
"""

import os
import shutil
import sys
import tempfile
import time

import numpy as np
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

from smongo import MongoClient as SmongoClient
from smongo import WireServer

PORT = 27020
DIM = 64


def separator(char="─", width=60):
    return char * width


class LocalEmbeddings(Embeddings):
    """Deterministic hash-based embeddings so the demo runs instantly.
    Swap for OpenAIEmbeddings or OllamaEmbeddings in production."""

    def _embed(self, text: str) -> list[float]:
        np.random.seed(abs(hash(text)) % (2**32))
        vec = np.random.rand(DIM).astype(np.float32)
        return (vec / np.linalg.norm(vec)).tolist()

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text)


KNOWLEDGE = [
    {
        "text": (
            "LangChain is a framework for developing applications powered "
            "by language models, with composable chains and agents."
        ),
        "category": "framework",
        "source": "langchain-docs",
    },
    {
        "text": (
            "smongo is an embedded MongoDB engine that runs locally using "
            "redb storage. No server, no Docker — just import and go."
        ),
        "category": "database",
        "source": "smongo-readme",
    },
    {
        "text": (
            "Vector search finds semantically similar documents using "
            "cosine similarity with a vendored HNSW index for approximate "
            "nearest-neighbor search."
        ),
        "category": "search",
        "source": "smongo-docs",
    },
    {
        "text": (
            "Retrieval-Augmented Generation (RAG) grounds LLM answers "
            "in real data from a knowledge base, reducing hallucinations."
        ),
        "category": "technique",
        "source": "langchain-docs",
    },
    {
        "text": (
            "Agents use LLMs to decide what actions to take and which "
            "tools to call, enabling autonomous multi-step reasoning."
        ),
        "category": "technique",
        "source": "langchain-docs",
    },
    {
        "text": (
            "MongoDB Atlas provides a fully managed cloud database service "
            "with built-in vector search and real-time sync."
        ),
        "category": "database",
        "source": "atlas-docs",
    },
    {
        "text": (
            "The wire protocol lets any MongoDB driver connect to smongo "
            "over TCP — PyMongo, mongosh, Compass all work unchanged."
        ),
        "category": "protocol",
        "source": "smongo-docs",
    },
    {
        "text": (
            "The smongo engine provides MVCC-style concurrency, durable "
            "on-disk storage, and full ACID transactions with snapshot "
            "isolation."
        ),
        "category": "database",
        "source": "smongo-docs",
    },
    {
        "text": (
            "LangChain's MongoDBAtlasVectorSearch integration manages "
            "index creation, document insertion, and similarity search "
            "through the official PyMongo driver."
        ),
        "category": "integration",
        "source": "langchain-mongodb-docs",
    },
    {
        "text": (
            "Embedding models convert text into dense vector "
            "representations that capture semantic meaning, enabling "
            "similarity-based retrieval."
        ),
        "category": "technique",
        "source": "ml-textbook",
    },
]


def main() -> None:
    try:
        from langchain_mongodb import MongoDBAtlasVectorSearch
    except ImportError:
        print("Install deps:  pip install langchain-mongodb langchain-core " "pymongo numpy")
        return

    db_path = tempfile.mkdtemp(prefix="smongo_lc_official_")

    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Official LangChain MongoDBAtlasVectorSearch + smongo  ║")
    print("║   Zero wrappers. Zero custom code. Just a conn string.  ║")
    print("╚══════════════════════════════════════════════════════════╝\n")

    # ── 1. Seed documents + embeddings via native smongo ──────
    print("1. Seeding knowledge base via native smongo client...")

    embeddings = LocalEmbeddings()

    native = SmongoClient(f"local://{db_path}")
    coll = native["langchain_db"]["vectors"]
    coll.insert_many(
        [
            {
                "text": doc["text"],
                "embedding": embeddings.embed_documents([doc["text"]])[0],
                "source": doc["source"],
                "category": doc["category"],
            }
            for doc in KNOWLEDGE
        ]
    )
    print(f"   Stored {coll.count_documents({})} documents " f"with {DIM}-dim embeddings")

    coll.create_index(
        {"embedding": "vectorSearch"},
        vectorSearchOptions={"dimensions": DIM, "metric": "cosine"},
        name="default",
        type="vectorSearch",
    )
    print("   Created vector search index 'default' (cosine, HNSW)\n")

    # ── 2. Start the wire server ──────────────────────────────
    print(f"2. Starting wire protocol server on port {PORT}...")

    with WireServer(db_path, port=PORT, local_client=native.get_local_client()) as _srv:
        time.sleep(0.3)

        from pymongo import MongoClient as PyMongoClient

        client = PyMongoClient(
            f"mongodb://localhost:{PORT}",
            serverSelectionTimeoutMS=5000,
            directConnection=True,
        )
        pymongo_coll = client["langchain_db"]["vectors"]

        # ── 3. Official LangChain vectorstore ─────────────────
        print("3. Using official MongoDBAtlasVectorSearch " "(zero custom code)...\n")

        vectorstore = MongoDBAtlasVectorSearch(
            collection=pymongo_coll,
            embedding=embeddings,
            index_name="default",
            text_key="text",
            embedding_key="embedding",
        )

        # ── 4. Scored similarity search ───────────────────────
        print(f"   {separator()}")
        print("   SIMILARITY SEARCH WITH SCORES")
        print(f"   {separator()}\n")

        queries = [
            "How do AI agents work?",
            "What is RAG and how does it help?",
            "Tell me about embedded databases",
        ]

        for query in queries:
            print(f'   Q: "{query}"')
            t0 = time.time()
            results = vectorstore.similarity_search_with_score(query, k=3)
            ms = (time.time() - t0) * 1000

            for rank, (doc, score) in enumerate(results, 1):
                score_str = f"{score:.4f}" if score is not None else "n/a"
                bar = "█" * int(score * 25) if score else ""
                snippet = doc.page_content[:65].replace("\n", " ")
                print(f"      {rank}. [{score_str}] {bar}")
                print(f"         {snippet}...")
            print(f"      ({ms:.0f}ms)\n")

        # ── 5. Filtered search by metadata ────────────────────
        print(f"   {separator()}")
        print("   FILTERED SEARCH (by category)")
        print(f"   {separator()}\n")

        filter_query = "How does smongo store data?"
        print(f'   Q: "{filter_query}"')
        print('   Filter: category = "database"\n')

        t0 = time.time()
        filtered = vectorstore.similarity_search_with_score(
            filter_query,
            k=3,
            pre_filter={"category": "database"},
        )
        ms = (time.time() - t0) * 1000

        for rank, (doc, score) in enumerate(filtered, 1):
            score_str = f"{score:.4f}" if score is not None else "n/a"
            cat = doc.metadata.get("category", "?")
            snippet = doc.page_content[:60].replace("\n", " ")
            print(f"      {rank}. [{score_str}] ({cat}) {snippet}...")
        print(f"      ({ms:.0f}ms)\n")

        # ── 6. Add documents via LangChain ────────────────────
        print(f"   {separator()}")
        print("   ADD DOCUMENTS (LangChain manages insert + embedding)")
        print(f"   {separator()}\n")

        new_docs = [
            Document(
                page_content=(
                    "smongo's aggregation pipeline supports 25+ stages "
                    "including $lookup joins, $graphLookup, $facet, and "
                    "$setWindowFields."
                ),
                metadata={"category": "features", "source": "smongo-docs"},
            ),
            Document(
                page_content=(
                    "Atlas sync pushes local writes to MongoDB Atlas and "
                    "pulls remote changes with conflict resolution via "
                    "per-document vector clocks."
                ),
                metadata={"category": "sync", "source": "smongo-docs"},
            ),
        ]

        t0 = time.time()
        vectorstore.add_documents(new_docs)
        ms = (time.time() - t0) * 1000
        total = pymongo_coll.count_documents({})
        print(f"   Added {len(new_docs)} documents via LangChain " f"({ms:.0f}ms)")
        print(f"   Total documents in collection: {total}\n")

        t0 = time.time()
        results = vectorstore.similarity_search_with_score(
            "What aggregation stages does smongo support?", k=2
        )
        ms = (time.time() - t0) * 1000
        print('   Q: "What aggregation stages does smongo support?"')
        for rank, (doc, score) in enumerate(results, 1):
            score_str = f"{score:.4f}" if score is not None else "n/a"
            snippet = doc.page_content[:65].replace("\n", " ")
            print(f"      {rank}. [{score_str}] {snippet}...")
        print(f"      ({ms:.0f}ms)\n")

        # ── 7. LangChain retriever ────────────────────────────
        print(f"   {separator()}")
        print("   LANGCHAIN RETRIEVER (for RAG chains)")
        print(f"   {separator()}\n")

        retriever = vectorstore.as_retriever(search_kwargs={"k": 3})

        q = "What makes smongo special for AI applications?"
        print(f'   Q: "{q}"\n')

        t0 = time.time()
        docs = retriever.invoke(q)
        ms = (time.time() - t0) * 1000

        for i, doc in enumerate(docs, 1):
            src = doc.metadata.get("source", "?")
            print(f"      {i}. [{src}] {doc.page_content[:70]}...")
        print(f"\n   Retrieved {len(docs)} context documents in {ms:.0f}ms\n")

        # ── 8. RAG prompt assembly ────────────────────────────
        print(f"   {separator()}")
        print("   RAG PROMPT ASSEMBLY")
        print(f"   {separator()}\n")

        from langchain_core.prompts import ChatPromptTemplate

        context = "\n".join(f"  - {d.page_content}" for d in docs)
        user_question = "What makes smongo special for AI applications?"

        rag_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "Answer using ONLY this context:\n{context}",
                ),
                ("human", "{question}"),
            ]
        )

        formatted = rag_prompt.format(context=context, question=user_question)
        for line in formatted.split("\n"):
            print(f"   {line}")

        print(f"\n   {separator('═')}")
        print("   This used the OFFICIAL MongoDBAtlasVectorSearch class.")
        print("   Zero custom code. Zero wrappers. Just a connection string.")
        print("   LangChain had no idea smongo was the engine.")
        print(f"   {separator('═')}\n")

        client.close()

    native.close()
    shutil.rmtree(db_path, ignore_errors=True)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
