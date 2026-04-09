#!/usr/bin/env python3
"""
05_fully_local_rag.py -- 100% local RAG: Ollama + LangChain + smongo.

Local embeddings (nomic-embed-text), local LLM (llama3.2), local vector
database (smongo over the wire protocol). Zero API keys. Zero cloud.
LangChain uses its official MongoDB integration -- smongo is invisible.

Prerequisites:
    brew install ollama
    ollama pull nomic-embed-text
    ollama pull llama3.2
    pip install langchain-ollama langchain-mongodb pymongo smongo

Run:
    python examples/ai_examples/05_fully_local_rag.py
"""

import os
import shutil
import sys
import tempfile
import time

from smongo import MongoClient as SmongoClient
from smongo import WireServer

PORT = 27023

KNOWLEDGE = [
    "smongo is an embedded MongoDB engine that runs entirely in-process on redb. No server process, no Docker, no network -- just import and go.",
    "The wire protocol server lets any standard MongoDB driver (PyMongo, mongosh, Compass) connect to smongo over TCP. Clients have no idea they're talking to an embedded engine.",
    "smongo supports $vectorSearch as a native aggregation stage. It performs in-memory cosine or euclidean similarity search using NumPy or USearch, with optional MQL pre-filtering.",
    "Full ACID transactions with snapshot isolation are supported across multiple collections via the embedded engine.",
    "The aggregation pipeline supports 25+ stages including $lookup joins, $graphLookup, $facet for parallel sub-pipelines, $setWindowFields, and $merge for materialized views.",
    "Atlas sync pushes local writes to MongoDB Atlas and pulls remote changes back, with per-document vector clocks for causal ordering and automatic conflict resolution.",
    "The query planner uses heuristic prefix-scoring to automatically select B-tree indexes. It supports compound, unique, sparse, TTL, text, hashed, wildcard, and partial indexes.",
    "The Rust core eliminates ~50 Python method dispatches per command by using typed PyO3 borrow() calls instead of call_method(). The GIL is still acquired but held for actual work only.",
]


def main() -> None:
    try:
        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.prompts import ChatPromptTemplate
        from langchain_mongodb import MongoDBAtlasVectorSearch
        from langchain_ollama import ChatOllama, OllamaEmbeddings
    except ImportError:
        print("Install deps:  pip install langchain-ollama langchain-mongodb pymongo smongo")
        return

    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Fully Local RAG: Ollama + LangChain + smongo          ║")
    print("║   Zero API keys. Zero cloud. 100% on your machine.      ║")
    print("╚══════════════════════════════════════════════════════════╝\n")

    # ── 1. Local models via Ollama ─────────────────────────────
    print("1. Loading local models from Ollama...")
    embeddings = OllamaEmbeddings(model="nomic-embed-text")
    llm = ChatOllama(model="llama3.2", temperature=0)
    print("   Embedding model: nomic-embed-text (137M params)")
    print("   LLM: llama3.2 (3B params)\n")

    # ── 2. Seed knowledge base via native smongo ───────────────
    print("2. Building knowledge base with real embeddings...")
    db_path = tempfile.mkdtemp(prefix="smongo_local_rag_")

    native = SmongoClient(f"local://{db_path}")
    coll = native["rag"]["knowledge"]

    t0 = time.time()
    embedded_docs = []
    for i, text in enumerate(KNOWLEDGE):
        vec = embeddings.embed_documents([text])[0]
        embedded_docs.append({"text": text, "embedding": vec, "chunk_id": i})
        print(f"   [{i+1}/{len(KNOWLEDGE)}] Embedded ({len(vec)} dims)")

    coll.insert_many(embedded_docs)
    embed_time = time.time() - t0
    print(f"   Stored {len(KNOWLEDGE)} chunks in {embed_time:.1f}s\n")
    native.close()

    # ── 3. Start wire server ───────────────────────────────────
    print(f"3. Starting wire protocol server on port {PORT}...")

    with WireServer(db_path, port=PORT) as _srv:
        time.sleep(0.3)

        from pymongo import MongoClient as PyMongoClient

        client = PyMongoClient(
            f"mongodb://localhost:{PORT}",
            serverSelectionTimeoutMS=5000,
            directConnection=True,
        )
        pymongo_coll = client["rag"]["knowledge"]

        # ── 4. Official LangChain MongoDB VectorStore ──────────
        print("4. Connecting official MongoDBAtlasVectorSearch...\n")

        vectorstore = MongoDBAtlasVectorSearch(
            collection=pymongo_coll,
            embedding=embeddings,
            index_name="default",
            text_key="text",
            embedding_key="embedding",
        )
        retriever = vectorstore.as_retriever(search_kwargs={"k": 3})

        # ── 5. RAG chain ──────────────────────────────────────
        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "You are a helpful assistant. Answer the user's question using "
                    "ONLY the context below. Be concise (2-3 sentences). If the "
                    "context doesn't contain the answer, say so.\n\n"
                    "Context:\n{context}",
                ),
                ("human", "{question}"),
            ]
        )

        chain = prompt | llm | StrOutputParser()

        questions = [
            "What is smongo and how is it different from MongoDB?",
            "How does vector search work in smongo?",
            "Can I connect standard MongoDB tools to smongo?",
        ]

        for q in questions:
            print(f"   Q: {q}")

            t0 = time.time()
            docs = retriever.invoke(q)
            retrieve_time = time.time() - t0

            context = "\n".join(f"- {d.page_content}" for d in docs)

            t0 = time.time()
            answer = chain.invoke({"context": context, "question": q})
            llm_time = time.time() - t0

            print(f"   A: {answer}")
            print(f"   (retrieval: {retrieve_time:.2f}s, generation: {llm_time:.2f}s)\n")

        print("   ────────────────────────────────────────────────────")
        print("   Every component ran locally on your machine:")
        print("     Embeddings:  Ollama nomic-embed-text")
        print("     Vector DB:   smongo (via official MongoDBAtlasVectorSearch)")
        print("     LLM:         Ollama llama3.2")
        print("     Framework:   LangChain (official integrations, zero custom code)")
        print("   ────────────────────────────────────────────────────\n")

        client.close()

    shutil.rmtree(db_path, ignore_errors=True)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
