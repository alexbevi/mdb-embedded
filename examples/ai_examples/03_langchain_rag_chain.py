#!/usr/bin/env python3
"""
03_langchain_rag_chain.py -- Official LangChain MongoDBAtlasVectorSearch, locally.

Starts smongo's embedded wire server, then uses the OFFICIAL LangChain
MongoDBAtlasVectorSearch class with a standard PyMongo connection.
LangChain has zero idea it's not talking to Atlas.

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
from langchain_core.embeddings import Embeddings

from smongo import MongoClient as SmongoClient
from smongo import WireServer

PORT = 27020


# ── Fast local embeddings (no model download) ─────────────────
class LocalEmbeddings(Embeddings):
    """Deterministic hash-based embeddings so the demo runs instantly.
    Swap for OpenAIEmbeddings or HuggingFaceEmbeddings in production."""

    DIM = 64

    def _embed(self, text: str) -> list[float]:
        np.random.seed(abs(hash(text)) % (2**32))
        vec = np.random.rand(self.DIM).astype(np.float32)
        return (vec / np.linalg.norm(vec)).tolist()

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text)


def main() -> None:
    try:
        from langchain_mongodb import MongoDBAtlasVectorSearch
    except ImportError:
        print("Install deps:  pip install langchain-mongodb langchain-core pymongo numpy")
        return

    db_path = tempfile.mkdtemp(prefix="smongo_lc_official_")

    # ── 1. Seed documents + embeddings via native smongo ───────
    print("── Official LangChain MongoDBAtlasVectorSearch + smongo ──\n")
    print("1. Seeding knowledge base via native smongo client...")

    embeddings = LocalEmbeddings()

    texts = [
        "LangChain is a framework for developing applications powered by language models.",
        "smongo is an embedded MongoDB engine that runs locally on WiredTiger B-trees.",
        "Vector search finds semantically similar documents using cosine similarity.",
        "Retrieval-Augmented Generation grounds LLM answers in real data from a knowledge base.",
        "Agents use LLMs to decide what actions to take and which tools to call.",
        "MongoDB Atlas provides a fully managed cloud database service with vector search.",
        "The wire protocol lets any MongoDB driver connect to smongo over TCP.",
        "WiredTiger provides MVCC concurrency and crash-safe storage with WAL journaling.",
    ]

    native = SmongoClient(f"local://{db_path}")
    coll = native["langchain_db"]["vectors"]
    coll.insert_many(
        [
            {"text": t, "embedding": embeddings.embed_documents([t])[0], "source": f"doc_{i}"}
            for i, t in enumerate(texts)
        ]
    )
    print(
        f"   Stored {coll.count_documents({})} documents with {LocalEmbeddings.DIM}-dim embeddings."
    )
    native.close()

    # ── 2. Start the wire server ───────────────────────────────
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
        pymongo_coll = client["langchain_db"]["vectors"]

        # ── 4. Use the OFFICIAL LangChain class -- no wrappers! ─
        print("3. Using official MongoDBAtlasVectorSearch (zero custom code)...\n")

        vectorstore = MongoDBAtlasVectorSearch(
            collection=pymongo_coll,
            embedding=embeddings,
            index_name="default",
            text_key="text",
            embedding_key="embedding",
            relevance_score_fn="cosine",
        )

        # ── 5. similarity_search_with_score -- the real deal ───
        queries = [
            "How do AI agents work?",
            "What is RAG and how does it help?",
            "Tell me about embedded databases",
        ]

        for query in queries:
            print(f'   Query: "{query}"')
            results = vectorstore.similarity_search_with_score(query, k=2)
            for doc, score in results:
                print(f"     [{score:.4f}] {doc.page_content[:70]}...")
            print()

        # ── 6. Use as a LangChain retriever ────────────────────
        print("4. Using as a LangChain retriever (for RAG chains)...\n")

        retriever = vectorstore.as_retriever(search_kwargs={"k": 3})
        docs = retriever.invoke("What makes smongo special for AI?")

        for doc in docs:
            print(f"   -> {doc.page_content[:75]}...")

        print(f"\n   Retrieved {len(docs)} context documents, ready to feed to any LLM.")

        # ── 7. Build a RAG prompt ──────────────────────────────
        print("\n5. Assembling RAG prompt...\n")

        from langchain_core.prompts import ChatPromptTemplate

        context = "\n".join(f"  - {d.page_content}" for d in docs)
        user_question = "What makes smongo special for AI applications?"

        rag_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", "Answer using ONLY this context:\n{context}"),
                ("human", "{question}"),
            ]
        )

        formatted = rag_prompt.format(context=context, question=user_question)
        for line in formatted.split("\n"):
            print(f"   {line}")

        print("\n   ────────────────────────────────────────────────")
        print("   This used the OFFICIAL MongoDBAtlasVectorSearch class.")
        print("   Zero custom code. Zero wrappers. Just a connection string.")
        print("   LangChain had no idea smongo was the engine.\n")

        client.close()

    shutil.rmtree(db_path, ignore_errors=True)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
