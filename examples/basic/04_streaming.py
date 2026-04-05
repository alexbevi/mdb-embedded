#!/usr/bin/env python3
"""
04_streaming.py -- Lazy reads and the streaming architecture.

Demonstrates how smongo's read path avoids materializing documents you
never need: find_one() deserializes exactly one document, count_documents()
never builds a list, and find().limit(N) only decodes N BSON blobs from
WiredTiger.

Run:
    python examples/basic/04_streaming.py
"""

import shutil
import tempfile
import time

from smongo import MongoClient


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_stream_")
    client = MongoClient(f"local://{db_path}")
    db = client["streaming_demo"]
    sensors = db["sensor_readings"]

    try:
        _run(sensors)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def _run(sensors) -> None:
    # ── Seed 2,000 documents ──────────────────────────────────
    print("── seeding 2,000 sensor readings ──")
    docs = [
        {
            "_id": f"r_{i:05d}",
            "sensor": f"sensor_{i % 20:02d}",
            "temp_c": 20.0 + (i % 30) * 0.5,
            "humidity": 40 + (i % 40),
            "location": ["floor_1", "floor_2", "floor_3", "roof"][i % 4],
        }
        for i in range(2_000)
    ]
    sensors.insert_many(docs)
    sensors.create_index([("sensor", 1)])
    sensors.create_index([("location", 1)])
    sensors.create_index([("temp_c", 1)])
    print(f"  inserted {sensors.count_documents({}):,} docs with 3 indexes\n")

    # ── find_one: only 1 document deserialized ────────────────
    print("── find_one (single doc from WiredTiger) ──")
    t0 = time.perf_counter()
    doc = sensors.find_one({"sensor": "sensor_07"})
    elapsed = (time.perf_counter() - t0) * 1000
    print(f"  found: {doc['_id']}  sensor={doc['sensor']}  temp={doc['temp_c']}C")
    print(f"  time: {elapsed:.2f}ms (1 BSON decode)\n")

    # ── count_documents: no list, no BSON decode ──────────────
    print("── count_documents (no intermediate list) ──")
    t0 = time.perf_counter()
    total = sensors.count_documents({})
    elapsed = (time.perf_counter() - t0) * 1000
    print(f"  total readings: {total:,}")
    print(f"  time: {elapsed:.2f}ms (cursor walk only, no BSON decode)\n")

    t0 = time.perf_counter()
    hot_count = sensors.count_documents({"temp_c": {"$gte": 30.0}})
    elapsed = (time.perf_counter() - t0) * 1000
    print(f"  hot readings (>=30C): {hot_count:,}")
    print(f"  time: {elapsed:.2f}ms (streaming count with filter)\n")

    # ── find().limit(10): only 10 BSON decodes ────────────────
    print("── find().limit(10) -- lazy cursor stops early ──")
    t0 = time.perf_counter()
    top_10 = sensors.find({"location": "roof"}).limit(10).to_list()
    elapsed_lazy = (time.perf_counter() - t0) * 1000
    print(f"  got {len(top_10)} roof readings in {elapsed_lazy:.2f}ms")
    for d in top_10[:3]:
        print(f"    {d['_id']}  temp={d['temp_c']}C  humidity={d['humidity']}%")
    print(f"    ... ({len(top_10) - 3} more)\n")

    print("All reads were lazy -- no unnecessary BSON materialization.")

    print()


if __name__ == "__main__":
    main()
