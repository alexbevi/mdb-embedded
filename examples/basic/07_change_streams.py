#!/usr/bin/env python3
"""
07_change_streams.py -- Real-time mutation tracking with the oplog.

Shows how to use collection.watch() to observe inserts, updates, and
deletes as they happen, plus how to inspect the raw oplog for audit
trails. All fully local -- no MongoDB server required.

Run:
    python examples/basic/07_change_streams.py
"""

import shutil
import tempfile
import threading
import time

from smongo import MongoClient


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_watch_")
    client = MongoClient(f"local://{db_path}")
    db = client["events_demo"]
    orders = db["orders"]

    try:
        _run(orders)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def _run(orders) -> None:
    # ── Set up a change stream listener ───────────────────────
    print("── starting change stream on 'orders' ──")
    events: list[dict] = []
    stop = threading.Event()

    def listener():
        with orders.watch() as stream:
            for event in stream:
                events.append(event)
                if stop.is_set():
                    break

    t = threading.Thread(target=listener, daemon=True)
    t.start()
    time.sleep(0.1)
    print("  listener running in background thread\n")

    # ── Perform mutations ─────────────────────────────────────
    print("── performing mutations ──")

    orders.insert_one({"_id": "ORD-001", "customer": "Alice", "total": 99.50, "status": "pending"})
    print("  inserted ORD-001")
    time.sleep(0.05)

    orders.insert_one({"_id": "ORD-002", "customer": "Bob", "total": 250.00, "status": "pending"})
    print("  inserted ORD-002")
    time.sleep(0.05)

    orders.update_one({"_id": "ORD-001"}, {"$set": {"status": "shipped"}})
    print("  updated ORD-001 -> shipped")
    time.sleep(0.05)

    orders.delete_one({"_id": "ORD-002"})
    print("  deleted ORD-002")
    time.sleep(0.1)

    # ── Stop listener and show captured events ────────────────
    stop.set()
    orders.insert_one({"_id": "_trigger", "x": 1})
    t.join(timeout=2)
    orders.delete_one({"_id": "_trigger"})

    print()
    print(f"── change stream captured {len(events)} events ──")
    for ev in events:
        op = ev.get("operationType", "?")
        doc_key = ev.get("documentKey", {}).get("_id", "?")
        if op == "insert":
            customer = ev.get("fullDocument", {}).get("customer", "?")
            print(f"  {op:8s}  _id={doc_key}  customer={customer}")
        elif op == "update":
            fields = ev.get("updateDescription", {}).get("updatedFields", {})
            print(f"  {op:8s}  _id={doc_key}  changed={fields}")
        elif op == "delete":
            print(f"  {op:8s}  _id={doc_key}")
        else:
            print(f"  {op:8s}  _id={doc_key}")

    # ── Inspect the raw oplog ─────────────────────────────────
    print()
    print("── raw oplog entries (last 5) ──")
    entries = orders.get_oplog()
    recent = entries[-5:] if len(entries) > 5 else entries
    for entry in recent:
        ts = entry.get("ts", 0)
        op = entry.get("op", "?")
        doc_id = entry.get("doc_id", "?")
        changed = entry.get("changed_fields", [])
        print(f"  ts={ts:.3f}  op={op:7s}  doc_id={doc_id}  changed_fields={changed}")

    print()


if __name__ == "__main__":
    main()
