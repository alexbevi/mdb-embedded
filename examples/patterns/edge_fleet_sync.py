#!/usr/bin/env python3
"""
edge_fleet_sync.py -- Edge fleet sync with MQL-native sync rules.

Simulates three IoT edge devices (sensor nodes), each running its own
smongo engine locally, syncing to a central "Atlas" (a local mongod).
Each device uses MQL sync rules to scope what it pushes and pulls:

    sync_rules = {"device_id": "$$NODE_ID"}

The same MQL you already know controls which documents sync -- no
separate DSL, no translation layer.

Prerequisites:
    pip install pymongo smongo
    # A local MongoDB must be running (or use docker-compose)

Run:
    python examples/patterns/edge_fleet_sync.py

Environment:
    MONGO_URI  -- connection string (default: mongodb://localhost:27017)
"""

import math
import os
import shutil
import tempfile
import time
from datetime import UTC, datetime, timedelta

from smongo import MongoClient, SyncManager

CENTRAL_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "edge_fleet_demo"

DEVICES = [
    {"node_id": "sensor-north-001", "facility": "plant_north", "base_temp": 20.0},
    {"node_id": "sensor-south-001", "facility": "plant_south", "base_temp": 23.0},
    {"node_id": "sensor-east-001", "facility": "warehouse_east", "base_temp": 18.0},
]


def generate_readings(device: dict, hours: int = 6) -> list[dict]:
    """Generate simulated sensor readings for a device."""
    readings = []
    base = datetime(2026, 4, 5, 6, 0, 0, tzinfo=UTC)
    for hour in range(hours):
        for minute in [0, 15, 30, 45]:
            ts = base + timedelta(hours=hour, minutes=minute)
            diurnal = 2.0 * math.sin((hour + 6) * math.pi / 12)
            noise = (hash(f"{device['node_id']}_{hour}_{minute}") % 100 - 50) / 100.0
            readings.append(
                {
                    "device_id": device["node_id"],
                    "facility": device["facility"],
                    "timestamp": ts.isoformat(),
                    "temp_c": round(device["base_temp"] + diurnal + noise, 2),
                    "humidity_pct": round(50 + (hash(f"h_{hour}_{minute}") % 20 - 10), 1),
                    "_lastModified": time.time(),
                }
            )
    return readings


def main() -> None:
    from pymongo import MongoClient as PyMongoClient

    print("=" * 64)
    print("  Edge Fleet Sync -- MQL-native sync rules")
    print("  3 devices, 1 central hub, same MQL everywhere")
    print("=" * 64)

    central = PyMongoClient(CENTRAL_URI)
    central[DB_NAME].drop_collection("readings")
    central[DB_NAME].drop_collection("fleet_config")

    tmp_dirs: list[str] = []
    managers: list[SyncManager] = []
    clients: list[MongoClient] = []

    try:
        # -- 1. Create edge devices ----------------------------------------
        print("\n1. Spinning up edge devices...\n")

        for dev in DEVICES:
            tmp = tempfile.mkdtemp(prefix=f"smongo_{dev['node_id']}_")
            tmp_dirs.append(tmp)

            client = MongoClient(f"local://{tmp}")
            clients.append(client)

            mgr = SyncManager(
                client,
                CENTRAL_URI,
                sync_config={
                    "mode": "bidirectional",
                    "sync_rules": {"device_id": "$$NODE_ID"},
                    "node_id": dev["node_id"],
                    "use_change_stream_pull": False,
                    "oplog_auto_compact": False,
                },
            )
            managers.append(mgr)

            coll = client[DB_NAME]["readings"]
            mgr.register_collection(DB_NAME, "readings", coll.backend)

            print(f"   {dev['node_id']:25s}  facility={dev['facility']}")

        # -- 2. Generate and insert local readings --------------------------
        print("\n2. Each device writes sensor data locally...\n")

        for i, dev in enumerate(DEVICES):
            readings = generate_readings(dev)
            coll = clients[i][DB_NAME]["readings"]
            coll.insert_many(readings)
            print(f"   {dev['node_id']:25s}  {len(readings)} readings written locally")

        # -- 3. Push to central ---------------------------------------------
        print("\n3. Pushing to central hub (each device only syncs its own data)...\n")

        for i, dev in enumerate(DEVICES):
            managers[i].push()
            st = managers[i].status()
            print(f"   {dev['node_id']:25s}  pushed={st['pushed']}")

        remote_total = central[DB_NAME]["readings"].count_documents({})
        print(f"\n   Central hub total: {remote_total} readings")

        for dev in DEVICES:
            count = central[DB_NAME]["readings"].count_documents({"device_id": dev["node_id"]})
            print(f"   {dev['node_id']:25s}  {count} docs on central")

        # -- 4. Verify isolation: pull doesn't cross-pollinate ---------------
        print("\n4. Pulling from central (each device only gets its own data)...\n")

        for i, dev in enumerate(DEVICES):
            managers[i].pull()
            local_count = clients[i][DB_NAME]["readings"].count_documents({})
            other_count = clients[i][DB_NAME]["readings"].count_documents(
                {"device_id": {"$ne": dev["node_id"]}}
            )
            print(
                f"   {dev['node_id']:25s}  local={local_count}  "
                f"foreign={other_count} (should be 0)"
            )

        # -- 5. Time-windowed sync example ----------------------------------
        print("\n5. Time-windowed sync demo...\n")

        central[DB_NAME]["events"].drop()
        now = time.time()
        central[DB_NAME]["events"].insert_many(
            [
                {"_id": "old_event", "msg": "ancient", "_lastModified": now - 86400 * 30},
                {"_id": "new_event", "msg": "recent", "_lastModified": now - 3600},
            ]
        )

        tw_tmp = tempfile.mkdtemp(prefix="smongo_timewin_")
        tmp_dirs.append(tw_tmp)
        tw_client = MongoClient(f"local://{tw_tmp}")
        clients.append(tw_client)

        tw_mgr = SyncManager(
            tw_client,
            CENTRAL_URI,
            sync_config={
                "mode": "pull_only",
                "sync_rules": {"_lastModified": {"$gt": "$$WINDOW_START"}},
                "variables": {"WINDOW_START": now - 86400 * 7},
                "use_change_stream_pull": False,
            },
        )
        managers.append(tw_mgr)

        tw_coll = tw_client[DB_NAME]["events"]
        tw_mgr.register_collection(DB_NAME, "events", tw_coll.backend)
        tw_mgr.pull()

        pulled_recent = tw_coll.find_one({"_id": "new_event"})
        pulled_old = tw_coll.find_one({"_id": "old_event"})
        print(f"   Recent event pulled: {'yes' if pulled_recent else 'no'}")
        print(f"   Old event pulled:    {'yes' if pulled_old else 'no'} (excluded by time window)")

        # -- 6. Summary -----------------------------------------------------
        print("\n" + "=" * 64)
        print("  Summary")
        print("=" * 64)
        print(f"\n  Devices:           {len(DEVICES)}")
        print(f"  Central hub docs:  {remote_total}")
        print('  Sync rules:        {{"device_id": "$$NODE_ID"}}')
        print('  Time-window rule:  {{"_lastModified": {{"$gt": "$$WINDOW_START"}}}}')
        print("\n  Each device wrote locally, pushed only its own data,")
        print("  and pulled only its own data back. Same MQL everywhere.")
        print("  No separate sync DSL. No translation layer.\n")

    finally:
        for mgr in managers:
            mgr.stop()
        for c in clients:
            try:
                c.close()
            except Exception:
                pass
        for d in tmp_dirs:
            shutil.rmtree(d, ignore_errors=True)
        central[DB_NAME].drop_collection("readings")
        central[DB_NAME].drop_collection("fleet_config")
        central[DB_NAME].drop_collection("events")
        central.close()


if __name__ == "__main__":
    main()
