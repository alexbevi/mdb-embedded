#!/usr/bin/env python3
"""
iot_timeseries.py -- IoT sensor data with time-window analytics.

Simulates 24 hours of temperature and humidity readings from sensors
across multiple facilities, then runs time-series analytics: hourly
averages, anomaly detection, facility comparison, and sensor health
monitoring.

Run:
    python examples/patterns/iot_timeseries.py
"""

import math
import shutil
import tempfile
from datetime import UTC, datetime, timedelta

from smongo import MongoClient


FACILITIES = ["plant_north", "plant_south", "warehouse"]
SENSORS_PER_FACILITY = 4


def main() -> None:
    db_path = tempfile.mkdtemp(prefix="smongo_iot_")
    client = MongoClient(f"local://{db_path}")
    db = client["iot"]

    try:
        seed(db)
        analytics(db)
    finally:
        client.close()
        shutil.rmtree(db_path, ignore_errors=True)


def seed(db) -> None:
    readings = db["readings"]

    print("── generating 24h of sensor data ──")
    base_time = datetime(2026, 3, 31, 0, 0, 0, tzinfo=UTC)
    docs = []

    for facility_idx, facility in enumerate(FACILITIES):
        for sensor_num in range(SENSORS_PER_FACILITY):
            sensor_id = f"{facility}_s{sensor_num:02d}"
            base_temp = 20.0 + facility_idx * 3
            base_humidity = 45 + facility_idx * 5

            for hour in range(24):
                for minute in [0, 15, 30, 45]:
                    ts = base_time + timedelta(hours=hour, minutes=minute)
                    diurnal = 3.0 * math.sin((hour - 6) * math.pi / 12)
                    noise = (hash(f"{sensor_id}_{hour}_{minute}") % 100 - 50) / 50.0
                    temp = round(base_temp + diurnal + noise, 1)
                    humidity = round(base_humidity + (hash(f"h_{sensor_id}_{hour}") % 20 - 10), 1)

                    is_anomaly = hour == 14 and sensor_num == 2 and facility == "plant_north"
                    if is_anomaly:
                        temp = round(temp + 15.0, 1)

                    docs.append({
                        "sensor_id": sensor_id,
                        "facility": facility,
                        "timestamp": ts.isoformat(),
                        "hour": hour,
                        "temp_c": temp,
                        "humidity_pct": humidity,
                    })

    readings.insert_many(docs)
    readings.create_index([("facility", 1), ("hour", 1)])
    readings.create_index([("sensor_id", 1)])
    readings.create_index([("temp_c", 1)])

    print(f"  {readings.count_documents({}):,} readings from "
          f"{len(FACILITIES)} facilities x {SENSORS_PER_FACILITY} sensors x 96 intervals\n")


def analytics(db) -> None:
    readings = db["readings"]

    # ── Hourly averages by facility ───────────────────────────
    print("── hourly temperature averages by facility (6am-noon) ──")
    results = readings.aggregate([
        {"$match": {"hour": {"$gte": 6, "$lte": 12}}},
        {"$group": {
            "_id": {"facility": "$facility", "hour": "$hour"},
            "avg_temp": {"$avg": "$temp_c"},
            "min_temp": {"$min": "$temp_c"},
            "max_temp": {"$max": "$temp_c"},
            "readings": {"$sum": 1},
        }},
        {"$sort": {"_id.facility": 1, "_id.hour": 1}},
    ])

    current_facility = None
    for r in results:
        fac = r["_id"]["facility"]
        if fac != current_facility:
            current_facility = fac
            print(f"\n  {fac}:")
        print(f"    {r['_id']['hour']:02d}:00  avg={r['avg_temp']:5.1f}C  "
              f"range=[{r['min_temp']:.1f}, {r['max_temp']:.1f}]  n={r['readings']}")

    # ── Anomaly detection: readings > 2 stddev from facility mean
    print("\n── anomaly detection: temp > facility mean + 10C ──")
    facility_stats = readings.aggregate([
        {"$group": {
            "_id": "$facility",
            "mean_temp": {"$avg": "$temp_c"},
        }},
    ])
    stats_map = {r["_id"]: r["mean_temp"] for r in facility_stats}

    for facility, mean in sorted(stats_map.items()):
        threshold = mean + 10.0
        anomalies = list(readings.find({
            "facility": facility,
            "temp_c": {"$gt": threshold},
        }))
        if anomalies:
            print(f"\n  {facility} (mean={mean:.1f}C, threshold={threshold:.1f}C):")
            for a in anomalies[:5]:
                print(f"    sensor={a['sensor_id']}  hour={a['hour']:02d}  "
                      f"temp={a['temp_c']}C  (+{a['temp_c'] - mean:.1f}C)")
        else:
            print(f"  {facility}: no anomalies (mean={mean:.1f}C)")

    # ── Facility comparison: daily stats ──────────────────────
    print("\n── facility comparison: 24h summary ──")
    results = readings.aggregate([
        {"$group": {
            "_id": "$facility",
            "avg_temp": {"$avg": "$temp_c"},
            "avg_humidity": {"$avg": "$humidity_pct"},
            "max_temp": {"$max": "$temp_c"},
            "min_temp": {"$min": "$temp_c"},
            "sensor_count": {"$addToSet": "$sensor_id"},
        }},
        {"$sort": {"avg_temp": 1}},
    ])
    for r in results:
        sensors = len(r.get("sensor_count", []))
        print(f"  {r['_id']:15s}  avg={r['avg_temp']:5.1f}C  "
              f"range=[{r['min_temp']:.1f}, {r['max_temp']:.1f}]  "
              f"humidity={r['avg_humidity']:.0f}%  sensors={sensors}")

    # ── Sensor health: readings per sensor ────────────────────
    print("\n── sensor health: expected 96 readings each ──")
    results = readings.aggregate([
        {"$group": {
            "_id": "$sensor_id",
            "count": {"$sum": 1},
        }},
        {"$match": {"count": {"$ne": 96}}},
        {"$sort": {"_id": 1}},
    ])
    missing = list(results)
    if missing:
        for r in missing:
            print(f"  WARNING: {r['_id']} has {r['count']} readings (expected 96)")
    else:
        print("  All sensors reported 96 readings -- no gaps detected")

    print()


if __name__ == "__main__":
    main()
