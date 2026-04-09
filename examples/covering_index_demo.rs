/// Covering Index Performance Demo
///
/// Shows the performance difference between regular index scans and covering index scans
/// for typical IoT sensor query patterns.
///
/// Run with: cargo run --example covering_index_demo --release

use bson::doc;
use smongo_engine::database::Database;
use smongo_engine::planner::{plan_query_with_projection, ExecutionPlan};
use std::time::Instant;

fn main() {
    println!("🚀 Covering Index Performance Demo\n");

    // Create in-memory database
    let db = Database::from_backend(
        smongo_engine::storage::MemBackend::new(),
        "iot_demo",
        None,
    );
    let sensors = db.collection("sensor_readings").unwrap();

    println!("📊 Setting up test data...");

    // Create compound index for covering queries
    sensors
        .create_index(
            doc! { "device_id": 1, "timestamp": 1, "temperature": 1, "humidity": 1 },
            None,
        )
        .unwrap();

    // Insert 10,000 sensor readings
    let num_docs = 10_000;
    for i in 0..num_docs {
        sensors
            .insert_one(doc! {
                "_id": format!("reading_{:05}", i),
                "device_id": format!("sensor_{:03}", i % 100),
                "timestamp": 1000000 + i * 60,  // 1-minute intervals
                "temperature": 20.0 + (i % 10) as f64,
                "humidity": 40.0 + (i % 20) as f64,
                "location": {
                    "floor": i % 5,
                    "room": format!("room_{}", i % 20)
                },
                "metadata": format!("Extra data that we don't need for most queries: {}", "x".repeat(100))
            })
            .unwrap();
    }

    println!("✅ Inserted {} sensor readings\n", num_docs);

    // Query 1: Covered query (only indexed fields in projection)
    println!("=== Query 1: COVERING INDEX ===");
    let query1 = doc! { "device_id": "sensor_042" };
    let projection1 = doc! { "timestamp": 1, "temperature": 1, "humidity": 1, "_id": 0 };

    let indexes = sensors.list_indexes().unwrap();
    let plan1 = plan_query_with_projection(&query1, &indexes, Some(&projection1));

    match &plan1.execution_plan {
        ExecutionPlan::CoveringIndexScan { .. } => {
            println!("✅ Using COVERING INDEX (no document fetch)");
        }
        _ => println!("❌ Not using covering index"),
    }

    let start = Instant::now();
    let results1 = sensors.execute_plan(&plan1.execution_plan, &query1).unwrap();
    let elapsed1 = start.elapsed();

    println!("Results: {} documents", results1.len());
    println!("Time: {:?}", elapsed1);
    println!("Estimated cost: {}", plan1.estimated_cost);
    println!("Reason: {}\n", plan1.reason);

    // Query 2: Non-covered query (needs fields not in index)
    println!("=== Query 2: REGULAR INDEX (with document fetch) ===");
    let query2 = doc! { "device_id": "sensor_042" };
    let projection2 = doc! { "timestamp": 1, "temperature": 1, "location": 1, "_id": 0 };

    let plan2 = plan_query_with_projection(&query2, &indexes, Some(&projection2));

    match &plan2.execution_plan {
        ExecutionPlan::CoveringIndexScan { .. } => {
            println!("✅ Using covering index");
        }
        _ => println!("❌ Using regular index (must fetch documents)"),
    }

    let start = Instant::now();
    let results2 = sensors.execute_plan(&plan2.execution_plan, &query2).unwrap();
    let elapsed2 = start.elapsed();

    println!("Results: {} documents", results2.len());
    println!("Time: {:?}", elapsed2);
    println!("Estimated cost: {}", plan2.estimated_cost);
    println!("Reason: {}\n", plan2.reason);

    // Performance comparison
    println!("=== PERFORMANCE COMPARISON ===");
    if elapsed1 < elapsed2 {
        let speedup = elapsed2.as_micros() as f64 / elapsed1.as_micros() as f64;
        println!("🚀 Covering index is {:.1}x FASTER!", speedup);
    }
    println!(
        "Covering:  {:?} (cost: {})",
        elapsed1, plan1.estimated_cost
    );
    println!(
        "Regular:   {:?} (cost: {})",
        elapsed2, plan2.estimated_cost
    );

    // Query 3: Range query with covering
    println!("\n=== Query 3: RANGE QUERY WITH COVERING ===");
    let query3 = doc! {
        "device_id": "sensor_042",
        "timestamp": { "$gte": 1000000, "$lt": 1100000 }
    };
    let projection3 = doc! { "temperature": 1, "humidity": 1, "_id": 0 };

    let plan3 = plan_query_with_projection(&query3, &indexes, Some(&projection3));

    match &plan3.execution_plan {
        ExecutionPlan::CoveringIndexScan { .. } => {
            println!("✅ Range query using COVERING INDEX");
        }
        _ => println!("❌ Not using covering index"),
    }

    let results3 = sensors.execute_plan(&plan3.execution_plan, &query3).unwrap();
    println!("Results: {} documents in time range", results3.len());
    println!("Estimated cost: {}\n", plan3.estimated_cost);

    // Best practices summary
    println!("=== BEST PRACTICES FOR COVERING INDEXES ===");
    println!("✅ DO:");
    println!("  • Create compound indexes on commonly queried + projected fields");
    println!("  • Use narrow projections (only fields you need)");
    println!("  • Exclude _id if you don't need it (_id: 0)");
    println!();
    println!("❌ DON'T:");
    println!("  • Include non-indexed fields in projection");
    println!("  • Use exclusion projections (field: 0)");
    println!("  • Omit projection (returns all fields)");
    println!();
    println!("📈 Expected Speedup:");
    println!("  • Small docs: 2-5x");
    println!("  • Medium docs: 5-10x");
    println!("  • Large docs (with metadata): 10-20x");
}
