//! Query planning and execution strategy selection.
//!
//! This module analyzes queries and selects the most efficient execution strategy,
//! including index selection for query optimization.

#[cfg(not(target_arch = "wasm32"))]
mod geo_plan;

use bson::{Bson, Document};

use crate::index::{is_2dsphere_keys, IndexSpec};

/// Query execution strategy
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionPlan {
    /// Full collection scan (no index used)
    CollectionScan,
    /// Index scan with post-filtering
    IndexScan {
        index_name: String,
        index_keys: Document,
    },
    /// Direct index seek for equality queries
    IndexSeek {
        index_name: String,
        index_keys: Document,
        seek_values: Document,
    },
    /// `$near` / `$nearSphere` with optional min/max distance (results sorted by distance).
    GeoNear {
        index_name: String,
        field: String,
        lon: f64,
        lat: f64,
        max_distance_m: Option<f64>,
        min_distance_m: Option<f64>,
    },
    /// `$geoWithin` with `$centerSphere` (cap covering + post-filter).
    GeoCapWithin {
        index_name: String,
        field: String,
        lon: f64,
        lat: f64,
        radius_m: f64,
    },
    /// `$geoWithin` / `$geoIntersects` with `$geometry` (S2 cell union + post-filter).
    GeoCellCover {
        index_name: String,
        field: String,
        cell_ids: Vec<u64>,
    },
    /// Union of plans for top-level `$or` (each branch must be indexable).
    OrUnionPlans {
        subplans: Vec<ExecutionPlan>,
    },
}

/// Query plan with estimated cost
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub execution_plan: ExecutionPlan,
    pub estimated_cost: u64,
    pub reason: String,
}

/// Analyze a query and select the best execution plan
pub fn plan_query(query: &Document, indexes: &[IndexSpec]) -> QueryPlan {
    if query.is_empty() {
        return QueryPlan {
            execution_plan: ExecutionPlan::CollectionScan,
            estimated_cost: u64::MAX,
            reason: "Empty query requires full collection scan".to_string(),
        };
    }

    if let Some(Bson::Array(branches)) = query.get("$or") {
        return plan_or_query(query, branches, indexes);
    }

    plan_simple_query(query, indexes)
}

fn plan_or_query(query: &Document, branches: &[Bson], indexes: &[IndexSpec]) -> QueryPlan {
    let mut base = Document::new();
    for (k, v) in query {
        if k != "$or" {
            base.insert(k.clone(), v.clone());
        }
    }

    let mut subplans: Vec<ExecutionPlan> = Vec::new();

    for b in branches {
        let Bson::Document(branch_doc) = b else {
            return QueryPlan {
                execution_plan: ExecutionPlan::CollectionScan,
                estimated_cost: u64::MAX,
                reason: "$or requires array of documents".to_string(),
            };
        };
        let mut merged = base.clone();
        for (k, v) in branch_doc {
            merged.insert(k.clone(), v.clone());
        }

        let sub = plan_query(&merged, indexes);
        if matches!(sub.execution_plan, ExecutionPlan::CollectionScan) {
            return QueryPlan {
                execution_plan: ExecutionPlan::CollectionScan,
                estimated_cost: u64::MAX,
                reason: "$or branch falls back to collection scan".to_string(),
            };
        }
        subplans.push(sub.execution_plan);
    }

    if subplans.is_empty() {
        return QueryPlan {
            execution_plan: ExecutionPlan::CollectionScan,
            estimated_cost: u64::MAX,
            reason: "Empty $or".to_string(),
        };
    }

    QueryPlan {
        execution_plan: ExecutionPlan::OrUnionPlans { subplans },
        estimated_cost: 200,
        reason: "Union of indexed $or branches".to_string(),
    }
}

fn plan_simple_query(query: &Document, indexes: &[IndexSpec]) -> QueryPlan {
    let mut best_plan: Option<QueryPlan> = None;

    for index_spec in indexes {
        if is_2dsphere_keys(&index_spec.keys) {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(plan) = geo_plan::evaluate_2dsphere_plan(query, index_spec) {
                best_plan = Some(pick_better(best_plan, plan));
            }
            continue;
        }

        if let Some(plan) = evaluate_index_for_query(query, index_spec) {
            best_plan = Some(pick_better(best_plan, plan));
        }
    }

    best_plan.unwrap_or_else(|| QueryPlan {
        execution_plan: ExecutionPlan::CollectionScan,
        estimated_cost: u64::MAX,
        reason: "No suitable index found".to_string(),
    })
}

fn pick_better(current: Option<QueryPlan>, candidate: QueryPlan) -> QueryPlan {
    match current {
        None => candidate,
        Some(prev) => {
            if candidate.estimated_cost < prev.estimated_cost {
                candidate
            } else {
                prev
            }
        }
    }
}

/// Evaluate if a btree index can be used for a query
fn evaluate_index_for_query(query: &Document, index_spec: &IndexSpec) -> Option<QueryPlan> {
    let first_index_field = index_spec.keys.iter().next()?.0;

    if !query.contains_key(first_index_field) {
        return None;
    }

    let query_value = query.get(first_index_field)?;

    if is_equality_query(query_value) {
        let mut seek_values = Document::new();
        seek_values.insert(first_index_field.clone(), query_value.clone());

        return Some(QueryPlan {
            execution_plan: ExecutionPlan::IndexSeek {
                index_name: index_spec.name.clone(),
                index_keys: index_spec.keys.clone(),
                seek_values,
            },
            estimated_cost: 10,
            reason: format!("Equality query on indexed field '{}'", first_index_field),
        });
    }

    if is_range_query(query_value) {
        return Some(QueryPlan {
            execution_plan: ExecutionPlan::IndexScan {
                index_name: index_spec.name.clone(),
                index_keys: index_spec.keys.clone(),
            },
            estimated_cost: 100,
            reason: format!("Range query on indexed field '{}'", first_index_field),
        });
    }

    None
}

fn is_equality_query(value: &Bson) -> bool {
    match value {
        Bson::Document(doc) => doc.len() == 1 && doc.contains_key("$eq"),
        _ => true,
    }
}

fn is_range_query(value: &Bson) -> bool {
    match value {
        Bson::Document(doc) => doc.keys().any(|k| {
            matches!(
                k.as_str(),
                "$gt" | "$gte" | "$lt" | "$lte"
            )
        }),
        _ => false,
    }
}

/// Calculate query selectivity score (lower is more selective)
pub fn calculate_selectivity(query: &Document) -> u32 {
    if query.is_empty() {
        return u32::MAX;
    }

    let mut selectivity = 0u32;

    for (field, value) in query {
        if field.starts_with('$') {
            selectivity += 50;
            continue;
        }

        match value {
            Bson::Document(doc) => {
                for op in doc.keys() {
                    selectivity += match op.as_str() {
                        "$eq" => 10,
                        "$ne" => 90,
                        "$gt" | "$gte" | "$lt" | "$lte" => 30,
                        "$in" => 20,
                        "$nin" => 80,
                        "$exists" => 70,
                        _ => 50,
                    };
                }
            }
            _ => selectivity += 10,
        }
    }

    selectivity
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use crate::index::IndexOptions;

    #[test]
    fn test_plan_empty_query() {
        let indexes = vec![];
        let plan = plan_query(&doc! {}, &indexes);
        assert!(matches!(plan.execution_plan, ExecutionPlan::CollectionScan));
    }

    #[test]
    fn test_plan_equality_query_with_index() {
        let indexes = vec![IndexSpec {
            name: "email_1".to_string(),
            keys: doc! { "email": 1 },
            options: IndexOptions::default(),
        }];

        let plan = plan_query(&doc! { "email": "alice@example.com" }, &indexes);

        assert!(matches!(plan.execution_plan, ExecutionPlan::IndexSeek { .. }));
        assert_eq!(plan.estimated_cost, 10);
    }

    #[test]
    fn test_plan_range_query_with_index() {
        let indexes = vec![IndexSpec {
            name: "age_1".to_string(),
            keys: doc! { "age": 1 },
            options: IndexOptions::default(),
        }];

        let plan = plan_query(&doc! { "age": { "$gte": 18 } }, &indexes);

        assert!(matches!(plan.execution_plan, ExecutionPlan::IndexScan { .. }));
        assert_eq!(plan.estimated_cost, 100);
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn test_plan_2dsphere_near() {
        let indexes = vec![IndexSpec {
            name: "loc_2dsphere".to_string(),
            keys: doc! { "loc": "2dsphere" },
            options: IndexOptions::default(),
        }];
        let plan = plan_query(
            &doc! { "loc": { "$near": { "$geometry": { "type": "Point", "coordinates": [0.0, 0.0] } }, "$maxDistance": 1000.0 } },
            &indexes,
        );
        assert!(matches!(plan.execution_plan, ExecutionPlan::GeoNear { .. }));
    }

    #[test]
    fn test_is_equality_query() {
        assert!(is_equality_query(&Bson::String("value".to_string())));
        assert!(is_equality_query(&Bson::Int32(42)));
        assert!(is_equality_query(&Bson::Document(doc! { "$eq": 42 })));
        assert!(!is_equality_query(&Bson::Document(doc! { "$gt": 42 })));
    }

    #[test]
    fn test_is_range_query() {
        assert!(is_range_query(&Bson::Document(doc! { "$gt": 18 })));
        assert!(is_range_query(&Bson::Document(doc! { "$gte": 18, "$lte": 65 })));
        assert!(!is_range_query(&Bson::String("value".to_string())));
        assert!(!is_range_query(&Bson::Document(doc! { "$eq": 42 })));
    }

    #[test]
    fn test_calculate_selectivity() {
        let sel = calculate_selectivity(&doc! { "email": "alice@example.com" });
        assert_eq!(sel, 10);

        let sel = calculate_selectivity(&doc! { "age": { "$gte": 18 } });
        assert_eq!(sel, 30);

        let sel = calculate_selectivity(&doc! {});
        assert_eq!(sel, u32::MAX);
    }

    #[test]
    fn test_select_best_index_among_multiple() {
        let indexes = vec![
            IndexSpec {
                name: "name_1".to_string(),
                keys: doc! { "name": 1 },
                options: IndexOptions::default(),
            },
            IndexSpec {
                name: "email_1".to_string(),
                keys: doc! { "email": 1 },
                options: IndexOptions::default(),
            },
        ];

        let plan = plan_query(&doc! { "email": "alice@example.com" }, &indexes);

        if let ExecutionPlan::IndexSeek { index_name, .. } = plan.execution_plan {
            assert_eq!(index_name, "email_1");
        } else {
            panic!("Expected IndexSeek");
        }
    }
}
