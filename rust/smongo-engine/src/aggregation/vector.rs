//! In-memory brute-force vector similarity search (`$vectorSearch`).
//!
//! Supports `cosine` and `euclidean` metrics with optional MQL pre-filter.
//! Uses plain `f32` slice operations — no external linear algebra dependency.

use bson::{Bson, Document};

use crate::paths::get_value;
use crate::query::eval_query;

use super::{AggregationError, AggregationResult};

pub fn vector_search_stage(docs: Vec<Document>, spec: &Bson) -> AggregationResult<Vec<Document>> {
    let vs_doc = spec
        .as_document()
        .ok_or_else(|| AggregationError::InvalidStage("$vectorSearch requires document".into()))?;

    let path = vs_doc
        .get_str("path")
        .map_err(|_| AggregationError::MissingField("$vectorSearch.path required".into()))?;
    let query_vector_bson = vs_doc
        .get_array("queryVector")
        .map_err(|_| AggregationError::MissingField("$vectorSearch.queryVector required".into()))?;

    let query_vec: Vec<f32> = query_vector_bson
        .iter()
        .filter_map(|v| v.as_f64().map(|f| f as f32))
        .collect();
    if query_vec.is_empty() {
        return Err(AggregationError::InvalidStage(
            "$vectorSearch.queryVector must be a non-empty numeric array".into(),
        ));
    }
    let dim = query_vec.len();

    let limit = vs_doc
        .get("limit")
        .and_then(|v| v.as_i64().or_else(|| v.as_i32().map(|i| i as i64)))
        .unwrap_or(10) as usize;
    let num_candidates = vs_doc
        .get("numCandidates")
        .and_then(|v| v.as_i64().or_else(|| v.as_i32().map(|i| i as i64)))
        .map(|n| n as usize)
        .unwrap_or_else(|| limit.max(docs.len()));
    let metric = vs_doc.get_str("metric").unwrap_or("cosine");
    let score_field = vs_doc.get_str("scoreField").unwrap_or("_vectorScore");
    let mql_filter = vs_doc.get_document("filter").ok();

    let candidates: Vec<&Document> = if let Some(filter) = mql_filter {
        docs.iter()
            .filter(|d| eval_query(d, filter).unwrap_or(false))
            .collect()
    } else {
        docs.iter().collect()
    };

    // BinaryHeap-based top-k selection: O(n log k) instead of O(n log n).
    // We use a min-heap (via Reverse) of size `heap_cap` so the smallest
    // score in the heap is always at the top and can be cheaply evicted.
    use super::total_ord::TotalF32;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let heap_cap = num_candidates.min(limit.max(num_candidates));
    let mut heap: BinaryHeap<Reverse<(TotalF32, usize)>> = BinaryHeap::with_capacity(heap_cap + 1);

    for (idx, doc) in candidates.iter().enumerate() {
        let Some(vec_val) = get_value(doc, path) else {
            continue;
        };
        let Bson::Array(arr) = vec_val else {
            continue;
        };
        if arr.len() != dim {
            continue;
        }
        let doc_vec: Vec<f32> = arr
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect();
        if doc_vec.len() != dim {
            continue;
        }

        let score = match metric {
            "cosine" => match cosine_similarity(&query_vec, &doc_vec) {
                Some(s) => s,
                None => continue,
            },
            "euclidean" => {
                let dist = euclidean_distance(&query_vec, &doc_vec);
                -dist
            }
            "dotProduct" => dot_product(&query_vec, &doc_vec),
            _ => {
                return Err(AggregationError::InvalidStage(format!(
                    "unsupported vector metric: {metric}"
                )));
            }
        };

        let key = TotalF32(score);
        if heap.len() < heap_cap {
            heap.push(Reverse((key, idx)));
        } else if let Some(&Reverse((ref min_score, _))) = heap.peek() {
            if key > *min_score {
                heap.pop();
                heap.push(Reverse((key, idx)));
            }
        }
    }

    // Drain the heap and sort the top-k by descending score.
    let mut scored: Vec<(usize, f32)> = heap
        .into_iter()
        .map(|Reverse((s, idx))| (idx, s.0))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(limit);

    let mut results = Vec::with_capacity(scored.len());
    for (idx, score) in scored {
        let mut doc = candidates[idx].clone();
        doc.insert(score_field.to_string(), Bson::Double(score as f64));
        results.push(doc);
    }

    Ok(results)
}

fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn norm(v: &[f32]) -> f32 {
    dot_product(v, v).sqrt()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f32> {
    let na = norm(a);
    let nb = norm(b);
    if na == 0.0 || nb == 0.0 {
        return None;
    }
    Some(dot_product(a, b) / (na * nb))
}

fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bson::doc;

    fn make_doc(id: i32, vec: Vec<f64>) -> Document {
        let bson_vec: Vec<Bson> = vec.into_iter().map(Bson::Double).collect();
        doc! { "_id": id, "embedding": bson_vec }
    }

    #[test]
    fn test_cosine_basic() {
        let docs = vec![
            make_doc(1, vec![1.0, 0.0, 0.0]),
            make_doc(2, vec![0.0, 1.0, 0.0]),
            make_doc(3, vec![0.9, 0.1, 0.0]),
        ];
        let spec = doc! {
            "path": "embedding",
            "queryVector": [1.0, 0.0, 0.0],
            "limit": 2,
            "metric": "cosine",
        };
        let results = vector_search_stage(docs, &Bson::Document(spec)).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_i32("_id").unwrap(), 1);
        assert_eq!(results[1].get_i32("_id").unwrap(), 3);
    }

    #[test]
    fn test_euclidean() {
        let docs = vec![
            make_doc(1, vec![0.0, 0.0]),
            make_doc(2, vec![3.0, 4.0]),
            make_doc(3, vec![1.0, 0.0]),
        ];
        let spec = doc! {
            "path": "embedding",
            "queryVector": [0.0, 0.0],
            "limit": 2,
            "metric": "euclidean",
        };
        let results = vector_search_stage(docs, &Bson::Document(spec)).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_i32("_id").unwrap(), 1);
        assert_eq!(results[1].get_i32("_id").unwrap(), 3);
    }

    #[test]
    fn test_with_filter() {
        let docs = vec![
            {
                let mut d = make_doc(1, vec![1.0, 0.0]);
                d.insert("active", true);
                d
            },
            {
                let mut d = make_doc(2, vec![0.9, 0.1]);
                d.insert("active", false);
                d
            },
            {
                let mut d = make_doc(3, vec![0.8, 0.2]);
                d.insert("active", true);
                d
            },
        ];
        let spec = doc! {
            "path": "embedding",
            "queryVector": [1.0, 0.0],
            "limit": 10,
            "metric": "cosine",
            "filter": { "active": true },
        };
        let results = vector_search_stage(docs, &Bson::Document(spec)).unwrap();
        assert_eq!(results.len(), 2);
        for r in &results {
            assert_eq!(r.get_bool("active").unwrap(), true);
        }
    }

    #[test]
    fn test_score_field() {
        let docs = vec![make_doc(1, vec![1.0, 0.0])];
        let spec = doc! {
            "path": "embedding",
            "queryVector": [1.0, 0.0],
            "limit": 1,
            "metric": "cosine",
            "scoreField": "_myScore",
        };
        let results = vector_search_stage(docs, &Bson::Document(spec)).unwrap();
        assert!(results[0].get_f64("_myScore").is_ok());
    }
}
