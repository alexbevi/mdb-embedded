//! Vector similarity search (`$vectorSearch`).
//!
//! Supports `cosine`, `euclidean`, and `dotProduct` metrics with optional MQL
//! pre-filter.  All scoring is done through [`VectorIndex`] (hora HNSW).

use bson::{Bson, Document};

use crate::index::vector_index::VectorIndex;
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

    let limit = vs_doc
        .get("limit")
        .and_then(|v| v.as_i64().or_else(|| v.as_i32().map(|i| i as i64)))
        .unwrap_or(10) as usize;
    let metric = vs_doc.get_str("metric").unwrap_or("cosine");
    let score_field = vs_doc.get_str("scoreField").unwrap_or("_vectorScore");
    let mql_filter = vs_doc.get_document("filter").ok();

    let candidates: Vec<Document> = if let Some(filter) = mql_filter {
        let mut filtered = Vec::new();
        for d in docs.iter() {
            if eval_query(d, filter).map_err(AggregationError::Other)? {
                filtered.push(d.clone());
            }
        }
        filtered
    } else {
        docs
    };

    let scored = score_documents(&candidates, path, &query_vec, limit, metric)?;

    let mut results = Vec::with_capacity(scored.len());
    for (mut doc, score) in scored {
        doc.insert(score_field.to_string(), Bson::Double(score as f64));
        results.push(doc);
    }

    Ok(results)
}

/// Score documents by vector similarity and return the top-k `(doc, score)` pairs.
///
/// This is the shared kernel used by the `$vectorSearch` stage,
/// `IndexProvider::vector_search`, and the streaming pipeline.
///
/// Always builds an HNSW graph via [`VectorIndex`] (hora) for ANN search.
pub fn score_documents(
    docs: &[Document],
    field: &str,
    query_vec: &[f32],
    limit: usize,
    metric: &str,
) -> AggregationResult<Vec<(Document, f32)>> {
    let dim = query_vec.len();
    let mut idx = VectorIndex::build(docs, field, dim, metric);
    let hits = idx.search(query_vec, limit);

    let id_score: std::collections::HashMap<String, f32> =
        hits.into_iter().collect();

    let mut results: Vec<(Document, f32)> = Vec::with_capacity(id_score.len());
    for doc in docs {
        let id_str = match doc.get("_id") {
            Some(bson::Bson::ObjectId(oid)) => oid.to_hex(),
            Some(bson::Bson::String(s)) => s.clone(),
            Some(bson::Bson::Int32(i)) => i.to_string(),
            Some(bson::Bson::Int64(i)) => i.to_string(),
            _ => continue,
        };
        if let Some(&score) = id_score.get(&id_str) {
            results.push((doc.clone(), score));
        }
    }

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    Ok(results)
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
