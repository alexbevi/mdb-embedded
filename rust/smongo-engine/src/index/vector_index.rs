//! HNSW-based approximate nearest-neighbor vector index.
//!
//! Wraps an HNSW graph that is persisted to a redb table
//! (`{collection}.vidx_{index_name}`) and loaded lazily on first access.

use std::collections::HashMap;

use bson::Document;

/// In-memory HNSW vector index with doc_id mapping.
pub struct VectorIndex {
    /// doc_id -> internal graph node id
    id_map: HashMap<String, u32>,
    /// internal graph node id -> doc_id
    reverse_map: Vec<String>,
    /// Raw vector storage (row-major, `dimensions` floats per row).
    vectors: Vec<f32>,
    /// Number of dimensions per vector.
    pub dimensions: usize,
    /// Similarity metric name.
    pub metric: String,
}

impl VectorIndex {
    /// Create an empty index.
    pub fn new(dimensions: usize, metric: &str) -> Self {
        Self {
            id_map: HashMap::new(),
            reverse_map: Vec::new(),
            vectors: Vec::new(),
            dimensions,
            metric: metric.to_string(),
        }
    }

    /// Bulk-build from documents.
    pub fn build(docs: &[Document], field: &str, dimensions: usize, metric: &str) -> Self {
        let mut idx = Self::new(dimensions, metric);
        for doc in docs {
            let id = match doc.get("_id") {
                Some(bson::Bson::ObjectId(oid)) => oid.to_hex(),
                Some(bson::Bson::String(s)) => s.clone(),
                Some(bson::Bson::Int32(i)) => i.to_string(),
                Some(bson::Bson::Int64(i)) => i.to_string(),
                _ => continue,
            };
            if let Some(vec) = extract_vector(doc, field, dimensions) {
                idx.insert(&id, &vec);
            }
        }
        idx
    }

    /// Insert a single vector.
    pub fn insert(&mut self, doc_id: &str, vector: &[f32]) {
        if vector.len() != self.dimensions {
            return;
        }
        if self.id_map.contains_key(doc_id) {
            self.remove(doc_id);
        }
        let node_id = self.reverse_map.len() as u32;
        self.id_map.insert(doc_id.to_string(), node_id);
        self.reverse_map.push(doc_id.to_string());
        self.vectors.extend_from_slice(vector);
    }

    /// Remove a vector by doc_id. Marks the slot as empty (lazy deletion).
    pub fn remove(&mut self, doc_id: &str) {
        if let Some(&node_id) = self.id_map.get(doc_id) {
            self.id_map.remove(doc_id);
            if (node_id as usize) < self.reverse_map.len() {
                self.reverse_map[node_id as usize] = String::new();
            }
        }
    }

    /// Brute-force search (the HNSW graph search will replace this once the
    /// external crate is wired in; this keeps the API surface ready).
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(String, f32)> {
        if query.len() != self.dimensions {
            return Vec::new();
        }
        let n = self.reverse_map.len();
        let mut scored: Vec<(f32, usize)> = Vec::with_capacity(n);

        for i in 0..n {
            if self.reverse_map[i].is_empty() {
                continue;
            }
            let offset = i * self.dimensions;
            let end = offset + self.dimensions;
            if end > self.vectors.len() {
                continue;
            }
            let doc_vec = &self.vectors[offset..end];
            let score = match self.metric.as_str() {
                "cosine" => cosine_sim(query, doc_vec),
                "euclidean" => -euclidean_dist(query, doc_vec),
                "dotProduct" => dot_product(query, doc_vec),
                _ => cosine_sim(query, doc_vec),
            };
            scored.push((score, i));
        }

        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored
            .into_iter()
            .take(k)
            .map(|(score, idx)| (self.reverse_map[idx].clone(), score))
            .collect()
    }

    /// Serialize the index to bytes for persistence.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.dimensions as u32).to_le_bytes());
        let metric_bytes = self.metric.as_bytes();
        buf.extend_from_slice(&(metric_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(metric_bytes);
        buf.extend_from_slice(&(self.reverse_map.len() as u32).to_le_bytes());
        for (i, doc_id) in self.reverse_map.iter().enumerate() {
            let id_bytes = doc_id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(id_bytes);
            let offset = i * self.dimensions;
            let end = offset + self.dimensions;
            if end <= self.vectors.len() {
                for &f in &self.vectors[offset..end] {
                    buf.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        let mut pos = 0usize;
        if data.len() < 8 {
            return None;
        }
        let dimensions = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        let metric_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + metric_len > data.len() {
            return None;
        }
        let metric = std::str::from_utf8(&data[pos..pos + metric_len])
            .ok()?
            .to_string();
        pos += metric_len;
        if pos + 4 > data.len() {
            return None;
        }
        let count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;

        let mut idx = Self::new(dimensions, &metric);
        for _ in 0..count {
            if pos + 4 > data.len() {
                return None;
            }
            let id_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + id_len > data.len() {
                return None;
            }
            let doc_id = std::str::from_utf8(&data[pos..pos + id_len])
                .ok()?
                .to_string();
            pos += id_len;
            let vec_bytes = dimensions * 4;
            if pos + vec_bytes > data.len() {
                return None;
            }
            let mut vec = Vec::with_capacity(dimensions);
            for _ in 0..dimensions {
                let f = f32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
                pos += 4;
                vec.push(f);
            }
            if !doc_id.is_empty() {
                idx.insert(&doc_id, &vec);
            } else {
                idx.reverse_map.push(String::new());
                idx.vectors.extend_from_slice(&vec);
            }
        }
        Some(idx)
    }

    /// Number of active entries.
    pub fn len(&self) -> usize {
        self.id_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_map.is_empty()
    }
}

fn extract_vector(doc: &Document, field: &str, dimensions: usize) -> Option<Vec<f32>> {
    let val = crate::paths::get_value(doc, field)?;
    let bson::Bson::Array(arr) = val else {
        return None;
    };
    if arr.len() != dimensions {
        return None;
    }
    let vec: Vec<f32> = arr
        .iter()
        .filter_map(|v| v.as_f64().map(|f| f as f32))
        .collect();
    if vec.len() == dimensions {
        Some(vec)
    } else {
        None
    }
}

fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    let dp = dot_product(a, b);
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dp / (na * nb)
}

fn euclidean_dist(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_vector_index_basic() {
        let mut idx = VectorIndex::new(3, "cosine");
        idx.insert("a", &[1.0, 0.0, 0.0]);
        idx.insert("b", &[0.0, 1.0, 0.0]);
        idx.insert("c", &[0.9, 0.1, 0.0]);
        assert_eq!(idx.len(), 3);

        let results = idx.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "a");
    }

    #[test]
    fn test_vector_index_serialize_roundtrip() {
        let mut idx = VectorIndex::new(2, "euclidean");
        idx.insert("x", &[1.0, 2.0]);
        idx.insert("y", &[3.0, 4.0]);
        let bytes = idx.to_bytes();
        let idx2 = VectorIndex::from_bytes(&bytes).unwrap();
        assert_eq!(idx2.len(), 2);
        assert_eq!(idx2.dimensions, 2);
        assert_eq!(idx2.metric, "euclidean");
    }

    #[test]
    fn test_vector_index_build() {
        let docs = vec![
            doc! { "_id": 1, "emb": [1.0, 0.0] },
            doc! { "_id": 2, "emb": [0.0, 1.0] },
        ];
        let idx = VectorIndex::build(&docs, "emb", 2, "cosine");
        assert_eq!(idx.len(), 2);
        let results = idx.search(&[1.0, 0.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "1");
    }

    #[test]
    fn test_vector_index_remove() {
        let mut idx = VectorIndex::new(2, "cosine");
        idx.insert("a", &[1.0, 0.0]);
        idx.insert("b", &[0.0, 1.0]);
        idx.remove("a");
        assert_eq!(idx.len(), 1);
        let results = idx.search(&[1.0, 0.0], 2);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "b");
    }
}
