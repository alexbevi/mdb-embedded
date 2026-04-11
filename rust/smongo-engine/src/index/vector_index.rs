//! HNSW-based approximate nearest-neighbor vector index.
//!
//! Wraps an hora `HNSWIndex` graph that is rebuilt lazily from raw vector
//! storage whenever mutations (`insert` / `remove`) invalidate the graph.
//! Persistence uses the same compact binary format (doc-ids + row-major f32
//! vectors) and rebuilds the HNSW graph on deserialization.

use std::collections::HashMap;

use bson::Document;
use hora::core::ann_index::ANNIndex;
use hora::core::metrics::Metric;
use hora::index::hnsw_idx::HNSWIndex;
use hora::index::hnsw_params::HNSWParams;

/// In-memory HNSW vector index with doc_id mapping.
pub struct VectorIndex {
    /// doc_id -> internal node id
    id_map: HashMap<String, u32>,
    /// internal node id -> doc_id
    reverse_map: Vec<String>,
    /// Raw vector storage (row-major, `dimensions` floats per row).
    vectors: Vec<f32>,
    /// Number of dimensions per vector.
    pub dimensions: usize,
    /// Similarity metric name (`"cosine"`, `"euclidean"`, `"dotProduct"`).
    pub metric: String,
    /// Built HNSW graph. `None` until the first build/search.
    hnsw: Option<HNSWIndex<f32, usize>>,
    /// Set after insert/remove to signal the graph needs a rebuild.
    dirty: bool,
    /// HNSW ef_construction (maps to hora `ef_build`). `None` = hora default (500).
    ef_construction: Option<usize>,
    /// HNSW M parameter (maps to hora `n_neighbor`). `None` = hora default (32).
    m: Option<usize>,
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
            hnsw: None,
            dirty: false,
            ef_construction: None,
            m: None,
        }
    }

    /// Bulk-build from documents using default HNSW parameters.
    pub fn build(docs: &[Document], field: &str, dimensions: usize, metric: &str) -> Self {
        Self::build_with_params(docs, field, dimensions, metric, None, None)
    }

    /// Bulk-build from documents with explicit HNSW tuning knobs.
    pub fn build_with_params(
        docs: &[Document],
        field: &str,
        dimensions: usize,
        metric: &str,
        ef_construction: Option<usize>,
        m: Option<usize>,
    ) -> Self {
        let mut idx = Self::new(dimensions, metric);
        idx.ef_construction = ef_construction;
        idx.m = m;
        for doc in docs {
            let id = match doc.get("_id") {
                Some(bson::Bson::ObjectId(oid)) => oid.to_hex(),
                Some(bson::Bson::String(s)) => s.clone(),
                Some(bson::Bson::Int32(i)) => i.to_string(),
                Some(bson::Bson::Int64(i)) => i.to_string(),
                _ => continue,
            };
            if let Some(vec) = extract_vector(doc, field, dimensions) {
                idx.raw_insert(&id, &vec);
            }
        }
        idx.rebuild_hnsw();
        idx
    }

    /// Insert a single vector. Marks the HNSW graph dirty.
    pub fn insert(&mut self, doc_id: &str, vector: &[f32]) {
        if vector.len() != self.dimensions {
            return;
        }
        self.raw_insert(doc_id, vector);
        self.dirty = true;
    }

    /// Remove a vector by doc_id. Marks the slot as empty (lazy deletion).
    pub fn remove(&mut self, doc_id: &str) {
        if let Some(&node_id) = self.id_map.get(doc_id) {
            self.id_map.remove(doc_id);
            if (node_id as usize) < self.reverse_map.len() {
                self.reverse_map[node_id as usize] = String::new();
            }
            self.dirty = true;
        }
    }

    /// HNSW-accelerated k-NN search. Rebuilds the graph if dirty.
    pub fn search(&mut self, query: &[f32], k: usize) -> Vec<(String, f32)> {
        if query.len() != self.dimensions {
            return Vec::new();
        }
        if self.id_map.is_empty() {
            return Vec::new();
        }
        self.ensure_built();

        let hnsw = match self.hnsw.as_ref() {
            Some(h) => h,
            None => return Vec::new(),
        };

        let search_vec = self.prepare_query(query);
        let results = hnsw.search_nodes(&search_vec, k);

        let is_euclidean = self.metric == "euclidean";
        let mut scored: Vec<(String, f32)> = Vec::with_capacity(results.len());
        for (node, distance) in results {
            if let Some(node_id) = node.idx() {
                let idx = *node_id;
                if idx < self.reverse_map.len() && !self.reverse_map[idx].is_empty() {
                    let score = if is_euclidean {
                        // hora returns squared euclidean; convert to -distance
                        -(distance.max(0.0).sqrt())
                    } else {
                        // cosine / dotProduct: negate trick makes score = -distance
                        -distance
                    };
                    scored.push((self.reverse_map[idx].clone(), score));
                }
            }
        }

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored
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

    /// Deserialize from bytes. The HNSW graph is rebuilt on first search.
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
                idx.raw_insert(&doc_id, &vec);
            } else {
                idx.reverse_map.push(String::new());
                idx.vectors.extend_from_slice(&vec);
            }
        }
        idx.dirty = true;
        Some(idx)
    }

    /// Number of active entries.
    pub fn len(&self) -> usize {
        self.id_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_map.is_empty()
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Insert into raw storage without marking dirty (used during bulk build).
    fn raw_insert(&mut self, doc_id: &str, vector: &[f32]) {
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

    /// Ensure the HNSW graph is built and up-to-date.
    fn ensure_built(&mut self) {
        if self.hnsw.is_none() || self.dirty {
            self.rebuild_hnsw();
        }
    }

    /// Prepare a raw vector for insertion into the hora graph.
    ///
    /// * For **cosine**: L2-normalize then negate.
    /// * For **dotProduct**: negate (so hora's "smallest first" finds
    ///   highest dot-product).
    /// * For **euclidean**: pass through unchanged.
    fn prepare_for_graph(&self, raw: &[f32]) -> Vec<f32> {
        match self.metric.as_str() {
            "cosine" => {
                let normed = l2_normalize(raw);
                normed.iter().map(|x| -x).collect()
            }
            "dotProduct" => raw.iter().map(|x| -x).collect(),
            _ => raw.to_vec(),
        }
    }

    /// Prepare a query vector for search. Stored vectors are negated for
    /// cosine/dotProduct so hora's "smallest first" finds highest similarity.
    /// The query is NOT negated—only normalized for cosine.
    fn prepare_query(&self, raw: &[f32]) -> Vec<f32> {
        match self.metric.as_str() {
            "cosine" => l2_normalize(raw),
            _ => raw.to_vec(),
        }
    }

    /// (Re-)build the HNSW graph from raw vector storage.
    fn rebuild_hnsw(&mut self) {
        let n = self.reverse_map.len();
        if n == 0 || self.dimensions == 0 {
            self.hnsw = None;
            self.dirty = false;
            return;
        }

        let mut params = HNSWParams::<f32>::default().max_item(n.max(128));
        if let Some(ef) = self.ef_construction {
            params = params.ef_build(ef);
        }
        if let Some(m) = self.m {
            params = params.n_neighbor(m).n_neighbor0(m * 2);
        }

        let hora_metric = match self.metric.as_str() {
            "euclidean" => Metric::Euclidean,
            _ => Metric::DotProduct,
        };

        let mut hnsw = HNSWIndex::<f32, usize>::new(self.dimensions, &params);

        for i in 0..n {
            if self.reverse_map[i].is_empty() {
                continue;
            }
            let offset = i * self.dimensions;
            let end = offset + self.dimensions;
            if end > self.vectors.len() {
                continue;
            }
            let prepared = self.prepare_for_graph(&self.vectors[offset..end]);
            let _ = hnsw.add(&prepared, i);
        }

        let _ = hnsw.build(hora_metric);
        self.hnsw = Some(hnsw);
        self.dirty = false;
    }
}

/// L2-normalize a vector. Returns the original if norm is zero.
fn l2_normalize(v: &[f32]) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 {
        return v.to_vec();
    }
    v.iter().map(|x| x / norm).collect()
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
        let mut idx2 = VectorIndex::from_bytes(&bytes).unwrap();
        assert_eq!(idx2.len(), 2);
        assert_eq!(idx2.dimensions, 2);
        assert_eq!(idx2.metric, "euclidean");
        let results = idx2.search(&[1.0, 2.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "x");
    }

    #[test]
    fn test_vector_index_build() {
        let docs = vec![
            doc! { "_id": 1, "emb": [1.0, 0.0] },
            doc! { "_id": 2, "emb": [0.0, 1.0] },
        ];
        let mut idx = VectorIndex::build(&docs, "emb", 2, "cosine");
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

    #[test]
    fn test_vector_index_build_with_params() {
        let docs = vec![
            doc! { "_id": "a", "v": [1.0, 0.0, 0.0] },
            doc! { "_id": "b", "v": [0.0, 1.0, 0.0] },
            doc! { "_id": "c", "v": [0.0, 0.0, 1.0] },
        ];
        let mut idx =
            VectorIndex::build_with_params(&docs, "v", 3, "euclidean", Some(100), Some(16));
        assert_eq!(idx.len(), 3);
        let results = idx.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "a");
    }

    #[test]
    fn test_vector_index_dot_product() {
        let mut idx = VectorIndex::new(3, "dotProduct");
        idx.insert("a", &[1.0, 0.0, 0.0]);
        idx.insert("b", &[0.5, 0.5, 0.0]);
        idx.insert("c", &[0.0, 1.0, 0.0]);

        let results = idx.search(&[1.0, 0.0, 0.0], 3);
        assert_eq!(results[0].0, "a");
        assert!(results[0].1 > results[1].1);
    }
}
