//! MongoDB-compatible index support for query optimization.
//!
//! This module provides index creation, maintenance, and query optimization
//! for the embedded database engine.
//!
//! # Features
//!
//! - **Single-field indexes**: Index on one field
//! - **Compound indexes**: Index on multiple fields
//! - **Unique constraints**: Enforce uniqueness
//! - **Query optimization**: Use indexes for faster queries
//!
//! # Example
//!
//! ```ignore
//! use smongo_engine::database::Database;
//! use bson::doc;
//!
//! let db = Database::open("./data/mydb")?;
//! let users = db.collection("users")?;
//!
//! // Create single-field index
//! users.create_index(doc! { "email": 1 }, None)?;
//!
//! // Create compound index
//! users.create_index(doc! { "age": 1, "name": -1 }, None)?;
//!
//! // Create unique index
//! users.create_index(
//!     doc! { "username": 1 },
//!     Some(IndexOptions { unique: true, ..Default::default() })
//! )?;
//!
//! // Queries now use indexes automatically
//! let result = users.find_one(doc! { "email": "alice@example.com" })?;
//! ```

use bson::{Bson, Document};
use serde::{Deserialize, Serialize};

/// `true` if keys are `{ "field": "2dsphere" }` or `{ "field": "2d" }` (single-field spherical geo).
pub fn is_2dsphere_keys(keys: &Document) -> bool {
    if keys.len() != 1 {
        return false;
    }
    keys.values().all(|v| {
        matches!(v, Bson::String(s) if s == "2dsphere" || s == "2d")
    })
}

/// Field name for a single-field `2dsphere` index, or `None`.
pub fn twodsphere_field(keys: &Document) -> Option<String> {
    if !is_2dsphere_keys(keys) {
        return None;
    }
    keys.keys().next().cloned()
}

/// Index specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSpec {
    /// Index name
    pub name: String,
    /// Index keys (field -> direction: 1 for ascending, -1 for descending)
    pub keys: Document,
    /// Index options
    pub options: IndexOptions,
}

/// Options for index creation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexOptions {
    /// Explicit index name (MongoDB `name`). When set, overrides [`generate_index_name`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Unique constraint
    pub unique: bool,
    /// Sparse index (only index documents with the field)
    pub sparse: bool,
    /// Background index creation (not yet supported)
    pub background: bool,
    /// TTL: automatically delete documents after this many seconds.
    /// Only valid on single-field indexes over a DateTime field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_after_seconds: Option<u64>,
}

/// Index direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexDirection {
    /// Ascending (1)
    Ascending,
    /// Descending (-1)
    Descending,
}

impl IndexDirection {
    /// Parse from BSON value
    pub fn from_bson(value: &Bson) -> Option<Self> {
        match value {
            Bson::Int32(1) | Bson::Int64(1) => Some(IndexDirection::Ascending),
            Bson::Int32(-1) | Bson::Int64(-1) => Some(IndexDirection::Descending),
            _ => None,
        }
    }
}

/// Extract index key from a document
///
/// # Arguments
///
/// * `doc` - Document to extract key from
/// * `keys` - Index key specification
///
/// # Returns
///
/// Serialized key bytes for the index
pub fn extract_index_key(doc: &Document, keys: &Document) -> Vec<u8> {
    use crate::paths::get_value;

    let mut key_parts = Vec::new();

    for (field, _direction) in keys {
        let value = get_value(doc, field);

        // Serialize the value
        let serialized = match value {
            Some(Bson::Null) => vec![0x00], // Null
            Some(Bson::Int32(n)) => n.to_be_bytes().to_vec(),
            Some(Bson::Int64(n)) => n.to_be_bytes().to_vec(),
            Some(Bson::Double(n)) => n.to_be_bytes().to_vec(),
            Some(Bson::String(s)) => s.as_bytes().to_vec(),
            Some(Bson::ObjectId(oid)) => oid.bytes().to_vec(),
            Some(Bson::Boolean(b)) => vec![if *b { 0x01 } else { 0x00 }],
            Some(Bson::DateTime(dt)) => dt.timestamp_millis().to_be_bytes().to_vec(),
            Some(_other) => {
                vec![0x02] // Placeholder for complex types
            }
            None => vec![0xFF], // Missing field
        };

        key_parts.push(serialized);
    }

    // Concatenate all parts with separators
    let mut result = Vec::new();
    for (i, part) in key_parts.iter().enumerate() {
        if i > 0 {
            result.push(0xFE); // Separator
        }
        result.extend_from_slice(part);
    }

    result
}

/// `2dsphere` index key bytes, or `None` if the document has no indexed point (sparse skip).
pub fn twodsphere_index_key(doc: &Document, keys: &Document) -> Option<Vec<u8>> {
    let field = twodsphere_field(keys)?;
    let val = crate::paths::get_value(doc, &field);
    let (lon, lat) = crate::geo::extract_lon_lat(val)?;
    let cell = crate::geo::cell_key_for_point(lon, lat);
    let id = match doc.get("_id") {
        Some(Bson::ObjectId(oid)) => oid.to_hex(),
        Some(Bson::String(s)) => s.clone(),
        Some(Bson::Int32(i)) => i.to_string(),
        Some(Bson::Int64(i)) => i.to_string(),
        Some(other) => format!("{}", other),
        None => return None,
    };
    let mut s = format!("{:016X}|", cell);
    s.push_str(&id);
    Some(s.into_bytes())
}

/// Generate index name from keys
///
/// # Arguments
///
/// * `keys` - Index key specification
///
/// # Returns
///
/// Generated index name (e.g., "field1_1_field2_-1")
pub fn generate_index_name(keys: &Document) -> String {
    let mut parts = Vec::new();

    for (field, direction) in keys {
        let dir_str = match direction {
            Bson::Int32(1) | Bson::Int64(1) => "1",
            Bson::Int32(-1) | Bson::Int64(-1) => "-1",
            Bson::String(s) => s.as_str(),
            _ => "1",
        };
        parts.push(format!("{}_{}", field, dir_str));
    }

    parts.join("_")
}

/// Rejects names that break storage layout (`collection.idx_<name>`) or collide with reserved ids.
pub fn validate_custom_index_name(name: &str) -> Result<(), String> {
    if name.contains('.') || name.contains('/') || name.contains('\\') {
        return Err(format!(
            "index name must not contain '.', '/', or '\\\\': {name:?}"
        ));
    }
    if name == "_id_" {
        return Err("index name '_id_' is reserved".to_string());
    }
    Ok(())
}

/// Check if a query can use an index
///
/// # Arguments
///
/// * `query` - Query document
/// * `index_keys` - Index key specification
///
/// # Returns
///
/// true if the query can potentially use this index
pub fn can_use_index(query: &Document, index_keys: &Document) -> bool {
    // Simple heuristic: check if any query field matches an index field
    // More sophisticated query planning could be added later

    for query_field in query.keys() {
        // Skip operators
        if query_field.starts_with('$') {
            continue;
        }

        // Check if this field is in the index
        if index_keys.contains_key(query_field) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_validate_custom_index_name() {
        assert!(validate_custom_index_name("ok_name_1").is_ok());
        assert!(validate_custom_index_name("bad.dot").is_err());
        assert!(validate_custom_index_name("_id_").is_err());
    }

    #[test]
    fn test_generate_index_name() {
        let keys = doc! { "email": 1 };
        assert_eq!(generate_index_name(&keys), "email_1");

        let keys = doc! { "age": 1, "name": -1 };
        let name = generate_index_name(&keys);
        assert!(name.contains("age_1"));
        assert!(name.contains("name_-1"));
    }

    #[test]
    fn test_extract_index_key() {
        let doc = doc! { "name": "Alice", "age": 30 };
        let keys = doc! { "age": 1 };
        let key = extract_index_key(&doc, &keys);
        assert!(!key.is_empty());
    }

    #[test]
    fn test_can_use_index() {
        let index_keys = doc! { "email": 1 };
        let query = doc! { "email": "alice@example.com" };
        assert!(can_use_index(&query, &index_keys));

        let query = doc! { "name": "Alice" };
        assert!(!can_use_index(&query, &index_keys));
    }

    #[test]
    fn test_index_direction_from_bson() {
        assert_eq!(
            IndexDirection::from_bson(&Bson::Int32(1)),
            Some(IndexDirection::Ascending)
        );
        assert_eq!(
            IndexDirection::from_bson(&Bson::Int32(-1)),
            Some(IndexDirection::Descending)
        );
    }
}
