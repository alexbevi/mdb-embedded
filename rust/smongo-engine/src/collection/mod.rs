//! MongoDB-compatible collection operations for pure Rust BSON documents.
//!
//! This module provides a high-level API for database operations, integrating
//! the query, update, and pluggable storage layers.
//!
//! # Features
//!
//! - **Insert operations**: `insert_one`, `insert_many`
//! - **Find operations**: `find_one`, `find` with query filters
//! - **Update operations**: `update_one`, `update_many`
//! - **Delete operations**: `delete_one`, `delete_many`
//! - **Index operations**: `create_index`, `drop_index`, `list_indexes`
//! - **Utility operations**: `count_documents`
//!
//! # Example
//!
//! ```ignore
//! use smongo_engine::collection::Collection;
//! use bson::doc;
//!
//! let collection = Collection::new(session, "users")?;
//!
//! // Insert a document
//! let result = collection.insert_one(doc! { "name": "Alice", "age": 30 })?;
//!
//! // Create index for faster queries
//! collection.create_index(doc! { "email": 1 }, None)?;
//!
//! // Find documents (uses index automatically)
//! let users = collection.find(doc! { "age": { "$gte": 18 } })?;
//!
//! // Update documents
//! let result = collection.update_many(
//!     doc! { "status": "pending" },
//!     doc! { "$set": { "status": "active" } }
//! )?;
//! ```

use bson::{oid::ObjectId, Bson, Document};
use std::collections::HashSet;
use std::io::Cursor;
use std::marker::PhantomData;

use crate::explain::{ExecutionStats, ExplainResult};
use crate::index::{
    decode_index_key, extract_index_key, generate_index_name, is_2dsphere_keys, validate_custom_index_name,
    IndexOptions, IndexSpec,
};
use crate::oplog::{append_oplog, AppendOplogOpts, CollectionOplogSettings};
#[cfg(not(target_arch = "wasm32"))]
use crate::index::twodsphere_index_key;
use crate::planner::{plan_query, ExecutionPlan};
use crate::query::eval_query;
use crate::update::apply_update;
use crate::storage::{DefaultSession, StorageCursor, StorageError, StorageResult, StorageSession};

mod geo_find;

#[cfg(not(target_arch = "wasm32"))]
fn now_epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(target_arch = "wasm32")]
fn now_epoch_millis() -> i64 {
    js_sys::Date::now() as i64
}

/// Result type for collection operations
pub type CollectionResult<T> = Result<T, CollectionError>;

/// Errors that can occur during collection operations
#[derive(Debug)]
pub enum CollectionError {
    /// Storage backend error
    StorageError(StorageError),
    /// BSON serialization/deserialization error
    BsonError(bson::ser::Error),
    /// BSON deserialization error
    BsonDeError(bson::de::Error),
    /// Query evaluation error
    QueryError(String),
    /// Update operation error
    UpdateError(String),
    /// Document missing required _id field
    MissingIdError,
    /// Index already exists
    IndexAlreadyExists(String),
    /// Index not found
    IndexNotFound(String),
    /// Unique constraint violation
    UniqueConstraintViolation(String),
    /// Invalid index specification
    InvalidIndexSpec(String),
    /// Other errors
    Other(String),
}

impl From<StorageError> for CollectionError {
    fn from(err: StorageError) -> Self {
        CollectionError::StorageError(err)
    }
}

impl From<bson::ser::Error> for CollectionError {
    fn from(err: bson::ser::Error) -> Self {
        CollectionError::BsonError(err)
    }
}

impl From<bson::de::Error> for CollectionError {
    fn from(err: bson::de::Error) -> Self {
        CollectionError::BsonDeError(err)
    }
}

impl std::fmt::Display for CollectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectionError::StorageError(e) => write!(f, "Storage error: {}", e),
            CollectionError::BsonError(e) => write!(f, "BSON serialization error: {}", e),
            CollectionError::BsonDeError(e) => write!(f, "BSON deserialization error: {}", e),
            CollectionError::QueryError(e) => write!(f, "Query error: {}", e),
            CollectionError::UpdateError(e) => write!(f, "Update error: {}", e),
            CollectionError::MissingIdError => write!(f, "Document missing _id field"),
            CollectionError::IndexAlreadyExists(name) => write!(f, "Index already exists: {}", name),
            CollectionError::IndexNotFound(name) => write!(f, "Index not found: {}", name),
            CollectionError::UniqueConstraintViolation(msg) => write!(f, "Unique constraint violation: {}", msg),
            CollectionError::InvalidIndexSpec(msg) => write!(f, "Invalid index specification: {}", msg),
            CollectionError::Other(e) => write!(f, "Error: {}", e),
        }
    }
}

impl std::error::Error for CollectionError {}

/// Result of an insert operation
#[derive(Debug, Clone)]
pub struct InsertOneResult {
    /// The _id of the inserted document
    pub inserted_id: Bson,
}

/// Result of an insert many operation
#[derive(Debug, Clone)]
pub struct InsertManyResult {
    /// The _ids of the inserted documents
    pub inserted_ids: Vec<Bson>,
}

/// Result of an update operation
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of documents matched by the query
    pub matched_count: u64,
    /// Number of documents actually modified
    pub modified_count: u64,
    /// The _id of the upserted document, if an upsert took place
    pub upserted_id: Option<Bson>,
}

/// Options for insert operations
#[derive(Debug, Clone, Default)]
pub struct InsertOptions {
    /// If true, sync should not push this entry (remote-originated writes).
    pub internal: bool,
}

/// Options for update operations
#[derive(Debug, Clone, Default)]
pub struct UpdateOptions {
    /// If true, insert a new document when no match is found
    pub upsert: bool,
    /// If true, sync should not push this entry (remote-originated writes).
    pub internal: bool,
}

/// Options for delete operations
#[derive(Debug, Clone, Default)]
pub struct DeleteOptions {
    /// If true, sync should not push this entry (remote-originated writes).
    pub internal: bool,
}

/// Options for find operations
#[derive(Debug, Clone, Default)]
pub struct FindOptions {
    /// Sort specification (e.g. `{age: -1}`)
    pub sort: Option<Document>,
    /// Maximum number of documents to return
    pub limit: Option<i64>,
    /// Number of documents to skip
    pub skip: Option<i64>,
    /// Projection specification (inclusion or exclusion)
    pub projection: Option<Document>,
}

/// Result of a delete operation
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Number of documents deleted
    pub deleted_count: u64,
}

fn extract_equality_fields(filter: &Document) -> Document {
    let mut doc = Document::new();
    for (key, value) in filter {
        if key.starts_with('$') {
            continue;
        }
        match value {
            Bson::Document(inner) if inner.keys().any(|k| k.starts_with('$')) => {
                // Operator expression like {$gt: 5} -- skip it
            }
            _ => {
                doc.insert(key.clone(), value.clone());
            }
        }
    }
    doc
}

fn ensure_id(doc: &mut Document) -> Bson {
    if let Some(id) = doc.get("_id") {
        id.clone()
    } else {
        let id = Bson::ObjectId(ObjectId::new());
        doc.insert("_id", id.clone());
        id
    }
}

fn extract_id_string(doc: &Document) -> CollectionResult<String> {
    match doc.get("_id") {
        Some(Bson::ObjectId(oid)) => Ok(oid.to_hex()),
        Some(Bson::String(s)) => Ok(s.clone()),
        Some(Bson::Int32(i)) => Ok(i.to_string()),
        Some(Bson::Int64(i)) => Ok(i.to_string()),
        Some(other) => Ok(format!("{}", other)),
        None => Err(CollectionError::MissingIdError),
    }
}

fn serialize_document(doc: &Document) -> CollectionResult<Vec<u8>> {
    let mut buf = Vec::new();
    doc.to_writer(&mut buf)?;
    Ok(buf)
}

fn deserialize_document(bytes: &[u8]) -> CollectionResult<Document> {
    let mut r = Cursor::new(bytes);
    let doc = Document::from_reader(&mut r)?;
    Ok(doc)
}

fn build_seek_prefix(index_keys: &Document, seek_values: &Document) -> Vec<u8> {
    let mut trimmed = Document::new();
    for (field, dir) in index_keys {
        if seek_values.contains_key(field) {
            trimmed.insert(field.clone(), dir.clone());
        } else {
            break; // stop at the first gap in the compound prefix
        }
    }
    extract_index_key(seek_values, &trimmed)
}

/// Extract the query vector and k from a filter that targets a vector field.
/// Looks for patterns like `{field: {$near: [...]}}` or `{field: [...]}` with
/// an optional `$k` or `$limit` hint.
fn extract_vector_query(filter: &Document, field: &str) -> Option<(Vec<f32>, usize)> {
    let mut k = 10usize;
    let arr = match filter.get(field)? {
        Bson::Array(arr) => arr.clone(),
        Bson::Document(d) => {
            if let Some(Bson::Int32(n)) = d.get("$k").or_else(|| d.get("$limit")) {
                k = (*n).max(1) as usize;
            } else if let Some(Bson::Int64(n)) = d.get("$k").or_else(|| d.get("$limit")) {
                k = (*n).max(1) as usize;
            }
            match d.get("$near").or_else(|| d.get("$vector")) {
                Some(Bson::Array(a)) => a.clone(),
                _ => return None,
            }
        }
        _ => return None,
    };
    let vec: Vec<f32> = arr
        .iter()
        .filter_map(|v| v.as_f64().map(|f| f as f32))
        .collect();
    if vec.len() != arr.len() || vec.is_empty() {
        return None;
    }
    Some((vec, k))
}

/// Apply projection to index document (fields from index keys)
fn apply_projection_to_index_doc(index_doc: &Document, projection: &Document) -> Document {
    let mut result = Document::new();

    for (field, value) in projection {
        if field == "_id" {
            continue; // _id handled separately
        }

        // Check if this is an inclusion projection
        let is_included = match value {
            Bson::Int32(n) => *n != 0,
            Bson::Int64(n) => *n != 0,
            Bson::Double(d) => *d != 0.0,
            Bson::Boolean(b) => *b,
            _ => true,
        };

        if is_included {
            if let Some(v) = index_doc.get(field) {
                result.insert(field.clone(), v.clone());
            }
        }
    }

    result
}

/// Check if _id should be included in projection
fn should_include_id(projection: &Document) -> bool {
    match projection.get("_id") {
        Some(Bson::Int32(0)) | Some(Bson::Int64(0)) | Some(Bson::Boolean(false)) => false,
        _ => true, // _id included by default
    }
}

/// A MongoDB-compatible collection backed by pluggable storage.
pub struct Collection<S: StorageSession = DefaultSession> {
    session: S,
    table_name: String,
    collection_name: String,
    validator: Option<Document>,
    /// When set, mutating operations append BSON oplog rows in the same storage transaction.
    oplog: Option<CollectionOplogSettings>,
}

// ============================================================
// STREAMING FIND CURSOR
// ============================================================

enum FindCursorState<C: StorageCursor> {
    CollectionScan {
        cursor: C,
    },
    IndexScan {
        index_cursor: C,
        data_cursor: C,
    },
    IndexSeek {
        index_cursor: C,
        data_cursor: C,
        seek_key: Vec<u8>,
        positioned: bool,
    },
    /// Precomputed matches (geospatial `$or` unions, `$near` sort, etc.).
    Materialized {
        docs: Vec<Document>,
        next_ix: usize,
    },
    /// Streaming covering index scan — reads directly from index keys without
    /// document fetch.  Each iteration decodes one index entry.
    CoveringIndexStream {
        index_cursor: C,
        index_keys: Document,
        projection: Document,
        seek_key: Option<Vec<u8>>,
        positioned: bool,
    },
}

/// Streaming cursor over query results. Yields matching documents one at a
/// time without materializing the full result set.
///
/// Created by [`Collection::find_iter`]. The lifetime parameter ensures the
/// cursor cannot outlive the `Collection` (and its underlying storage session).
pub struct FindCursor<'a, C: StorageCursor> {
    state: FindCursorState<C>,
    filter: Document,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a, C: StorageCursor> Iterator for FindCursor<'a, C> {
    type Item = CollectionResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                FindCursorState::CollectionScan { cursor } => {
                    if cursor.next().is_err() {
                        return None;
                    }
                    let doc_bytes = match cursor.get_value_raw() {
                        Ok(b) => b,
                        Err(e) => return Some(Err(e.into())),
                    };
                    let doc = match deserialize_document(&doc_bytes) {
                        Ok(d) => d,
                        Err(e) => return Some(Err(e)),
                    };
                    match eval_query(&doc, &self.filter) {
                        Ok(true) => return Some(Ok(doc)),
                        Ok(false) => continue,
                        Err(e) => return Some(Err(CollectionError::QueryError(e))),
                    }
                }
                FindCursorState::IndexScan {
                    index_cursor,
                    data_cursor,
                } => {
                    if index_cursor.next().is_err() {
                        return None;
                    }
                    let id_str = match index_cursor.get_value_str() {
                        Ok(s) => s,
                        Err(e) => return Some(Err(e.into())),
                    };
                    data_cursor.set_key_str(&id_str);
                    if data_cursor.search().is_err() {
                        continue;
                    }
                    let doc_bytes = match data_cursor.get_value_raw() {
                        Ok(b) => b,
                        Err(e) => return Some(Err(e.into())),
                    };
                    let doc = match deserialize_document(&doc_bytes) {
                        Ok(d) => d,
                        Err(e) => return Some(Err(e)),
                    };
                    match eval_query(&doc, &self.filter) {
                        Ok(true) => return Some(Ok(doc)),
                        Ok(false) => continue,
                        Err(e) => return Some(Err(CollectionError::QueryError(e))),
                    }
                }
                FindCursorState::Materialized { docs, next_ix } => {
                    if *next_ix < docs.len() {
                        let doc = docs[*next_ix].clone();
                        *next_ix += 1;
                        return Some(Ok(doc));
                    }
                    return None;
                }
                FindCursorState::IndexSeek {
                    index_cursor,
                    data_cursor,
                    seek_key,
                    positioned,
                } => {
                    if !*positioned {
                        *positioned = true;
                        index_cursor.set_key_raw(seek_key);
                        match index_cursor.search_near() {
                            Ok(exact) => {
                                if exact < 0 && index_cursor.next().is_err() {
                                    return None;
                                }
                            }
                            Err(_) => return None,
                        }
                    } else if index_cursor.next().is_err() {
                        return None;
                    }

                    let index_key_raw = match index_cursor.get_key_raw() {
                        Ok(k) => k,
                        Err(e) => return Some(Err(e.into())),
                    };
                    if !index_key_raw.starts_with(seek_key) {
                        return None;
                    }

                    let id_str = match index_cursor.get_value_str() {
                        Ok(s) => s,
                        Err(e) => return Some(Err(e.into())),
                    };
                    data_cursor.set_key_str(&id_str);
                    if data_cursor.search().is_err() {
                        continue;
                    }
                    let doc_bytes = match data_cursor.get_value_raw() {
                        Ok(b) => b,
                        Err(e) => return Some(Err(e.into())),
                    };
                    let doc = match deserialize_document(&doc_bytes) {
                        Ok(d) => d,
                        Err(e) => return Some(Err(e)),
                    };
                    match eval_query(&doc, &self.filter) {
                        Ok(true) => return Some(Ok(doc)),
                        Ok(false) => continue,
                        Err(e) => return Some(Err(CollectionError::QueryError(e))),
                    }
                }
                FindCursorState::CoveringIndexStream {
                    index_cursor,
                    index_keys,
                    projection,
                    seek_key,
                    positioned,
                } => {
                    if !*positioned {
                        *positioned = true;
                        if let Some(sk) = seek_key {
                            index_cursor.set_key_raw(sk);
                            match index_cursor.search_near() {
                                Ok(exact) => {
                                    if exact < 0 && index_cursor.next().is_err() {
                                        return None;
                                    }
                                }
                                Err(_) => return None,
                            }
                        } else if index_cursor.next().is_err() {
                            return None;
                        }
                    } else if index_cursor.next().is_err() {
                        return None;
                    }

                    if let Some(sk) = seek_key {
                        let raw = match index_cursor.get_key_raw() {
                            Ok(k) => k,
                            Err(e) => return Some(Err(e.into())),
                        };
                        if !raw.starts_with(sk) {
                            return None;
                        }
                    }

                    let key_raw = match index_cursor.get_key_raw() {
                        Ok(k) => k,
                        Err(e) => return Some(Err(e.into())),
                    };
                    if let Some(mut doc) =
                        crate::index::decode_index_key(&key_raw, index_keys)
                    {
                        let id_str = match index_cursor.get_value_str() {
                            Ok(s) => s,
                            Err(e) => return Some(Err(e.into())),
                        };
                        doc.insert("_id".to_string(), bson::Bson::String(id_str));
                        let projected = apply_projection_to_index_doc(&doc, projection);
                        return Some(Ok(projected));
                    }
                    continue;
                }
            }
        }
    }
}

impl<S: StorageSession> Collection<S> {
    /// Create a new collection using `name` as the storage table name.
    pub fn new(session: S, name: &str) -> CollectionResult<Self> {
        Self::with_table_uri(session, name, name)
    }

    /// Create a collection with a custom storage table name.
    ///
    /// Use this when you need a namespaced table like `mydb_users`.
    pub fn with_table_uri(
        session: S,
        name: &str,
        table_uri: &str,
    ) -> CollectionResult<Self> {
        session.create_table(table_uri)?;

        Ok(Collection {
            session,
            table_name: table_uri.to_string(),
            collection_name: name.to_string(),
            validator: None,
            oplog: None,
        })
    }

    /// Attach oplog settings (creates oplog table). Used for sync / change streams on redb.
    pub fn with_oplog_settings(mut self, oplog: CollectionOplogSettings) -> CollectionResult<Self> {
        self.session
            .create_table(&oplog.oplog_table)
            .map_err(CollectionError::from)?;
        self.oplog = Some(oplog);
        Ok(self)
    }

    fn map_oplog_err(e: crate::oplog::OplogError) -> CollectionError {
        CollectionError::Other(e.to_string())
    }

    fn with_oplog_transaction<R>(
        &self,
        f: impl FnOnce(&Self) -> CollectionResult<R>,
    ) -> CollectionResult<R> {
        if self.oplog.is_none() {
            return f(self);
        }
        self.session.begin_transaction().map_err(CollectionError::from)?;
        let r = f(self);
        match &r {
            Ok(_) => {
                self.session.commit_transaction().map_err(CollectionError::from)?;
            }
            Err(_) => {
                let _ = self.session.rollback_transaction();
            }
        }
        r
    }

    fn append_oplog_if_enabled(
        &self,
        op: &str,
        doc_id: Bson,
        payload: Option<Document>,
        internal: bool,
        changed_fields: Option<Vec<String>>,
    ) -> CollectionResult<()> {
        if internal {
            return Ok(());
        }
        let Some(ref cfg) = self.oplog else {
            return Ok(());
        };
        append_oplog(
            &self.session,
            cfg,
            op,
            doc_id,
            payload,
            AppendOplogOpts {
                changed_fields,
                ..Default::default()
            },
        )
        .map_err(Self::map_oplog_err)?;
        Ok(())
    }

    fn doc_top_level_changed(before: &Document, after: &Document) -> Vec<String> {
        let mut names = HashSet::new();
        for k in before.keys().chain(after.keys()) {
            if k != "_id" {
                names.insert(k.clone());
            }
        }
        let mut out: Vec<String> = names
            .into_iter()
            .filter(|k| before.get(k) != after.get(k))
            .collect();
        out.sort();
        out
    }

    /// Attach a JSON Schema validator. Documents will be validated on insert/update.
    pub fn set_validator(&mut self, schema: Option<Document>) {
        self.validator = schema;
    }

    /// Get the current validator schema, if any.
    pub fn validator(&self) -> Option<&Document> {
        self.validator.as_ref()
    }

    fn validate_doc(&self, doc: &Document) -> CollectionResult<()> {
        if let Some(ref schema) = self.validator {
            crate::schema::validate_document(doc, schema).map_err(|e| {
                CollectionError::Other(format!("Validation failed: {}", e))
            })
        } else {
            Ok(())
        }
    }

    /// Get a cursor for the collection table
    fn cursor(&self) -> StorageResult<S::Cursor> {
        self.session.open_cursor(&self.table_name)
    }

    fn fetch_doc_by_id_str(&self, id: &str) -> CollectionResult<Option<Document>> {
        let mut data_cursor = self.cursor().map_err(CollectionError::from)?;
        data_cursor.set_key_str(id);
        if data_cursor.search().is_err() {
            return Ok(None);
        }
        let doc_bytes = data_cursor.get_value_raw().map_err(CollectionError::from)?;
        Ok(Some(deserialize_document(&doc_bytes)?))
    }

    /// Run a planner [`ExecutionPlan`] and return all documents matching `filter`.
    pub fn execute_plan(&self, plan: &ExecutionPlan, filter: &Document) -> CollectionResult<Vec<Document>> {
        match plan {
            ExecutionPlan::CollectionScan => self.collect_collection_scan(filter),
            ExecutionPlan::IndexScan {
                index_name,
                index_keys,
            } => self.collect_index_scan(filter, index_name, index_keys),
            ExecutionPlan::IndexSeek {
                index_name,
                index_keys,
                seek_values,
            } => self.collect_index_seek(filter, index_name, index_keys, seek_values),
            ExecutionPlan::CoveringIndexScan {
                index_name,
                index_keys,
                seek_values,
                projection,
            } => self.collect_covering_index_scan(filter, index_name, index_keys, seek_values.as_ref(), projection),
            ExecutionPlan::SortedIndexScan {
                index_name,
                limit,
                ..
            } => self.collect_sorted_index_scan(filter, index_name, *limit),
            ExecutionPlan::BitmapScan {
                index_name,
                field,
            } => self.collect_bitmap_scan(filter, index_name, field),
            ExecutionPlan::PrefixIndexScan {
                index_name,
                index_keys,
                prefix_length,
            } => self.collect_prefix_index_scan(filter, index_name, index_keys, *prefix_length),
            ExecutionPlan::TextIndexScan {
                index_name,
                fields,
            } => self.collect_text_index_scan(filter, index_name, fields),
            ExecutionPlan::VectorIndexSearch {
                index_name,
                field,
                dimensions,
                metric,
            } => self.collect_vector_index_search(filter, index_name, field, *dimensions, metric),
            ExecutionPlan::GeoNear { .. }
            | ExecutionPlan::GeoCapWithin { .. }
            | ExecutionPlan::GeoCellCover { .. } => geo_find::materialize_geo_plan(self, plan, filter),
            ExecutionPlan::OrUnionPlans { subplans } => {
                let mut seen = HashSet::new();
                let mut out = Vec::new();
                for sub in subplans {
                    for doc in self.execute_plan(sub, filter)? {
                        let id_str = extract_id_string(&doc)?;
                        if seen.insert(id_str) {
                            out.push(doc);
                        }
                    }
                }
                Ok(out)
            }
        }
    }

    /// Walk a B-tree index in order, fetch docs, post-filter, stop at `limit`.
    fn collect_sorted_index_scan(
        &self,
        filter: &Document,
        index_name: &str,
        limit: usize,
    ) -> CollectionResult<Vec<Document>> {
        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = self
            .session
            .open_cursor(&index_table_name)
            .map_err(CollectionError::from)?;
        let mut out = Vec::with_capacity(limit);
        if index_cursor.next().is_err() {
            return Ok(out);
        }
        loop {
            let id_str = index_cursor.get_value_str().map_err(CollectionError::from)?;
            if let Some(doc) = self.fetch_doc_by_id_str(&id_str)? {
                if filter.is_empty()
                    || eval_query(&doc, filter).map_err(CollectionError::QueryError)?
                {
                    out.push(doc);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
            if index_cursor.next().is_err() {
                break;
            }
        }
        Ok(out)
    }

    fn collect_collection_scan(&self, filter: &Document) -> CollectionResult<Vec<Document>> {
        let mut cursor = self.cursor().map_err(CollectionError::from)?;
        let mut out = Vec::new();
        if cursor.next().is_err() {
            return Ok(out);
        }
        loop {
            let doc_bytes = cursor.get_value_raw().map_err(CollectionError::from)?;
            let doc = deserialize_document(&doc_bytes)?;
            if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                out.push(doc);
            }
            if cursor.next().is_err() {
                break;
            }
        }
        Ok(out)
    }

    fn collect_index_scan(
        &self,
        filter: &Document,
        index_name: &str,
        _index_keys: &Document,
    ) -> CollectionResult<Vec<Document>> {
        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = self
            .session
            .open_cursor(&index_table_name)
            .map_err(CollectionError::from)?;
        let mut out = Vec::new();
        if index_cursor.next().is_err() {
            return Ok(out);
        }
        loop {
            let id_str = index_cursor.get_value_str().map_err(CollectionError::from)?;
            if let Some(doc) = self.fetch_doc_by_id_str(&id_str)? {
                if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                    out.push(doc);
                }
            }
            if index_cursor.next().is_err() {
                break;
            }
        }
        Ok(out)
    }

    fn collect_index_seek(
        &self,
        filter: &Document,
        index_name: &str,
        index_keys: &Document,
        seek_values: &Document,
    ) -> CollectionResult<Vec<Document>> {
        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = self
            .session
            .open_cursor(&index_table_name)
            .map_err(CollectionError::from)?;
        let seek_key = build_seek_prefix(index_keys, seek_values);
        let mut out = Vec::new();
        index_cursor.set_key_raw(&seek_key);
        match index_cursor.search_near() {
            Ok(exact) => {
                if exact < 0 && index_cursor.next().is_err() {
                    return Ok(out);
                }
            }
            Err(_) => return Ok(out),
        }
        loop {
            let index_key_raw = index_cursor.get_key_raw().map_err(CollectionError::from)?;
            if !index_key_raw.starts_with(&seek_key) {
                break;
            }
            let id_str = index_cursor.get_value_str().map_err(CollectionError::from)?;
            if let Some(doc) = self.fetch_doc_by_id_str(&id_str)? {
                if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                    out.push(doc);
                }
            }
            if index_cursor.next().is_err() {
                break;
            }
        }
        Ok(out)
    }

    fn collect_covering_index_scan(
        &self,
        filter: &Document,
        index_name: &str,
        index_keys: &Document,
        seek_values: Option<&Document>,
        projection: &Document,
    ) -> CollectionResult<Vec<Document>> {
        eprintln!("  index_name: {}", index_name);
        eprintln!("  index_keys: {:?}", index_keys);
        eprintln!("  seek_values: {:?}", seek_values);
        eprintln!("  projection: {:?}", projection);

        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = self
            .session
            .open_cursor(&index_table_name)
            .map_err(CollectionError::from)?;

        let mut out = Vec::new();

        // If we have seek values, position cursor at the seek point
        if let Some(seek_vals) = seek_values {
            let seek_key = build_seek_prefix(index_keys, seek_vals);
            index_cursor.set_key_raw(&seek_key);
            match index_cursor.search_near() {
                Ok(exact) => {
                    if exact < 0 && index_cursor.next().is_err() {
                        return Ok(out);
                    }
                }
                Err(_) => return Ok(out),
            }

            // Scan matching prefix
            loop {
                let index_key_raw = index_cursor.get_key_raw().map_err(CollectionError::from)?;
                if !index_key_raw.starts_with(&seek_key) {
                    break;
                }

                // Decode index key into field values
                if let Some(index_doc) = decode_index_key(&index_key_raw, index_keys) {

                    // Apply filter FIRST (to full index doc)
                    let filter_result = eval_query(&index_doc, filter).map_err(CollectionError::QueryError)?;

                    if filter_result {
                        // Build projected document from index data
                        let mut projected = apply_projection_to_index_doc(&index_doc, projection);

                        // Add _id if needed
                        if should_include_id(projection) {
                            let id_str = index_cursor.get_value_str().map_err(CollectionError::from)?;
                            projected.insert("_id".to_string(), Bson::String(id_str));
                        }

                        out.push(projected);
                    } else {
                    }
                }

                if index_cursor.next().is_err() {
                    break;
                }
            }
        } else {
            // Full index scan (range query)
            if index_cursor.next().is_err() {
                return Ok(out);
            }

            loop {
                let index_key_raw = index_cursor.get_key_raw().map_err(CollectionError::from)?;

                // Decode index key into field values
                if let Some(index_doc) = decode_index_key(&index_key_raw, index_keys) {
                    // Apply filter FIRST (to full index doc)
                    if eval_query(&index_doc, filter).map_err(CollectionError::QueryError)? {
                        // Build projected document from index data
                        let mut projected = apply_projection_to_index_doc(&index_doc, projection);

                        // Add _id if needed
                        if should_include_id(projection) {
                            let id_str = index_cursor.get_value_str().map_err(CollectionError::from)?;
                            projected.insert("_id".to_string(), Bson::String(id_str));
                        }

                        out.push(projected);
                    }
                }

                if index_cursor.next().is_err() {
                    break;
                }
            }
        }

        Ok(out)
    }

    /// Execute a bitmap index scan: load the bitmap from the index table,
    /// look up matching positions, resolve doc IDs, fetch and post-filter.
    #[cfg(not(target_arch = "wasm32"))]
    fn collect_bitmap_scan(
        &self,
        filter: &Document,
        index_name: &str,
        field: &str,
    ) -> CollectionResult<Vec<Document>> {
        use crate::index::bitmap_index::BitmapIndex;

        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = match self.session.open_cursor(&index_table_name) {
            Ok(c) => c,
            Err(_) => return self.collect_collection_scan(filter),
        };

        let mut bitmap = BitmapIndex::new();
        if index_cursor.next().is_ok() {
            loop {
                let key_raw = index_cursor.get_key_raw().map_err(CollectionError::from)?;
                let id_str = index_cursor.get_value_str().map_err(CollectionError::from)?;
                bitmap.insert(&id_str, &key_raw);
                if index_cursor.next().is_err() {
                    break;
                }
            }
        }

        let seek_bytes = match filter.get(field) {
            Some(Bson::String(s)) => Some(s.as_bytes().to_vec()),
            Some(Bson::Int32(n)) => Some(n.to_be_bytes().to_vec()),
            Some(Bson::Int64(n)) => Some(n.to_be_bytes().to_vec()),
            Some(Bson::Boolean(b)) => Some(vec![if *b { 0x01 } else { 0x00 }]),
            Some(Bson::Document(d)) if d.contains_key("$in") => None,
            _ => None,
        };

        let positions = if let Some(ref sb) = seek_bytes {
            bitmap.lookup(sb)
        } else if let Some(Bson::Document(d)) = filter.get(field) {
            if let Some(Bson::Array(arr)) = d.get("$in") {
                let vals: Vec<Vec<u8>> = arr
                    .iter()
                    .filter_map(|v| match v {
                        Bson::String(s) => Some(s.as_bytes().to_vec()),
                        Bson::Int32(n) => Some(n.to_be_bytes().to_vec()),
                        Bson::Int64(n) => Some(n.to_be_bytes().to_vec()),
                        _ => None,
                    })
                    .collect();
                bitmap.lookup_in(&vals)
            } else {
                return self.collect_collection_scan(filter);
            }
        } else {
            return self.collect_collection_scan(filter);
        };

        let ids = bitmap.positions_to_ids(&positions);
        let mut out = Vec::new();
        for id_str in ids {
            if let Some(doc) = self.fetch_doc_by_id_str(&id_str)? {
                if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                    out.push(doc);
                }
            }
        }
        Ok(out)
    }

    #[cfg(target_arch = "wasm32")]
    fn collect_bitmap_scan(
        &self,
        filter: &Document,
        _index_name: &str,
        _field: &str,
    ) -> CollectionResult<Vec<Document>> {
        self.collect_collection_scan(filter)
    }

    /// Execute a prefix-truncated B-tree scan: seek to the prefix, collect
    /// candidate doc IDs from entries sharing that prefix, then fetch and
    /// post-filter.
    fn collect_prefix_index_scan(
        &self,
        filter: &Document,
        index_name: &str,
        index_keys: &Document,
        prefix_length: usize,
    ) -> CollectionResult<Vec<Document>> {
        use crate::index::prefix_index::truncate_key;

        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = match self.session.open_cursor(&index_table_name) {
            Ok(c) => c,
            Err(_) => return self.collect_collection_scan(filter),
        };

        let seek_doc = Document::from_iter(
            index_keys
                .keys()
                .filter_map(|k| filter.get(k).map(|v| (k.clone(), v.clone()))),
        );
        if seek_doc.is_empty() {
            return self.collect_index_scan(filter, index_name, index_keys);
        }

        let full_key = extract_index_key(&seek_doc, index_keys);
        let prefix = truncate_key(&full_key, prefix_length);

        index_cursor.set_key_raw(&prefix);
        match index_cursor.search_near() {
            Ok(exact) => {
                if exact < 0 && index_cursor.next().is_err() {
                    return Ok(Vec::new());
                }
            }
            Err(_) => return Ok(Vec::new()),
        }

        let mut out = Vec::new();
        loop {
            let key_raw = index_cursor.get_key_raw().map_err(CollectionError::from)?;
            if !key_raw.starts_with(&prefix) {
                break;
            }
            let id_str = index_cursor.get_value_str().map_err(CollectionError::from)?;
            if let Some(doc) = self.fetch_doc_by_id_str(&id_str)? {
                if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                    out.push(doc);
                }
            }
            if index_cursor.next().is_err() {
                break;
            }
        }
        Ok(out)
    }

    /// Execute a text index scan: build a transient TextIndex from all
    /// collection documents, search with the query text, fetch by ID, and
    /// post-filter.
    #[cfg(not(target_arch = "wasm32"))]
    fn collect_text_index_scan(
        &self,
        filter: &Document,
        _index_name: &str,
        fields: &[String],
    ) -> CollectionResult<Vec<Document>> {
        use crate::index::text_index::TextIndex;

        let search_text = match filter.get("$text") {
            Some(Bson::Document(td)) => match td.get("$search") {
                Some(Bson::String(s)) => s.clone(),
                _ => return self.collect_collection_scan(filter),
            },
            Some(Bson::String(s)) => s.clone(),
            _ => return self.collect_collection_scan(filter),
        };

        let all_docs = self.collect_collection_scan(&Document::new())?;
        let text_idx = TextIndex::build(&all_docs, fields, None);
        let scored = text_idx.search(&search_text, None);

        let id_set: HashSet<String> = scored.into_iter().map(|(id, _)| id).collect();

        let mut out = Vec::new();
        for doc in &all_docs {
            let id_str = extract_id_string(doc)?;
            if id_set.contains(&id_str) {
                let non_text_filter: Document = filter
                    .iter()
                    .filter(|(k, _)| k.as_str() != "$text")
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if non_text_filter.is_empty()
                    || eval_query(doc, &non_text_filter).map_err(CollectionError::QueryError)?
                {
                    out.push(doc.clone());
                }
            }
        }
        Ok(out)
    }

    #[cfg(target_arch = "wasm32")]
    fn collect_text_index_scan(
        &self,
        filter: &Document,
        _index_name: &str,
        _fields: &[String],
    ) -> CollectionResult<Vec<Document>> {
        self.collect_collection_scan(filter)
    }

    /// Execute a vector index search: build a transient VectorIndex from all
    /// collection documents, run ANN search, fetch matching documents, and
    /// post-filter.
    #[cfg(not(target_arch = "wasm32"))]
    fn collect_vector_index_search(
        &self,
        filter: &Document,
        _index_name: &str,
        field: &str,
        dimensions: usize,
        metric: &str,
    ) -> CollectionResult<Vec<Document>> {
        use crate::index::vector_index::VectorIndex;

        let (query_vec, k) = match extract_vector_query(filter, field) {
            Some(v) => v,
            None => return self.collect_collection_scan(filter),
        };

        let all_docs = self.collect_collection_scan(&Document::new())?;
        let vec_idx = VectorIndex::build(&all_docs, field, dimensions, metric);
        let results = vec_idx.search(&query_vec, k);

        let id_set: HashSet<String> = results.into_iter().map(|(id, _)| id).collect();

        let mut out = Vec::new();
        for doc in &all_docs {
            let id_str = extract_id_string(doc)?;
            if id_set.contains(&id_str) {
                out.push(doc.clone());
            }
        }
        Ok(out)
    }

    #[cfg(target_arch = "wasm32")]
    fn collect_vector_index_search(
        &self,
        filter: &Document,
        _index_name: &str,
        _field: &str,
        _dimensions: usize,
        _metric: &str,
    ) -> CollectionResult<Vec<Document>> {
        self.collect_collection_scan(filter)
    }

    /// Insert a single document into the collection
    ///
    /// If the document does not have an `_id` field, one will be generated.
    ///
    /// # Arguments
    ///
    /// * `document` - The document to insert
    ///
    /// # Returns
    ///
    /// `InsertOneResult` containing the inserted document's `_id`
    pub fn insert_one(&self, document: Document) -> CollectionResult<InsertOneResult> {
        self.insert_one_with_options(document, InsertOptions::default())
    }

    pub fn insert_one_with_options(
        &self,
        document: Document,
        opts: InsertOptions,
    ) -> CollectionResult<InsertOneResult> {
        self.with_oplog_transaction(|col| col.insert_one_inner(document, opts.internal))
    }

    fn insert_one_inner(&self, mut document: Document, internal: bool) -> CollectionResult<InsertOneResult> {
        // Ensure document has _id
        let inserted_id = ensure_id(&mut document);

        self.validate_doc(&document)?;

        // Check unique constraints before inserting
        self.insert_into_indexes(&document)?;

        // Serialize document
        let doc_bytes = serialize_document(&document)?;

        // Extract _id as string key
        let key_str = extract_id_string(&document)?;

        // Insert into storage
        let mut cursor = self.cursor()?;
        cursor.set_key_str(&key_str);
        cursor.set_value_raw(&doc_bytes);
        cursor.insert()?;

        self.append_oplog_if_enabled(
            "insert",
            inserted_id.clone(),
            Some(document),
            internal,
            None,
        )?;

        Ok(InsertOneResult { inserted_id })
    }

    /// Insert multiple documents into the collection
    ///
    /// If any document does not have an `_id` field, one will be generated.
    ///
    /// # Arguments
    ///
    /// * `documents` - The documents to insert
    ///
    /// # Returns
    ///
    /// `InsertManyResult` containing the inserted documents' `_id`s
    pub fn insert_many(&self, documents: Vec<Document>) -> CollectionResult<InsertManyResult> {
        self.insert_many_with_options(documents, InsertOptions::default())
    }

    pub fn insert_many_with_options(
        &self,
        mut documents: Vec<Document>,
        opts: InsertOptions,
    ) -> CollectionResult<InsertManyResult> {
        self.with_oplog_transaction(|col| {
            let mut inserted_ids = Vec::with_capacity(documents.len());
            let mut cursor = col.cursor()?;

            for document in &mut documents {
                let inserted_id = ensure_id(document);
                inserted_ids.push(inserted_id.clone());

                col.validate_doc(document)?;
                col.insert_into_indexes(document)?;
                let doc_bytes = serialize_document(document)?;
                let key_str = extract_id_string(document)?;

                cursor.set_key_str(&key_str);
                cursor.set_value_raw(&doc_bytes);
                cursor.insert()?;

                col.append_oplog_if_enabled(
                    "insert",
                    inserted_id,
                    Some(document.clone()),
                    opts.internal,
                    None,
                )?;
            }

            Ok(InsertManyResult { inserted_ids })
        })
    }

    /// Find a single document matching the query filter
    ///
    /// # Arguments
    ///
    /// * `filter` - Query filter (empty document matches all)
    ///
    /// # Returns
    ///
    /// The first matching document, or `None` if no match found
    pub fn find_one(&self, filter: Document) -> CollectionResult<Option<Document>> {
        // Use query planner to optimize the query
        let indexes = self.list_indexes()?;
        let plan = plan_query(&filter, &indexes);

        match &plan.execution_plan {
            ExecutionPlan::IndexSeek {
                index_name,
                index_keys,
                seek_values,
            } => self.find_one_with_index_seek(
                &filter,
                index_name,
                index_keys,
                seek_values,
            ),
            ExecutionPlan::IndexScan {
                index_name,
                index_keys,
            } => self.find_one_with_index_scan(&filter, index_name, index_keys),
            ExecutionPlan::CollectionScan => self.find_one_with_collection_scan(&filter),
            // All other plans (covering, sorted, geo, bitmap, text, prefix,
            // vector, or-union) materialize through execute_plan.
            _ => {
                Ok(self
                    .execute_plan(&plan.execution_plan, &filter)?
                    .into_iter()
                    .next())
            }
        }
    }

    /// Return a streaming iterator over all documents matching the filter.
    ///
    /// Unlike [`find`], this does **not** materialize the full result set.
    /// Documents are deserialized and filtered lazily as the iterator is
    /// advanced, using the query planner to choose the best access strategy
    /// (collection scan, index scan, or index seek).
    pub fn find_iter(&self, filter: Document) -> CollectionResult<FindCursor<'_, S::Cursor>> {
        let indexes = self.list_indexes()?;
        let plan = plan_query(&filter, &indexes);

        let state = match &plan.execution_plan {
            ExecutionPlan::IndexSeek {
                index_name,
                index_keys,
                seek_values,
            } => {
                let index_table = format!("{}.idx_{}", self.collection_name, index_name);
                let index_cursor = self.session.open_cursor(&index_table)?;
                let data_cursor = self.cursor()?;
                let seek_key = build_seek_prefix(index_keys, seek_values);
                FindCursorState::IndexSeek {
                    index_cursor,
                    data_cursor,
                    seek_key,
                    positioned: false,
                }
            }
            ExecutionPlan::IndexScan {
                index_name,
                index_keys: _,
            } => {
                let index_table = format!("{}.idx_{}", self.collection_name, index_name);
                let index_cursor = self.session.open_cursor(&index_table)?;
                let data_cursor = self.cursor()?;
                FindCursorState::IndexScan {
                    index_cursor,
                    data_cursor,
                }
            }
            ExecutionPlan::CollectionScan => {
                let cursor = self.cursor()?;
                FindCursorState::CollectionScan { cursor }
            }
            // All other plan types (covering, sorted, geo, bitmap, text,
            // prefix, vector, or-union) materialize through execute_plan.
            _ => {
                let docs = self.execute_plan(&plan.execution_plan, &filter)?;
                FindCursorState::Materialized {
                    docs,
                    next_ix: 0,
                }
            }
        };

        Ok(FindCursor {
            state,
            filter,
            _lifetime: PhantomData,
        })
    }

    /// Find all documents matching the query filter
    ///
    /// # Arguments
    ///
    /// * `filter` - Query filter (empty document matches all)
    ///
    /// # Returns
    ///
    /// Vector of all matching documents
    pub fn find(&self, filter: Document) -> CollectionResult<Vec<Document>> {
        self.find_iter(filter)?.collect()
    }

    /// Find all documents matching the filter, with sort/skip/limit/projection.
    ///
    /// Uses [`plan_query_full`] so that covering indexes, sorted index scans,
    /// and other combined optimizations can fire when the caller provides
    /// projection, sort, and/or limit.
    pub fn find_with_options(
        &self,
        filter: Document,
        options: FindOptions,
    ) -> CollectionResult<Vec<Document>> {
        use crate::aggregation::stages;
        use crate::planner::plan_query_full;

        let indexes = self.list_indexes()?;
        let plan = plan_query_full(
            &filter,
            &indexes,
            options.projection.as_ref(),
            options.sort.as_ref(),
            options.limit,
        );

        // If the planner chose SortedIndexScan, we can skip the post-sort.
        let skip_post_sort = matches!(
            plan.execution_plan,
            ExecutionPlan::SortedIndexScan { .. }
        );

        let mut docs = self.execute_plan(&plan.execution_plan, &filter)?;

        if !skip_post_sort {
            if let Some(ref sort_doc) = options.sort {
                docs = stages::stage_sort(docs, &Bson::Document(sort_doc.clone()))
                    .map_err(|e| CollectionError::Other(e.to_string()))?;
            }
        }

        if let Some(n) = options.skip {
            if n > 0 {
                docs = stages::stage_skip(docs, &Bson::Int64(n))
                    .map_err(|e| CollectionError::Other(e.to_string()))?;
            }
        }

        // SortedIndexScan already limits, but apply limit for other plans.
        if !skip_post_sort {
            if let Some(n) = options.limit {
                if n > 0 {
                    docs = stages::stage_limit(docs, &Bson::Int64(n))
                        .map_err(|e| CollectionError::Other(e.to_string()))?;
                }
            }
        }

        if let Some(ref proj_doc) = options.projection {
            docs = stages::stage_project(docs, &Bson::Document(proj_doc.clone()))
                .map_err(|e| CollectionError::Other(e.to_string()))?;
        }

        Ok(docs)
    }

    /// Find one document with options (projection, sort to pick which "first").
    pub fn find_one_with_options(
        &self,
        filter: Document,
        options: FindOptions,
    ) -> CollectionResult<Option<Document>> {
        let find_opts = FindOptions {
            limit: Some(1),
            ..options
        };
        let mut docs = self.find_with_options(filter, find_opts)?;
        Ok(if docs.is_empty() { None } else { Some(docs.swap_remove(0)) })
    }

    // ============================================================
    // EXPLAIN OPERATIONS (Phase 9)
    // ============================================================

    /// Explain how a find_one query would execute without running it
    ///
    /// # Arguments
    ///
    /// * `filter` - Query filter to explain
    ///
    /// # Returns
    ///
    /// ExplainResult showing execution plan and estimated statistics
    ///
    /// # Example
    ///
    /// ```ignore
    /// let explain = collection.explain_find_one(doc! { "email": "alice@example.com" })?;
    /// println!("Plan: {:?}", explain.execution_plan);
    /// println!("Index: {:?}", explain.index_used);
    /// println!("{}", explain.summary());
    /// ```
    pub fn explain_find_one(&self, filter: Document) -> CollectionResult<ExplainResult> {
        let indexes = self.list_indexes()?;
        let plan = plan_query(&filter, &indexes);

        let mut explain = ExplainResult::new(filter.clone(), plan.execution_plan.clone(), plan.reason);

        // Estimate statistics by sampling the collection
        self.estimate_query_stats(&filter, &plan.execution_plan, &mut explain.execution_stats)?;

        Ok(explain)
    }

    /// Explain how a find query would execute without running it
    ///
    /// # Arguments
    ///
    /// * `filter` - Query filter to explain
    ///
    /// # Returns
    ///
    /// ExplainResult showing execution plan and estimated statistics
    ///
    /// # Example
    ///
    /// ```ignore
    /// let explain = collection.explain_find(doc! { "age": { "$gte": 18 } })?;
    /// println!("Would examine {} documents", explain.execution_stats.documents_examined);
    /// println!("Would return {} documents", explain.execution_stats.documents_returned);
    /// println!("Efficiency: {:.1}%", explain.efficiency() * 100.0);
    /// ```
    pub fn explain_find(&self, filter: Document) -> CollectionResult<ExplainResult> {
        let indexes = self.list_indexes()?;
        let plan = plan_query(&filter, &indexes);

        let mut explain = ExplainResult::new(filter.clone(), plan.execution_plan.clone(), plan.reason);

        // Estimate statistics by sampling the collection
        self.estimate_query_stats(&filter, &plan.execution_plan, &mut explain.execution_stats)?;

        Ok(explain)
    }

    /// Explain how an aggregation pipeline's initial data fetch would execute.
    ///
    /// Runs the pipeline optimizer to extract any leading `$match` stages,
    /// then explains the resulting `find()` the same way `explain_find` does.
    /// This shows whether the pipeline benefits from index usage.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let explain = collection.explain_aggregate(vec![
    ///     doc! { "$match": { "status": "active" } },
    ///     doc! { "$group": { "_id": "$dept", "count": { "$count": {} } } },
    /// ])?;
    /// println!("{}", explain.summary());
    /// ```
    pub fn explain_aggregate(&self, pipeline: Vec<Document>) -> CollectionResult<ExplainResult> {
        let (leading_match, _remaining) =
            crate::aggregation::optimize_pipeline(&pipeline);
        let filter = leading_match.unwrap_or_default();
        self.explain_find(filter)
    }

    /// Estimate query statistics by analyzing the collection
    fn estimate_query_stats(
        &self,
        filter: &Document,
        execution_plan: &ExecutionPlan,
        stats: &mut ExecutionStats,
    ) -> CollectionResult<()> {
        // For explain, we do a full scan to get accurate statistics
        // In production, this could be optimized with sampling

        match execution_plan {
            ExecutionPlan::CollectionScan => {
                // Count all documents and matching documents
                let mut cursor = self.cursor()?;
                if cursor.next().is_err() {
                    return Ok(());
                }

                loop {
                    stats.inc_documents_examined();

                    let doc_bytes = cursor.get_value_raw()?;
                    let doc = deserialize_document(&doc_bytes)?;

                    if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                        stats.inc_documents_returned();
                    }

                    if cursor.next().is_err() {
                        break;
                    }
                }
            }
            ExecutionPlan::IndexScan { index_name, .. }
            | ExecutionPlan::IndexSeek { index_name, .. }
            | ExecutionPlan::CoveringIndexScan { index_name, .. }
            | ExecutionPlan::SortedIndexScan { index_name, .. }
            | ExecutionPlan::GeoNear { index_name, .. }
            | ExecutionPlan::GeoCapWithin { index_name, .. }
            | ExecutionPlan::GeoCellCover { index_name, .. } => {
                let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
                let mut index_cursor = self.session.open_cursor(&index_table_name)?;

                if index_cursor.next().is_err() {
                    return Ok(());
                }

                let is_covering = matches!(execution_plan, ExecutionPlan::CoveringIndexScan { .. });

                loop {
                    stats.inc_index_entries_examined();

                    if !is_covering {
                        let id_str = index_cursor.get_value_str()?;

                        let mut data_cursor = self.cursor()?;
                        data_cursor.set_key_str(&id_str);
                        if data_cursor.search().is_ok() {
                            stats.inc_documents_examined();

                            let doc_bytes = data_cursor.get_value_raw()?;
                            let doc = deserialize_document(&doc_bytes)?;

                            if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                                stats.inc_documents_returned();
                            }
                        }
                    } else {
                        stats.inc_documents_returned();
                    }

                    if index_cursor.next().is_err() {
                        break;
                    }
                }
            }
            // Plans without a traditional index table — materialize and count.
            ExecutionPlan::OrUnionPlans { .. }
            | ExecutionPlan::VectorIndexSearch { .. }
            | ExecutionPlan::BitmapScan { .. }
            | ExecutionPlan::TextIndexScan { .. }
            | ExecutionPlan::PrefixIndexScan { .. } => {
                let docs = self.execute_plan(execution_plan, filter)?;
                for _ in docs {
                    stats.inc_documents_examined();
                    stats.inc_documents_returned();
                }
            }
        }

        Ok(())
    }

    /// Update a single document matching the query filter
    ///
    /// # Arguments
    ///
    /// * `filter` - Query filter to match documents
    /// * `update` - Update operations to apply
    ///
    /// # Returns
    ///
    /// `UpdateResult` with matched and modified counts
    pub fn update_one(
        &self,
        filter: Document,
        update: Document,
    ) -> CollectionResult<UpdateResult> {
        self.update_one_with_options(filter, update, UpdateOptions::default())
    }

    /// Update one document with options (e.g. upsert).
    pub fn update_one_with_options(
        &self,
        filter: Document,
        update: Document,
        options: UpdateOptions,
    ) -> CollectionResult<UpdateResult> {
        self.with_oplog_transaction(|col| col.update_one_inner(filter, update, options))
    }

    fn update_one_inner(
        &self,
        filter: Document,
        update: Document,
        options: UpdateOptions,
    ) -> CollectionResult<UpdateResult> {
        let mut cursor = self.cursor()?;
        let mut matched_count = 0;
        let mut modified_count = 0;

        let mut found = false;
        if cursor.next().is_ok() {
            loop {
                let doc_bytes = cursor.get_value_raw()?;
                let mut doc = deserialize_document(&doc_bytes)?;

                let matches = eval_query(&doc, &filter).map_err(CollectionError::QueryError)?;

                if matches {
                    found = true;
                    matched_count += 1;

                    let original_doc = doc.clone();

                    apply_update(&mut doc, &update).map_err(CollectionError::UpdateError)?;

                    if doc != original_doc {
                        modified_count += 1;
                        self.update_in_indexes(&original_doc, &doc)?;
                        let updated_bytes = serialize_document(&doc)?;
                        cursor.set_value_raw(&updated_bytes);
                        cursor.update()?;

                        let id = doc
                            .get("_id")
                            .cloned()
                            .ok_or(CollectionError::MissingIdError)?;
                        let changed = Some(Self::doc_top_level_changed(&original_doc, &doc));
                        self.append_oplog_if_enabled(
                            "update",
                            id,
                            Some(update.clone()),
                            options.internal,
                            changed,
                        )?;
                    }

                    break;
                }

                if cursor.next().is_err() {
                    break;
                }
            }
        }

        if !found && options.upsert {
            let mut new_doc = extract_equality_fields(&filter);
            if !new_doc.contains_key("_id") {
                new_doc.insert("_id", Bson::ObjectId(ObjectId::new()));
            }
            crate::update::apply_update_for_upsert(&mut new_doc, &update)
                .map_err(CollectionError::UpdateError)?;
            let upserted_id = new_doc.get("_id").cloned().unwrap_or(Bson::Null);
            self.insert_one_inner(new_doc, options.internal)?;
            return Ok(UpdateResult {
                matched_count: 0,
                modified_count: 0,
                upserted_id: Some(upserted_id),
            });
        }

        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id: None,
        })
    }

    /// Update all documents matching the query filter
    ///
    /// # Arguments
    ///
    /// * `filter` - Query filter to match documents
    /// * `update` - Update operations to apply
    ///
    /// # Returns
    ///
    /// `UpdateResult` with matched and modified counts
    pub fn update_many(
        &self,
        filter: Document,
        update: Document,
    ) -> CollectionResult<UpdateResult> {
        self.update_many_with_options(filter, update, UpdateOptions::default())
    }

    /// Update many documents with options (e.g. upsert).
    pub fn update_many_with_options(
        &self,
        filter: Document,
        update: Document,
        options: UpdateOptions,
    ) -> CollectionResult<UpdateResult> {
        self.with_oplog_transaction(|col| col.update_many_inner(filter, update, options))
    }

    fn update_many_inner(
        &self,
        filter: Document,
        update: Document,
        options: UpdateOptions,
    ) -> CollectionResult<UpdateResult> {
        let mut cursor = self.cursor()?;
        let mut matched_count = 0;
        let mut modified_count = 0;

        if cursor.next().is_ok() {
            loop {
                let doc_bytes = cursor.get_value_raw()?;
                let mut doc = deserialize_document(&doc_bytes)?;

                let matches = eval_query(&doc, &filter).map_err(CollectionError::QueryError)?;

                if matches {
                    matched_count += 1;

                    let original_doc = doc.clone();

                    apply_update(&mut doc, &update).map_err(CollectionError::UpdateError)?;

                    if doc != original_doc {
                        modified_count += 1;
                        self.update_in_indexes(&original_doc, &doc)?;
                        let updated_bytes = serialize_document(&doc)?;
                        cursor.set_value_raw(&updated_bytes);
                        cursor.update()?;

                        let id = doc
                            .get("_id")
                            .cloned()
                            .ok_or(CollectionError::MissingIdError)?;
                        let changed = Some(Self::doc_top_level_changed(&original_doc, &doc));
                        self.append_oplog_if_enabled(
                            "update",
                            id,
                            Some(update.clone()),
                            options.internal,
                            changed,
                        )?;
                    }
                }

                if cursor.next().is_err() {
                    break;
                }
            }
        }

        if matched_count == 0 && options.upsert {
            let mut new_doc = extract_equality_fields(&filter);
            if !new_doc.contains_key("_id") {
                new_doc.insert("_id", Bson::ObjectId(ObjectId::new()));
            }
            crate::update::apply_update_for_upsert(&mut new_doc, &update)
                .map_err(CollectionError::UpdateError)?;
            let upserted_id = new_doc.get("_id").cloned().unwrap_or(Bson::Null);
            self.insert_one_inner(new_doc, options.internal)?;
            return Ok(UpdateResult {
                matched_count: 0,
                modified_count: 0,
                upserted_id: Some(upserted_id),
            });
        }

        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id: None,
        })
    }

    /// Delete a single document matching the query filter
    ///
    /// # Arguments
    ///
    /// * `filter` - Query filter to match documents
    ///
    /// # Returns
    ///
    /// `DeleteResult` with deleted count
    pub fn delete_one(&self, filter: Document) -> CollectionResult<DeleteResult> {
        self.delete_one_with_options(filter, DeleteOptions::default())
    }

    pub fn delete_one_with_options(
        &self,
        filter: Document,
        options: DeleteOptions,
    ) -> CollectionResult<DeleteResult> {
        self.with_oplog_transaction(|col| col.delete_one_inner(filter, options))
    }

    fn delete_one_inner(
        &self,
        filter: Document,
        options: DeleteOptions,
    ) -> CollectionResult<DeleteResult> {
        let mut cursor = self.cursor()?;
        let mut deleted_count = 0;

        if cursor.next().is_err() {
            return Ok(DeleteResult { deleted_count });
        }

        loop {
            let doc_bytes = cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;
            let matches = eval_query(&doc, &filter).map_err(CollectionError::QueryError)?;

            if matches {
                let id = doc
                    .get("_id")
                    .cloned()
                    .ok_or(CollectionError::MissingIdError)?;
                self.remove_from_indexes(&doc)?;
                cursor.remove()?;
                deleted_count += 1;
                self.append_oplog_if_enabled("delete", id, None, options.internal, None)?;
                break;
            }

            if cursor.next().is_err() {
                break;
            }
        }

        Ok(DeleteResult { deleted_count })
    }

    /// Delete all documents matching the query filter
    ///
    /// # Arguments
    ///
    /// * `filter` - Query filter to match documents
    ///
    /// # Returns
    ///
    /// `DeleteResult` with deleted count
    pub fn delete_many(&self, filter: Document) -> CollectionResult<DeleteResult> {
        self.delete_many_with_options(filter, DeleteOptions::default())
    }

    pub fn delete_many_with_options(
        &self,
        filter: Document,
        options: DeleteOptions,
    ) -> CollectionResult<DeleteResult> {
        self.with_oplog_transaction(|col| col.delete_many_inner(filter, options))
    }

    fn delete_many_inner(
        &self,
        filter: Document,
        options: DeleteOptions,
    ) -> CollectionResult<DeleteResult> {
        let mut cursor = self.cursor()?;
        let mut deleted_count = 0;

        if cursor.next().is_err() {
            return Ok(DeleteResult { deleted_count });
        }

        loop {
            let doc_bytes = cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;
            let matches = eval_query(&doc, &filter).map_err(CollectionError::QueryError)?;

            if matches {
                let id = doc
                    .get("_id")
                    .cloned()
                    .ok_or(CollectionError::MissingIdError)?;
                self.remove_from_indexes(&doc)?;
                cursor.remove()?;
                deleted_count += 1;
                self.append_oplog_if_enabled("delete", id, None, options.internal, None)?;
            }

            if cursor.next().is_err() {
                break;
            }
        }

        Ok(DeleteResult { deleted_count })
    }

    /// Count documents matching the query filter
    ///
    /// # Arguments
    ///
    /// * `filter` - Optional query filter (None or empty document matches all)
    ///
    /// # Returns
    ///
    /// Number of matching documents
    pub fn count_documents(&self, filter: Option<Document>) -> CollectionResult<u64> {
        let filter = filter.unwrap_or_default();
        let mut count = 0;
        let mut cursor = self.cursor()?;

        // Position cursor at first record
        if cursor.next().is_err() {
            return Ok(0); // Empty collection
        }

        loop {
            // Get value bytes
            let doc_bytes = cursor.get_value_raw()?;

            // Deserialize document
            let doc = deserialize_document(&doc_bytes)?;

            // Evaluate query
            let matches = eval_query(&doc, &filter).map_err(CollectionError::QueryError)?;

            if matches {
                count += 1;
            }

            // Move to next document
            if cursor.next().is_err() {
                break;
            }
        }

        Ok(count)
    }

    /// Execute aggregation pipeline as a streaming iterator.
    ///
    /// Returns a lazy iterator that processes documents through the pipeline
    /// with constant memory for consecutive streaming stages.  Blocking
    /// stages materialize only at their own boundary.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut stream = collection.aggregate_stream(vec![
    ///     doc! { "$match": { "status": "active" } },
    ///     doc! { "$project": { "name": 1 } },
    ///     doc! { "$limit": 10 },
    /// ])?;
    /// while let Some(result) = stream.next() {
    ///     let doc = result?;
    ///     println!("{:?}", doc);
    /// }
    /// ```
    pub fn aggregate_stream(
        &self,
        pipeline: Vec<Document>,
    ) -> CollectionResult<crate::aggregation::DocStream> {
        let (leading_match, remaining_pipeline) =
            crate::aggregation::optimize_pipeline(&pipeline);

        let docs = match leading_match {
            Some(filter) => self.find(filter)?,
            None => self.find(Document::new())?,
        };

        crate::aggregation::aggregate_stream(docs, &remaining_pipeline)
            .map_err(|e| CollectionError::Other(format!("Aggregation error: {}", e)))
    }

    /// Execute aggregation pipeline, collecting all results.
    ///
    /// Internally delegates to the streaming pipeline and collects results.
    ///
    /// # Arguments
    ///
    /// * `pipeline` - Array of aggregation stages
    ///
    /// # Returns
    ///
    /// Vector of documents after pipeline execution
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = collection.aggregate(vec![
    ///     doc! { "$match": { "age": { "$gte": 18 } } },
    ///     doc! { "$group": { "_id": "$status", "count": { "$count": {} } } },
    ///     doc! { "$sort": { "count": -1 } },
    /// ])?;
    /// ```
    pub fn aggregate(&self, pipeline: Vec<Document>) -> CollectionResult<Vec<Document>> {
        let stream = self.aggregate_stream(pipeline)?;
        stream
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CollectionError::Other(format!("Aggregation error: {}", e)))
    }

    // ============================================================
    // QUERY EXECUTION STRATEGIES (Phase 8)
    // ============================================================

    /// Find one document using collection scan
    fn find_one_with_collection_scan(&self, filter: &Document) -> CollectionResult<Option<Document>> {
        let mut cursor = self.cursor()?;

        if cursor.next().is_err() {
            return Ok(None);
        }

        loop {
            let doc_bytes = cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;

            if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                return Ok(Some(doc));
            }

            if cursor.next().is_err() {
                break;
            }
        }

        Ok(None)
    }

    /// Find one document using index scan
    fn find_one_with_index_scan(
        &self,
        filter: &Document,
        index_name: &str,
        _index_keys: &Document,
    ) -> CollectionResult<Option<Document>> {
        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = self.session.open_cursor(&index_table_name)?;

        if index_cursor.next().is_err() {
            return Ok(None);
        }

        loop {
            let id_str = index_cursor.get_value_str()?;

            let mut data_cursor = self.cursor()?;
            data_cursor.set_key_str(&id_str);
            if data_cursor.search().is_ok() {
                let doc_bytes = data_cursor.get_value_raw()?;
                let doc = deserialize_document(&doc_bytes)?;

                if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                    return Ok(Some(doc));
                }
            }

            if index_cursor.next().is_err() {
                break;
            }
        }

        Ok(None)
    }

    /// Find one document using index seek (direct lookup)
    fn find_one_with_index_seek(
        &self,
        filter: &Document,
        index_name: &str,
        index_keys: &Document,
        seek_values: &Document,
    ) -> CollectionResult<Option<Document>> {
        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = self.session.open_cursor(&index_table_name)?;

        let seek_key = build_seek_prefix(index_keys, seek_values);

        // Position cursor at or just after the seek prefix via search_near
        index_cursor.set_key_raw(&seek_key);
        match index_cursor.search_near() {
            Ok(exact) => {
                if exact < 0 && index_cursor.next().is_err() {
                    return Ok(None);
                }
            }
            Err(_) => return Ok(None),
        }

        loop {
            let index_key_raw = index_cursor.get_key_raw()?;

            if index_key_raw.starts_with(&seek_key) {
                let id_str = index_cursor.get_value_str()?;

                let mut data_cursor = self.cursor()?;
                data_cursor.set_key_str(&id_str);
                if data_cursor.search().is_ok() {
                    let doc_bytes = data_cursor.get_value_raw()?;
                    let doc = deserialize_document(&doc_bytes)?;

                    if eval_query(&doc, filter).map_err(CollectionError::QueryError)? {
                        return Ok(Some(doc));
                    }
                }
            } else {
                break;
            }

            if index_cursor.next().is_err() {
                break;
            }
        }

        Ok(None)
    }

    // ============================================================
    // INDEX OPERATIONS (Phase 7)
    // ============================================================

    /// Create an index on the collection
    ///
    /// # Arguments
    ///
    /// * `keys` - Index key specification (e.g., `doc! { "email": 1 }` for ascending, -1 for descending)
    /// * `options` - Optional index options (unique, sparse, etc.)
    ///
    /// # Returns
    ///
    /// Index name
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Single-field index
    /// collection.create_index(doc! { "email": 1 }, None)?;
    ///
    /// // Compound index
    /// collection.create_index(doc! { "age": 1, "name": -1 }, None)?;
    ///
    /// // Unique index
    /// collection.create_index(
    ///     doc! { "username": 1 },
    ///     Some(IndexOptions { unique: true, ..Default::default() })
    /// )?;
    /// ```
    pub fn create_index(
        &self,
        keys: Document,
        options: Option<IndexOptions>,
    ) -> CollectionResult<String> {
        // Validate keys
        if keys.is_empty() {
            return Err(CollectionError::InvalidIndexSpec(
                "Index keys cannot be empty".to_string(),
            ));
        }

        let mut opts = options.clone().unwrap_or_default();
        let index_name = match opts.name.take() {
            Some(n) => {
                let t = n.trim();
                if t.is_empty() {
                    generate_index_name(&keys)
                } else {
                    validate_custom_index_name(t).map_err(CollectionError::InvalidIndexSpec)?;
                    t.to_string()
                }
            }
            None => generate_index_name(&keys),
        };
        if is_2dsphere_keys(&keys) {
            #[cfg(target_arch = "wasm32")]
            {
                return Err(CollectionError::InvalidIndexSpec(
                    "2dsphere index is not supported on wasm32".to_string(),
                ));
            }
            #[cfg(not(target_arch = "wasm32"))]
            {
                if keys.len() != 1 {
                    return Err(CollectionError::InvalidIndexSpec(
                        "2dsphere index must be defined on exactly one field".to_string(),
                    ));
                }
                if opts.unique {
                    return Err(CollectionError::InvalidIndexSpec(
                        "unique 2dsphere indexes are not supported".to_string(),
                    ));
                }
            }
        }

        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);

        let existing = self.list_indexes()?;
        if existing.iter().any(|s| s.name == index_name) {
            return Err(CollectionError::IndexAlreadyExists(index_name));
        }

        // Key format: raw index_key_bytes + _id_bytes (B-tree sorting)
        // Value format: string _id (to look up the document in the key_format=S data table)
        self.session.create_table(&index_table_name)?;

        // Store index metadata in a special metadata table
        let metadata_table = format!("{}.indexes_metadata", self.collection_name);
        self.session
            .create_table(&metadata_table)?;

        let index_spec = IndexSpec {
            name: index_name.clone(),
            keys: keys.clone(),
            options: opts,
        };

        // Serialize index spec
        let spec_bytes = bson::to_vec(&index_spec).map_err(|e| {
            CollectionError::Other(format!("Failed to serialize index spec: {}", e))
        })?;

        // Store metadata
        let mut metadata_cursor = self.session.open_cursor(&metadata_table)?;
        metadata_cursor.set_key_str(&index_name);
        metadata_cursor.set_value_raw(&spec_bytes);
        metadata_cursor.insert()?;

        // Build index by scanning existing documents
        self.rebuild_index(&index_name, &keys, &index_spec.options)?;

        let payload = bson::to_document(&index_spec).ok();
        self.append_oplog_if_enabled(
            "index_create",
            Bson::String(index_name.clone()),
            payload,
            false,
            None,
        )?;

        Ok(index_name)
    }

    /// Drop an index from the collection
    ///
    /// # Arguments
    ///
    /// * `index_name` - Name of the index to drop (or "*" to drop all indexes)
    ///
    /// # Example
    ///
    /// ```ignore
    /// collection.drop_index("email_1")?;
    /// collection.drop_index("*")?; // Drop all indexes
    /// ```
    pub fn drop_index(&self, index_name: &str) -> CollectionResult<()> {
        if index_name == "*" {
            let indexes = self.list_indexes()?;
            for spec in indexes {
                if spec.name != "_id_" {
                    self.drop_index(&spec.name)?;
                }
            }
            return Ok(());
        }

        let drop_session = self.session.open_sibling_session()?;
        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        drop_session.drop_table(&index_table_name)?;

        let metadata_table = format!("{}.indexes_metadata", self.collection_name);
        let mut metadata_cursor = self.session.open_cursor(&metadata_table)?;
        metadata_cursor.set_key_str(index_name);
        if metadata_cursor.search().is_ok() {
            metadata_cursor.remove()?;
        }

        self.append_oplog_if_enabled(
            "index_drop",
            Bson::String(index_name.to_string()),
            None,
            false,
            None,
        )?;

        Ok(())
    }

    /// List all indexes on the collection
    ///
    /// # Returns
    ///
    /// Vector of IndexSpec describing each index
    ///
    /// # Example
    ///
    /// ```ignore
    /// let indexes = collection.list_indexes()?;
    /// for idx in indexes {
    ///     println!("Index: {}, Keys: {:?}", idx.name, idx.keys);
    /// }
    /// ```
    pub fn list_indexes(&self) -> CollectionResult<Vec<IndexSpec>> {
        let metadata_table = format!("{}.indexes_metadata", self.collection_name);

        // Try to open metadata table
        let mut metadata_cursor = match self.session.open_cursor(&metadata_table) {
            Ok(cursor) => cursor,
            Err(_) => {
                // No indexes yet
                return Ok(vec![]);
            }
        };

        let mut indexes = Vec::new();

        // Scan metadata table
        if metadata_cursor.next().is_ok() {
            loop {
                let spec_bytes = metadata_cursor.get_value_raw()?;
                let index_spec: IndexSpec = bson::from_slice(&spec_bytes).map_err(|e| {
                    CollectionError::Other(format!("Failed to deserialize index spec: {}", e))
                })?;
                indexes.push(index_spec);

                if metadata_cursor.next().is_err() {
                    break;
                }
            }
        }

        Ok(indexes)
    }

    /// Clear all entries from a secondary index table (metadata unchanged).
    fn clear_index_table(&self, index_name: &str) -> CollectionResult<()> {
        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = self.session.open_cursor(&index_table_name)?;
        index_cursor.set_key_raw(&[]);
        let mut keys: Vec<Vec<u8>> = Vec::new();
        if index_cursor.search().is_ok() {
            loop {
                keys.push(index_cursor.get_key_raw()?);
                if index_cursor.next().is_err() {
                    break;
                }
            }
        }
        for k in keys {
            index_cursor.set_key_raw(&k);
            if index_cursor.search().is_ok() {
                index_cursor.remove()?;
            }
        }
        Ok(())
    }

    /// Rebuild every secondary index from collection data (`_id_` unchanged).
    ///
    /// Returns the number of secondary indexes rebuilt.
    pub fn rebuild_all_indexes(&self) -> CollectionResult<i64> {
        let indexes = self.list_indexes()?;
        let mut n: i64 = 0;
        for spec in indexes {
            if spec.name == "_id_" {
                continue;
            }
            self.clear_index_table(&spec.name)?;
            self.rebuild_index(&spec.name, &spec.keys, &spec.options)?;
            n += 1;
        }
        Ok(n)
    }

    // ============================================================
    // INTERNAL INDEX MAINTENANCE
    // ============================================================

    /// Check whether any entry in the index B-tree has `prefix` as a key prefix.
    ///
    /// Uses `search_near` to jump close to `prefix` in the sorted key space,
    /// then checks whether the landing position (or its immediate successor)
    /// starts with `prefix`.
    fn index_has_prefix<C: StorageCursor>(
        cursor: &mut C,
        prefix: &[u8],
    ) -> CollectionResult<bool> {
        cursor.set_key_raw(prefix);
        match cursor.search_near() {
            Ok(exact) => {
                if exact < 0 && cursor.next().is_err() {
                    return Ok(false);
                }
                let found = cursor.get_key_raw()?;
                Ok(found.starts_with(prefix))
            }
            Err(_) => Ok(false),
        }
    }

    /// Rebuild an index by scanning all documents
    fn rebuild_index(
        &self,
        index_name: &str,
        keys: &Document,
        options: &IndexOptions,
    ) -> CollectionResult<()> {
        let index_table_name = format!("{}.idx_{}", self.collection_name, index_name);
        let mut index_cursor = self.session.open_cursor(&index_table_name)?;
        let mut data_cursor = self.cursor()?;

        // Scan all documents
        if data_cursor.next().is_err() {
            return Ok(()); // Empty collection
        }

        loop {
            let doc_bytes = data_cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;
            let id_str = extract_id_string(&doc)?;

            if let Some(ref pfe) = options.partial_filter_expression {
                if !pfe.is_empty() && !crate::query::eval_query(&doc, pfe).unwrap_or(false) {
                    if data_cursor.next().is_err() {
                        break;
                    }
                    continue;
                }
            }

            if is_2dsphere_keys(keys) {
                #[cfg(target_arch = "wasm32")]
                {
                    if data_cursor.next().is_err() {
                        break;
                    }
                    continue;
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let Some(combined_key) = twodsphere_index_key(&doc, keys) else {
                        if options.sparse {
                            if data_cursor.next().is_err() {
                                break;
                            }
                            continue;
                        }
                        let field = crate::index::twodsphere_field(keys)
                            .unwrap_or_else(|| "?".to_string());
                        return Err(CollectionError::Other(format!(
                            "2dsphere indexed field '{field}' must be a GeoJSON Point or [longitude, latitude] array"
                        )));
                    };
                    index_cursor.set_key_raw(&combined_key);
                    index_cursor.set_value_str(&id_str);
                    index_cursor.insert()?;
                    if data_cursor.next().is_err() {
                        break;
                    }
                    continue;
                }
            }

            // Extract index key
            let index_key_bytes = extract_index_key(&doc, keys);

            if options.unique
                && Self::index_has_prefix(&mut index_cursor, &index_key_bytes)?
            {
                let field_names: Vec<&str> = keys.keys().map(|s| s.as_str()).collect();
                return Err(CollectionError::UniqueConstraintViolation(format!(
                    "Duplicate key for index on fields: {}",
                    field_names.join(", ")
                )));
            }

            // Combine index key + _id for the key (ensures uniqueness for non-unique indexes)
            let mut combined_key = index_key_bytes.clone();
            combined_key.extend_from_slice(id_str.as_bytes());

            // Insert into index
            index_cursor.set_key_raw(&combined_key);
            index_cursor.set_value_str(&id_str);
            index_cursor.insert()?;

            if data_cursor.next().is_err() {
                break;
            }
        }

        Ok(())
    }

    /// Insert document into all indexes, dispatching by [`IndexType`].
    fn insert_into_indexes(&self, doc: &Document) -> CollectionResult<()> {
        use crate::index::{resolve_index_type, IndexType};

        let indexes = self.list_indexes()?;
        let id_str = extract_id_string(doc)?;

        for index_spec in indexes {
            if let Some(ref pfe) = index_spec.options.partial_filter_expression {
                if !pfe.is_empty() && !crate::query::eval_query(doc, pfe).unwrap_or(false) {
                    continue;
                }
            }

            match resolve_index_type(&index_spec.keys, &index_spec.options) {
                IndexType::TwoDSphere => {
                    #[cfg(target_arch = "wasm32")]
                    {
                        continue;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    {
                        let index_table_name = format!("{}.idx_{}", self.collection_name, index_spec.name);
                        let mut index_cursor = self.session.open_cursor(&index_table_name)?;
                        let Some(combined_key) = twodsphere_index_key(doc, &index_spec.keys) else {
                            if index_spec.options.sparse {
                                continue;
                            }
                            let field = crate::index::twodsphere_field(&index_spec.keys)
                                .unwrap_or_else(|| "?".to_string());
                            return Err(CollectionError::Other(format!(
                                "2dsphere indexed field '{field}' must be a GeoJSON Point or [longitude, latitude] array"
                            )));
                        };
                        index_cursor.set_key_raw(&combined_key);
                        index_cursor.set_value_str(&id_str);
                        index_cursor.insert()?;
                    }
                }
                IndexType::BTree => {
                    let index_table_name = format!("{}.idx_{}", self.collection_name, index_spec.name);
                    let mut index_cursor = self.session.open_cursor(&index_table_name)?;
                    let index_key_bytes = extract_index_key(doc, &index_spec.keys);

                    if index_spec.options.unique
                        && Self::index_has_prefix(&mut index_cursor, &index_key_bytes)?
                    {
                        let field_names: Vec<&str> =
                            index_spec.keys.keys().map(|s| s.as_str()).collect();
                        return Err(CollectionError::UniqueConstraintViolation(format!(
                            "Duplicate key for index '{}' on fields: {}",
                            index_spec.name,
                            field_names.join(", ")
                        )));
                    }

                    let mut combined_key = index_key_bytes;
                    combined_key.extend_from_slice(id_str.as_bytes());
                    index_cursor.set_key_raw(&combined_key);
                    index_cursor.set_value_str(&id_str);
                    index_cursor.insert()?;
                }
                IndexType::Text => {
                    // Text index maintenance: insert postings for each token
                    // in the indexed fields. Table: {coll}.ftx_{name}
                    let table = format!("{}.ftx_{}", self.collection_name, index_spec.name);
                    let fields = crate::index::text_fields(&index_spec.keys);
                    if let Ok(mut cursor) = self.session.open_cursor(&table) {
                        for field in &fields {
                            if let Some(bson::Bson::String(text)) = crate::paths::get_value(doc, field) {
                                #[cfg(not(target_arch = "wasm32"))]
                                for token in crate::index::text_index::tokenize(text) {
                                    let mut key = token.as_bytes().to_vec();
                                    key.push(0xFE);
                                    key.extend_from_slice(id_str.as_bytes());
                                    cursor.set_key_raw(&key);
                                    cursor.set_value_str(&id_str);
                                    let _ = cursor.insert();
                                }
                            }
                        }
                    }
                }
                IndexType::VectorSearch => {
                    // Vector index maintenance: extract vector, store in
                    // serialised graph table {coll}.vidx_{name}.
                    // Full incremental maintenance deferred to IndexProvider.
                }
                IndexType::Bitmap => {
                    // Bitmap index maintenance: update roaring bitmap for
                    // the document's field value. Table: {coll}.bmx_{name}
                    // Full incremental maintenance deferred to IndexProvider.
                }
                IndexType::Prefix => {
                    let prefix_length = index_spec
                        .options
                        .prefix_options
                        .as_ref()
                        .map(|p| p.prefix_length)
                        .unwrap_or(32);
                    let table = format!("{}.pfx_{}", self.collection_name, index_spec.name);
                    if let Ok(mut cursor) = self.session.open_cursor(&table) {
                        let full_key = extract_index_key(doc, &index_spec.keys);
                        let truncated = crate::index::prefix_index::truncate_key(&full_key, prefix_length);
                        let mut combined = truncated;
                        combined.extend_from_slice(id_str.as_bytes());
                        cursor.set_key_raw(&combined);
                        cursor.set_value_str(&id_str);
                        let _ = cursor.insert();
                    }
                }
            }
        }

        Ok(())
    }

    /// Remove document from all indexes, dispatching by [`IndexType`].
    fn remove_from_indexes(&self, doc: &Document) -> CollectionResult<()> {
        use crate::index::{resolve_index_type, IndexType};

        let indexes = self.list_indexes()?;
        let id_str = extract_id_string(doc)?;

        for index_spec in indexes {
            match resolve_index_type(&index_spec.keys, &index_spec.options) {
                IndexType::TwoDSphere => {
                    #[cfg(target_arch = "wasm32")]
                    {
                        continue;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    {
                        let Some(combined_key) = twodsphere_index_key(doc, &index_spec.keys) else {
                            continue;
                        };
                        let table = format!("{}.idx_{}", self.collection_name, index_spec.name);
                        let mut cursor = self.session.open_cursor(&table)?;
                        cursor.set_key_raw(&combined_key);
                        if cursor.search().is_ok() {
                            cursor.remove()?;
                        }
                    }
                }
                IndexType::BTree => {
                    let table = format!("{}.idx_{}", self.collection_name, index_spec.name);
                    let mut cursor = self.session.open_cursor(&table)?;
                    let index_key_bytes = extract_index_key(doc, &index_spec.keys);
                    let mut combined_key = index_key_bytes;
                    combined_key.extend_from_slice(id_str.as_bytes());
                    cursor.set_key_raw(&combined_key);
                    if cursor.search().is_ok() {
                        cursor.remove()?;
                    }
                }
                IndexType::Text => {
                    let table = format!("{}.ftx_{}", self.collection_name, index_spec.name);
                    let fields = crate::index::text_fields(&index_spec.keys);
                    if let Ok(mut cursor) = self.session.open_cursor(&table) {
                        for field in &fields {
                            if let Some(bson::Bson::String(text)) = crate::paths::get_value(doc, field) {
                                #[cfg(not(target_arch = "wasm32"))]
                                for token in crate::index::text_index::tokenize(text) {
                                    let mut key = token.as_bytes().to_vec();
                                    key.push(0xFE);
                                    key.extend_from_slice(id_str.as_bytes());
                                    cursor.set_key_raw(&key);
                                    if cursor.search().is_ok() {
                                        let _ = cursor.remove();
                                    }
                                }
                            }
                        }
                    }
                }
                IndexType::VectorSearch => {
                    // Vector index removal deferred to IndexProvider rebuild.
                }
                IndexType::Bitmap => {
                    // Bitmap removal deferred to IndexProvider rebuild.
                }
                IndexType::Prefix => {
                    let prefix_length = index_spec
                        .options
                        .prefix_options
                        .as_ref()
                        .map(|p| p.prefix_length)
                        .unwrap_or(32);
                    let table = format!("{}.pfx_{}", self.collection_name, index_spec.name);
                    if let Ok(mut cursor) = self.session.open_cursor(&table) {
                        let full_key = extract_index_key(doc, &index_spec.keys);
                        let truncated = crate::index::prefix_index::truncate_key(&full_key, prefix_length);
                        let mut combined = truncated;
                        combined.extend_from_slice(id_str.as_bytes());
                        cursor.set_key_raw(&combined);
                        if cursor.search().is_ok() {
                            let _ = cursor.remove();
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Update document in all indexes (remove old, insert new)
    fn update_in_indexes(&self, old_doc: &Document, new_doc: &Document) -> CollectionResult<()> {
        self.remove_from_indexes(old_doc)?;
        self.insert_into_indexes(new_doc)?;
        Ok(())
    }

    // ============================================================
    // TRANSACTION OPERATIONS
    // ============================================================

    /// Begin a transaction on this collection's storage session.
    ///
    /// All subsequent CRUD operations will be part of the transaction
    /// until [`commit_transaction`] or [`rollback_transaction`] is called.
    pub fn begin_transaction(&self) -> CollectionResult<()> {
        self.session.begin_transaction().map_err(CollectionError::from)
    }

    /// Commit the active transaction, making all writes durable.
    pub fn commit_transaction(&self) -> CollectionResult<()> {
        self.session.commit_transaction().map_err(CollectionError::from)
    }

    /// Roll back the active transaction, discarding all writes.
    pub fn rollback_transaction(&self) -> CollectionResult<()> {
        self.session.rollback_transaction().map_err(CollectionError::from)
    }

    /// Execute `f` inside a transaction. Commits on `Ok`, rolls back on `Err`.
    pub fn with_transaction<F, R>(&self, f: F) -> CollectionResult<R>
    where
        F: FnOnce() -> CollectionResult<R>,
    {
        self.begin_transaction()?;
        match f() {
            Ok(result) => {
                self.commit_transaction()?;
                Ok(result)
            }
            Err(e) => {
                let _ = self.rollback_transaction();
                Err(e)
            }
        }
    }

    // ============================================================
    // TTL REAPER
    // ============================================================

    /// Delete documents that have expired according to TTL indexes.
    ///
    /// Scans each TTL index for entries whose date field is older than
    /// `now - expire_after_seconds`, and removes the corresponding documents.
    /// Returns the total number of deleted documents.
    ///
    /// This is a synchronous, caller-driven operation (no background thread).
    pub fn reap_expired(&self) -> CollectionResult<u64> {
        let indexes = self.list_indexes()?;
        let mut total_deleted = 0u64;

        for index_spec in &indexes {
            let expire_secs = match index_spec.options.expire_after_seconds {
                Some(s) => s,
                None => continue,
            };
            if index_spec.keys.len() != 1 {
                continue;
            }

            let now_millis = now_epoch_millis();
            let cutoff_millis = now_millis - (expire_secs as i64 * 1000);

            let index_table = format!("{}.idx_{}", self.collection_name, index_spec.name);
            let mut index_cursor = self.session.open_cursor(&index_table)?;

            let mut expired_ids = Vec::new();
            while index_cursor.next().is_ok() {
                let key_raw = index_cursor.get_key_raw()?;
                if key_raw.len() >= 8 {
                    let date_bytes: [u8; 8] = key_raw[..8].try_into().unwrap_or_default();
                    let date_millis = i64::from_be_bytes(date_bytes);
                    if date_millis > cutoff_millis {
                        break;
                    }
                    expired_ids.push(index_cursor.get_value_str()?);
                }
            }
            drop(index_cursor);

            for id_str in expired_ids {
                let mut data_cursor = self.cursor()?;
                data_cursor.set_key_str(&id_str);
                if data_cursor.search().is_ok() {
                    let doc_bytes = data_cursor.get_value_raw()?;
                    let doc = deserialize_document(&doc_bytes)?;
                    self.remove_from_indexes(&doc)?;
                    // Re-position after index removal and delete
                    data_cursor.set_key_str(&id_str);
                    if data_cursor.search().is_ok() {
                        data_cursor.remove()?;
                        total_deleted += 1;
                    }
                }
            }
        }

        Ok(total_deleted)
    }

    /// Access the underlying storage session (for transaction sharing).
    pub fn session(&self) -> &S {
        &self.session
    }

    /// Get the collection name.
    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }

    /// Get the storage table name.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

// ============================================================
// COLLECTION VIEW (for multi-collection transactions)
// ============================================================

/// A collection handle that borrows a shared [`StorageSession`] from a
/// [`TransactionSession`](crate::database::TransactionSession).
///
/// Provides the same core CRUD surface as [`Collection`], but multiple
/// `CollectionView`s can share one session for atomic multi-collection writes.
pub struct CollectionView<'a, S: StorageSession = DefaultSession> {
    session: &'a S,
    table_name: String,
    collection_name: String,
}

impl<'a, S: StorageSession> CollectionView<'a, S> {
    /// Create a view backed by the given session.
    pub fn new(session: &'a S, name: &str, table_uri: &str) -> CollectionResult<Self> {
        session.create_table(table_uri)?;
        Ok(CollectionView {
            session,
            table_name: table_uri.to_string(),
            collection_name: name.to_string(),
        })
    }

    fn cursor(&self) -> StorageResult<S::Cursor> {
        self.session.open_cursor(&self.table_name)
    }

    fn list_indexes(&self) -> CollectionResult<Vec<IndexSpec>> {
        let meta_table = format!("{}.indexes_metadata", self.collection_name);
        let mut meta_cursor = match self.session.open_cursor(&meta_table) {
            Ok(c) => c,
            Err(_) => return Ok(vec![]),
        };
        let mut indexes = Vec::new();
        while meta_cursor.next().is_ok() {
            let bytes = meta_cursor.get_value_raw()?;
            let spec: IndexSpec = bson::from_slice(&bytes)
                .map_err(|e| CollectionError::Other(format!("index spec: {}", e)))?;
            indexes.push(spec);
        }
        Ok(indexes)
    }

    fn insert_into_indexes(&self, doc: &Document) -> CollectionResult<()> {
        use crate::index::{resolve_index_type, IndexType};
        let indexes = self.list_indexes()?;
        let id_str = extract_id_string(doc)?;
        for spec in indexes {
            match resolve_index_type(&spec.keys, &spec.options) {
                IndexType::BTree => {
                    let idx_table = format!("{}.idx_{}", self.collection_name, spec.name);
                    let mut idx_cursor = self.session.open_cursor(&idx_table)?;
                    let key_bytes = extract_index_key(doc, &spec.keys);
                    if spec.options.unique
                        && Collection::<S>::index_has_prefix(&mut idx_cursor, &key_bytes)?
                    {
                        let fields: Vec<&str> = spec.keys.keys().map(|s| s.as_str()).collect();
                        return Err(CollectionError::UniqueConstraintViolation(format!(
                            "Duplicate key for index '{}' on fields: {}",
                            spec.name,
                            fields.join(", ")
                        )));
                    }
                    let mut combined = key_bytes;
                    combined.extend_from_slice(id_str.as_bytes());
                    idx_cursor.set_key_raw(&combined);
                    idx_cursor.set_value_str(&id_str);
                    idx_cursor.insert()?;
                }
                // CollectionView only handles BTree; advanced index types
                // are maintained through the main Collection path.
                _ => {}
            }
        }
        Ok(())
    }

    fn remove_from_indexes(&self, doc: &Document) -> CollectionResult<()> {
        use crate::index::{resolve_index_type, IndexType};
        let indexes = self.list_indexes()?;
        let id_str = extract_id_string(doc)?;
        for spec in indexes {
            match resolve_index_type(&spec.keys, &spec.options) {
                IndexType::BTree => {
                    let idx_table = format!("{}.idx_{}", self.collection_name, spec.name);
                    let mut idx_cursor = self.session.open_cursor(&idx_table)?;
                    let key_bytes = extract_index_key(doc, &spec.keys);
                    let mut combined = key_bytes;
                    combined.extend_from_slice(id_str.as_bytes());
                    idx_cursor.set_key_raw(&combined);
                    if idx_cursor.search().is_ok() {
                        idx_cursor.remove()?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn insert_one(&self, mut document: Document) -> CollectionResult<InsertOneResult> {
        let inserted_id = ensure_id(&mut document);
        self.insert_into_indexes(&document)?;
        let doc_bytes = serialize_document(&document)?;
        let key_str = extract_id_string(&document)?;
        let mut cursor = self.cursor()?;
        cursor.set_key_str(&key_str);
        cursor.set_value_raw(&doc_bytes);
        cursor.insert()?;
        Ok(InsertOneResult { inserted_id })
    }

    pub fn find_one(&self, filter: Document) -> CollectionResult<Option<Document>> {
        let mut cursor = self.cursor()?;
        while cursor.next().is_ok() {
            let doc_bytes = cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;
            if eval_query(&doc, &filter).map_err(CollectionError::QueryError)? {
                return Ok(Some(doc));
            }
        }
        Ok(None)
    }

    pub fn find(&self, filter: Document) -> CollectionResult<Vec<Document>> {
        let mut results = Vec::new();
        let mut cursor = self.cursor()?;
        while cursor.next().is_ok() {
            let doc_bytes = cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;
            if eval_query(&doc, &filter).map_err(CollectionError::QueryError)? {
                results.push(doc);
            }
        }
        Ok(results)
    }

    pub fn update_one(
        &self,
        filter: Document,
        update: Document,
    ) -> CollectionResult<UpdateResult> {
        let mut cursor = self.cursor()?;
        if cursor.next().is_ok() {
            loop {
                let doc_bytes = cursor.get_value_raw()?;
                let mut doc = deserialize_document(&doc_bytes)?;
                if eval_query(&doc, &filter).map_err(CollectionError::QueryError)? {
                    let original = doc.clone();
                    apply_update(&mut doc, &update).map_err(CollectionError::UpdateError)?;
                    if doc != original {
                        self.remove_from_indexes(&original)?;
                        self.insert_into_indexes(&doc)?;
                        let updated_bytes = serialize_document(&doc)?;
                        cursor.set_value_raw(&updated_bytes);
                        cursor.update()?;
                        return Ok(UpdateResult {
                            matched_count: 1,
                            modified_count: 1,
                            upserted_id: None,
                        });
                    }
                    return Ok(UpdateResult {
                        matched_count: 1,
                        modified_count: 0,
                        upserted_id: None,
                    });
                }
                if cursor.next().is_err() {
                    break;
                }
            }
        }
        Ok(UpdateResult {
            matched_count: 0,
            modified_count: 0,
            upserted_id: None,
        })
    }

    pub fn update_many(
        &self,
        filter: Document,
        update: Document,
    ) -> CollectionResult<UpdateResult> {
        let mut cursor = self.cursor()?;
        let mut matched = 0u64;
        let mut modified = 0u64;
        if cursor.next().is_ok() {
            loop {
                let doc_bytes = cursor.get_value_raw()?;
                let mut doc = deserialize_document(&doc_bytes)?;
                if eval_query(&doc, &filter).map_err(CollectionError::QueryError)? {
                    matched += 1;
                    let original = doc.clone();
                    apply_update(&mut doc, &update).map_err(CollectionError::UpdateError)?;
                    if doc != original {
                        modified += 1;
                        self.remove_from_indexes(&original)?;
                        self.insert_into_indexes(&doc)?;
                        let updated_bytes = serialize_document(&doc)?;
                        cursor.set_value_raw(&updated_bytes);
                        cursor.update()?;
                    }
                }
                if cursor.next().is_err() {
                    break;
                }
            }
        }
        Ok(UpdateResult {
            matched_count: matched,
            modified_count: modified,
            upserted_id: None,
        })
    }

    pub fn delete_one(&self, filter: Document) -> CollectionResult<DeleteResult> {
        let mut cursor = self.cursor()?;
        while cursor.next().is_ok() {
            let doc_bytes = cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;
            if eval_query(&doc, &filter).map_err(CollectionError::QueryError)? {
                self.remove_from_indexes(&doc)?;
                cursor.remove()?;
                return Ok(DeleteResult { deleted_count: 1 });
            }
        }
        Ok(DeleteResult { deleted_count: 0 })
    }

    pub fn delete_many(&self, filter: Document) -> CollectionResult<DeleteResult> {
        let mut cursor = self.cursor()?;
        let mut deleted = 0u64;
        while cursor.next().is_ok() {
            let doc_bytes = cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;
            if eval_query(&doc, &filter).map_err(CollectionError::QueryError)? {
                self.remove_from_indexes(&doc)?;
                cursor.remove()?;
                deleted += 1;
            }
        }
        Ok(DeleteResult { deleted_count: deleted })
    }

    pub fn count_documents(&self, filter: Option<Document>) -> CollectionResult<u64> {
        let filter = filter.unwrap_or_default();
        let mut count = 0;
        let mut cursor = self.cursor()?;
        while cursor.next().is_ok() {
            let doc_bytes = cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;
            if eval_query(&doc, &filter).map_err(CollectionError::QueryError)? {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Run an aggregation pipeline over the view's snapshot.
    ///
    /// Loads all documents from the underlying table, then pipes them through
    /// the engine's in-memory aggregation framework.
    pub fn aggregate(&self, pipeline: Vec<Document>) -> CollectionResult<Vec<Document>> {
        let mut docs = Vec::new();
        let mut cursor = self.cursor()?;
        while cursor.next().is_ok() {
            let doc_bytes = cursor.get_value_raw()?;
            let doc = deserialize_document(&doc_bytes)?;
            docs.push(doc);
        }
        crate::aggregation::aggregate(docs, &pipeline)
            .map_err(|e| CollectionError::Other(e.to_string()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bson::doc;
    use crate::database::Database;
    use tempfile::TempDir;

    fn setup_collection() -> (TempDir, Collection) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("testdb")).unwrap();
        let collection = db.collection("test").unwrap();
        (temp_dir, collection)
    }

    #[test]
    fn test_insert_one_generates_id() {
        let (_temp_dir, collection) = setup_collection();
        let doc = doc! { "name": "Alice", "age": 30 };
        let result = collection.insert_one(doc).unwrap();
        assert!(matches!(result.inserted_id, Bson::ObjectId(_)));
    }

    #[test]
    fn test_insert_one_preserves_id() {
        let (_temp_dir, collection) = setup_collection();
        let id = ObjectId::new();
        let doc = doc! { "_id": id, "name": "Bob" };
        let result = collection.insert_one(doc).unwrap();
        assert_eq!(result.inserted_id, Bson::ObjectId(id));
    }

    #[test]
    fn test_insert_many() {
        let (_temp_dir, collection) = setup_collection();
        let docs = vec![
            doc! { "name": "Alice" },
            doc! { "name": "Bob" },
            doc! { "name": "Charlie" },
        ];
        let result = collection.insert_many(docs).unwrap();
        assert_eq!(result.inserted_ids.len(), 3);
    }

    #[test]
    fn test_find_one() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "name": "Alice", "age": 30 })
            .unwrap();
        collection
            .insert_one(doc! { "name": "Bob", "age": 25 })
            .unwrap();

        let result = collection.find_one(doc! { "name": "Alice" }).unwrap();
        assert!(result.is_some());
        let doc = result.unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn test_find_one_not_found() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "name": "Alice" })
            .unwrap();

        let result = collection.find_one(doc! { "name": "Bob" }).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_find_all() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "name": "Alice", "age": 30 })
            .unwrap();
        collection
            .insert_one(doc! { "name": "Bob", "age": 25 })
            .unwrap();
        collection
            .insert_one(doc! { "name": "Charlie", "age": 35 })
            .unwrap();

        let results = collection.find(doc! {}).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_find_with_filter() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "name": "Alice", "age": 30 })
            .unwrap();
        collection
            .insert_one(doc! { "name": "Bob", "age": 25 })
            .unwrap();
        collection
            .insert_one(doc! { "name": "Charlie", "age": 35 })
            .unwrap();

        let results = collection.find(doc! { "age": { "$gte": 30 } }).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_update_one() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "name": "Alice", "age": 30 })
            .unwrap();
        collection
            .insert_one(doc! { "name": "Bob", "age": 25 })
            .unwrap();

        let result = collection
            .update_one(doc! { "name": "Alice" }, doc! { "$set": { "age": 31 } })
            .unwrap();
        assert_eq!(result.matched_count, 1);
        assert_eq!(result.modified_count, 1);

        let doc = collection.find_one(doc! { "name": "Alice" }).unwrap().unwrap();
        assert_eq!(doc.get_i32("age").unwrap(), 31);
    }

    #[test]
    fn test_update_one_no_match() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "name": "Alice" })
            .unwrap();

        let result = collection
            .update_one(doc! { "name": "Bob" }, doc! { "$set": { "age": 30 } })
            .unwrap();
        assert_eq!(result.matched_count, 0);
        assert_eq!(result.modified_count, 0);
    }

    #[test]
    fn test_update_many() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "status": "pending", "value": 10 })
            .unwrap();
        collection
            .insert_one(doc! { "status": "pending", "value": 20 })
            .unwrap();
        collection
            .insert_one(doc! { "status": "active", "value": 30 })
            .unwrap();

        let result = collection
            .update_many(
                doc! { "status": "pending" },
                doc! { "$set": { "status": "active" } },
            )
            .unwrap();
        assert_eq!(result.matched_count, 2);
        assert_eq!(result.modified_count, 2);

        let active = collection.find(doc! { "status": "active" }).unwrap();
        assert_eq!(active.len(), 3);
    }

    #[test]
    fn test_delete_one() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "name": "Alice" })
            .unwrap();
        collection
            .insert_one(doc! { "name": "Bob" })
            .unwrap();

        let result = collection.delete_one(doc! { "name": "Alice" }).unwrap();
        assert_eq!(result.deleted_count, 1);

        let remaining = collection.find(doc! {}).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].get_str("name").unwrap(), "Bob");
    }

    #[test]
    fn test_delete_one_no_match() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "name": "Alice" })
            .unwrap();

        let result = collection.delete_one(doc! { "name": "Bob" }).unwrap();
        assert_eq!(result.deleted_count, 0);
    }

    #[test]
    fn test_delete_many() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "status": "old", "value": 10 })
            .unwrap();
        collection
            .insert_one(doc! { "status": "old", "value": 20 })
            .unwrap();
        collection
            .insert_one(doc! { "status": "new", "value": 30 })
            .unwrap();

        let result = collection.delete_many(doc! { "status": "old" }).unwrap();
        assert_eq!(result.deleted_count, 2);

        let remaining = collection.find(doc! {}).unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_count_documents_all() {
        let (_temp_dir, collection) = setup_collection();
        collection.insert_one(doc! { "a": 1 }).unwrap();
        collection.insert_one(doc! { "a": 2 }).unwrap();
        collection.insert_one(doc! { "a": 3 }).unwrap();

        let count = collection.count_documents(None).unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_count_documents_with_filter() {
        let (_temp_dir, collection) = setup_collection();
        collection.insert_one(doc! { "age": 25 }).unwrap();
        collection.insert_one(doc! { "age": 30 }).unwrap();
        collection.insert_one(doc! { "age": 35 }).unwrap();

        let count = collection
            .count_documents(Some(doc! { "age": { "$gte": 30 } }))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_count_empty_collection() {
        let (_temp_dir, collection) = setup_collection();
        let count = collection.count_documents(None).unwrap();
        assert_eq!(count, 0);
    }

    // ============================================================
    // INDEX TESTS (Phase 7)
    // ============================================================

    #[test]
    fn test_create_index_single_field() {
        let (_temp_dir, collection) = setup_collection();

        // Create index
        let index_name = collection.create_index(doc! { "email": 1 }, None).unwrap();
        assert_eq!(index_name, "email_1");

        // Verify index exists
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "email_1");
    }

    #[test]
    fn test_create_index_compound() {
        let (_temp_dir, collection) = setup_collection();

        // Create compound index
        let index_name = collection.create_index(doc! { "age": 1, "name": -1 }, None).unwrap();
        assert!(index_name.contains("age_1"));
        assert!(index_name.contains("name_-1"));

        // Verify index exists
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 1);
    }

    #[test]
    fn test_create_index_custom_name() {
        let (_temp_dir, collection) = setup_collection();
        collection
            .insert_one(doc! { "email": "a@example.com" })
            .unwrap();
        let nm = collection
            .create_index(
                doc! { "email": 1 },
                Some(crate::index::IndexOptions {
                    name: Some("atlas_email_idx".to_string()),
                    ..Default::default()
                }),
            )
            .unwrap();
        assert_eq!(nm, "atlas_email_idx");
        let indexes = collection.list_indexes().unwrap();
        assert!(indexes.iter().any(|s| s.name == "atlas_email_idx"));
    }

    #[test]
    fn test_create_index_rejects_bad_custom_name() {
        let (_temp_dir, collection) = setup_collection();
        let result = collection.create_index(
            doc! { "email": 1 },
            Some(crate::index::IndexOptions {
                name: Some("bad.name".to_string()),
                ..Default::default()
            }),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_create_index_unique() {
        let (_temp_dir, collection) = setup_collection();

        // Insert documents
        collection.insert_one(doc! { "email": "alice@example.com", "name": "Alice" }).unwrap();

        // Create unique index
        let index_name = collection.create_index(
            doc! { "email": 1 },
            Some(crate::index::IndexOptions {
                unique: true,
                ..Default::default()
            })
        ).unwrap();
        assert_eq!(index_name, "email_1");

        // Try to insert duplicate - should fail
        let result = collection.insert_one(doc! { "email": "alice@example.com", "name": "Alice2" });
        assert!(result.is_err());
        assert!(matches!(result, Err(CollectionError::UniqueConstraintViolation(_))));
    }

    #[test]
    fn test_create_index_on_existing_data() {
        let (_temp_dir, collection) = setup_collection();

        // Insert documents first
        collection.insert_one(doc! { "email": "alice@example.com", "age": 30 }).unwrap();
        collection.insert_one(doc! { "email": "bob@example.com", "age": 25 }).unwrap();
        collection.insert_one(doc! { "email": "charlie@example.com", "age": 35 }).unwrap();

        // Create index on existing data
        let index_name = collection.create_index(doc! { "email": 1 }, None).unwrap();
        assert_eq!(index_name, "email_1");

        // Verify index exists and was built
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 1);
    }

    #[test]
    fn test_create_index_duplicate_fails() {
        let (_temp_dir, collection) = setup_collection();

        // Create index
        collection.create_index(doc! { "email": 1 }, None).unwrap();

        // Try to create same index again - should fail
        let result = collection.create_index(doc! { "email": 1 }, None);
        assert!(result.is_err());
        assert!(matches!(result, Err(CollectionError::IndexAlreadyExists(_))));
    }

    #[test]
    fn test_list_indexes() {
        let (_temp_dir, collection) = setup_collection();

        // Initially no indexes
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 0);

        // Create multiple indexes
        collection.create_index(doc! { "email": 1 }, None).unwrap();
        collection.create_index(doc! { "age": 1 }, None).unwrap();
        collection.create_index(doc! { "name": 1, "age": -1 }, None).unwrap();

        // List indexes
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 3);
    }

    #[test]
    fn test_drop_index() {
        let (_temp_dir, collection) = setup_collection();

        // Create index
        let index_name = collection.create_index(doc! { "email": 1 }, None).unwrap();

        // Verify index exists
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 1);

        // Drop index
        collection.drop_index(&index_name).unwrap();

        // Verify index is gone
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 0);
    }

    #[test]
    fn test_drop_all_indexes() {
        let (_temp_dir, collection) = setup_collection();

        // Create multiple indexes
        collection.create_index(doc! { "email": 1 }, None).unwrap();
        collection.create_index(doc! { "age": 1 }, None).unwrap();
        collection.create_index(doc! { "name": 1 }, None).unwrap();

        // Verify indexes exist
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 3);

        // Drop all indexes
        collection.drop_index("*").unwrap();

        // Verify all indexes are gone
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 0);
    }

    #[test]
    fn test_index_maintained_on_insert() {
        let (_temp_dir, collection) = setup_collection();

        // Create index first
        collection.create_index(doc! { "email": 1 }, None).unwrap();

        // Insert document
        collection.insert_one(doc! { "email": "alice@example.com", "name": "Alice" }).unwrap();

        // Index should be maintained (verified implicitly by successful insert)
        let count = collection.count_documents(None).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_index_maintained_on_update() {
        let (_temp_dir, collection) = setup_collection();

        // Insert document
        collection.insert_one(doc! { "email": "alice@example.com", "age": 30 }).unwrap();

        // Create index
        collection.create_index(doc! { "email": 1 }, None).unwrap();

        // Update document
        collection.update_one(
            doc! { "email": "alice@example.com" },
            doc! { "$set": { "age": 31 } }
        ).unwrap();

        // Verify update worked
        let doc = collection.find_one(doc! { "email": "alice@example.com" }).unwrap().unwrap();
        assert_eq!(doc.get_i32("age").unwrap(), 31);
    }

    #[test]
    fn test_index_maintained_on_delete() {
        let (_temp_dir, collection) = setup_collection();

        // Insert documents
        collection.insert_one(doc! { "email": "alice@example.com", "name": "Alice" }).unwrap();
        collection.insert_one(doc! { "email": "bob@example.com", "name": "Bob" }).unwrap();

        // Create index
        collection.create_index(doc! { "email": 1 }, None).unwrap();

        // Delete document
        let result = collection.delete_one(doc! { "email": "alice@example.com" }).unwrap();
        assert_eq!(result.deleted_count, 1);

        // Verify document is gone
        let count = collection.count_documents(None).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_unique_constraint_on_existing_duplicates() {
        let (_temp_dir, collection) = setup_collection();

        // Insert documents with duplicate values
        collection.insert_one(doc! { "email": "alice@example.com", "name": "Alice1" }).unwrap();
        collection.insert_one(doc! { "email": "alice@example.com", "name": "Alice2" }).unwrap();

        // Try to create unique index - should fail
        let result = collection.create_index(
            doc! { "email": 1 },
            Some(crate::index::IndexOptions {
                unique: true,
                ..Default::default()
            })
        );
        assert!(result.is_err());
        assert!(matches!(result, Err(CollectionError::UniqueConstraintViolation(_))));
    }

    #[test]
    fn test_unique_constraint_on_update() {
        let (_temp_dir, collection) = setup_collection();

        // Insert documents
        collection.insert_one(doc! { "email": "alice@example.com", "name": "Alice" }).unwrap();
        collection.insert_one(doc! { "email": "bob@example.com", "name": "Bob" }).unwrap();

        // Create unique index
        collection.create_index(
            doc! { "email": 1 },
            Some(crate::index::IndexOptions {
                unique: true,
                ..Default::default()
            })
        ).unwrap();

        // Try to update to create duplicate - should fail
        let result = collection.update_one(
            doc! { "email": "bob@example.com" },
            doc! { "$set": { "email": "alice@example.com" } }
        );
        assert!(result.is_err());
        assert!(matches!(result, Err(CollectionError::UniqueConstraintViolation(_))));
    }

    #[test]
    fn test_create_index_empty_keys_fails() {
        let (_temp_dir, collection) = setup_collection();

        // Try to create index with empty keys - should fail
        let result = collection.create_index(doc! {}, None);
        assert!(result.is_err());
        assert!(matches!(result, Err(CollectionError::InvalidIndexSpec(_))));
    }

    #[test]
    fn test_multiple_indexes_on_same_collection() {
        let (_temp_dir, collection) = setup_collection();

        // Insert documents
        collection.insert_one(doc! { "email": "alice@example.com", "age": 30, "status": "active" }).unwrap();
        collection.insert_one(doc! { "email": "bob@example.com", "age": 25, "status": "inactive" }).unwrap();

        // Create multiple indexes
        collection.create_index(doc! { "email": 1 }, None).unwrap();
        collection.create_index(doc! { "age": 1 }, None).unwrap();
        collection.create_index(doc! { "status": 1 }, None).unwrap();

        // Verify all indexes exist
        let indexes = collection.list_indexes().unwrap();
        assert_eq!(indexes.len(), 3);

        // All CRUD operations should still work
        collection.insert_one(doc! { "email": "charlie@example.com", "age": 35, "status": "active" }).unwrap();
        let count = collection.count_documents(None).unwrap();
        assert_eq!(count, 3);
    }

    // ============================================================
    // QUERY OPTIMIZATION TESTS (Phase 8)
    // ============================================================

    #[test]
    fn test_query_uses_index_for_equality() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data
        collection.insert_one(doc! { "email": "alice@example.com", "name": "Alice" }).unwrap();
        collection.insert_one(doc! { "email": "bob@example.com", "name": "Bob" }).unwrap();
        collection.insert_one(doc! { "email": "charlie@example.com", "name": "Charlie" }).unwrap();

        // Create index
        collection.create_index(doc! { "email": 1 }, None).unwrap();

        // Query should use index (equality query)
        let result = collection.find_one(doc! { "email": "bob@example.com" }).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().get_str("name").unwrap(), "Bob");
    }

    #[test]
    fn test_query_uses_index_for_range() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data
        for i in 1..=10 {
            collection.insert_one(doc! { "age": i * 10, "name": format!("Person{}", i) }).unwrap();
        }

        // Create index on age
        collection.create_index(doc! { "age": 1 }, None).unwrap();

        // Range query should use index
        let results = collection.find(doc! { "age": { "$gte": 50 } }).unwrap();
        assert_eq!(results.len(), 6); // 50, 60, 70, 80, 90, 100
    }

    #[test]
    fn test_query_without_index_still_works() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data (no index)
        collection.insert_one(doc! { "name": "Alice", "city": "NYC" }).unwrap();
        collection.insert_one(doc! { "name": "Bob", "city": "SF" }).unwrap();

        // Query without index should still work (collection scan)
        let results = collection.find(doc! { "city": "NYC" }).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn test_query_selects_best_index() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data
        collection.insert_one(doc! { "email": "alice@example.com", "age": 30 }).unwrap();
        collection.insert_one(doc! { "email": "bob@example.com", "age": 25 }).unwrap();

        // Create multiple indexes
        collection.create_index(doc! { "email": 1 }, None).unwrap();
        collection.create_index(doc! { "age": 1 }, None).unwrap();

        // Query on email should use email index
        let result = collection.find_one(doc! { "email": "alice@example.com" }).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().get_i32("age").unwrap(), 30);
    }

    #[test]
    fn test_find_with_index_returns_correct_results() {
        let (_temp_dir, collection) = setup_collection();

        // Insert many documents
        for i in 1..=100 {
            collection.insert_one(doc! {
                "user_id": i,
                "email": format!("user{}@example.com", i),
                "score": i * 10
            }).unwrap();
        }

        // Create indexes
        collection.create_index(doc! { "email": 1 }, None).unwrap();
        collection.create_index(doc! { "score": 1 }, None).unwrap();

        // Test equality query with index
        let result = collection.find_one(doc! { "email": "user50@example.com" }).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().get_i32("user_id").unwrap(), 50);

        // Test range query with index
        let results = collection.find(doc! { "score": { "$gte": 900 } }).unwrap();
        assert_eq!(results.len(), 11); // 900, 910, ..., 1000
    }

    #[test]
    fn test_compound_index_query_optimization() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data
        collection.insert_one(doc! { "category": "books", "price": 10 }).unwrap();
        collection.insert_one(doc! { "category": "books", "price": 20 }).unwrap();
        collection.insert_one(doc! { "category": "electronics", "price": 100 }).unwrap();

        // Create compound index
        collection.create_index(doc! { "category": 1, "price": 1 }, None).unwrap();

        // Query on first field should use index
        let results = collection.find(doc! { "category": "books" }).unwrap();
        assert_eq!(results.len(), 2);
    }

    // ============================================================
    // EXPLAIN TESTS (Phase 9)
    // ============================================================

    #[test]
    fn test_explain_find_one_collection_scan() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data without index
        collection.insert_one(doc! { "name": "Alice", "age": 30 }).unwrap();
        collection.insert_one(doc! { "name": "Bob", "age": 25 }).unwrap();

        // Explain query without index
        let explain = collection.explain_find_one(doc! { "name": "Alice" }).unwrap();

        // Should use collection scan
        assert!(matches!(
            explain.execution_plan,
            crate::explain::ExecutionPlanExplain::CollectionScan
        ));
        assert_eq!(explain.index_used, None);
        assert_eq!(explain.execution_stats.documents_examined, 2);
        assert_eq!(explain.execution_stats.documents_returned, 1);
    }

    #[test]
    fn test_explain_find_one_with_index() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data
        collection.insert_one(doc! { "email": "alice@example.com", "age": 30 }).unwrap();
        collection.insert_one(doc! { "email": "bob@example.com", "age": 25 }).unwrap();

        // Create index
        collection.create_index(doc! { "email": 1 }, None).unwrap();

        // Explain equality query with index
        let explain = collection.explain_find_one(doc! { "email": "alice@example.com" }).unwrap();

        // Should use index seek
        assert!(matches!(
            explain.execution_plan,
            crate::explain::ExecutionPlanExplain::IndexSeek { .. }
        ));
        assert_eq!(explain.index_used, Some("email_1".to_string()));
        assert!(explain.execution_stats.index_entries_examined > 0);
    }

    #[test]
    fn test_explain_find_with_range_query() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data
        for i in 1..=10 {
            collection.insert_one(doc! { "age": i * 10, "name": format!("Person{}", i) }).unwrap();
        }

        // Create index
        collection.create_index(doc! { "age": 1 }, None).unwrap();

        // Explain range query
        let explain = collection.explain_find(doc! { "age": { "$gte": 50 } }).unwrap();

        // Should use index scan
        assert!(matches!(
            explain.execution_plan,
            crate::explain::ExecutionPlanExplain::IndexScan { .. }
        ));
        assert_eq!(explain.index_used, Some("age_1".to_string()));
        assert_eq!(explain.execution_stats.documents_returned, 6); // 50, 60, 70, 80, 90, 100
    }

    #[test]
    fn test_explain_efficiency() {
        let (_temp_dir, collection) = setup_collection();

        // Insert many documents
        for i in 1..=100 {
            collection.insert_one(doc! { "value": i, "category": if i % 10 == 0 { "special" } else { "normal" } }).unwrap();
        }

        // Explain query that matches 10% of documents
        let explain = collection.explain_find(doc! { "category": "special" }).unwrap();

        assert_eq!(explain.execution_stats.documents_examined, 100);
        assert_eq!(explain.execution_stats.documents_returned, 10);
        assert_eq!(explain.efficiency(), 0.1);
        assert!(!explain.is_efficient()); // Exactly 10%, needs > 10% to be efficient
    }

    #[test]
    fn test_explain_summary() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data
        collection.insert_one(doc! { "email": "alice@example.com" }).unwrap();
        collection.create_index(doc! { "email": 1 }, None).unwrap();

        let explain = collection.explain_find_one(doc! { "email": "alice@example.com" }).unwrap();
        let summary = explain.summary();

        // Summary should contain key information
        assert!(summary.contains("IXSEEK"));
        assert!(summary.contains("email_1"));
        assert!(summary.contains("Examined"));
        assert!(summary.contains("Returned"));
        assert!(summary.contains("Efficiency"));
    }

    #[test]
    fn test_explain_empty_collection() {
        let (_temp_dir, collection) = setup_collection();

        // Explain query on empty collection
        let explain = collection.explain_find(doc! { "field": "value" }).unwrap();

        assert_eq!(explain.execution_stats.documents_examined, 0);
        assert_eq!(explain.execution_stats.documents_returned, 0);
        assert_eq!(explain.efficiency(), 1.0); // Avoid division by zero
    }

    #[test]
    fn test_explain_with_multiple_indexes() {
        let (_temp_dir, collection) = setup_collection();

        // Insert test data
        collection.insert_one(doc! { "email": "alice@example.com", "age": 30 }).unwrap();

        // Create multiple indexes
        collection.create_index(doc! { "email": 1 }, None).unwrap();
        collection.create_index(doc! { "age": 1 }, None).unwrap();

        // Explain should select best index
        let explain = collection.explain_find_one(doc! { "email": "alice@example.com" }).unwrap();

        assert_eq!(explain.index_used, Some("email_1".to_string()));
        assert!(matches!(
            explain.execution_plan,
            crate::explain::ExecutionPlanExplain::IndexSeek { .. }
        ));
    }

    // ============================================================
    // INDEX-AWARE AGGREGATION TESTS
    // ============================================================

    #[test]
    fn test_aggregate_uses_index_for_leading_match() {
        let (_temp_dir, collection) = setup_collection();

        for i in 0..20 {
            collection
                .insert_one(doc! { "status": if i % 2 == 0 { "active" } else { "inactive" }, "val": i })
                .unwrap();
        }

        collection.create_index(doc! { "status": 1 }, None).unwrap();

        let explain = collection
            .explain_aggregate(vec![
                doc! { "$match": { "status": "active" } },
                doc! { "$sort": { "val": 1 } },
            ])
            .unwrap();

        assert!(matches!(
            explain.execution_plan,
            crate::explain::ExecutionPlanExplain::IndexSeek { .. }
        ));
        assert_eq!(explain.index_used, Some("status_1".to_string()));
    }

    #[test]
    fn test_aggregate_merges_consecutive_matches() {
        let (_temp_dir, collection) = setup_collection();

        collection.insert_one(doc! { "status": "active", "age": 30 }).unwrap();
        collection.insert_one(doc! { "status": "active", "age": 15 }).unwrap();
        collection.insert_one(doc! { "status": "inactive", "age": 40 }).unwrap();

        collection.create_index(doc! { "status": 1 }, None).unwrap();

        let results = collection
            .aggregate(vec![
                doc! { "$match": { "status": "active" } },
                doc! { "$match": { "age": { "$gte": 18 } } },
                doc! { "$count": "total" },
            ])
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_i32("total").unwrap(), 1);
    }

    #[test]
    fn test_aggregate_no_match_still_works() {
        let (_temp_dir, collection) = setup_collection();

        collection.insert_one(doc! { "x": 1 }).unwrap();
        collection.insert_one(doc! { "x": 2 }).unwrap();
        collection.insert_one(doc! { "x": 3 }).unwrap();

        let results = collection
            .aggregate(vec![
                doc! { "$sort": { "x": -1 } },
                doc! { "$limit": 2 },
            ])
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_i32("x").unwrap(), 3);

        let explain = collection
            .explain_aggregate(vec![doc! { "$sort": { "x": -1 } }])
            .unwrap();
        assert!(matches!(
            explain.execution_plan,
            crate::explain::ExecutionPlanExplain::CollectionScan
        ));
    }

    #[test]
    fn test_aggregate_match_not_first_stage_not_pushed_down() {
        let (_temp_dir, collection) = setup_collection();

        collection.insert_one(doc! { "dept": "eng", "salary": 100 }).unwrap();
        collection.insert_one(doc! { "dept": "eng", "salary": 200 }).unwrap();
        collection.insert_one(doc! { "dept": "hr", "salary": 150 }).unwrap();

        collection.create_index(doc! { "dept": 1 }, None).unwrap();

        let explain = collection
            .explain_aggregate(vec![
                doc! { "$group": { "_id": "$dept", "total": { "$sum": "$salary" } } },
                doc! { "$match": { "total": { "$gte": 200 } } },
            ])
            .unwrap();

        assert!(matches!(
            explain.execution_plan,
            crate::explain::ExecutionPlanExplain::CollectionScan
        ));
        assert_eq!(explain.index_used, None);
    }

    #[test]
    fn test_aggregate_with_index_produces_correct_results() {
        let (_temp_dir, collection) = setup_collection();

        for i in 1..=50 {
            collection
                .insert_one(doc! {
                    "category": if i % 3 == 0 { "a" } else if i % 3 == 1 { "b" } else { "c" },
                    "value": i
                })
                .unwrap();
        }

        collection.create_index(doc! { "category": 1 }, None).unwrap();

        let results = collection
            .aggregate(vec![
                doc! { "$match": { "category": "a" } },
                doc! { "$group": { "_id": bson::Bson::Null, "total": { "$sum": "$value" } } },
            ])
            .unwrap();

        assert_eq!(results.len(), 1);
        let expected_sum: i32 = (1..=50).filter(|i| i % 3 == 0).sum();
        assert_eq!(results[0].get_i64("total").unwrap(), expected_sum as i64);
    }

    #[test]
    fn test_aggregate_range_match_uses_index() {
        let (_temp_dir, collection) = setup_collection();

        for i in 0..30 {
            collection.insert_one(doc! { "score": i * 10 }).unwrap();
        }

        collection.create_index(doc! { "score": 1 }, None).unwrap();

        let explain = collection
            .explain_aggregate(vec![
                doc! { "$match": { "score": { "$gte": 200 } } },
                doc! { "$count": "high_scorers" },
            ])
            .unwrap();

        assert!(matches!(
            explain.execution_plan,
            crate::explain::ExecutionPlanExplain::IndexScan { .. }
        ));
        assert_eq!(explain.index_used, Some("score_1".to_string()));
    }

    #[test]
    fn test_explain_plan_reason() {
        let (_temp_dir, collection) = setup_collection();

        // Without index
        let explain = collection.explain_find(doc! { "field": "value" }).unwrap();
        assert!(explain.plan_reason.contains("No suitable index") || explain.plan_reason.len() > 0);

        // With index
        collection.insert_one(doc! { "email": "test@example.com" }).unwrap();
        collection.create_index(doc! { "email": 1 }, None).unwrap();

        let explain = collection.explain_find_one(doc! { "email": "test@example.com" }).unwrap();
        assert!(explain.plan_reason.contains("Equality") || explain.plan_reason.contains("email"));
    }

    // ============================================================
    // FIND_ITER TESTS
    // ============================================================

    #[test]
    fn test_find_iter_collection_scan() {
        let (_td, col) = setup_collection();
        col.insert_one(doc! { "name": "Alice", "age": 30 }).unwrap();
        col.insert_one(doc! { "name": "Bob", "age": 25 }).unwrap();
        col.insert_one(doc! { "name": "Charlie", "age": 35 }).unwrap();

        let results: Vec<_> = col
            .find_iter(doc! { "age": { "$gte": 30 } })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_find_iter_with_index() {
        let (_td, col) = setup_collection();
        col.insert_one(doc! { "email": "a@b.c", "name": "Alice" }).unwrap();
        col.insert_one(doc! { "email": "b@b.c", "name": "Bob" }).unwrap();
        col.create_index(doc! { "email": 1 }, None).unwrap();

        let results: Vec<_> = col
            .find_iter(doc! { "email": "a@b.c" })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn test_find_iter_empty_collection() {
        let (_td, col) = setup_collection();
        let results: Vec<_> = col
            .find_iter(doc! {})
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_iter_lazy_take() {
        let (_td, col) = setup_collection();
        for i in 0..100 {
            col.insert_one(doc! { "n": i }).unwrap();
        }
        let results: Vec<_> = col
            .find_iter(doc! {})
            .unwrap()
            .take(5)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 5);
    }

    // ============================================================
    // TRANSACTION TESTS
    // ============================================================

    #[test]
    fn test_single_collection_transaction_commit() {
        let (_td, col) = setup_collection();
        col.with_transaction(|| {
            col.insert_one(doc! { "x": 1 })?;
            col.insert_one(doc! { "x": 2 })?;
            Ok(())
        })
        .unwrap();
        assert_eq!(col.count_documents(None).unwrap(), 2);
    }

    #[test]
    fn test_single_collection_transaction_rollback() {
        let (_td, col) = setup_collection();
        col.insert_one(doc! { "x": 0 }).unwrap();
        let result: Result<(), _> = col.with_transaction(|| {
            col.insert_one(doc! { "x": 1 })?;
            Err(CollectionError::Other("deliberate abort".into()))
        });
        assert!(result.is_err());
        assert_eq!(col.count_documents(None).unwrap(), 1);
    }
}
