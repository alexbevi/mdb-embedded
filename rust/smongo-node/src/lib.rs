//! Node.js binding for smongo embedded database engine.
//!
//! Provides a MongoDB-compatible API for Node.js via napi-rs.
//! Documents cross the boundary as plain JS objects, with automatic
//! BSON <-> JSON conversion handled internally.

use std::sync::Arc;

use bson::{Bson, Document};
use napi_derive::napi;

use smongo_engine::collection::{
    Collection as EngineCollection, FindOptions as EngineFindOptions,
    UpdateOptions as EngineUpdateOptions,
};
use smongo_engine::database::Database as EngineDatabase;
use smongo_engine::database::TransactionSession as EngineTransactionSession;
use smongo_engine::index::IndexOptions as EngineIndexOptions;

// ============================================================
// JSON <-> BSON CONVERSION
// ============================================================

fn json_to_bson(value: &serde_json::Value) -> Bson {
    match value {
        serde_json::Value::Object(map) => {
            let doc: Document = map
                .iter()
                .map(|(k, v)| (k.clone(), json_to_bson(v)))
                .collect();
            Bson::Document(doc)
        }
        serde_json::Value::Array(arr) => Bson::Array(arr.iter().map(json_to_bson).collect()),
        serde_json::Value::String(s) => Bson::String(s.clone()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i64::from(i32::MIN) && i <= i64::from(i32::MAX) {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Int64(i)
                }
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                Bson::Null
            }
        }
        serde_json::Value::Bool(b) => Bson::Boolean(*b),
        serde_json::Value::Null => Bson::Null,
    }
}

fn json_to_doc(value: serde_json::Value) -> napi::Result<Document> {
    match json_to_bson(&value) {
        Bson::Document(doc) => Ok(doc),
        _ => Err(napi::Error::from_reason("Expected a JSON object")),
    }
}

fn json_vec_to_docs(value: serde_json::Value) -> napi::Result<Vec<Document>> {
    match value {
        serde_json::Value::Array(arr) => arr.into_iter().map(json_to_doc).collect(),
        _ => Err(napi::Error::from_reason("Expected a JSON array of objects")),
    }
}

fn bson_to_json(bson: &Bson) -> serde_json::Value {
    match bson {
        Bson::Document(doc) => doc_to_json(doc),
        Bson::Array(arr) => serde_json::Value::Array(arr.iter().map(bson_to_json).collect()),
        Bson::ObjectId(oid) => serde_json::Value::String(oid.to_hex()),
        Bson::String(s) => serde_json::Value::String(s.clone()),
        Bson::Int32(i) => serde_json::Value::Number((*i).into()),
        Bson::Int64(i) => serde_json::Value::Number((*i).into()),
        Bson::Double(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Bson::Boolean(b) => serde_json::Value::Bool(*b),
        Bson::Null => serde_json::Value::Null,
        other => other.clone().into_relaxed_extjson(),
    }
}

fn doc_to_json(doc: &Document) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = doc
        .iter()
        .map(|(k, v)| (k.clone(), bson_to_json(v)))
        .collect();
    serde_json::Value::Object(map)
}

fn docs_to_json_array(docs: &[Document]) -> serde_json::Value {
    serde_json::Value::Array(docs.iter().map(doc_to_json).collect())
}

fn parse_find_options(options: &Option<serde_json::Value>) -> EngineFindOptions {
    match options {
        Some(o) => {
            let sort = o.get("sort").and_then(|v| {
                if let Bson::Document(d) = json_to_bson(v) { Some(d) } else { None }
            });
            let limit = o.get("limit").and_then(|v| v.as_i64());
            let skip = o.get("skip").and_then(|v| v.as_i64());
            let projection = o.get("projection").and_then(|v| {
                if let Bson::Document(d) = json_to_bson(v) { Some(d) } else { None }
            });
            EngineFindOptions { sort, limit, skip, projection }
        }
        None => EngineFindOptions::default(),
    }
}

fn parse_update_options(options: &Option<serde_json::Value>) -> EngineUpdateOptions {
    match options {
        Some(o) => EngineUpdateOptions {
            upsert: o.get("upsert").and_then(|v| v.as_bool()).unwrap_or(false),
            ..Default::default()
        },
        None => EngineUpdateOptions::default(),
    }
}

// ============================================================
// MongoClient — top-level entry point (MongoDB driver-like API)
// ============================================================

#[napi]
pub struct MongoClient {
    base_path: String,
}

#[napi]
impl MongoClient {
    /// Create a new MongoClient.
    ///
    /// Accepts a URI-like path: `local://./my_data` or just a filesystem path.
    #[napi(constructor)]
    pub fn new(uri: String) -> Self {
        let base_path = uri
            .strip_prefix("local://")
            .unwrap_or(&uri)
            .to_string();
        MongoClient { base_path }
    }

    /// Open a named database under the client's base path.
    #[napi]
    pub fn db(&self, name: String) -> napi::Result<Database> {
        let path = format!("{}/{}", self.base_path, name);
        Database::open(path)
    }
}

// ============================================================
// Database — wraps smongo_engine::database::Database
// ============================================================

#[napi]
pub struct Database {
    inner: Arc<EngineDatabase>,
}

#[napi]
impl Database {
    /// Open or create a database at the given path.
    #[napi(factory)]
    pub fn open(path: String) -> napi::Result<Self> {
        let db = EngineDatabase::open(&path)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(Database {
            inner: Arc::new(db),
        })
    }

    /// Get the database name (derived from the path).
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    /// Get the database path.
    #[napi(getter)]
    pub fn path(&self) -> String {
        self.inner.path().to_string()
    }

    /// Get or create a collection.
    #[napi]
    pub fn collection(&self, name: String) -> napi::Result<Collection> {
        let col = self.inner.collection(&name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(Collection {
            inner: Some(col),
            _db_ref: Arc::clone(&self.inner),
        })
    }

    /// List all collection names in the database.
    #[napi(js_name = "listCollectionNames")]
    pub fn list_collection_names(&self) -> napi::Result<Vec<String>> {
        self.inner
            .list_collection_names()
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Drop a collection by name.
    #[napi(js_name = "dropCollection")]
    pub fn drop_collection(&self, name: String) -> napi::Result<()> {
        self.inner
            .drop_collection(&name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Get database statistics.
    #[napi]
    pub fn stats(&self) -> napi::Result<serde_json::Value> {
        let s = self.inner.stats()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({
            "collectionCount": s.collection_count,
            "sizeBytes": s.size_bytes,
        }))
    }

    /// Start a new client session for multi-collection transactions.
    ///
    /// Returns a `ClientSession` that supports `startTransaction()`,
    /// `commitTransaction()`, and `abortTransaction()`, with collection
    /// operations scoped to the transaction.
    #[napi(js_name = "startSession")]
    pub fn start_session(&self) -> napi::Result<ClientSession> {
        let session = self.inner.start_session()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(ClientSession { inner: session })
    }

    /// Reap expired documents from all TTL-indexed collections.
    ///
    /// Returns the total number of documents removed.
    #[napi(js_name = "reapTtl")]
    pub fn reap_ttl(&self) -> napi::Result<i64> {
        let count = self.inner.reap_ttl()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(count as i64)
    }
}

// ============================================================
// Collection — wraps smongo_engine::collection::Collection
// ============================================================

#[napi]
pub struct Collection {
    inner: Option<EngineCollection>,
    _db_ref: Arc<EngineDatabase>,
}

impl Collection {
    fn engine(&self) -> napi::Result<&EngineCollection> {
        self.inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("Collection has been closed"))
    }
}

#[napi]
impl Collection {
    /// Release the underlying WiredTiger session and collection handle.
    ///
    /// Must be called before `db.dropCollection()` when a JS-side handle
    /// was previously obtained for the same collection name, because
    /// WiredTiger refuses to drop tables while any session holds cached
    /// cursors on them.
    #[napi]
    pub fn close(&mut self) {
        self.inner.take();
    }

    // ---- INSERT ----

    #[napi(js_name = "insertOne")]
    pub fn insert_one(&self, document: serde_json::Value) -> napi::Result<serde_json::Value> {
        let doc = json_to_doc(document)?;
        let result = self.engine()?.insert_one(doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({
            "insertedId": bson_to_json(&result.inserted_id),
        }))
    }

    #[napi(js_name = "insertMany")]
    pub fn insert_many(&self, documents: serde_json::Value) -> napi::Result<serde_json::Value> {
        let docs = json_vec_to_docs(documents)?;
        let result = self.engine()?.insert_many(docs)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let ids: Vec<serde_json::Value> = result
            .inserted_ids
            .iter()
            .map(bson_to_json)
            .collect();
        Ok(serde_json::json!({ "insertedIds": ids }))
    }

    // ---- FIND ----

    #[napi(js_name = "findOne")]
    pub fn find_one(
        &self,
        filter: serde_json::Value,
        options: Option<serde_json::Value>,
    ) -> napi::Result<Option<serde_json::Value>> {
        let engine = self.engine()?;
        let filter_doc = json_to_doc(filter)?;
        let opts = parse_find_options(&options);
        let has_options = options.is_some();
        let result = if has_options {
            engine.find_one_with_options(filter_doc, opts)
        } else {
            engine.find_one(filter_doc)
        }.map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(result.map(|d| doc_to_json(&d)))
    }

    #[napi]
    pub fn find(
        &self,
        filter: serde_json::Value,
        options: Option<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let engine = self.engine()?;
        let filter_doc = json_to_doc(filter)?;
        let opts = parse_find_options(&options);
        let has_options = options.is_some();
        let docs = if has_options {
            engine.find_with_options(filter_doc, opts)
        } else {
            engine.find(filter_doc)
        }.map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(docs_to_json_array(&docs))
    }

    // ---- UPDATE ----

    #[napi(js_name = "updateOne")]
    pub fn update_one(
        &self,
        filter: serde_json::Value,
        update: serde_json::Value,
        options: Option<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let filter_doc = json_to_doc(filter)?;
        let update_doc = json_to_doc(update)?;
        let opts = parse_update_options(&options);
        let result = self.engine()?.update_one_with_options(filter_doc, update_doc, opts)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let mut res = serde_json::json!({
            "matchedCount": result.matched_count,
            "modifiedCount": result.modified_count,
        });
        if let Some(id) = &result.upserted_id {
            res["upsertedId"] = bson_to_json(id);
        }
        Ok(res)
    }

    #[napi(js_name = "updateMany")]
    pub fn update_many(
        &self,
        filter: serde_json::Value,
        update: serde_json::Value,
        options: Option<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let filter_doc = json_to_doc(filter)?;
        let update_doc = json_to_doc(update)?;
        let opts = parse_update_options(&options);
        let result = self.engine()?.update_many_with_options(filter_doc, update_doc, opts)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let mut res = serde_json::json!({
            "matchedCount": result.matched_count,
            "modifiedCount": result.modified_count,
        });
        if let Some(id) = &result.upserted_id {
            res["upsertedId"] = bson_to_json(id);
        }
        Ok(res)
    }

    // ---- DELETE ----

    #[napi(js_name = "deleteOne")]
    pub fn delete_one(&self, filter: serde_json::Value) -> napi::Result<serde_json::Value> {
        let filter_doc = json_to_doc(filter)?;
        let result = self.engine()?.delete_one(filter_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({ "deletedCount": result.deleted_count }))
    }

    #[napi(js_name = "deleteMany")]
    pub fn delete_many(&self, filter: serde_json::Value) -> napi::Result<serde_json::Value> {
        let filter_doc = json_to_doc(filter)?;
        let result = self.engine()?.delete_many(filter_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({ "deletedCount": result.deleted_count }))
    }

    // ---- COUNT ----

    #[napi(js_name = "countDocuments")]
    pub fn count_documents(
        &self,
        filter: Option<serde_json::Value>,
    ) -> napi::Result<i64> {
        let filter_doc = filter.map(json_to_doc).transpose()?;
        let count = self.engine()?.count_documents(filter_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(count as i64)
    }

    // ---- AGGREGATION ----

    #[napi]
    pub fn aggregate(&self, pipeline: serde_json::Value) -> napi::Result<serde_json::Value> {
        let stages = json_vec_to_docs(pipeline)?;
        let docs = self.engine()?.aggregate(stages)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(docs_to_json_array(&docs))
    }

    #[napi(js_name = "explainAggregate")]
    pub fn explain_aggregate(
        &self,
        pipeline: serde_json::Value,
    ) -> napi::Result<serde_json::Value> {
        let stages = json_vec_to_docs(pipeline)?;
        let explain = self.engine()?.explain_aggregate(stages)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({
            "executionPlan": match &explain.execution_plan {
                smongo_engine::explain::ExecutionPlanExplain::CollectionScan => "COLLSCAN",
                smongo_engine::explain::ExecutionPlanExplain::IndexScan { .. } => "IXSCAN",
                smongo_engine::explain::ExecutionPlanExplain::IndexSeek { .. } => "IXSEEK",
                smongo_engine::explain::ExecutionPlanExplain::Geo { .. } => "GEO",
                smongo_engine::explain::ExecutionPlanExplain::OrUnion => "OR_UNION",
            },
            "indexUsed": explain.index_used,
            "planReason": explain.plan_reason,
            "executionStats": {
                "documentsExamined": explain.execution_stats.documents_examined,
                "documentsReturned": explain.execution_stats.documents_returned,
                "indexEntriesExamined": explain.execution_stats.index_entries_examined,
            },
            "efficiency": explain.efficiency(),
            "summary": explain.summary(),
        }))
    }

    // ---- INDEXES ----

    #[napi(js_name = "createIndex")]
    pub fn create_index(
        &self,
        keys: serde_json::Value,
        options: Option<serde_json::Value>,
    ) -> napi::Result<String> {
        let keys_doc = json_to_doc(keys)?;
        let opts = options.map(|o| EngineIndexOptions {
            name: o
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            unique: o.get("unique").and_then(|v| v.as_bool()).unwrap_or(false),
            sparse: o.get("sparse").and_then(|v| v.as_bool()).unwrap_or(false),
            background: o
                .get("background")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            expire_after_seconds: o
                .get("expireAfterSeconds")
                .and_then(|v| v.as_u64()),
        });
        self.engine()?
            .create_index(keys_doc, opts)
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    #[napi(js_name = "reapExpired")]
    pub fn reap_expired(&self) -> napi::Result<i64> {
        let count = self.engine()?.reap_expired()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(count as i64)
    }

    #[napi(js_name = "dropIndex")]
    pub fn drop_index(&self, name: String) -> napi::Result<()> {
        self.engine()?
            .drop_index(&name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    #[napi(js_name = "listIndexes")]
    pub fn list_indexes(&self) -> napi::Result<serde_json::Value> {
        let indexes = self.engine()?.list_indexes()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let arr: Vec<serde_json::Value> = indexes
            .iter()
            .map(|idx| {
                let keys = doc_to_json(&idx.keys);
                let mut opts = serde_json::json!({
                    "unique": idx.options.unique,
                    "sparse": idx.options.sparse,
                    "background": idx.options.background,
                });
                if let Some(ttl) = idx.options.expire_after_seconds {
                    opts["expireAfterSeconds"] = serde_json::json!(ttl);
                }
                serde_json::json!({
                    "name": idx.name,
                    "keys": keys,
                    "options": opts,
                })
            })
            .collect();
        Ok(serde_json::Value::Array(arr))
    }
}

// ============================================================
// ClientSession — multi-collection transaction support
// ============================================================

#[napi]
pub struct ClientSession {
    inner: EngineTransactionSession,
}

#[napi]
impl ClientSession {
    /// Begin a transaction on this session.
    #[napi(js_name = "startTransaction")]
    pub fn start_transaction(&self) -> napi::Result<()> {
        self.inner
            .begin_transaction()
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Commit the current transaction.
    #[napi(js_name = "commitTransaction")]
    pub fn commit_transaction(&self) -> napi::Result<()> {
        self.inner
            .commit_transaction()
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Abort / roll back the current transaction.
    #[napi(js_name = "abortTransaction")]
    pub fn abort_transaction(&self) -> napi::Result<()> {
        self.inner
            .rollback_transaction()
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Insert a single document into a collection within this transaction.
    ///
    /// Returns `{ insertedId: string }`.
    #[napi(js_name = "insertOne")]
    pub fn insert_one(
        &self,
        collection_name: String,
        document: serde_json::Value,
    ) -> napi::Result<serde_json::Value> {
        let col = self.inner.collection(&collection_name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let doc = json_to_doc(document)?;
        let result = col.insert_one(doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({
            "insertedId": bson_to_json(&result.inserted_id),
        }))
    }

    /// Find a single document within this transaction.
    #[napi(js_name = "findOne")]
    pub fn find_one(
        &self,
        collection_name: String,
        filter: serde_json::Value,
    ) -> napi::Result<Option<serde_json::Value>> {
        let col = self.inner.collection(&collection_name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let filter_doc = json_to_doc(filter)?;
        let result = col.find_one(filter_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(result.map(|d| doc_to_json(&d)))
    }

    /// Find all documents matching the filter within this transaction.
    #[napi]
    pub fn find(
        &self,
        collection_name: String,
        filter: serde_json::Value,
    ) -> napi::Result<serde_json::Value> {
        let col = self.inner.collection(&collection_name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let filter_doc = json_to_doc(filter)?;
        let docs = col.find(filter_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(docs_to_json_array(&docs))
    }

    /// Update a single document within this transaction.
    ///
    /// Returns `{ matchedCount, modifiedCount }`.
    #[napi(js_name = "updateOne")]
    pub fn update_one(
        &self,
        collection_name: String,
        filter: serde_json::Value,
        update: serde_json::Value,
    ) -> napi::Result<serde_json::Value> {
        let col = self.inner.collection(&collection_name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let filter_doc = json_to_doc(filter)?;
        let update_doc = json_to_doc(update)?;
        let result = col.update_one(filter_doc, update_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({
            "matchedCount": result.matched_count,
            "modifiedCount": result.modified_count,
        }))
    }

    /// Update all documents matching the filter within this transaction.
    ///
    /// Returns `{ matchedCount, modifiedCount }`.
    #[napi(js_name = "updateMany")]
    pub fn update_many(
        &self,
        collection_name: String,
        filter: serde_json::Value,
        update: serde_json::Value,
    ) -> napi::Result<serde_json::Value> {
        let col = self.inner.collection(&collection_name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let filter_doc = json_to_doc(filter)?;
        let update_doc = json_to_doc(update)?;
        let result = col.update_many(filter_doc, update_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({
            "matchedCount": result.matched_count,
            "modifiedCount": result.modified_count,
        }))
    }

    /// Delete a single document within this transaction.
    ///
    /// Returns `{ deletedCount }`.
    #[napi(js_name = "deleteOne")]
    pub fn delete_one(
        &self,
        collection_name: String,
        filter: serde_json::Value,
    ) -> napi::Result<serde_json::Value> {
        let col = self.inner.collection(&collection_name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let filter_doc = json_to_doc(filter)?;
        let result = col.delete_one(filter_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({ "deletedCount": result.deleted_count }))
    }

    /// Delete all documents matching the filter within this transaction.
    ///
    /// Returns `{ deletedCount }`.
    #[napi(js_name = "deleteMany")]
    pub fn delete_many(
        &self,
        collection_name: String,
        filter: serde_json::Value,
    ) -> napi::Result<serde_json::Value> {
        let col = self.inner.collection(&collection_name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let filter_doc = json_to_doc(filter)?;
        let result = col.delete_many(filter_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(serde_json::json!({ "deletedCount": result.deleted_count }))
    }

    /// Count documents matching the filter within this transaction.
    #[napi(js_name = "countDocuments")]
    pub fn count_documents(
        &self,
        collection_name: String,
        filter: Option<serde_json::Value>,
    ) -> napi::Result<i64> {
        let col = self.inner.collection(&collection_name)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let filter_doc = filter.map(json_to_doc).transpose()?;
        let count = col.count_documents(filter_doc)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(count as i64)
    }
}
