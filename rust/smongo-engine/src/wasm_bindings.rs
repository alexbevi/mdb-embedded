//! WASM bindings for smongo-engine: browser-compatible database API
//!
//! Exposes a minimal JavaScript API over wasm-bindgen. Follows the C ABI pattern:
//! raw BSON bytes at every boundary to avoid expensive type marshalling.
//!
//! # Design
//!
//! - `WasmDatabase` wraps `Database<MemBackend>` (no file I/O on WASM)
//! - `WasmCollection` wraps `Collection<MemBackend>`
//! - All document parameters and results are BSON byte arrays (`Vec<u8>`)
//! - JavaScript layer uses MongoDB's `bson` package for serialization
//!
//! # Example (from JavaScript)
//!
//! ```javascript
//! import init, { WasmDatabase } from './pkg/smongo_engine.js';
//! import { BSON } from 'bson';
//!
//! await init();
//! const db = new WasmDatabase('mydb');
//! const coll = db.collection('users');
//!
//! // Insert
//! const doc = { name: 'Alice', age: 30 };
//! const docBytes = BSON.serialize(doc);
//! const resultBytes = coll.insert_one(Array.from(docBytes));
//! const result = BSON.deserialize(new Uint8Array(resultBytes));
//!
//! // Find
//! const filterBytes = BSON.serialize({ name: 'Alice' });
//! const docsBytes = coll.find(Array.from(filterBytes));
//! const docs = BSON.deserialize(new Uint8Array(docsBytes)).results;
//! ```

use wasm_bindgen::prelude::*;
use bson::{doc, from_slice, to_vec};
use std::collections::BTreeMap;
use js_sys::{Map, Iterator};
use web_sys::FileSystemSyncAccessHandle;

use crate::collection::Collection;
use crate::database::Database;
use crate::storage::{MemBackend, MemSession, OpfsBackend, OpfsSession};

/// WASM-compatible database handle.
///
/// Uses in-memory storage only (`MemBackend`). For persistent storage in browsers,
/// use [`WasmOpfsDatabase`] with OPFS sync handles (typically via `initOpfsDatabase` /
/// `smongo-browser.js`). Lifecycle, errors, and recovery: `wasm/PERSISTENCE-AND-LIFECYCLE.md`.
#[wasm_bindgen]
pub struct WasmDatabase {
    inner: Database<MemBackend>,
}

#[wasm_bindgen]
impl WasmDatabase {
    /// Create a new in-memory database.
    ///
    /// # Arguments
    /// * `name` - Database name (used for oplog namespacing)
    ///
    /// # Returns
    /// `WasmDatabase` handle
    #[wasm_bindgen(constructor)]
    pub fn new(name: String) -> WasmDatabase {
        let backend = MemBackend::new();
        let db = Database::from_backend(backend, &name, None);
        WasmDatabase { inner: db }
    }

    /// Get a collection handle.
    ///
    /// # Arguments
    /// * `name` - Collection name
    ///
    /// # Returns
    /// `WasmCollection` handle (collection is created lazily on first write)
    ///
    /// # Errors
    /// Returns `JsValue` error if collection creation fails
    pub fn collection(&self, name: String) -> Result<WasmCollection, JsValue> {
        let coll = self
            .inner
            .collection(&name)
            .map_err(|e| JsValue::from_str(&format!("Collection error: {}", e)))?;
        Ok(WasmCollection { inner: coll })
    }
}

/// WASM-compatible collection handle.
///
/// All methods accept and return raw BSON bytes.
#[wasm_bindgen]
pub struct WasmCollection {
    inner: Collection<MemSession>,
}

#[wasm_bindgen]
impl WasmCollection {
    /// Insert a single document.
    ///
    /// # Arguments
    /// * `doc_bytes` - Raw BSON bytes representing the document to insert
    ///
    /// # Returns
    /// Raw BSON bytes containing `{ insertedId: ObjectId }` on success
    ///
    /// # Errors
    /// Returns `JsValue` error if BSON parsing or insert fails
    pub fn insert_one(&self, doc_bytes: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let doc = from_slice(&doc_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error: {}", e)))?;

        let result = self
            .inner
            .insert_one(doc)
            .map_err(|e| JsValue::from_str(&format!("Insert error: {}", e)))?;

        let result_doc = doc! { "insertedId": result.inserted_id };
        to_vec(&result_doc)
            .map_err(|e| JsValue::from_str(&format!("BSON serialize error: {}", e)))
    }

    /// Find documents matching a filter.
    ///
    /// # Arguments
    /// * `filter_bytes` - Raw BSON bytes representing the query filter (empty doc matches all)
    ///
    /// # Returns
    /// Raw BSON bytes containing `{ results: [doc1, doc2, ...] }` on success
    ///
    /// # Errors
    /// Returns `JsValue` error if BSON parsing or query fails
    pub fn find(&self, filter_bytes: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let filter = from_slice(&filter_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error: {}", e)))?;

        let docs = self
            .inner
            .find(filter)
            .map_err(|e| JsValue::from_str(&format!("Find error: {}", e)))?;

        // Wrap in object with "results" key for consistent return format
        let result = doc! { "results": docs };
        to_vec(&result)
            .map_err(|e| JsValue::from_str(&format!("BSON serialize error: {}", e)))
    }

    /// Count documents matching a filter.
    ///
    /// # Arguments
    /// * `filter_bytes` - Raw BSON bytes representing the query filter
    ///
    /// # Returns
    /// Count as f64 (JavaScript number)
    ///
    /// # Errors
    /// Returns `JsValue` error if BSON parsing or count fails
    pub fn count_documents(&self, filter_bytes: Vec<u8>) -> Result<f64, JsValue> {
        let filter = from_slice(&filter_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error: {}", e)))?;

        let count = self
            .inner
            .count_documents(filter)
            .map_err(|e| JsValue::from_str(&format!("Count error: {}", e)))?;

        Ok(count as f64)
    }

    /// Delete documents matching a filter.
    ///
    /// # Arguments
    /// * `filter_bytes` - Raw BSON bytes representing the query filter
    ///
    /// # Returns
    /// Raw BSON bytes containing `{ deletedCount: i64 }` on success
    ///
    /// # Errors
    /// Returns `JsValue` error if BSON parsing or delete fails
    pub fn delete_many(&self, filter_bytes: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let filter = from_slice(&filter_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error: {}", e)))?;

        let result = self
            .inner
            .delete_many(filter)
            .map_err(|e| JsValue::from_str(&format!("Delete error: {}", e)))?;

        let result_doc = doc! { "deletedCount": result.deleted_count as i64 };
        to_vec(&result_doc)
            .map_err(|e| JsValue::from_str(&format!("BSON serialize error: {}", e)))
    }

    /// Update documents matching a filter.
    ///
    /// # Arguments
    /// * `filter_bytes` - Raw BSON bytes representing the query filter
    /// * `update_bytes` - Raw BSON bytes representing the update operators (e.g., `{ "$set": { ... } }`)
    ///
    /// # Returns
    /// Raw BSON bytes containing `{ matchedCount: i64, modifiedCount: i64 }` on success
    ///
    /// # Errors
    /// Returns `JsValue` error if BSON parsing or update fails
    pub fn update_many(
        &self,
        filter_bytes: Vec<u8>,
        update_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, JsValue> {
        let filter = from_slice(&filter_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error (filter): {}", e)))?;

        let update = from_slice(&update_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error (update): {}", e)))?;

        let result = self
            .inner
            .update_many(filter, update)
            .map_err(|e| JsValue::from_str(&format!("Update error: {}", e)))?;

        let result_doc = doc! {
            "matchedCount": result.matched_count as i64,
            "modifiedCount": result.modified_count as i64
        };
        to_vec(&result_doc)
            .map_err(|e| JsValue::from_str(&format!("BSON serialize error: {}", e)))
    }
}
// OPFS persistent storage variants

#[wasm_bindgen]
pub struct WasmOpfsDatabase {
    inner: Database<OpfsBackend>,
}

#[wasm_bindgen]
impl WasmOpfsDatabase {
    #[wasm_bindgen(constructor)]
    pub fn new(name: String, handles: JsValue) -> Result<WasmOpfsDatabase, JsValue> {
        let js_map = handles.dyn_into::<Map>()
            .map_err(|_| JsValue::from_str("handles must be a Map"))?;

        let mut map = BTreeMap::new();
        let iter = js_map.entries();

        loop {
            let next = Iterator::next(&iter)
                .map_err(|e| JsValue::from_str(&format!("Iterator error: {:?}", e)))?;

            if next.done() {
                break;
            }

            let entry = next.value();
            let arr: js_sys::Array = entry.into();
            let key = arr.get(0).as_string()
                .ok_or_else(|| JsValue::from_str("key must be string"))?;
            let handle = arr.get(1).dyn_into::<FileSystemSyncAccessHandle>()
                .map_err(|_| JsValue::from_str("value must be FileSystemSyncAccessHandle"))?;

            map.insert(key, handle);
        }

        let backend = OpfsBackend::from_handles(map);
        let db = Database::from_backend(backend, &name, None);
        Ok(WasmOpfsDatabase { inner: db })
    }

    pub fn collection(&self, name: String) -> Result<WasmOpfsCollection, JsValue> {
        let coll = self.inner.collection(&name)
            .map_err(|e| JsValue::from_str(&format!("Collection error: {}", e)))?;
        Ok(WasmOpfsCollection { inner: coll })
    }
}

#[wasm_bindgen]
pub struct WasmOpfsCollection {
    inner: Collection<OpfsSession>,
}

#[wasm_bindgen]
impl WasmOpfsCollection {
    pub fn insert_one(&self, doc_bytes: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let doc = from_slice(&doc_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error: {}", e)))?;
        let result = self.inner.insert_one(doc)
            .map_err(|e| JsValue::from_str(&format!("Insert error: {}", e)))?;
        let result_doc = doc! { "insertedId": result.inserted_id };
        to_vec(&result_doc)
            .map_err(|e| JsValue::from_str(&format!("BSON serialize error: {}", e)))
    }

    pub fn find(&self, filter_bytes: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let filter = from_slice(&filter_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error: {}", e)))?;
        let docs = self.inner.find(filter)
            .map_err(|e| JsValue::from_str(&format!("Find error: {}", e)))?;
        let result = doc! { "results": docs };
        to_vec(&result)
            .map_err(|e| JsValue::from_str(&format!("BSON serialize error: {}", e)))
    }

    pub fn count_documents(&self, filter_bytes: Vec<u8>) -> Result<f64, JsValue> {
        let filter = from_slice(&filter_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error: {}", e)))?;
        let count = self.inner.count_documents(filter)
            .map_err(|e| JsValue::from_str(&format!("Count error: {}", e)))?;
        Ok(count as f64)
    }

    pub fn delete_many(&self, filter_bytes: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let filter = from_slice(&filter_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error: {}", e)))?;
        let result = self.inner.delete_many(filter)
            .map_err(|e| JsValue::from_str(&format!("Delete error: {}", e)))?;
        let result_doc = doc! { "deletedCount": result.deleted_count as i64 };
        to_vec(&result_doc)
            .map_err(|e| JsValue::from_str(&format!("BSON serialize error: {}", e)))
    }

    pub fn update_many(&self, filter_bytes: Vec<u8>, update_bytes: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let filter = from_slice(&filter_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error (filter): {}", e)))?;
        let update = from_slice(&update_bytes)
            .map_err(|e| JsValue::from_str(&format!("BSON parse error (update): {}", e)))?;
        let result = self.inner.update_many(filter, update)
            .map_err(|e| JsValue::from_str(&format!("Update error: {}", e)))?;
        let result_doc = doc! {
            "matchedCount": result.matched_count as i64,
            "modifiedCount": result.modified_count as i64
        };
        to_vec(&result_doc)
            .map_err(|e| JsValue::from_str(&format!("BSON serialize error: {}", e)))
    }
}
