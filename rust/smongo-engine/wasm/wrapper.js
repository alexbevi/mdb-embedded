// smongo WASM wrapper: Friendlier JavaScript API over raw wasm-bindgen exports
//
// Provides automatic BSON serialization/deserialization using MongoDB's official
// `bson` package. This layer keeps the wasm-bindgen boundary clean (raw bytes only)
// while giving JavaScript users a natural object-based API.
//
// In-memory only. For persistent OPFS (worker + Web Locks + multi-tab RPC), use
// opfs-wrapper.js or smongo-browser.js.

import init, { WasmDatabase } from '../pkg/smongo_engine.js';
import { BSON } from '../node_modules/bson/lib/bson.mjs';

/**
 * Initialize the WASM module. Must be called once before using Database/Collection.
 * @returns {Promise<void>}
 */
export async function initSmongo() {
  await init();
}

/**
 * Database handle (in-memory storage).
 */
export class Database {
  /**
   * Create a new database.
   * @param {string} name - Database name
   */
  constructor(name) {
    this._db = new WasmDatabase(name);
  }

  /**
   * Get a collection handle.
   * @param {string} name - Collection name
   * @returns {Collection}
   */
  collection(name) {
    const wasmColl = this._db.collection(name);
    return new Collection(wasmColl);
  }
}

/**
 * Collection handle for CRUD operations.
 */
export class Collection {
  constructor(wasmColl) {
    this._coll = wasmColl;
  }

  /**
   * Insert a single document.
   * @param {Object} doc - Document to insert (plain JS object)
   * @returns {Object} Result with insertedId field
   */
  insertOne(doc) {
    const bytes = BSON.serialize(doc);
    const resultBytes = this._coll.insert_one(Array.from(bytes));
    return BSON.deserialize(new Uint8Array(resultBytes));
  }

  /**
   * Find documents matching a filter.
   * @param {Object} filter - Query filter (empty object matches all)
   * @returns {Array<Object>} Array of matching documents
   */
  find(filter = {}) {
    const bytes = BSON.serialize(filter);
    const resultBytes = this._coll.find(Array.from(bytes));
    const result = BSON.deserialize(new Uint8Array(resultBytes));
    return result.results;
  }

  /**
   * Count documents matching a filter.
   * @param {Object} filter - Query filter
   * @returns {number} Document count
   */
  countDocuments(filter = {}) {
    const bytes = BSON.serialize(filter);
    return this._coll.count_documents(Array.from(bytes));
  }

  /**
   * Delete documents matching a filter.
   * @param {Object} filter - Query filter
   * @returns {Object} Result with deletedCount field
   */
  deleteMany(filter) {
    const bytes = BSON.serialize(filter);
    const resultBytes = this._coll.delete_many(Array.from(bytes));
    return BSON.deserialize(new Uint8Array(resultBytes));
  }

  /**
   * Update documents matching a filter.
   * @param {Object} filter - Query filter
   * @param {Object} update - Update operators (e.g., { "$set": { ... } })
   * @returns {Object} Result with matchedCount and modifiedCount fields
   */
  updateMany(filter, update) {
    const filterBytes = BSON.serialize(filter);
    const updateBytes = BSON.serialize(update);
    const resultBytes = this._coll.update_many(Array.from(filterBytes), Array.from(updateBytes));
    return BSON.deserialize(new Uint8Array(resultBytes));
  }
}
