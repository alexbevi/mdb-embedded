// OPFS Worker - handles WASM + OPFS in worker context where sync access is available
import init, { WasmOpfsDatabase } from '../pkg/smongo_engine.js';
import { BSON } from '../node_modules/bson/lib/bson.mjs';

/** Keep in sync with opfs-wrapper.js OPFS_RPC_LIMITS (defense in depth). */
const MAX_DB_NAME_LEN = 128;
const MAX_COLL_LEN = 256;
const MAX_COLLECTIONS = 256;

let db = null;

/**
 * @param {number} id
 * @param {unknown} err
 * @param {string} [errorCode]
 */
function postErr(id, err, errorCode = 'OPFS_WORKER_ERROR') {
  const msg = err instanceof Error ? err.message : String(err);
  self.postMessage({ id, error: msg, errorCode });
}

/**
 * @param {unknown} name
 * @returns {name is string}
 */
function validDbName(name) {
  return (
    typeof name === 'string' &&
    name.length > 0 &&
    name.length <= MAX_DB_NAME_LEN &&
    /^[a-zA-Z0-9._-]+$/.test(name) &&
    !name.includes('..') &&
    !name.startsWith('.') &&
    !name.endsWith('.')
  );
}

/**
 * @param {unknown} name
 * @returns {name is string}
 */
function validCollName(name) {
  return typeof name === 'string' && name.length > 0 && name.length <= MAX_COLL_LEN && /^[a-zA-Z0-9._-]+$/.test(name);
}

/**
 * @param {number} id
 * @returns {boolean}
 */
function requireDb(id) {
  if (!db) {
    postErr(id, new Error('Database not initialized; send init first'), 'OPFS_NOT_INITIALIZED');
    return false;
  }
  return true;
}

self.onmessage = async (e) => {
  const data = e.data;
  if (!data || typeof data !== 'object') {
    return;
  }
  const { id, type, payload } = data;
  if (typeof id !== 'number' || !Number.isFinite(id) || typeof type !== 'string') {
    return;
  }

  const p = payload && typeof payload === 'object' ? payload : {};

  try {
    switch (type) {
      case 'init': {
        if (db) {
          postErr(id, new Error('OPFS worker already initialized'), 'OPFS_ALREADY_INITIALIZED');
          break;
        }

        const { dbName, collections } = p;
        if (!validDbName(dbName)) {
          postErr(id, new Error('Invalid dbName in worker init'), 'OPFS_INVALID_DB_NAME');
          break;
        }
        if (!Array.isArray(collections) || collections.length === 0 || collections.length > MAX_COLLECTIONS) {
          postErr(id, new Error('collections must be a non-empty array'), 'OPFS_INVALID_PAYLOAD');
          break;
        }
        if (!collections.every((c) => validCollName(c))) {
          postErr(id, new Error('Invalid collection name in init'), 'OPFS_INVALID_COLLECTION');
          break;
        }

        await init();

        const root = await navigator.storage.getDirectory();
        const dbDir = await root.getDirectoryHandle(dbName, { create: true });

        for (const collName of collections) {
          await dbDir.getFileHandle(collName, { create: true });
        }

        const handlesMap = new Map();
        for await (const [tableName, entry] of dbDir.entries()) {
          if (entry.kind === 'file') {
            const fileHandle = await dbDir.getFileHandle(tableName);
            const syncHandle = await fileHandle.createSyncAccessHandle();
            handlesMap.set(tableName, syncHandle);
          }
        }

        db = new WasmOpfsDatabase(dbName, handlesMap);
        self.postMessage({ id, result: { success: true } });
        break;
      }

      case 'insertOne': {
        if (!requireDb(id)) break;
        const { collection, doc } = p;
        if (!validCollName(collection) || doc === undefined || doc === null || typeof doc !== 'object' || Array.isArray(doc)) {
          postErr(id, new Error('insertOne: invalid collection or doc'), 'OPFS_INVALID_PAYLOAD');
          break;
        }
        const coll = db.collection(collection);
        const bytes = BSON.serialize(doc);
        const resultBytes = coll.insert_one(Array.from(bytes));
        const result = BSON.deserialize(new Uint8Array(resultBytes));
        self.postMessage({ id, result });
        break;
      }

      case 'find': {
        if (!requireDb(id)) break;
        const { collection, filter } = p;
        if (!validCollName(collection)) {
          postErr(id, new Error('find: invalid collection'), 'OPFS_INVALID_COLLECTION');
          break;
        }
        const f = filter === undefined ? {} : filter;
        if (typeof f !== 'object' || f === null || Array.isArray(f)) {
          postErr(id, new Error('find: filter must be an object'), 'OPFS_INVALID_PAYLOAD');
          break;
        }
        const coll = db.collection(collection);
        const bytes = BSON.serialize(f);
        const resultBytes = coll.find(Array.from(bytes));
        const result = BSON.deserialize(new Uint8Array(resultBytes));
        self.postMessage({ id, result: result.results });
        break;
      }

      case 'countDocuments': {
        if (!requireDb(id)) break;
        const { collection, filter } = p;
        if (!validCollName(collection)) {
          postErr(id, new Error('countDocuments: invalid collection'), 'OPFS_INVALID_COLLECTION');
          break;
        }
        const f = filter === undefined ? {} : filter;
        if (typeof f !== 'object' || f === null || Array.isArray(f)) {
          postErr(id, new Error('countDocuments: filter must be an object'), 'OPFS_INVALID_PAYLOAD');
          break;
        }
        const coll = db.collection(collection);
        const bytes = BSON.serialize(f);
        const count = coll.count_documents(Array.from(bytes));
        self.postMessage({ id, result: count });
        break;
      }

      case 'deleteMany': {
        if (!requireDb(id)) break;
        const { collection, filter } = p;
        if (!validCollName(collection)) {
          postErr(id, new Error('deleteMany: invalid collection'), 'OPFS_INVALID_COLLECTION');
          break;
        }
        if (typeof filter !== 'object' || filter === null || Array.isArray(filter)) {
          postErr(id, new Error('deleteMany: filter must be an object'), 'OPFS_INVALID_PAYLOAD');
          break;
        }
        const coll = db.collection(collection);
        const bytes = BSON.serialize(filter);
        const resultBytes = coll.delete_many(Array.from(bytes));
        const result = BSON.deserialize(new Uint8Array(resultBytes));
        self.postMessage({ id, result });
        break;
      }

      case 'updateMany': {
        if (!requireDb(id)) break;
        const { collection, filter, update } = p;
        if (!validCollName(collection)) {
          postErr(id, new Error('updateMany: invalid collection'), 'OPFS_INVALID_COLLECTION');
          break;
        }
        if (typeof filter !== 'object' || filter === null || Array.isArray(filter)) {
          postErr(id, new Error('updateMany: invalid filter'), 'OPFS_INVALID_PAYLOAD');
          break;
        }
        if (typeof update !== 'object' || update === null || Array.isArray(update)) {
          postErr(id, new Error('updateMany: invalid update'), 'OPFS_INVALID_PAYLOAD');
          break;
        }
        const coll = db.collection(collection);
        const filterBytes = BSON.serialize(filter);
        const updateBytes = BSON.serialize(update);
        const resultBytes = coll.update_many(Array.from(filterBytes), Array.from(updateBytes));
        const result = BSON.deserialize(new Uint8Array(resultBytes));
        self.postMessage({ id, result });
        break;
      }

      case 'shutdown': {
        if (db) {
          db.free();
          db = null;
        }
        self.postMessage({ id, result: { success: true } });
        break;
      }

      default:
        postErr(id, new Error(`Unknown message type: ${type}`), 'OPFS_INVALID_REQUEST');
    }
  } catch (error) {
    postErr(id, error, 'OPFS_WORKER_ERROR');
  }
};
