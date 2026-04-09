//! C ABI for smongo embedded database engine.
//!
//! All document data crosses the FFI boundary as raw BSON bytes.
//! Error handling follows the SQLite pattern: return codes + thread-local
//! error message via `smongo_last_error()`.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::io::Cursor;
use std::os::raw::c_char;
use std::ptr;
use std::slice;

use bson::Document;
use smongo_engine::collection::{Collection, CollectionError, FindOptions, UpdateOptions};
use smongo_engine::database::{Database, DatabaseError, TransactionSession};
use smongo_engine::index::IndexOptions;

// ---------------------------------------------------------------------------
// Return codes
// ---------------------------------------------------------------------------

pub const SMONGO_OK: i32 = 0;
pub const SMONGO_ERROR: i32 = -1;
pub const SMONGO_ERROR_NULL_PTR: i32 = -2;
pub const SMONGO_ERROR_INVALID_UTF8: i32 = -3;
pub const SMONGO_ERROR_INVALID_BSON: i32 = -4;
pub const SMONGO_ERROR_NOT_FOUND: i32 = -5;

// ---------------------------------------------------------------------------
// Thread-local error state
// ---------------------------------------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(msg: &str) {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = CString::new(msg).ok();
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

fn map_db_error(err: DatabaseError) -> i32 {
    set_last_error(&err.to_string());
    match err {
        DatabaseError::DatabaseNotFound | DatabaseError::CollectionNotFound(_) => {
            SMONGO_ERROR_NOT_FOUND
        }
        _ => SMONGO_ERROR,
    }
}

fn map_col_error(err: CollectionError) -> i32 {
    set_last_error(&err.to_string());
    SMONGO_ERROR
}

// ---------------------------------------------------------------------------
// Opaque handle types
// ---------------------------------------------------------------------------

/// Opaque database handle — C consumers only see a pointer.
pub struct SmongoDb {
    inner: Database,
}

/// Opaque collection handle — C consumers only see a pointer.
pub struct SmongoCollection {
    inner: Collection,
}

/// Opaque cursor for iterating query results — C consumers only see a pointer.
pub struct SmongoCursor {
    docs: Vec<Document>,
    position: usize,
    current_bytes: Option<Vec<u8>>,
}

/// Opaque session handle for multi-collection transactions.
pub struct SmongoSession {
    inner: TransactionSession,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn deserialize_doc(data: *const u8, len: usize) -> Result<Document, i32> {
    if data.is_null() {
        set_last_error("null BSON data pointer");
        return Err(SMONGO_ERROR_NULL_PTR);
    }
    let bytes = unsafe { slice::from_raw_parts(data, len) };
    let mut reader = Cursor::new(bytes);
    Document::from_reader(&mut reader).map_err(|e| {
        set_last_error(&format!("BSON deserialization error: {}", e));
        SMONGO_ERROR_INVALID_BSON
    })
}

fn serialize_doc(doc: &Document) -> Result<Vec<u8>, i32> {
    let mut buf = Vec::new();
    doc.to_writer(&mut buf).map_err(|e| {
        set_last_error(&format!("BSON serialization error: {}", e));
        SMONGO_ERROR
    })?;
    Ok(buf)
}

/// Write serialized bytes into caller-provided out-pointers.
///
/// Allocates a buffer that the caller must free with `smongo_free`.
///
/// # Safety
/// `out` and `out_len` must be valid, non-null pointers.
unsafe fn write_bytes_out(
    bytes: Vec<u8>,
    out: *mut *mut u8,
    out_len: *mut usize,
) {
    let len = bytes.len();
    let boxed = bytes.into_boxed_slice();
    let ptr = Box::into_raw(boxed) as *mut u8;
    unsafe {
        *out = ptr;
        *out_len = len;
    }
}

/// Parse a BSON document whose keys are stringified array indices ("0", "1", ...)
/// into an ordered `Vec<Document>`.
fn parse_bson_array_doc(array_doc: &Document) -> Result<Vec<Document>, i32> {
    let mut indexed: Vec<(usize, Document)> = Vec::new();
    for (key, value) in array_doc.iter() {
        let idx: usize = match key.parse() {
            Ok(i) => i,
            Err(_) => {
                set_last_error(&format!(
                    "expected a BSON array document; unexpected key: {}",
                    key
                ));
                return Err(SMONGO_ERROR_INVALID_BSON);
            }
        };
        let doc = match value.as_document() {
            Some(d) => d.clone(),
            None => {
                set_last_error(&format!("element at index {} is not a document", key));
                return Err(SMONGO_ERROR_INVALID_BSON);
            }
        };
        indexed.push((idx, doc));
    }
    indexed.sort_by_key(|(i, _)| *i);
    Ok(indexed.into_iter().map(|(_, d)| d).collect())
}

fn cstr_to_str<'a>(s: *const c_char) -> Result<&'a str, i32> {
    if s.is_null() {
        set_last_error("null string pointer");
        return Err(SMONGO_ERROR_NULL_PTR);
    }
    unsafe { CStr::from_ptr(s) }.to_str().map_err(|_| {
        set_last_error("invalid UTF-8 in string argument");
        SMONGO_ERROR_INVALID_UTF8
    })
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Open or create a database at the given filesystem path.
///
/// On success, writes a heap-allocated `SmongoDb` handle into `*out` and
/// returns `SMONGO_OK`.  The caller must eventually pass this handle to
/// `smongo_close`.
///
/// # Safety
/// * `path` must be a valid, NUL-terminated C string.
/// * `out` must be a valid pointer to a `*mut SmongoDb`.
#[no_mangle]
pub unsafe extern "C" fn smongo_open(
    path: *const c_char,
    out: *mut *mut SmongoDb,
) -> i32 {
    clear_last_error();

    if out.is_null() {
        set_last_error("null output pointer");
        return SMONGO_ERROR_NULL_PTR;
    }

    let path_str = match cstr_to_str(path) {
        Ok(s) => s,
        Err(code) => return code,
    };

    match Database::open(path_str) {
        Ok(db) => {
            let handle = Box::new(SmongoDb { inner: db });
            unsafe { *out = Box::into_raw(handle) };
            SMONGO_OK
        }
        Err(e) => map_db_error(e),
    }
}

/// Close the database and free the handle.
///
/// After this call, `db` is invalid and must not be used.
///
/// # Safety
/// `db` must be a handle previously returned by `smongo_open`, or null
/// (in which case this is a no-op).
#[no_mangle]
pub unsafe extern "C" fn smongo_close(db: *mut SmongoDb) {
    clear_last_error();
    if !db.is_null() {
        drop(unsafe { Box::from_raw(db) });
    }
}

/// Drop the entire database, removing all data files.
///
/// This consumes the database handle — it must not be used after this call.
/// Pass null for a no-op.
///
/// # Safety
/// `db` must be a handle previously returned by `smongo_open`, or null.
#[no_mangle]
pub unsafe extern "C" fn smongo_drop(db: *mut SmongoDb) -> i32 {
    clear_last_error();
    if db.is_null() {
        return SMONGO_OK;
    }
    let handle = unsafe { Box::from_raw(db) };
    match handle.inner.drop() {
        Ok(()) => SMONGO_OK,
        Err(e) => map_db_error(e),
    }
}

/// List all collection names in the database.
///
/// On success, writes a BSON document containing a `names` array of strings
/// into `*result` / `*result_len`. Free the result with `smongo_free`.
///
/// # Safety
/// `db`, `result`, and `result_len` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn smongo_list_collection_names(
    db: *mut SmongoDb,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if db.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let db_ref = unsafe { &*db };
    match db_ref.inner.list_collection_names() {
        Ok(names) => {
            let bson_names: Vec<bson::Bson> =
                names.into_iter().map(bson::Bson::String).collect();
            let result_doc = bson::doc! { "names": bson_names };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_db_error(e),
    }
}

/// Drop a collection by name.
///
/// # Safety
/// `db` must be a valid `SmongoDb` handle. `name` must be a valid C string.
#[no_mangle]
pub unsafe extern "C" fn smongo_drop_collection(
    db: *mut SmongoDb,
    name: *const c_char,
) -> i32 {
    clear_last_error();

    if db.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name_str = match cstr_to_str(name) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let db_ref = unsafe { &*db };
    match db_ref.inner.drop_collection(name_str) {
        Ok(()) => SMONGO_OK,
        Err(e) => map_db_error(e),
    }
}

/// Get database statistics.
///
/// On success, writes a BSON document containing `collectionCount` and
/// `sizeBytes` into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_stats(
    db: *mut SmongoDb,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if db.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let db_ref = unsafe { &*db };
    match db_ref.inner.stats() {
        Ok(s) => {
            let result_doc = bson::doc! {
                "collectionCount": s.collection_count as i64,
                "sizeBytes": s.size_bytes as i64,
            };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_db_error(e),
    }
}

// ---------------------------------------------------------------------------
// Collection access
// ---------------------------------------------------------------------------

/// Obtain a collection handle from an open database.
///
/// On success, writes a heap-allocated `SmongoCollection` into `*out`.
/// The caller must free it with `smongo_collection_free`.
///
/// # Safety
/// * `db` must be a valid `SmongoDb` handle.
/// * `name` must be a valid, NUL-terminated C string.
/// * `out` must be a valid pointer to a `*mut SmongoCollection`.
#[no_mangle]
pub unsafe extern "C" fn smongo_collection(
    db: *mut SmongoDb,
    name: *const c_char,
    out: *mut *mut SmongoCollection,
) -> i32 {
    clear_last_error();

    if db.is_null() || out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name_str = match cstr_to_str(name) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let db_ref = unsafe { &*db };
    match db_ref.inner.collection(name_str) {
        Ok(col) => {
            let handle = Box::new(SmongoCollection { inner: col });
            unsafe { *out = Box::into_raw(handle) };
            SMONGO_OK
        }
        Err(e) => map_db_error(e),
    }
}

/// Free a collection handle.
///
/// # Safety
/// `col` must be a handle previously returned by `smongo_collection`, or null.
#[no_mangle]
pub unsafe extern "C" fn smongo_collection_free(col: *mut SmongoCollection) {
    clear_last_error();
    if !col.is_null() {
        drop(unsafe { Box::from_raw(col) });
    }
}

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

/// Insert a single BSON document into the collection.
///
/// On success, writes the result (a BSON document containing `insertedId`)
/// into `*result` / `*result_len`.  Free the result buffer with `smongo_free`.
///
/// # Safety
/// All pointer arguments must be valid. `doc` must point to `doc_len` bytes
/// of valid BSON.
#[no_mangle]
pub unsafe extern "C" fn smongo_insert_one(
    col: *mut SmongoCollection,
    doc: *const u8,
    doc_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let document = match deserialize_doc(doc, doc_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.insert_one(document) {
        Ok(res) => {
            let result_doc = bson::doc! { "insertedId": res.inserted_id };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Insert multiple BSON documents into the collection.
///
/// `docs` must be a BSON document whose keys are stringified array indices
/// ("0", "1", ...), each mapping to a document — the standard BSON
/// array-as-document representation.
///
/// On success, writes a BSON result document containing `insertedIds` (an
/// array of the generated IDs) into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_insert_many(
    col: *mut SmongoCollection,
    docs: *const u8,
    docs_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let array_doc = match deserialize_doc(docs, docs_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let documents = match parse_bson_array_doc(&array_doc) {
        Ok(v) => v,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.insert_many(documents) {
        Ok(res) => {
            let ids: Vec<bson::Bson> = res.inserted_ids.into_iter().collect();
            let result_doc = bson::doc! { "insertedIds": ids };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

// ---------------------------------------------------------------------------
// Find
// ---------------------------------------------------------------------------

/// Find all documents matching the BSON filter, returning a cursor.
///
/// On success, writes a cursor handle into `*cursor_out`.  Iterate with
/// `smongo_cursor_next` and free with `smongo_cursor_free`.
///
/// # Safety
/// All pointer arguments must be valid. `filter` must point to valid BSON.
#[no_mangle]
pub unsafe extern "C" fn smongo_find(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    cursor_out: *mut *mut SmongoCursor,
) -> i32 {
    clear_last_error();

    if col.is_null() || cursor_out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.find(filter_doc) {
        Ok(docs) => {
            let cursor = Box::new(SmongoCursor {
                docs,
                position: 0,
                current_bytes: None,
            });
            unsafe { *cursor_out = Box::into_raw(cursor) };
            SMONGO_OK
        }
        Err(e) => map_col_error(e),
    }
}

/// Find a single document matching the BSON filter.
///
/// On success, writes the matching document's BSON bytes into
/// `*result` / `*result_len`.  Returns `SMONGO_ERROR_NOT_FOUND` if no
/// document matches (with `*result` set to null).
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_find_one(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.find_one(filter_doc) {
        Ok(Some(doc)) => match serialize_doc(&doc) {
            Ok(bytes) => {
                unsafe { write_bytes_out(bytes, result, result_len) };
                SMONGO_OK
            }
            Err(code) => code,
        },
        Ok(None) => {
            unsafe {
                *result = ptr::null_mut();
                *result_len = 0;
            }
            SMONGO_ERROR_NOT_FOUND
        }
        Err(e) => map_col_error(e),
    }
}

/// Find all documents with options (sort/limit/skip/projection).
///
/// `options` is a BSON document with optional keys: `sort`, `limit`, `skip`, `projection`.
/// Pass null/0 for options/options_len to use defaults (equivalent to `smongo_find`).
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_find_with_options(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    options: *const u8,
    options_len: usize,
    cursor_out: *mut *mut SmongoCursor,
) -> i32 {
    clear_last_error();

    if col.is_null() || cursor_out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let opts = if options.is_null() || options_len == 0 {
        FindOptions::default()
    } else {
        match deserialize_doc(options, options_len) {
            Ok(d) => parse_find_options_from_bson(&d),
            Err(code) => return code,
        }
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.find_with_options(filter_doc, opts) {
        Ok(docs) => {
            let cursor = Box::new(SmongoCursor {
                docs,
                position: 0,
                current_bytes: None,
            });
            unsafe { *cursor_out = Box::into_raw(cursor) };
            SMONGO_OK
        }
        Err(e) => map_col_error(e),
    }
}

/// Find one document with options (sort/projection).
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_find_one_with_options(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    options: *const u8,
    options_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let opts = if options.is_null() || options_len == 0 {
        FindOptions::default()
    } else {
        match deserialize_doc(options, options_len) {
            Ok(d) => parse_find_options_from_bson(&d),
            Err(code) => return code,
        }
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.find_one_with_options(filter_doc, opts) {
        Ok(Some(doc)) => match serialize_doc(&doc) {
            Ok(bytes) => {
                unsafe { write_bytes_out(bytes, result, result_len) };
                SMONGO_OK
            }
            Err(code) => code,
        },
        Ok(None) => {
            unsafe {
                *result = ptr::null_mut();
                *result_len = 0;
            }
            SMONGO_ERROR_NOT_FOUND
        }
        Err(e) => map_col_error(e),
    }
}

fn parse_find_options_from_bson(doc: &Document) -> FindOptions {
    let sort = doc.get_document("sort").ok().cloned();
    let limit = doc.get_i64("limit").ok().or_else(|| doc.get_i32("limit").ok().map(|n| n as i64));
    let skip = doc.get_i64("skip").ok().or_else(|| doc.get_i32("skip").ok().map(|n| n as i64));
    let projection = doc.get_document("projection").ok().cloned();
    FindOptions { sort, limit, skip, projection }
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

/// Update a single document matching the filter.
///
/// On success, writes a BSON result document containing `matchedCount` and
/// `modifiedCount` into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_update_one(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    update: *const u8,
    update_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };
    let update_doc = match deserialize_doc(update, update_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.update_one(filter_doc, update_doc) {
        Ok(res) => {
            let result_doc = bson::doc! {
                "matchedCount": res.matched_count as i64,
                "modifiedCount": res.modified_count as i64,
            };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Update a single document with options (e.g. upsert).
///
/// `options` is a BSON document with optional keys: `upsert` (bool).
/// Pass null/0 for options/options_len to use defaults.
///
/// Result includes `matchedCount`, `modifiedCount`, and optionally `upsertedId`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_update_one_with_options(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    update: *const u8,
    update_len: usize,
    options: *const u8,
    options_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };
    let update_doc = match deserialize_doc(update, update_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let opts = if options.is_null() || options_len == 0 {
        UpdateOptions::default()
    } else {
        match deserialize_doc(options, options_len) {
            Ok(d) => UpdateOptions {
                upsert: d.get_bool("upsert").unwrap_or(false),
                ..Default::default()
            },
            Err(code) => return code,
        }
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.update_one_with_options(filter_doc, update_doc, opts) {
        Ok(res) => {
            let mut result_doc = bson::doc! {
                "matchedCount": res.matched_count as i64,
                "modifiedCount": res.modified_count as i64,
            };
            if let Some(id) = res.upserted_id {
                result_doc.insert("upsertedId", id);
            }
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

/// Delete a single document matching the filter.
///
/// On success, writes a BSON result document containing `deletedCount`
/// into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_delete_one(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.delete_one(filter_doc) {
        Ok(res) => {
            let result_doc = bson::doc! {
                "deletedCount": res.deleted_count as i64,
            };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Update all documents matching the filter.
///
/// On success, writes a BSON result document containing `matchedCount` and
/// `modifiedCount` into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_update_many(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    update: *const u8,
    update_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };
    let update_doc = match deserialize_doc(update, update_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.update_many(filter_doc, update_doc) {
        Ok(res) => {
            let mut result_doc = bson::doc! {
                "matchedCount": res.matched_count as i64,
                "modifiedCount": res.modified_count as i64,
            };
            if let Some(id) = res.upserted_id {
                result_doc.insert("upsertedId", id);
            }
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Delete all documents matching the filter.
///
/// On success, writes a BSON result document containing `deletedCount`
/// into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_delete_many(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.delete_many(filter_doc) {
        Ok(res) => {
            let result_doc = bson::doc! {
                "deletedCount": res.deleted_count as i64,
            };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

// ---------------------------------------------------------------------------
// Count
// ---------------------------------------------------------------------------

/// Count documents matching the BSON filter.
///
/// Pass null for `filter` / 0 for `filter_len` to count all documents.
///
/// # Safety
/// `col` and `count_out` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn smongo_count(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    count_out: *mut i64,
) -> i32 {
    clear_last_error();

    if col.is_null() || count_out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_opt = if filter.is_null() || filter_len == 0 {
        None
    } else {
        match deserialize_doc(filter, filter_len) {
            Ok(d) => Some(d),
            Err(code) => return code,
        }
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.count_documents(filter_opt) {
        Ok(count) => {
            unsafe { *count_out = count as i64 };
            SMONGO_OK
        }
        Err(e) => map_col_error(e),
    }
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

/// Advance the cursor and get the next document's BSON bytes.
///
/// Returns `SMONGO_OK` while documents remain.  When the cursor is
/// exhausted, returns `SMONGO_ERROR_NOT_FOUND` with `*doc` set to null.
///
/// The returned pointer is valid until the next call to `smongo_cursor_next`
/// or `smongo_cursor_free` on the same cursor.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_cursor_next(
    cursor: *mut SmongoCursor,
    doc: *mut *const u8,
    doc_len: *mut usize,
) -> i32 {
    clear_last_error();

    if cursor.is_null() || doc.is_null() || doc_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let cursor_ref = unsafe { &mut *cursor };

    if cursor_ref.position >= cursor_ref.docs.len() {
        unsafe {
            *doc = ptr::null();
            *doc_len = 0;
        }
        return SMONGO_ERROR_NOT_FOUND;
    }

    match serialize_doc(&cursor_ref.docs[cursor_ref.position]) {
        Ok(bytes) => {
            cursor_ref.position += 1;
            let len = bytes.len();
            cursor_ref.current_bytes = Some(bytes);
            let buf = cursor_ref.current_bytes.as_ref().unwrap_or_else(|| {
                // SAFETY: we just assigned Some above
                unreachable!()
            });
            unsafe {
                *doc = buf.as_ptr();
                *doc_len = len;
            }
            SMONGO_OK
        }
        Err(code) => code,
    }
}

/// Free a cursor and all its resources.
///
/// # Safety
/// `cursor` must be a handle previously returned by `smongo_find` or
/// `smongo_aggregate`, or null (no-op).
#[no_mangle]
pub unsafe extern "C" fn smongo_cursor_free(cursor: *mut SmongoCursor) {
    clear_last_error();
    if !cursor.is_null() {
        drop(unsafe { Box::from_raw(cursor) });
    }
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

/// Create an index on the collection.
///
/// `keys` is a BSON document mapping field names to direction (1 or -1).
/// `name` may be null to auto-generate the index name.
///
/// On success, writes a BSON result document containing `indexName` into
/// `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_create_index(
    col: *mut SmongoCollection,
    keys: *const u8,
    keys_len: usize,
    name: *const c_char,
    unique: i32,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let keys_doc = match deserialize_doc(keys, keys_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let index_name = if name.is_null() {
        None
    } else {
        match cstr_to_str(name) {
            Ok(s) => Some(s.to_string()),
            Err(code) => return code,
        }
    };

    let options = IndexOptions {
        name: index_name,
        unique: unique != 0,
        ..Default::default()
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.create_index(keys_doc, Some(options)) {
        Ok(index_name) => {
            let result_doc = bson::doc! { "indexName": index_name };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Drop an index by name.
///
/// # Safety
/// `col` must be a valid collection handle. `name` must be a valid C string.
#[no_mangle]
pub unsafe extern "C" fn smongo_drop_index(
    col: *mut SmongoCollection,
    name: *const c_char,
) -> i32 {
    clear_last_error();

    if col.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name_str = match cstr_to_str(name) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.drop_index(name_str) {
        Ok(()) => SMONGO_OK,
        Err(e) => map_col_error(e),
    }
}

/// List all indexes on the collection.
///
/// On success, writes a BSON document containing an `indexes` array into
/// `*result` / `*result_len`. Each element is a document with `name`, `keys`,
/// and `options` fields.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_list_indexes(
    col: *mut SmongoCollection,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let col_ref = unsafe { &*col };
    match col_ref.inner.list_indexes() {
        Ok(indexes) => {
            let arr: Vec<bson::Bson> = indexes
                .iter()
                .map(|idx| {
                    let mut opts = bson::doc! {
                        "unique": idx.options.unique,
                        "sparse": idx.options.sparse,
                    };
                    if let Some(ttl) = idx.options.expire_after_seconds {
                        opts.insert("expireAfterSeconds", ttl as i64);
                    }
                    bson::Bson::Document(bson::doc! {
                        "name": &idx.name,
                        "keys": idx.keys.clone(),
                        "options": opts,
                    })
                })
                .collect();
            let result_doc = bson::doc! { "indexes": arr };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Rebuild all indexes on the collection.
///
/// On success, writes the number of index entries rebuilt into `*count_out`.
///
/// # Safety
/// `col` and `count_out` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn smongo_rebuild_all_indexes(
    col: *mut SmongoCollection,
    count_out: *mut i64,
) -> i32 {
    clear_last_error();

    if col.is_null() || count_out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let col_ref = unsafe { &*col };
    match col_ref.inner.rebuild_all_indexes() {
        Ok(count) => {
            unsafe { *count_out = count };
            SMONGO_OK
        }
        Err(e) => map_col_error(e),
    }
}

// ---------------------------------------------------------------------------
// Explain (find)
// ---------------------------------------------------------------------------

/// Explain the execution plan for a find query.
///
/// On success, writes a BSON explain document into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_explain_find(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.explain_find(filter_doc) {
        Ok(explain) => {
            let result_doc = explain_to_bson(&explain);
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Explain the execution plan for a find-one query.
///
/// On success, writes a BSON explain document into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_explain_find_one(
    col: *mut SmongoCollection,
    filter: *const u8,
    filter_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.explain_find_one(filter_doc) {
        Ok(explain) => {
            let result_doc = explain_to_bson(&explain);
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

fn explain_to_bson(explain: &smongo_engine::explain::ExplainResult) -> Document {
    bson::doc! {
        "executionPlan": format!("{:?}", explain.execution_plan),
        "indexUsed": explain.index_used.clone().unwrap_or_default(),
        "planReason": &explain.plan_reason,
        "documentsExamined": explain.execution_stats.documents_examined as i64,
        "documentsReturned": explain.execution_stats.documents_returned as i64,
        "indexEntriesExamined": explain.execution_stats.index_entries_examined as i64,
        "summary": explain.summary(),
    }
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

/// Execute an aggregation pipeline.
///
/// The pipeline is passed as a BSON document whose top-level keys are the
/// stringified array indices ("0", "1", "2", ...), each mapping to a stage
/// document.  This is the standard BSON array-as-document representation.
///
/// On success, writes a cursor handle into `*cursor_out`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_aggregate(
    col: *mut SmongoCollection,
    pipeline: *const u8,
    pipeline_len: usize,
    cursor_out: *mut *mut SmongoCursor,
) -> i32 {
    clear_last_error();

    if col.is_null() || cursor_out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let pipeline_doc = match deserialize_doc(pipeline, pipeline_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let pipeline_vec = match parse_bson_array_doc(&pipeline_doc) {
        Ok(v) => v,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.aggregate(pipeline_vec) {
        Ok(docs) => {
            let cursor = Box::new(SmongoCursor {
                docs,
                position: 0,
                current_bytes: None,
            });
            unsafe { *cursor_out = Box::into_raw(cursor) };
            SMONGO_OK
        }
        Err(e) => map_col_error(e),
    }
}

// ---------------------------------------------------------------------------
// Explain aggregation
// ---------------------------------------------------------------------------

/// Explain how an aggregation pipeline's initial data fetch would execute.
///
/// Runs the pipeline optimizer, extracts leading `$match` stages, and returns
/// a BSON document with the execution plan, index used, and statistics.
///
/// On success, writes the explain BSON into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_explain_aggregate(
    col: *mut SmongoCollection,
    pipeline: *const u8,
    pipeline_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let pipeline_doc = match deserialize_doc(pipeline, pipeline_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let pipeline_vec = match parse_bson_array_doc(&pipeline_doc) {
        Ok(v) => v,
        Err(code) => return code,
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.explain_aggregate(pipeline_vec) {
        Ok(explain) => {
            let result_doc = explain_to_bson(&explain);
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

// ---------------------------------------------------------------------------
// Session-based transactions
// ---------------------------------------------------------------------------

/// Start a new session for multi-collection transactions.
///
/// On success, writes a heap-allocated `SmongoSession` handle into `*out`.
/// Free with `smongo_session_free`.
///
/// # Safety
/// `db` must be a valid `SmongoDb` handle. `out` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn smongo_start_session(
    db: *mut SmongoDb,
    out: *mut *mut SmongoSession,
) -> i32 {
    clear_last_error();

    if db.is_null() || out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let db_ref = unsafe { &*db };
    match db_ref.inner.start_session() {
        Ok(session) => {
            let handle = Box::new(SmongoSession { inner: session });
            unsafe { *out = Box::into_raw(handle) };
            SMONGO_OK
        }
        Err(e) => map_db_error(e),
    }
}

/// Begin a transaction on the session.
///
/// # Safety
/// `session` must be a valid `SmongoSession` handle.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_begin_transaction(
    session: *mut SmongoSession,
) -> i32 {
    clear_last_error();

    if session.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let session_ref = unsafe { &*session };
    match session_ref.inner.begin_transaction() {
        Ok(()) => SMONGO_OK,
        Err(e) => map_db_error(e),
    }
}

/// Commit the current transaction on the session.
///
/// # Safety
/// `session` must be a valid `SmongoSession` handle.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_commit_transaction(
    session: *mut SmongoSession,
) -> i32 {
    clear_last_error();

    if session.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let session_ref = unsafe { &*session };
    match session_ref.inner.commit_transaction() {
        Ok(()) => SMONGO_OK,
        Err(e) => map_db_error(e),
    }
}

/// Rollback the current transaction on the session.
///
/// # Safety
/// `session` must be a valid `SmongoSession` handle.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_rollback_transaction(
    session: *mut SmongoSession,
) -> i32 {
    clear_last_error();

    if session.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let session_ref = unsafe { &*session };
    match session_ref.inner.rollback_transaction() {
        Ok(()) => SMONGO_OK,
        Err(e) => map_db_error(e),
    }
}

/// Insert a document into a collection within this session's transaction.
///
/// On success, writes the BSON result (`insertedId`) into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_insert_one(
    session: *mut SmongoSession,
    coll_name: *const c_char,
    doc: *const u8,
    doc_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if session.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name = match cstr_to_str(coll_name) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let document = match deserialize_doc(doc, doc_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let session_ref = unsafe { &*session };
    let col = match session_ref.inner.collection(name) {
        Ok(c) => c,
        Err(e) => return map_db_error(e),
    };

    match col.insert_one(document) {
        Ok(res) => {
            let result_doc = bson::doc! { "insertedId": res.inserted_id };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Find all documents in a collection within this session's transaction.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_find(
    session: *mut SmongoSession,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    cursor_out: *mut *mut SmongoCursor,
) -> i32 {
    clear_last_error();

    if session.is_null() || cursor_out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name = match cstr_to_str(coll_name) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let session_ref = unsafe { &*session };
    let col = match session_ref.inner.collection(name) {
        Ok(c) => c,
        Err(e) => return map_db_error(e),
    };

    match col.find(filter_doc) {
        Ok(docs) => {
            let cursor = Box::new(SmongoCursor {
                docs,
                position: 0,
                current_bytes: None,
            });
            unsafe { *cursor_out = Box::into_raw(cursor) };
            SMONGO_OK
        }
        Err(e) => map_col_error(e),
    }
}

/// Find one document in a collection within this session's transaction.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_find_one(
    session: *mut SmongoSession,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if session.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name = match cstr_to_str(coll_name) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let session_ref = unsafe { &*session };
    let col = match session_ref.inner.collection(name) {
        Ok(c) => c,
        Err(e) => return map_db_error(e),
    };

    match col.find_one(filter_doc) {
        Ok(Some(doc)) => match serialize_doc(&doc) {
            Ok(bytes) => {
                unsafe { write_bytes_out(bytes, result, result_len) };
                SMONGO_OK
            }
            Err(code) => code,
        },
        Ok(None) => {
            unsafe {
                *result = ptr::null_mut();
                *result_len = 0;
            }
            SMONGO_ERROR_NOT_FOUND
        }
        Err(e) => map_col_error(e),
    }
}

/// Delete a single document in a collection within this session's transaction.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_delete_one(
    session: *mut SmongoSession,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if session.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name = match cstr_to_str(coll_name) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let session_ref = unsafe { &*session };
    let col = match session_ref.inner.collection(name) {
        Ok(c) => c,
        Err(e) => return map_db_error(e),
    };

    match col.delete_one(filter_doc) {
        Ok(res) => {
            let result_doc = bson::doc! { "deletedCount": res.deleted_count as i64 };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Update a single document in a collection within this session's transaction.
///
/// On success, writes a BSON result document containing `matchedCount` and
/// `modifiedCount` into `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_update_one(
    session: *mut SmongoSession,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    update: *const u8,
    update_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if session.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name = match cstr_to_str(coll_name) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };
    let update_doc = match deserialize_doc(update, update_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let session_ref = unsafe { &*session };
    let col = match session_ref.inner.collection(name) {
        Ok(c) => c,
        Err(e) => return map_db_error(e),
    };

    match col.update_one(filter_doc, update_doc) {
        Ok(res) => {
            let result_doc = bson::doc! {
                "matchedCount": res.matched_count as i64,
                "modifiedCount": res.modified_count as i64,
            };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Update all documents matching the filter within this session's transaction.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_update_many(
    session: *mut SmongoSession,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    update: *const u8,
    update_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if session.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name = match cstr_to_str(coll_name) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };
    let update_doc = match deserialize_doc(update, update_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let session_ref = unsafe { &*session };
    let col = match session_ref.inner.collection(name) {
        Ok(c) => c,
        Err(e) => return map_db_error(e),
    };

    match col.update_many(filter_doc, update_doc) {
        Ok(res) => {
            let result_doc = bson::doc! {
                "matchedCount": res.matched_count as i64,
                "modifiedCount": res.modified_count as i64,
            };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Delete all documents matching the filter within this session's transaction.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_delete_many(
    session: *mut SmongoSession,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if session.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name = match cstr_to_str(coll_name) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let filter_doc = match deserialize_doc(filter, filter_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let session_ref = unsafe { &*session };
    let col = match session_ref.inner.collection(name) {
        Ok(c) => c,
        Err(e) => return map_db_error(e),
    };

    match col.delete_many(filter_doc) {
        Ok(res) => {
            let result_doc = bson::doc! { "deletedCount": res.deleted_count as i64 };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

/// Run an aggregation pipeline within this session's transaction.
///
/// Pipeline format is the same as `smongo_aggregate` — a BSON document with
/// keys "0", "1", ... mapping to stage documents.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_aggregate(
    session: *mut SmongoSession,
    coll_name: *const c_char,
    pipeline: *const u8,
    pipeline_len: usize,
    cursor_out: *mut *mut SmongoCursor,
) -> i32 {
    clear_last_error();

    if session.is_null() || cursor_out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name = match cstr_to_str(coll_name) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let pipeline_doc = match deserialize_doc(pipeline, pipeline_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let pipeline_vec = match parse_bson_array_doc(&pipeline_doc) {
        Ok(v) => v,
        Err(code) => return code,
    };

    let session_ref = unsafe { &*session };
    let col = match session_ref.inner.collection(name) {
        Ok(c) => c,
        Err(e) => return map_db_error(e),
    };

    match col.aggregate(pipeline_vec) {
        Ok(docs) => {
            let cursor = Box::new(SmongoCursor {
                docs,
                position: 0,
                current_bytes: None,
            });
            unsafe { *cursor_out = Box::into_raw(cursor) };
            SMONGO_OK
        }
        Err(e) => map_col_error(e),
    }
}

/// Count documents in a collection within this session's transaction.
///
/// Pass null for `filter` / 0 for `filter_len` to count all documents.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_session_count(
    session: *mut SmongoSession,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    count_out: *mut i64,
) -> i32 {
    clear_last_error();

    if session.is_null() || count_out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let name = match cstr_to_str(coll_name) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let filter_opt = if filter.is_null() || filter_len == 0 {
        None
    } else {
        match deserialize_doc(filter, filter_len) {
            Ok(d) => Some(d),
            Err(code) => return code,
        }
    };

    let session_ref = unsafe { &*session };
    let col = match session_ref.inner.collection(name) {
        Ok(c) => c,
        Err(e) => return map_db_error(e),
    };

    match col.count_documents(filter_opt) {
        Ok(count) => {
            unsafe { *count_out = count as i64 };
            SMONGO_OK
        }
        Err(e) => map_col_error(e),
    }
}

/// Free a session handle.
///
/// # Safety
/// `session` must be a handle previously returned by `smongo_start_session`,
/// or null (no-op).
#[no_mangle]
pub unsafe extern "C" fn smongo_session_free(session: *mut SmongoSession) {
    clear_last_error();
    if !session.is_null() {
        drop(unsafe { Box::from_raw(session) });
    }
}

// ---------------------------------------------------------------------------
// TTL reaping
// ---------------------------------------------------------------------------

/// Reap expired documents from all TTL-indexed collections in the database.
///
/// On success, writes the number of removed documents into `*count_out`.
///
/// # Safety
/// `db` and `count_out` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn smongo_reap_ttl(
    db: *mut SmongoDb,
    count_out: *mut i64,
) -> i32 {
    clear_last_error();

    if db.is_null() || count_out.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let db_ref = unsafe { &*db };
    match db_ref.inner.reap_ttl() {
        Ok(count) => {
            unsafe { *count_out = count as i64 };
            SMONGO_OK
        }
        Err(e) => map_db_error(e),
    }
}

/// Create an index with TTL (expire_after_seconds) support.
///
/// Pass `expire_after_seconds < 0` to create a non-TTL index.
///
/// On success, writes a BSON result document containing `indexName` into
/// `*result` / `*result_len`.
///
/// # Safety
/// All pointer arguments must be valid.
#[no_mangle]
pub unsafe extern "C" fn smongo_create_index_with_ttl(
    col: *mut SmongoCollection,
    keys: *const u8,
    keys_len: usize,
    unique: i32,
    expire_after_seconds: i64,
    result: *mut *mut u8,
    result_len: *mut usize,
) -> i32 {
    clear_last_error();

    if col.is_null() || result.is_null() || result_len.is_null() {
        set_last_error("null pointer argument");
        return SMONGO_ERROR_NULL_PTR;
    }

    let keys_doc = match deserialize_doc(keys, keys_len) {
        Ok(d) => d,
        Err(code) => return code,
    };

    let ttl = if expire_after_seconds >= 0 {
        Some(expire_after_seconds as u64)
    } else {
        None
    };

    let options = IndexOptions {
        unique: unique != 0,
        expire_after_seconds: ttl,
        ..Default::default()
    };

    let col_ref = unsafe { &*col };
    match col_ref.inner.create_index(keys_doc, Some(options)) {
        Ok(index_name) => {
            let result_doc = bson::doc! { "indexName": index_name };
            match serialize_doc(&result_doc) {
                Ok(bytes) => {
                    unsafe { write_bytes_out(bytes, result, result_len) };
                    SMONGO_OK
                }
                Err(code) => code,
            }
        }
        Err(e) => map_col_error(e),
    }
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

/// Get the last error message for the current thread.
///
/// Returns a NUL-terminated string pointer that is valid until the next
/// smongo API call on the same thread.  Returns null if no error.
///
/// # Safety
/// This function is always safe to call.
#[no_mangle]
pub extern "C" fn smongo_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| {
        let borrow = cell.borrow();
        match borrow.as_ref() {
            Some(cstr) => cstr.as_ptr(),
            None => ptr::null(),
        }
    })
}

// ---------------------------------------------------------------------------
// Memory management
// ---------------------------------------------------------------------------

/// Free a buffer previously returned by a smongo function.
///
/// # Safety
/// * `ptr` must have been returned by a smongo function (e.g.
///   `smongo_insert_one`, `smongo_find_one`, etc.), or be null (no-op).
/// * `len` must match the `*result_len` value that was written alongside
///   the pointer.
/// * The buffer must not have been freed already.
#[no_mangle]
pub unsafe extern "C" fn smongo_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        drop(unsafe { Vec::from_raw_parts(ptr, len, len) });
    }
}
