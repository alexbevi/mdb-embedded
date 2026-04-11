use bson::Document;
use std::marker::PhantomData;

use super::{
    apply_projection_to_index_doc, deserialize_document, Collection, CollectionError,
    CollectionResult,
};
use crate::query::eval_query;
use crate::storage::{StorageCursor, StorageSession};

pub(super) enum FindCursorState<C: StorageCursor> {
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
    #[allow(dead_code)]
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
    pub(super) state: FindCursorState<C>,
    pub(super) filter: Document,
    pub(super) _lifetime: PhantomData<&'a ()>,
}

impl<'a, C: StorageCursor> Iterator for FindCursor<'a, C> {
    type Item = CollectionResult<Document>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        find_cursor_next(&mut self.state, &self.filter)
    }
}

/// Owned streaming iterator over query results.
///
/// Unlike [`FindCursor`] (which borrows its parent `Collection` via
/// `PhantomData`), this type **consumes** the `Collection` and holds the
/// cursor state directly -- no lifetime parameters, no `Box<dyn>`, no
/// type erasure.  The `Collection` is kept alive so the storage session
/// (which the cursors reference via `Arc`) remains valid.
///
/// This makes it safe to hand to FFI layers (e.g. PyO3 `#[pyclass]`) that
/// require owned, `Send`, `'static` types.
///
/// Created by [`Collection::find_into_iter`].
pub struct OwnedFindIter<S: StorageSession> {
    /// Keeps the session alive for the cursors inside `state`.
    pub(super) _collection: Collection<S>,
    pub(super) state: FindCursorState<S::Cursor>,
    pub(super) filter: Document,
}

impl<S: StorageSession> Iterator for OwnedFindIter<S> {
    type Item = CollectionResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        find_cursor_next(&mut self.state, &self.filter)
    }
}

/// Shared iteration logic for both [`FindCursor`] and [`OwnedFindIter`].
fn find_cursor_next<C: StorageCursor>(
    state: &mut FindCursorState<C>,
    filter: &Document,
) -> Option<CollectionResult<Document>> {
    loop {
        match state {
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
                match eval_query(&doc, filter) {
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
                match eval_query(&doc, filter) {
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
                match eval_query(&doc, filter) {
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
                if let Some(mut doc) = crate::index::decode_index_key(&key_raw, index_keys) {
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
