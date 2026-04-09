//! Implement `smongo_engine::storage::{StorageSession, StorageCursor}` for WiredTiger sessions.
//! This lets `smongo_engine::collection::Collection<WtSession>` share the same WT tables as
//! `RustLocalCollection` (data `key_format=S,value_format=u`, index `u`/`S`, metadata `S`/`u`).

use smongo_engine::storage::{StorageCursor, StorageError, StorageResult, StorageSession};

use crate::wt_safe::{WtCursor, WtError, WtSession};

fn map_wt_err(e: WtError) -> StorageError {
    if e.is_not_found() {
        StorageError::NotFound(e.message)
    } else {
        StorageError::Other(e.to_string())
    }
}

fn map_wt<T>(r: Result<T, WtError>) -> StorageResult<T> {
    r.map_err(map_wt_err)
}

/// WiredTiger `session.create` config derived from the table URI/name.
fn wt_create_config(name: &str) -> &'static str {
    if name.contains(".indexes_metadata") {
        "key_format=S,value_format=u"
    } else if name.contains(".idx_") {
        "key_format=u,value_format=S"
    } else {
        "key_format=S,value_format=u"
    }
}

impl StorageSession for WtSession {
    type Cursor = WtCursor;

    fn create_table(&self, name: &str) -> StorageResult<()> {
        let config = wt_create_config(name);
        match self.create(name, config) {
            Ok(()) => Ok(()),
            Err(e) => {
                let m = e.message.to_lowercase();
                if m.contains("already exists")
                    || m.contains("file exists")
                    || m.contains("eexist")
                    || m.contains("object exists")
                {
                    Ok(())
                } else {
                    Err(StorageError::Other(e.to_string()))
                }
            }
        }
    }

    fn drop_table(&self, name: &str) -> StorageResult<()> {
        map_wt(self.drop_table(name, Some("force")))
    }

    fn open_cursor(&self, table_name: &str) -> StorageResult<Self::Cursor> {
        map_wt(self.open_cursor(table_name, None))
    }

    fn begin_transaction(&self) -> StorageResult<()> {
        map_wt(self.begin_transaction(None))
    }

    fn commit_transaction(&self) -> StorageResult<()> {
        map_wt(self.commit_transaction(None))
    }

    fn rollback_transaction(&self) -> StorageResult<()> {
        map_wt(self.rollback_transaction(None))
    }

    fn open_sibling_session(&self) -> StorageResult<Self> {
        map_wt(self.open_sibling())
    }
}

impl StorageCursor for WtCursor {
    fn set_key_str(&mut self, key: &str) {
        WtCursor::set_key_str(self, key);
    }

    fn get_key_str(&self) -> StorageResult<String> {
        map_wt(WtCursor::get_key_str(self))
    }

    fn set_key_raw(&mut self, data: &[u8]) {
        WtCursor::set_key_raw(self, data);
    }

    fn get_key_raw(&self) -> StorageResult<Vec<u8>> {
        map_wt(WtCursor::get_key_raw(self))
    }

    fn set_value_str(&mut self, value: &str) {
        WtCursor::set_value_str(self, value);
    }

    fn get_value_str(&self) -> StorageResult<String> {
        map_wt(WtCursor::get_value_str(self))
    }

    fn set_value_raw(&mut self, data: &[u8]) {
        WtCursor::set_value_raw(self, data);
    }

    fn get_value_raw(&self) -> StorageResult<Vec<u8>> {
        map_wt(WtCursor::get_value_raw(self))
    }

    fn search(&mut self) -> StorageResult<()> {
        map_wt(WtCursor::search(self))
    }

    fn search_near(&mut self) -> StorageResult<i32> {
        map_wt(WtCursor::search_near(self))
    }

    fn next(&mut self) -> StorageResult<()> {
        map_wt(WtCursor::next(self))
    }

    fn insert(&mut self) -> StorageResult<()> {
        map_wt(WtCursor::insert(self))
    }

    fn update(&mut self) -> StorageResult<()> {
        map_wt(WtCursor::update(self))
    }

    fn remove(&mut self) -> StorageResult<()> {
        match WtCursor::remove(self) {
            Ok(()) => Ok(()),
            Err(e) if e.is_not_found() => Ok(()),
            Err(e) => Err(map_wt_err(e)),
        }
    }

    fn reset(&mut self) -> StorageResult<()> {
        map_wt(WtCursor::reset(self))
    }
}
