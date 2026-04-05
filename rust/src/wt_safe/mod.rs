//! Safe Rust wrappers around WiredTiger's raw C API.
//!
//! These types enforce correct resource management (RAII) and provide a
//! Rust-idiomatic interface while keeping the raw FFI details behind `unsafe`.
//!
//! These wrappers are used extensively by the PyO3 bridge layer (`wt_bridge`)
//! and the storage engine (`storage_engine`, `local_collection`, etc.).

mod connection;
mod session;
mod cursor;

pub use connection::WtConnection;
pub(crate) use connection::open_session_from_conn_ptr;
pub use cursor::WtCursor;
pub use session::WtSession;

use std::ffi::CString;
use std::os::raw::c_int;

use wiredtiger_sys::WT_SUCCESS;

#[cfg(test)]
use wiredtiger_sys::{WtLibrary, WT_NOTFOUND};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct WtError {
    pub code: c_int,
    pub message: String,
}

impl std::fmt::Display for WtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WiredTiger error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for WtError {}

impl WtError {
    /// Whether this error represents a WT_NOTFOUND condition.
    pub fn is_not_found(&self) -> bool {
        self.code == wiredtiger_sys::WT_NOTFOUND
    }
}

pub type WtResult<T> = Result<T, WtError>;

/// Convert a Rust `&str` to a `CString`, mapping interior NUL to `WtError`.
pub(crate) fn cstr(s: &str) -> WtResult<CString> {
    CString::new(s).map_err(|e| WtError {
        code: -1,
        message: format!("interior NUL in string: {e}"),
    })
}

/// Convert an `Option<&str>` to an `Option<CString>`.
pub(crate) fn optional_cstr(s: Option<&str>) -> WtResult<Option<CString>> {
    s.map(cstr).transpose()
}

pub(crate) fn check(rc: c_int) -> WtResult<()> {
    if rc == WT_SUCCESS {
        Ok(())
    } else {
        Err(WtError {
            code: rc,
            message: wiredtiger_sys::wt_strerror(rc).to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// Integration tests -- validates ABI correctness against the real WT binary
// ---------------------------------------------------------------------------
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::{Arc, OnceLock};

    static WT_LIB: OnceLock<Arc<WtLibrary>> = OnceLock::new();

    fn load_wt() -> Arc<WtLibrary> {
        WT_LIB
            .get_or_init(|| {
                Arc::new(WtLibrary::load_from_pip().expect("failed to load WiredTiger library"))
            })
            .clone()
    }

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    #[test]
    fn test_open_close_connection() {
        let lib = load_wt();
        let dir = temp_dir();
        let mut conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("failed to open connection");
        conn.close().expect("failed to close connection");
    }

    #[test]
    fn test_session_create_drop_table() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:test", "key_format=S,value_format=S")
            .expect("create table");
        session
            .drop_table("table:test", Some("force"))
            .expect("drop table");
    }

    #[test]
    fn test_cursor_open_close() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:oc", "key_format=S,value_format=S")
            .expect("create");
        let mut cursor = session.open_cursor("table:oc", None).expect("open_cursor");
        cursor.close().expect("close cursor");
    }

    #[test]
    fn test_cursor_insert_search_next() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:kv", "key_format=S,value_format=S")
            .expect("create");

        // Insert two records
        {
            let mut cursor = session.open_cursor("table:kv", None).expect("open_cursor");
            cursor.set_key_str("alpha");
            cursor.set_value_str("one");
            cursor.insert().expect("insert alpha");

            cursor.set_key_str("beta");
            cursor.set_value_str("two");
            cursor.insert().expect("insert beta");
        }

        // Search exact match
        {
            let mut cursor = session.open_cursor("table:kv", None).expect("open_cursor");
            cursor.set_key_str("alpha");
            cursor.search().expect("search alpha");
            let val = cursor.get_value_str().expect("get_value");
            assert_eq!(val, "one");
        }

        // Iterate with next()
        {
            let cursor = session.open_cursor("table:kv", None).expect("open_cursor");
            cursor.next().expect("next 1");
            let k1 = cursor.get_key_str().expect("key 1");
            assert_eq!(k1, "alpha");

            cursor.next().expect("next 2");
            let k2 = cursor.get_key_str().expect("key 2");
            assert_eq!(k2, "beta");

            // next() past end should return NOTFOUND
            let err = cursor.next().unwrap_err();
            assert_eq!(err.code, WT_NOTFOUND);
        }
    }

    #[test]
    fn test_cursor_search_near() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:sn", "key_format=S,value_format=S")
            .expect("create");

        let mut cursor = session.open_cursor("table:sn", None).expect("open_cursor");
        cursor.set_key_str("b");
        cursor.set_value_str("val_b");
        cursor.insert().expect("insert");

        cursor.set_key_str("d");
        cursor.set_value_str("val_d");
        cursor.insert().expect("insert");

        // search_near for "c" should land on "b" or "d"
        cursor.set_key_str("c");
        let exact = cursor.search_near().expect("search_near");
        assert_ne!(exact, 0); // not exact match
        let found_key = cursor.get_key_str().expect("key");
        assert!(found_key == "b" || found_key == "d");
    }

    #[test]
    fn test_cursor_update_remove() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:ur", "key_format=S,value_format=S")
            .expect("create");

        let mut cursor = session.open_cursor("table:ur", None).expect("open_cursor");

        // Insert
        cursor.set_key_str("key1");
        cursor.set_value_str("original");
        cursor.insert().expect("insert");

        // Update
        cursor.set_key_str("key1");
        cursor.set_value_str("updated");
        cursor.update().expect("update");

        // Verify update
        cursor.set_key_str("key1");
        cursor.search().expect("search");
        assert_eq!(cursor.get_value_str().expect("val"), "updated");

        // Remove
        cursor.set_key_str("key1");
        cursor.remove().expect("remove");

        // Verify removed
        cursor.set_key_str("key1");
        let err = cursor.search().unwrap_err();
        assert_eq!(err.code, WT_NOTFOUND);
    }

    #[test]
    fn test_cursor_prev() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:prev", "key_format=S,value_format=S")
            .expect("create");

        let mut cursor = session.open_cursor("table:prev", None).expect("open_cursor");
        for (k, v) in &[("a", "1"), ("b", "2"), ("c", "3")] {
            cursor.set_key_str(k);
            cursor.set_value_str(v);
            cursor.insert().expect("insert");
        }

        // prev() from end -> "c" -> "b" -> "a"
        cursor.reset().expect("reset");
        cursor.prev().expect("prev 1");
        assert_eq!(cursor.get_key_str().expect("key"), "c");
        cursor.prev().expect("prev 2");
        assert_eq!(cursor.get_key_str().expect("key"), "b");
        cursor.prev().expect("prev 3");
        assert_eq!(cursor.get_key_str().expect("key"), "a");
    }

    #[test]
    fn test_raw_bytes_value() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:raw", "key_format=S,value_format=u")
            .expect("create");

        let mut cursor = session.open_cursor("table:raw", None).expect("open_cursor");
        let data = b"\x00\x01\x02\xff\xfe\xfd";
        cursor.set_key_str("binkey");
        cursor.set_value_raw(data);
        cursor.insert().expect("insert");

        cursor.set_key_str("binkey");
        cursor.search().expect("search");
        let got = cursor.get_value_raw().expect("get_value_raw");
        assert_eq!(got, data);
    }

    #[test]
    fn test_transaction_commit() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:txn", "key_format=S,value_format=S")
            .expect("create");

        session.begin_transaction(None).expect("begin");
        let mut cursor = session.open_cursor("table:txn", None).expect("open_cursor");
        cursor.set_key_str("txn_key");
        cursor.set_value_str("txn_val");
        cursor.insert().expect("insert");
        drop(cursor);
        session.commit_transaction(None).expect("commit");

        // Verify the committed data is visible
        let mut cursor2 = session.open_cursor("table:txn", None).expect("open_cursor");
        cursor2.set_key_str("txn_key");
        cursor2.search().expect("search");
        assert_eq!(cursor2.get_value_str().expect("val"), "txn_val");
    }

    #[test]
    fn test_transaction_rollback() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:rb", "key_format=S,value_format=S")
            .expect("create");

        session.begin_transaction(None).expect("begin");
        let mut cursor = session.open_cursor("table:rb", None).expect("open_cursor");
        cursor.set_key_str("rb_key");
        cursor.set_value_str("rb_val");
        cursor.insert().expect("insert");
        drop(cursor);
        session.rollback_transaction(None).expect("rollback");

        // Data should NOT be visible after rollback
        let mut cursor2 = session.open_cursor("table:rb", None).expect("open_cursor");
        cursor2.set_key_str("rb_key");
        let err = cursor2.search().unwrap_err();
        assert_eq!(err.code, WT_NOTFOUND);
    }

    #[test]
    fn test_checkpoint() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:cp", "key_format=S,value_format=S")
            .expect("create");

        let mut cursor = session.open_cursor("table:cp", None).expect("open_cursor");
        cursor.set_key_str("cp_key");
        cursor.set_value_str("cp_val");
        cursor.insert().expect("insert");
        drop(cursor);

        session.checkpoint(None).expect("checkpoint");
    }

    #[test]
    fn test_verify_compact() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:vc", "key_format=S,value_format=S")
            .expect("create");

        session.verify("table:vc", None).expect("verify");
        session.compact("table:vc", None).expect("compact");
    }

    #[test]
    fn test_cursor_reset() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:rst", "key_format=S,value_format=S")
            .expect("create");

        let mut cursor = session.open_cursor("table:rst", None).expect("open_cursor");
        cursor.set_key_str("r1");
        cursor.set_value_str("v1");
        cursor.insert().expect("insert");
        cursor.set_key_str("r2");
        cursor.set_value_str("v2");
        cursor.insert().expect("insert");

        cursor.next().expect("next");
        assert_eq!(cursor.get_key_str().expect("key"), "r1");

        // After reset, next should return first record again
        cursor.reset().expect("reset");
        cursor.next().expect("next");
        assert_eq!(cursor.get_key_str().expect("key"), "r1");
    }

    #[test]
    fn test_metadata_cursor() {
        let lib = load_wt();
        let dir = temp_dir();
        let conn =
            WtConnection::open(lib, dir.path().to_str().unwrap(), Some("create"))
                .expect("open");
        let session = conn.open_session(None).expect("open_session");
        session
            .create("table:meta_test", "key_format=S,value_format=S")
            .expect("create");

        // Metadata cursor can enumerate tables
        let cursor = session
            .open_cursor("metadata:", None)
            .expect("open metadata cursor");
        let mut found = false;
        loop {
            match cursor.next() {
                Ok(()) => {
                    let key = cursor.get_key_str().expect("key");
                    if key.contains("meta_test") {
                        found = true;
                        break;
                    }
                }
                Err(e) if e.code == WT_NOTFOUND => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(found, "metadata should list table:meta_test");
    }

    #[test]
    fn loaded_version_matches_abi_constants() {
        let lib = load_wt();
        let v = lib.version();
        assert_eq!(
            v.major,
            wiredtiger_sys::ABI_MAJOR,
            "loaded WT major {v} != expected ABI_MAJOR={}",
            wiredtiger_sys::ABI_MAJOR,
        );
        assert_eq!(
            v.minor,
            wiredtiger_sys::ABI_MINOR,
            "loaded WT minor {v} != expected ABI_MINOR={}",
            wiredtiger_sys::ABI_MINOR,
        );
    }
}
