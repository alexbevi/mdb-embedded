//! Safe wrapper around `WT_CONNECTION` -- open, close, and session creation.
use std::ffi::CString;
use std::ptr;

use wiredtiger_sys::{WtLibrary, WT_CONNECTION, WT_SESSION};

use super::{check, WtError, WtResult, WtSession};

pub struct WtConnection {
    raw: *mut WT_CONNECTION,
    _lib: std::sync::Arc<WtLibrary>,
}

// SAFETY: WtConnection wraps a WT_CONNECTION pointer which, per WiredTiger's
// documentation, supports concurrent access from multiple threads with its own
// internal locking.  The Arc<WtLibrary> ensures the shared library stays loaded.
unsafe impl Send for WtConnection {}
unsafe impl Sync for WtConnection {}

impl WtConnection {
    /// Open (or create) a WiredTiger database.
    pub fn open(
        lib: std::sync::Arc<WtLibrary>,
        home: &str,
        config: Option<&str>,
    ) -> WtResult<Self> {
        let c_home = CString::new(home).map_err(|e| WtError {
            code: -1,
            message: format!("invalid home path: {e}"),
        })?;
        let c_config = config.map(CString::new).transpose().map_err(|e| WtError {
            code: -1,
            message: format!("invalid config: {e}"),
        })?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());

        let mut conn: *mut WT_CONNECTION = ptr::null_mut();
        // SAFETY: c_home and config_ptr point to valid null-terminated C strings
        // (or null for config_ptr).  wiredtiger_open writes a new connection
        // handle into `conn` on success.
        let rc =
            unsafe { lib.wiredtiger_open(c_home.as_ptr(), ptr::null_mut(), config_ptr, &mut conn) };
        check(rc)?;
        Ok(Self {
            raw: conn,
            _lib: lib,
        })
    }

    /// Open a new session on this connection.
    pub fn open_session(&self, config: Option<&str>) -> WtResult<WtSession> {
        let c_config = config.map(CString::new).transpose().map_err(|e| WtError {
            code: -1,
            message: format!("invalid config: {e}"),
        })?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());

        let mut sess: *mut WT_SESSION = ptr::null_mut();
        let open_session = unsafe { (*self.raw).open_session }.ok_or_else(|| WtError {
            code: -1,
            message: "open_session vtable null".into(),
        })?;
        // SAFETY: raw is a valid WT_CONNECTION, open_session is a valid vtable
        // entry, and config_ptr is either null or points to a valid C string.
        let rc = unsafe { open_session(self.raw, ptr::null_mut(), config_ptr, &mut sess) };
        check(rc)?;
        Ok(unsafe { WtSession::from_raw(sess) })
    }

    pub fn raw_ptr(&self) -> *mut WT_CONNECTION {
        self.raw
    }

    /// The version of the loaded WiredTiger library.
    pub fn wt_version(&self) -> wiredtiger_sys::WtVersion {
        self._lib.version()
    }

    /// Close the connection. Called automatically on drop.
    pub fn close(&mut self) -> WtResult<()> {
        if !self.raw.is_null() {
            let close_fn = unsafe { (*self.raw).close }.ok_or_else(|| WtError {
                code: -1,
                message: "close vtable null".into(),
            })?;
            // SAFETY: raw is a valid, non-null WT_CONNECTION and close_fn is a
            // valid vtable entry.  After this call the pointer is invalidated.
            let rc = unsafe { close_fn(self.raw, ptr::null()) };
            self.raw = ptr::null_mut();
            check(rc)?;
        }
        Ok(())
    }
}

impl Drop for WtConnection {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Open a `WtSession` from a raw `WT_CONNECTION` pointer without owning
/// the connection.  Used by components (`RustLocalCollection`,
/// `RustLocalDB`) that hold a borrowed connection pointer.
///
/// # Safety
/// `conn` must be a valid, open `WT_CONNECTION` pointer.
pub(crate) fn open_session_from_conn_ptr(conn: *mut WT_CONNECTION) -> WtResult<WtSession> {
    if conn.is_null() {
        return Err(WtError {
            code: -1,
            message: "null connection pointer".into(),
        });
    }
    // SAFETY: conn is checked non-null above; we dereference to read the vtable.
    let open_fn = unsafe { (*conn).open_session }.ok_or_else(|| WtError {
        code: -1,
        message: "open_session vtable null".into(),
    })?;
    let mut sess: *mut WT_SESSION = ptr::null_mut();
    // SAFETY: conn is a valid WT_CONNECTION and open_fn is a valid vtable entry.
    let rc = unsafe { open_fn(conn, ptr::null_mut(), ptr::null(), &mut sess) };
    check(rc)?;
    // SAFETY: open_session succeeded, so sess is a valid WT_SESSION pointer.
    Ok(unsafe { WtSession::from_raw(sess) })
}
