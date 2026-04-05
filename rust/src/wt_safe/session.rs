//! Safe wrapper around `WT_SESSION` -- table creation, cursor management, and transactions.
use std::ptr;

use wiredtiger_sys::{WT_CURSOR, WT_SESSION};

use super::{check, cstr, optional_cstr, WtCursor, WtError, WtResult};

pub struct WtSession {
    raw: *mut WT_SESSION,
}

// SAFETY: WtSession wraps a WT_SESSION pointer.  WiredTiger sessions are not
// thread-safe, but Send is required so sessions can be moved between threads
// (e.g. from a spawning thread to a worker).  We intentionally do NOT impl Sync
// -- callers must ensure single-threaded access.
unsafe impl Send for WtSession {}

impl WtSession {
    /// Wrap an already-opened raw session pointer.
    /// # Safety
    /// The pointer must be a valid, open WT_SESSION.
    pub unsafe fn from_raw(raw: *mut WT_SESSION) -> Self {
        Self { raw }
    }

    pub fn raw_ptr(&self) -> *mut WT_SESSION {
        self.raw
    }

    pub fn create(&self, name: &str, config: &str) -> WtResult<()> {
        let c_name = cstr(name)?;
        let c_config = cstr(config)?;
        let create_fn = unsafe { (*self.raw).create }
            .ok_or_else(|| WtError { code: -1, message: "create vtable null".into() })?;
        // SAFETY: raw is a valid WT_SESSION; c_name and c_config are valid C strings.
        let rc = unsafe { create_fn(self.raw, c_name.as_ptr(), c_config.as_ptr()) };
        check(rc)
    }

    pub fn drop_table(&self, name: &str, config: Option<&str>) -> WtResult<()> {
        let c_name = cstr(name)?;
        let c_config = optional_cstr(config)?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let drop_fn = unsafe { (*self.raw).drop }
            .ok_or_else(|| WtError { code: -1, message: "drop vtable null".into() })?;
        // SAFETY: raw is a valid WT_SESSION; c_name/config_ptr are valid or null.
        let rc = unsafe { drop_fn(self.raw, c_name.as_ptr(), config_ptr) };
        check(rc)
    }

    pub fn open_cursor(&self, uri: &str, config: Option<&str>) -> WtResult<WtCursor> {
        let c_uri = cstr(uri)?;
        let c_config = optional_cstr(config)?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let open_cursor_fn = unsafe { (*self.raw).open_cursor }
            .ok_or_else(|| WtError { code: -1, message: "open_cursor vtable null".into() })?;
        let mut cursor: *mut WT_CURSOR = ptr::null_mut();
        // SAFETY: raw is a valid WT_SESSION; c_uri/config_ptr are valid or null.
        let rc = unsafe {
            open_cursor_fn(self.raw, c_uri.as_ptr(), ptr::null_mut(), config_ptr, &mut cursor)
        };
        check(rc)?;
        Ok(WtCursor { raw: cursor, _key_buf: None, _val_buf: None })
    }

    pub fn checkpoint(&self, config: Option<&str>) -> WtResult<()> {
        let c_config = optional_cstr(config)?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let cp_fn = unsafe { (*self.raw).checkpoint }
            .ok_or_else(|| WtError { code: -1, message: "checkpoint vtable null".into() })?;
        // SAFETY: raw is a valid WT_SESSION; config_ptr is valid or null.
        let rc = unsafe { cp_fn(self.raw, config_ptr) };
        check(rc)
    }

    pub fn begin_transaction(&self, config: Option<&str>) -> WtResult<()> {
        let c_config = optional_cstr(config)?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let begin_fn = unsafe { (*self.raw).begin_transaction }
            .ok_or_else(|| WtError { code: -1, message: "begin_transaction vtable null".into() })?;
        // SAFETY: raw is a valid WT_SESSION; config_ptr is valid or null.
        let rc = unsafe { begin_fn(self.raw, config_ptr) };
        check(rc)
    }

    pub fn commit_transaction(&self, config: Option<&str>) -> WtResult<()> {
        let c_config = optional_cstr(config)?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let commit_fn = unsafe { (*self.raw).commit_transaction }
            .ok_or_else(|| WtError { code: -1, message: "commit_transaction vtable null".into() })?;
        // SAFETY: raw is a valid WT_SESSION; config_ptr is valid or null.
        let rc = unsafe { commit_fn(self.raw, config_ptr) };
        check(rc)
    }

    pub fn rollback_transaction(&self, config: Option<&str>) -> WtResult<()> {
        let c_config = optional_cstr(config)?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let rollback_fn = unsafe { (*self.raw).rollback_transaction }
            .ok_or_else(|| WtError { code: -1, message: "rollback_transaction vtable null".into() })?;
        // SAFETY: raw is a valid WT_SESSION; config_ptr is valid or null.
        let rc = unsafe { rollback_fn(self.raw, config_ptr) };
        check(rc)
    }

    pub fn verify(&self, name: &str, config: Option<&str>) -> WtResult<()> {
        let c_name = cstr(name)?;
        let c_config = optional_cstr(config)?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let verify_fn = unsafe { (*self.raw).verify }
            .ok_or_else(|| WtError { code: -1, message: "verify vtable null".into() })?;
        // SAFETY: raw is a valid WT_SESSION; c_name/config_ptr are valid or null.
        let rc = unsafe { verify_fn(self.raw, c_name.as_ptr(), config_ptr) };
        check(rc)
    }

    pub fn compact(&self, name: &str, config: Option<&str>) -> WtResult<()> {
        let c_name = cstr(name)?;
        let c_config = optional_cstr(config)?;
        let config_ptr = c_config.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let compact_fn = unsafe { (*self.raw).compact }
            .ok_or_else(|| WtError { code: -1, message: "compact vtable null".into() })?;
        // SAFETY: raw is a valid WT_SESSION; c_name/config_ptr are valid or null.
        let rc = unsafe { compact_fn(self.raw, c_name.as_ptr(), config_ptr) };
        check(rc)
    }

    /// Close the session. Called automatically on drop.
    pub fn close(&mut self) -> WtResult<()> {
        if !self.raw.is_null() {
            let close_fn = unsafe { (*self.raw).close }
                .ok_or_else(|| WtError { code: -1, message: "session close vtable null".into() })?;
            // SAFETY: raw is a valid WT_SESSION; after close the pointer is invalidated.
            let rc = unsafe { close_fn(self.raw, ptr::null()) };
            self.raw = ptr::null_mut();
            check(rc)?;
        }
        Ok(())
    }
}

impl Drop for WtSession {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
