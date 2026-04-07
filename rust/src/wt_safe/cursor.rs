//! Safe wrapper around `WT_CURSOR` -- key/value access, iteration, and mutation.
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

use wiredtiger_sys::{
    wt_shim_get_key_str, wt_shim_get_value_raw, wt_shim_get_value_str, wt_shim_set_key_str,
    wt_shim_set_value_raw, wt_shim_set_value_str, WT_CURSOR, WT_DUPLICATE_KEY, WT_ITEM,
    WT_NOTFOUND,
};

use super::{check, WtError, WtResult};

pub struct WtCursor {
    pub(in crate::wt_safe) raw: *mut WT_CURSOR,
    pub(in crate::wt_safe) _key_buf: Option<CString>,
    pub(in crate::wt_safe) _val_buf: Option<CString>,
}

impl WtCursor {
    // -- key operations (key_format=S) --

    pub fn set_key_str(&mut self, key: &str) {
        let c_key = match CString::new(key) {
            Ok(c) => c,
            Err(_) => return, // interior NUL -- silently skip (matches prior SWIG behavior)
        };
        // SAFETY: raw is a valid WT_CURSOR; fn_ptr is the set_key vtable entry.
        // c_key is kept alive in _key_buf until the next set_key or cursor close.
        let fn_ptr = unsafe { (*self.raw).set_key };
        unsafe { wt_shim_set_key_str(fn_ptr, self.raw, c_key.as_ptr()) };
        self._key_buf = Some(c_key);
    }

    pub fn get_key_str(&self) -> WtResult<String> {
        let mut key_ptr: *const c_char = ptr::null();
        // SAFETY: raw is a valid positioned WT_CURSOR; fn_ptr is the get_key vtable entry.
        // key_ptr is written by WiredTiger and valid until the next cursor operation.
        let fn_ptr = unsafe { (*self.raw).get_key };
        let rc = unsafe { wt_shim_get_key_str(fn_ptr, self.raw, &mut key_ptr) };
        check(rc)?;
        // SAFETY: key_ptr was written by a successful get_key call.
        let key = unsafe { CStr::from_ptr(key_ptr) }
            .to_string_lossy()
            .into_owned();
        Ok(key)
    }

    // -- value operations (value_format=u, raw bytes) --

    /// Set the cursor value from raw bytes (value_format=u).
    ///
    /// # Contract
    /// The `data` slice must remain valid and unmodified until the next
    /// positioning or modify operation (insert/update/remove/search/next/prev).
    pub fn set_value_raw(&self, data: &[u8]) {
        let item = WT_ITEM {
            data: data.as_ptr() as *const c_void,
            size: data.len(),
            mem: ptr::null_mut(),
            memsize: 0,
            flags: 0,
        };
        // SAFETY: raw is a valid WT_CURSOR.  The WT_ITEM references `data` which
        // the caller guarantees is alive until the next cursor operation.
        let fn_ptr = unsafe { (*self.raw).set_value };
        unsafe { wt_shim_set_value_raw(fn_ptr, self.raw, &item) };
    }

    pub fn get_value_raw(&self) -> WtResult<Vec<u8>> {
        let mut item = WT_ITEM {
            data: ptr::null(),
            size: 0,
            mem: ptr::null_mut(),
            memsize: 0,
            flags: 0,
        };
        // SAFETY: raw is a valid positioned WT_CURSOR.  WiredTiger writes into
        // item.data/size; the pointer is valid until the next cursor operation.
        let fn_ptr = unsafe { (*self.raw).get_value };
        let rc = unsafe { wt_shim_get_value_raw(fn_ptr, self.raw, &mut item) };
        check(rc)?;
        // SAFETY: get_value succeeded so item.data/size describe a valid region.
        let bytes =
            unsafe { std::slice::from_raw_parts(item.data as *const u8, item.size) }.to_vec();
        Ok(bytes)
    }

    // -- value operations (value_format=S, string) --

    pub fn set_value_str(&mut self, value: &str) {
        let c_value = match CString::new(value) {
            Ok(c) => c,
            Err(_) => return, // interior NUL
        };
        // SAFETY: raw is a valid WT_CURSOR.  c_value is kept alive in _val_buf.
        let fn_ptr = unsafe { (*self.raw).set_value };
        unsafe { wt_shim_set_value_str(fn_ptr, self.raw, c_value.as_ptr()) };
        self._val_buf = Some(c_value);
    }

    pub fn get_value_str(&self) -> WtResult<String> {
        let mut val_ptr: *const c_char = ptr::null();
        // SAFETY: raw is a valid positioned WT_CURSOR.  val_ptr is written by
        // WiredTiger and valid until the next cursor operation.
        let fn_ptr = unsafe { (*self.raw).get_value };
        let rc = unsafe { wt_shim_get_value_str(fn_ptr, self.raw, &mut val_ptr) };
        check(rc)?;
        // SAFETY: val_ptr was written by a successful get_value call.
        let val = unsafe { CStr::from_ptr(val_ptr) }
            .to_string_lossy()
            .into_owned();
        Ok(val)
    }

    // -- cursor navigation --

    pub fn search(&self) -> WtResult<()> {
        let search_fn = unsafe { (*self.raw).search }.ok_or_else(|| WtError {
            code: -1,
            message: "search vtable null".into(),
        })?;
        // SAFETY: raw is a valid WT_CURSOR with a key set.
        let rc = unsafe { search_fn(self.raw) };
        check(rc)
    }

    /// Returns the `exact` value: 0 = exact match, <0 = positioned before, >0 = positioned after.
    pub fn search_near(&self) -> WtResult<c_int> {
        let search_near_fn = unsafe { (*self.raw).search_near }.ok_or_else(|| WtError {
            code: -1,
            message: "search_near vtable null".into(),
        })?;
        let mut exact: c_int = 0;
        // SAFETY: raw is a valid WT_CURSOR with a key set.
        let rc = unsafe { search_near_fn(self.raw, &mut exact) };
        check(rc)?;
        Ok(exact)
    }

    pub fn next(&self) -> WtResult<()> {
        let next_fn = unsafe { (*self.raw).next }.ok_or_else(|| WtError {
            code: -1,
            message: "next vtable null".into(),
        })?;
        // SAFETY: raw is a valid WT_CURSOR.
        let rc = unsafe { next_fn(self.raw) };
        check(rc)
    }

    pub fn prev(&self) -> WtResult<()> {
        let prev_fn = unsafe { (*self.raw).prev }.ok_or_else(|| WtError {
            code: -1,
            message: "prev vtable null".into(),
        })?;
        // SAFETY: raw is a valid WT_CURSOR.
        let rc = unsafe { prev_fn(self.raw) };
        check(rc)
    }

    pub fn raw_ptr(&self) -> *mut WT_CURSOR {
        self.raw
    }

    /// Return the raw WT error code from the last next() call.
    /// Useful for checking WT_NOTFOUND without converting to WtError.
    pub fn next_raw(&self) -> c_int {
        // SAFETY: raw is a valid WT_CURSOR.  We return the raw rc for the
        // caller to interpret (commonly checking for WT_NOTFOUND).
        match unsafe { (*self.raw).next } {
            Some(next_fn) => unsafe { next_fn(self.raw) },
            None => -1,
        }
    }

    pub fn insert(&self) -> WtResult<()> {
        let insert_fn = unsafe { (*self.raw).insert }.ok_or_else(|| WtError {
            code: -1,
            message: "insert vtable null".into(),
        })?;
        // SAFETY: raw is a valid WT_CURSOR with key/value set.
        let rc = unsafe { insert_fn(self.raw) };
        check(rc)
    }

    pub fn update(&self) -> WtResult<()> {
        let update_fn = unsafe { (*self.raw).update }.ok_or_else(|| WtError {
            code: -1,
            message: "update vtable null".into(),
        })?;
        // SAFETY: raw is a valid WT_CURSOR with key/value set.
        let rc = unsafe { update_fn(self.raw) };
        check(rc)
    }

    pub fn remove(&self) -> WtResult<()> {
        let remove_fn = unsafe { (*self.raw).remove }.ok_or_else(|| WtError {
            code: -1,
            message: "remove vtable null".into(),
        })?;
        // SAFETY: raw is a valid WT_CURSOR with key set.
        let rc = unsafe { remove_fn(self.raw) };
        check(rc)
    }

    pub fn reset(&self) -> WtResult<()> {
        let reset_fn = unsafe { (*self.raw).reset }.ok_or_else(|| WtError {
            code: -1,
            message: "reset vtable null".into(),
        })?;
        // SAFETY: raw is a valid WT_CURSOR.
        let rc = unsafe { reset_fn(self.raw) };
        check(rc)
    }

    /// Close the cursor. Called automatically on drop.
    pub fn close(&mut self) -> WtResult<()> {
        if !self.raw.is_null() {
            let close_fn = unsafe { (*self.raw).close }.ok_or_else(|| WtError {
                code: -1,
                message: "cursor close vtable null".into(),
            })?;
            // SAFETY: raw is a valid WT_CURSOR; after close the pointer is invalidated.
            let rc = unsafe { close_fn(self.raw) };
            self.raw = ptr::null_mut();
            check(rc)?;
        }
        Ok(())
    }

    /// Check whether the last error was WT_NOTFOUND.
    pub fn is_not_found(rc: c_int) -> bool {
        rc == WT_NOTFOUND
    }

    /// Check whether the error is a duplicate key.
    pub fn is_duplicate_key(rc: c_int) -> bool {
        rc == WT_DUPLICATE_KEY
    }
}

impl Drop for WtCursor {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
