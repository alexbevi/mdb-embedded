//! `#[pyclass]` bridge types that wrap `wt_safe` types and expose the same
//! Python API as the SWIG `wiredtiger` module's Session and Cursor objects.
//!
//! This allows Python code that currently uses SWIG WT objects to switch
//! seamlessly to Rust-managed sessions/cursors, while Rust code uses the
//! underlying `WtSession`/`WtCursor` directly without PyO3 overhead.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use wiredtiger_sys::{
    wt_shim_get_key_str, wt_shim_get_value_raw, wt_shim_get_value_str, wt_shim_set_key_str,
    wt_shim_set_value_raw, wt_shim_set_value_str, WT_CURSOR, WT_ITEM, WT_NOTFOUND,
};

use crate::wt_safe::{WtError, WtResult, WtSession};

// Extension trait to convert WtResult to PyResult conveniently
pub(crate) trait WtResultExt<T> {
    fn py(self) -> PyResult<T>;
}

impl<T> WtResultExt<T> for WtResult<T> {
    fn py(self) -> PyResult<T> {
        self.map_err(|e| PyRuntimeError::new_err(format!("WiredTiger error {}: {}", e.code, e.message)))
    }
}

// Thread-local override: when a multi-document wire transaction is active,
// this holds the raw WT_SESSION pointer from the TransactionSession so that
// all collection/index operations route through it automatically.
thread_local! {
    pub(crate) static TXN_SESSION_OVERRIDE: std::cell::Cell<Option<*mut wiredtiger_sys::WT_SESSION>> =
        const { std::cell::Cell::new(None) };
}

/// Set the thread-local transaction session override.
pub(crate) fn set_txn_session_override(raw: *mut wiredtiger_sys::WT_SESSION) {
    TXN_SESSION_OVERRIDE.with(|c| c.set(Some(raw)));
}

/// Clear the thread-local transaction session override.
pub(crate) fn clear_txn_session_override() {
    TXN_SESSION_OVERRIDE.with(|c| c.set(None));
}

/// Borrow a `WtSession` from an `Option<*mut WT_SESSION>`, returning a
/// Python error if the session has been closed (`None`).
///
/// If a thread-local transaction session override is active, that session
/// is returned instead (multi-document transaction support).
pub(crate) fn borrow_wt_session(
    raw: Option<*mut wiredtiger_sys::WT_SESSION>,
    context: &str,
) -> PyResult<std::mem::ManuallyDrop<WtSession>> {
    let effective = TXN_SESSION_OVERRIDE.with(|c| c.get()).or(raw);
    let raw =
        effective.ok_or_else(|| PyRuntimeError::new_err(format!("{context} session is closed")))?;
    // SAFETY: raw was set during construction from a valid WT_SESSION and is
    // cleared to None on close().  ManuallyDrop prevents Drop from closing
    // the session we don't own.
    Ok(std::mem::ManuallyDrop::new(unsafe {
        WtSession::from_raw(raw)
    }))
}

/// Retrieve a vtable function pointer or return a Python error.
fn vtable_fn<F>(opt: Option<F>, name: &str) -> PyResult<F> {
    opt.ok_or_else(|| PyRuntimeError::new_err(format!("{name} vtable null")))
}

// ---------------------------------------------------------------------------
// RustWtSession -- #[pyclass] bridge for WT sessions
// ---------------------------------------------------------------------------

/// Python-facing WiredTiger session -- thin PyO3 bridge over [`crate::wt_safe::WtSession`].
#[pyclass]
pub struct RustWtSession {
    inner: Option<WtSession>,
}

// SAFETY: WT sessions are not thread-safe at the C level.  Sync is required
// by PyO3 for #[pyclass].  Safety is ensured by the single-owner invariant:
// each RustWtSession belongs to exactly one RustLocalCollection (or
// transaction), and access is serialized by the collection's InlineRwLock.
// Under free-threaded Python, PyO3's RefCell-like borrow checking prevents
// concurrent &mut self access from multiple threads.
unsafe impl Sync for RustWtSession {}

impl RustWtSession {
    /// Construct from an already-opened safe session (Rust-side only).
    pub fn from_safe(session: WtSession) -> Self {
        Self {
            inner: Some(session),
        }
    }

    /// Borrow the inner safe session (Rust-side only).
    pub fn get(&self) -> WtResult<&WtSession> {
        self.inner.as_ref().ok_or_else(|| WtError {
            code: -1,
            message: "session is closed".into(),
        })
    }

    /// Mutably borrow the inner safe session.
    pub fn get_mut(&mut self) -> WtResult<&mut WtSession> {
        self.inner.as_mut().ok_or_else(|| WtError {
            code: -1,
            message: "session is closed".into(),
        })
    }

    /// Take ownership of the inner session (for close).
    pub fn take(&mut self) -> Option<WtSession> {
        self.inner.take()
    }

    /// Open a cursor without going through Python dispatch.
    pub(crate) fn open_cursor_typed(
        &self,
        uri: &str,
        config: Option<&str>,
    ) -> PyResult<RustWtCursor> {
        let session = self.get().py()?;
        let cursor = session.open_cursor(uri, config).py()?;
        Ok(RustWtCursor::from_safe(cursor))
    }

    pub(crate) fn create_typed(&self, name: &str, config: &str) -> PyResult<()> {
        self.get().py()?.create(name, config).py()?;
        Ok(())
    }

    pub(crate) fn checkpoint_typed(&self, config: Option<&str>) -> PyResult<()> {
        self.get().py()?.checkpoint(config).py()?;
        Ok(())
    }

    pub(crate) fn close_typed(&mut self) -> PyResult<()> {
        if let Some(mut session) = self.inner.take() {
            session.close().py()?;
        }
        Ok(())
    }
}

#[pymethods]
impl RustWtSession {
    fn open_cursor(
        &self,
        uri: &str,
        to_dup: Option<&Bound<'_, PyAny>>,
        config: Option<&str>,
    ) -> PyResult<RustWtCursor> {
        let _ = to_dup; // to_dup not yet supported
        let session = self.get().py()?;
        let cursor = session.open_cursor(uri, config).py()?;
        Ok(RustWtCursor::from_safe(cursor))
    }

    fn create(&self, name: &str, config: &str) -> PyResult<()> {
        self.get().py()?.create(name, config).py()?;
        Ok(())
    }

    #[pyo3(signature = (name, config=None))]
    fn drop_table(&self, name: &str, config: Option<&str>) -> PyResult<()> {
        self.get().py()?.drop_table(name, config).py()?;
        Ok(())
    }

    // Python SWIG module uses session.drop(name) or session.drop(name, "force")
    // but "drop" is a reserved word in Rust. We expose it via __getattr__ or
    // as drop_table + a Python alias. The Python shim handles mapping.

    #[pyo3(signature = (config=None))]
    fn begin_transaction(&self, config: Option<&str>) -> PyResult<()> {
        self.get().py()?.begin_transaction(config).py()?;
        Ok(())
    }

    #[pyo3(signature = (config=None))]
    fn commit_transaction(&self, config: Option<&str>) -> PyResult<()> {
        self.get().py()?.commit_transaction(config).py()?;
        Ok(())
    }

    #[pyo3(signature = (config=None))]
    fn rollback_transaction(&self, config: Option<&str>) -> PyResult<()> {
        self.get().py()?.rollback_transaction(config).py()?;
        Ok(())
    }

    #[pyo3(signature = (config=None))]
    fn checkpoint(&self, config: Option<&str>) -> PyResult<()> {
        self.get().py()?.checkpoint(config).py()?;
        Ok(())
    }

    #[pyo3(signature = (name, config=None))]
    fn compact(&self, name: &str, config: Option<&str>) -> PyResult<()> {
        self.get().py()?.compact(name, config).py()?;
        Ok(())
    }

    #[pyo3(signature = (name, config=None))]
    fn verify(&self, name: &str, config: Option<&str>) -> PyResult<()> {
        self.get().py()?.verify(name, config).py()?;
        Ok(())
    }

    #[pyo3(signature = (config=None))]
    fn close(&mut self, config: Option<&str>) -> PyResult<()> {
        let _ = config;
        if let Some(mut session) = self.inner.take() {
            session.close().py()?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RustWtCursor -- #[pyclass] bridge for WT cursors
// ---------------------------------------------------------------------------

/// Python-facing WiredTiger cursor -- thin PyO3 bridge over [`crate::wt_safe::WtCursor`].
#[pyclass]
pub struct RustWtCursor {
    raw: *mut WT_CURSOR,
    closed: bool,
    value_is_string: bool,
    _key_buf: Option<CString>,
    _val_buf: Option<CString>,
}

// SAFETY: RustWtCursor wraps a raw WT_CURSOR pointer.  WT cursors are
// session-local and not thread-safe at the C level.  Send+Sync is required by
// PyO3's #[pyclass].  Safety is ensured by the single-owner invariant: cursors
// are created, used, and closed within a single operation scope.  They are never
// stored in shared state or handed across connection boundaries.  Under
// free-threaded Python, PyO3's borrow checking prevents concurrent &mut self.
unsafe impl Send for RustWtCursor {}
unsafe impl Sync for RustWtCursor {}

impl RustWtCursor {
    pub fn from_safe(cursor: crate::wt_safe::WtCursor) -> Self {
        let raw = cursor.raw_ptr();
        let value_is_string = unsafe {
            let vf = (*raw).value_format;
            if vf.is_null() {
                false
            } else {
                CStr::from_ptr(vf).to_bytes() == b"S"
            }
        };
        // SAFETY: We take exclusive ownership of the raw pointer by
        // mem::forget-ing the WtCursor (preventing its Drop from closing).
        // RustWtCursor's own Drop will close the cursor.
        std::mem::forget(cursor);
        Self {
            raw,
            closed: false,
            value_is_string,
            _key_buf: None,
            _val_buf: None,
        }
    }

    fn check_open(&self) -> PyResult<()> {
        if self.closed || self.raw.is_null() {
            Err(PyRuntimeError::new_err("cursor is closed"))
        } else {
            Ok(())
        }
    }

    fn check_wt(rc: c_int) -> PyResult<()> {
        if rc == 0 {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err(format!(
                "WiredTiger error {}: {}",
                rc,
                wiredtiger_sys::wt_strerror(rc)
            )))
        }
    }

    pub fn raw_ptr(&self) -> *mut WT_CURSOR {
        self.raw
    }

    pub(crate) fn set_key_str(&mut self, key: &str) -> PyResult<()> {
        self.check_open()?;
        let c_key = CString::new(key).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let fn_ptr = unsafe { (*self.raw).set_key };
        unsafe { wt_shim_set_key_str(fn_ptr, self.raw, c_key.as_ptr()) };
        self._key_buf = Some(c_key);
        Ok(())
    }

    pub(crate) fn get_key_str(&self) -> PyResult<String> {
        self.check_open()?;
        let mut key_ptr: *const c_char = ptr::null();
        let fn_ptr = unsafe { (*self.raw).get_key };
        let rc = unsafe { wt_shim_get_key_str(fn_ptr, self.raw, &mut key_ptr) };
        Self::check_wt(rc)?;
        let key = unsafe { std::ffi::CStr::from_ptr(key_ptr) }
            .to_string_lossy()
            .into_owned();
        Ok(key)
    }

    pub(crate) fn get_value_string(&self) -> PyResult<String> {
        self.check_open()?;
        let mut val_ptr: *const c_char = ptr::null();
        let fn_ptr = unsafe { (*self.raw).get_value };
        let rc = unsafe { wt_shim_get_value_str(fn_ptr, self.raw, &mut val_ptr) };
        Self::check_wt(rc)?;
        let val = unsafe { std::ffi::CStr::from_ptr(val_ptr) }
            .to_string_lossy()
            .into_owned();
        Ok(val)
    }

    pub(crate) fn set_value_string(&mut self, value: &str) -> PyResult<()> {
        self.check_open()?;
        let c_val = CString::new(value).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let fn_ptr = unsafe { (*self.raw).set_value };
        unsafe { wt_shim_set_value_str(fn_ptr, self.raw, c_val.as_ptr()) };
        self._val_buf = Some(c_val);
        Ok(())
    }

    pub(crate) fn next_rc(&self) -> PyResult<i32> {
        self.check_open()?;
        let next_fn = vtable_fn(unsafe { (*self.raw).next }, "next")?;
        let rc = unsafe { next_fn(self.raw) };
        if rc == WT_NOTFOUND {
            return Ok(rc);
        }
        Self::check_wt(rc)?;
        Ok(0)
    }

    pub(crate) fn insert_typed(&self) -> PyResult<()> {
        self.check_open()?;
        let insert_fn = vtable_fn(unsafe { (*self.raw).insert }, "insert")?;
        let rc = unsafe { insert_fn(self.raw) };
        Self::check_wt(rc)
    }

    pub(crate) fn remove_typed(&self) -> PyResult<()> {
        self.check_open()?;
        let remove_fn = vtable_fn(unsafe { (*self.raw).remove }, "remove")?;
        let rc = unsafe { remove_fn(self.raw) };
        if rc == WT_NOTFOUND {
            return Ok(());
        }
        Self::check_wt(rc)
    }

    pub(crate) fn close_typed(&mut self) -> PyResult<()> {
        if !self.closed && !self.raw.is_null() {
            let close_fn = vtable_fn(unsafe { (*self.raw).close }, "close")?;
            let rc = unsafe { close_fn(self.raw) };
            self.raw = ptr::null_mut();
            self.closed = true;
            Self::check_wt(rc)?;
        }
        Ok(())
    }

    /// Set key + set value (string) + update in one call, matching cursor[key] = value.
    pub(crate) fn set_item_str(&mut self, key: &str, value: &str) -> PyResult<()> {
        self.set_key_str(key)?;
        self.set_value_string(value)?;
        self.check_open()?;
        let update_fn = vtable_fn(unsafe { (*self.raw).update }, "update")?;
        let rc = unsafe { update_fn(self.raw) };
        Self::check_wt(rc)
    }
}

#[pymethods]
impl RustWtCursor {
    fn set_key(&mut self, key: &str) -> PyResult<()> {
        self.check_open()?;
        let c_key = CString::new(key).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let fn_ptr = unsafe { (*self.raw).set_key };
        unsafe { wt_shim_set_key_str(fn_ptr, self.raw, c_key.as_ptr()) };
        self._key_buf = Some(c_key);
        Ok(())
    }

    fn get_key(&self) -> PyResult<String> {
        self.check_open()?;
        let mut key_ptr: *const c_char = ptr::null();
        let fn_ptr = unsafe { (*self.raw).get_key };
        let rc = unsafe { wt_shim_get_key_str(fn_ptr, self.raw, &mut key_ptr) };
        Self::check_wt(rc)?;
        let key = unsafe { std::ffi::CStr::from_ptr(key_ptr) }
            .to_string_lossy()
            .into_owned();
        Ok(key)
    }

    /// Auto-dispatching get_value: returns `str` for value_format=S tables
    /// (like oplog/sync) and `bytes` for value_format=u tables (like document
    /// data). Matches the SWIG WiredTiger Python binding behavior.
    fn get_value<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.check_open()?;
        if self.value_is_string {
            let mut val_ptr: *const c_char = ptr::null();
            let fn_ptr = unsafe { (*self.raw).get_value };
            let rc = unsafe { wt_shim_get_value_str(fn_ptr, self.raw, &mut val_ptr) };
            Self::check_wt(rc)?;
            let val = unsafe { std::ffi::CStr::from_ptr(val_ptr) }
                .to_string_lossy()
                .into_owned();
            Ok(PyString::new(py, &val).into_any())
        } else {
            let mut item = WT_ITEM {
                data: ptr::null(),
                size: 0,
                mem: ptr::null_mut(),
                memsize: 0,
                flags: 0,
            };
            let fn_ptr = unsafe { (*self.raw).get_value };
            let rc = unsafe { wt_shim_get_value_raw(fn_ptr, self.raw, &mut item) };
            Self::check_wt(rc)?;
            let bytes = unsafe { std::slice::from_raw_parts(item.data as *const u8, item.size) };
            Ok(PyBytes::new(py, bytes).into_any())
        }
    }

    /// Get value as string (for value_format=S tables like oplog/sync).
    fn get_value_str(&self) -> PyResult<String> {
        self.check_open()?;
        let mut val_ptr: *const c_char = ptr::null();
        let fn_ptr = unsafe { (*self.raw).get_value };
        let rc = unsafe { wt_shim_get_value_str(fn_ptr, self.raw, &mut val_ptr) };
        Self::check_wt(rc)?;
        let val = unsafe { std::ffi::CStr::from_ptr(val_ptr) }
            .to_string_lossy()
            .into_owned();
        Ok(val)
    }

    /// Set value from raw bytes (for value_format=u).
    fn set_value(&mut self, value: &[u8]) -> PyResult<()> {
        self.check_open()?;
        let item = WT_ITEM {
            data: value.as_ptr() as *const c_void,
            size: value.len(),
            mem: ptr::null_mut(),
            memsize: 0,
            flags: 0,
        };
        let fn_ptr = unsafe { (*self.raw).set_value };
        unsafe { wt_shim_set_value_raw(fn_ptr, self.raw, &item) };
        Ok(())
    }

    fn search(&self) -> PyResult<i32> {
        self.check_open()?;
        let search_fn = vtable_fn(unsafe { (*self.raw).search }, "search")?;
        // SAFETY: raw is a valid WT_CURSOR with a key set.
        let rc = unsafe { search_fn(self.raw) };
        if rc == WT_NOTFOUND {
            return Ok(rc);
        }
        Self::check_wt(rc)?;
        Ok(0)
    }

    fn search_near(&self) -> PyResult<i32> {
        self.check_open()?;
        let search_near_fn = vtable_fn(unsafe { (*self.raw).search_near }, "search_near")?;
        let mut exact: c_int = 0;
        // SAFETY: raw is a valid WT_CURSOR with a key set.
        let rc = unsafe { search_near_fn(self.raw, &mut exact) };
        if rc == WT_NOTFOUND {
            return Err(PyKeyError::new_err("WT_NOTFOUND"));
        }
        Self::check_wt(rc)?;
        Ok(exact)
    }

    fn next(&self) -> PyResult<i32> {
        self.check_open()?;
        let next_fn = vtable_fn(unsafe { (*self.raw).next }, "next")?;
        // SAFETY: raw is a valid WT_CURSOR.
        let rc = unsafe { next_fn(self.raw) };
        if rc == WT_NOTFOUND {
            return Ok(rc);
        }
        Self::check_wt(rc)?;
        Ok(0)
    }

    fn prev(&self) -> PyResult<i32> {
        self.check_open()?;
        let prev_fn = vtable_fn(unsafe { (*self.raw).prev }, "prev")?;
        // SAFETY: raw is a valid WT_CURSOR.
        let rc = unsafe { prev_fn(self.raw) };
        if rc == WT_NOTFOUND {
            return Ok(rc);
        }
        Self::check_wt(rc)?;
        Ok(0)
    }

    fn insert(&self) -> PyResult<()> {
        self.check_open()?;
        let insert_fn = vtable_fn(unsafe { (*self.raw).insert }, "insert")?;
        // SAFETY: raw is a valid WT_CURSOR with key/value set.
        let rc = unsafe { insert_fn(self.raw) };
        Self::check_wt(rc)
    }

    fn update(&self) -> PyResult<()> {
        self.check_open()?;
        let update_fn = vtable_fn(unsafe { (*self.raw).update }, "update")?;
        // SAFETY: raw is a valid WT_CURSOR with key/value set.
        let rc = unsafe { update_fn(self.raw) };
        Self::check_wt(rc)
    }

    fn remove(&self) -> PyResult<()> {
        self.check_open()?;
        let remove_fn = vtable_fn(unsafe { (*self.raw).remove }, "remove")?;
        // SAFETY: raw is a valid WT_CURSOR with key set.
        let rc = unsafe { remove_fn(self.raw) };
        if rc == WT_NOTFOUND {
            return Err(PyKeyError::new_err("WT_NOTFOUND"));
        }
        Self::check_wt(rc)
    }

    fn reset(&self) -> PyResult<()> {
        self.check_open()?;
        let reset_fn = vtable_fn(unsafe { (*self.raw).reset }, "reset")?;
        // SAFETY: raw is a valid WT_CURSOR.
        let rc = unsafe { reset_fn(self.raw) };
        Self::check_wt(rc)
    }

    fn close(&mut self) -> PyResult<()> {
        if !self.closed && !self.raw.is_null() {
            let close_fn = vtable_fn(unsafe { (*self.raw).close }, "close")?;
            // SAFETY: raw is a valid WT_CURSOR; after close the pointer is invalidated.
            let rc = unsafe { close_fn(self.raw) };
            self.raw = ptr::null_mut();
            self.closed = true;
            Self::check_wt(rc)?;
        }
        Ok(())
    }

    /// `cursor[key] = value` -- SWIG-compatible __setitem__.
    /// Sets key (string), sets value (str or bytes), then calls update().
    /// Accepts `str` for value_format=S tables (e.g. oplog) and `bytes` for
    /// value_format=u tables (e.g. document data).
    fn __setitem__(&mut self, key: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.check_open()?;

        // set_key
        let c_key = CString::new(key).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let fn_ptr = unsafe { (*self.raw).set_key };
        unsafe { wt_shim_set_key_str(fn_ptr, self.raw, c_key.as_ptr()) };
        self._key_buf = Some(c_key);

        // set_value: dispatch on Python type
        if let Ok(s) = value.extract::<String>() {
            // str → null-terminated CString via set_value_str (value_format=S)
            let c_val = CString::new(s).map_err(|e| PyValueError::new_err(e.to_string()))?;
            let fn_ptr = unsafe { (*self.raw).set_value };
            unsafe { wt_shim_set_value_str(fn_ptr, self.raw, c_val.as_ptr()) };
            self._val_buf = Some(c_val);
        } else {
            // bytes → raw WT_ITEM (value_format=u)
            let bytes: &[u8] = value
                .extract()
                .map_err(|_| PyTypeError::new_err("value must be str or bytes"))?;
            let item = WT_ITEM {
                data: bytes.as_ptr() as *const c_void,
                size: bytes.len(),
                mem: ptr::null_mut(),
                memsize: 0,
                flags: 0,
            };
            let fn_ptr = unsafe { (*self.raw).set_value };
            unsafe { wt_shim_set_value_raw(fn_ptr, self.raw, &item) };
        }

        // update (which inserts-or-updates when overwrite=true)
        let update_fn = vtable_fn(unsafe { (*self.raw).update }, "update")?;
        // SAFETY: raw is a valid WT_CURSOR with key/value set.
        let rc = unsafe { update_fn(self.raw) };
        Self::check_wt(rc)
    }

    /// `cursor[index]` -- for statistics cursors.
    /// Index 0 = description, 1 = key string, 2 = value string.
    fn __getitem__<'py>(&self, py: Python<'py>, index: i32) -> PyResult<Bound<'py, PyAny>> {
        self.check_open()?;

        // For statistics cursors (format "SSq"), get_value returns 3 columns:
        // description (string), key (string), value (i64 as string).
        // The SWIG cursor[i] is implemented as getting column i.
        //
        // We implement this by calling get_value_str which gives us a single
        // string for "S" format, but statistics cursors have format "SSq".
        // For simplicity, we return the key for index 0 and the raw value for others.

        // For statistics cursors, use the raw get_value which returns the whole
        // value. The Python SWIG binding returns different columns based on index.
        // We'll use get_key for index 0/1 and get_value for index 2.
        match index {
            0 => {
                // Description: for stats cursors, this is the key
                let mut key_ptr: *const c_char = ptr::null();
                let fn_ptr = unsafe { (*self.raw).get_key };
                let rc = unsafe { wt_shim_get_key_str(fn_ptr, self.raw, &mut key_ptr) };
                Self::check_wt(rc)?;
                let key = unsafe { std::ffi::CStr::from_ptr(key_ptr) }
                    .to_string_lossy()
                    .into_owned();
                Ok(PyString::new(py, &key).into_any())
            }
            1 => {
                // Stat description string (column 1 of value for SSq format)
                let mut val_ptr: *const c_char = ptr::null();
                let fn_ptr = unsafe { (*self.raw).get_value };
                let rc = unsafe { wt_shim_get_value_str(fn_ptr, self.raw, &mut val_ptr) };
                Self::check_wt(rc)?;
                let val = unsafe { std::ffi::CStr::from_ptr(val_ptr) }
                    .to_string_lossy()
                    .into_owned();
                Ok(PyString::new(py, &val).into_any())
            }
            2 => {
                // Stat value (i64 as string in the third column)
                // For simplicity return raw value as string
                let mut val_ptr: *const c_char = ptr::null();
                let fn_ptr = unsafe { (*self.raw).get_value };
                let rc = unsafe { wt_shim_get_value_str(fn_ptr, self.raw, &mut val_ptr) };
                Self::check_wt(rc)?;
                let val = unsafe { std::ffi::CStr::from_ptr(val_ptr) }
                    .to_string_lossy()
                    .into_owned();
                Ok(PyString::new(py, &val).into_any())
            }
            _ => Err(PyKeyError::new_err(format!("invalid stats index: {index}"))),
        }
    }
}

impl Drop for RustWtCursor {
    fn drop(&mut self) {
        if !self.closed && !self.raw.is_null() {
            // SAFETY: raw is a valid WT_CURSOR.  We use `if let` instead of
            // unwrap/expect to avoid panicking inside Drop.
            if let Some(close_fn) = unsafe { (*self.raw).close } {
                let _ = unsafe { close_fn(self.raw) };
            }
            self.raw = ptr::null_mut();
            self.closed = true;
        }
    }
}
