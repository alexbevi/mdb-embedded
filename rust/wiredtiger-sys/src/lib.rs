//! Raw FFI bindings to WiredTiger, dynamically loaded from the pip-installed
//! SWIG extension at runtime.
//!
//! # Design
//!
//! WiredTiger uses vtable-style structs: `WT_CONNECTION`, `WT_SESSION`, and
//! `WT_CURSOR` contain function-pointer fields.  Only the top-level entry point
//! `wiredtiger_open` needs to be resolved via `dlopen`; all subsequent calls go
//! through the vtable pointers on the returned structs.
//!
//! # ABI Safety
//!
//! The struct layouts below are derived from the WiredTiger 11.3.x public header
//! (`src/include/wiredtiger.in`).  They include all fields present in the *real*
//! C struct (including `#if !defined(SWIG)` and `#ifndef DOXYGEN` sections),
//! because the `.so` we dlopen contains the full C library.  Fields we don't
//! call are typed as `*const c_void` to keep the offsets correct.
//!
//! At load time, [`WtLibrary::load`] resolves the `wiredtiger_version` symbol
//! and verifies that the loaded library's **major.minor** version matches
//! [`ABI_MAJOR`]`.`[`ABI_MINOR`].  A mismatch means the vtable struct layouts
//! in this crate are likely wrong, which would cause silent memory corruption.
//! The check can be bypassed by setting `SMONGO_SKIP_WT_ABI_CHECK=1` (at your
//! own risk).

#![allow(non_camel_case_types, non_snake_case, dead_code)]

use std::os::raw::{c_char, c_int, c_void};
use std::path::PathBuf;

pub const WT_SUCCESS: c_int = 0;
pub const WT_NOTFOUND: c_int = -31803;
pub const WT_DUPLICATE_KEY: c_int = -31804;
pub const WT_ROLLBACK: c_int = -31800;

/// Expected WiredTiger major version for our struct layouts.
pub const ABI_MAJOR: c_int = 11;
/// Expected WiredTiger minor version for our struct layouts.
pub const ABI_MINOR: c_int = 3;

// WT_INTPACK64_MAXSIZE = sizeof(int64_t) + 1 = 9
const WT_INTPACK64_MAXSIZE: usize = 9;

// ---------------------------------------------------------------------------
// WT_ITEM -- generic byte buffer used for raw values
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct WT_ITEM {
    pub data: *const c_void,
    pub size: usize,
    pub mem: *mut c_void,   // internal
    pub memsize: usize,     // internal
    pub flags: u32,         // internal
}

// ---------------------------------------------------------------------------
// Forward declarations
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct WT_EVENT_HANDLER {
    _opaque: [u8; 0],
}

// ---------------------------------------------------------------------------
// WT_CURSOR -- data access handle (WiredTiger 11.3.1)
// ---------------------------------------------------------------------------
//
// Field order transcribed from wiredtiger.in line 199-805.
// Every field is present in the struct as allocated by the C library.
#[repr(C)]
pub struct WT_CURSOR {
    // ---- public data fields ----
    pub session: *mut WT_SESSION,
    pub uri: *const c_char,
    pub key_format: *const c_char,
    pub value_format: *const c_char,

    // ---- vtable: "Data access" ----
    pub get_key: *const c_void,             // int (*)(WT_CURSOR*, ...) -- variadic
    pub get_value: *const c_void,           // int (*)(WT_CURSOR*, ...) -- variadic
    pub get_raw_key_value: *const c_void,   // int (*)(WT_CURSOR*, WT_ITEM*, WT_ITEM*)
    pub set_key: *const c_void,             // void (*)(WT_CURSOR*, ...) -- variadic
    pub set_value: *const c_void,           // void (*)(WT_CURSOR*, ...) -- variadic

    // ---- vtable: "Cursor positioning" ----
    pub compare: *const c_void,             // int (*)(WT_CURSOR*, WT_CURSOR*, int*)
    pub equals: *const c_void,              // int (*)(WT_CURSOR*, WT_CURSOR*, int*)
    pub next: Option<unsafe extern "C" fn(cursor: *mut WT_CURSOR) -> c_int>,
    pub prev: Option<unsafe extern "C" fn(cursor: *mut WT_CURSOR) -> c_int>,
    pub reset: Option<unsafe extern "C" fn(cursor: *mut WT_CURSOR) -> c_int>,
    pub search: Option<unsafe extern "C" fn(cursor: *mut WT_CURSOR) -> c_int>,
    pub search_near:
        Option<unsafe extern "C" fn(cursor: *mut WT_CURSOR, exactp: *mut c_int) -> c_int>,

    // ---- vtable: "Data modification" ----
    pub insert: Option<unsafe extern "C" fn(cursor: *mut WT_CURSOR) -> c_int>,
    pub modify: *const c_void,              // int (*)(WT_CURSOR*, WT_MODIFY*, int)
    pub update: Option<unsafe extern "C" fn(cursor: *mut WT_CURSOR) -> c_int>,
    pub remove: Option<unsafe extern "C" fn(cursor: *mut WT_CURSOR) -> c_int>,
    pub reserve: *const c_void,             // int (*)(WT_CURSOR*)

    // ---- #ifndef DOXYGEN ----
    pub checkpoint_id: *const c_void,       // uint64_t (*)(WT_CURSOR*)

    // ---- remaining public vtable ----
    pub close: Option<unsafe extern "C" fn(cursor: *mut WT_CURSOR) -> c_int>,
    pub largest_key: *const c_void,         // int (*)(WT_CURSOR*)
    pub reconfigure: *const c_void,         // int (*)(WT_CURSOR*, const char*)
    pub bound: *const c_void,               // int (*)(WT_CURSOR*, const char*)

    // ---- #if !defined(SWIG) && !defined(DOXYGEN) -- private fields ----
    pub _cache: *const c_void,              // int (*)(WT_CURSOR*)
    pub _reopen: *const c_void,             // int (*)(WT_CURSOR*, bool)
    pub _uri_hash: u64,
    // TAILQ_ENTRY(wt_cursor) q
    pub _q_tqe_next: *mut WT_CURSOR,
    pub _q_tqe_prev: *mut *mut WT_CURSOR,
    pub _recno: u64,
    pub _raw_recno_buf: [u8; WT_INTPACK64_MAXSIZE],
    // padding inserted by compiler to align next pointer (7 bytes on 64-bit)
    pub _pad_recno: [u8; 7],
    pub _json_private: *mut c_void,
    pub _lang_private: *mut c_void,
    pub key: WT_ITEM,
    pub value: WT_ITEM,
    pub _saved_err: c_int,
    pub _pad_saved_err: [u8; 4],            // align to 8 for next pointer
    pub _internal_uri: *const c_char,
    pub _lower_bound: WT_ITEM,
    pub _upper_bound: WT_ITEM,
    pub _flags: u64,
}

// ---------------------------------------------------------------------------
// WT_SESSION -- per-thread database context (WiredTiger 11.3.1)
// ---------------------------------------------------------------------------
//
// Field order transcribed from wiredtiger.in line 822-2091.
#[repr(C)]
pub struct WT_SESSION {
    // ---- data fields ----
    pub connection: *mut WT_CONNECTION,
    pub app_private: *mut c_void,  // #if !defined(SWIG) -- present in real struct

    // ---- vtable (exact order from header) ----
    pub close:
        Option<unsafe extern "C" fn(session: *mut WT_SESSION, config: *const c_char) -> c_int>,
    pub reconfigure_session: *const c_void, // int (*)(WT_SESSION*, const char*)
    pub strerror: *const c_void,            // const char* (*)(WT_SESSION*, int)
    pub open_cursor: Option<
        unsafe extern "C" fn(
            session: *mut WT_SESSION,
            uri: *const c_char,
            to_dup: *mut WT_CURSOR,
            config: *const c_char,
            cursorp: *mut *mut WT_CURSOR,
        ) -> c_int,
    >,
    pub alter: *const c_void,               // int (*)(WT_SESSION*, const char*, const char*)
    pub bind_configuration: *const c_void,  // int (*)(WT_SESSION*, const char*, ...)
    pub create: Option<
        unsafe extern "C" fn(
            session: *mut WT_SESSION,
            name: *const c_char,
            config: *const c_char,
        ) -> c_int,
    >,
    pub compact: Option<
        unsafe extern "C" fn(
            session: *mut WT_SESSION,
            name: *const c_char,
            config: *const c_char,
        ) -> c_int,
    >,
    pub drop: Option<
        unsafe extern "C" fn(
            session: *mut WT_SESSION,
            name: *const c_char,
            config: *const c_char,
        ) -> c_int,
    >,
    pub join: *const c_void,                // int (*)(WT_SESSION*, WT_CURSOR*, WT_CURSOR*, const char*)
    pub log_flush: *const c_void,           // int (*)(WT_SESSION*, const char*)
    pub log_printf: *const c_void,          // int (*)(WT_SESSION*, const char*, ...)
    pub rename: *const c_void,              // int (*)(WT_SESSION*, const char*, const char*, const char*)
    pub reset: Option<unsafe extern "C" fn(session: *mut WT_SESSION) -> c_int>,
    pub salvage: *const c_void,             // int (*)(WT_SESSION*, const char*, const char*)
    pub truncate: *const c_void,            // int (*)(WT_SESSION*, const char*, WT_CURSOR*, WT_CURSOR*, const char*)
    pub verify: Option<
        unsafe extern "C" fn(
            session: *mut WT_SESSION,
            name: *const c_char,
            config: *const c_char,
        ) -> c_int,
    >,
    pub begin_transaction:
        Option<unsafe extern "C" fn(session: *mut WT_SESSION, config: *const c_char) -> c_int>,
    pub commit_transaction:
        Option<unsafe extern "C" fn(session: *mut WT_SESSION, config: *const c_char) -> c_int>,
    pub prepare_transaction: *const c_void, // int (*)(WT_SESSION*, const char*)
    pub rollback_transaction:
        Option<unsafe extern "C" fn(session: *mut WT_SESSION, config: *const c_char) -> c_int>,
    pub query_timestamp: *const c_void,     // int (*)(WT_SESSION*, char*, const char*)
    pub timestamp_transaction: *const c_void, // int (*)(WT_SESSION*, const char*)
    pub timestamp_transaction_uint: *const c_void, // int (*)(WT_SESSION*, WT_TS_TXN_TYPE, uint64_t)
    pub checkpoint:
        Option<unsafe extern "C" fn(session: *mut WT_SESSION, config: *const c_char) -> c_int>,
    pub reset_snapshot: *const c_void,      // int (*)(WT_SESSION*)
    pub transaction_pinned_range: *const c_void, // int (*)(WT_SESSION*, uint64_t*)

    // ---- #ifndef DOXYGEN ----
    pub get_rollback_reason: *const c_void, // const char* (*)(WT_SESSION*)
    pub breakpoint: *const c_void,          // int (*)(WT_SESSION*)
}

// ---------------------------------------------------------------------------
// WT_CONNECTION -- database handle (WiredTiger 11.3.1)
// ---------------------------------------------------------------------------
//
// Field order transcribed from wiredtiger.in line 2106-2939.
// NOTE: WT_CONNECTION has NO data fields before the vtable -- the first
// member is `close`.
#[repr(C)]
pub struct WT_CONNECTION {
    pub close: Option<
        unsafe extern "C" fn(connection: *mut WT_CONNECTION, config: *const c_char) -> c_int,
    >,
    // #ifndef DOXYGEN
    pub debug_info: *const c_void,          // int (*)(WT_CONNECTION*, const char*)

    pub reconfigure: *const c_void,         // int (*)(WT_CONNECTION*, const char*)
    pub get_home: *const c_void,            // const char* (*)(WT_CONNECTION*)
    pub compile_configuration: *const c_void, // int (*)(WT_CONNECTION*, const char*, const char*, const char**, size_t*)
    pub configure_method: *const c_void,    // int (*)(WT_CONNECTION*, const char*, const char*, const char*, const char*, WT_CONFIG_CHECK*, size_t)
    pub is_new: *const c_void,              // int (*)(WT_CONNECTION*)
    pub open_session: Option<
        unsafe extern "C" fn(
            connection: *mut WT_CONNECTION,
            event_handler: *mut WT_EVENT_HANDLER,
            config: *const c_char,
            sessionp: *mut *mut WT_SESSION,
        ) -> c_int,
    >,
    pub query_timestamp: *const c_void,     // int (*)(WT_CONNECTION*, char*, const char*)
    pub set_timestamp: *const c_void,       // int (*)(WT_CONNECTION*, const char*)
    pub rollback_to_stable: *const c_void,  // int (*)(WT_CONNECTION*, const char*)
    pub load_extension: *const c_void,      // int (*)(WT_CONNECTION*, const char*, const char*)
    pub add_data_source: *const c_void,     // int (*)(WT_CONNECTION*, const char*, WT_DATA_SOURCE*, const char*)
    pub add_collator: *const c_void,        // int (*)(WT_CONNECTION*, const char*, WT_COLLATOR*, const char*)
    pub add_compressor: *const c_void,      // int (*)(WT_CONNECTION*, const char*, WT_COMPRESSOR*, const char*)
    pub add_encryptor: *const c_void,       // int (*)(WT_CONNECTION*, const char*, WT_ENCRYPTOR*, const char*)
    pub add_extractor: *const c_void,       // int (*)(WT_CONNECTION*, const char*, WT_EXTRACTOR*, const char*)
    pub set_file_system: *const c_void,     // int (*)(WT_CONNECTION*, WT_FILE_SYSTEM*, const char*)
    // #if !defined(DOXYGEN) && !defined(SWIG) -- present in real struct
    pub add_storage_source: *const c_void,  // int (*)(WT_CONNECTION*, const char*, WT_STORAGE_SOURCE*, const char*)
    // #if !defined(DOXYGEN)
    pub get_storage_source: *const c_void,  // int (*)(WT_CONNECTION*, const char*, WT_STORAGE_SOURCE**)
    pub get_extension_api: *const c_void,   // WT_EXTENSION_API* (*)(WT_CONNECTION*)
}

// ---------------------------------------------------------------------------
// C shim functions for variadic vtable calls (ARM64 safe)
// ---------------------------------------------------------------------------
//
// On ARM64/Apple Silicon, variadic functions use a different calling convention
// from non-variadic ones.  These C shims correctly dispatch through variadic
// function pointers.
extern "C" {
    pub fn wt_shim_set_key_str(
        fn_ptr: *const c_void,
        cursor: *mut WT_CURSOR,
        key: *const c_char,
    );
    pub fn wt_shim_set_key_raw(
        fn_ptr: *const c_void,
        cursor: *mut WT_CURSOR,
        item: *const WT_ITEM,
    );
    pub fn wt_shim_set_value_raw(
        fn_ptr: *const c_void,
        cursor: *mut WT_CURSOR,
        item: *const WT_ITEM,
    );
    pub fn wt_shim_set_value_str(
        fn_ptr: *const c_void,
        cursor: *mut WT_CURSOR,
        value: *const c_char,
    );
    pub fn wt_shim_get_key_str(
        fn_ptr: *const c_void,
        cursor: *mut WT_CURSOR,
        keyp: *mut *const c_char,
    ) -> c_int;
    pub fn wt_shim_get_key_raw(
        fn_ptr: *const c_void,
        cursor: *mut WT_CURSOR,
        keyp: *mut WT_ITEM,
    ) -> c_int;
    pub fn wt_shim_get_value_raw(
        fn_ptr: *const c_void,
        cursor: *mut WT_CURSOR,
        valuep: *mut WT_ITEM,
    ) -> c_int;
    pub fn wt_shim_get_value_str(
        fn_ptr: *const c_void,
        cursor: *mut WT_CURSOR,
        valp: *mut *const c_char,
    ) -> c_int;
    pub fn wt_shim_get_value_ssq(
        fn_ptr: *const c_void,
        cursor: *mut WT_CURSOR,
        descp: *mut *const c_char,
        namep: *mut *const c_char,
        valp: *mut i64,
    ) -> c_int;
}

// ---------------------------------------------------------------------------
// Function pointer types for wiredtiger_open and wiredtiger_version
// ---------------------------------------------------------------------------
type WtOpenFn = unsafe extern "C" fn(
    home: *const c_char,
    event_handler: *mut WT_EVENT_HANDLER,
    config: *const c_char,
    connectionp: *mut *mut WT_CONNECTION,
) -> c_int;

type WtVersionFn = unsafe extern "C" fn(
    majorp: *mut c_int,
    minorp: *mut c_int,
    patchp: *mut c_int,
) -> *const c_char;

/// Version tuple returned by [`WtLibrary::version`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WtVersion {
    pub major: c_int,
    pub minor: c_int,
    pub patch: c_int,
}

impl std::fmt::Display for WtVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

// ---------------------------------------------------------------------------
// WtLibrary -- dynamically loaded WiredTiger
// ---------------------------------------------------------------------------

/// A handle to the dynamically loaded WiredTiger library.
///
/// Provides [`wiredtiger_open`] resolved from the pip-installed SWIG extension.
/// All subsequent WT operations go through vtable pointers on the returned
/// `WT_CONNECTION` / `WT_SESSION` / `WT_CURSOR` structs.
///
/// At load time the library's `wiredtiger_version()` is called and compared
/// against [`ABI_MAJOR`]`.`[`ABI_MINOR`].  A mismatch causes [`load`](Self::load)
/// to return an error describing the incompatibility.
pub struct WtLibrary {
    _lib: libloading::Library,
    open_fn: WtOpenFn,
    version: WtVersion,
}

unsafe impl Send for WtLibrary {}
unsafe impl Sync for WtLibrary {}

impl WtLibrary {
    /// Load WiredTiger from the given shared library path.
    ///
    /// Resolves both `wiredtiger_open` and `wiredtiger_version`, then verifies
    /// the loaded library's major.minor version matches this crate's struct
    /// layouts ([`ABI_MAJOR`]`.`[`ABI_MINOR`]).  Returns an error if the
    /// versions are incompatible, unless `SMONGO_SKIP_WT_ABI_CHECK=1` is set.
    ///
    /// # Safety
    ///
    /// The `.so` / `.dylib` at `path` must contain valid `wiredtiger_open` and
    /// `wiredtiger_version` symbols with the standard WT ABI.
    pub unsafe fn load(path: &std::path::Path) -> Result<Self, String> {
        let lib = unsafe {
            libloading::Library::new(path)
                .map_err(|e| format!("dlopen {}: {e}", path.display()))?
        };
        let open_fn: WtOpenFn = unsafe {
            let sym = lib
                .get::<WtOpenFn>(b"wiredtiger_open\0")
                .map_err(|e| format!("dlsym wiredtiger_open: {e}"))?;
            *sym
        };
        let version_fn: WtVersionFn = unsafe {
            let sym = lib
                .get::<WtVersionFn>(b"wiredtiger_version\0")
                .map_err(|e| format!("dlsym wiredtiger_version: {e}"))?;
            *sym
        };

        let version = unsafe { query_version(version_fn) };

        let lib = Self { _lib: lib, open_fn, version };
        lib.verify_abi()?;
        Ok(lib)
    }

    /// Locate and load WiredTiger from the pip-installed `wiredtiger` package.
    ///
    /// Searches the Python site-packages directory for `_wiredtiger.*.so` (or
    /// `.dylib` on macOS).  Falls back to a user-specified `WIREDTIGER_LIB`
    /// environment variable.
    pub fn load_from_pip() -> Result<Self, String> {
        if let Ok(explicit) = std::env::var("WIREDTIGER_LIB") {
            return unsafe { Self::load(std::path::Path::new(&explicit)) };
        }

        let so_path = find_wt_swig_so()?;
        unsafe { Self::load(&so_path) }
    }

    /// The version of the loaded WiredTiger library.
    pub fn version(&self) -> WtVersion {
        self.version
    }

    /// Call `wiredtiger_open(home, event_handler, config, &connectionp)`.
    ///
    /// # Safety
    ///
    /// Caller must ensure `home` points to a valid, writable directory and
    /// `config` is a valid WT configuration string (or null).
    pub unsafe fn wiredtiger_open(
        &self,
        home: *const c_char,
        event_handler: *mut WT_EVENT_HANDLER,
        config: *const c_char,
        connectionp: *mut *mut WT_CONNECTION,
    ) -> c_int {
        (self.open_fn)(home, event_handler, config, connectionp)
    }

    fn verify_abi(&self) -> Result<(), String> {
        check_abi_compatibility(self.version)
    }
}

/// Verify that `loaded` major.minor matches [`ABI_MAJOR`]`.`[`ABI_MINOR`].
///
/// Returns `Ok(())` if compatible or if `SMONGO_SKIP_WT_ABI_CHECK=1` is set.
pub fn check_abi_compatibility(loaded: WtVersion) -> Result<(), String> {
    if std::env::var("SMONGO_SKIP_WT_ABI_CHECK").as_deref() == Ok("1") {
        return Ok(());
    }

    if loaded.major != ABI_MAJOR || loaded.minor != ABI_MINOR {
        return Err(format!(
            "WiredTiger ABI mismatch: loaded library is {loaded}, but smongo's \
             FFI struct layouts require {ABI_MAJOR}.{ABI_MINOR}.x. \
             The vtable offsets in WT_CONNECTION, WT_SESSION, and WT_CURSOR \
             are version-specific — using a mismatched library risks silent \
             memory corruption.\n\n\
             To fix this:\n  \
               pip install 'wiredtiger>={ABI_MAJOR}.{ABI_MINOR},<{ABI_MAJOR}.{next_minor}'\n\n\
             To bypass this check (UNSAFE):\n  \
               export SMONGO_SKIP_WT_ABI_CHECK=1",
            next_minor = ABI_MINOR + 1,
        ));
    }

    Ok(())
}

/// Call `wiredtiger_version()` to retrieve the library's version triple.
///
/// # Safety
/// `version_fn` must be a valid pointer to the real `wiredtiger_version` symbol.
unsafe fn query_version(version_fn: WtVersionFn) -> WtVersion {
    let mut major: c_int = 0;
    let mut minor: c_int = 0;
    let mut patch: c_int = 0;
    let _ = unsafe { version_fn(&mut major, &mut minor, &mut patch) };
    WtVersion { major, minor, patch }
}

// ---------------------------------------------------------------------------
// Error-code-to-string
// ---------------------------------------------------------------------------

/// Map a WT error code to its string description.
pub fn wt_strerror(err: c_int) -> &'static str {
    match err {
        0 => "WT_SUCCESS",
        -31800 => "WT_ROLLBACK",
        -31803 => "WT_NOTFOUND",
        -31804 => "WT_DUPLICATE_KEY",
        _ => "WT_UNKNOWN",
    }
}

// ---------------------------------------------------------------------------
// Locate the pip-installed SWIG .so
// ---------------------------------------------------------------------------

fn find_wt_swig_so() -> Result<PathBuf, String> {
    // Strategy 1: Use VIRTUAL_ENV env var
    if let Ok(venv) = std::env::var("VIRTUAL_ENV") {
        let lib_dir = PathBuf::from(&venv).join("lib");
        if let Some(path) = search_for_wt_so(&lib_dir) {
            return Ok(path);
        }
    }

    // Strategy 2: Walk site-packages looking for the SWIG .so
    let site_packages_dirs = [
        ".venv/lib",
        "venv/lib",
    ];
    for base in &site_packages_dirs {
        let lib_dir = PathBuf::from(base);
        if let Some(path) = search_for_wt_so(&lib_dir) {
            return Ok(path);
        }
    }

    // Strategy 3: Search pyenv versions
    if let Ok(home) = std::env::var("HOME") {
        let pyenv_dir = PathBuf::from(&home).join(".pyenv/versions");
        if pyenv_dir.is_dir() {
            if let Some(path) = search_for_wt_so(&pyenv_dir) {
                return Ok(path);
            }
        }
    }

    // Strategy 4: Ask python3 directly (covers system installs, GHA setup-python, etc.)
    if let Ok(output) = std::process::Command::new("python3")
        .args([
            "-c",
            "import pathlib, wiredtiger; \
             print(next(pathlib.Path(wiredtiger.__file__).parent.glob('_wiredtiger.*')))",
        ])
        .output()
    {
        if output.status.success() {
            let path_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let path = PathBuf::from(&path_str);
            if path.exists() {
                return Ok(path);
            }
        }
    }

    Err(
        "Could not find _wiredtiger SWIG extension. \
         Set WIREDTIGER_LIB to the .so path, or ensure \
         `pip install wiredtiger` was run in the active venv."
            .to_string(),
    )
}

fn search_for_wt_so(base: &std::path::Path) -> Option<PathBuf> {
    let walker = walkdir(base);
    for entry in walker {
        let os_name = entry.file_name();
        let name = os_name.to_string_lossy();
        if name.starts_with("_wiredtiger.") && (name.ends_with(".so") || name.ends_with(".dylib"))
        {
            return Some(entry.path().to_path_buf());
        }
    }
    None
}

/// Simple recursive directory walker (avoids pulling in the `walkdir` crate).
fn walkdir(base: &std::path::Path) -> Vec<std::fs::DirEntry> {
    let mut result = Vec::new();
    walkdir_inner(base, &mut result);
    result
}

fn walkdir_inner(dir: &std::path::Path, out: &mut Vec<std::fs::DirEntry>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walkdir_inner(&path, out);
            } else {
                out.push(entry);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_strings() {
        assert_eq!(wt_strerror(WT_SUCCESS), "WT_SUCCESS");
        assert_eq!(wt_strerror(WT_NOTFOUND), "WT_NOTFOUND");
        assert_eq!(wt_strerror(WT_DUPLICATE_KEY), "WT_DUPLICATE_KEY");
        assert_eq!(wt_strerror(WT_ROLLBACK), "WT_ROLLBACK");
    }

    #[test]
    fn version_display_format() {
        let v = WtVersion { major: 11, minor: 3, patch: 1 };
        assert_eq!(v.to_string(), "11.3.1");
    }

    #[test]
    fn abi_check_accepts_matching_version() {
        std::env::remove_var("SMONGO_SKIP_WT_ABI_CHECK");
        let v = WtVersion { major: ABI_MAJOR, minor: ABI_MINOR, patch: 99 };
        assert!(check_abi_compatibility(v).is_ok());
    }

    #[test]
    fn abi_check_rejects_wrong_major() {
        std::env::remove_var("SMONGO_SKIP_WT_ABI_CHECK");
        let v = WtVersion { major: 99, minor: ABI_MINOR, patch: 0 };
        let err = check_abi_compatibility(v).unwrap_err();
        assert!(err.contains("ABI mismatch"), "expected ABI mismatch error, got: {err}");
        assert!(err.contains("99.3.0"), "error should mention the loaded version");
    }

    #[test]
    fn abi_check_rejects_wrong_minor() {
        std::env::remove_var("SMONGO_SKIP_WT_ABI_CHECK");
        let v = WtVersion { major: ABI_MAJOR, minor: 999, patch: 0 };
        let err = check_abi_compatibility(v).unwrap_err();
        assert!(err.contains("ABI mismatch"), "expected ABI mismatch error, got: {err}");
    }

    #[test]
    fn abi_check_bypass_with_env_var() {
        std::env::set_var("SMONGO_SKIP_WT_ABI_CHECK", "1");
        let v = WtVersion { major: 99, minor: 99, patch: 0 };
        assert!(check_abi_compatibility(v).is_ok(), "ABI check should be skipped");
        std::env::remove_var("SMONGO_SKIP_WT_ABI_CHECK");
    }

    #[test]
    fn abi_error_message_includes_fix_instructions() {
        std::env::remove_var("SMONGO_SKIP_WT_ABI_CHECK");
        let v = WtVersion { major: 12, minor: 0, patch: 0 };
        let err = check_abi_compatibility(v).unwrap_err();
        assert!(err.contains("pip install"), "error should include pip install instructions");
        assert!(err.contains("SMONGO_SKIP_WT_ABI_CHECK"), "error should mention bypass env var");
    }
}
