//! Process-wide cache for frequently-imported Python modules.
//!
//! Each accessor resolves the module on first call and returns the cached
//! `Bound<'_, PyModule>` on subsequent calls -- turning a Python dict lookup
//! into a Rust atomic load.
//!
//! Uses `pyo3::sync::PyOnceLock` so that threads blocked waiting for
//! initialization detach from the Python runtime, avoiding deadlocks
//! with the free-threaded (no-GIL) interpreter's stop-the-world pauses.

use std::sync::OnceLock;

use pyo3::prelude::*;
use pyo3::sync::{OnceLockExt, PyOnceLock};
use pyo3::types::PyModule;

macro_rules! cached_module {
    ($lock:ident, $fn_name:ident, $module:literal) => {
        static $lock: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

        pub fn $fn_name(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
            let m = $lock.get_or_try_init(py, || Ok::<_, PyErr>(py.import($module)?.unbind()))?;
            Ok(m.bind(py).clone())
        }
    };
}

macro_rules! cached_attr {
    ($lock:ident, $fn_name:ident, $module_fn:ident, $attr:literal) => {
        static $lock: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

        pub fn $fn_name(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
            let a = $lock.get_or_try_init(py, || {
                Ok::<_, PyErr>($module_fn(py)?.getattr($attr)?.unbind())
            })?;
            Ok(a.bind(py).clone())
        }
    };
}

macro_rules! cached_nested_attr {
    ($lock:ident, $fn_name:ident, $module_fn:ident, $attr1:literal, $attr2:literal) => {
        static $lock: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

        pub fn $fn_name(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
            let a = $lock.get_or_try_init(py, || {
                Ok::<_, PyErr>($module_fn(py)?.getattr($attr1)?.getattr($attr2)?.unbind())
            })?;
            Ok(a.bind(py).clone())
        }
    };
}

// ── Standard library ─────────────────────────────────────────────────
cached_module!(DATETIME, datetime, "datetime");
cached_module!(JSON, json_mod, "json");
cached_module!(RE, re_mod, "re");
cached_module!(BUILTINS, builtins, "builtins");
cached_module!(COPY, copy_mod, "copy");
cached_module!(TIME, time_mod, "time");
cached_module!(OPERATOR, operator_mod, "operator");
cached_module!(UUID, uuid_mod, "uuid");
cached_module!(PLATFORM, platform_mod, "platform");
cached_module!(OS, os_mod, "os");
cached_module!(SYS, sys_mod, "sys");
cached_module!(LOGGING, logging_mod, "logging");
cached_module!(RANDOM, random_mod, "random");
cached_module!(SECRETS, secrets_mod, "secrets");
cached_module!(RESOURCE, resource_mod, "resource");

// ── Third-party ──────────────────────────────────────────────────────
cached_module!(BSON, bson_mod, "bson");
cached_module!(BSON_JSON_UTIL, bson_json_util, "bson.json_util");

// ── Cached class/function attributes (resolved once, atomic thereafter) ──
cached_attr!(BSON_OBJECTID_CLS, bson_objectid_cls, bson_mod, "ObjectId");
cached_attr!(
    BSON_DECIMAL128_CLS,
    bson_decimal128_cls,
    bson_mod,
    "Decimal128"
);
cached_attr!(BSON_REGEX_CLS, bson_regex_cls, bson_mod, "Regex");
cached_attr!(
    BSON_TIMESTAMP_CLS,
    bson_timestamp_cls,
    bson_mod,
    "Timestamp"
);
cached_attr!(BSON_BINARY_CLS, bson_binary_cls, bson_mod, "Binary");
cached_attr!(BSON_INT64_CLS, bson_int64_cls, bson_mod, "Int64");
cached_attr!(BUILTINS_INT, builtins_int, builtins, "int");
cached_attr!(BUILTINS_FLOAT, builtins_float, builtins, "float");
cached_attr!(BUILTINS_ROUND, builtins_round, builtins, "round");
cached_attr!(
    DATETIME_DATETIME_CLS,
    datetime_datetime_cls,
    datetime,
    "datetime"
);
cached_nested_attr!(
    DATETIME_TZ_UTC,
    datetime_tz_utc,
    datetime,
    "timezone",
    "utc"
);

// ── Cached system info (platform/os -- static for process lifetime) ──

pub struct CachedSystemInfo {
    pub system: String,
    pub release: String,
    pub machine: String,
    pub node: String,
    pub platform_name: String,
    pub num_cores: i64,
    pub page_size: i64,
    pub total_memory_mb: i64,
}

static SYSTEM_INFO: OnceLock<CachedSystemInfo> = OnceLock::new();

pub fn system_info(py: Python<'_>) -> PyResult<&'static CachedSystemInfo> {
    Ok(SYSTEM_INFO.get_or_init_py_attached(py, || {
        let platform = platform_mod(py).ok();
        let os = os_mod(py).ok();

        let (system, release, machine, node, platform_name) = match &platform {
            Some(p) => (
                p.call_method0("system")
                    .and_then(|v| v.extract())
                    .unwrap_or_default(),
                p.call_method0("release")
                    .and_then(|v| v.extract())
                    .unwrap_or_default(),
                p.call_method0("machine")
                    .and_then(|v| v.extract())
                    .unwrap_or_default(),
                p.call_method0("node")
                    .and_then(|v| v.extract())
                    .unwrap_or_default(),
                p.call_method0("platform")
                    .and_then(|v| v.extract())
                    .unwrap_or_default(),
            ),
            None => Default::default(),
        };

        let (num_cores, page_size) = match &os {
            Some(o) => (
                o.call_method0("cpu_count")
                    .and_then(|v| v.extract())
                    .unwrap_or(1),
                if o.hasattr("sysconf").unwrap_or(false) {
                    o.call_method1("sysconf", ("SC_PAGE_SIZE",))
                        .and_then(|v| v.extract())
                        .unwrap_or(4096)
                } else {
                    4096
                },
            ),
            None => (1, 4096),
        };

        let total_memory_mb: i64 = smongo_wire_context(py)
            .and_then(|m| m.call_method0("get_total_memory_mb")?.extract())
            .unwrap_or(0);

        CachedSystemInfo {
            system,
            release,
            machine,
            node,
            platform_name,
            num_cores,
            page_size,
            total_memory_mb,
        }
    }))
}

static CACHED_PID: OnceLock<i64> = OnceLock::new();

pub fn cached_pid(py: Python<'_>) -> PyResult<i64> {
    Ok(*CACHED_PID.get_or_init_py_attached(py, || {
        os_mod(py)
            .and_then(|o| o.call_method0("getpid")?.extract())
            .unwrap_or(0)
    }))
}

// ── smongo internals ─────────────────────────────────────────────────
cached_module!(
    SMONGO_AGG_STAGES,
    smongo_agg_stages,
    "smongo.aggregation.stages"
);
cached_module!(
    SMONGO_AGG_CONSTANTS,
    smongo_agg_constants,
    "smongo.aggregation.constants"
);
cached_module!(
    SMONGO_AGG_JOINS,
    smongo_agg_joins,
    "smongo.aggregation.joins"
);
cached_module!(
    SMONGO_WIRE_CONTEXT,
    smongo_wire_context,
    "smongo.wire.context"
);
cached_module!(SMONGO_AUDIT, smongo_audit, "smongo.audit");
cached_module!(SMONGO_OBJECTID, smongo_objectid, "smongo.objectid");
cached_module!(
    SMONGO_STORAGE_TXN,
    smongo_storage_txn,
    "smongo.storage.transaction"
);
