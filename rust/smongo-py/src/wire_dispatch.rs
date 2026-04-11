//! Command dispatch infrastructure -- handler lookup, timing, counters, exception mapping.
//!
//! The handler registry (`_HANDLERS` dict) and the `_register` decorator stay
//! in Python. This module provides the dispatch loop, opcounters, and the
//! monotonic logical clock used for `operationTime` / `$clusterTime`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::wire_context::{CachedImports, ConnectionContext};

// ── Opcounters ──────────────────────────────────────────────────────

static OPCOUNTERS: Mutex<Option<HashMap<String, i64>>> = Mutex::new(None);

fn ensure_counters() -> parking_lot::MutexGuard<'static, Option<HashMap<String, i64>>> {
    let mut guard = OPCOUNTERS.lock();
    if guard.is_none() {
        let mut m = HashMap::new();
        for k in &["insert", "query", "update", "delete", "getmore", "command"] {
            m.insert((*k).to_string(), 0);
        }
        *guard = Some(m);
    }
    guard
}

#[pyfunction]
#[allow(clippy::expect_used)]
pub fn inc_counter(name: &str) {
    let mut guard = ensure_counters();
    let map = guard
        .as_mut()
        .expect("opcounters map initialized by ensure_counters");
    *map.entry(name.to_string()).or_insert(0) += 1;
}

#[pyfunction]
#[allow(clippy::expect_used)]
pub fn get_opcounters(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let guard = ensure_counters();
    let map = guard
        .as_ref()
        .expect("opcounters map initialized by ensure_counters");
    let d = PyDict::new(py);
    for (k, v) in map.iter() {
        d.set_item(k, *v)?;
    }
    Ok(d.unbind())
}

// ── Logical clock ───────────────────────────────────────────────────

static LOGICAL_CLOCK: AtomicI64 = AtomicI64::new(0);

#[pyfunction]
pub fn next_timestamp(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let inc = LOGICAL_CLOCK.fetch_add(1, Ordering::Relaxed) + 1;
    let epoch_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let bson_mod = crate::cached_modules::bson_mod(py)?;
    let ts = bson_mod.getattr("Timestamp")?.call1((epoch_secs, inc))?;
    Ok(ts.unbind())
}

// ── Op-kind / top-bucket maps ───────────────────────────────────────
// Note: these map to MongoDB's internal profiler/top naming conventions.
// "delete" → "remove" matches `db.currentOp()` and `top` output where
// the operation kind is historically called "remove."  The *opcounters*
// subsystem (serverStatus) uses "delete" — that naming lives in the
// individual handlers via `inc_counter("delete")`.

fn op_kind_for(cmd: &str) -> &'static str {
    match cmd {
        "find" | "aggregate" | "count" | "distinct" => "query",
        "getMore" => "getmore",
        "insert" => "insert",
        "update" => "update",
        "delete" => "remove",
        "findAndModify" | "findandmodify" | "bulkWrite" => "command",
        _ => "command",
    }
}

fn top_bucket_for(cmd: &str) -> &'static str {
    match cmd {
        "find" | "aggregate" | "count" | "distinct" => "queries",
        "getMore" => "getmore",
        "insert" => "insert",
        "update" => "update",
        "delete" => "remove",
        _ => "commands",
    }
}

// ── Lazy CachedImports builder ──────────────────────────────────────
// Construction is centralized in `CachedImports::from_python()` — see
// `wire_context.rs`.  Both `RustWireServer::new()` and the lazy-init
// path below call that single method.

/// Return the set of command names handled by Rust-native handlers.
/// Used by `test_registry_parity.py` to verify registry alignment
/// without fragile source parsing.
#[pyfunction]
pub fn rust_handler_names(py: Python<'_>) -> PyResult<Py<pyo3::types::PyList>> {
    let handlers = &*crate::wire_commands::RUST_HANDLERS;
    let list = pyo3::types::PyList::empty(py);
    for key in handlers.keys() {
        list.append(*key)?;
    }
    Ok(list.unbind())
}

// ── Dispatch ────────────────────────────────────────────────────────

/// Route a command document to the appropriate handler, with full
/// timing, opcounter, and exception-mapping infrastructure in Rust.
///
/// `handlers` is the Python `_HANDLERS` dict mapping command names to callables.
/// `error_response_fn` and `make_error_fn` are the wire error helpers.
///
/// **Warning:** `command_doc` is mutated in-place (e.g. `$db` injected if
/// absent).  Callers must not assume the dict is unchanged after return.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn rs_dispatch(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    handlers: &Bound<'_, PyDict>,
    command_doc: &Bound<'_, PyDict>,
    doc_sequences: Option<&Bound<'_, PyAny>>,
    make_error_fn: &Bound<'_, PyAny>,
    error_response_fn: &Bound<'_, PyAny>,
    exception_types: &Bound<'_, PyDict>,
    audit_mod: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let ctx_typed: &Bound<'_, ConnectionContext> = ctx.cast()?;

    // Lazy-init CachedImports when the context was not created by
    // RustWireServer (Python wire server path, tests).  This is a
    // no-op when the Rust wire server already set `cached`.
    {
        let needs_init = ctx_typed.borrow().cached.is_none();
        if needs_init {
            let imports = CachedImports::from_python(py)?;
            ctx_typed.borrow_mut().cached = Some(Arc::new(imports));
        }
    }

    // Ensure $db is present
    if command_doc.get_item("$db")?.is_none() {
        command_doc.set_item("$db", "test")?;
    }

    // Touch session
    let lsid = command_doc.get_item("lsid")?;
    if let Some(ref lsid_val) = lsid {
        if !lsid_val.is_none() {
            let sr = ctx_typed.borrow().session_registry.clone_ref(py);
            sr.bind(py).call_method1("touch", (lsid_val,))?;
        }
    }

    // Resolve command name (first key)
    let cmd_name: String = {
        let mut iter = command_doc.keys().try_iter()?;
        match iter.next() {
            Some(Ok(key)) => key.extract::<String>()?,
            _ => {
                return make_error_fn
                    .call1(("CommandNotFound", "empty command document"))
                    .map(|r| r.unbind());
            }
        }
    };

    // Check Rust-native handlers first, then fall back to Python
    let rust_handler = crate::wire_commands::RUST_HANDLERS
        .get(cmd_name.as_str())
        .copied();

    // Verify the command exists in either Rust or Python registries
    if rust_handler.is_none() && handlers.get_item(&cmd_name)?.is_none() {
        return make_error_fn
            .call1(("CommandNotFound", format!("no such command: '{cmd_name}'")))
            .map(|r| r.unbind());
    }

    // Global "command" counter: every dispatched command increments this.
    // Individual handlers additionally increment their op-specific counter
    // (e.g. "query", "insert") — matching MongoDB's opcounters semantics
    // where "command" counts total commands and per-op counters overlap.
    inc_counter("command");

    // Build namespace
    let db_name: String = command_doc
        .get_item("$db")?
        .map(|v| v.extract::<String>().unwrap_or_else(|_| "test".into()))
        .unwrap_or_else(|| "test".into());
    let coll_hint = command_doc.get_item(&cmd_name)?;
    let ns = match &coll_hint {
        Some(v) => {
            if let Ok(s) = v.extract::<String>() {
                if !s.is_empty() {
                    format!("{db_name}.{s}")
                } else {
                    db_name.clone()
                }
            } else {
                db_name.clone()
            }
        }
        None => db_name.clone(),
    };

    // Start op tracking -- extract typed fields once, drop borrow before Python calls
    let op_kind = op_kind_for(&cmd_name);
    let (tracker, conn_id) = {
        let c = ctx_typed.borrow();
        (c.op_tracker.clone_ref(py), c.connection_id)
    };
    let op_id = tracker
        .bind(py)
        .call_method1("start_op", (op_kind, &ns, command_doc, conn_id))?;
    let t0 = Instant::now();

    // Call handler with exception mapping -- Rust handlers take priority.
    let seqs = match doc_sequences {
        Some(s) => s.clone(),
        None => PyDict::new(py).into_any(),
    };
    let handler_result: Result<Bound<'_, PyAny>, PyErr> = if let Some(rs_handler) = rust_handler {
        match rs_handler(py, ctx_typed, command_doc, &seqs) {
            Ok(obj) => {
                let obj: Py<PyAny> = obj;
                Ok(obj.into_bound(py))
            }
            Err(err) => match map_handler_error(
                py,
                &err,
                &cmd_name,
                make_error_fn,
                error_response_fn,
                exception_types,
            ) {
                Ok(resp) => Ok(resp),
                Err(reraise) => Err(reraise),
            },
        }
    } else {
        wire_logger(py)?.call_method1(
            "debug",
            (format!("command '{cmd_name}' handled by Python fallback (no Rust handler)"),),
        )?;
        let handler = handlers.get_item(&cmd_name)?.ok_or_else(|| {
            PyRuntimeError::new_err(format!(
                "command handler missing for '{cmd_name}' (registry inconsistency)"
            ))
        })?;
        match handler.call1((ctx, command_doc, &seqs)) {
            Ok(r) => Ok(r),
            Err(err) => match map_handler_error(
                py,
                &err,
                &cmd_name,
                make_error_fn,
                error_response_fn,
                exception_types,
            ) {
                Ok(resp) => Ok(resp),
                Err(reraise) => Err(reraise),
            },
        }
    };

    // Record timing (always, regardless of success/failure -- mirrors Python finally block).
    // Errors in timing/profiling are logged but never propagate — the command response
    // must still reach the client even if instrumentation fails.
    let elapsed_us = t0.elapsed().as_micros() as i64;
    if let Err(e) = tracker.bind(py).call_method1("finish_op", (&op_id,)) {
        log_internal_error(py, "finish_op", &e);
    }
    let top_bucket = top_bucket_for(&cmd_name);
    {
        let top_stats = ctx_typed.borrow().top_stats.clone_ref(py);
        if let Err(e) = top_stats
            .bind(py)
            .call_method1("record", (&ns, top_bucket, elapsed_us))
        {
            log_internal_error(py, "top_stats.record", &e);
        }
    }
    {
        let (profiler, plan_summary) = {
            let c = ctx_typed.borrow();
            (c.profiler.clone_ref(py), c.last_plan_summary.clone())
        };
        let kwargs = PyDict::new(py);
        let _ = kwargs.set_item("command", command_doc);
        let _ = kwargs.set_item("plan_summary", &plan_summary);
        if let Err(e) =
            profiler
                .bind(py)
                .call_method("log", (op_kind, &ns, elapsed_us / 1000), Some(&kwargs))
        {
            log_internal_error(py, "profiler.log", &e);
        }
    }
    ctx_typed.borrow_mut().last_plan_summary = String::new();

    let resp = handler_result?;

    // Audit logging
    let audit_fallback;
    let audit_ref = match audit_mod {
        Some(m) => m,
        None => {
            audit_fallback = crate::cached_modules::smongo_audit(py)?.into_any();
            &audit_fallback
        }
    };
    let audit_enabled: bool = audit_ref.call_method0("is_enabled")?.extract()?;
    if audit_enabled {
        let user_str: String = ctx_typed
            .borrow()
            .authenticated_user
            .lock()
            .clone()
            .unwrap_or_default();
        let db_name: String = command_doc
            .get_item("$db")?
            .map(|v| v.extract::<String>().unwrap_or_default())
            .unwrap_or_default();
        let addr = ctx_typed.borrow().address.clone_ref(py);
        let remote_str = addr.bind(py).str()?.to_string();
        let resp_dict = resp.cast::<PyDict>();
        let ok_val: f64 = resp_dict
            .ok()
            .and_then(|d| d.get_item("ok").ok().flatten())
            .and_then(|v| {
                v.extract::<f64>()
                    .or_else(|_| v.extract::<i64>().map(|i| i as f64))
                    .ok()
            })
            .unwrap_or(0.0);
        let success = ok_val >= 1.0;
        let duration_ms = elapsed_us as f64 / 1000.0;
        audit_ref.call_method1(
            "log_command",
            (
                user_str,
                db_name,
                &cmd_name,
                &ns,
                remote_str,
                success,
                duration_ms,
            ),
        )?;
    }

    // Always attach operationTime and $clusterTime (mongod does this since 3.6;
    // mongosh, Compass, and drivers like langchain-mongodb expect it).
    {
        let ts = next_timestamp(py)?;
        let ts_bound = ts.bind(py);
        resp.set_item("operationTime", ts_bound)?;

        let bson_mod = crate::cached_modules::bson_mod(py)?;
        let binary_cls = bson_mod.getattr("Binary")?;
        let zero_hash = binary_cls.call1((vec![0u8; 20],))?;
        let sig = PyDict::new(py);
        sig.set_item("hash", zero_hash)?;
        let key_id = crate::cached_modules::bson_int64_cls(py)?.call1((0i64,))?;
        sig.set_item("keyId", key_id)?;
        let cluster_time = PyDict::new(py);
        cluster_time.set_item("clusterTime", ts_bound)?;
        cluster_time.set_item("signature", sig)?;
        resp.set_item("$clusterTime", cluster_time)?;
    }

    Ok(resp.unbind())
}

/// Get a Python logger for ``smongo.wire.commands``.
fn wire_logger<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let log_mod = crate::cached_modules::logging_mod(py)?;
    log_mod.call_method1("getLogger", ("smongo.wire.commands",))
}

/// Best-effort warning for instrumentation failures that must not propagate.
fn log_internal_error(py: Python<'_>, context: &str, err: &PyErr) {
    if let Ok(logger) = wire_logger(py) {
        let msg = format!(
            "dispatch instrumentation error in {context}: {err}",
            err = err.value(py),
        );
        let _ = logger.call_method1("warning", (msg,));
    }
}

fn map_handler_error<'py>(
    py: Python<'py>,
    err: &PyErr,
    cmd_name: &str,
    make_error_fn: &Bound<'py, PyAny>,
    error_response_fn: &Bound<'py, PyAny>,
    exception_types: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyAny>> {
    let msg = err.value(py).str()?.to_string();

    // Domain-specific exception types (registered from Python at init time).
    let ns_err_type = exception_types.get_item("NamespaceError")?;
    let tms_err_type = exception_types.get_item("TooManySessions")?;
    let txn_err_type = exception_types.get_item("TransactionError")?;
    let dup_err_type = exception_types.get_item("DuplicateKeyError")?;
    let val_err_type = exception_types.get_item("ValidationError")?;
    let storage_err_type = exception_types.get_item("StorageError")?;

    if let Some(ty) = ns_err_type {
        if err.is_instance(py, ty.cast()?) {
            return make_error_fn.call1(("InvalidNamespace", &msg));
        }
    }
    if let Some(ty) = tms_err_type {
        if err.is_instance(py, ty.cast()?) {
            return error_response_fn.call1((261i64, "TooManyLogicalSessions", &msg));
        }
    }
    if let Some(ty) = txn_err_type {
        if err.is_instance(py, ty.cast()?) {
            return error_response_fn.call1((251i64, "NoSuchTransaction", &msg));
        }
    }
    if let Some(ty) = dup_err_type {
        if err.is_instance(py, ty.cast()?) {
            return error_response_fn.call1((11000i64, "DuplicateKey", &msg));
        }
    }
    if let Some(ty) = val_err_type {
        if err.is_instance(py, ty.cast()?) {
            return error_response_fn.call1((121i64, "DocumentValidationFailure", &msg));
        }
    }

    // NotImplementedError
    if err.is_instance_of::<pyo3::exceptions::PyNotImplementedError>(py) {
        return make_error_fn.call1(("CommandNotSupported", &msg));
    }

    // StorageError (embedded engine / I/O)
    if let Some(ty) = storage_err_type {
        if err.is_instance(py, ty.cast()?) {
            wire_logger(py)?.call_method1(
                "exception",
                (format!("Storage engine error in command '{cmd_name}'"),),
            )?;
            return error_response_fn.call1((1i64, "InternalError", &msg));
        }
    }

    // Catch-all for common Python exceptions
    if err.is_instance_of::<pyo3::exceptions::PyKeyError>(py)
        || err.is_instance_of::<pyo3::exceptions::PyTypeError>(py)
        || err.is_instance_of::<pyo3::exceptions::PyValueError>(py)
        || err.is_instance_of::<pyo3::exceptions::PyIndexError>(py)
        || err.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py)
        || err.is_instance_of::<pyo3::exceptions::PyOSError>(py)
        || err.is_instance_of::<pyo3::exceptions::PyAttributeError>(py)
    {
        wire_logger(py)?.call_method1(
            "exception",
            (format!("Unhandled error in command '{cmd_name}'"),),
        )?;
        return error_response_fn.call1((1i64, "InternalError", &msg));
    }

    // Unknown exception type -- re-raise to caller
    Err(err.clone_ref(py))
}
