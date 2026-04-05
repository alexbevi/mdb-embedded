//! Wire protocol session commands: `startSession`, `endSessions`, `killSessions`, `abortTransaction`, `commitTransaction`.
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::wire_context::ConnectionContext;
use crate::wire_errors::make_error;

use super::{ok_dict, HandlerFn};

fn cmd_start_session(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let sr = ctx.borrow().session_registry.clone_ref(py);
    let sid = sr.bind(py).call_method0("create")?;
    let binary_cls = crate::cached_modules::bson_mod(py)?.getattr("Binary")?;
    let sid_bytes = sid.getattr("bytes")?;
    let binary = binary_cls.call1((sid_bytes, 4))?;
    let id_dict = PyDict::new(py);
    id_dict.set_item("id", binary)?;
    let resp = PyDict::new(py);
    resp.set_item("id", id_dict)?;
    resp.set_item("timeoutMinutes", 30)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_end_sessions(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let sids = cmd
        .get_item("endSessions")?
        .unwrap_or_else(|| PyList::empty(py).into_any());
    let sr = ctx.borrow().session_registry.clone_ref(py);
    sr.bind(py).call_method1("end", (&sids,))?;
    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_refresh_sessions(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let sids = cmd
        .get_item("refreshSessions")?
        .unwrap_or_else(|| PyList::empty(py).into_any());
    let sr = ctx.borrow().session_registry.clone_ref(py);
    sr.bind(py).call_method1("refresh", (&sids,))?;
    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_kill_sessions(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let sids = cmd
        .get_item("killSessions")?
        .unwrap_or_else(|| PyList::empty(py).into_any());
    let sr = ctx.borrow().session_registry.clone_ref(py);
    sr.bind(py).call_method1("kill", (&sids,))?;
    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_kill_all_sessions(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let sr = ctx.borrow().session_registry.clone_ref(py);
    sr.bind(py).call_method0("expire_all")?;
    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_abort_txn(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let lsid = match cmd.get_item("lsid")? {
        Some(v) if !v.is_none() => v,
        _ => {
            let r = make_error(py, "InvalidOptions", "abortTransaction requires lsid")?;
            return Ok(r.into_any().unbind());
        }
    };
    ctx.call_method1("abort_transaction", (&lsid,))?;
    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_commit_txn(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let lsid = match cmd.get_item("lsid")? {
        Some(v) if !v.is_none() => v,
        _ => {
            let r = make_error(py, "InvalidOptions", "commitTransaction requires lsid")?;
            return Ok(r.into_any().unbind());
        }
    };
    ctx.call_method1("commit_transaction", (&lsid,))?;
    Ok(ok_dict(py)?.into_any().unbind())
}

fn cmd_start_txn(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let lsid = match cmd.get_item("lsid")? {
        Some(v) if !v.is_none() => v,
        _ => {
            let r = make_error(py, "InvalidOptions", "startTransaction requires lsid")?;
            return Ok(r.into_any().unbind());
        }
    };
    ctx.call_method1("start_transaction", (&lsid,))?;
    Ok(ok_dict(py)?.into_any().unbind())
}

pub(crate) fn register(m: &mut HashMap<&'static str, HandlerFn>) {
    m.insert("startSession", cmd_start_session);
    m.insert("endSessions", cmd_end_sessions);
    m.insert("refreshSessions", cmd_refresh_sessions);
    m.insert("killSessions", cmd_kill_sessions);
    m.insert("killAllSessions", cmd_kill_all_sessions);
    m.insert("abortTransaction", cmd_abort_txn);
    m.insert("commitTransaction", cmd_commit_txn);
    m.insert("startTransaction", cmd_start_txn);
}
