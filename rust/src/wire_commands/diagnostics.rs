//! Wire protocol diagnostic commands: `buildInfo`, `serverStatus`, `collStats`, `dbStats`.
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::wire_context::ConnectionContext;
use crate::wire_errors::make_error;

use super::{bson_int64, dict_get_bool, dict_get_i64, dict_get_str, get_collection, HandlerFn};

fn cmd_conn_status(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let resp = PyDict::new(py);
    let auth = PyDict::new(py);

    let users_list = PyList::empty(py);
    let roles_list = PyList::empty(py);

    {
        let ctx_ref = ctx.borrow();
        let user = ctx_ref.authenticated_user.lock().clone();
        let db = ctx_ref.authenticated_db.lock().clone();
        let roles = ctx_ref.authenticated_roles.lock().clone();
        drop(ctx_ref);

        if let (Some(u), Some(d)) = (user, db) {
            let user_dict = PyDict::new(py);
            user_dict.set_item("user", &u)?;
            user_dict.set_item("db", &d)?;
            users_list.append(user_dict)?;

            for (role, role_db) in &roles {
                let role_dict = PyDict::new(py);
                role_dict.set_item("role", role)?;
                role_dict.set_item("db", role_db)?;
                roles_list.append(role_dict)?;
            }
        }
    }

    auth.set_item("authenticatedUsers", users_list)?;
    auth.set_item("authenticatedUserRoles", roles_list)?;
    resp.set_item("authInfo", auth)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_explain(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let inner = cmd.get_item("explain")?;
    if let Some(inner) = inner {
        if let Ok(inner_dict) = inner.cast::<PyDict>() {
            let db_name = dict_get_str(cmd, "$db", "test")?;
            let coll_name = inner_dict
                .get_item("find")?
                .or(inner_dict.get_item("aggregate")?)
                .and_then(|v| v.extract::<String>().ok())
                .unwrap_or_default();
            if !coll_name.is_empty() {
                let coll = get_collection(ctx, &db_name, &coll_name)?;
                let raw_filter = inner_dict
                    .get_item("filter")?
                    .unwrap_or_else(|| PyDict::new(py).into_any());
                let plan = coll.call_method1("explain", (raw_filter,))?;
                let resp = PyDict::new(py);
                resp.set_item("queryPlanner", plan)?;
                resp.set_item("ok", 1.0)?;
                return Ok(resp.into_any().unbind());
            }
        }
    }
    let wp = PyDict::new(py);
    wp.set_item("stage", "UNKNOWN")?;
    let qp = PyDict::new(py);
    qp.set_item("winningPlan", wp)?;
    let resp = PyDict::new(py);
    resp.set_item("queryPlanner", qp)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_current_op(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (tracker, conn_id) = {
        let c = ctx.borrow();
        (c.op_tracker.clone_ref(py), c.connection_id)
    };
    let ops = tracker.bind(py).call_method0("active_ops")?;
    let all_flag = dict_get_bool(cmd, "$all", false)?;
    let filtered = if !all_flag {
        let result = PyList::empty(py);
        for o in ops.cast::<PyList>()?.iter() {
            let o_conn = o.get_item("connectionId")?;
            if o_conn.eq(conn_id)? {
                result.append(o)?;
            }
        }
        result.into_any()
    } else {
        ops
    };
    let resp = PyDict::new(py);
    resp.set_item("inprog", filtered)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_kill_op(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let op_id = match cmd.get_item("op")? {
        Some(v) => v,
        None => {
            let r = make_error(py, "InvalidOptions", "killOp requires 'op' field")?;
            return Ok(r.into_any().unbind());
        }
    };
    let tracker = ctx.borrow().op_tracker.clone_ref(py);
    let op_int: i64 = op_id.extract()?;
    let killed = tracker.bind(py).call_method1("kill_op", (op_int,))?;
    let resp = PyDict::new(py);
    if !killed.is_truthy()? {
        resp.set_item("info", format!("operation {op_int} not found"))?;
    } else {
        resp.set_item("info", format!("attempting to kill op {op_int}"))?;
    }
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_top(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let top_stats = ctx.borrow().top_stats.clone_ref(py);
    let snap = top_stats.bind(py).call_method0("snapshot")?;
    let resp = PyDict::new(py);
    resp.set_item("totals", snap)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_profile(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let profiler = ctx.borrow().profiler.clone_ref(py);
    let profiler = profiler.bind(py);
    let old_level = profiler.getattr("level")?;
    let old_slow = profiler.getattr("slow_ms")?;

    if let Some(new_level) = cmd.get_item("profile")? {
        if let Ok(level) = new_level.extract::<i32>() {
            if (0..=2).contains(&level) {
                profiler.setattr("level", level)?;
            }
        }
    }
    if let Some(slow_ms) = cmd.get_item("slowms")? {
        if let Ok(ms) = slow_ms.extract::<i64>() {
            profiler.setattr("slow_ms", ms)?;
        }
    }

    let resp = PyDict::new(py);
    resp.set_item("was", old_level)?;
    resp.set_item("slowms", old_slow)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_read_profile(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let limit = dict_get_i64(cmd, "limit", 100)?;
    let profiler = ctx.borrow().profiler.clone_ref(py);
    let entries = profiler.bind(py).call_method1("get_entries", (limit,))?;
    let cursor_dict = PyDict::new(py);
    cursor_dict.set_item("id", bson_int64(py, 0)?)?;
    cursor_dict.set_item("ns", "admin.system.profile")?;
    cursor_dict.set_item("firstBatch", entries)?;
    let resp = PyDict::new(py);
    resp.set_item("cursor", cursor_dict)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

pub(crate) fn register(m: &mut HashMap<&'static str, HandlerFn>) {
    m.insert("connectionStatus", cmd_conn_status);
    m.insert("explain", cmd_explain);
    m.insert("currentOp", cmd_current_op);
    m.insert("killOp", cmd_kill_op);
    m.insert("top", cmd_top);
    m.insert("profile", cmd_profile);
    m.insert("setProfilingLevel", cmd_profile);
    m.insert("system.profile", cmd_read_profile);
}
