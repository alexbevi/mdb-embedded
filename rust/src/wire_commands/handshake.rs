//! Wire protocol handshake commands: `hello`, `ismaster`, `ping`, `whatsmyuri`,
//! `saslStart`, `saslContinue`, `logout`.
use std::collections::HashMap;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use crate::wire_context::ConnectionContext;
use crate::wire_errors::make_error;

use super::{bson_int64, dict_get_bool, dict_get_str, ok_dict, HandlerFn};

fn resolve_audit_mod(py: Python<'_>, ctx: &Bound<'_, ConnectionContext>) -> PyResult<Py<PyAny>> {
    let ctx_ref = ctx.borrow();
    if let Ok(cached) = ctx_ref.cached_imports() {
        return Ok(cached.audit_mod.clone_ref(py));
    }
    drop(ctx_ref);
    Ok(crate::cached_modules::smongo_audit(py)?.into_any().unbind())
}

fn cmd_hello(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let server_compressors = crate::wire_msg::available_compressors();
    let compressor_id_map = &*crate::wire_msg::COMPRESSOR_IDS;

    let client_compressors = match cmd.get_item("compression")? {
        Some(v) => v,
        None => PyList::empty(py).into_any(),
    };

    let mut compressors: Vec<String> = Vec::new();
    if let Ok(list) = client_compressors.cast::<PyList>() {
        for item in list.iter() {
            let name: String = item.extract()?;
            if server_compressors.contains(&name) {
                compressors.push(name);
            }
        }
    }

    let compressor_id = ctx.borrow().compressor_id.clone_ref(py);
    if !compressors.is_empty() && compressor_id.bind(py).is_none() {
        if let Some(&cid) = compressor_id_map.get(compressors[0].as_str()) {
            ctx.borrow_mut().compressor_id = cid.into_pyobject(py)?.into_any().unbind();
        }
    }

    let topology_pid = ctx.borrow().cached_imports()?.topology_pid.clone_ref(py);

    let utc = crate::cached_modules::datetime_tz_utc(py)?;
    let now = crate::cached_modules::datetime_datetime_cls(py)?.call_method1("now", (&utc,))?;

    let conn_id = ctx.borrow().connection_id;

    let resp = PyDict::new(py);
    resp.set_item("ismaster", true)?;
    resp.set_item("isWritablePrimary", true)?;
    let topo = PyDict::new(py);
    topo.set_item("processId", topology_pid.bind(py))?;
    topo.set_item("counter", bson_int64(py, 0)?)?;
    resp.set_item("topologyVersion", topo)?;
    resp.set_item("maxBsonObjectSize", 16 * 1024 * 1024)?;
    resp.set_item("maxMessageSizeBytes", 48 * 1024 * 1024)?;
    resp.set_item("maxWriteBatchSize", 100_000)?;
    resp.set_item("localTime", now)?;
    resp.set_item("logicalSessionTimeoutMinutes", 30)?;
    resp.set_item("connectionId", conn_id)?;
    resp.set_item("minWireVersion", 0)?;
    resp.set_item("maxWireVersion", 21)?;
    resp.set_item("readOnly", false)?;
    resp.set_item("ok", 1.0)?;

    if !compressors.is_empty() {
        let py_list = PyList::new(py, &compressors)?;
        resp.set_item("compression", py_list)?;
    }
    if dict_get_bool(cmd, "helloOk", false)? {
        resp.set_item("helloOk", true)?;
    }
    if let Some(ssm) = cmd.get_item("saslSupportedMechs")? {
        let ssm_str: String = ssm.extract().unwrap_or_default();
        let store = ctx.borrow().cached_imports()?.user_store.clone_ref(py);
        let store = store.bind(py);
        let user_doc = store.call_method1("get", (&ssm_str,))?;
        if !user_doc.is_none() {
            if let Ok(mechs) = user_doc.get_item("mechanisms") {
                resp.set_item("saslSupportedMechs", mechs)?;
            } else {
                let mechs = PyList::new(py, ["SCRAM-SHA-256"])?;
                resp.set_item("saslSupportedMechs", mechs)?;
            }
        } else {
            let mechs = PyList::new(py, ["SCRAM-SHA-256"])?;
            resp.set_item("saslSupportedMechs", mechs)?;
        }
    }
    Ok(resp.into_any().unbind())
}

fn cmd_ping(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let d = ok_dict(py)?;
    Ok(d.into_any().unbind())
}

fn cmd_build_info(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let si = crate::cached_modules::system_info(py)?;
    let git_version = ctx.borrow().cached_imports()?.git_version.clone_ref(py);

    let sys_info = format!("{} {} {}", si.system, si.release, si.machine);

    let resp = PyDict::new(py);
    resp.set_item("version", "7.0.0-smongo")?;
    resp.set_item("gitVersion", git_version)?;
    resp.set_item("sysInfo", sys_info)?;
    resp.set_item("versionArray", (7, 0, 0, 0))?;
    resp.set_item("bits", 64)?;
    let modules = PyList::new(py, ["embedded", "wiredtiger"])?;
    resp.set_item("modules", modules)?;
    resp.set_item("allocator", "system")?;
    resp.set_item("javascriptEngine", "none")?;
    let openssl = PyDict::new(py);
    openssl.set_item("running", "disabled")?;
    resp.set_item("openssl", openssl)?;
    let build_env = PyDict::new(py);
    build_env.set_item("cc", "")?;
    build_env.set_item("cxx", "")?;
    build_env.set_item("ccflags", "")?;
    build_env.set_item("cxxflags", "")?;
    build_env.set_item("target_arch", &si.machine)?;
    build_env.set_item("target_os", si.system.to_lowercase())?;
    resp.set_item("buildEnvironment", build_env)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_host_info(
    py: Python<'_>,
    _ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let si = crate::cached_modules::system_info(py)?;
    let utc = crate::cached_modules::datetime_tz_utc(py)?;
    let now = crate::cached_modules::datetime_datetime_cls(py)?.call_method1("now", (&utc,))?;

    let resp = PyDict::new(py);
    let sys_dict = PyDict::new(py);
    sys_dict.set_item("currentTime", now)?;
    sys_dict.set_item("hostname", &si.node)?;
    sys_dict.set_item("cpuAddrSize", 64)?;
    sys_dict.set_item("cpuArch", &si.machine)?;
    sys_dict.set_item("numCores", si.num_cores)?;
    sys_dict.set_item("memSizeMB", si.total_memory_mb)?;
    resp.set_item("system", sys_dict)?;

    let os_dict = PyDict::new(py);
    os_dict.set_item("type", &si.system)?;
    os_dict.set_item("name", &si.platform_name)?;
    os_dict.set_item("version", &si.release)?;
    resp.set_item("os", os_dict)?;

    let extra = PyDict::new(py);
    extra.set_item("pageSize", si.page_size)?;
    resp.set_item("extra", extra)?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_whatsmyuri(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let address = ctx.borrow().address.clone_ref(py);
    let address = address.into_bound(py);
    let host: String = address.get_item(0)?.extract()?;
    let port: i64 = address.get_item(1)?.extract()?;
    let resp = PyDict::new(py);
    resp.set_item("you", format!("{host}:{port}"))?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn cmd_sasl_start(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let mechanism = dict_get_str(cmd, "mechanism", "")?;
    if mechanism != "SCRAM-SHA-256" {
        let r = make_error(
            py,
            "AuthenticationFailed",
            &format!("unsupported SASL mechanism: {mechanism}"),
        )?;
        return Ok(r.into_any().unbind());
    }

    let payload: Vec<u8> = match cmd.get_item("payload")?.map(|v| v.extract()).transpose()? {
        Some(p) => p,
        None => {
            let r = make_error(
                py,
                "AuthenticationFailed",
                "saslStart requires a payload; connect without credentials if authentication is not needed",
            )?;
            return Ok(r.into_any().unbind());
        }
    };

    let db_name = dict_get_str(cmd, "$db", "admin")?;

    let username = crate::scram::parse_username(&payload)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let store = ctx.borrow().cached_imports()?.user_store.clone_ref(py);
    let store = store.bind(py);
    let key = format!("{db_name}.{username}");
    let user_doc = store.call_method1("get", (&key,))?;

    if user_doc.is_none() {
        let r = make_error(
            py,
            "AuthenticationFailed",
            &format!("user \"{username}\" not found in \"{db_name}\""),
        )?;
        return Ok(r.into_any().unbind());
    }

    let creds = user_doc.call_method1("get", ("credentials",))?;
    if creds.is_none() {
        let r = make_error(py, "AuthenticationFailed", "user has no credentials")?;
        return Ok(r.into_any().unbind());
    }
    let scram_cred_py = creds.call_method1("get", ("SCRAM-SHA-256",))?;
    if scram_cred_py.is_none() {
        let r = make_error(py, "AuthenticationFailed", "user has no SCRAM-SHA-256 credentials")?;
        return Ok(r.into_any().unbind());
    }

    let credential = extract_scram_credential(py, &scram_cred_py)?;

    let conversation = crate::scram::ScramConversation::from_client_first(&payload, &credential)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let server_first = conversation.server_first_message();

    {
        let ctx_ref = ctx.borrow();
        let mut guard = ctx_ref.scram_conversation.lock();
        *guard = Some(conversation);
    }

    let resp = PyDict::new(py);
    resp.set_item("conversationId", 1)?;
    resp.set_item("done", false)?;
    resp.set_item("payload", PyBytes::new(py, &server_first))?;
    resp.set_item("ok", 1.0)?;
    Ok(resp.into_any().unbind())
}

fn extract_scram_credential(
    _py: Python<'_>,
    cred_py: &Bound<'_, PyAny>,
) -> PyResult<crate::scram::ScramCredential> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;

    let salt_b64: String = cred_py.get_item("salt")?.extract()?;
    let stored_key_b64: String = cred_py.get_item("storedKey")?.extract()?;
    let server_key_b64: String = cred_py.get_item("serverKey")?.extract()?;
    let iteration_count: u32 = cred_py.get_item("iterationCount")?.extract()?;

    let salt = b64
        .decode(&salt_b64)
        .map_err(|e| PyRuntimeError::new_err(format!("invalid salt base64: {e}")))?;
    let stored_key_vec = b64
        .decode(&stored_key_b64)
        .map_err(|e| PyRuntimeError::new_err(format!("invalid storedKey base64: {e}")))?;
    let server_key_vec = b64
        .decode(&server_key_b64)
        .map_err(|e| PyRuntimeError::new_err(format!("invalid serverKey base64: {e}")))?;

    if stored_key_vec.len() != 32 || server_key_vec.len() != 32 {
        return Err(PyRuntimeError::new_err("stored/server key must be 32 bytes"));
    }

    let mut stored_key = [0u8; 32];
    let mut server_key = [0u8; 32];
    stored_key.copy_from_slice(&stored_key_vec);
    server_key.copy_from_slice(&server_key_vec);

    Ok(crate::scram::ScramCredential {
        salt,
        stored_key,
        server_key,
        iteration_count,
    })
}

fn cmd_sasl_continue(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let payload: Vec<u8> = cmd
        .get_item("payload")?
        .map(|v| v.extract())
        .transpose()?
        .ok_or_else(|| PyRuntimeError::new_err("saslContinue missing payload"))?;

    let db_name = dict_get_str(cmd, "$db", "admin")?;

    let conversation = {
        let ctx_ref = ctx.borrow();
        let mut guard = ctx_ref.scram_conversation.lock();
        guard.take()
    };

    let conversation = match conversation {
        Some(c) => c,
        None => {
            let r = make_error(py, "AuthenticationFailed", "no active SCRAM conversation")?;
            return Ok(r.into_any().unbind());
        }
    };

    match conversation.verify_client_final(&payload) {
        Ok(server_final) => {
            let username = conversation.username.clone();
            {
                let ctx_ref = ctx.borrow();
                let mut ug = ctx_ref.authenticated_user.lock();
                *ug = Some(username.clone());
                drop(ug);
                let mut dg = ctx_ref.authenticated_db.lock();
                *dg = Some(db_name.clone());
            }

            let store = ctx.borrow().cached_imports()?.user_store.clone_ref(py);
            let store = store.bind(py);
            let key = format!("{db_name}.{username}");
            let user_doc = store.call_method1("get", (&key,))?;
            let mut role_pairs: Vec<(String, String)> = Vec::new();
            if !user_doc.is_none() {
                let roles_val = user_doc.call_method1("get", ("roles", PyList::empty(py)))?;
                if let Ok(roles_list) = roles_val.cast::<PyList>() {
                    for item in roles_list.iter() {
                        if let Ok(d) = item.cast::<PyDict>() {
                            let role: String = d.get_item("role")?
                                .map(|v| v.extract()).transpose()?.unwrap_or_default();
                            let rdb: String = d.get_item("db")?
                                .map(|v| v.extract()).transpose()?.unwrap_or_default();
                            if !role.is_empty() {
                                role_pairs.push((role, rdb));
                            }
                        } else if let Ok(s) = item.extract::<String>() {
                            if !s.is_empty() {
                                role_pairs.push((s, db_name.clone()));
                            }
                        }
                    }
                }
            }
            {
                let ctx_ref = ctx.borrow();
                let mut rg = ctx_ref.authenticated_roles.lock();
                *rg = role_pairs;
            }

            let audit_mod = resolve_audit_mod(py, ctx)?;
            let audit_on: bool = audit_mod.call_method0(py, "is_enabled")?.extract(py)?;
            if audit_on {
                let addr = ctx.borrow().address.clone_ref(py);
                let remote: String = addr.bind(py).str().map(|s| s.to_string()).unwrap_or_default();
                audit_mod.call_method1(
                    py,
                    "log_auth_event",
                    ("authenticate", &username, &db_name, &remote, true, ""),
                )?;
            }

            let resp = PyDict::new(py);
            resp.set_item("conversationId", 1)?;
            resp.set_item("done", true)?;
            resp.set_item("payload", PyBytes::new(py, &server_final))?;
            resp.set_item("ok", 1.0)?;
            Ok(resp.into_any().unbind())
        }
        Err(e) => {
            let audit_mod = resolve_audit_mod(py, ctx)?;
            let audit_on: bool = audit_mod.call_method0(py, "is_enabled")?.extract(py)?;
            if audit_on {
                let addr = ctx.borrow().address.clone_ref(py);
                let remote: String = addr.bind(py).str().map(|s| s.to_string()).unwrap_or_default();
                let reason = format!("SCRAM verification failed: {e}");
                audit_mod.call_method1(
                    py,
                    "log_auth_event",
                    ("authFailure", &conversation.username, &db_name, &remote, false, &reason),
                )?;
            }

            let r = make_error(
                py,
                "AuthenticationFailed",
                &format!("SCRAM verification failed: {e}"),
            )?;
            Ok(r.into_any().unbind())
        }
    }
}

fn cmd_logout(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,
    _cmd: &Bound<'_, PyDict>,
    _seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let (user, db) = {
        let ctx_ref = ctx.borrow();
        let user = ctx_ref.authenticated_user.lock().clone();
        let db = ctx_ref.authenticated_db.lock().clone();
        let mut ug = ctx_ref.authenticated_user.lock();
        *ug = None;
        drop(ug);
        let mut dg = ctx_ref.authenticated_db.lock();
        *dg = None;
        drop(dg);
        let mut rg = ctx_ref.authenticated_roles.lock();
        rg.clear();
        (user, db)
    };

    let audit_mod = resolve_audit_mod(py, ctx)?;
    let audit_on: bool = audit_mod.call_method0(py, "is_enabled")?.extract(py)?;
    if audit_on {
        let addr = ctx.borrow().address.clone_ref(py);
        let remote: String = addr.bind(py).str().map(|s| s.to_string()).unwrap_or_default();
        let u = user.unwrap_or_default();
        let d = db.unwrap_or_default();
        audit_mod.call_method1(py, "log_auth_event", ("logout", &u, &d, &remote, true, ""))?;
    }
    Ok(ok_dict(py)?.into_any().unbind())
}

pub(crate) fn register(m: &mut HashMap<&'static str, HandlerFn>) {
    m.insert("hello", cmd_hello);
    m.insert("ismaster", cmd_hello);
    m.insert("isMaster", cmd_hello);
    m.insert("ping", cmd_ping);
    m.insert("buildInfo", cmd_build_info);
    m.insert("buildinfo", cmd_build_info);
    m.insert("hostInfo", cmd_host_info);
    m.insert("whatsmyuri", cmd_whatsmyuri);
    m.insert("saslStart", cmd_sasl_start);
    m.insert("saslContinue", cmd_sasl_continue);
    m.insert("logout", cmd_logout);
}
