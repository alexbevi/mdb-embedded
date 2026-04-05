//! Tokio-based TCP wire protocol server replacing the Python threading server.
//!
//! Each connection is handled as a Tokio task. The server acquires the GIL
//! only for command dispatch (which calls into Python storage via PyO3).
//! Wire framing, header parsing, and response encoding happen in Rust without
//! holding the GIL.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::wrap_pyfunction;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use crate::wire_context::ConnectionContext;

const CONNECTION_TIMEOUT_SEC: u64 = 300;
const HEADER_SIZE: usize = 16;
const MAX_MSG_SIZE: usize = 48 * 1024 * 1024;
const OP_QUERY: i32 = 2004;
const OP_COMPRESSED: i32 = 2012;
const OP_MSG: i32 = 2013;

struct ServerState {
    shutdown: AtomicBool,
    conn_semaphore: Arc<Semaphore>,
    conn_id_gen: AtomicI64,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    auth_required: bool,
    // Python objects shared across connections
    local_client: Py<PyAny>,
    cursor_registry: Py<PyAny>,
    session_registry: Py<PyAny>,
    op_tracker: Py<PyAny>,
    param_store: Py<PyAny>,
    top_stats: Py<PyAny>,
    profiler: Py<PyAny>,
    log_buffer: Py<PyAny>,
    conn_counter: Py<PyAny>,
    free_monitoring: Py<PyAny>,
    sync_mgr: Py<PyAny>,
    // Cached dispatch infrastructure (initialized once, avoids per-message py.import())
    handlers: Py<PyDict>,
    make_error_fn: Py<PyAny>,
    error_response_fn: Py<PyAny>,
    exception_types: Py<PyDict>,
    // Cached module-level Python refs (avoids per-command py.import())
    cached_imports: Arc<crate::wire_context::CachedImports>,
    audit_mod: Py<PyAny>,
}

/// Tokio TCP server: wire framing in Rust, command dispatch into Python via PyO3.
#[pyclass(module = "smongo._smongo_core")]
pub struct RustWireServer {
    state: Option<Arc<ServerState>>,
    host: String,
    port: u16,
    runtime: Option<tokio::runtime::Runtime>,
    owns_sync_mgr: bool,
    _owns_local_client: bool,
}

#[pymethods]
impl RustWireServer {
    #[new]
    #[pyo3(signature = (
        host, port, local_client, cursor_registry, session_registry,
        op_tracker, param_store, top_stats, profiler, log_buffer,
        conn_counter, free_monitoring, sync_mgr, max_connections=1024,
        owns_sync_mgr=false, owns_local_client=false,
        tls_cert_file=None, tls_key_file=None, auth_required=false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        host: String,
        port: u16,
        local_client: &Bound<'_, PyAny>,
        cursor_registry: &Bound<'_, PyAny>,
        session_registry: &Bound<'_, PyAny>,
        op_tracker: &Bound<'_, PyAny>,
        param_store: &Bound<'_, PyAny>,
        top_stats: &Bound<'_, PyAny>,
        profiler: &Bound<'_, PyAny>,
        log_buffer: &Bound<'_, PyAny>,
        conn_counter: &Bound<'_, PyAny>,
        free_monitoring: &Bound<'_, PyAny>,
        sync_mgr: &Bound<'_, PyAny>,
        max_connections: usize,
        owns_sync_mgr: bool,
        owns_local_client: bool,
        tls_cert_file: Option<String>,
        tls_key_file: Option<String>,
        auth_required: bool,
    ) -> PyResult<Self> {
        let tls_acceptor = match (&tls_cert_file, &tls_key_file) {
            (Some(cert), Some(key)) => Some(build_tls_acceptor(cert, key)?),
            (Some(_), None) | (None, Some(_)) => {
                return Err(PyRuntimeError::new_err(
                    "both tls_cert_file and tls_key_file are required for TLS",
                ));
            }
            _ => None,
        };

        // Cache dispatch infrastructure: handler registry, error helpers, exception types.
        let registry_mod = py.import("smongo.wire.commands._registry")?;
        let handlers: Py<PyDict> = registry_mod.getattr("_HANDLERS")?.cast::<PyDict>()?.clone().unbind();

        let make_error_fn = wrap_pyfunction!(crate::wire_errors::make_error, py)?.into_any().unbind();
        let error_response_fn = wrap_pyfunction!(crate::wire_errors::error_response, py)?.into_any().unbind();

        let exc_types = PyDict::new(py);
        exc_types.set_item("NamespaceError", py.get_type::<crate::wire_context::NamespaceError>())?;
        exc_types.set_item("TooManySessions", py.get_type::<crate::wire_sessions::TooManySessions>())?;
        exc_types.set_item("TransactionError", py.get_type::<crate::wire_transactions::TransactionError>())?;
        exc_types.set_item("DuplicateKeyError", py.get_type::<crate::index_manager::DuplicateKeyError>())?;
        exc_types.set_item("ValidationError", py.get_type::<crate::schema::ValidationError>())?;
        if let Ok(compat_mod) = py.import("smongo._compat") {
            if let Ok(wte) = compat_mod.getattr("WTError") {
                exc_types.set_item("WTError", wte)?;
            }
        }
        let exception_types = exc_types.unbind();

        // Cache module-level Python refs used by command handlers.
        let topology_pid = registry_mod.getattr("_TOPOLOGY_PROCESS_ID")?.unbind();
        let git_version = registry_mod.getattr("_GIT_VERSION")?.unbind();
        let server_start: f64 = registry_mod.getattr("_SERVER_START")?.extract()?;
        let help_dict = registry_mod.getattr("_HELP")?.unbind();

        let users_mod = py.import("smongo.wire.commands.users")?;
        let user_store = users_mod.getattr("_USER_STORE")?.unbind();
        let user_store_lock = users_mod.getattr("_USER_STORE_LOCK")?.unbind();

        let audit_mod: Py<PyAny> = py.import("smongo.audit")?.into_any().unbind();

        let cached_imports = Arc::new(crate::wire_context::CachedImports {
            user_store,
            user_store_lock,
            audit_mod: audit_mod.clone_ref(py),
            topology_pid,
            git_version,
            server_start,
            help_dict,
            handlers: handlers.clone_ref(py),
        });

        let state = Arc::new(ServerState {
            shutdown: AtomicBool::new(false),
            conn_semaphore: Arc::new(Semaphore::new(max_connections)),
            conn_id_gen: AtomicI64::new(1),
            tls_acceptor,
            auth_required,
            local_client: local_client.clone().unbind(),
            cursor_registry: cursor_registry.clone().unbind(),
            session_registry: session_registry.clone().unbind(),
            op_tracker: op_tracker.clone().unbind(),
            param_store: param_store.clone().unbind(),
            top_stats: top_stats.clone().unbind(),
            profiler: profiler.clone().unbind(),
            log_buffer: log_buffer.clone().unbind(),
            conn_counter: conn_counter.clone().unbind(),
            free_monitoring: free_monitoring.clone().unbind(),
            sync_mgr: if sync_mgr.is_none() { py.None() } else { sync_mgr.clone().unbind() },
            handlers,
            make_error_fn,
            error_response_fn,
            exception_types,
            cached_imports,
            audit_mod,
        });

        Ok(Self {
            state: Some(state),
            host,
            port,
            runtime: None,
            owns_sync_mgr,
            _owns_local_client: owns_local_client,
        })
    }

    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("server not initialized"))?
            .clone();
        state.shutdown.store(false, Ordering::SeqCst);

        // Start reapers and sync in Python
        let cr = state.cursor_registry.bind(py);
        cr.call_method0("start_reaper")?;
        let sr = state.session_registry.bind(py);
        sr.call_method0("start_reaper")?;
        let lb = state.log_buffer.bind(py);
        lb.call_method0("install")?;

        if self.owns_sync_mgr {
            let sm = state.sync_mgr.bind(py);
            if !sm.is_none() {
                sm.call_method0("start")?;
            }
        }

        let host = self.host.clone();
        let port = self.port;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("tokio runtime: {e}")))?;

        rt.spawn(async move {
            if let Err(e) = accept_loop(state, &host, port).await {
                log::error!("Wire server accept loop error: {e}");
            }
        });

        self.runtime = Some(rt);
        Ok(())
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(ref state) = self.state {
            state.shutdown.store(true, Ordering::SeqCst);

            let cr = state.cursor_registry.bind(py);
            cr.call_method0("stop_reaper")?;
            let sr = state.session_registry.bind(py);
            sr.call_method0("stop_reaper")?;

            if self.owns_sync_mgr {
                let sm = state.sync_mgr.bind(py);
                if !sm.is_none() {
                    let _ = sm.call_method0("stop");
                }
            }
        }

        if let Some(rt) = self.runtime.take() {
            rt.shutdown_timeout(std::time::Duration::from_secs(3));
        }
        Ok(())
    }

    fn serve_forever(&mut self, py: Python<'_>) -> PyResult<()> {
        self.start(py)?;
        loop {
            py.check_signals()?;
            std::thread::sleep(std::time::Duration::from_millis(100));
            if let Some(ref state) = self.state {
                if state.shutdown.load(Ordering::SeqCst) {
                    break;
                }
            }
        }
        self.stop(py)
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<Self>> {
        slf.bind(py).borrow_mut().start(py)?;
        Ok(slf)
    }

    fn __exit__(&mut self, py: Python<'_>, _exc_type: &Bound<'_, PyAny>, _exc_val: &Bound<'_, PyAny>, _exc_tb: &Bound<'_, PyAny>) -> PyResult<bool> {
        self.stop(py)?;
        Ok(false)
    }
}

async fn accept_loop(state: Arc<ServerState>, host: &str, port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(format!("{host}:{port}")).await?;

    loop {
        if state.shutdown.load(Ordering::Relaxed) {
            break;
        }

        let accept_result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            listener.accept(),
        ).await;

        let (stream, addr) = match accept_result {
            Ok(Ok((s, a))) => (s, a),
            Ok(Err(e)) => {
                if !state.shutdown.load(Ordering::Relaxed) {
                    log::warn!("Accept error: {e}");
                }
                continue;
            }
            Err(_) => continue, // timeout, check shutdown
        };

        let permit = match state.conn_semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                // Max connections: send error and close
                drop(stream);
                continue;
            }
        };

        let conn_id = state.conn_id_gen.fetch_add(1, Ordering::Relaxed);
        let state_clone = state.clone();

        if let Some(ref tls_acc) = state.tls_acceptor {
            let tls = tls_acc.clone();
            tokio::spawn(async move {
                let _permit = permit;
                match tls.accept(stream).await {
                    Ok(tls_stream) => {
                        connection_loop(state_clone, tls_stream, addr, conn_id).await;
                    }
                    Err(e) => {
                        log::warn!("TLS handshake failed for {addr}: {e}");
                    }
                }
            });
        } else {
            tokio::spawn(async move {
                let _permit = permit;
                connection_loop(state_clone, stream, addr, conn_id).await;
            });
        }
    }

    Ok(())
}

async fn connection_loop<S: AsyncRead + AsyncWrite + Unpin + Send>(
    state: Arc<ServerState>,
    mut stream: S,
    addr: std::net::SocketAddr,
    conn_id: i64,
) {
    // Create a persistent ConnectionContext for this connection so that auth
    // state, DB cache, and transaction sessions survive across messages.
    let ctx_handle: Option<Py<ConnectionContext>> = Python::attach(|py| {
        let cc = state.conn_counter.bind(py);
        let _ = cc.call_method0("connect");
        create_connection_context(py, &state, conn_id, addr)
            .ok()
            .map(|ctx| ctx.unbind())
    });

    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut req_id_gen: i64 = 1;

    loop {
        if state.shutdown.load(Ordering::Relaxed) {
            break;
        }

        let timeout = tokio::time::timeout(
            std::time::Duration::from_secs(CONNECTION_TIMEOUT_SEC),
            stream.read_buf(&mut buf),
        );

        match timeout.await {
            Ok(Ok(0)) => break, // client disconnected
            Ok(Ok(_n)) => {}
            Ok(Err(_)) => break,
            Err(_) => break, // timeout
        }

        // Process all complete messages in the buffer
        while buf.len() >= 4 {
            let msg_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if !(HEADER_SIZE..=MAX_MSG_SIZE).contains(&msg_len) {
                Python::attach(|py| {
                    let cc = state.conn_counter.bind(py);
                    let _ = cc.call_method0("disconnect");
                });
                return;
            }
            if buf.len() < msg_len {
                break; // need more data
            }

            let msg_data: Vec<u8> = buf.split_to(msg_len).to_vec();
            let op_code = i32::from_le_bytes([msg_data[12], msg_data[13], msg_data[14], msg_data[15]]);

            let response_bytes: Option<Vec<u8>> = Python::attach(|py| -> Option<Vec<u8>> {
                let ctx = ctx_handle.as_ref()?.bind(py);
                handle_message(py, &state, &msg_data, op_code, ctx, &mut req_id_gen).ok().flatten()
            });

            if let Some(resp) = response_bytes {
                if stream.write_all(&resp).await.is_err() {
                    break;
                }
            }
        }
    }

    Python::attach(|py| {
        let cc = state.conn_counter.bind(py);
        let _ = cc.call_method0("disconnect");
    });
}

fn handle_message(
    py: Python<'_>,
    state: &ServerState,
    msg_data: &[u8],
    op_code: i32,
    ctx: &Bound<'_, ConnectionContext>,
    req_id_gen: &mut i64,
) -> PyResult<Option<Vec<u8>>> {
    let handlers = state.handlers.bind(py);
    let make_error_fn = state.make_error_fn.bind(py);
    let error_response_fn = state.error_response_fn.bind(py);
    let exception_types = state.exception_types.bind(py);
    let audit_mod = state.audit_mod.bind(py);

    match op_code {
        OP_MSG => {
            let result = crate::wire_msg::decode_msg(py, msg_data);
            let (header, flags, body_doc_py, doc_sequences_py) = match result {
                Ok(r) => r,
                Err(_) => {
                    let resp_id = *req_id_gen as i32;
                    *req_id_gen += 1;
                    let error_doc = crate::wire_errors::make_error(py, "InternalError", "OP_MSG decode error")?;
                    let resp = crate::wire_msg::encode_msg(py, resp_id, 0, &error_doc, false)?;
                    return Ok(Some(resp.bind(py).as_bytes().to_vec()));
                }
            };
            let more_to_come = (flags & 0x02) != 0;
            let body_doc = body_doc_py.bind(py);
            let doc_sequences = doc_sequences_py.bind(py);

            // Auth gate: reject unauthenticated commands when auth is required
            if state.auth_required {
                if let Some(reject) = check_auth_gate(py, ctx, body_doc, header.request_id, req_id_gen)? {
                    return Ok(Some(reject));
                }
            }

            let body_dict: &Bound<'_, PyDict> = body_doc.cast()?;
            let response_doc = crate::wire_dispatch::rs_dispatch(
                py, ctx.as_any(), handlers, body_dict,
                Some(doc_sequences), make_error_fn, error_response_fn, exception_types,
                Some(&audit_mod),
            )?;

            if !more_to_come {
                let resp_id = *req_id_gen as i32;
                *req_id_gen += 1;
                let response_dict: &Bound<'_, PyDict> = response_doc.bind(py).cast()?;
                let mut resp_bytes = crate::wire_msg::encode_msg(py, resp_id, header.request_id, response_dict, false)?;

                let compressor_id = ctx.borrow().compressor_id.clone_ref(py);
                if !compressor_id.bind(py).is_none() {
                    let cid: i32 = compressor_id.bind(py).extract()?;
                    let resp_bound = resp_bytes.bind(py);
                    resp_bytes = crate::wire_msg::encode_compressed(py, resp_bound.as_bytes(), cid)?;
                }
                return Ok(Some(resp_bytes.bind(py).as_bytes().to_vec()));
            }
            Ok(None)
        }
        OP_COMPRESSED => {
            let result = crate::wire_msg::decode_compressed(py, msg_data);
            let inner_msg = match result {
                Ok(r) => r,
                Err(_) => {
                    let resp_id = *req_id_gen as i32;
                    *req_id_gen += 1;
                    let error_doc = crate::wire_errors::make_error(py, "InternalError", "OP_COMPRESSED decode error")?;
                    let resp = crate::wire_msg::encode_msg(py, resp_id, 0, &error_doc, false)?;
                    return Ok(Some(resp.bind(py).as_bytes().to_vec()));
                }
            };
            let inner_bytes: &[u8] = inner_msg.bind(py).as_bytes();
            let inner_op = i32::from_le_bytes([inner_bytes[12], inner_bytes[13], inner_bytes[14], inner_bytes[15]]);
            handle_message(py, state, inner_bytes, inner_op, ctx, req_id_gen)
        }
        OP_QUERY => {
            let (header, _flags, _coll, _skip, _limit, query_doc_py) =
                crate::wire_msg::decode_query(py, msg_data)?;
            let query_doc = query_doc_py.bind(py);

            let hello_keys = ["isMaster", "ismaster", "hello"];
            let is_handshake = hello_keys.iter().any(|k| {
                query_doc.get_item(*k).map(|v| !v.is_none()).unwrap_or(false)
            });

            // Auth gate for OP_QUERY: only allow handshake commands when auth is required
            if state.auth_required && !is_handshake {
                if let Some(reject) = check_auth_gate(py, ctx, query_doc, header.request_id, req_id_gen)? {
                    return Ok(Some(reject));
                }
            }

            let response_doc = if is_handshake {
                let hello_cmd = PyDict::new(py);
                hello_cmd.set_item("hello", 1)?;
                hello_cmd.set_item("helloOk", true)?;
                hello_cmd.set_item("$db", "admin")?;
                crate::wire_dispatch::rs_dispatch(
                    py, ctx.as_any(), handlers, &hello_cmd,
                    None, make_error_fn, error_response_fn, exception_types,
                    Some(&audit_mod),
                )?
            } else {
                let query_dict: &Bound<'_, PyDict> = query_doc.cast()?;
                crate::wire_dispatch::rs_dispatch(
                    py, ctx.as_any(), handlers, query_dict,
                    None, make_error_fn, error_response_fn, exception_types,
                    Some(&audit_mod),
                )?
            };

            let resp_id = *req_id_gen as i32;
            *req_id_gen += 1;
            let ok_val = response_doc.bind(py).get_item("ok").ok();
            let ok_zero = ok_val
                .as_ref()
                .and_then(|v| v.extract::<f64>().ok())
                .is_some_and(|x| x == 0.0);
            let response_flags: i32 = if ok_zero { 0x02 } else { 0 };
            let response_dict: &Bound<'_, PyDict> = response_doc.bind(py).cast()?;
            let docs_list = PyList::new(py, [response_dict])?;
            let resp_bytes = crate::wire_msg::encode_reply(py, resp_id, header.request_id, &docs_list, 0, 0, response_flags)?;
            Ok(Some(resp_bytes.bind(py).as_bytes().to_vec()))
        }
        _ => {
            let resp_id = *req_id_gen as i32;
            *req_id_gen += 1;
            let error_doc = crate::wire_errors::make_error(py, "CommandNotSupported", &format!("unsupported opcode: {op_code}"))?;
            let resp = crate::wire_msg::encode_msg(py, resp_id, 0, &error_doc, false)?;
            Ok(Some(resp.bind(py).as_bytes().to_vec()))
        }
    }
}

fn create_connection_context<'py>(
    py: Python<'py>,
    state: &ServerState,
    conn_id: i64,
    addr: std::net::SocketAddr,
) -> PyResult<Bound<'py, ConnectionContext>> {
    let address = (addr.ip().to_string(), addr.port() as i64);
    let address_py = address.into_pyobject(py)?;
    let mut ctx = crate::wire_context::ConnectionContext::new(
        py,
        state.local_client.bind(py),
        conn_id,
        address_py.as_any(),
        state.cursor_registry.bind(py),
        Some(state.sync_mgr.bind(py)),
        Some(state.session_registry.bind(py)),
        Some(state.op_tracker.bind(py)),
        Some(state.param_store.bind(py)),
        Some(state.top_stats.bind(py)),
        Some(state.profiler.bind(py)),
        Some(state.log_buffer.bind(py)),
        Some(state.conn_counter.bind(py)),
        Some(state.free_monitoring.bind(py)),
    )?;
    ctx.cached = Some(Arc::clone(&state.cached_imports));
    Bound::new(py, ctx)
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

fn build_tls_acceptor(cert_path: &str, key_path: &str) -> PyResult<tokio_rustls::TlsAcceptor> {
    let cert_data = std::fs::read(cert_path)
        .map_err(|e| PyRuntimeError::new_err(format!("cannot read TLS cert {cert_path}: {e}")))?;
    let key_data = std::fs::read(key_path)
        .map_err(|e| PyRuntimeError::new_err(format!("cannot read TLS key {key_path}: {e}")))?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_data[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| PyRuntimeError::new_err(format!("invalid TLS certificate: {e}")))?;

    let key = rustls_pemfile::private_key(&mut &key_data[..])
        .map_err(|e| PyRuntimeError::new_err(format!("invalid TLS private key: {e}")))?
        .ok_or_else(|| PyRuntimeError::new_err("no private key found in TLS key file"))?;

    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| PyRuntimeError::new_err(format!("TLS configuration error: {e}")))?;

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

// ---------------------------------------------------------------------------
// Auth gate
// ---------------------------------------------------------------------------

const AUTH_EXEMPT_COMMANDS: &[&str] = &[
    "hello", "ismaster", "isMaster", "ping", "saslStart", "saslContinue",
    "logout", "buildInfo", "buildinfo", "whatsmyuri", "getnonce",
    "connectionStatus",
];

fn check_auth_gate<'py>(
    py: Python<'py>,
    ctx: &Bound<'py, ConnectionContext>,
    body_doc: &Bound<'py, PyAny>,
    request_id_val: i32,
    req_id_gen: &mut i64,
) -> PyResult<Option<Vec<u8>>> {
    let cmd_name = first_dict_key(py, body_doc);
    if AUTH_EXEMPT_COMMANDS.contains(&cmd_name.as_str()) {
        return Ok(None);
    }

    {
        let cc_ref = ctx.borrow();
        let is_authed = cc_ref.authenticated_user.lock().is_some();

        if !is_authed {
            drop(cc_ref);
            let resp_id = *req_id_gen as i32;
            *req_id_gen += 1;
            let error_doc = crate::wire_errors::make_error(py, "Unauthorized", "command requires authentication")?;
            let resp = crate::wire_msg::encode_msg(py, resp_id, request_id_val, &error_doc, false)?;
            return Ok(Some(resp.bind(py).as_bytes().to_vec()));
        }

        let roles = cc_ref.authenticated_roles.lock().clone();
        drop(cc_ref);

        let target_db = if let Ok(d) = body_doc.cast::<PyDict>() {
            d.get_item("$db")
                .ok()
                .flatten()
                .and_then(|v| v.extract::<String>().ok())
                .unwrap_or_else(|| "test".to_string())
        } else {
            "test".to_string()
        };

        if !crate::rbac::check_privilege(&roles, &cmd_name, &target_db) {
            let resp_id = *req_id_gen as i32;
            *req_id_gen += 1;
            let msg = format!("not authorized on {target_db} to execute command {cmd_name}");
            let error_doc = crate::wire_errors::make_error(py, "Unauthorized", &msg)?;
            let resp = crate::wire_msg::encode_msg(py, resp_id, request_id_val, &error_doc, false)?;
            return Ok(Some(resp.bind(py).as_bytes().to_vec()));
        }
    }

    Ok(None)
}

fn first_dict_key(py: Python<'_>, obj: &Bound<'_, PyAny>) -> String {
    if let Ok(d) = obj.cast::<PyDict>() {
        if let Some((key, _)) = d.iter().next() {
            if let Ok(s) = key.extract::<String>() {
                return s;
            }
        }
    }
    let _ = py;
    String::new()
}
