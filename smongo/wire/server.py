"""
TCP wire protocol server -- accepts MongoDB driver connections over OP_MSG.

Each connection gets its own daemon thread with a private ConnectionContext
(and thus private WiredTiger sessions).  The server shares a single
LocalClient and CursorRegistry across all connections.
"""

from __future__ import annotations

import logging
import socket
import struct
import threading
from itertools import count

from ..storage import LocalClient
from ..sync import SyncManager
from .commands import dispatch
from .context import (
    ConnectionContext,
    ConnectionCounter,
    FreeMonitoringState,
    LogBuffer,
    ParameterStore,
)
from .cursors import CursorRegistry
from .errors import make_error
from .msg import (
    HEADER_SIZE,
    MAX_MSG_SIZE,
    OP_COMPRESSED,
    OP_MSG,
    OP_QUERY,
    ChecksumMismatch,
    MsgHeader,
    ProtocolError,
    decode_compressed,
    decode_header,
    decode_msg,
    decode_query,
    encode_compressed,
    encode_msg,
    encode_reply,
)
from .profiler import OperationTracker, Profiler, TopStats
from .sessions import SessionRegistry

log = logging.getLogger("smongo.wire.server")


CONNECTION_TIMEOUT_SEC = 300
MAX_CONNECTIONS = 1024


class WireServer:
    """MongoDB-compatible wire protocol server backed by the embedded engine.

    When ``auth_required``, ``tls_cert_file``, or ``tls_key_file`` are
    specified, the server automatically delegates to ``RustWireServer``
    (Tokio async, TLS via rustls, SCRAM-SHA-256 auth gate).  Otherwise
    it uses the lightweight Python TCP accept loop.
    """

    _UNSET = object()

    def __init__(
        self,
        db_path: str = "local_wt_data",
        host: str = "127.0.0.1",
        port: int = 27017,
        sync: str | SyncManager | None = None,
        max_connections: int = MAX_CONNECTIONS,
        local_client: LocalClient | None = None,
        auth_required: bool | object = _UNSET,
        tls_cert_file: str | None = None,
        tls_key_file: str | None = None,
        audit_log: str | None = None,
    ) -> None:
        self.host = host
        self.port = port

        if audit_log is not None:
            from ..audit import configure_audit
            configure_audit(audit_log)

        auth_was_set = auth_required is not WireServer._UNSET
        auth_bool = bool(auth_required) if auth_was_set else False
        self._use_rust_server = auth_was_set or tls_cert_file is not None

        if local_client is not None:
            self._local_client = local_client
        elif self._use_rust_server:
            from smongo._smongo_core import RustLocalClient
            self._local_client = RustLocalClient(db_path)
        else:
            self._local_client = LocalClient(db_path)
        self._owns_local_client = local_client is None
        self._cursor_registry = CursorRegistry()
        self._session_registry = SessionRegistry()
        self._op_tracker = OperationTracker()
        self._param_store = ParameterStore()
        self._top_stats = TopStats()
        self._profiler = Profiler()
        self._log_buffer = LogBuffer()
        self._conn_counter = ConnectionCounter(max_connections)
        self._free_monitoring = FreeMonitoringState()

        self._owns_sync_mgr = False
        if isinstance(sync, SyncManager):
            self._sync_mgr: SyncManager | None = sync
        elif isinstance(sync, str):
            from ..client import MongoClient

            local_mc = MongoClient(f"local://{db_path}")
            self._sync_mgr = SyncManager(local_mc, sync)
            self._owns_sync_mgr = True
        else:
            self._sync_mgr = None
        self._rust_server: object | None = None

        if self._use_rust_server:
            from smongo._smongo_core import RustWireServer

            self._rust_server = RustWireServer(
                host,
                port,
                self._local_client,
                self._cursor_registry,
                self._session_registry,
                self._op_tracker,
                self._param_store,
                self._top_stats,
                self._profiler,
                self._log_buffer,
                self._conn_counter,
                self._free_monitoring,
                self._sync_mgr,
                max_connections,
                self._owns_sync_mgr,
                self._owns_local_client,
                tls_cert_file,
                tls_key_file,
                auth_bool,
            )
        else:
            self._shutdown = threading.Event()
            self._server_socket: socket.socket | None = None
            self._accept_thread: threading.Thread | None = None
            self._conn_threads: set[threading.Thread] = set()
            self._conn_id_gen = count(1)
            self._conn_semaphore = threading.Semaphore(max_connections)

    def start(self) -> None:
        """Bind, listen, and begin accepting connections in a background thread."""
        if self._use_rust_server:
            self._rust_server.start()  # type: ignore[union-attr]
            return
        self._log_buffer.install()
        self._cursor_registry.start_reaper()
        self._session_registry.start_reaper()
        if self._owns_sync_mgr and self._sync_mgr is not None:
            self._sync_mgr.start()
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(128)
        self._server_socket.settimeout(1.0)

        log.info("Wire server listening on %s:%d", self.host, self.port)

        self._accept_thread = threading.Thread(
            target=self._accept_loop, daemon=True, name="wire-accept"
        )
        self._accept_thread.start()

    def stop(self) -> None:
        """Signal shutdown, close the listener, and join connection threads."""
        if self._use_rust_server:
            self._rust_server.stop()  # type: ignore[union-attr]
            return
        self._shutdown.set()
        self._cursor_registry.stop_reaper()
        self._session_registry.stop_reaper()
        if self._owns_sync_mgr and self._sync_mgr is not None:
            self._sync_mgr.stop()
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass
        if self._accept_thread:
            self._accept_thread.join(timeout=3)
        for t in self._conn_threads:
            t.join(timeout=2)
        log.info("Wire server stopped")

    def serve_forever(self) -> None:
        """Start the server and block until interrupted or stopped."""
        self.start()
        try:
            if self._use_rust_server:
                self._rust_server.serve_forever()  # type: ignore[union-attr]
            else:
                self._shutdown.wait()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def __enter__(self) -> WireServer:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    # -- internals --------------------------------------------------------

    def _accept_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                assert self._server_socket is not None
                client_sock, address = self._server_socket.accept()
            except TimeoutError:
                continue
            except OSError:
                if not self._shutdown.is_set():
                    log.exception("Accept error")
                break

            if not self._conn_semaphore.acquire(timeout=0.1):
                log.warning("Max connections reached, rejecting %s", address)
                try:
                    error_doc = make_error("OperationFailed", "too many connections")
                    client_sock.sendall(encode_msg(0, 0, error_doc))
                except OSError:
                    pass
                finally:
                    try:
                        client_sock.close()
                    except OSError:
                        pass
                continue

            conn_id = next(self._conn_id_gen)
            log.debug("Connection #%d from %s", conn_id, address)
            t = threading.Thread(
                target=self._connection_loop,
                args=(client_sock, address, conn_id),
                daemon=True,
                name=f"wire-conn-{conn_id}",
            )
            self._conn_threads.add(t)
            t.start()

    @staticmethod
    def _recv_exact(sock: socket.socket, nbytes: int) -> bytes | None:
        """Read exactly *nbytes* from *sock*, returning None on disconnect."""
        buf = bytearray()
        while len(buf) < nbytes:
            chunk = sock.recv(nbytes - len(buf))
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

    def _connection_loop(self, sock: socket.socket, address: tuple[str, int], conn_id: int) -> None:
        sock.settimeout(CONNECTION_TIMEOUT_SEC)
        self._conn_counter.connect()
        ctx = ConnectionContext(
            self._local_client,
            conn_id,
            address,
            self._cursor_registry,
            sync_mgr=self._sync_mgr,
            session_registry=self._session_registry,
            op_tracker=self._op_tracker,
            param_store=self._param_store,
            top_stats=self._top_stats,
            profiler=self._profiler,
            log_buffer=self._log_buffer,
            conn_counter=self._conn_counter,
            free_monitoring=self._free_monitoring,
        )
        req_id_gen = count(1)

        try:
            while not self._shutdown.is_set():
                length_bytes = self._recv_exact(sock, 4)
                if not length_bytes:
                    break

                msg_length = struct.unpack("<i", length_bytes)[0]
                if msg_length < HEADER_SIZE or msg_length > MAX_MSG_SIZE:
                    log.warning("Conn #%d: bad message length %d", conn_id, msg_length)
                    break

                remaining = self._recv_exact(sock, msg_length - 4)
                if not remaining:
                    break

                full_msg = length_bytes + remaining
                header = decode_header(full_msg)

                if header.op_code == OP_MSG:
                    self._handle_op_msg(sock, full_msg, ctx, req_id_gen)
                elif header.op_code == OP_COMPRESSED:
                    self._handle_op_compressed(sock, full_msg, ctx, req_id_gen)
                elif header.op_code == OP_QUERY:
                    self._handle_op_query(sock, full_msg, ctx, req_id_gen)
                else:
                    self._handle_unknown_opcode(sock, header, ctx, req_id_gen)

        except (ConnectionResetError, BrokenPipeError, TimeoutError, OSError) as exc:
            log.debug("Conn #%d closed: %s", conn_id, exc)
        except (
            struct.error,
            UnicodeDecodeError,
            KeyError,
            TypeError,
            ValueError,
            IndexError,
            RuntimeError,
        ):
            log.exception("Error in connection #%d", conn_id)
        finally:
            self._conn_counter.disconnect()
            self._conn_semaphore.release()
            self._conn_threads.discard(threading.current_thread())
            try:
                sock.close()
            except OSError:
                pass
            log.debug("Conn #%d terminated", conn_id)

    @staticmethod
    def _handle_op_msg(
        sock: socket.socket, data: bytes, ctx: ConnectionContext, req_id_gen: count[int]
    ) -> None:
        try:
            header, flags, body_doc, doc_sequences = decode_msg(data)
        except (ProtocolError, ChecksumMismatch) as exc:
            log.warning("OP_MSG decode error: %s", exc)
            resp_id = next(req_id_gen)
            error_doc = make_error("InternalError", str(exc))
            sock.sendall(encode_msg(resp_id, 0, error_doc))
            return

        more_to_come = bool(flags & 0x02)

        response_doc = dispatch(ctx, body_doc, doc_sequences)

        if not more_to_come:
            resp_id = next(req_id_gen)
            resp_bytes = encode_msg(resp_id, header.request_id, response_doc)
            if ctx.compressor_id is not None:
                resp_bytes = encode_compressed(resp_bytes, ctx.compressor_id)
            sock.sendall(resp_bytes)

    @staticmethod
    def _handle_op_compressed(
        sock: socket.socket, data: bytes, ctx: ConnectionContext, req_id_gen: count[int]
    ) -> None:
        try:
            inner_msg = decode_compressed(data)
        except ProtocolError as exc:
            log.warning("OP_COMPRESSED decode error: %s", exc)
            resp_id = next(req_id_gen)
            error_doc = make_error("InternalError", str(exc))
            sock.sendall(encode_msg(resp_id, 0, error_doc))
            return

        inner_header = decode_header(inner_msg)

        if inner_header.op_code == OP_MSG:
            WireServer._handle_op_msg(sock, inner_msg, ctx, req_id_gen)
        elif inner_header.op_code == OP_QUERY:
            WireServer._handle_op_query(sock, inner_msg, ctx, req_id_gen)
        else:
            log.warning("OP_COMPRESSED wraps unsupported opcode %d", inner_header.op_code)
            resp_id = next(req_id_gen)
            error_doc = make_error(
                "CommandNotSupported",
                f"unsupported opcode {inner_header.op_code} inside OP_COMPRESSED",
            )
            sock.sendall(encode_msg(resp_id, inner_header.request_id, error_doc))

    @staticmethod
    def _handle_op_query(
        sock: socket.socket, data: bytes, ctx: ConnectionContext, req_id_gen: count[int]
    ) -> None:
        header, _flags, _coll_name, _skip, _limit, query_doc = decode_query(data)

        if any(k in query_doc for k in ("isMaster", "ismaster", "hello")):
            response_doc = dispatch(ctx, {"hello": 1, "helloOk": True, "$db": "admin"}, {})
        else:
            response_doc = dispatch(ctx, query_doc, {})

        resp_id = next(req_id_gen)
        response_flags = 0
        if response_doc.get("ok") == 0:
            response_flags = 0x02
        sock.sendall(
            encode_reply(resp_id, header.request_id, [response_doc], response_flags=response_flags)
        )

    @staticmethod
    def _handle_unknown_opcode(
        sock: socket.socket, header: MsgHeader, ctx: ConnectionContext, req_id_gen: count[int]
    ) -> None:
        log.warning("Unsupported opcode %d from connection #%d", header.op_code, ctx.connection_id)
        resp_id = next(req_id_gen)
        error_doc = make_error(
            "CommandNotSupported",
            f"unsupported opcode: {header.op_code}",
        )
        sock.sendall(encode_msg(resp_id, header.request_id, error_doc))
