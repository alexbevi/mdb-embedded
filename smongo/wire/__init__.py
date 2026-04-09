"""
smongo.wire -- MongoDB wire protocol server for the embedded engine.

Start a server programmatically::

    from smongo.wire import WireServer
    server = WireServer("my_data_dir", host="127.0.0.1", port=27018)
    server.start()        # non-blocking
    server.serve_forever()  # or block until Ctrl-C

Or from the command line::

    python -m smongo.wire --db-path my_data_dir --port 27018
"""

from ..sync import SyncManager
from .msg import ChecksumMismatch, ProtocolError
from .server import WireServer


def run_server(
    db_path: str = "local_redb_data",
    host: str = "127.0.0.1",
    port: int = 27018,
    sync: str | SyncManager | None = None,
) -> WireServer:
    """Convenience: create, start, and block on a WireServer."""
    server = WireServer(db_path, host, port, sync=sync)
    server.serve_forever()
    return server


__all__ = ["ChecksumMismatch", "ProtocolError", "WireServer", "run_server"]
