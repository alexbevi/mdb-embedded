"""Allow ``python -m smongo.wire`` to launch the wire server."""

import argparse
import logging

from . import WireServer


def main() -> None:
    parser = argparse.ArgumentParser(
        description="smongo wire protocol server -- Small MongoDB, real protocol"
    )
    parser.add_argument(
        "--db-path",
        default="local_wt_data",
        help="Path to WiredTiger database directory (default: local_wt_data)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=27017, help="Listen port (default: 27017)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    print(
        f"smongo wire server on {args.host}:{args.port}  (db: {args.db_path})  -- small but mighty"
    )
    server = WireServer(args.db_path, args.host, args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
