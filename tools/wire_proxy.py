#!/usr/bin/env python3
"""TCP proxy for debugging MongoDB wire protocol traffic between a client
(e.g. Compass) and the smongo wire server. Logs every message with decoded
BSON in both directions.

Usage:
    python tools/wire_proxy.py --listen 27099 --target 27018
    # Then connect Compass to localhost:27099
"""
import argparse
import socket
import struct
import sys
import threading
import traceback

try:
    import bson as pybson
except ImportError:
    sys.exit("pip install pymongo   (for the bson library)")

OP_MSG = 2013
OP_COMPRESSED = 2012
OP_REPLY = 1
OP_QUERY = 2004

OPCODE_NAMES = {1: "OP_REPLY", 2004: "OP_QUERY", 2012: "OP_COMPRESSED", 2013: "OP_MSG"}

# Decompressors (best-effort)
def _decompress(cid, data, expected_size):
    if cid == 0:
        return data
    if cid == 1:
        import snappy
        return snappy.decompress(data)
    if cid == 2:
        import zlib
        return zlib.decompress(data)
    if cid == 3:
        import zstandard
        return zstandard.ZstdDecompressor().decompress(data, max_output_size=expected_size)
    return data


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("disconnected")
        buf += chunk
    return buf


def read_message(sock):
    hdr = read_exact(sock, 16)
    msg_len, req_id, resp_to, opcode = struct.unpack("<iiiI", hdr)
    body = read_exact(sock, msg_len - 16)
    return hdr + body, msg_len, req_id, resp_to, opcode


def decode_op_msg_body(payload):
    """Decode the OP_MSG payload (after header). Returns (flags, bson_doc)."""
    flags = struct.unpack("<I", payload[:4])[0]
    kind = payload[4]
    bson_bytes = payload[5:]
    try:
        doc = pybson.decode(bson_bytes)
    except Exception as e:
        doc = {"__decode_error__": str(e)}
    return flags, doc


def decode_message(raw, msg_len, req_id, resp_to, opcode):
    """Decode a wire message and return a human-readable summary."""
    body = raw[16:]
    info = {"opcode": OPCODE_NAMES.get(opcode, str(opcode)), "req_id": req_id, "resp_to": resp_to, "len": msg_len}

    if opcode == OP_MSG:
        flags, doc = decode_op_msg_body(body)
        info["flags"] = flags
        info["doc"] = doc
        cmd_name = next(iter(doc), "?") if isinstance(doc, dict) else "?"
        info["cmd"] = cmd_name

    elif opcode == OP_COMPRESSED:
        orig_opcode = struct.unpack("<i", body[:4])[0]
        uncomp_size = struct.unpack("<i", body[4:8])[0]
        cid = body[8]
        compressed = body[9:]
        info["original_opcode"] = OPCODE_NAMES.get(orig_opcode, str(orig_opcode))
        info["compressor_id"] = cid
        info["uncompressed_size"] = uncomp_size
        info["compressed_size"] = len(compressed)
        try:
            decompressed = _decompress(cid, compressed, uncomp_size)
            info["decompressed_size"] = len(decompressed)
            info["size_match"] = len(decompressed) == uncomp_size
            if orig_opcode == OP_MSG:
                flags, doc = decode_op_msg_body(decompressed)
                info["flags"] = flags
                info["doc"] = doc
                cmd_name = next(iter(doc), "?") if isinstance(doc, dict) else "?"
                info["cmd"] = cmd_name
        except Exception as e:
            info["decompress_error"] = str(e)

    elif opcode == OP_QUERY:
        try:
            flags = struct.unpack("<i", body[:4])[0]
            # cstring collection name
            end = body.index(b"\x00", 4)
            coll = body[4:end].decode("utf-8")
            skip = struct.unpack("<i", body[end+1:end+5])[0]
            limit = struct.unpack("<i", body[end+5:end+9])[0]
            bson_bytes = body[end+9:]
            doc = pybson.decode(bson_bytes)
            info["collection"] = coll
            info["doc"] = doc
        except Exception as e:
            info["decode_error"] = str(e)

    elif opcode == OP_REPLY:
        try:
            resp_flags = struct.unpack("<i", body[:4])[0]
            cursor_id = struct.unpack("<q", body[4:12])[0]
            starting = struct.unpack("<i", body[12:16])[0]
            num_returned = struct.unpack("<i", body[16:20])[0]
            docs = []
            offset = 20
            for _ in range(num_returned):
                doc_len = struct.unpack("<i", body[offset:offset+4])[0]
                doc = pybson.decode(body[offset:offset+doc_len])
                docs.append(doc)
                offset += doc_len
            info["cursor_id"] = cursor_id
            info["num_returned"] = num_returned
            info["docs"] = docs
        except Exception as e:
            info["decode_error"] = str(e)

    return info


def format_doc_summary(doc, max_depth=2):
    """Summarize a document for logging."""
    if not isinstance(doc, dict):
        return str(doc)[:200]
    lines = []
    for k, v in doc.items():
        if k == "firstBatch" and isinstance(v, list):
            lines.append(f"  firstBatch: [{len(v)} docs]")
            for i, d in enumerate(v[:3]):
                keys = list(d.keys()) if isinstance(d, dict) else "?"
                lines.append(f"    [{i}] keys={keys}")
            if len(v) > 3:
                lines.append(f"    ... {len(v)-3} more")
        elif k == "nextBatch" and isinstance(v, list):
            lines.append(f"  nextBatch: [{len(v)} docs]")
            for i, d in enumerate(v[:3]):
                keys = list(d.keys()) if isinstance(d, dict) else "?"
                lines.append(f"    [{i}] keys={keys}")
        elif isinstance(v, dict) and max_depth > 0:
            lines.append(f"  {k}: {{...}}")
            for k2, v2 in v.items():
                if k2 in ("firstBatch", "nextBatch") and isinstance(v2, list):
                    lines.append(f"    {k2}: [{len(v2)} docs]")
                    for i, d in enumerate(v2[:3]):
                        keys = list(d.keys()) if isinstance(d, dict) else "?"
                        lines.append(f"      [{i}] keys={keys}")
                    if len(v2) > 3:
                        lines.append(f"      ... {len(v2)-3} more")
                else:
                    lines.append(f"    {k2}: {str(v2)[:100]}")
        elif isinstance(v, list):
            lines.append(f"  {k}: [{len(v)} items] {str(v)[:100]}")
        else:
            lines.append(f"  {k}: {str(v)[:100]}")
    return "\n".join(lines)


def proxy_stream(src, dst, direction, conn_id):
    """Forward traffic from src to dst, logging each wire message."""
    try:
        while True:
            raw, msg_len, req_id, resp_to, opcode = read_message(src)
            dst.sendall(raw)

            info = decode_message(raw, msg_len, req_id, resp_to, opcode)

            tag = f"[conn{conn_id}] {direction}"
            op = info.get("opcode", "?")
            cmd = info.get("cmd", "")
            compressed = " (COMPRESSED)" if opcode == OP_COMPRESSED else ""

            print(f"\n{'='*70}")
            print(f"{tag} {op}{compressed}  reqID={req_id} respTo={resp_to} len={msg_len}")
            if cmd:
                print(f"{tag} command: {cmd}")
            if "flags" in info:
                print(f"{tag} flags: {info['flags']}")
            if opcode == OP_COMPRESSED:
                print(f"{tag} compressor={info.get('compressor_id')} "
                      f"compressed={info.get('compressed_size')} "
                      f"uncompressed={info.get('uncompressed_size')} "
                      f"decompressed={info.get('decompressed_size')} "
                      f"match={info.get('size_match')}")
                if "decompress_error" in info:
                    print(f"{tag} *** DECOMPRESS ERROR: {info['decompress_error']} ***")

            doc = info.get("doc")
            if doc:
                print(f"{tag} body:")
                print(format_doc_summary(doc))

            if "docs" in info:
                for i, d in enumerate(info["docs"][:3]):
                    print(f"{tag} doc[{i}]: keys={list(d.keys())}")

            sys.stdout.flush()

    except ConnectionError:
        print(f"\n[conn{conn_id}] {direction} disconnected")
    except Exception:
        traceback.print_exc()
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            dst.close()
        except Exception:
            pass


_conn_counter = 0

def handle_connection(client_sock, target_host, target_port):
    global _conn_counter
    _conn_counter += 1
    cid = _conn_counter
    print(f"\n[conn{cid}] New connection from {client_sock.getpeername()}")
    try:
        server_sock = socket.create_connection((target_host, target_port), timeout=5)
    except Exception as e:
        print(f"[conn{cid}] Cannot connect to target {target_host}:{target_port}: {e}")
        client_sock.close()
        return

    t1 = threading.Thread(target=proxy_stream, args=(client_sock, server_sock, "CLIENT->", cid), daemon=True)
    t2 = threading.Thread(target=proxy_stream, args=(server_sock, client_sock, "SERVER->", cid), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    print(f"[conn{cid}] Connection closed")


def main():
    parser = argparse.ArgumentParser(description="MongoDB wire protocol proxy/logger")
    parser.add_argument("--listen", type=int, default=27099, help="Port to listen on")
    parser.add_argument("--target", type=int, default=27018, help="Wire server port to forward to")
    args = parser.parse_args()

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", args.listen))
    listener.listen(5)
    print(f"Wire proxy listening on 127.0.0.1:{args.listen} -> 127.0.0.1:{args.target}")
    print("Connect Compass to: mongodb://localhost:{args.listen}/?directConnection=true")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            client, addr = listener.accept()
            threading.Thread(target=handle_connection, args=(client, "127.0.0.1", args.target), daemon=True).start()
    except KeyboardInterrupt:
        print("\nStopping proxy")
    finally:
        listener.close()


if __name__ == "__main__":
    main()
