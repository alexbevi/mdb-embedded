"""
BSON Boundary Adapter -- isolates all BSON <-> engine type conversion at the wire edge.

Inbound:  wire BSON types  -> engine-friendly Python dicts (engine ObjectId, plain dicts).
Outbound: engine dicts     -> BSON-encodable dicts (bson.ObjectId for wire transport).
"""

from typing import Any, overload

from bson import Decimal128, Regex
from bson import ObjectId as BsonObjectId

from .._types import Document
from ..objectid import ObjectId as EngineObjectId

MAX_NESTING_DEPTH = 100


@overload
def normalize_inbound(doc: dict[str, Any]) -> Document: ...
@overload
def normalize_inbound(doc: None) -> None: ...
def normalize_inbound(doc: dict[str, Any] | None) -> Document | None:
    """Recursively convert BSON wire types to engine-compatible Python types."""
    if doc is None:
        return None
    if not isinstance(doc, dict):
        return doc
    return {k: _convert_inbound(v, 1) for k, v in doc.items()}


def _convert_inbound(value: Any, depth: int = 0) -> Any:
    if depth > MAX_NESTING_DEPTH:
        raise ValueError(
            f"document exceeds maximum nesting depth of {MAX_NESTING_DEPTH}"
        )
    if isinstance(value, BsonObjectId):
        return EngineObjectId(str(value))
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    if isinstance(value, Regex):
        flags = str(value.flags) if value.flags else ""
        return {"$regex": value.pattern, "$options": flags}
    if isinstance(value, dict):
        return {k: _convert_inbound(v, depth + 1) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert_inbound(v, depth + 1) for v in value]
    return value


@overload
def normalize_outbound(doc: dict[str, Any]) -> dict[str, Any]: ...
@overload
def normalize_outbound(doc: None) -> None: ...
def normalize_outbound(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    """Recursively convert engine output to BSON-encodable types for the wire."""
    if doc is None:
        return None
    if not isinstance(doc, dict):
        return doc
    return {k: _convert_outbound(k, v, 1) for k, v in doc.items()}


def _convert_outbound(key: str | None, value: Any, depth: int = 0) -> Any:
    if depth > MAX_NESTING_DEPTH:
        raise ValueError(
            f"document exceeds maximum nesting depth of {MAX_NESTING_DEPTH}"
        )
    if isinstance(value, EngineObjectId):
        return BsonObjectId(str(value))
    if key == "_id" and isinstance(value, str) and _is_objectid_hex(value):
        return BsonObjectId(value)
    if isinstance(value, dict):
        return {k: _convert_outbound(k, v, depth + 1) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert_outbound(None, v, depth + 1) for v in value]
    return value


def _is_objectid_hex(s: str) -> bool:
    if len(s) != 24:
        return False
    try:
        bytes.fromhex(s)
        return True
    except ValueError:
        return False


def normalize_outbound_docs(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convenience: normalize a list of documents for wire output."""
    return [normalize_outbound(d) for d in docs]
