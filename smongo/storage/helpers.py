from __future__ import annotations

import logging
from typing import Any

from bson import SON
from bson import ObjectId as BsonObjectId
from bson import decode as _bson_decode
from bson import encode as _bson_encode

from .._types import Document
from ..objectid import ObjectId

log = logging.getLogger("smongo.storage")


def _to_bson(doc: Document) -> bytes:
    """Normalize a document for BSON encoding and return raw bytes."""

    def _normalize(v: Any) -> Any:
        if isinstance(v, dict):
            return {k: _normalize(val) for k, val in v.items()}
        if isinstance(v, list):
            return [_normalize(item) for item in v]
        if isinstance(v, ObjectId):
            return BsonObjectId(str(v))
        if isinstance(v, BsonObjectId):
            return v
        if isinstance(v, str | int | float | bool | None | bytes):
            return v
        return str(v)

    return _bson_encode(_normalize(doc))


def _from_bson(raw: bytes) -> Document:
    """Decode raw BSON bytes into a plain dict, converting bson.ObjectId to engine ObjectId."""
    doc = _bson_decode(raw)
    doc = dict(doc) if isinstance(doc, SON) else doc
    result: Document = _denormalize(doc)
    return result


def _denormalize(v: Any) -> Any:
    """Convert bson library types back to engine types after decoding."""
    if isinstance(v, BsonObjectId):
        return ObjectId(str(v))
    if isinstance(v, dict):
        return {k: _denormalize(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_denormalize(item) for item in v]
    return v
