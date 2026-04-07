from __future__ import annotations

import logging
from typing import Any

from bson import ObjectId as BsonObjectId

from smongo._smongo_core import from_bson as _from_bson
from smongo._smongo_core import to_bson as _to_bson

from ..objectid import ObjectId

__all__ = [
    "_denormalize",
    "_from_bson",
    "_to_bson",
]

log = logging.getLogger("smongo.storage")


def _denormalize(v: Any) -> Any:
    """Convert bson library types back to engine types after decoding."""
    if isinstance(v, BsonObjectId):
        return ObjectId(str(v))
    if isinstance(v, dict):
        return {k: _denormalize(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_denormalize(item) for item in v]
    return v
