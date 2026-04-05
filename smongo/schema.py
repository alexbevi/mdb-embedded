"""
$jsonSchema validator -- enforces document structure on insert and update.

The validation engine is implemented in Rust (_smongo_core.validate_document)
for zero-dispatch overhead on the write path.  This module re-exports the Rust
symbols so that ``from smongo.schema import ValidationError, validate_document``
continues to work for all Python callers.

Supported schema keywords:
    required, properties, type/bsonType, minimum, maximum, exclusiveMinimum,
    exclusiveMaximum, minLength, maxLength, enum, pattern, minItems, maxItems,
    uniqueItems, additionalProperties, minProperties, maxProperties, items
"""

from __future__ import annotations

from typing import Any

from smongo._smongo_core import ValidationError  # Rust-defined exception
from smongo._smongo_core import validate_document as _rs_validate

MAX_NESTING_DEPTH = 100


def validate_document(doc: dict[str, Any], schema: Any) -> None:
    """Validate *doc* against a ``$jsonSchema`` spec.

    Raises ``ValidationError`` if the document fails validation.
    Falsy schemas (``None``, ``{}``, ``0``) are treated as "no validation".
    """
    if not schema:
        return
    _rs_validate(doc, schema)


__all__ = [
    "MAX_NESTING_DEPTH",
    "ValidationError",
    "validate_document",
]
