"""
$jsonSchema validator -- enforces document structure on insert and update.

Implements the core subset of JSON Schema that MongoDB supports:
    required, properties, type, minimum, maximum, minLength, maxLength,
    enum, pattern, minItems, maxItems, additionalProperties
"""

from typing import Any

from .query import _safe_regex

MAX_NESTING_DEPTH = 100


class ValidationError(Exception):
    pass


_MONGO_TYPE_MAP: dict[str, type | tuple[type, ...]] = {
    "string": str,
    "number": (int, float),
    "int": int,
    "long": int,
    "double": float,
    "bool": bool,
    "boolean": bool,
    "object": dict,
    "array": list,
    "null": type(None),
}


def _resolve_py_types(bson_type: Any) -> tuple[type, ...]:
    """Map MongoDB schema type names to Python runtime types."""
    type_names: list[str]
    if isinstance(bson_type, str):
        type_names = [bson_type]
    elif isinstance(bson_type, list):
        type_names = [t for t in bson_type if isinstance(t, str)]
    else:
        return ()

    resolved: list[type] = []
    for tn in type_names:
        mapped = _MONGO_TYPE_MAP.get(tn)
        if isinstance(mapped, type):
            resolved.append(mapped)
        elif isinstance(mapped, tuple):
            resolved.extend(t for t in mapped if isinstance(t, type))
    return tuple(resolved)


def validate_document(doc: dict[str, Any], schema: dict[str, Any]) -> None:
    """
    Validate a document against a $jsonSchema spec.
    Raises ValidationError if the document fails validation.
    """
    if not schema:
        return
    _validate_object(doc, schema, path="", depth=0)


def _validate_object(value: Any, schema: dict[str, Any], path: str, depth: int = 0) -> None:
    if depth > MAX_NESTING_DEPTH:
        raise ValidationError(f"Document exceeds maximum nesting depth of {MAX_NESTING_DEPTH}")
    bson_type = schema.get("bsonType") or schema.get("type")
    if bson_type:
        py_types = _resolve_py_types(bson_type)
        if py_types and not isinstance(value, py_types):
            raise ValidationError(
                f"Document failed validation at '{path}': expected type "
                f"{bson_type}, got {type(value).__name__}"
            )

    if not isinstance(value, dict):
        _validate_scalar(value, schema, path)
        return

    required = schema.get("required", [])
    for field in required:
        if field not in value:
            raise ValidationError(
                f"Document failed validation: missing required field '{_join(path, field)}'"
            )

    properties = schema.get("properties", {})
    for field, field_schema in properties.items():
        if field in value:
            _validate_value(value[field], field_schema, _join(path, field), depth + 1)

    if schema.get("additionalProperties") is False:
        allowed = set(properties.keys())
        for field in value:
            if field not in allowed and field != "_id":
                raise ValidationError(
                    f"Document failed validation: additional property '{_join(path, field)}' not allowed"
                )

    if "minProperties" in schema and len(value) < schema["minProperties"]:
        raise ValidationError(f"Document failed validation at '{path}': too few properties")
    if "maxProperties" in schema and len(value) > schema["maxProperties"]:
        raise ValidationError(f"Document failed validation at '{path}': too many properties")


def _validate_value(value: Any, schema: dict[str, Any], path: str, depth: int = 0) -> None:
    if value is None:
        bson_type = schema.get("bsonType") or schema.get("type")
        if bson_type:
            allowed = [bson_type] if isinstance(bson_type, str) else bson_type
            if "null" not in allowed:
                raise ValidationError(f"Document failed validation at '{path}': null not allowed")
        return

    bson_type = schema.get("bsonType") or schema.get("type")
    if bson_type:
        py_types = _resolve_py_types(bson_type)
        if py_types and not isinstance(value, py_types):
            raise ValidationError(
                f"Document failed validation at '{path}': expected type {bson_type}, got {type(value).__name__}"
            )

    if isinstance(value, dict):
        _validate_object(value, schema, path, depth)
    elif isinstance(value, list):
        _validate_array(value, schema, path, depth)
    else:
        _validate_scalar(value, schema, path)


def _validate_scalar(value: Any, schema: dict[str, Any], path: str) -> None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            raise ValidationError(
                f"Document failed validation at '{path}': value {value} < minimum {schema['minimum']}"
            )
        if "maximum" in schema and value > schema["maximum"]:
            raise ValidationError(
                f"Document failed validation at '{path}': value {value} > maximum {schema['maximum']}"
            )
        if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
            raise ValidationError(
                f"Document failed validation at '{path}': value {value} <= exclusiveMinimum"
            )
        if "exclusiveMaximum" in schema and value >= schema["exclusiveMaximum"]:
            raise ValidationError(
                f"Document failed validation at '{path}': value {value} >= exclusiveMaximum"
            )

    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            raise ValidationError(f"Document failed validation at '{path}': string too short")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            raise ValidationError(f"Document failed validation at '{path}': string too long")
        if "pattern" in schema and not _safe_regex(schema["pattern"]).search(value):
            raise ValidationError(f"Document failed validation at '{path}': pattern mismatch")

    if "enum" in schema and value not in schema["enum"]:
        raise ValidationError(
            f"Document failed validation at '{path}': value not in enum {schema['enum']}"
        )


def _validate_array(value: list[Any], schema: dict[str, Any], path: str, depth: int = 0) -> None:
    if "minItems" in schema and len(value) < schema["minItems"]:
        raise ValidationError(f"Document failed validation at '{path}': too few items")
    if "maxItems" in schema and len(value) > schema["maxItems"]:
        raise ValidationError(f"Document failed validation at '{path}': too many items")
    if schema.get("uniqueItems"):
        seen: list[Any] = []
        for item in value:
            if item in seen:
                raise ValidationError(f"Document failed validation at '{path}': duplicate items")
            seen.append(item)
    items_schema = schema.get("items")
    if items_schema:
        for i, item in enumerate(value):
            _validate_value(item, items_schema, f"{path}[{i}]", depth + 1)


def _join(base: str, field: str) -> str:
    return f"{base}.{field}" if base else field
