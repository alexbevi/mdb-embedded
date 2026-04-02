"""Tests for smongo.schema -- $jsonSchema validation."""

import pytest

from smongo.schema import ValidationError, validate_document


class TestSchemaBasic:
    def test_empty_schema_passes(self):
        validate_document({"x": 1}, {})

    def test_none_schema_passes(self):
        validate_document({"x": 1}, None)

    def test_falsy_schema_passes(self):
        validate_document({"x": 1}, 0)


class TestSchemaRequired:
    def test_required_present(self):
        schema = {"required": ["name", "age"]}
        validate_document({"name": "Alice", "age": 30}, schema)

    def test_required_missing(self):
        schema = {"required": ["name", "age"]}
        with pytest.raises(ValidationError, match="missing required field 'age'"):
            validate_document({"name": "Alice"}, schema)

    def test_required_all_missing(self):
        schema = {"required": ["name"]}
        with pytest.raises(ValidationError, match="missing required field 'name'"):
            validate_document({}, schema)


class TestSchemaType:
    def test_bson_type_object(self):
        schema = {"bsonType": "object"}
        validate_document({"x": 1}, schema)

    def test_bson_type_string_field(self):
        schema = {"properties": {"name": {"bsonType": "string"}}}
        validate_document({"name": "Alice"}, schema)

    def test_bson_type_string_wrong(self):
        schema = {"properties": {"name": {"bsonType": "string"}}}
        with pytest.raises(ValidationError, match="expected type string"):
            validate_document({"name": 42}, schema)

    def test_bson_type_int(self):
        schema = {"properties": {"age": {"bsonType": "int"}}}
        validate_document({"age": 30}, schema)

    def test_bson_type_double(self):
        schema = {"properties": {"score": {"bsonType": "double"}}}
        validate_document({"score": 3.14}, schema)

    def test_bson_type_bool(self):
        schema = {"properties": {"active": {"bsonType": "bool"}}}
        validate_document({"active": True}, schema)

    def test_bson_type_array(self):
        schema = {"properties": {"tags": {"bsonType": "array"}}}
        validate_document({"tags": [1, 2]}, schema)

    def test_bson_type_null_allowed(self):
        schema = {"properties": {"x": {"bsonType": ["string", "null"]}}}
        validate_document({"x": None}, schema)

    def test_bson_type_null_disallowed(self):
        schema = {"properties": {"x": {"bsonType": "string"}}}
        with pytest.raises(ValidationError, match="null not allowed"):
            validate_document({"x": None}, schema)

    def test_type_alias(self):
        schema = {"properties": {"x": {"type": "string"}}}
        validate_document({"x": "hello"}, schema)


class TestSchemaProperties:
    def test_nested_properties(self):
        schema = {
            "properties": {
                "address": {
                    "bsonType": "object",
                    "properties": {
                        "zip": {"bsonType": "string"},
                    },
                },
            },
        }
        validate_document({"address": {"zip": "10001"}}, schema)

    def test_nested_property_wrong_type(self):
        schema = {
            "properties": {
                "address": {
                    "bsonType": "object",
                    "properties": {
                        "zip": {"bsonType": "string"},
                    },
                },
            },
        }
        with pytest.raises(ValidationError, match="address.zip"):
            validate_document({"address": {"zip": 10001}}, schema)

    def test_property_not_present_skipped(self):
        schema = {"properties": {"name": {"bsonType": "string"}}}
        validate_document({}, schema)  # name not present, no error


class TestSchemaAdditionalProperties:
    def test_additional_properties_false_rejects(self):
        schema = {
            "properties": {"name": {"bsonType": "string"}},
            "additionalProperties": False,
        }
        with pytest.raises(ValidationError, match="additional property.*not allowed"):
            validate_document({"name": "Alice", "extra": 1}, schema)

    def test_additional_properties_allows_id(self):
        schema = {
            "properties": {"name": {"bsonType": "string"}},
            "additionalProperties": False,
        }
        validate_document({"_id": "abc", "name": "Alice"}, schema)

    def test_additional_properties_true_allows(self):
        schema = {
            "properties": {"name": {"bsonType": "string"}},
        }
        validate_document({"name": "Alice", "extra": 1}, schema)


class TestSchemaMinMaxProperties:
    def test_min_properties_ok(self):
        schema = {"minProperties": 2}
        validate_document({"a": 1, "b": 2}, schema)

    def test_min_properties_fail(self):
        schema = {"minProperties": 3}
        with pytest.raises(ValidationError, match="too few properties"):
            validate_document({"a": 1}, schema)

    def test_max_properties_ok(self):
        schema = {"maxProperties": 2}
        validate_document({"a": 1, "b": 2}, schema)

    def test_max_properties_fail(self):
        schema = {"maxProperties": 1}
        with pytest.raises(ValidationError, match="too many properties"):
            validate_document({"a": 1, "b": 2}, schema)


class TestSchemaNumericConstraints:
    def test_minimum(self):
        schema = {"properties": {"x": {"bsonType": "int", "minimum": 0}}}
        validate_document({"x": 5}, schema)

    def test_minimum_fail(self):
        schema = {"properties": {"x": {"bsonType": "int", "minimum": 10}}}
        with pytest.raises(ValidationError, match="< minimum"):
            validate_document({"x": 5}, schema)

    def test_maximum(self):
        schema = {"properties": {"x": {"bsonType": "int", "maximum": 100}}}
        validate_document({"x": 50}, schema)

    def test_maximum_fail(self):
        schema = {"properties": {"x": {"bsonType": "int", "maximum": 10}}}
        with pytest.raises(ValidationError, match="> maximum"):
            validate_document({"x": 50}, schema)

    def test_exclusive_minimum(self):
        schema = {"properties": {"x": {"bsonType": "int", "exclusiveMinimum": 5}}}
        with pytest.raises(ValidationError, match="exclusiveMinimum"):
            validate_document({"x": 5}, schema)

    def test_exclusive_maximum(self):
        schema = {"properties": {"x": {"bsonType": "int", "exclusiveMaximum": 10}}}
        with pytest.raises(ValidationError, match="exclusiveMaximum"):
            validate_document({"x": 10}, schema)

    def test_bool_excluded_from_numeric_checks(self):
        schema = {"properties": {"x": {"minimum": 5}}}
        validate_document({"x": True}, schema)  # bool not checked


class TestSchemaStringConstraints:
    def test_min_length(self):
        schema = {"properties": {"s": {"bsonType": "string", "minLength": 3}}}
        with pytest.raises(ValidationError, match="string too short"):
            validate_document({"s": "ab"}, schema)

    def test_max_length(self):
        schema = {"properties": {"s": {"bsonType": "string", "maxLength": 3}}}
        with pytest.raises(ValidationError, match="string too long"):
            validate_document({"s": "abcdef"}, schema)

    def test_pattern(self):
        schema = {"properties": {"email": {"bsonType": "string", "pattern": r"@"}}}
        validate_document({"email": "a@b.com"}, schema)

    def test_pattern_fail(self):
        schema = {"properties": {"email": {"bsonType": "string", "pattern": r"@"}}}
        with pytest.raises(ValidationError, match="pattern mismatch"):
            validate_document({"email": "nope"}, schema)


class TestSchemaEnum:
    def test_enum_valid(self):
        schema = {"properties": {"status": {"enum": ["active", "inactive"]}}}
        validate_document({"status": "active"}, schema)

    def test_enum_invalid(self):
        schema = {"properties": {"status": {"enum": ["active", "inactive"]}}}
        with pytest.raises(ValidationError, match="value not in enum"):
            validate_document({"status": "deleted"}, schema)


class TestSchemaArray:
    def test_min_items(self):
        schema = {"properties": {"tags": {"bsonType": "array", "minItems": 2}}}
        with pytest.raises(ValidationError, match="too few items"):
            validate_document({"tags": [1]}, schema)

    def test_max_items(self):
        schema = {"properties": {"tags": {"bsonType": "array", "maxItems": 2}}}
        with pytest.raises(ValidationError, match="too many items"):
            validate_document({"tags": [1, 2, 3]}, schema)

    def test_unique_items(self):
        schema = {"properties": {"tags": {"bsonType": "array", "uniqueItems": True}}}
        with pytest.raises(ValidationError, match="duplicate items"):
            validate_document({"tags": [1, 2, 1]}, schema)

    def test_items_sub_schema(self):
        schema = {"properties": {"scores": {"bsonType": "array", "items": {"bsonType": "int"}}}}
        validate_document({"scores": [1, 2, 3]}, schema)

    def test_items_sub_schema_fail(self):
        schema = {"properties": {"scores": {"bsonType": "array", "items": {"bsonType": "int"}}}}
        with pytest.raises(ValidationError, match="expected type int"):
            validate_document({"scores": [1, "two", 3]}, schema)
