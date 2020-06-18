import pytest

from alluvium.avro import (
    AvroArraySchema,
    AvroComplexType,
    AvroCustomType,
    AvroEnumSchema,
    AvroFixedSchema,
    AvroMapSchema,
    AvroPrimitiveType,
    AvroRecordField,
    AvroRecordSchema,
    AvroUnionSchema,
    FieldOrdering,
    InvalidSchemaException,
)


def test_enum_schema_ok():
    schema = AvroEnumSchema("metasyntactic_enums", symbols=["FOO", "BAR", "BAZ"])
    assert schema.avro_schema() == {
        "name": "metasyntactic_enums",
        "type": "enum",
        "symbols": ["FOO", "BAR", "BAZ"],
    }


@pytest.mark.parametrize(
    "optional_kwargs", [{"namespace": "meta"}, {"aliases": ["coolio"]}, {"doc": "doc.com"}]
)
def test_enum_schema_ok_optional(optional_kwargs):
    schema = AvroEnumSchema("metasyntactic_enums", symbols=["FOO", "BAR", "BAZ"], **optional_kwargs)
    base = {"name": "metasyntactic_enums", "type": "enum", "symbols": ["FOO", "BAR", "BAZ"]}
    base.update(optional_kwargs)
    assert schema.avro_schema() == base


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"symbols": ["FOO", "BAR", "BAZ", "FOO"]},
        {"symbols": []},
        {"symbols": ["FOO", "BAR", "BAZ"], "default": "QUX"},
    ],
)
def test_enum_schema_error(bad_kwargs):
    with pytest.raises(InvalidSchemaException):
        AvroEnumSchema("metasyntactic_enums", **bad_kwargs)


def test_fixed_schema_ok():
    schema = AvroFixedSchema("stable", 12)
    assert schema.avro_schema() == {"name": "stable", "size": 12, "type": "fixed"}


@pytest.mark.parametrize("optional_kwargs", [{"namespace": "foo"}, {"aliases": ["alias"]}])
def test_fixed_schema_optional(optional_kwargs):
    schema = AvroFixedSchema("stable", 12, **optional_kwargs)
    base = {"name": "stable", "size": 12, "type": "fixed"}
    base.update(optional_kwargs)
    assert schema.avro_schema() == base


def test_union_schema_ok():
    schema = AvroUnionSchema(elements=[AvroPrimitiveType.INTEGER, AvroPrimitiveType.LONG])
    assert schema.avro_schema() == ["int", "long"]

    schema = AvroUnionSchema(elements=[AvroPrimitiveType.INTEGER, AvroFixedSchema("stable", 12)])
    assert schema.avro_schema() == ["int", {"name": "stable", "size": 12, "type": "fixed"}]


def test_union_schema_bad():
    with pytest.raises(InvalidSchemaException):
        AvroUnionSchema(elements=[])


def test_map_schema_ok():
    schema = AvroMapSchema(values=AvroPrimitiveType.INTEGER)
    assert schema.avro_schema() == {"type": "map", "values": "int"}

    schema = AvroMapSchema(values=AvroFixedSchema("stable", 12))
    assert schema.avro_schema() == {
        "type": "map",
        "values": {"name": "stable", "size": 12, "type": "fixed"},
    }


def test_array_schema_ok():
    schema = AvroArraySchema(items=AvroPrimitiveType.STRING)
    assert schema.avro_schema() == {"type": "array", "items": "string"}

    schema = AvroArraySchema(items=AvroFixedSchema("stable", 12))
    assert schema.avro_schema() == {
        "type": "array",
        "items": {"name": "stable", "size": 12, "type": "fixed"},
    }


def test_record_schema_ok():
    schema = AvroRecordSchema(
        name="order",
        fields=[
            AvroRecordField(name="order_id", avro_type=AvroPrimitiveType.INTEGER),
            AvroRecordField(
                name="product_ids", avro_type=AvroArraySchema(items=AvroPrimitiveType.INTEGER)
            ),
        ],
    )
    assert schema.avro_schema() == {
        "type": "record",
        "name": "order",
        "fields": [
            {"name": "order_id", "type": "int"},
            {"name": "product_ids", "type": {"type": "array", "items": "int"}},
        ],
    }


@pytest.mark.parametrize(
    "optional_kwargs", [{"doc": "foo"}, {"default": "foo"}, {"aliases": ["foo"]}]
)
def test_record_field_optional_ok(optional_kwargs):
    field = AvroRecordField(name="order_id", avro_type=AvroPrimitiveType.STRING, **optional_kwargs)
    base = {"name": "order_id", "type": "string"}
    base.update(optional_kwargs)
    assert field.generate_avro_field() == base


@pytest.mark.parametrize(
    "optional_kwargs", [{"doc": "foo"}, {"namespace": "foo"}, {"aliases": ["foo"]}]
)
def test_record_schema_optional_ok(optional_kwargs):
    schema = AvroRecordSchema(
        name="order",
        fields=[AvroRecordField(name="order_id", avro_type=AvroPrimitiveType.INTEGER)],
        **optional_kwargs
    )
    base = {"type": "record", "name": "order", "fields": [{"name": "order_id", "type": "int"}]}
    base.update(optional_kwargs)
    assert schema.avro_schema() == base


def test_record_schema_no_fields():
    with pytest.raises(InvalidSchemaException):
        AvroRecordSchema(name="foo", fields=[])


def test_all_custom_type():
    schema = AvroArraySchema(items=AvroCustomType.ANY)
    assert schema.avro_schema() == {
        "type": "array",
        "items": ["null", "boolean", "int", "long", "float", "double", "bytes", "string"],
    }
