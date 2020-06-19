"""
Defines the schema you expect from your CDC events.

Provides syntactic sugar for Any types and Either Types.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, List, Optional, Union


class InvalidSchemaException(Exception):
    def __init__(self, field_type, message, name=None):
        self.error_field_type = field_type
        self.name = name
        super(InvalidSchemaException, self).__init__(
            self, "Error with field <{} {}>. {}".format(field_type, name, message)
        )


class AvroPrimitiveType(Enum):
    NULL = "null"
    BOOLEAN = "boolean"
    INTEGER = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    BYTES = "bytes"
    STRING = "string"


class AvroComplexType(Enum):
    RECORD = "record"
    ENUM = "enum"
    ARRAY = "array"
    MAP = "map"
    UNIONS = "unions"
    FIXED = "fixed"


class AvroCustomType(Enum):
    ANY = "any"


def resolve_primitive_type(avro_type: Union[AvroPrimitiveType, AvroCustomType]):
    if isinstance(avro_type, AvroPrimitiveType):
        return avro_type.value
    if avro_type == AvroCustomType.ANY:
        return AvroUnionSchema([primitive for primitive in AvroPrimitiveType]).avro_schema()
    raise ValueError("Invalid base type provided")


class AvroSchema(ABC):
    def __init__(self, avro_type: Union[AvroPrimitiveType, AvroComplexType]):
        self.type = avro_type

    @abstractmethod
    def avro_schema(self):
        pass


class FieldOrdering(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"
    IGNORE = "ignore"


class AvroRecordField(object):
    def __init__(
        self,
        name: str,
        avro_field_type: Union[AvroSchema, AvroPrimitiveType],
        doc: str = None,
        default: Any = None,
        order: FieldOrdering = None,
        aliases: List[str] = None,
    ):
        self.name = name
        self.avro_field_type = avro_field_type
        self.doc = doc
        self.default = default
        self.order = order
        self.aliases = aliases

    def generate_avro_field(self):
        base = {
            "type": resolve_primitive_type(self.avro_field_type)
            if isinstance(self.avro_field_type, (AvroPrimitiveType, AvroCustomType))
            else self.avro_field_type.avro_schema(),
            "name": self.name,
        }
        if self.doc:
            base["doc"] = self.doc
        if self.default:
            base["default"] = self.default
        if self.order:
            base["order"] = self.order.value
        if self.aliases:
            base["aliases"] = self.aliases
        return base


class AvroRecordSchema(AvroSchema):
    avro_type = AvroComplexType.RECORD

    def __init__(
        self,
        name: str,
        fields: List[AvroRecordField],
        namespace: str = None,
        doc: str = None,
        aliases: List[str] = None,
    ):
        if not fields:
            raise InvalidSchemaException(
                self.avro_type, "Cannot provide a record schema without any fields.", name=name
            )
        self.name = name
        self.fields = fields
        self.namespace = namespace
        self.doc = doc
        self.aliases = aliases

    def avro_schema(self):
        base = {
            "type": self.avro_type.value,
            "name": self.name,
            "fields": [field.generate_avro_field() for field in self.fields],
        }
        if self.doc:
            base["doc"] = self.doc
        if self.namespace:
            base["namespace"] = self.namespace
        if self.aliases:
            base["aliases"] = self.aliases
        return base


class AvroArraySchema(AvroSchema):
    avro_type = AvroComplexType.ARRAY

    def __init__(self, items: Union[AvroSchema, AvroPrimitiveType]):
        self.items = items

    def avro_schema(self):
        return {
            "type": self.avro_type.value,
            "items": resolve_primitive_type(self.items)
            if isinstance(self.items, (AvroPrimitiveType, AvroCustomType))
            else self.items.avro_schema(),
        }


class AvroMapSchema(AvroSchema):
    avro_type = AvroComplexType.MAP

    def __init__(self, values: Union[AvroSchema, AvroPrimitiveType]):
        self.values = values

    def avro_schema(self):
        return {
            "type": self.avro_type.value,
            "values": resolve_primitive_type(self.values)
            if isinstance(self.values, (AvroPrimitiveType, AvroCustomType))
            else self.values.avro_schema(),
        }


class AvroUnionSchema(AvroSchema):
    avro_type = AvroComplexType.UNIONS

    def __init__(self, elements: List[Union[AvroSchema, AvroPrimitiveType]]):
        if not elements:
            raise InvalidSchemaException(
                self.avro_type, "Cannot provide a union schema without any values."
            )
        self.elements = elements

    def avro_schema(self):
        return [
            resolve_primitive_type(element)
            if isinstance(element, (AvroPrimitiveType, AvroCustomType))
            else element.avro_schema()
            for element in self.elements
        ]


class AvroFixedSchema(AvroSchema):
    avro_type = AvroComplexType.FIXED

    def __init__(self, name: str, size: int, namespace: str = None, aliases: List[str] = None):
        self.name = name
        self.size = size
        self.namespace = namespace
        self.aliases = aliases

    def avro_schema(self):
        base = {"type": self.avro_type.value, "name": self.name, "size": self.size}
        if self.namespace:
            base["namespace"] = self.namespace
        if self.aliases:
            base["aliases"] = self.aliases
        return base


class AvroEnumSchema(AvroSchema):
    avro_type = AvroComplexType.ENUM

    def __init__(
        self,
        name: str,
        symbols: List[str],
        namespace: str = None,
        aliases: List[str] = None,
        doc: str = None,
        default: str = None,
    ):
        self.name = name
        if not symbols:
            raise InvalidSchemaException(
                self.avro_type, "Must provide symbols to your enum symbol array.", self.name
            )
        if len(symbols) != len(set(symbols)):
            raise InvalidSchemaException(
                self.avro_type, "No duplicate symbols allowed for enum schema.", self.name
            )
        self.symbols = symbols
        self.namespace = namespace
        self.aliases = aliases
        self.doc = doc
        if default and default not in symbols:
            raise InvalidSchemaException(
                self.avro_type,
                "Provided default {} which must be in the possible symbols {}".format(
                    default, self.symbols
                ),
                self.name,
            )
        self.default = default

    def avro_schema(self):
        base = {"name": self.name, "type": self.avro_type.value, "symbols": self.symbols}
        if self.namespace:
            base["namespace"] = self.namespace
        if self.aliases:
            base["aliases"] = self.aliases
        if self.doc:
            base["doc"] = self.doc
        if self.default:
            base["default"] = self.default
        return base
