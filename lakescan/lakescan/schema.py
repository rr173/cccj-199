"""Schema model: field identity by positive integer field_id, exact widening rules.

Supported top-level primitive types: int32, int64, decimal(precision, scale),
utf8, boolean.  Renames never change identity; only field_id does.  The only
allowed evolutions are exact widenings:

  int32            -> int64
  decimal(p, s)    -> decimal(p', s)   with p' >= p   (scale must be identical)
  utf8             -> utf8
  boolean          -> boolean
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Optional

import pyarrow as pa

from .errors import (
    ContractError,
    DuplicateFieldError,
    TypeEvolutionError,
    UnknownFieldError,
)

INT32_MIN, INT32_MAX = -(2**31), 2**31 - 1
INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1

_KINDS = ("int32", "int64", "decimal", "utf8", "boolean")


@dataclass(frozen=True)
class FieldType:
    kind: str
    precision: Optional[int] = None  # decimal only
    scale: Optional[int] = None  # decimal only

    def __post_init__(self):
        if self.kind not in _KINDS:
            raise ContractError(f"unknown type kind: {self.kind!r}")
        if self.kind == "decimal":
            p, s = self.precision, self.scale
            if not isinstance(p, int) or not isinstance(s, int):
                raise ContractError("decimal requires integer precision and scale")
            if not (1 <= p <= 38) or not (0 <= s <= p):
                raise ContractError(f"illegal decimal({p},{s})")
        elif self.precision is not None or self.scale is not None:
            raise ContractError(f"type {self.kind} takes no precision/scale")

    # -- factories ---------------------------------------------------------
    @staticmethod
    def parse(obj: Any) -> "FieldType":
        if not isinstance(obj, dict):
            raise ContractError(f"type must be an object, got {obj!r}")
        kind = obj.get("kind")
        if kind == "decimal":
            return FieldType("decimal", obj.get("precision"), obj.get("scale"))
        return FieldType(kind)

    def to_json(self) -> dict:
        if self.kind == "decimal":
            return {"kind": "decimal", "precision": self.precision, "scale": self.scale}
        return {"kind": self.kind}

    # -- widening ----------------------------------------------------------
    def widens_to(self, other: "FieldType") -> bool:
        """True iff every value of ``self`` is exactly representable in ``other``."""
        if self.kind == other.kind == "decimal":
            return self.scale == other.scale and other.precision >= self.precision
        if self.kind == other.kind:
            return True
        if self.kind == "int32" and other.kind == "int64":
            return True
        return False

    def common_exact(self, other: "FieldType") -> Optional["FieldType"]:
        """The wider of the two types if exactly comparable, else None."""
        if self.widens_to(other):
            return other
        if other.widens_to(self):
            return self
        return None

    def to_pyarrow(self) -> pa.DataType:
        if self.kind == "int32":
            return pa.int32()
        if self.kind == "int64":
            return pa.int64()
        if self.kind == "decimal":
            return pa.decimal128(self.precision, self.scale)
        if self.kind == "utf8":
            return pa.string()
        if self.kind == "boolean":
            return pa.bool_()
        raise ContractError(f"unknown type kind: {self.kind!r}")  # pragma: no cover

    def accepts_physical(self, pa_type: pa.DataType) -> bool:
        """Whether a parquet physical column type matches this declared type."""
        if self.kind == "utf8":
            return pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type)
        return pa_type == self.to_pyarrow()


@dataclass(frozen=True)
class Field:
    field_id: int
    name: str
    type: FieldType
    required: bool = False


class Schema:
    """An immutable schema: a set of top-level fields keyed by field_id."""

    def __init__(self, schema_id: int, fields: tuple[Field, ...]):
        if not isinstance(schema_id, int) or schema_id < 0:
            raise ContractError(f"schema_id must be a non-negative int, got {schema_id!r}")
        self.schema_id = schema_id
        self.fields = fields
        by_id: dict[int, Field] = {}
        by_name: dict[str, Field] = {}
        for f in fields:
            if not isinstance(f.field_id, int) or f.field_id <= 0:
                raise ContractError(f"field_id must be a positive int, got {f.field_id!r}")
            if f.field_id in by_id:
                raise DuplicateFieldError(
                    f"schema {schema_id}: duplicate field_id {f.field_id}"
                )
            if f.name in by_name:
                raise DuplicateFieldError(
                    f"schema {schema_id}: duplicate physical name {f.name!r}"
                )
            by_id[f.field_id] = f
            by_name[f.name] = f
        self._by_id = by_id
        self._by_name = by_name

    @staticmethod
    def parse(obj: Any) -> "Schema":
        if not isinstance(obj, dict) or "schema_id" not in obj or "fields" not in obj:
            raise ContractError(f"schema must have schema_id and fields: {obj!r}")
        fields = []
        for fo in obj["fields"]:
            try:
                fields.append(
                    Field(
                        field_id=fo["field_id"],
                        name=fo["name"],
                        type=FieldType.parse(fo["type"]),
                        required=bool(fo.get("required", False)),
                    )
                )
            except KeyError as e:
                raise ContractError(f"field missing key {e}: {fo!r}") from e
            if not isinstance(fo["name"], str) or not fo["name"]:
                raise ContractError(f"field name must be a non-empty string: {fo!r}")
        return Schema(obj["schema_id"], tuple(fields))

    def field(self, field_id: int) -> Field:
        try:
            return self._by_id[field_id]
        except KeyError:
            raise UnknownFieldError(
                f"field_id {field_id} not in schema {self.schema_id}"
            ) from None

    def has(self, field_id: int) -> bool:
        return field_id in self._by_id

    def get(self, field_id: int) -> Optional[Field]:
        return self._by_id.get(field_id)

    def __contains__(self, field_id: int) -> bool:
        return field_id in self._by_id


def validate_cross_schema_compatibility(schemas: dict[int, Schema]) -> None:
    """Every field_id must evolve only by exact widening across all schemas."""
    by_id: dict[int, list[tuple[int, FieldType]]] = {}
    for sid, schema in schemas.items():
        for f in schema.fields:
            by_id.setdefault(f.field_id, []).append((sid, f.type))
    for fid, occurrences in by_id.items():
        for i in range(len(occurrences)):
            for j in range(i + 1, len(occurrences)):
                sid_a, ta = occurrences[i]
                sid_b, tb = occurrences[j]
                if ta.common_exact(tb) is None:
                    raise TypeEvolutionError(
                        f"field_id {fid}: incompatible types {ta} (schema {sid_a}) "
                        f"vs {tb} (schema {sid_b}); only exact widening is allowed"
                    )


# ---------------------------------------------------------------------------
# Canonical key encoding for equality deletes.
#
# Exact, type-tagged, order-stable.  Values are encoded in their *canonical
# comparison domain*: all integers as signed 8-byte big-endian (int32 values
# widen exactly), decimals as (scale, unscaled big-endian bytes) -- scales are
# guaranteed equal by the widening rules -- utf8 as raw bytes, boolean as one
# byte.  Null sorts/encodes distinctly from every non-null value and null
# equals null.  No stringification, no float approximation, no name matching.
# ---------------------------------------------------------------------------

_ENC_NULL = b"\x00"
_ENC_VAL = b"\x01"


def encode_key_value(ftype: FieldType, value: Any) -> bytes:
    if value is None:
        return _ENC_NULL
    if ftype.kind in ("int32", "int64"):
        v = int(value)
        if not (INT64_MIN <= v <= INT64_MAX):
            raise ContractError(f"integer key out of int64 range: {v}")
        return _ENC_VAL + struct.pack(">q", v)
    if ftype.kind == "decimal":
        # value: python int unscaled, or Decimal
        unscaled = _decimal_unscaled(value, ftype.scale)
        nbytes = max(1, (unscaled.bit_length() + 8) // 8)
        body = unscaled.to_bytes(nbytes, "big", signed=True)
        return _ENC_VAL + struct.pack(">i", ftype.scale) + struct.pack(">I", len(body)) + body
    if ftype.kind == "utf8":
        b = str(value).encode("utf-8")
        return _ENC_VAL + struct.pack(">I", len(b)) + b
    if ftype.kind == "boolean":
        return _ENC_VAL + (b"\x01" if value else b"\x00")
    raise ContractError(f"cannot encode key of type {ftype.kind}")  # pragma: no cover


def _decimal_unscaled(value: Any, scale: int) -> int:
    """Exact unscaled integer of a decimal value at the given scale."""
    from decimal import Decimal

    if isinstance(value, Decimal):
        scaled = value.scaleb(scale)
        if scaled != scaled.to_integral_value():
            raise ContractError(f"decimal {value} not representable at scale {scale}")
        return int(scaled)
    if isinstance(value, int):
        return value * (10**scale)
    raise ContractError(f"unsupported decimal key value: {value!r}")


def encode_key_tuple(field_types: list[FieldType], values: list[Any]) -> bytes:
    """Concatenated encoding; unambiguous because field order/types are fixed."""
    if len(field_types) == 1:  # hot path for the common single-field key
        return encode_key_value(field_types[0], values[0])
    return b"".join(encode_key_value(t, v) for t, v in zip(field_types, values))
