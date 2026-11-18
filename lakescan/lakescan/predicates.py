"""Scan predicates: eq / lt / is_null combined with AND / OR / NOT.

Evaluation follows SQL three-valued logic: comparisons against null yield
UNKNOWN, AND/OR/NOT are Kleene connectives, and only rows whose predicate
evaluates to TRUE are selected.

Literals are exact typed values.  Integer literals compare in the int64
domain; decimal literals are rescaled to the column's scale by exact integer
arithmetic (a literal that cannot be represented exactly is rejected, never
rounded); utf8 and boolean literals compare natively.  lt on boolean is
rejected.
"""

from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

import pyarrow as pa
import pyarrow.compute as pc

from .errors import PredicateError
from .schema import FieldType, INT64_MAX, INT64_MIN, Schema


@dataclass(frozen=True)
class Literal:
    kind: str  # null | int | utf8 | bool | decimal
    value: Any = None  # decimal: unscaled int
    scale: Optional[int] = None  # decimal only

    @staticmethod
    def parse(obj: Any) -> "Literal":
        if not isinstance(obj, dict) or "kind" not in obj:
            raise PredicateError(f"literal must be an object with kind: {obj!r}")
        kind = obj["kind"]
        if kind == "null":
            return Literal("null")
        if kind == "int":
            v = obj.get("value")
            if not isinstance(v, int) or isinstance(v, bool):
                raise PredicateError(f"int literal needs integer value: {obj!r}")
            if not (INT64_MIN <= v <= INT64_MAX):
                raise PredicateError(f"int literal out of int64 range: {v}")
            return Literal("int", v)
        if kind == "utf8":
            v = obj.get("value")
            if not isinstance(v, str):
                raise PredicateError(f"utf8 literal needs string value: {obj!r}")
            return Literal("utf8", v)
        if kind == "bool":
            v = obj.get("value")
            if not isinstance(v, bool):
                raise PredicateError(f"bool literal needs boolean value: {obj!r}")
            return Literal("bool", v)
        if kind == "decimal":
            unscaled, scale = obj.get("unscaled"), obj.get("scale")
            if (
                not isinstance(unscaled, int)
                or isinstance(unscaled, bool)
                or not isinstance(scale, int)
                or isinstance(scale, bool)
                or scale < 0
            ):
                raise PredicateError(f"decimal literal needs integer unscaled/scale: {obj!r}")
            return Literal("decimal", unscaled, scale)
        raise PredicateError(f"unknown literal kind: {kind!r}")

    def decimal_value(self) -> Decimal:
        return Decimal(self.value).scaleb(-self.scale)


# ---------------------------------------------------------------------------
# Predicate tree
# ---------------------------------------------------------------------------


class Pred:
    def field_ids(self) -> set:
        raise NotImplementedError


@dataclass(frozen=True)
class Eq(Pred):
    field_id: int
    literal: Literal

    def field_ids(self):
        return {self.field_id}


@dataclass(frozen=True)
class Lt(Pred):
    field_id: int
    literal: Literal

    def field_ids(self):
        return {self.field_id}


@dataclass(frozen=True)
class IsNull(Pred):
    field_id: int

    def field_ids(self):
        return {self.field_id}


@dataclass(frozen=True)
class And(Pred):
    args: tuple

    def field_ids(self):
        out = set()
        for a in self.args:
            out |= a.field_ids()
        return out


@dataclass(frozen=True)
class Or(Pred):
    args: tuple

    def field_ids(self):
        out = set()
        for a in self.args:
            out |= a.field_ids()
        return out


@dataclass(frozen=True)
class Not(Pred):
    arg: Pred

    def field_ids(self):
        return self.arg.field_ids()


def parse_predicate(obj: Any) -> Pred:
    if not isinstance(obj, dict) or "op" not in obj:
        raise PredicateError(f"predicate must be an object with op: {obj!r}")
    op = obj["op"]
    if op in ("eq", "lt"):
        fid = obj.get("field_id")
        if not isinstance(fid, int) or isinstance(fid, bool) or fid <= 0:
            raise PredicateError(f"{op}: field_id must be a positive int: {obj!r}")
        lit = Literal.parse(obj.get("literal"))
        return Eq(fid, lit) if op == "eq" else Lt(fid, lit)
    if op == "is_null":
        fid = obj.get("field_id")
        if not isinstance(fid, int) or isinstance(fid, bool) or fid <= 0:
            raise PredicateError(f"is_null: field_id must be a positive int: {obj!r}")
        return IsNull(fid)
    if op in ("and", "or"):
        args = obj.get("args")
        if not isinstance(args, list) or not args:
            raise PredicateError(f"{op}: args must be a non-empty list")
        parsed = tuple(parse_predicate(a) for a in args)
        return And(parsed) if op == "and" else Or(parsed)
    if op == "not":
        arg = obj.get("arg")
        if arg is None:
            raise PredicateError("not: missing arg")
        return Not(parse_predicate(arg))
    raise PredicateError(f"unknown predicate op: {op!r}")


# ---------------------------------------------------------------------------
# Validation against the target schema (exact typing, widening only)
# ---------------------------------------------------------------------------


def _rescale_decimal_literal(lit: Literal, ftype: FieldType) -> tuple:
    """Return (unscaled, precision) of the literal at the field's scale.

    Exact integer arithmetic only; a literal that cannot be represented at the
    column scale exactly is rejected.
    """
    assert lit.kind == "decimal"
    ds = lit.scale - ftype.scale
    if ds >= 0:
        # Python % is exact: nonzero remainder means the literal cannot be
        # represented at the column scale without rounding -> reject.
        if lit.value % (10**ds) != 0:
            raise PredicateError(
                f"decimal literal scale {lit.scale} cannot be represented exactly "
                f"at column scale {ftype.scale}"
            )
        unscaled = lit.value // (10**ds)
    else:
        unscaled = lit.value * (10 ** (-ds))
    digits = len(str(abs(unscaled))) if unscaled else 1
    precision = max(ftype.precision, digits)
    return unscaled, precision


def validate_predicate(pred: Pred, schema: Schema) -> None:
    from .errors import UnknownFieldError

    for fid in pred.field_ids():
        if fid not in schema:
            raise UnknownFieldError(
                f"predicate references field_id {fid} not in target schema "
                f"{schema.schema_id}"
            )
    _validate_node(pred, schema)


def _validate_node(pred: Pred, schema: Schema) -> None:
    if isinstance(pred, (Eq, Lt)):
        f = schema.field(pred.field_id)
        lit, ft = pred.literal, f.type
        if lit.kind == "null":
            return  # always UNKNOWN; allowed
        ok = True
        if ft.kind in ("int32", "int64"):
            ok = lit.kind == "int"
        elif ft.kind == "decimal":
            ok = lit.kind == "decimal"
            if ok:
                _rescale_decimal_literal(lit, ft)
        elif ft.kind == "utf8":
            ok = lit.kind == "utf8"
        elif ft.kind == "boolean":
            ok = lit.kind == "bool"
            if isinstance(pred, Lt):
                raise PredicateError("lt is not supported on boolean fields")
        if not ok:
            raise PredicateError(
                f"literal kind {lit.kind} does not match field {pred.field_id} "
                f"type {ft.kind}"
            )
    elif isinstance(pred, IsNull):
        pass
    elif isinstance(pred, (And, Or)):
        for a in pred.args:
            _validate_node(a, schema)
    elif isinstance(pred, Not):
        _validate_node(pred.arg, schema)


# ---------------------------------------------------------------------------
# Batch evaluation (Kleene three-valued logic via pyarrow)
# ---------------------------------------------------------------------------


def evaluate(pred: Pred, column) -> pa.Array:
    """column: field_id -> pa.Array (already widened to the target type).

    Returns a boolean array where null means UNKNOWN.
    """
    if isinstance(pred, Eq):
        return _compare(pred, column, pc.equal)
    if isinstance(pred, Lt):
        return _compare(pred, column, pc.less)
    if isinstance(pred, IsNull):
        return pc.is_null(column(pred.field_id))
    if isinstance(pred, And):
        out = evaluate(pred.args[0], column)
        for a in pred.args[1:]:
            out = pc.and_kleene(out, evaluate(a, column))
        return out
    if isinstance(pred, Or):
        out = evaluate(pred.args[0], column)
        for a in pred.args[1:]:
            out = pc.or_kleene(out, evaluate(a, column))
        return out
    if isinstance(pred, Not):
        return pc.invert(evaluate(pred.arg, column))
    raise PredicateError(f"unknown predicate node: {pred!r}")  # pragma: no cover


def _compare(pred, column, kernel) -> pa.Array:
    arr = column(pred.field_id)
    lit = pred.literal
    if lit.kind == "null":
        return pa.nulls(len(arr), pa.bool_())
    pa_type = arr.type
    if lit.kind == "int":
        scalar = pa.scalar(lit.value, type=pa_type)
    elif lit.kind == "utf8":
        scalar = pa.scalar(lit.value, type=pa_type)
    elif lit.kind == "bool":
        scalar = pa.scalar(lit.value, type=pa_type)
    elif lit.kind == "decimal":
        ftype_scale = pa_type.scale
        ds = lit.scale - ftype_scale
        if ds >= 0:
            unscaled = lit.value // (10**ds)
        else:
            unscaled = lit.value * (10 ** (-ds))
        precision = max(pa_type.precision, len(str(abs(unscaled))) if unscaled else 1)
        value = Decimal(unscaled).scaleb(-ftype_scale)
        scalar = pa.scalar(value, type=pa.decimal128(precision, ftype_scale))
        if scalar.type != pa_type:
            arr = pc.cast(arr, scalar.type)  # exact precision widening
    else:  # pragma: no cover
        raise PredicateError(f"bad literal {lit}")
    return kernel(arr, scalar)


def true_mask(pred: Pred, column) -> pa.Array:
    """Boolean mask (no nulls): exactly the rows where pred is TRUE."""
    m = evaluate(pred, column)
    return pc.fill_null(m, False)


# ---------------------------------------------------------------------------
# Conservative statistics pruning.
#
# definitely_not_true() may only return True when NO row in the row group can
# evaluate the predicate to TRUE.  Missing or untrusted statistics always keep
# the candidate.  Output must be identical to a full scan without pruning.
# ---------------------------------------------------------------------------


class ColStats:
    __slots__ = ("present", "has_min_max", "min", "max", "null_count", "min_max_exact")

    def __init__(self, present=False, has_min_max=False, vmin=None, vmax=None,
                 null_count=None, min_max_exact=False):
        self.present = present
        self.has_min_max = has_min_max
        self.min = vmin
        self.max = vmax
        self.null_count = null_count
        self.min_max_exact = min_max_exact


def definitely_not_true(pred: Pred, stats_for) -> bool:
    """stats_for: field_id -> ColStats (of the physical column, or absent)."""
    if isinstance(pred, Eq):
        st = stats_for(pred.field_id)
        if not st.present:
            return True  # column all-null -> eq is never TRUE
        if pred.literal.kind == "null":
            return True
        return _eq_out_of_range(pred.literal, st)
    if isinstance(pred, Lt):
        st = stats_for(pred.field_id)
        if not st.present:
            return True
        if pred.literal.kind == "null":
            return True
        return _lt_out_of_range(pred.literal, st)
    if isinstance(pred, IsNull):
        st = stats_for(pred.field_id)
        if not st.present:
            return False  # all-null column: is_null is TRUE everywhere
        if st.null_count == 0:
            return True
        return False  # unknown null_count -> keep candidate
    if isinstance(pred, And):
        return any(definitely_not_true(a, stats_for) for a in pred.args)
    if isinstance(pred, Or):
        return all(definitely_not_true(a, stats_for) for a in pred.args)
    if isinstance(pred, Not):
        return False  # conservative: never prune on NOT
    return False  # pragma: no cover


def _lit_py_value(lit: Literal):
    if lit.kind == "int":
        return lit.value
    if lit.kind == "utf8":
        return lit.value
    if lit.kind == "bool":
        return lit.value
    if lit.kind == "decimal":
        return lit.decimal_value()
    return None


def _stats_usable(lit: Literal, st: ColStats) -> bool:
    if not st.has_min_max or st.min is None or st.max is None:
        return False
    if lit.kind == "utf8" and not st.min_max_exact:
        return False  # possibly truncated string stats -> untrusted
    return True


def _eq_out_of_range(lit: Literal, st: ColStats) -> bool:
    if not _stats_usable(lit, st):
        return False
    v = _lit_py_value(lit)
    try:
        return v < st.min or v > st.max
    except TypeError:
        return False  # incomparable -> keep candidate


def _lt_out_of_range(lit: Literal, st: ColStats) -> bool:
    if not _stats_usable(lit, st):
        return False
    v = _lit_py_value(lit)
    try:
        return st.min >= v
    except TypeError:
        return False
