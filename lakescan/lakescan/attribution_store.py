"""Streaming extraction of the two visible input sides into on-disk stores.

Both sides reuse the library's :class:`~lakescan.scan.Scanner`, so the lake
snapshot semantics (each snapshot's own active_files, POS/EQ deletes, field
identity by field_id, exact type widening, digest verification, read guards)
are applied exactly as everywhere else in the package.  What lives here is
the attribution-specific validation and the bounded-memory SQLite layout.

Stores (one work directory, all tables WITHOUT ROWID where possible, capped
page cache, chunked writers -- no full input is ever held in Python)::

  facts(fid BLOB PK, ek BLOB, bt INTEGER, kc INTEGER,
        fact_payload TEXT, fact_file_id TEXT, fact_row_pos INTEGER)
  dim(ek BLOB, rid BLOB, pub INTEGER, seq INTEGER PK, op INTEGER,
      vfrom INTEGER, vto INTEGER, prio INTEGER,
      dim_payload TEXT, dim_file_id TEXT, dim_row_pos INTEGER)

``seq`` is the insertion order of the visible dimension rows; the duplicate
identity@publication-time rule is enforced by a separate UNIQUE index so even
byte-identical twins reject the whole task.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from .attribution_config import AttributionConfig, SideConfig
from .errors import (
    AttributionConfigError,
    AttributionValidationError,
)
from .schema import Field, FieldType
from .scan import Scanner
from .table import TableContract

# operation codes stored in SQLite (integers index and compare cheaply)
OP_PUT = 1
OP_WITHDRAW = 2
_OP_NAMES = {"put": OP_PUT, "withdraw": OP_WITHDRAW}

_INSERT_CHUNK = 10000
_CACHE_PAGES = "-4096"  # ~4 MiB page cache per connection

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def _utf8_blob(value: str) -> sqlite3.Binary:
    """Raw UTF-8 bytes.

    The join domain and every ordering rule (fact-id output order, rule-id
    evidence order) use the exact UTF-8 byte sequence, so the raw encoding is
    both the equality form and the sort form -- no canonical tag/length
    prefix is needed.
    """
    return sqlite3.Binary(value.encode("utf-8"))


def exact_json_value(value: Any) -> Any:
    """Projected parquet value -> JSON-able exact form.

    Decimals become their exact string form (never float); bools, ints and
    strings pass through.  Mirrors :func:`lakescan.diff.value_to_jsonable` but
    is intentionally local so the attribution package stays self-contained.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return value
    raise AttributionValidationError(
        f"payload value of type {type(value).__name__} is not representable"
    )


def restore_json_value(ftype: FieldType, value: Any) -> Any:
    """Inverse of :func:`exact_json_value` for the exact supported types."""
    if value is None:
        return None
    kind = ftype.kind
    if kind == "utf8":
        return str(value)
    if kind in ("int32", "int64"):
        return int(value)
    if kind == "boolean":
        return bool(value)
    if kind == "decimal":
        return Decimal(str(value))
    raise AttributionConfigError(f"unsupported output payload type {kind}")


def _is_int64(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) \
        and INT64_MIN <= value <= INT64_MAX


def _require_role_field(contract: TableContract, side: SideConfig,
                        role: str, expect_kinds: tuple) -> Field:
    target = contract.schema(side.schema_id)
    fid = side.role(role)
    fld = target.get(fid)
    if fld is None:
        raise AttributionConfigError(
            f"{side.label} side role {role!r} references field_id {fid} which "
            f"is absent from target schema {side.schema_id}"
        )
    if fld.type.kind not in expect_kinds:
        raise AttributionConfigError(
            f"{side.label} side role {role!r} field_id {fid} has type "
            f"{fld.type.kind}; expected one of {list(expect_kinds)}"
        )
    return fld


def validate_side_schemas(cfg: AttributionConfig,
                          fact_contract: TableContract,
                          dim_contract: TableContract) -> None:
    """Everything rejectable before the first data file is opened.

    Role fields must exist in the side's target schema with the right type
    family; payload fields must exist there too.  Physical data-file schemas
    are re-checked by the scanner itself (exact widening, required fields).
    """
    f_target = fact_contract.schema(cfg.fact.schema_id)
    d_target = dim_contract.schema(cfg.dimension.schema_id)
    for role in ("fact_id", "fact_entity_key"):
        _require_role_field(fact_contract, cfg.fact, role, ("utf8",))
    for role in ("fact_business_time", "fact_knowledge_cutoff"):
        fld = _require_role_field(
            fact_contract, cfg.fact, role, ("int32", "int64"))
        if fld.type.kind != "int64":
            # int32 widens exactly into int64; the scanner will emit int64.
            # Only reject genuine narrowing situations (none other exist for
            # integer types), so merely document that int32 is accepted.
            pass
    for role in ("dim_entity_key", "dim_rule_id", "dim_operation"):
        _require_role_field(dim_contract, cfg.dimension, role, ("utf8",))
    for role in ("dim_published_at", "dim_valid_from", "dim_valid_to",
                 "dim_priority"):
        _require_role_field(
            dim_contract, cfg.dimension, role, ("int32", "int64"))
    for side, target in ((cfg.fact, f_target), (cfg.dimension, d_target)):
        for col in side.payload:
            fld = target.get(col.field_id)
            if fld is None:
                raise AttributionConfigError(
                    f"{side.label} payload field_id {col.field_id} is absent "
                    f"from target schema {side.schema_id}"
                )
            if fld.type.kind not in ("utf8", "int32", "int64", "decimal",
                                     "boolean"):
                raise AttributionConfigError(
                    f"{side.label} payload field_id {col.field_id} has "
                    f"unsupported type {fld.type.kind}"
                )


# ---------------------------------------------------------------------------
# on-disk stores
# ---------------------------------------------------------------------------


class AttributionStore:
    """SQLite backing store for both extracted sides, plus engine tables."""

    def __init__(self, db_path: str):
        if os.path.exists(db_path):
            os.remove(db_path)
        self.db = sqlite3.connect(db_path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("PRAGMA temp_store=FILE")
        self.db.execute(f"PRAGMA cache_size={_CACHE_PAGES}")
        self.db.execute(
            "CREATE TABLE facts("
            " fid BLOB PRIMARY KEY, ek BLOB NOT NULL, bt INTEGER NOT NULL,"
            " kc INTEGER NOT NULL, fact_payload TEXT NOT NULL,"
            " fact_file_id TEXT NOT NULL, fact_row_pos INTEGER NOT NULL"
            ") WITHOUT ROWID"
        )
        self.db.execute(
            "CREATE TABLE dim("
            " ek BLOB NOT NULL, rid BLOB NOT NULL, pub INTEGER NOT NULL,"
            " seq INTEGER PRIMARY KEY, op INTEGER NOT NULL,"
            " vfrom INTEGER, vto INTEGER, prio INTEGER,"
            " dim_payload TEXT, dim_file_id TEXT NOT NULL,"
            " dim_row_pos INTEGER NOT NULL)"
        )
        # identity (ek, rid) + publication time must be unique among visible
        # records -- including byte-identical twins.
        self.db.execute(
            "CREATE UNIQUE INDEX dim_ident_pub ON dim(ek, rid, pub)"
        )
        self.db.execute("CREATE INDEX dim_entity ON dim(ek, pub, seq)")
        # covering index for as-of version windows (the engine's hot path):
        # both the per-rule max(seq) selection and the interval rebuild are
        # served from the index without touching the dim table heap.
        self.db.execute(
            "CREATE INDEX dim_window ON dim(ek, pub, seq, rid, op, vfrom, "
            "vto, prio)"
        )
        self.fact_count = 0
        self.dim_count = 0

    def close(self) -> None:
        self.db.close()


# ---------------------------------------------------------------------------
# shared extraction helpers
# ---------------------------------------------------------------------------


@dataclass
class _SideRuntime:
    contract: TableContract
    side: SideConfig
    scanner: Scanner
    payload_fields: tuple  # tuple[Field] aligned with side.payload
    payload_ids: tuple
    name_by_id: dict


def _build_runtime(contract: TableContract, side: SideConfig,
                   work_dir: str, batch_size: int,
                   outer_guard) -> _SideRuntime:
    target = contract.schema(side.schema_id)
    payload_fields = tuple(target.field(c.field_id) for c in side.payload)
    all_ids = tuple(sorted(set(side.roles.values()) |
                           {c.field_id for c in side.payload}))
    name_by_id = {fid: target.field(fid).name for fid in all_ids}
    scanner = Scanner(contract.root, work_dir=work_dir, batch_size=batch_size)
    scanner.bind_read_guard(outer_guard)
    return _SideRuntime(
        contract=contract, side=side, scanner=scanner,
        payload_fields=payload_fields, payload_ids=all_ids,
        name_by_id=name_by_id,
    )


def _payload_json(rt: _SideRuntime, value_by_id: dict) -> str:
    obj = {
        str(col.field_id): exact_json_value(value_by_id.get(col.field_id))
        for col in rt.side.payload
    }
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


# ---------------------------------------------------------------------------
# fact extraction
# ---------------------------------------------------------------------------


def extract_facts(store: AttributionStore, contract: TableContract,
                  side: SideConfig, work_dir: str, batch_size: int,
                  outer_guard) -> int:
    rt = _build_runtime(contract, side, work_dir, batch_size, outer_guard)
    r = side.roles
    buf: list = []
    try:
        for sb in rt.scanner.scan(side.snapshot, side.schema_id,
                                  rt.payload_ids):
            names = sb.batch.schema.names
            cols = {n: sb.batch.column(n).to_pylist() for n in names}
            for i in range(sb.num_rows):
                values = {
                    fid: cols[rt.name_by_id[fid]][i]
                    for fid in rt.payload_ids
                }
                fid_val = values[r["fact_id"]]
                ek_val = values[r["fact_entity_key"]]
                bt = values[r["fact_business_time"]]
                kc = values[r["fact_knowledge_cutoff"]]
                if fid_val is None or not isinstance(fid_val, str) \
                        or not fid_val:
                    raise AttributionValidationError(
                        f"fact id must be a non-empty UTF-8 string; visible "
                        f"row ({sb.file_id}, {sb.positions[i]}) has "
                        f"{fid_val!r}"
                    )
                if ek_val is None or not isinstance(ek_val, str) or not ek_val:
                    raise AttributionValidationError(
                        f"fact entity key must be a non-empty UTF-8 string; "
                        f"visible row ({sb.file_id}, {sb.positions[i]}) "
                        f"has {ek_val!r}"
                    )
                if not _is_int64(bt):
                    raise AttributionValidationError(
                        f"fact business_time must be int64; visible row "
                        f"({sb.file_id}, {sb.positions[i]}) has {bt!r}"
                    )
                if not _is_int64(kc):
                    raise AttributionValidationError(
                        f"fact knowledge_cutoff must be int64; visible row "
                        f"({sb.file_id}, {sb.positions[i]}) has {kc!r}"
                    )
                blob_fid = _utf8_blob(fid_val)
                blob_ek = _utf8_blob(ek_val)
                buf.append((
                    blob_fid, blob_ek, bt, kc, _payload_json(rt, values),
                    sb.file_id, sb.positions[i],
                ))
                if len(buf) >= _INSERT_CHUNK:
                    _flush_facts(store, buf)
        if buf:
            _flush_facts(store, buf)
        store.db.commit()
        return store.fact_count
    except BaseException:
        store.db.commit()
        raise


def _flush_facts(store: AttributionStore, buf: list) -> None:
    try:
        store.db.executemany(
            "INSERT INTO facts(fid, ek, bt, kc, fact_payload, "
            "fact_file_id, fact_row_pos) VALUES (?, ?, ?, ?, ?, ?, ?)",
            buf,
        )
    except sqlite3.IntegrityError as e:
        # PRIMARY KEY violation -> duplicate visible fact id
        raise AttributionValidationError(
            f"duplicate fact id among visible rows; fact ids must be unique "
            f"(first duplicate near {buf[0][5]}/{buf[0][6]})"
        ) from e
    store.fact_count += len(buf)
    buf.clear()


# ---------------------------------------------------------------------------
# dimension extraction
# ---------------------------------------------------------------------------


def extract_dimensions(store: AttributionStore, contract: TableContract,
                       side: SideConfig, work_dir: str, batch_size: int,
                       outer_guard) -> int:
    rt = _build_runtime(contract, side, work_dir, batch_size, outer_guard)
    r = side.roles
    seq = 0
    buf: list = []
    try:
        for sb in rt.scanner.scan(side.snapshot, side.schema_id,
                                  rt.payload_ids):
            names = sb.batch.schema.names
            cols = {n: sb.batch.column(n).to_pylist() for n in names}
            for i in range(sb.num_rows):
                pos = sb.positions[i]
                coord = f"({sb.file_id}, {pos})"
                values = {
                    fid: cols[rt.name_by_id[fid]][i]
                    for fid in rt.payload_ids
                }
                ek_val = values[r["dim_entity_key"]]
                rid = values[r["dim_rule_id"]]
                pub = values[r["dim_published_at"]]
                op_text = values[r["dim_operation"]]
                if ek_val is None or not isinstance(ek_val, str) or not ek_val:
                    raise AttributionValidationError(
                        f"dimension entity key must be a non-empty UTF-8 "
                        f"string; visible row {coord} has {ek_val!r}"
                    )
                if rid is None or not isinstance(rid, str) or not rid:
                    raise AttributionValidationError(
                        f"rule id must be a non-empty UTF-8 string; visible "
                        f"row {coord} has {rid!r}"
                    )
                if not _is_int64(pub):
                    raise AttributionValidationError(
                        f"published_at must be int64; visible row {coord} "
                        f"has {pub!r}"
                    )
                if op_text not in _OP_NAMES:
                    raise AttributionValidationError(
                        f"operation must be 'put' or 'withdraw'; visible row "
                        f"{coord} has {op_text!r}"
                    )
                op = _OP_NAMES[op_text]
                vfrom = values[r["dim_valid_from"]]
                vto = values[r["dim_valid_to"]]
                prio = values[r["dim_priority"]]
                payload_json: Optional[str] = None
                if op == OP_PUT:
                    if not _is_int64(vfrom):
                        raise AttributionValidationError(
                            f"put.valid_from must be int64; visible row "
                            f"{coord} has {vfrom!r}"
                        )
                    if vto is not None and not _is_int64(vto):
                        raise AttributionValidationError(
                            f"put.valid_to must be null or int64; visible "
                            f"row {coord} has {vto!r}"
                        )
                    if vto is not None and not vfrom < vto:
                        raise AttributionValidationError(
                            f"put interval must satisfy valid_from < "
                            f"valid_to; visible row {coord} has "
                            f"[{vfrom}, {vto})"
                        )
                    if not _is_int64(prio):
                        raise AttributionValidationError(
                            f"put.priority must be a non-null int64; visible "
                            f"row {coord} has {prio!r}"
                        )
                    payload_json = _payload_json(rt, values)
                else:
                    # withdraw carries no interval, priority or payload
                    if vfrom is not None or vto is not None or prio is not None:
                        raise AttributionValidationError(
                            f"withdraw records must leave valid_from, "
                            f"valid_to and priority empty; visible row "
                            f"{coord} has ({vfrom!r}, {vto!r}, {prio!r})"
                        )
                    if _has_non_null_payload(rt, values):
                        raise AttributionValidationError(
                            f"withdraw records must carry no dimension "
                            f"payload; visible row {coord} has payload fields"
                        )
                blob_ek = _utf8_blob(ek_val)
                blob_rid = _utf8_blob(rid)
                buf.append((
                    blob_ek, blob_rid, pub, seq, op, vfrom, vto, prio,
                    payload_json, sb.file_id, pos,
                ))
                seq += 1
                if len(buf) >= _INSERT_CHUNK:
                    _flush_dim(store, buf)
        if buf:
            _flush_dim(store, buf)
        store.db.commit()
        store.dim_count = seq
        return seq
    except BaseException:
        store.db.commit()
        raise


def _has_non_null_payload(rt: _SideRuntime, value_by_id: dict) -> bool:
    return any(
        value_by_id.get(col.field_id) is not None for col in rt.side.payload
    )


def _flush_dim(store: AttributionStore, buf: list) -> None:
    try:
        store.db.executemany(
            "INSERT INTO dim(ek, rid, pub, seq, op, vfrom, vto, prio, "
            "dim_payload, dim_file_id, dim_row_pos) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            buf,
        )
    except sqlite3.IntegrityError as e:
        first = buf[0]
        raise AttributionValidationError(
            "two visible revision records share rule identity "
            "(entity_key, rule_id) and publication time -- including "
            f"identical twins; first seen near ({first[9]}, {first[10]})"
        ) from e
    buf.clear()
