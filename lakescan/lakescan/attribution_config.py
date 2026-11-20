"""Mapping configuration for the bitemporal fact attribution task.

One JSON document wires two local lake-table directories (a *fact* side and a
*dimension/revision* side), the snapshot and target schema to read on each
side, the field roles each side contributes, the field ids of the output
payload columns, and the one-shot output directory.

Role vocabulary
---------------

Fact side roles (all values are positive integer field ids)::

    fact_id            non-null unique UTF-8 fact identity
    fact_entity_key    non-null UTF-8 entity key (join domain)
    fact_business_time int64 business event time
    fact_knowledge_cutoff int64 as-of knowledge cutoff
    fact_payload       list of field ids copied verbatim into the output

Dimension side roles::

    dim_entity_key     non-null UTF-8 entity key
    dim_rule_id        non-null UTF-8 rule identity
    dim_published_at   int64 publication time of the revision record
    dim_operation      UTF-8 "put" | "withdraw"
    dim_valid_from     int64 inclusive validity start (put only)
    dim_valid_to       nullable int64 exclusive validity end (put only)
    dim_priority       non-null int64 priority (put only)
    dim_payload        list of field ids copied for matched/ambiguous evidence

Output wiring::

    output_dir         a directory that must not yet exist
    output_fact_payload  field ids (from the fact target schema) and output
                         column names for the fact payload columns
    output_dim_payload   field ids (from the dimension target schema) and
                         output column names for the dimension payload columns

The fixed output columns (status, rule id, publication time, provenance, the
two timestamps, ...) have stable reserved names; payload columns are named by
the config so no collision with a reserved name is possible.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .errors import AttributionConfigError

# Reserved output column names; payload columns may never reuse them.
RESERVED_COLUMNS = (
    "fact_id",
    "entity_key",
    "business_time",
    "knowledge_cutoff",
    "attribution_status",
    "matched_rule_id",
    "matched_published_at",
    "matched_dim_file_id",
    "matched_dim_row_position",
    "candidate_count",
    "evidence_1_rule_id",
    "evidence_1_published_at",
    "evidence_1_dim_file_id",
    "evidence_1_dim_row_position",
    "evidence_2_rule_id",
    "evidence_2_published_at",
    "evidence_2_dim_file_id",
    "evidence_2_dim_row_position",
    "fact_file_id",
    "fact_row_position",
)

_FACT_ROLE_IDS = (
    "fact_id",
    "fact_entity_key",
    "fact_business_time",
    "fact_knowledge_cutoff",
)
_DIM_ROLE_IDS = (
    "dim_entity_key",
    "dim_rule_id",
    "dim_published_at",
    "dim_operation",
    "dim_valid_from",
    "dim_valid_to",
    "dim_priority",
)
_SIDE_KEYS = ("table", "snapshot", "schema")


@dataclass(frozen=True)
class PayloadColumn:
    """One output payload column: source field id and output column name."""

    field_id: int
    name: str


@dataclass(frozen=True)
class SideConfig:
    label: str  # "fact" | "dimension"
    table: str
    snapshot: str
    schema_id: int
    roles: dict  # role name -> field id
    payload: tuple  # tuple[PayloadColumn]

    def role(self, name: str) -> int:
        try:
            return self.roles[name]
        except KeyError:
            raise AttributionConfigError(
                f"{self.label} side is missing required role {name!r}"
            ) from None


@dataclass(frozen=True)
class AttributionConfig:
    fact: SideConfig
    dimension: SideConfig
    output_dir: str
    work_dir: Optional[str] = None
    batch_size: int = 8192

    @staticmethod
    def parse(obj: Any) -> "AttributionConfig":
        if not isinstance(obj, dict):
            raise AttributionConfigError(
                f"mapping config must be a JSON object, got {type(obj).__name__}"
            )
        fact = _parse_side(obj, "fact", _FACT_ROLE_IDS, ("fact_payload",))
        dim = _parse_side(obj, "dimension", _DIM_ROLE_IDS, ("dim_payload",))
        output_dir = obj.get("output_dir")
        if not isinstance(output_dir, str) or not output_dir:
            raise AttributionConfigError(
                "output_dir must be a non-empty string naming a new directory"
            )
        work_dir = obj.get("work_dir")
        if work_dir is not None and (
            not isinstance(work_dir, str) or not work_dir
        ):
            raise AttributionConfigError("work_dir must be a non-empty string")
        batch_size = obj.get("batch_size", 8192)
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size <= 0
        ):
            raise AttributionConfigError(
                f"batch_size must be a positive int, got {batch_size!r}"
            )
        cfg = AttributionConfig(
            fact=fact, dimension=dim, output_dir=output_dir,
            work_dir=work_dir, batch_size=batch_size,
        )
        _check_output_name_uniqueness(cfg)
        return cfg

    @staticmethod
    def load(path) -> "AttributionConfig":
        p = Path(path)
        if not p.is_file():
            raise AttributionConfigError(f"mapping config not found: {p}")
        with open(p, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return AttributionConfig.parse(raw)

    def all_role_field_ids(self) -> set:
        return set(self.fact.roles.values()) | set(self.dimension.roles.values())


def _parse_side(raw: dict, label: str, required_roles: tuple,
                payload_keys: tuple) -> SideConfig:
    side = raw.get(label)
    if not isinstance(side, dict):
        raise AttributionConfigError(f"config section {label!r} must be an object")
    for key in _SIDE_KEYS:
        if key not in side:
            raise AttributionConfigError(f"{label} section missing {key!r}")
    if not isinstance(side["table"], str) or not side["table"]:
        raise AttributionConfigError(f"{label}.table must be a non-empty string")
    if not isinstance(side["snapshot"], str) or not side["snapshot"]:
        raise AttributionConfigError(f"{label}.snapshot must be a non-empty string")
    schema_id = side["schema"]
    if not isinstance(schema_id, int) or isinstance(schema_id, bool) \
            or schema_id < 0:
        raise AttributionConfigError(
            f"{label}.schema must be a non-negative int, got {schema_id!r}"
        )
    role_obj = side.get("roles")
    if not isinstance(role_obj, dict):
        raise AttributionConfigError(f"{label}.roles must be an object")
    roles: dict = {}
    for name in required_roles:
        if name not in role_obj:
            raise AttributionConfigError(
                f"{label}.roles is missing required role {name!r}"
            )
        fid = role_obj[name]
        if not isinstance(fid, int) or isinstance(fid, bool) or fid <= 0:
            raise AttributionConfigError(
                f"{label}.roles.{name} must be a positive int field id, "
                f"got {fid!r}"
            )
        roles[name] = fid
    # unknown roles are rejected rather than silently ignored
    unknown = set(role_obj) - set(required_roles)
    if unknown:
        raise AttributionConfigError(
            f"{label}.roles contains unknown roles: {sorted(unknown)}"
        )
    if len(set(roles.values())) != len(roles):
        raise AttributionConfigError(
            f"{label}.roles: the same field id is wired to more than one role"
        )
    payload_key = payload_keys[0]
    payload = _parse_payload(side.get(payload_key, []), label, payload_key)
    payload_ids = {c.field_id for c in payload}
    overlap = payload_ids & set(roles.values())
    if overlap:
        raise AttributionConfigError(
            f"{label}.{payload_key}: payload field ids {sorted(overlap)} are "
            f"already used as role columns"
        )
    if len(payload_ids) != len(payload):
        raise AttributionConfigError(
            f"{label}.{payload_key}: duplicate field id in payload list"
        )
    return SideConfig(
        label=label, table=side["table"], snapshot=side["snapshot"],
        schema_id=schema_id, roles=roles, payload=tuple(payload),
    )


def _parse_payload(obj: Any, label: str, payload_key: str) -> list:
    if not isinstance(obj, list):
        raise AttributionConfigError(
            f"{label}.{payload_key} must be a list of "
            f"{{\"field_id\": int, \"name\": str}} entries"
        )
    out = []
    names = set()
    for item in obj:
        if not isinstance(item, dict) or set(item) != {"field_id", "name"}:
            raise AttributionConfigError(
                f"{label}.{payload_key} entries must be objects with exactly "
                f"field_id and name: {item!r}"
            )
        fid = item["field_id"]
        name = item["name"]
        if not isinstance(fid, int) or isinstance(fid, bool) or fid <= 0:
            raise AttributionConfigError(
                f"{label}.{payload_key}: field_id must be a positive int, "
                f"got {fid!r}"
            )
        if not isinstance(name, str) or not name:
            raise AttributionConfigError(
                f"{label}.{payload_key}: output column name must be a "
                f"non-empty string, got {name!r}"
            )
        if name in names:
            raise AttributionConfigError(
                f"{label}.{payload_key}: duplicate output column name {name!r}"
            )
        names.add(name)
        out.append(PayloadColumn(field_id=fid, name=name))
    return out


def _check_output_name_uniqueness(cfg: AttributionConfig) -> None:
    reserved = set(RESERVED_COLUMNS)
    seen = set(reserved)
    for side in (cfg.fact, cfg.dimension):
        for col in side.payload:
            if col.name in seen:
                where = "reserved" if col.name in reserved else "another payload"
                raise AttributionConfigError(
                    f"output column name {col.name!r} ({side.label} field "
                    f"{col.field_id}) collides with {where} output column"
                )
            seen.add(col.name)
