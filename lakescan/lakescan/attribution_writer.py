"""Typed Parquet output, manifest and one-shot atomic directory delivery.

The output directory is delivered exactly once: every artifact is first
written into a unique sibling staging directory, fsynced and only then
renamed onto the (never-before-existing) destination.  Any failure removes
the staging directory; an existing destination directory is never touched
and no partial result is ever visible under its name.

Output columns keep the exact target-schema primitive types (int32 widens to
int64 only where the input contract already widened it; decimals keep
precision/scale; nullability is preserved) and each column's role is
distinguishable by a fixed naming convention plus explicit role metadata in
the manifest.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterator, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .attribution_config import RESERVED_COLUMNS, AttributionConfig
from .attribution_engine import (
    STATUS_AMBIGUOUS,
    STATUS_MATCHED,
    STATUS_UNMATCHED,
)
from .attribution_store import restore_json_value
from .errors import AttributionOutputError, AttributionValidationError
from .schema import Schema

DATA_FILE_NAME = "attribution.parquet"
MANIFEST_FILE_NAME = "manifest.json"
_ROW_GROUP_ROWS = 10000


class OutputSchema:
    """Resolves the fixed columns plus configured payload columns."""

    def __init__(self, cfg: AttributionConfig, fact_schema: Schema,
                 dim_schema: Schema):
        self.cfg = cfg
        self.fact_payload = tuple(
            (col, fact_schema.field(col.field_id)) for col in cfg.fact.payload
        )
        self.dim_payload = tuple(
            (col, dim_schema.field(col.field_id)) for col in cfg.dimension.payload
        )
        self._build_arrow_schema()

    def _int64(self, name: str) -> pa.Field:
        return pa.field(name, pa.int64())

    def _utf8(self, name: str, nullable: bool = True) -> pa.Field:
        return pa.field(name, pa.string(), nullable=nullable)

    def _build_arrow_schema(self) -> None:
        fields = [
            self._utf8("fact_id", nullable=False),
            self._utf8("entity_key", nullable=False),
            self._int64("business_time"),
            self._int64("knowledge_cutoff"),
            self._utf8("attribution_status", nullable=False),
            self._utf8("matched_rule_id"),
            self._int64("matched_published_at"),
            self._utf8("matched_dim_file_id"),
            self._int64("matched_dim_row_position"),
            self._int64("candidate_count"),
            self._utf8("evidence_1_rule_id"),
            self._int64("evidence_1_published_at"),
            self._utf8("evidence_1_dim_file_id"),
            self._int64("evidence_1_dim_row_position"),
            self._utf8("evidence_2_rule_id"),
            self._int64("evidence_2_published_at"),
            self._utf8("evidence_2_dim_file_id"),
            self._int64("evidence_2_dim_row_position"),
            self._utf8("fact_file_id", nullable=False),
            self._int64("fact_row_position"),
        ]
        # fact payload: always present, keeps the fact target-schema type and
        # nullability; dimension payload: null unless the row is matched.
        self.fact_payload_names = []
        for col, fld in self.fact_payload:
            fields.append(pa.field(col.name, fld.type.to_pyarrow(),
                                   nullable=not fld.required))
            self.fact_payload_names.append((col, fld))
        self.dim_payload_names = []
        for col, fld in self.dim_payload:
            fields.append(pa.field(col.name, fld.type.to_pyarrow(),
                                   nullable=True))
            self.dim_payload_names.append((col, fld))
        self.arrow_schema = pa.schema(fields)
        assert len(RESERVED_COLUMNS) == 20
        self.column_roles = self._role_map()

    def _role_map(self) -> dict:
        roles = {
            "fact_id": "fact.identity",
            "entity_key": "join.entity_key",
            "business_time": "fact.business_time",
            "knowledge_cutoff": "fact.knowledge_cutoff",
            "attribution_status": "result.status",
            "matched_rule_id": "result.matched_rule_id",
            "matched_published_at": "result.matched_published_at",
            "matched_dim_file_id": "result.matched_dim_source",
            "matched_dim_row_position": "result.matched_dim_source",
            "candidate_count": "result.ambiguous_candidate_count",
            "evidence_1_rule_id": "result.ambiguous_evidence_1",
            "evidence_1_published_at": "result.ambiguous_evidence_1",
            "evidence_1_dim_file_id": "result.ambiguous_evidence_1",
            "evidence_1_dim_row_position": "result.ambiguous_evidence_1",
            "evidence_2_rule_id": "result.ambiguous_evidence_2",
            "evidence_2_published_at": "result.ambiguous_evidence_2",
            "evidence_2_dim_file_id": "result.ambiguous_evidence_2",
            "evidence_2_dim_row_position": "result.ambiguous_evidence_2",
            "fact_file_id": "fact.source_coordinate",
            "fact_row_position": "fact.source_coordinate",
        }
        for col, fld in self.fact_payload:
            roles[col.name] = f"fact.payload.field_id={col.field_id}"
        for col, fld in self.dim_payload:
            roles[col.name] = f"dimension.payload.field_id={col.field_id}"
        return roles

    # ------------------------------------------------------------------
    # row -> typed columns
    # ------------------------------------------------------------------

    def empty_table(self) -> pa.Table:
        return self.arrow_schema.empty_table()

    def build_table(self, rows: list) -> pa.Table:
        n = len(rows)
        cols: dict[str, list] = {name: [] for name in self.arrow_schema.names}
        fact_payload_objs = [
            json.loads(r["fact_payload"]) if r.get("fact_payload") is not None
            else {}
            for r in rows
        ]
        dim_payload_objs = [
            json.loads(r["dim_payload"]) if r.get("dim_payload") is not None
            else None
            for r in rows
        ]
        for i, r in enumerate(rows):
            status = r["status"]
            cols["fact_id"].append(r["fid"])
            cols["entity_key"].append(r["entity_key"])
            cols["business_time"].append(r["business_time"])
            cols["knowledge_cutoff"].append(r["knowledge_cutoff"])
            cols["attribution_status"].append(status)
            cols["fact_file_id"].append(r["fact_file_id"])
            cols["fact_row_position"].append(r["fact_row_position"])
            if status == STATUS_MATCHED:
                cols["matched_rule_id"].append(r["matched_rule_id"])
                cols["matched_published_at"].append(r["matched_published_at"])
                cols["matched_dim_file_id"].append(r["matched_dim_file_id"])
                cols["matched_dim_row_position"].append(
                    r["matched_dim_row_position"])
            else:
                cols["matched_rule_id"].append(None)
                cols["matched_published_at"].append(None)
                cols["matched_dim_file_id"].append(None)
                cols["matched_dim_row_position"].append(None)
            if status == STATUS_AMBIGUOUS:
                cols["candidate_count"].append(r["candidate_count"])
                cols["evidence_1_rule_id"].append(r["evidence_1_rule_id"])
                cols["evidence_1_published_at"].append(
                    r["evidence_1_published_at"])
                cols["evidence_1_dim_file_id"].append(
                    r["evidence_1_dim_file_id"])
                cols["evidence_1_dim_row_position"].append(
                    r["evidence_1_dim_row_position"])
                if "evidence_2_rule_id" in r:
                    cols["evidence_2_rule_id"].append(r["evidence_2_rule_id"])
                    cols["evidence_2_published_at"].append(
                        r["evidence_2_published_at"])
                    cols["evidence_2_dim_file_id"].append(
                        r["evidence_2_dim_file_id"])
                    cols["evidence_2_dim_row_position"].append(
                        r["evidence_2_dim_row_position"])
                else:
                    for nm in ("evidence_2_rule_id", "evidence_2_published_at",
                               "evidence_2_dim_file_id",
                               "evidence_2_dim_row_position"):
                        cols[nm].append(None)
            else:
                cols["candidate_count"].append(None)
                for nm in ("evidence_1_rule_id", "evidence_1_published_at",
                           "evidence_1_dim_file_id", "evidence_1_dim_row_position",
                           "evidence_2_rule_id", "evidence_2_published_at",
                           "evidence_2_dim_file_id",
                           "evidence_2_dim_row_position"):
                    cols[nm].append(None)
            for col, fld in self.fact_payload_names:
                cols[col.name].append(
                    restore_json_value(fld.type,
                                       fact_payload_objs[i].get(str(col.field_id))))
            for col, fld in self.dim_payload_names:
                if status == STATUS_MATCHED and dim_payload_objs[i] is not None:
                    cols[col.name].append(
                        restore_json_value(
                            fld.type,
                            dim_payload_objs[i].get(str(col.field_id))))
                else:
                    # selected dimension fields stay empty for unmatched /
                    # ambiguous rows: downstream must never treat ambiguity
                    # as attribution
                    cols[col.name].append(None)
        arrays = []
        for field in self.arrow_schema:
            arrays.append(pa.array(cols[field.name], type=field.type))
        return pa.Table.from_arrays(arrays, schema=self.arrow_schema)


def write_output(cfg: AttributionConfig, output_schema: OutputSchema,
                 decisions: Iterator[dict], stats: "RunStats",
                 manifest_extra: dict) -> Path:
    """Stream decisions into the one-shot output directory.

    The destination must not exist.  All writes go to a sibling staging
    directory which is atomically renamed into place only after the data
    file and manifest have been fsynced.
    """
    dest = Path(cfg.output_dir)
    if dest.exists():
        raise AttributionOutputError(
            f"output directory already exists; one-shot delivery refuses to "
            f"overwrite it: {dest}"
        )
    parent = dest.parent if str(dest.parent) not in ("", ".") else Path(".")
    parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".attr-stage-", dir=str(parent)))
    data_path = staging / DATA_FILE_NAME
    writer: Optional[pq.ParquetWriter] = None
    try:
        buf: list = []

        def flush():
            nonlocal writer, buf
            if not buf:
                return
            table = output_schema.build_table(buf)
            if writer is None:
                writer = pq.ParquetWriter(data_path, output_schema.arrow_schema,
                                          compression="snappy", version="2.6")
            writer.write_table(table, row_group_size=_ROW_GROUP_ROWS)
            buf.clear()

        for decision in decisions:
            stats.record(decision["status"])
            buf.append(decision)
            if len(buf) >= _ROW_GROUP_ROWS:
                flush()
        if not buf and writer is None:
            # zero facts: still deliver an empty, correctly typed data file
            pq.write_table(output_schema.empty_table(), data_path,
                           compression="snappy", version="2.6")
        else:
            # flush the final partial row group (the common small-table case)
            flush()
            writer.close()
            writer = None
        _fsync_file(data_path)

        manifest = _build_manifest(cfg, stats, data_path, manifest_extra,
                                   output_schema)
        manifest_path = staging / MANIFEST_FILE_NAME
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, indent=2,
                      sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        _fsync_dir(staging)
        os.replace(staging, dest)
        _fsync_dir(parent)
        return dest
    except BaseException:
        if writer is not None:
            writer.close()
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _fsync_file(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class RunStats:
    def __init__(self):
        self.total = 0
        self.matched = 0
        self.unmatched = 0
        self.ambiguous = 0

    def record(self, status: str) -> None:
        self.total += 1
        if status == STATUS_MATCHED:
            self.matched += 1
        elif status == STATUS_UNMATCHED:
            self.unmatched += 1
        elif status == STATUS_AMBIGUOUS:
            self.ambiguous += 1
        else:  # pragma: no cover - engine statuses are closed
            raise AttributionValidationError(f"unknown status {status!r}")

    def to_json(self) -> dict:
        return {
            "total_facts": self.total,
            "matched": self.matched,
            "unmatched": self.unmatched,
            "ambiguous": self.ambiguous,
        }


def _build_manifest(cfg: AttributionConfig, stats: RunStats,
                    data_path: Path, manifest_extra: dict,
                    output_schema: OutputSchema) -> dict:
    data_stat = data_path.stat()
    manifest = {
        "format": "lakescan-attribution/1",
        "inputs": manifest_extra["inputs"],
        "source_manifest_digests": manifest_extra["source_manifest_digests"],
        "mapping": manifest_extra["mapping"],
        "row_counts": stats.to_json(),
        "output_files": [
            {
                "name": DATA_FILE_NAME,
                "sha256": _sha256_file(data_path),
                "size_bytes": data_stat.st_size,
                "columns": [
                    {"name": f.name, "arrow_type": str(f.type),
                     "nullable": f.nullable,
                     "role": output_schema.column_roles[f.name]}
                    for f in output_schema.arrow_schema
                ],
            }
        ],
    }
    return manifest
