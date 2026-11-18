"""Delete loading and matching: POS_DELETE and EQ_DELETE.

Applicability rules (enforced while loading, before any data row is scanned):

* POS_DELETE entry (target_file_id, row_position) applies only when the target
  is an active DATA file of this snapshot, target.data_seq <= delete.data_seq
  (non-strict), partitions are equal, and the position is inside the target's
  row range.  Targets that are not active DATA files are kept as
  ``inactive_target`` diagnostics -- content-identical sibling files are never
  touched because matching is by file_id.  Out-of-range positions are a
  snapshot error.  ``file_seq`` is never consulted here.
* EQ_DELETE applies to active DATA files with equal partition and
  DATA.data_seq strictly less than delete.data_seq.  Keys are exact typed
  values compared by field_id with allowed exact widening; a field missing in
  the data file's schema participates as null (null key equals null).

Delete key sets may far exceed memory, so keys live in an on-disk SQLite
index; only small chunks pass through Python.  Duplicate deletes and overlaps
between the two kinds collapse naturally: each physical row is deleted once.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass, field as _dc_field
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

import pyarrow.parquet as pq

from .errors import (
    ContractError,
    PositionOutOfRangeError,
    UnrepresentableKeyError,
    UnknownFieldError,
)
from .schema import FieldType, encode_key_tuple, encode_key_value
from .table import (
    KIND_DATA,
    KIND_EQ_DELETE,
    KIND_POS_DELETE,
    POS_POSITION_FIELD,
    POS_TARGET_FILE_FIELD,
    FileEntry,
    Snapshot,
    partition_to_json,
)

_INSERT_CHUNK = 20000
_PROBE_CHUNK = 500


@dataclass
class DeleteDiagnostic:
    delete_file_id: str
    kind: str
    reason: str  # inactive_target | partition_mismatch | data_seq_excluded
    detail: dict

    def to_json(self) -> dict:
        return {
            "delete_file_id": self.delete_file_id,
            "kind": self.kind,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass
class EqGroup:
    """All EQ_DELETE files sharing (partition, sorted field-id tuple)."""

    group_id: int
    partition: tuple
    field_ids: tuple
    field_types: list  # FieldType, aligned with field_ids
    delete_files: list = _dc_field(default_factory=list)  # (file_id, data_seq)


class DeleteIndex:
    """On-disk index of position deletes and equality-delete keys."""

    def __init__(self, work_dir):
        os.makedirs(work_dir, exist_ok=True)
        self.db_path = os.path.join(work_dir, "delete_index.sqlite")
        # every plan rebuilds the index from the contract, from scratch
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = sqlite3.connect(self.db_path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("PRAGMA temp_store=FILE")
        self.db.execute("PRAGMA cache_size=-4096")  # ~4MB page cache: bounded
        self.db.execute(
            "CREATE TABLE pos_keys("
            " target_file_id TEXT NOT NULL,"
            " pos INTEGER NOT NULL,"
            " delete_file_id TEXT NOT NULL,"
            " PRIMARY KEY(target_file_id, pos, delete_file_id)"
            ") WITHOUT ROWID"
        )
        self.db.execute(
            "CREATE TABLE eq_groups("
            " group_id INTEGER PRIMARY KEY,"
            " part_key TEXT NOT NULL,"
            " field_ids TEXT NOT NULL"
            ")"
        )
        self.db.execute(
            "CREATE TABLE eq_keys("
            " group_id INTEGER NOT NULL,"
            " key BLOB NOT NULL,"
            " delete_file_id TEXT NOT NULL,"
            " data_seq INTEGER NOT NULL,"
            " PRIMARY KEY(group_id, key, delete_file_id)"
            ") WITHOUT ROWID"
        )
        self.diagnostics: list[DeleteDiagnostic] = []
        self.groups: dict[int, EqGroup] = {}
        self._group_ids: dict[tuple, int] = {}
        self.pos_entry_count = 0
        self.eq_key_count = 0

    def close(self):
        self.db.close()

    # -- group bookkeeping -------------------------------------------------

    def _group_for(self, partition: tuple, field_ids: tuple,
                   field_types: list) -> EqGroup:
        gkey = (partition, field_ids)
        gid = self._group_ids.get(gkey)
        if gid is not None:
            return self.groups[gid]
        part_key = repr(partition)
        cur = self.db.execute(
            "INSERT INTO eq_groups(part_key, field_ids) VALUES (?, ?)",
            (part_key, repr(field_ids)),
        )
        gid = cur.lastrowid
        grp = EqGroup(gid, partition, field_ids, list(field_types))
        self.groups[gid] = grp
        self._group_ids[gkey] = gid
        return grp

    # -- loading -----------------------------------------------------------

    def load_pos_deletes(self, entry: FileEntry, parquet_path: Path,
                         schema, data_entries: dict, row_counts: dict) -> None:
        tgt_field = schema.get(POS_TARGET_FILE_FIELD)
        pos_field = schema.get(POS_POSITION_FIELD)
        if tgt_field is None or pos_field is None:
            raise UnknownFieldError(
                f"POS_DELETE {entry.file_id}: schema {schema.schema_id} must map "
                f"field ids {POS_TARGET_FILE_FIELD} (target_file_id) and "
                f"{POS_POSITION_FIELD} (row_position)"
            )
        pf = pq.ParquetFile(parquet_path)
        _require_columns(pf, entry, [tgt_field.name, pos_field.name])
        buf = []
        for batch in pf.iter_batches(
            batch_size=_INSERT_CHUNK, columns=[tgt_field.name, pos_field.name]
        ):
            targets = batch.column(0).to_pylist()
            positions = batch.column(1).to_pylist()
            for t, p in zip(targets, positions):
                if t is None or p is None:
                    raise ContractError(
                        f"POS_DELETE {entry.file_id}: null target/position"
                    )
                if not isinstance(t, str):
                    raise ContractError(
                        f"POS_DELETE {entry.file_id}: target_file_id must be utf8"
                    )
                if isinstance(p, bool) or not isinstance(p, int):
                    raise ContractError(
                        f"POS_DELETE {entry.file_id}: row_position must be an integer"
                    )
                if p < 0:
                    raise PositionOutOfRangeError(
                        f"POS_DELETE {entry.file_id}: negative row_position {p} "
                        f"for target {t!r}"
                    )
                target = data_entries.get(t)
                if target is None or target.kind != KIND_DATA:
                    self.diagnostics.append(
                        DeleteDiagnostic(
                            entry.file_id, KIND_POS_DELETE, "inactive_target",
                            {"target_file_id": t, "row_position": p},
                        )
                    )
                    continue
                if target.partition != entry.partition:
                    self.diagnostics.append(
                        DeleteDiagnostic(
                            entry.file_id, KIND_POS_DELETE, "partition_mismatch",
                            {
                                "target_file_id": t,
                                "row_position": p,
                                "target_partition": partition_to_json(target.partition),
                                "delete_partition": partition_to_json(entry.partition),
                            },
                        )
                    )
                    continue
                # non-strict: target.data_seq <= delete.data_seq
                if target.data_seq > entry.data_seq:
                    self.diagnostics.append(
                        DeleteDiagnostic(
                            entry.file_id, KIND_POS_DELETE, "data_seq_excluded",
                            {
                                "target_file_id": t,
                                "row_position": p,
                                "target_data_seq": target.data_seq,
                                "delete_data_seq": entry.data_seq,
                            },
                        )
                    )
                    continue
                if p >= row_counts[t]:
                    raise PositionOutOfRangeError(
                        f"POS_DELETE {entry.file_id}: row_position {p} out of "
                        f"range for {t} ({row_counts[t]} rows)"
                    )
                buf.append((t, p, entry.file_id))
                if len(buf) >= _INSERT_CHUNK:
                    self._flush_pos(buf)
        if buf:
            self._flush_pos(buf)

    def _flush_pos(self, buf):
        self.db.executemany(
            "INSERT OR IGNORE INTO pos_keys(target_file_id, pos, delete_file_id)"
            " VALUES (?, ?, ?)",
            buf,
        )
        self.pos_entry_count += len(buf)
        buf.clear()

    def load_eq_deletes(self, entry: FileEntry, parquet_path: Path, schema) -> EqGroup:
        assert entry.eq_field_ids
        fields = []
        for fid in entry.eq_field_ids:
            f = schema.get(fid)
            if f is None:
                raise UnknownFieldError(
                    f"EQ_DELETE {entry.file_id}: field_id {fid} not in its "
                    f"schema {schema.schema_id}"
                )
            fields.append(f)
        field_types = [f.type for f in fields]
        grp = self._group_for(entry.partition, entry.eq_field_ids, field_types)
        # Same group, different declared types: allowed only when exactly
        # comparable (e.g. int32 vs int64) -- the canonical encoding domain is
        # identical, so the group keeps the wider type.
        if grp.field_types != field_types:
            merged = []
            for have, new in zip(grp.field_types, field_types):
                common = have.common_exact(new)
                if common is None:
                    raise ContractError(
                        f"EQ_DELETE {entry.file_id}: key type {new} is not "
                        f"exactly comparable with group type {have}"
                    )
                merged.append(common)
            grp.field_types = merged
        grp.delete_files.append((entry.file_id, entry.data_seq))

        pf = pq.ParquetFile(parquet_path)
        names = [f.name for f in fields]
        _require_columns(pf, entry, names)
        buf = []
        single = len(fields) == 1
        for batch in pf.iter_batches(batch_size=_INSERT_CHUNK, columns=names):
            if single:
                col = _validated_key_column(batch.column(0), fields[0].type,
                                            entry.file_id)
                ftype0 = field_types[0]
                for v in col.to_pylist():
                    buf.append(
                        (
                            grp.group_id,
                            sqlite3.Binary(encode_key_value(ftype0, v)),
                            entry.file_id,
                            entry.data_seq,
                        )
                    )
                    if len(buf) >= _INSERT_CHUNK:
                        self._flush_eq(buf)
                continue
            cols = []
            for i, f in enumerate(fields):
                cols.append(
                    _validated_key_column(batch.column(i), f.type, entry.file_id)
                )
            pyrows = zip(*(c.to_pylist() for c in cols))
            for row in pyrows:
                buf.append(
                    (
                        grp.group_id,
                        sqlite3.Binary(encode_key_tuple(field_types, list(row))),
                        entry.file_id,
                        entry.data_seq,
                    )
                )
                if len(buf) >= _INSERT_CHUNK:
                    self._flush_eq(buf)
        if buf:
            self._flush_eq(buf)
        return grp

    def _flush_eq(self, buf):
        self.db.executemany(
            "INSERT OR IGNORE INTO eq_keys(group_id, key, delete_file_id, data_seq)"
            " VALUES (?, ?, ?, ?)",
            buf,
        )
        self.eq_key_count += len(buf)
        buf.clear()

    def commit(self):
        self.db.commit()

    # -- probing -----------------------------------------------------------

    def pos_deleted_positions(self, file_id: str, lo: int, hi: int) -> set:
        """Deleted positions of ``file_id`` inside [lo, hi)."""
        cur = self.db.execute(
            "SELECT DISTINCT pos FROM pos_keys WHERE target_file_id=? "
            "AND pos>=? AND pos<?",
            (file_id, lo, hi),
        )
        return {r[0] for r in cur.fetchall()}

    def pos_delete_files_for(self, file_id: str, pos: int) -> list:
        cur = self.db.execute(
            "SELECT delete_file_id FROM pos_keys WHERE target_file_id=? AND pos=?",
            (file_id, pos),
        )
        return sorted(r[0] for r in cur.fetchall())

    def eq_matched_keys(self, group_id: int, data_seq: int,
                        keys: list) -> set:
        """Which of ``keys`` are deleted for a DATA file with this data_seq.

        Strict rule: only delete files with data_seq > DATA.data_seq apply.
        """
        matched = set()
        for i in range(0, len(keys), _PROBE_CHUNK):
            chunk = keys[i : i + _PROBE_CHUNK]
            marks = ",".join("?" * len(chunk))
            cur = self.db.execute(
                f"SELECT DISTINCT key FROM eq_keys WHERE group_id=? AND data_seq>? "
                f"AND key IN ({marks})",
                [group_id, data_seq] + [sqlite3.Binary(k) for k in chunk],
            )
            matched.update(r[0] for r in cur.fetchall())
        return matched

    def eq_delete_files_for(self, group_id: int, data_seq: int, key: bytes) -> list:
        cur = self.db.execute(
            "SELECT delete_file_id FROM eq_keys WHERE group_id=? AND key=? "
            "AND data_seq>? ORDER BY delete_file_id",
            (group_id, key, data_seq),
        )
        return [r[0] for r in cur.fetchall()]


def _require_columns(pf: pq.ParquetFile, entry: FileEntry, names: Iterable[str]):
    have = set(pf.schema_arrow.names)
    missing = [n for n in names if n not in have]
    if missing:
        raise ContractError(
            f"{entry.kind} {entry.file_id}: parquet is missing physical "
            f"column(s) {missing} required by schema {entry.schema_id}"
        )


def _validated_key_column(arr, ftype: FieldType, delete_file_id: str):
    """Cast a physical key column to its declared type, rejecting
    unrepresentable values (e.g. int64 overflow into int32, decimal precision
    loss).  This is the 'unrepresentable key' rejection; it aborts the whole
    scan rather than skipping the delete file."""
    import pyarrow.compute as pc

    target = ftype.to_pyarrow()
    if arr.type == target:
        return arr
    try:
        return pc.cast(arr, target, safe=True)
    except Exception as e:
        raise UnrepresentableKeyError(
            f"EQ_DELETE {delete_file_id}: key column not representable as "
            f"{ftype}: {e}"
        ) from e
