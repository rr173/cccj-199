"""Repeatable cross-snapshot key diff between two fixed lake snapshots.

Given a source snapshot, a target snapshot, a target schema and one or more
stable business-key field ids, this module computes the business-record
changes between the two snapshots' *visible* rows:

* ``insert`` -- a key present in the target snapshot only
* ``delete`` -- a key present in the source snapshot only
* ``update`` -- a key present on both sides whose normalized non-key fields
  differ under the target schema
* unchanged keys are not emitted.

Design contract (see README, section "Cross-snapshot diff"):

* Each side is scanned with that snapshot's *own* active_files and its own
  POS/EQ delete semantics.  ``parent`` is diagnostic only and never replaces
  actual snapshot membership; the two snapshots may live on different
  branches.
* Business-key fields are matched by field_id, must exist on both sides and
  be exactly compatible (the contract's exact-widening rules) with the key
  field types in the target schema.  Nullable non-key fields absent from a
  data file's schema are filled with null.  Keys are never stringified and
  numbers are never compared approximately: the type-tagged canonical key
  encoding from :mod:`lakescan.schema` is the identity domain.
* Within each snapshot's visible rows the key must be non-null and unique.
  Null keys and duplicate keys fail the *whole* comparison -- no arbitrary
  pairing.  ``row_uid`` and physical file positions are never cross-snapshot
  identities: compaction, file replacement or duplicate physical rows with
  the same row_uid cannot change the key rules.
* Data may far exceed memory.  Each side is streamed into its own on-disk
  SQLite store (bounded page cache, chunked writes, never a Python dict/set
  of keys).  Output is produced by an external merge over the two sorted
  stores, written to a temporary artifact, and atomically published only
  after both inputs are fully validated, key uniqueness has passed on both
  sides and the export completed.  Re-running identical inputs yields
  byte-identical output: every ordering decision comes from the canonical
  key bytes, never from file/row-group enumeration order.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from .errors import (
    DiffError,
    DuplicateKeyError,
    KeyFieldError,
    NullKeyError,
)
from .scan import Scanner
from .schema import Field, FieldType, Schema, encode_key_tuple, encode_key_value
from .table import TableContract

_INSERT_CHUNK = 10000
# hard cap on the number of rows materialized as Python objects at once
_PYTHON_ROW_CAP = 20000


# ---------------------------------------------------------------------------
# exact, byte-stable value serialization for output and update detection
# ---------------------------------------------------------------------------


def value_to_jsonable(value: Any) -> Any:
    """Projected value -> an exact JSON-able form.

    Decimals are rendered as their exact string form (never float); bools,
    ints, strings and ``None`` pass through.  Booleans are checked before int
    because ``bool`` is a subclass of ``int``.
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
    raise DiffError(f"value of type {type(value).__name__} is not JSON-able")


def canonical_value(ftype: FieldType, value: Any) -> str:
    """Canonical textual form of one normalized non-key field value.

    Used for update detection (same key, different payload).  The form is
    exact and type-tagged so distinct typed values can never collide: there
    is no float anywhere in the pipeline and decimals are compared as exact
    strings in the target-schema type they were normalized into.
    """
    if value is None:
        return "null"
    if ftype.kind == "boolean":
        return "b:" + ("1" if value else "0")
    if ftype.kind in ("int32", "int64"):
        return "i:" + str(int(value))
    if ftype.kind == "decimal":
        # value arrives normalized to the target decimal type (pyarrow hands
        # back a Decimal); format exactly, never via float.
        return f"d({ftype.scale}):" + format(Decimal(value), "f")
    if ftype.kind == "utf8":
        return "s:" + str(value)
    raise DiffError(f"cannot canonicalize type {ftype.kind}")  # pragma: no cover


def payload_canonical(non_key_fields: Sequence[Field], values: dict) -> str:
    """Stable canonical string of the non-key projection of one row."""
    parts = []
    for f in non_key_fields:
        parts.append(str(f.field_id) + "=" + canonical_value(f.type, values.get(f.field_id)))
    return "|".join(parts)


def projection_json(target: Schema, field_ids: Sequence[int], values: dict) -> dict:
    """The output projection object: field_id (as string) -> exact value."""
    out = {}
    for fid in field_ids:
        f = target.field(fid)
        out[str(fid)] = value_to_jsonable(values.get(fid))
    return out


# ---------------------------------------------------------------------------
# one snapshot side: validation + streamed on-disk store
# ---------------------------------------------------------------------------


class _SideStore:
    """On-disk table of (canonical key blob, canonical payload, projection).

    The primary key constraint makes duplicate-key rejection atomic inside
    SQLite; we still track the offending key for the error message.  Only
    fixed-size chunks of rows pass through Python.  The page cache is capped
    so arbitrarily large key sets stay bounded in memory.
    """

    def __init__(self, db_path: str):
        if os.path.exists(db_path):
            os.remove(db_path)
        self.db = sqlite3.connect(db_path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("PRAGMA temp_store=FILE")
        self.db.execute("PRAGMA cache_size=-4096")  # ~4MB page cache
        self.db.execute(
            "CREATE TABLE rows("
            " key BLOB PRIMARY KEY,"
            " payload TEXT NOT NULL,"
            " projection TEXT NOT NULL"
            ") WITHOUT ROWID"
        )
        self.row_count = 0
        self._db_path = db_path

    def insert_many(self, rows: list) -> Optional[bytes]:
        """Insert one chunk.  Returns the first duplicate key blob, if any.

        Uniqueness is enforced by the PRIMARY KEY index itself -- no Python
        set is involved.  A duplicate means the whole diff must fail; callers
        surface it as :class:`DuplicateKeyError`.
        """
        try:
            self.db.executemany(
                "INSERT INTO rows(key, payload, projection) VALUES (?, ?, ?)",
                rows,
            )
        except sqlite3.IntegrityError:
            # fall back to row-by-row to identify the offending key
            for row in rows:
                try:
                    self.db.execute(
                        "INSERT INTO rows(key, payload, projection) "
                        "VALUES (?, ?, ?)", row,
                    )
                except sqlite3.IntegrityError:
                    return row[0]
            raise  # pragma: no cover
        self.row_count += len(rows)
        return None

    def commit(self) -> None:
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def iter_sorted(self) -> Iterator[tuple]:
        """Yield (key, payload, projection) in canonical key order."""
        cur = self.db.execute(
            "SELECT key, payload, projection FROM rows ORDER BY key"
        )
        while True:
            chunk = cur.fetchmany(_PYTHON_ROW_CAP)
            if not chunk:
                return
            for row in chunk:
                yield row

    def __len__(self) -> int:
        return self.row_count


@dataclass
class _SideRun:
    label: str  # "source" | "target"
    snapshot_id: str
    store: _SideStore
    key_field_ids: tuple
    key_types: list  # target-schema FieldType of each key field
    non_key_fields: tuple  # target-schema Field objects, sorted by field_id
    all_field_ids: tuple  # key + non-key in field_id order for projection


class _SideExtractor:
    """Streams one snapshot's visible rows into a :class:`_SideStore`.

    Reuses the library's :class:`Scanner` for everything it already owns:
    per-snapshot active_files, digest verification, POS/EQ application with
    the strict/non-strict data_seq rules, target-schema widening and null
    filling of missing nullable fields.  The diff-specific logic (key
    presence/compatibility, non-null uniqueness, canonical payloads) lives
    here, not in the scanner.
    """

    def __init__(self, contract: TableContract, label: str, snapshot_id: str,
                 target: Schema, key_field_ids: tuple, work_dir: str,
                 batch_size: int):
        self.contract = contract
        self.label = label
        self.snapshot_id = snapshot_id
        self.target = target
        self.key_field_ids = key_field_ids
        self.batch_size = batch_size
        db_path = os.path.join(work_dir, f"{label}_rows.sqlite")
        self.store = _SideStore(db_path)
        self.key_types = [target.field(fid).type for fid in key_field_ids]
        nk = [f for f in target.fields if f.field_id not in set(key_field_ids)]
        self.non_key_fields = tuple(sorted(nk, key=lambda f: f.field_id))
        self.all_field_ids = tuple(
            sorted(target._by_id.keys())
        )

    # -- pre-scan validation ------------------------------------------------

    def validate(self) -> None:
        """Everything rejectable before rows are streamed.

        * snapshot exists (unknown snapshot -> ContractError from the table)
        * every key field exists in this snapshot's data-file schemas and is
          exactly compatible with the target-schema key type
        * required target fields absent from a data file are rejected
          (the scanner would do this too, but we want a diff-specific message)
        """
        snapshot = self.contract.snapshot(self.snapshot_id)
        for entry in snapshot.files:
            if entry.kind != "DATA":
                continue
            fschema = self.contract.schema(entry.schema_id)
            for fid in self.key_field_ids:
                sf = fschema.get(fid)
                if sf is None:
                    raise KeyFieldError(
                        f"{self.label} snapshot {self.snapshot_id}: business-key "
                        f"field_id {fid} is absent from DATA file {entry.file_id} "
                        f"(schema {entry.schema_id}); keys must exist on both "
                        f"sides and be non-null"
                    )
                tf = self.target.field(fid)
                if not sf.type.widens_to(tf.type):
                    raise KeyFieldError(
                        f"{self.label} snapshot {self.snapshot_id}: key field_id "
                        f"{fid} in DATA file {entry.file_id} has {sf.type} which "
                        f"is not exactly compatible with target-schema type "
                        f"{tf.type}"
                    )

    # -- streaming extraction -----------------------------------------------

    def run(self) -> _SideRun:
        # A fresh scanner per side: each side owns its own delete index and
        # its own snapshot's active_files.  Projection covers the *whole*
        # target schema so a user output projection can never omit a value
        # needed for update detection.
        scanner = Scanner(
            self.contract.root,
            work_dir=os.path.join(
                os.path.dirname(self.store._db_path), f"{self.label}_scan"
            ),
            batch_size=self.batch_size,
        )
        projection = self.all_field_ids
        buf: list = []
        dup_reported: Optional[bytes] = None
        try:
            for sb in scanner.scan(self.snapshot_id, self.target.schema_id,
                                   projection):
                names = sb.batch.schema.names
                cols = {n: sb.batch.column(n).to_pylist() for n in names}
                name_by_id = {
                    fid: self.target.field(fid).name
                    for fid in self.all_field_ids
                }
                for i in range(sb.num_rows):
                    values = {
                        fid: cols[name_by_id[fid]][i]
                        for fid in self.all_field_ids
                    }
                    key_values = [values[fid] for fid in self.key_field_ids]
                    if any(v is None for v in key_values):
                        null_pos = [
                            fid for fid, v in
                            zip(self.key_field_ids, key_values) if v is None
                        ]
                        raise NullKeyError(
                            f"{self.label} snapshot {self.snapshot_id}: null "
                            f"business key in visible row (file {sb.file_id}, "
                            f"position {sb.positions[i]}); null key field(s) "
                            f"{null_pos}; the whole comparison fails"
                        )
                    if len(self.key_field_ids) == 1:
                        key_blob = encode_key_value(
                            self.key_types[0], key_values[0]
                        )
                    else:
                        key_blob = encode_key_tuple(
                            self.key_types, key_values)
                    payload = payload_canonical(self.non_key_fields, values)
                    proj = json.dumps(
                        projection_json(self.target, self.all_field_ids,
                                        values),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    buf.append((sqlite3.Binary(key_blob), payload, proj))
                    if len(buf) >= _INSERT_CHUNK:
                        dup = self.store.insert_many(buf)
                        buf.clear()
                        if dup is not None:
                            dup_reported = dup
                            break
                if dup_reported is not None:
                    break
            if dup_reported is None and buf:
                dup_reported = self.store.insert_many(buf)
                buf.clear()
            if dup_reported is not None:
                raise DuplicateKeyError(
                    f"{self.label} snapshot {self.snapshot_id}: duplicate "
                    f"business key {_describe_key(self.key_field_ids, dup_reported)} "
                    f"appears in more than one visible row; keys must be unique "
                    f"within a snapshot, the whole comparison fails"
                )
            self.store.commit()
            return _SideRun(
                label=self.label,
                snapshot_id=self.snapshot_id,
                store=self.store,
                key_field_ids=self.key_field_ids,
                key_types=self.key_types,
                non_key_fields=self.non_key_fields,
                all_field_ids=self.all_field_ids,
            )
        except BaseException:
            # always release the SQLite handle, including on null-key/dup
            # failures and KeyboardInterrupt, so scratch cleanup never blocks
            self.store.commit()
            self.store.close()
            raise


def _describe_key(key_field_ids: tuple, key_blob: bytes) -> str:
    """Human-readable key description for error messages (diagnostic only)."""
    return f"field_ids={list(key_field_ids)} canonical_blob={key_blob.hex()}"


# ---------------------------------------------------------------------------
# external merge over two sorted on-disk stores
# ---------------------------------------------------------------------------


@dataclass
class DiffStats:
    insert: int = 0
    delete: int = 0
    update: int = 0
    unchanged: int = 0
    source_rows: int = 0
    target_rows: int = 0

    def to_json(self) -> dict:
        return {
            "insert": self.insert,
            "delete": self.delete,
            "update": self.update,
            "unchanged": self.unchanged,
            "source_visible_rows": self.source_rows,
            "target_visible_rows": self.target_rows,
        }


class _Merger:
    """External merge join of the two side stores; emits change records.

    Both iterators are sorted by the same canonical key domain, so matching
    is a streaming merge with O(1) Python state beyond the current rows --
    no full-key Python dict or set is ever built.
    """

    def __init__(self, source: _SideRun, target: _SideRun,
                 target_schema: Schema):
        self.source = source
        self.target = target
        self.target_schema = target_schema
        self.stats = DiffStats(
            source_rows=len(source.store), target_rows=len(target.store)
        )

    def __iter__(self) -> Iterator[dict]:
        si = iter(self.source.store.iter_sorted())
        ti = iter(self.target.store.iter_sorted())
        s_row = next(si, None)
        t_row = next(ti, None)
        while s_row is not None or t_row is not None:
            if t_row is None:
                yield from self._source_only(si, s_row)
                return
            if s_row is None:
                yield from self._target_only(ti, t_row)
                return
            s_key, s_payload, s_proj = s_row
            t_key, t_payload, t_proj = t_row
            if s_key == t_key:
                if s_payload == t_payload:
                    self.stats.unchanged += 1
                else:
                    self.stats.update += 1
                    yield self._record("update", s_row, t_row)
                s_row = next(si, None)
                t_row = next(ti, None)
            elif s_key < t_key:
                self.stats.delete += 1
                yield self._record("delete", s_row, None)
                s_row = next(si, None)
            else:
                self.stats.insert += 1
                yield self._record("insert", None, t_row)
                t_row = next(ti, None)

    def _source_only(self, it, row):
        while row is not None:
            self.stats.delete += 1
            yield self._record("delete", row, None)
            row = next(it, None)

    def _target_only(self, it, row):
        while row is not None:
            self.stats.insert += 1
            yield self._record("insert", None, row)
            row = next(it, None)

    def _record(self, change: str, s_row, t_row) -> dict:
        key_obj = self._key_object(s_row if s_row is not None else t_row)
        rec = {
            "change": change,
            "key": key_obj,
            "source_snapshot": self.source.snapshot_id,
            "target_snapshot": self.target.snapshot_id,
        }
        if s_row is not None:
            rec["before"] = json.loads(s_row[2])
        else:
            rec["before"] = None
        if t_row is not None:
            rec["after"] = json.loads(t_row[2])
        else:
            rec["after"] = None
        return rec

    def _key_object(self, row) -> dict:
        key_blob = row[0]
        proj = json.loads(row[2])
        return {
            str(fid): proj[str(fid)] for fid in self.source.key_field_ids
        }


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------


@dataclass
class DiffResult:
    output_path: Path
    source_snapshot: str
    target_snapshot: str
    target_schema_id: int
    key_field_ids: tuple
    stats: DiffStats
    work_dir: Optional[Path]
    """Scratch directory used for the on-disk side stores.  Populated when an
    explicit work_dir was supplied (retained for inspection); ``None`` when
    the differ auto-created scratch space, which is removed on exit."""


class TableDiffer:
    """Compute and atomically publish a repeatable two-snapshot key diff."""

    def __init__(self, root, work_dir: Optional[str] = None,
                 batch_size: int = 8192):
        self.root = Path(root)
        self._work_dir = work_dir
        self.batch_size = batch_size

    def compare(self, source_snapshot: str, target_snapshot: str,
                target_schema_id: int, key_field_ids: Sequence[int],
                output_path) -> DiffResult:
        key_field_ids = tuple(key_field_ids)
        self._validate_key_args(key_field_ids)
        contract = TableContract.load(self.root)
        target = contract.schema(target_schema_id)
        for fid in key_field_ids:
            if fid not in target:
                raise KeyFieldError(
                    f"business-key field_id {fid} not in target schema "
                    f"{target_schema_id}"
                )
        # snapshots must exist; parent chain is intentionally never consulted
        contract.snapshot(source_snapshot)
        contract.snapshot(target_snapshot)

        owns_work_dir = self._work_dir is None
        work_dir = Path(
            self._work_dir or tempfile.mkdtemp(prefix="lakescan-diff-")
        )
        run_dir = work_dir / (
            f"diff_{_safe(source_snapshot)}__{_safe(target_snapshot)}"
        )
        if run_dir.exists():
            shutil.rmtree(run_dir)
        (run_dir / "source_scan").mkdir(parents=True)
        (run_dir / "target_scan").mkdir(parents=True)

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # stage next to the destination so os.replace is same-filesystem
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=output_path.name + ".", suffix=".tmp",
            dir=str(output_path.parent),
        )
        os.close(tmp_fd)
        tmp_path = Path(tmp_name)

        source_run = target_run = None
        try:
            source_ex = _SideExtractor(
                contract, "source", source_snapshot, target, key_field_ids,
                str(run_dir), self.batch_size,
            )
            target_ex = _SideExtractor(
                contract, "target", target_snapshot, target, key_field_ids,
                str(run_dir), self.batch_size,
            )
            # all pre-scan validation on both sides before any visible row is
            # read, so a rejectable input never produces a partial artifact
            source_ex.validate()
            target_ex.validate()

            source_run = source_ex.run()
            target_run = target_ex.run()

            merger = _Merger(source_run, target_run, target)
            # stream records to the staging file; the full result is never
            # held in Python memory
            with open(tmp_path, "w", encoding="utf-8", newline="\n") as fh:
                for rec in merger:
                    fh.write(json.dumps(
                        rec, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"),
                    ))
                    fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())

            # both inputs fully validated, uniqueness passed, export complete:
            # publish atomically.  An existing destination is replaced, never
            # overwritten in place by a half-finished file.
            os.replace(tmp_name, output_path)
            return DiffResult(
                output_path=output_path,
                source_snapshot=source_snapshot,
                target_snapshot=target_snapshot,
                target_schema_id=target_schema_id,
                key_field_ids=key_field_ids,
                stats=merger.stats,
                work_dir=run_dir if self._work_dir is not None else None,
            )
        except BaseException:
            # failure leaves no seemingly-complete target behind
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        finally:
            if source_run is not None:
                source_run.store.close()
            if target_run is not None:
                target_run.store.close()
            # auto-created scratch space is removed on every exit; an
            # explicitly supplied work_dir is retained for inspection.
            if owns_work_dir:
                shutil.rmtree(work_dir, ignore_errors=True)

    @staticmethod
    def _validate_key_args(key_field_ids: Sequence[int]) -> None:
        if not key_field_ids:
            raise KeyFieldError(
                "at least one business-key field_id is required"
            )
        seen = set()
        for fid in key_field_ids:
            if not isinstance(fid, int) or isinstance(fid, bool) or fid <= 0:
                raise KeyFieldError(
                    f"business-key field_id must be a positive int, got {fid!r}"
                )
            if fid in seen:
                raise KeyFieldError(
                    f"business-key field_id {fid} listed more than once"
                )
            seen.add(fid)


def _safe(text: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in text)


def compare_snapshots(root, source_snapshot: str, target_snapshot: str,
                      target_schema_id: int, key_field_ids: Sequence[int],
                      output_path, work_dir: Optional[str] = None,
                      batch_size: int = 8192) -> DiffResult:
    """Convenience wrapper: one-shot atomic diff export."""
    return TableDiffer(root, work_dir=work_dir, batch_size=batch_size).compare(
        source_snapshot, target_snapshot, target_schema_id,
        key_field_ids, output_path,
    )
