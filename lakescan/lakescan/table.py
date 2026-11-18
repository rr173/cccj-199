"""table.json contract: immutable schemas, file directory, snapshots with parents.

A snapshot declares its *complete* active_files set.  Snapshots may fork; the
parent chain is used for explanation only.  Reads are resolved exclusively
against the snapshot's own active_files -- files from other branches are never
pulled in.  ``file_seq`` identifies a file entry; it plays no role in delete
applicability (that is governed by ``data_seq`` alone).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field as _dc_field
from pathlib import Path, PurePosixPath
from typing import Any, Optional

from .errors import ContractError, SourceFileMissing, SourceVersionMismatch
from .schema import Schema, validate_cross_schema_compatibility

KIND_DATA = "DATA"
KIND_POS_DELETE = "POS_DELETE"
KIND_EQ_DELETE = "EQ_DELETE"
_KINDS = (KIND_DATA, KIND_POS_DELETE, KIND_EQ_DELETE)

# Reserved well-known field ids of POS_DELETE files (target_file_id, row_position).
POS_TARGET_FILE_FIELD = 1000
POS_POSITION_FIELD = 1001

FORMAT = "offline-lake/1"


def _canon_partition_value(v: Any) -> tuple:
    """Typed canonical form of a partition value (exact, no floats allowed)."""
    if v is None:
        return ("n", None)
    if isinstance(v, bool):
        return ("b", v)
    if isinstance(v, int):
        return ("i", v)
    if isinstance(v, str):
        return ("s", v)
    raise ContractError(
        f"partition value must be string/int/bool/null, got {type(v).__name__}: {v!r}"
    )


def canon_partition(obj: Any) -> tuple:
    """A partition is an explicit equality tuple, e.g. {"tenant": "t1"}.

    An empty value ("" or null) is distinct from the field being absent:
    (("tenant", ("s", "")),) != (("tenant", ("n", None)),) != ().
    """
    if obj is None:
        obj = {}
    if not isinstance(obj, dict):
        raise ContractError(f"partition must be an object, got {obj!r}")
    items = []
    for k, v in obj.items():
        if not isinstance(k, str) or not k:
            raise ContractError(f"partition key must be a non-empty string: {k!r}")
        items.append((k, _canon_partition_value(v)))
    items.sort(key=lambda kv: kv[0])
    return tuple(items)


def partition_to_json(canon: tuple) -> dict:
    out = {}
    for k, (tag, v) in canon:
        out[k] = v
    return out


@dataclass(frozen=True)
class FileEntry:
    file_id: str
    kind: str
    path: str  # POSIX-relative to table root
    sha256: str
    partition: tuple  # canonical, see canon_partition
    data_seq: int
    file_seq: int  # identity only; never used for delete applicability
    schema_id: int
    eq_field_ids: Optional[tuple] = None  # EQ_DELETE only

    @staticmethod
    def parse(obj: Any) -> "FileEntry":
        if not isinstance(obj, dict):
            raise ContractError(f"file entry must be an object: {obj!r}")
        try:
            file_id = obj["file_id"]
            kind = obj["kind"]
            path = obj["path"]
            sha256 = obj["sha256"]
            data_seq = obj["data_seq"]
            file_seq = obj["file_seq"]
            schema_id = obj["schema_id"]
        except KeyError as e:
            raise ContractError(f"file entry missing key {e}: {obj!r}") from e
        if not isinstance(file_id, str) or not file_id:
            raise ContractError(f"file_id must be a non-empty string: {obj!r}")
        if kind not in _KINDS:
            raise ContractError(f"unknown file kind {kind!r}")
        _validate_relpath(path)
        if not isinstance(sha256, str) or len(sha256) != 64 or any(
            c not in "0123456789abcdef" for c in sha256
        ):
            raise ContractError(f"sha256 must be 64 lowercase hex chars: {sha256!r}")
        for name, val in (("data_seq", data_seq), ("file_seq", file_seq)):
            if not isinstance(val, int) or isinstance(val, bool) or val < 0:
                raise ContractError(f"{name} must be a non-negative int: {val!r}")
        if not isinstance(schema_id, int):
            raise ContractError(f"schema_id must be an int: {schema_id!r}")
        eq_field_ids = obj.get("eq_field_ids")
        if kind == KIND_EQ_DELETE:
            if not isinstance(eq_field_ids, list) or not eq_field_ids:
                raise ContractError(
                    f"EQ_DELETE {file_id}: eq_field_ids must be a non-empty list"
                )
            seen = set()
            for fid in eq_field_ids:
                if not isinstance(fid, int) or isinstance(fid, bool) or fid <= 0:
                    raise ContractError(f"EQ_DELETE {file_id}: bad field id {fid!r}")
                if fid in seen:
                    raise ContractError(
                        f"EQ_DELETE {file_id}: duplicate field id {fid}"
                    )
                seen.add(fid)
            eq_field_ids = tuple(sorted(eq_field_ids))
        elif eq_field_ids is not None:
            raise ContractError(f"{kind} {file_id}: eq_field_ids not allowed")
        return FileEntry(
            file_id=file_id,
            kind=kind,
            path=path,
            sha256=sha256,
            partition=canon_partition(obj.get("partition")),
            data_seq=data_seq,
            file_seq=file_seq,
            schema_id=schema_id,
            eq_field_ids=eq_field_ids,
        )


def _validate_relpath(path: Any) -> None:
    if not isinstance(path, str) or not path:
        raise ContractError(f"path must be a non-empty string: {path!r}")
    p = PurePosixPath(path)
    if p.is_absolute() or ".." in p.parts:
        raise ContractError(f"path must be relative and stay inside the table: {path!r}")


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    parent: Optional[str]
    files: tuple  # FileEntry, in the order listed by the contract

    def data_files(self) -> list:
        return [f for f in self.files if f.kind == KIND_DATA]

    def delete_files(self) -> list:
        return [f for f in self.files if f.kind != KIND_DATA]


class TableContract:
    def __init__(self, root: Path, raw: dict):
        self.root = Path(root)
        if raw.get("format") != FORMAT:
            raise ContractError(f"unsupported format: {raw.get('format')!r}")
        self.table_id = raw.get("table_id", "")
        schemas = {}
        for sobj in raw.get("schemas", []):
            s = Schema.parse(sobj)
            if s.schema_id in schemas:
                raise ContractError(f"duplicate schema_id {s.schema_id}")
            schemas[s.schema_id] = s
        if not schemas:
            raise ContractError("contract declares no schemas")
        validate_cross_schema_compatibility(schemas)
        self.schemas = schemas

        snapshots = {}
        for sobj in raw.get("snapshots", []):
            snap = self._parse_snapshot(sobj, schemas)
            if snap.snapshot_id in snapshots:
                raise ContractError(f"duplicate snapshot_id {snap.snapshot_id}")
            snapshots[snap.snapshot_id] = snap
        if not snapshots:
            raise ContractError("contract declares no snapshots")
        # parent references + acyclicity
        for snap in snapshots.values():
            seen = {snap.snapshot_id}
            cur = snap.parent
            while cur is not None:
                if cur not in snapshots:
                    raise ContractError(
                        f"snapshot {snap.snapshot_id}: unknown parent {cur!r}"
                    )
                if cur in seen:
                    raise ContractError(f"snapshot parent cycle at {cur!r}")
                seen.add(cur)
                cur = snapshots[cur].parent
        self.snapshots = snapshots

    @staticmethod
    def _parse_snapshot(obj: Any, schemas: dict) -> Snapshot:
        if not isinstance(obj, dict):
            raise ContractError(f"snapshot must be an object: {obj!r}")
        sid = obj.get("snapshot_id")
        if not isinstance(sid, str) or not sid:
            raise ContractError(f"snapshot_id must be a non-empty string: {obj!r}")
        parent = obj.get("parent")
        if parent is not None and not isinstance(parent, str):
            raise ContractError(f"snapshot {sid}: parent must be a string or null")
        files = []
        seen_ids = set()
        for fobj in obj.get("active_files", []):
            entry = FileEntry.parse(fobj)
            if entry.file_id in seen_ids:
                raise ContractError(
                    f"snapshot {sid}: duplicate file_id {entry.file_id!r}"
                )
            seen_ids.add(entry.file_id)
            if entry.schema_id not in schemas:
                raise ContractError(
                    f"snapshot {sid}: file {entry.file_id}: unknown schema_id "
                    f"{entry.schema_id}"
                )
            files.append(entry)
        if not files:
            raise ContractError(f"snapshot {sid}: empty active_files")
        return Snapshot(sid, parent, tuple(files))

    @classmethod
    def load(cls, root) -> "TableContract":
        root = Path(root)
        tj = root / "table.json"
        if not tj.is_file():
            raise SourceFileMissing(f"table.json not found under {root}")
        with open(tj, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return cls(root, raw)

    def lineage(self, snapshot_id: str) -> list:
        """Parent chain for explanation: [snapshot_id, parent, ...]."""
        chain = []
        cur = self.snapshot(snapshot_id)
        while cur is not None:
            chain.append(cur.snapshot_id)
            cur = self.snapshots.get(cur.parent) if cur.parent else None
        return chain

    def snapshot(self, snapshot_id: str) -> Snapshot:
        try:
            return self.snapshots[snapshot_id]
        except KeyError:
            raise ContractError(f"unknown snapshot_id {snapshot_id!r}") from None

    def schema(self, schema_id: int) -> Schema:
        try:
            return self.schemas[schema_id]
        except KeyError:
            raise ContractError(f"unknown schema_id {schema_id!r}") from None


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_source(root: Path, entry: FileEntry) -> Path:
    """Locate a file by its contract path and verify content digest.

    The digest is the file's identity: after copying a table directory, files
    are located by path and pinned by sha256.  If the bytes changed, we raise
    SourceVersionMismatch instead of serving stale offsets/results.
    """
    p = Path(root) / entry.path
    if not p.is_file():
        raise SourceFileMissing(f"{entry.file_id}: {entry.path} not found under {root}")
    actual = sha256_of(p)
    if actual != entry.sha256:
        raise SourceVersionMismatch(
            f"{entry.file_id} ({entry.path}): source version mismatch; "
            f"table.json pins sha256={entry.sha256[:16]}... but file hashes to "
            f"{actual[:16]}...; refusing to reuse stale results"
        )
    return p
