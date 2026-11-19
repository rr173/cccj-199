"""Scan planning and execution over one fixed snapshot.

Pipeline per DATA file, in the snapshot's listed order:

1. plan-time validation (everything rejectable is rejected here, before any
   data row is read): source digests, schema/physical agreement, projection
   and predicate field ids, read-path widening (illegal narrowing rejected),
   delete loading with key validation, eq<->data type compatibility.
2. optional conservative row-group pruning from parquet statistics.
3. streaming batches with absolute physical positions (0-based, continuous
   across row groups).
4. delete matching (position + equality) *before* the final projection, so
   projecting away a delete key column never changes the result.
5. SQL three-valued predicate evaluation; only TRUE rows are emitted.
6. projection to the requested field ids, emitted with (file_id,
   row_position) provenance.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field as _dc_field
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .deletes import DeleteIndex, EqGroup
from .errors import (
    ContractError,
    PredicateError,
    SnapshotError,
    TypeEvolutionError,
    UnknownFieldError,
)
from .predicates import (
    ColStats,
    Pred,
    definitely_not_true,
    parse_predicate,
    true_mask,
    validate_predicate,
)
from .schema import FieldType, Schema, encode_key_tuple, encode_key_value
from .table import (
    KIND_DATA,
    KIND_EQ_DELETE,
    KIND_POS_DELETE,
    FileEntry,
    Snapshot,
    TableContract,
    partition_to_json,
    verify_source,
)


@dataclass
class ScanBatch:
    """One streamed output batch: projected columns plus source positions."""

    file_id: str
    positions: list  # physical row numbers, 0-based, continuous across row groups
    batch: pa.RecordBatch  # projected columns only, target-schema types/names

    @property
    def num_rows(self) -> int:
        return len(self.positions)


@dataclass
class VerifiedDataFile:
    entry: FileEntry
    path: Path
    num_rows: int
    rg_rows: list
    rg_offsets: list
    arrow_names: list
    arrow_types: dict  # physical name -> pa.DataType


@dataclass
class DataFilePlan:
    data: VerifiedDataFile
    selected_rgs: list  # row-group indexes kept after pruning
    rg_skipped: dict  # rg index -> reason
    phys_columns: list  # physical columns to read
    # field_id -> ("physical", name) | ("null", None) for projection/predicate
    target_field_source: dict
    eq_key_source: dict  # field_id -> physical name | None (missing => null)
    groups: list  # applicable EqGroup


@dataclass
class Plan:
    contract: TableContract
    snapshot: Snapshot
    target_schema: Schema
    projection: tuple
    predicate: Optional[Pred]
    data_files: list  # VerifiedDataFile, listed order
    delete_index: DeleteIndex
    file_plans: list  # DataFilePlan
    work_dir: str
    read_guard: Any = None
    """Read-protection claim held for this plan's lifetime.

    On a managed table every plan (including plans produced for the explain
    interfaces) owns a guard created before the first source file is opened.
    It is released by :meth:`close`, by using the plan/scan as a context
    manager, or when the object is garbage collected.  ``None`` on an
    unmanaged table.  Streaming scans that pause between batches remain
    protected until the iterator is closed or exhausted."""

    def close(self) -> None:
        """Release the read guard (if any).  Idempotent."""
        guard = self.read_guard
        if guard is not None:
            self.read_guard = None
            close = getattr(guard, "close", None)
            if close is not None:
                close()

    def __enter__(self) -> "Plan":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    def __del__(self):  # best effort; explicit close/context manager is the contract
        try:
            self.close()
        except Exception:
            pass


class Scanner:
    def __init__(self, root, work_dir: Optional[str] = None,
                 batch_size: int = 65536, prune: bool = True):
        self.root = Path(root)
        self.work_dir = work_dir or tempfile.mkdtemp(prefix="lakescan-")
        self.batch_size = batch_size
        self.prune = prune
        self._contract: Optional[TableContract] = None
        # outer read claim supplied by callers that protect several snapshots
        # in one guard (the two-sided compare does this).
        self._outer_guard: Any = None

    def bind_read_guard(self, guard) -> None:
        """Reuse an existing read guard instead of creating a per-plan token.

        The caller retains ownership of the guard's lifecycle; plans built on
        this scanner will not close it.  Passing ``None`` restores the default
        behaviour of acquiring a fresh guard per plan on managed tables."""
        self._outer_guard = guard

    # ------------------------------------------------------------------
    # plan
    # ------------------------------------------------------------------

    def plan(self, snapshot_id: str, target_schema_id: int,
             projection, predicate=None) -> Plan:
        # Lifecycle preflight happens BEFORE any source file is opened.  On a
        # managed table this re-verifies the bound manifest and atomically
        # acquires read protection; a concurrently committed retirement makes
        # this raise SnapshotRetiredError here rather than mid-read.  Cached
        # contracts are never trusted: every new plan re-runs the preflight,
        # so a scanner reused after a retirement cannot read a dead snapshot.
        from .lifecycle.reader import read_preflight

        guard = read_preflight(
            self.root, [snapshot_id], reason="scan-plan",
            outer_guard=self._outer_guard,
        )
        owns_guard = guard is not None and guard is not self._outer_guard
        try:
            contract = self._contract or TableContract.load(self.root)
            self._contract = contract
            # on managed tables, fail closed if retirement raced the preflight
            from .lifecycle.state import is_managed, load_state

            if is_managed(self.root):
                state = load_state(self.root)
                if snapshot_id in set(state.retired):
                    from .errors import SnapshotRetiredError

                    raise SnapshotRetiredError(
                        f"snapshot {snapshot_id!r} is retired and can no longer be read"
                    )
            snapshot = contract.snapshot(snapshot_id)
            target = contract.schema(target_schema_id)

            projection = tuple(projection)
            if len(set(projection)) != len(projection):
                raise ContractError(f"duplicate field id in projection: {projection}")
            for fid in projection:
                if not isinstance(fid, int) or fid not in target:
                    raise UnknownFieldError(
                        f"projection field_id {fid!r} not in target schema "
                        f"{target_schema_id}"
                    )
            if isinstance(predicate, (str, dict)):
                predicate = parse_predicate(
                    json.loads(predicate) if isinstance(predicate, str) else predicate
                )
            if predicate is not None and not isinstance(predicate, Pred):
                raise PredicateError(f"bad predicate: {predicate!r}")
            if predicate is not None:
                validate_predicate(predicate, target)

            # -- verify sources & collect data-file metadata -------------------
            data_entries: dict[str, FileEntry] = {}
            data_files: list[VerifiedDataFile] = []
            delete_entries: list = []
            for entry in snapshot.files:
                vpath = verify_source(self.root, entry)
                if entry.kind == KIND_DATA:
                    vf = self._open_data_file(contract, entry, vpath)
                    data_entries[entry.file_id] = entry
                    data_files.append(vf)
                else:
                    delete_entries.append((entry, vpath))

            row_counts = {vf.entry.file_id: vf.num_rows for vf in data_files}

            # -- load deletes (validation errors abort here, pre-scan) ---------
            index = DeleteIndex(self.work_dir)
            for entry, vpath in delete_entries:
                schema = contract.schema(entry.schema_id)
                if entry.kind == KIND_POS_DELETE:
                    index.load_pos_deletes(entry, vpath, schema, data_entries, row_counts)
                elif entry.kind == KIND_EQ_DELETE:
                    index.load_eq_deletes(entry, vpath, schema)
            index.commit()

            # -- eq <-> data type compatibility (exact widening only) ----------
            for grp in index.groups.values():
                for vf in data_files:
                    if vf.entry.partition != grp.partition:
                        continue
                    if not any(ds > vf.entry.data_seq for _, ds in grp.delete_files):
                        continue
                    fschema = contract.schema(vf.entry.schema_id)
                    for fid, del_type in zip(grp.field_ids, grp.field_types):
                        f = fschema.get(fid)
                        if f is None:
                            continue  # missing nullable field participates as null
                        if del_type.common_exact(f.type) is None:
                            raise TypeEvolutionError(
                                f"EQ key field {fid} ({del_type}) is not exactly "
                                f"comparable with {vf.entry.file_id}'s {f.type}; "
                                f"refusing to narrow"
                            )

            # -- per-data-file read plans --------------------------------------
            file_plans = []
            for vf in data_files:
                file_plans.append(
                    self._plan_data_file(contract, vf, target, projection,
                                         predicate, index)
                )

            return Plan(
                contract=contract,
                snapshot=snapshot,
                target_schema=target,
                projection=projection,
                predicate=predicate,
                data_files=data_files,
                delete_index=index,
                file_plans=file_plans,
                work_dir=self.work_dir,
                # an outer guard is owned by the caller; only self-acquired
                # guards are closed with this plan.
                read_guard=guard if owns_guard else None,
            )
        except BaseException:
            if owns_guard and guard is not None:
                guard.close()
            raise

    def _open_data_file(self, contract: TableContract, entry: FileEntry,
                        vpath: Path) -> VerifiedDataFile:
        fschema = contract.schema(entry.schema_id)
        pf = pq.ParquetFile(vpath)
        arrow_names = pf.schema_arrow.names
        arrow_types = {f.name: f.type for f in pf.schema_arrow}
        for fld in fschema.fields:
            if fld.name not in arrow_types:
                raise ContractError(
                    f"DATA {entry.file_id}: parquet lacks physical column "
                    f"{fld.name!r} required by schema {entry.schema_id}"
                )
            if not fld.type.accepts_physical(arrow_types[fld.name]):
                raise ContractError(
                    f"DATA {entry.file_id}: physical column {fld.name!r} has "
                    f"type {arrow_types[fld.name]}, declared {fld.type}"
                )
        md = pf.metadata
        rg_rows = [md.row_group(i).num_rows for i in range(md.num_row_groups)]
        offsets = [0]
        for n in rg_rows:
            offsets.append(offsets[-1] + n)
        return VerifiedDataFile(
            entry=entry,
            path=vpath,
            num_rows=md.num_rows,
            rg_rows=rg_rows,
            rg_offsets=offsets[:-1],
            arrow_names=arrow_names,
            arrow_types=arrow_types,
        )

    def _plan_data_file(self, contract, vf: VerifiedDataFile, target: Schema,
                        projection, predicate, index: DeleteIndex) -> DataFilePlan:
        entry = vf.entry
        fschema = contract.schema(entry.schema_id)

        # applicable eq groups: same partition; seq strictness applied at probe
        groups = [g for g in index.groups.values()
                  if g.partition == entry.partition
                  and any(ds > entry.data_seq for _, ds in g.delete_files)]

        # fields needed from the file
        wanted = set(projection)
        if predicate is not None:
            wanted |= predicate.field_ids()
        target_field_source = {}
        phys = []
        for fid in sorted(wanted):
            tf = target.field(fid)
            sf = fschema.get(fid)
            if sf is None:
                if tf.required:
                    raise ContractError(
                        f"DATA {entry.file_id}: required field {fid} "
                        f"({tf.name!r}) absent from file schema "
                        f"{entry.schema_id}"
                    )
                target_field_source[fid] = ("null", None)
                continue
            if not sf.type.widens_to(tf.type):
                raise TypeEvolutionError(
                    f"DATA {entry.file_id}: field {fid} stored as {sf.type} "
                    f"cannot be read as {tf.type} (illegal narrowing)"
                )
            target_field_source[fid] = ("physical", sf.name)
            if sf.name not in phys:
                phys.append(sf.name)

        eq_key_source = {}
        for grp in groups:
            for fid in grp.field_ids:
                if fid in eq_key_source:
                    continue
                sf = fschema.get(fid)
                eq_key_source[fid] = sf.name if sf is not None else None
                if sf is not None and sf.name not in phys:
                    phys.append(sf.name)

        # row-group pruning (conservative; missing stats keep candidates)
        selected, skipped = [], {}
        rgs = list(range(len(vf.rg_rows)))
        if self.prune and predicate is not None:
            pf = pq.ParquetFile(vf.path)
            md = pf.metadata
            name_to_idx = {n: i for i, n in enumerate(vf.arrow_names)}

            def stats_for(fid, _rg=None):
                sf = fschema.get(fid)
                if sf is None:
                    return ColStats(present=False)
                ci = name_to_idx[sf.name]
                st = md.row_group(_rg).column(ci).statistics
                if st is None:
                    return ColStats(present=True)
                return ColStats(
                    present=True,
                    has_min_max=bool(st.has_min_max),
                    vmin=st.min if st.has_min_max else None,
                    vmax=st.max if st.has_min_max else None,
                    null_count=st.null_count,
                    min_max_exact=bool(getattr(st, "is_min_max_exact", False)),
                )

            for rg in rgs:
                if definitely_not_true(predicate, lambda fid: stats_for(fid, rg)):
                    skipped[rg] = "predicate cannot be TRUE (parquet statistics)"
                else:
                    selected.append(rg)
        else:
            selected = rgs

        return DataFilePlan(
            data=vf,
            selected_rgs=selected,
            rg_skipped=skipped,
            phys_columns=phys,
            target_field_source=target_field_source,
            eq_key_source=eq_key_source,
            groups=groups,
        )

    # ------------------------------------------------------------------
    # scan
    # ------------------------------------------------------------------

    def scan(self, snapshot_id: str, target_schema_id: int, projection,
             predicate=None, _plan: Optional[Plan] = None) -> Iterator[ScanBatch]:
        owns_plan = _plan is None
        plan = _plan or self.plan(snapshot_id, target_schema_id, projection,
                                  predicate)
        target = plan.target_schema
        try:
            for fp in plan.file_plans:
                yield from self._scan_file(plan, fp, target)
        finally:
            # The guard spans the whole streaming lifetime -- including a
            # reader paused between batches -- and is released on normal
            # exhaustion, explicit generator close, or an exception.  A plan
            # handed in by the caller follows the caller's own lifecycle.
            if owns_plan:
                plan.close()

    def _scan_file(self, plan: Plan, fp: DataFilePlan,
                   target: Schema) -> Iterator[ScanBatch]:
        vf = fp.data
        entry = vf.entry
        fschema = plan.contract.schema(entry.schema_id)
        pf = pq.ParquetFile(vf.path)
        proj_fields = [target.field(fid) for fid in plan.projection]

        for rg in fp.selected_rgs:
            base = vf.rg_offsets[rg]
            consumed = 0
            # when nothing is needed but row counts, read one cheap column
            read_cols = fp.phys_columns or vf.arrow_names[:1]
            for batch in pf.iter_batches(
                batch_size=self.batch_size, row_groups=[rg],
                columns=read_cols,
            ):
                n = batch.num_rows
                if n == 0:
                    continue
                start = base + consumed
                consumed += n
                positions = list(range(start, start + n))
                colidx = {name: i for i, name in enumerate(batch.schema.names)}

                def phys_col(name):
                    return batch.column(colidx[name])

                # target-typed columns for projection/predicate
                tcols: dict[int, pa.Array] = {}

                def target_col(fid):
                    arr = tcols.get(fid)
                    if arr is None:
                        kind, name = fp.target_field_source[fid]
                        tf = target.field(fid)
                        if kind == "null":
                            arr = pa.nulls(n, tf.type.to_pyarrow())
                        else:
                            arr = phys_col(name)
                            want = tf.type.to_pyarrow()
                            if arr.type != want:
                                arr = pc.cast(arr, want, safe=True)
                        tcols[fid] = arr
                    return arr

                # ---- delete matching (before projection) -----------------
                dead = self._pos_mask(plan, entry.file_id, start, n)
                for grp in fp.groups:
                    keys = self._encode_data_keys(
                        grp, fp, fschema, phys_col, n)
                    matched = plan.delete_index.eq_matched_keys(
                        grp.group_id, entry.data_seq, keys)
                    if matched:
                        gm = [k in matched for k in keys]
                        dead = [d or g for d, g in zip(dead, gm)]
                keep = pa.array([not d for d in dead])

                # ---- predicate (three-valued; only TRUE selected) --------
                if plan.predicate is not None:
                    pm = true_mask(plan.predicate, target_col)
                    final = pc.and_(keep, pm)
                else:
                    final = keep

                # ---- projection ------------------------------------------
                out_arrays = []
                out_names = []
                mask_arr = final
                for tf in proj_fields:
                    out_arrays.append(target_col(tf.field_id).filter(mask_arr))
                    out_names.append(tf.name)
                out_pos = [p for p, m in zip(positions, final.to_pylist()) if m]
                if not out_pos:
                    continue
                rb = pa.record_batch(out_arrays, names=out_names)
                yield ScanBatch(entry.file_id, out_pos, rb)

    def _pos_mask(self, plan: Plan, file_id: str, start: int, n: int) -> list:
        deleted = plan.delete_index.pos_deleted_positions(
            file_id, start, start + n)
        return [p in deleted for p in range(start, start + n)]

    @staticmethod
    def _encode_data_keys(grp: EqGroup, fp: DataFilePlan, fschema: Schema,
                          phys_col: Callable, n: int) -> list:
        """Encode this batch's key tuples in the group's canonical domain.

        A field absent from the data file's schema participates as null.
        """
        if len(grp.field_ids) == 1:  # hot path
            fid = grp.field_ids[0]
            name = fp.eq_key_source.get(fid)
            ftype0 = grp.field_types[0]
            if name is None:
                return [encode_key_value(ftype0, None)] * n
            return [encode_key_value(ftype0, v)
                    for v in phys_col(name).to_pylist()]
        cols = []
        for fid in grp.field_ids:
            name = fp.eq_key_source.get(fid)
            if name is None:
                cols.append([None] * n)
            else:
                cols.append(phys_col(name).to_pylist())
        out = []
        for row in zip(*cols):
            out.append(encode_key_tuple(grp.field_types, list(row)))
        return out

    # ------------------------------------------------------------------
    # explain
    # ------------------------------------------------------------------

    def explain_row(self, snapshot_id: str, file_id: str, position: int,
                    target_schema_id: Optional[int] = None) -> dict:
        """Explain which delete file(s) exclude one physical row."""
        contract = self._contract or TableContract.load(self.root)
        self._contract = contract
        snapshot = contract.snapshot(snapshot_id)
        if target_schema_id is None:
            target_schema_id = snapshot.files[0].schema_id
        plan = self.plan(snapshot_id, target_schema_id, projection=())
        try:
            vf = next((d for d in plan.data_files if d.entry.file_id == file_id), None)
            if vf is None:
                raise ContractError(
                    f"{file_id!r} is not an active DATA file of snapshot "
                    f"{snapshot_id!r}"
                )
            if not (0 <= position < vf.num_rows):
                raise SnapshotError(
                    f"row_position {position} out of range for {file_id} "
                    f"({vf.num_rows} rows)"
                )
            reasons = []
            for dfid in plan.delete_index.pos_delete_files_for(file_id, position):
                reasons.append({
                    "delete_file_id": dfid,
                    "kind": KIND_POS_DELETE,
                    "detail": f"position delete on ({file_id}, {position})",
                })
            fp = next(p for p in plan.file_plans if p.data.entry.file_id == file_id)
            if fp.groups:
                fschema = contract.schema(vf.entry.schema_id)
                row_values = self._read_row(vf, fschema, fp, position)
                for grp in fp.groups:
                    values = [row_values.get(fid) for fid in grp.field_ids]
                    key = encode_key_tuple(grp.field_types, values)
                    for dfid in plan.delete_index.eq_delete_files_for(
                            grp.group_id, vf.entry.data_seq, key):
                        reasons.append({
                            "delete_file_id": dfid,
                            "kind": KIND_EQ_DELETE,
                            "detail": "equality key match on field ids "
                                      f"{list(grp.field_ids)}",
                        })
            return {
                "snapshot": snapshot_id,
                "file_id": file_id,
                "row_position": position,
                "deleted": bool(reasons),
                "reasons": reasons,
            }
        finally:
            plan.close()

    def _read_row(self, vf: VerifiedDataFile, fschema: Schema,
                  fp: DataFilePlan, position: int) -> dict:
        # locate row group
        rg = 0
        for i, off in enumerate(vf.rg_offsets):
            if off <= position:
                rg = i
        within = position - vf.rg_offsets[rg]
        names = [n for n in
                 (fp.eq_key_source.get(fid) for fid in
                  {f for g in fp.groups for f in g.field_ids})
                 if n]
        names = sorted(set(names))
        pf = pq.ParquetFile(vf.path)
        tbl = pf.read_row_group(rg, columns=names or None)
        out = {}
        for fid in {f for g in fp.groups for f in g.field_ids}:
            name = fp.eq_key_source.get(fid)
            if name is None:
                out[fid] = None
            else:
                out[fid] = tbl.column(name)[within].as_py()
        return out

    def explain_plan(self, snapshot_id: str, target_schema_id: int,
                     projection, predicate=None) -> dict:
        plan = self.plan(snapshot_id, target_schema_id, projection, predicate)
        try:
            return self._explain_plan_dict(plan, snapshot_id, target_schema_id)
        finally:
            plan.close()

    def _explain_plan_dict(self, plan: Plan, snapshot_id: str,
                           target_schema_id: int) -> dict:
        idx = plan.delete_index
        files = []
        plans_by_id = {p.data.entry.file_id: p for p in plan.file_plans}
        for entry in plan.snapshot.files:
            item = {
                "file_id": entry.file_id,
                "kind": entry.kind,
                "data_seq": entry.data_seq,
                "file_seq": entry.file_seq,
                "schema_id": entry.schema_id,
                "partition": partition_to_json(entry.partition),
                "path": entry.path,
                "sha256": entry.sha256,
            }
            if entry.kind == KIND_DATA:
                fp = plans_by_id[entry.file_id]
                item.update({
                    "rows": fp.data.num_rows,
                    "row_groups": len(fp.data.rg_rows),
                    "row_groups_kept": fp.selected_rgs,
                    "row_groups_pruned": {str(k): v for k, v in fp.rg_skipped.items()},
                })
            files.append(item)
        deletes = []
        for entry in plan.snapshot.files:
            if entry.kind == KIND_POS_DELETE:
                cur = idx.db.execute(
                    "SELECT DISTINCT target_file_id FROM pos_keys "
                    "WHERE delete_file_id=? ORDER BY 1", (entry.file_id,))
                applies = [r[0] for r in cur.fetchall()]
                deletes.append({
                    "file_id": entry.file_id, "kind": entry.kind,
                    "data_seq": entry.data_seq, "applies_to": applies,
                    "rule": "target active DATA, target.data_seq <= "
                            "delete.data_seq, equal partition, valid position",
                })
            elif entry.kind == KIND_EQ_DELETE:
                applies = []
                for vf in plan.data_files:
                    if vf.entry.partition == entry.partition and \
                            entry.data_seq > vf.entry.data_seq:
                        applies.append(vf.entry.file_id)
                deletes.append({
                    "file_id": entry.file_id, "kind": entry.kind,
                    "data_seq": entry.data_seq,
                    "eq_field_ids": list(entry.eq_field_ids or ()),
                    "applies_to": sorted(applies),
                    "rule": "equal partition and DATA.data_seq strictly less "
                            "than delete.data_seq",
                })
        return {
            "table": plan.contract.table_id,
            "snapshot": snapshot_id,
            "lineage": plan.contract.lineage(snapshot_id),
            "note": "reads resolve only against this snapshot's active_files; "
                    "parent chain is explanatory; file_seq is identity only",
            "target_schema": target_schema_id,
            "projection": list(plan.projection),
            "files": files,
            "delete_files": deletes,
            "delete_index": {
                "work_dir": plan.work_dir,
                "pos_entries": idx.pos_entry_count,
                "eq_keys": idx.eq_key_count,
                "eq_groups": len(idx.groups),
            },
            "diagnostics": [d.to_json() for d in idx.diagnostics],
        }


def collect(scan: Iterator[ScanBatch]) -> list:
    """Materialize a scan as [(file_id, row_position, {name: value}), ...]."""
    rows = []
    for sb in scan:
        cols = {name: sb.batch.column(name).to_pylist() for name in sb.batch.schema.names}
        for i, pos in enumerate(sb.positions):
            rows.append((sb.file_id, pos, {k: v[i] for k, v in cols.items()}))
    return rows
