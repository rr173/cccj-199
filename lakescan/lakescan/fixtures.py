"""Build the offline-lake drill table: parquet files + table.json.

Layout under the chosen root:

    table.json
    files/*.parquet

The fixtures cover, in one table: the main drill (A/B/EQ6/POS6), branch
snapshots, strict/non-strict data_seq boundaries, cross-row-group positions,
POS/EQ overlap, null keys, field renames vs. same-name new ids, decimal
widening, diagnostics (inactive_target / partition_mismatch /
data_seq_excluded), and broken snapshots that must fail at scan time.
"""

from __future__ import annotations

import json
import shutil
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .table import FORMAT, sha256_of

# schema ids
S1, S2, S3 = 1, 2, 3
S_POS, S_EQ_K, S_EQ_CODE7, S_EQ_AMT, S_EQ_CODE9 = 90, 91, 92, 93, 94

SCHEMAS = [
    {
        "schema_id": S1,
        "fields": [
            {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"}, "required": True},
            {"field_id": 2, "name": "k", "type": {"kind": "int32"}},
            {"field_id": 3, "name": "amt", "type": {"kind": "decimal", "precision": 10, "scale": 2}},
            {"field_id": 7, "name": "code", "type": {"kind": "utf8"}},
        ],
    },
    {
        "schema_id": S2,
        "fields": [
            {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"}, "required": True},
            {"field_id": 2, "name": "k", "type": {"kind": "int64"}},
            {"field_id": 3, "name": "amt", "type": {"kind": "decimal", "precision": 14, "scale": 2}},
            {"field_id": 7, "name": "code_v2", "type": {"kind": "utf8"}},  # renamed, same identity
            {"field_id": 9, "name": "code", "type": {"kind": "utf8"}},  # same name, NEW identity
            {"field_id": 4, "name": "flag", "type": {"kind": "boolean"}},
        ],
    },
    {
        "schema_id": S3,
        "fields": [
            {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"}, "required": True},
            {"field_id": 2, "name": "k", "type": {"kind": "int32"}},
            {"field_id": 3, "name": "amt", "type": {"kind": "decimal", "precision": 10, "scale": 2}},
            # note: no field 7 here at all
        ],
    },
    {
        "schema_id": S_POS,
        "fields": [
            {"field_id": 1000, "name": "target_file_id", "type": {"kind": "utf8"}, "required": True},
            {"field_id": 1001, "name": "row_position", "type": {"kind": "int64"}, "required": True},
        ],
    },
    {"schema_id": S_EQ_K, "fields": [{"field_id": 2, "name": "k", "type": {"kind": "int32"}}]},
    {"schema_id": S_EQ_CODE7, "fields": [{"field_id": 7, "name": "code", "type": {"kind": "utf8"}}]},
    {
        "schema_id": S_EQ_AMT,
        "fields": [{"field_id": 3, "name": "amt", "type": {"kind": "decimal", "precision": 10, "scale": 2}}],
    },
    {"schema_id": S_EQ_CODE9, "fields": [{"field_id": 9, "name": "code", "type": {"kind": "utf8"}}]},
]

_T1 = {"tenant": "t1"}


class _Builder:
    def __init__(self, root: Path):
        self.root = Path(root)
        (self.root / "files").mkdir(parents=True, exist_ok=True)
        self._seq = 0

    def write(self, name: str, columns: dict, pa_schema: pa.Schema,
              row_group_size=None) -> tuple:
        rel = f"files/{name}.parquet"
        p = self.root / rel
        table = pa.table(columns, schema=pa_schema)
        pq.write_table(table, p, row_group_size=row_group_size)
        return rel, sha256_of(p)

    def copy(self, name: str, src_rel: str) -> tuple:
        rel = f"files/{name}.parquet"
        shutil.copyfile(self.root / src_rel, self.root / rel)
        return rel, sha256_of(self.root / rel)


def _s1(row_uid, k, amt, code):
    return (row_uid, k, amt, code)


def build(root) -> Path:
    root = Path(root)
    b = _Builder(root)
    files = {}  # logical name -> (rel, sha)

    # ---- DATA schemas' physical arrow schemas ----------------------------
    s1_arrow = pa.schema([
        ("row_uid", pa.string()),
        ("k", pa.int32()),
        ("amt", pa.decimal128(10, 2)),
        ("code", pa.string()),
    ])
    s2_arrow = pa.schema([
        ("row_uid", pa.string()),
        ("k", pa.int64()),
        ("amt", pa.decimal128(14, 2)),
        ("code_v2", pa.string()),
        ("code", pa.string()),
        ("flag", pa.bool_()),
    ])
    pos_arrow = pa.schema([("target_file_id", pa.string()), ("row_position", pa.int64())])
    eqk_arrow = pa.schema([("k", pa.int32())])
    eqcode_arrow = pa.schema([("code", pa.string())])
    eqamt_arrow = pa.schema([("amt", pa.decimal128(10, 2))])

    D = Decimal

    def s1_rows(rows):
        return {
            "row_uid": [r[0] for r in rows],
            "k": [r[1] for r in rows],
            "amt": pa.array([D(r[2]) if r[2] is not None else None for r in rows],
                            pa.decimal128(10, 2)),
            "code": [r[3] for r in rows],
        }

    def s2_rows(rows):
        return {
            "row_uid": [r[0] for r in rows],
            "k": [r[1] for r in rows],
            "amt": pa.array([D(r[2]) if r[2] is not None else None for r in rows],
                            pa.decimal128(14, 2)),
            "code_v2": [r[3] for r in rows],
            "code": [r[4] for r in rows],
            "flag": [r[5] for r in rows],
        }

    # ---- main drill -------------------------------------------------------
    files["A"] = b.write("A", s1_rows([
        ("u1", 1, "10.00", "a"),
        ("u1", None, "20.00", None),   # same business row_uid as row 0
        ("v2", 2, "30.00", "b"),
    ]), s1_arrow, row_group_size=2)      # row groups [0,1] [2]
    files["B"] = b.write("B", s1_rows([
        ("b1", 1, "11.00", "x"),
        ("b2", 3, "12.00", "y"),
    ]), s1_arrow)
    files["EQ6"] = b.write("EQ6", {"k": [1, 1]}, eqk_arrow)  # duplicate key
    files["POS6"] = b.write("POS6", {
        "target_file_id": ["A", "A"], "row_position": [1, 1],  # duplicate entry
    }, pos_arrow)

    # ---- branch files -----------------------------------------------------
    files["C"] = b.write("C", s1_rows([("c1", 8, "1.00", "c")]), s1_arrow)
    files["D"] = b.write("D", s1_rows([("d1", 9, "2.00", "d")]), s1_arrow)

    # ---- boundary + file_seq irrelevance ----------------------------------
    files["E"] = b.write("E", s1_rows([("e0", 5, "1.00", "e0"),
                                       ("e1", 6, "2.00", "e1")]), s1_arrow)
    files["J"] = b.write("J", s1_rows([("j0", 9, "1.00", "j0"),
                                       ("j1", 1, "2.00", "j1")]), s1_arrow)
    files["EQ_SAME"] = b.write("EQ_SAME", {"k": [5]}, eqk_arrow)
    files["POS_SAME"] = b.write("POS_SAME", {
        "target_file_id": ["E"], "row_position": [1],
    }, pos_arrow)
    files["EQ_J"] = b.write("EQ_J", {"k": [9]}, eqk_arrow)

    # ---- overlap + cross-row-group + identical-content files --------------
    files["F"] = b.write("F", s1_rows([
        ("f0", 7, "1.00", "f0"),
        ("f1", 8, "2.00", "f1"),
        ("f2", 9, "3.00", "f2"),
        ("f3", 9, "4.00", "f3"),
        ("f4", 10, "5.00", "f4"),
    ]), s1_arrow, row_group_size=2)      # row groups [0,1] [2,3] [4]
    files["EQF"] = b.write("EQF", {"k": [9]}, eqk_arrow)
    files["POSF"] = b.write("POSF", {
        "target_file_id": ["F", "F"], "row_position": [3, 4],
    }, pos_arrow)
    files["K1"] = b.write("K1", s1_rows([("kA", 1, "1.00", "kA"),
                                         ("kB", 2, "2.00", "kB")]), s1_arrow)
    files["K2"] = b.copy("K2", files["K1"][0])  # identical bytes, other file_id
    files["POS_K"] = b.write("POS_K", {
        "target_file_id": ["K1"], "row_position": [0],
    }, pos_arrow)

    # ---- rename + null keys ------------------------------------------------
    files["G"] = b.write("G", s1_rows([
        ("g0", 1, "1.00", "keep"),
        ("g1", 2, "2.00", None),
        ("g2", 3, "3.00", "keep"),
    ]), s1_arrow)
    files["H"] = b.write("H", s2_rows([
        ("h0", 1, "10.00", "z", "new1", True),
        ("h1", None, "20.00", None, None, False),
    ]), s2_arrow)
    # N uses schema 3: field 7 is absent entirely (not just null)
    files["N"] = b.write("N", {
        "row_uid": ["n0", "n1"],
        "k": pa.array([1, 2], pa.int32()),
        "amt": pa.array([D("1.00"), D("2.00")], pa.decimal128(10, 2)),
    }, pa.schema([("row_uid", pa.string()), ("k", pa.int32()),
                  ("amt", pa.decimal128(10, 2))]))
    files["EQ_NULL7"] = b.write("EQ_NULL7", {"code": [None]}, eqcode_arrow)
    files["EQ9"] = b.write("EQ9", {"code": ["new1"]}, eqcode_arrow)

    # ---- decimal widening ---------------------------------------------------
    files["P"] = b.write("P", s1_rows([
        ("p0", 1, "10.50", "p0"),
        ("p1", 2, "20.25", "p1"),
        ("p2", 3, "99.99", "p2"),
    ]), s1_arrow)
    files["Q"] = b.write("Q", s2_rows([
        ("q0", 1, "10.50", "q0old", "q0new", True),
        ("q1", 2, "20.25", None, None, None),
    ]), s2_arrow)
    files["EQ_AMT"] = b.write("EQ_AMT", {
        "amt": pa.array([D("10.50")], pa.decimal128(10, 2)),
    }, eqamt_arrow)

    # ---- three-valued-logic playground --------------------------------------
    files["M"] = b.write("M", s2_rows([
        ("m0", 1, "1.00", "m0", "c0", True),
        ("m1", None, "2.00", "m1", None, False),
        ("m2", 2, "3.00", None, "c2", None),
        ("m3", 3, "4.00", "m3", "c3", True),
    ]), s2_arrow)

    # ---- diagnostics ---------------------------------------------------------
    files["POS_BAD1"] = b.write("POS_BAD1", {
        "target_file_id": ["GHOST", "B"], "row_position": [0, 0],
    }, pos_arrow)
    files["POS_BAD2"] = b.write("POS_BAD2", {
        "target_file_id": ["A"], "row_position": [0],
    }, pos_arrow)

    # ---- broken snapshots (must fail at scan time, not at build) ------------
    files["POS_OOR"] = b.write("POS_OOR", {
        "target_file_id": ["A"], "row_position": [99],
    }, pos_arrow)
    files["EQ_BADKEY"] = b.write("EQ_BADKEY", {
        "k": pa.array([2**40], pa.int64()),  # not representable in declared int32
    }, pa.schema([("k", pa.int64())]))

    # ---- assemble table.json -------------------------------------------------
    def ent(name, kind, schema_id, data_seq, file_seq, partition=None,
            eq_field_ids=None):
        rel, sha = files[name]
        e = {
            "file_id": name,
            "kind": kind,
            "path": rel,
            "sha256": sha,
            "partition": dict(partition if partition is not None else _T1),
            "data_seq": data_seq,
            "file_seq": file_seq,
            "schema_id": schema_id,
        }
        if eq_field_ids is not None:
            e["eq_field_ids"] = eq_field_ids
        return e

    A = ent("A", "DATA", S1, 5, 5)
    B = ent("B", "DATA", S1, 6, 60)
    EQ6 = ent("EQ6", "EQ_DELETE", S_EQ_K, 6, 66, eq_field_ids=[2])
    POS6 = ent("POS6", "POS_DELETE", S_POS, 6, 67)
    C = ent("C", "DATA", S1, 7, 70)
    D = ent("D", "DATA", S1, 7, 71)
    E = ent("E", "DATA", S1, 6, 80)
    J = ent("J", "DATA", S1, 5, 9999)  # huge file_seq must not matter
    EQ_SAME = ent("EQ_SAME", "EQ_DELETE", S_EQ_K, 6, 81, eq_field_ids=[2])
    POS_SAME = ent("POS_SAME", "POS_DELETE", S_POS, 6, 82)
    EQ_J = ent("EQ_J", "EQ_DELETE", S_EQ_K, 6, 83, eq_field_ids=[2])
    F = ent("F", "DATA", S1, 5, 90)
    EQF = ent("EQF", "EQ_DELETE", S_EQ_K, 6, 91, eq_field_ids=[2])
    POSF = ent("POSF", "POS_DELETE", S_POS, 6, 92)
    K1 = ent("K1", "DATA", S1, 5, 95)
    K2 = ent("K2", "DATA", S1, 5, 96)
    POS_K = ent("POS_K", "POS_DELETE", S_POS, 6, 97)
    G = ent("G", "DATA", S1, 5, 100)
    H = ent("H", "DATA", S2, 7, 101)
    N = ent("N", "DATA", S3, 5, 105)  # schema without field 7
    EQ_NULL7 = ent("EQ_NULL7", "EQ_DELETE", S_EQ_CODE7, 6, 102, eq_field_ids=[7])
    EQ9 = ent("EQ9", "EQ_DELETE", S_EQ_CODE9, 8, 103, eq_field_ids=[9])
    P = ent("P", "DATA", S1, 5, 110)
    Q = ent("Q", "DATA", S2, 7, 111)
    EQ_AMT = ent("EQ_AMT", "EQ_DELETE", S_EQ_AMT, 6, 112, eq_field_ids=[3])
    M = ent("M", "DATA", S2, 5, 120)
    POS_BAD1 = ent("POS_BAD1", "POS_DELETE", S_POS, 5, 130)
    POS_BAD2 = ent("POS_BAD2", "POS_DELETE", S_POS, 6, 131, partition={"tenant": "t2"})
    POS_OOR = ent("POS_OOR", "POS_DELETE", S_POS, 6, 140)
    EQ_BADKEY = ent("EQ_BADKEY", "EQ_DELETE", S_EQ_K, 6, 141, eq_field_ids=[2])

    snapshots = [
        {"snapshot_id": "s_base", "parent": None, "active_files": [A, B]},
        {"snapshot_id": "s_main", "parent": "s_base",
         "active_files": [A, B, EQ6, POS6]},
        {"snapshot_id": "s_nopos", "parent": "s_base",
         "active_files": [A, B, EQ6]},
        {"snapshot_id": "s_fork_c", "parent": "s_main",
         "active_files": [A, B, EQ6, POS6, C]},
        {"snapshot_id": "s_fork_d", "parent": "s_main",
         "active_files": [A, B, EQ6, POS6, D]},
        {"snapshot_id": "s_bound", "parent": "s_base",
         "active_files": [E, J, EQ_SAME, POS_SAME, EQ_J]},
        {"snapshot_id": "s_overlap", "parent": "s_base",
         "active_files": [F, K1, K2, EQF, POSF, POS_K]},
        {"snapshot_id": "s_ren", "parent": "s_base",
         "active_files": [G, H, N, EQ_NULL7, EQ9]},
        {"snapshot_id": "s_n_plain", "parent": "s_base",
         "active_files": [N]},
        {"snapshot_id": "s_dec", "parent": "s_base",
         "active_files": [P, Q, EQ_AMT]},
        {"snapshot_id": "s_3vl", "parent": "s_base",
         "active_files": [M]},
        {"snapshot_id": "s_diag", "parent": "s_base",
         "active_files": [A, B, POS_BAD1, POS_BAD2]},
        {"snapshot_id": "s_broken_pos", "parent": "s_base",
         "active_files": [A, POS_OOR]},
        {"snapshot_id": "s_badkey", "parent": "s_base",
         "active_files": [A, EQ_BADKEY]},
    ]

    table_json = {
        "format": FORMAT,
        "table_id": "drill",
        "schemas": SCHEMAS,
        "snapshots": snapshots,
    }
    with open(root / "table.json", "w", encoding="utf-8") as fh:
        json.dump(table_json, fh, indent=2)
    return root
