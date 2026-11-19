"""Runtime-built fixture table for the cross-snapshot key diff.

The table is generated on demand by the tests (and rebuildable via the CLI)
so the repository carries no bulky data artifacts.  It deliberately packs
every acceptance scenario into one small, hand-derivable story:

schemas
  10 (S_A): row_uid(1, utf8), k(2, int32) the business key,
            amt(3, decimal(10,2)), code(7, utf8)
  20 (S_B): same identities with exact widenings: k int32 -> int64,
            amt decimal(10,2) -> decimal(14,2); field 7 renamed code -> code_v2;
            a *new* field 9 reuses the physical name "code" (new identity,
            does not inherit field 7); adds nullable flag(4, boolean)
  90: POS_DELETE schema (1000/1001); 91: EQ_DELETE schema on key field 2

snapshots (all in partition {"tenant": "t1"} unless noted)
  d_source     baseline, with its own POS + EQ deletes already applied
  d_target     forked evolution: inserts, updates, one delete-then-reinsert
               of the same key, target-only POS/EQ deletes, compacted and
               replacement files (same row_uid appearing in new files)
  d_branch_b   a different branch off the baseline (active_files disjoint
               from d_target's added files)
  d_broken_null a snapshot whose visible rows contain a null business key
  d_broken_dup  a snapshot whose visible rows contain a duplicated key

The expected diff records are derived key-by-key in the tests and again,
independently, in verify_compare.py without importing any library code.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .table import FORMAT, sha256_of

S_A, S_B, S_POS, S_EQK = 10, 20, 90, 91

PART = {"tenant": "t1"}

SCHEMAS = [
    {
        "schema_id": S_A,
        "fields": [
            {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 2, "name": "k", "type": {"kind": "int32"},
             "required": True},
            {"field_id": 3, "name": "amt",
             "type": {"kind": "decimal", "precision": 10, "scale": 2}},
            {"field_id": 7, "name": "code", "type": {"kind": "utf8"}},
        ],
    },
    {
        "schema_id": S_B,
        "fields": [
            {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 2, "name": "k", "type": {"kind": "int64"},
             "required": True},
            {"field_id": 3, "name": "amt",
             "type": {"kind": "decimal", "precision": 14, "scale": 2}},
            {"field_id": 7, "name": "code_v2", "type": {"kind": "utf8"}},
            {"field_id": 9, "name": "code", "type": {"kind": "utf8"}},
            {"field_id": 4, "name": "flag", "type": {"kind": "boolean"}},
        ],
    },
    {
        "schema_id": S_POS,
        "fields": [
            {"field_id": 1000, "name": "target_file_id",
             "type": {"kind": "utf8"}, "required": True},
            {"field_id": 1001, "name": "row_position",
             "type": {"kind": "int64"}, "required": True},
        ],
    },
    {"schema_id": S_EQK,
     "fields": [{"field_id": 2, "name": "k", "type": {"kind": "int32"}}]},
]

A_ARROW = pa.schema([
    ("row_uid", pa.string()),
    ("k", pa.int32()),
    ("amt", pa.decimal128(10, 2)),
    ("code", pa.string()),
])
B_ARROW = pa.schema([
    ("row_uid", pa.string()),
    ("k", pa.int64()),
    ("amt", pa.decimal128(14, 2)),
    ("code_v2", pa.string()),
    ("code", pa.string()),
    ("flag", pa.bool_()),
])
POS_ARROW = pa.schema([("target_file_id", pa.string()),
                       ("row_position", pa.int64())])
EQK_ARROW = pa.schema([("k", pa.int32())])


def _a_rows(rows):
    """rows: (row_uid, k, amt_str_or_None, code_or_None)."""
    return {
        "row_uid": [r[0] for r in rows],
        "k": pa.array([r[1] for r in rows], pa.int32()),
        "amt": pa.array(
            [Decimal(r[2]) if r[2] is not None else None for r in rows],
            pa.decimal128(10, 2)),
        "code": [r[3] for r in rows],
    }


def _b_rows(rows):
    """rows: (row_uid, k, amt, code_v2, code(=field9), flag)."""
    return {
        "row_uid": [r[0] for r in rows],
        "k": pa.array([r[1] for r in rows], pa.int64()),
        "amt": pa.array(
            [Decimal(r[2]) if r[2] is not None else None for r in rows],
            pa.decimal128(14, 2)),
        "code_v2": [r[3] for r in rows],
        "code": [r[4] for r in rows],
        "flag": [r[5] for r in rows],
    }


def build_diff_table(root) -> Path:
    root = Path(root)
    files_dir = root / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    made = {}

    def write(name, columns, arrow, row_group_size=None):
        rel = f"files/{name}.parquet"
        p = root / rel
        pq.write_table(pa.table(columns, schema=arrow), p,
                       row_group_size=row_group_size)
        made[name] = (rel, sha256_of(p))
        return rel

    # ------------------------------------------------------------------
    # source-side data (schema S_A, data_seq=5)
    # ------------------------------------------------------------------
    # SA1: k 1..4; SA2: k 5..8; SA3: k 9..11.  Row groups are split so
    # position deletes cross row-group boundaries.
    write("SA1", _a_rows([
        ("u1", 1, "10.00", "c1"),
        ("u2", 2, "20.00", "c2"),
        ("u3", 3, "30.00", "c3"),
        ("u4", 4, "40.00", None),
    ]), A_ARROW, row_group_size=2)
    write("SA2", _a_rows([
        ("u5", 5, "50.00", "c5"),
        ("u6", 6, "60.00", "c6"),
        ("u7", 7, "70.00", None),
        ("u8", 8, "80.00", "c8"),
    ]), A_ARROW, row_group_size=2)
    write("SA3", _a_rows([
        ("u9", 9, "90.00", "c9"),
        ("u10", 10, "100.00", "c10"),
        # duplicate business row_uid "u9" but a distinct key k=11: physical
        # duplication of row_uid must not affect key-based identity
        ("u9", 11, "110.00", "c11"),
    ]), A_ARROW)

    # source deletes (seq=6): EQ removes k=3 (from SA1), POS removes SA2 row 1
    # i.e. k=6; the duplicate POS entry deletes that physical row only once.
    write("SRC_EQ", {"k": pa.array([3], pa.int32())}, EQK_ARROW)
    write("SRC_POS", {
        "target_file_id": ["SA2", "SA2"],
        "row_position": pa.array([1, 1], pa.int64()),
    }, POS_ARROW)

    # ------------------------------------------------------------------
    # target-side evolution (new data seq=7, deletes seq=8)
    #
    # Every old physical row whose key is re-homed into a TA* file is first
    # removed by TGT_POS (delete-then-reinsert / compaction); the business
    # delete of k=8 is carried by TGT_EQ so the strict/non-strict data_seq
    # rules are exercised by both delete kinds on both snapshots.
    # ------------------------------------------------------------------
    # TA1 (schema S_B): updated k=1/k=2, re-inserted k=4 (its old SA1 row is
    # POS-deleted) and updated k=7.  Field 9 ("code", a NEW field identity)
    # is populated only for the re-inserted k=4.
    write("TA1", _b_rows([
        ("u1", 1, "10.00", "c1", None, True),     # update: flag None->True
        ("u2", 2, "25.00", "c2", None, None),     # update: amt 20.00->25.00
        ("u4", 4, "40.00", None, "new9", False),  # re-insert + field 9 value
        ("u7", 7, "70.00", "c7x", None, None),    # update: code None->c7x
    ]), B_ARROW)
    # TA2 is the compaction target: k=9 keeps identical values (only its
    # file layout changes), k=10 is updated, k=12/k=13 are brand new keys.
    write("TA2", _b_rows([
        ("u9", 9, "90.00", "c9", None, None),
        ("u10", 10, "105.00", "c10", None, True),
        ("u12", 12, "120.00", "c12", None, None),
        ("u13", 13, None, "c13", None, False),
    ]), B_ARROW, row_group_size=3)
    # target deletes (seq 8).  POS: the old physical rows of every re-homed
    # key -- (SA1,0)=k1 (SA1,1)=k2 (SA1,3)=k4 (SA2,2)=k7 (SA3,0)=k9
    # (SA3,1)=k10.  EQ: k=8, applicable strictly to the old seq-5 files only.
    write("TGT_POS", {
        "target_file_id": ["SA1", "SA1", "SA1", "SA2", "SA3", "SA3"],
        "row_position": pa.array([0, 1, 3, 2, 0, 1], pa.int64()),
    }, POS_ARROW)
    write("TGT_EQ", {"k": pa.array([8], pa.int32())}, EQK_ARROW)

    # ------------------------------------------------------------------
    # other branch (off the baseline): disjoint added DATA file
    # ------------------------------------------------------------------
    write("BR1", _a_rows([
        ("b20", 20, "200.00", "b20"),
        ("b21", 21, "210.00", "b21"),
    ]), A_ARROW)

    # ------------------------------------------------------------------
    # broken inputs: null key and duplicated key among visible rows
    # ------------------------------------------------------------------
    write("BAD_NULL", _a_rows([
        ("n0", 1, "1.00", "n0"),
        ("n1", None, "2.00", "n1"),
    ]), A_ARROW)
    write("BAD_DUP", _a_rows([
        ("d0", 30, "1.00", "d0"),
        ("d1", 30, "2.00", "d1"),
    ]), A_ARROW)

    def ent(name, kind, schema_id, data_seq, file_seq, partition=None,
            eq_field_ids=None):
        rel, sha = made[name]
        e = {
            "file_id": name,
            "kind": kind,
            "path": rel,
            "sha256": sha,
            "partition": dict(partition if partition is not None else PART),
            "data_seq": data_seq,
            "file_seq": file_seq,
            "schema_id": schema_id,
        }
        if eq_field_ids is not None:
            e["eq_field_ids"] = eq_field_ids
        return e

    SA1 = ent("SA1", "DATA", S_A, 5, 101)
    SA2 = ent("SA2", "DATA", S_A, 5, 102)
    SA3 = ent("SA3", "DATA", S_A, 5, 103)
    SRC_EQ = ent("SRC_EQ", "EQ_DELETE", S_EQK, 6, 104, eq_field_ids=[2])
    SRC_POS = ent("SRC_POS", "POS_DELETE", S_POS, 6, 105)
    TA1 = ent("TA1", "DATA", S_B, 7, 201)
    TA2 = ent("TA2", "DATA", S_B, 7, 202)
    TGT_POS = ent("TGT_POS", "POS_DELETE", S_POS, 8, 203)
    TGT_EQ = ent("TGT_EQ", "EQ_DELETE", S_EQK, 8, 204, eq_field_ids=[2])
    BR1 = ent("BR1", "DATA", S_A, 7, 301)
    BAD_NULL = ent("BAD_NULL", "DATA", S_A, 5, 401)
    BAD_DUP = ent("BAD_DUP", "DATA", S_A, 5, 402)

    snapshots = [
        # baseline with its own deletes
        {"snapshot_id": "d_source", "parent": None,
         "active_files": [SA1, SA2, SA3, SRC_EQ, SRC_POS]},
        # forked target: old files retained (each snapshot lists full members)
        {"snapshot_id": "d_target", "parent": "d_source",
         "active_files": [SA1, SA2, SA3, SRC_EQ, SRC_POS,
                          TA1, TA2, TGT_POS, TGT_EQ]},
        # independent branch off the baseline, different added file
        {"snapshot_id": "d_branch_b", "parent": "d_source",
         "active_files": [SA1, SA2, SA3, SRC_EQ, SRC_POS, BR1]},
        # broken: null business key visible
        {"snapshot_id": "d_broken_null", "parent": "d_source",
         "active_files": [SA1, SA2, SA3, SRC_EQ, SRC_POS, BAD_NULL]},
        # broken: duplicate business key visible
        {"snapshot_id": "d_broken_dup", "parent": "d_source",
         "active_files": [SA1, SA2, SA3, SRC_EQ, SRC_POS, BAD_DUP]},
    ]

    table_json = {
        "format": FORMAT,
        "table_id": "diff_drill",
        "schemas": SCHEMAS,
        "snapshots": snapshots,
    }
    with open(root / "table.json", "w", encoding="utf-8") as fh:
        json.dump(table_json, fh, indent=2)
    return root


def source_visible_keys():
    """Hand-derived visible key set of d_source after its own deletes.

    SA1 k=1,2,3,4 minus EQ k=3            -> {1,2,4}
    SA2 k=5,6,7,8 minus POS position 1(k=6) -> {5,7,8}
    SA3 k=9,10,11                          -> {9,10,11}
    """
    return [1, 2, 4, 5, 7, 8, 9, 10, 11]


def target_visible_keys():
    """Hand-derived visible key set of d_target.

    Old SA* files first lose the source deletes (EQ k=3, POS k=6), then the
    target deletes (seq 8): TGT_POS removes the re-homed physical rows
    (SA1,0)=k1 (SA1,1)=k2 (SA1,3)=k4 (SA2,2)=k7 (SA3,0)=k9 (SA3,1)=k10 and
    TGT_EQ removes k=8 (strictly, old files seq 5 < 8 only).  Surviving
    old-file keys: SA2 k=5 and SA3 k=11.
    TA* files have data_seq 7, which is not strictly less than any delete
    seq (6 or 8), so no delete ever applies to them.  TA1 adds
    {1,2,4,7}; TA2 adds {9,10,12,13}.
    Union: {1,2,4,5,7,9,10,11,12,13} -- every key appears exactly once.
    """
    return [1, 2, 4, 5, 7, 9, 10, 11, 12, 13]


def expected_changes():
    """Hand-derived expected change records of d_source -> d_target.

    deletes : {8}
    inserts : {12, 13}
    updates : {1 (flag None->True), 2 (amt 20.00->25.00),
               4 (field 9 null->'new9', flag None->False),
               7 (code null->'c7x'),
               10 (amt 100.00->105.00, flag None->True)}
    unchanged (not emitted): {5, 9, 11}; k=9 changed only its physical file,
    k=11 shares row_uid 'u9' with k=9 but is a distinct business key.
    """
    return {
        "delete": [8],
        "insert": [12, 13],
        "update": [1, 2, 4, 7, 10],
        "unchanged": [5, 9, 11],
    }
