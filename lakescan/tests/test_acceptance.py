"""Acceptance suite for the offline-lake scan library.

Covers: main drill, POS removal restoring a row, branch snapshots,
strict/non-strict data_seq boundaries, cross-row-group positions, POS/EQ
overlap (deleted once), null keys, rename vs. same-name new id, projection
dropping delete keys, SQL three-valued predicates, source byte changes,
disk-spilled large delete sets under a memory bound, reopen/copy rescans,
pruning equivalence, and the pre-scan rejection battery.
"""

import json
import shutil
import tracemalloc
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakescan import (
    ContractError,
    PositionOutOfRangeError,
    PredicateError,
    Scanner,
    SourceVersionMismatch,
    TypeEvolutionError,
    UnknownFieldError,
    UnrepresentableKeyError,
    collect,
)
from lakescan.fixtures import build
from lakescan.table import sha256_of


@pytest.fixture(scope="session")
def table(tmp_path_factory):
    root = tmp_path_factory.mktemp("drill") / "table"
    build(root)
    return root


def scan_rows(table, snapshot, schema=2, projection=(1, 2), predicate=None,
              prune=True, work_dir=None):
    sc = Scanner(table, prune=prune, work_dir=work_dir)
    return collect(sc.scan(snapshot, schema, projection, predicate))


def triplet(rows):
    return [(f, p, tuple(fields.values())) for f, p, fields in rows]


# ---------------------------------------------------------------------------
# main drill
# ---------------------------------------------------------------------------


def test_main_drill(table):
    rows = scan_rows(table, "s_main")
    got = [(f, p, fields["row_uid"], fields["k"]) for f, p, fields in rows]
    # A0 (k=1) deleted by EQ6; A1 deleted by POS6; B.seq=6 not < 6 so B0 keeps k=1
    assert got == [("A", 2, "v2", 2), ("B", 0, "b1", 1), ("B", 1, "b2", 3)]


def test_pos_removed_restores_row_and_null_not_matched(table):
    rows = scan_rows(table, "s_nopos")
    got = [(f, p, fields["row_uid"], fields["k"]) for f, p, fields in rows]
    assert got == [
        ("A", 1, "u1", None),  # restored; EQ key k=1 must not eat the null
        ("A", 2, "v2", 2),
        ("B", 0, "b1", 1),
        ("B", 1, "b2", 3),
    ]


def test_business_row_uid_never_deduplicated(table):
    rows = scan_rows(table, "s_base", projection=(1,))
    uids = [fields["row_uid"] for _, _, fields in rows]
    # A rows 0 and 1 share row_uid "u1": two physical rows, both present
    assert uids.count("u1") == 2


# ---------------------------------------------------------------------------
# snapshots
# ---------------------------------------------------------------------------


def test_branch_snapshots_are_isolated(table):
    got_c = {f for f, _, _ in scan_rows(table, "s_fork_c")}
    got_d = {f for f, _, _ in scan_rows(table, "s_fork_d")}
    assert "C" in got_c and "D" not in got_c
    assert "D" in got_d and "C" not in got_d
    base = {f for f, _, _ in scan_rows(table, "s_main")}
    assert "C" not in base and "D" not in base


def test_seq_boundaries_and_file_seq_irrelevant(table):
    rows = scan_rows(table, "s_bound")
    got = [(f, p, fields["row_uid"], fields["k"]) for f, p, fields in rows]
    # EQ@6 k=5 does NOT delete E0 (E.seq 6 is not strictly < 6)
    # POS@6 (E,1) DOES delete E1 (non-strict <=)
    # J.file_seq=9999 is identity only: EQ_J@6 k=9 deletes J0 (J.seq 5 < 6)
    assert got == [("E", 0, "e0", 5), ("J", 1, "j1", 1)]


# ---------------------------------------------------------------------------
# positions, overlap, identical content
# ---------------------------------------------------------------------------


def test_overlap_and_cross_rowgroup_positions(table):
    rows = scan_rows(table, "s_overlap")
    got = [(f, p, fields["row_uid"]) for f, p, fields in rows]
    # F: EQ k=9 deletes F2,F3; POS deletes F3 (overlap, once) and F4 (row group 2)
    assert ("F", 0, "f0") in got and ("F", 1, "f1") in got
    assert not any(f == "F" and p in (2, 3, 4) for f, p, _ in got)
    # POS_K targets K1 row 0 only; K2 has identical bytes but keeps both rows
    assert ("K1", 1, "kB") in got and not any(f == "K1" and p == 0 for f, p, _ in got)
    assert ("K2", 0, "kA") in got and ("K2", 1, "kB") in got


def test_explain_row_reports_each_delete_kind(table):
    sc = Scanner(table)
    r0 = sc.explain_row("s_main", "A", 0)
    assert r0["deleted"] and [r["delete_file_id"] for r in r0["reasons"]] == ["EQ6"]
    r1 = sc.explain_row("s_main", "A", 1)
    assert [r["delete_file_id"] for r in r1["reasons"]] == ["POS6"]
    alive = sc.explain_row("s_main", "A", 2)
    assert not alive["deleted"] and alive["reasons"] == []
    both = sc.explain_row("s_overlap", "F", 3)
    assert sorted(r["delete_file_id"] for r in both["reasons"]) == ["EQF", "POSF"]
    cross_rg = sc.explain_row("s_overlap", "F", 4)  # row group 2
    assert [r["delete_file_id"] for r in cross_rg["reasons"]] == ["POSF"]


# ---------------------------------------------------------------------------
# schema identity: rename, same-name new id, null keys
# ---------------------------------------------------------------------------


def test_rename_null_keys_and_new_identity(table):
    rows = scan_rows(table, "s_ren", projection=(1, 7, 9))
    got = {(f, p): fields for f, p, fields in rows}
    # G1 deleted by EQ_NULL7 (null key matches missing/null code); G0/G2 survive
    assert got[("G", 0)]["code_v2"] == "keep"  # field 7 renamed, identity kept
    assert got[("G", 0)]["code"] is None  # field 9: same name, NEW identity, no inherit
    assert ("G", 1) not in got
    # H0 deleted by EQ9 on field 9; H1 survives with both code fields null
    assert ("H", 0) not in got
    assert got[("H", 1)]["code_v2"] is None and got[("H", 1)]["code"] is None
    # N's schema has no field 7 at all: it participates as null, so the null
    # key of EQ_NULL7 matches EVERY row of N
    assert not any(f == "N" for f, _ in got)


def test_eq_null_key_matches_missing_field_as_null(table):
    # EQ_NULL7 (field 7, key null) applies to G (seq 5 < 6): G1 has code=null
    sc = Scanner(table)
    r = sc.explain_row("s_ren", "G", 1)
    assert r["deleted"] and r["reasons"][0]["delete_file_id"] == "EQ_NULL7"
    # N's schema lacks field 7 entirely -> null key matches every row
    for pos in (0, 1):
        r = sc.explain_row("s_ren", "N", pos)
        assert r["deleted"] and r["reasons"][0]["delete_file_id"] == "EQ_NULL7"
    # without the delete, N's missing fields read as null
    rows = scan_rows(table, "s_n_plain", projection=(1, 7, 9))
    got = [(f, p, fields["row_uid"], fields["code_v2"], fields["code"])
           for f, p, fields in rows]
    assert got == [("N", 0, "n0", None, None), ("N", 1, "n1", None, None)]


# ---------------------------------------------------------------------------
# decimal widening
# ---------------------------------------------------------------------------


def test_decimal_precision_widening_exact(table):
    rows = scan_rows(table, "s_dec", projection=(1, 3))
    got = [(f, p, fields["row_uid"], fields["amt"]) for f, p, fields in rows]
    assert got == [
        ("P", 1, "p1", Decimal("20.25")),  # P0 (10.50) deleted by EQ_AMT
        ("P", 2, "p2", Decimal("99.99")),
        ("Q", 0, "q0", Decimal("10.50")),  # Q.seq 7 not < 6: survives
        ("Q", 1, "q1", Decimal("20.25")),
    ]
    # values arrived as decimal(14,2) widened from decimal(10,2), exactly
    assert rows[0][2]["amt"].as_tuple().exponent == -2


# ---------------------------------------------------------------------------
# projection / predicates
# ---------------------------------------------------------------------------


def test_projection_dropping_delete_key_still_deletes(table):
    rows = scan_rows(table, "s_main", projection=(1,))  # no k column
    got = [(f, p, fields["row_uid"]) for f, p, fields in rows]
    assert got == [("A", 2, "v2"), ("B", 0, "b1"), ("B", 1, "b2")]


P = lambda op, **kw: dict(op=op, **kw)
INT = lambda v: {"kind": "int", "value": v}


@pytest.mark.parametrize(
    "predicate,expected_positions",
    [
        (P("eq", field_id=2, literal={"kind": "null"}), []),  # always UNKNOWN
        (P("is_null", field_id=2), [1]),
        (P("lt", field_id=2, literal=INT(3)), [0, 2]),  # null -> UNKNOWN, excluded
        (P("or", args=[P("eq", field_id=2, literal=INT(1)),
                       P("is_null", field_id=2)]), [0, 1]),
        (P("and", args=[P("lt", field_id=2, literal=INT(3)),
                        P("not", arg=P("is_null", field_id=2))]), [0, 2]),
        (P("not", arg=P("or", args=[P("eq", field_id=2, literal=INT(1)),
                                    P("eq", field_id=2, literal=INT(3))])), [2]),
        (P("eq", field_id=4, literal={"kind": "bool", "value": True}), [0, 3]),
        (P("and", args=[P("is_null", field_id=4),
                        P("eq", field_id=2, literal=INT(2))]), [2]),
    ],
)
def test_three_valued_predicates(table, predicate, expected_positions):
    rows = scan_rows(table, "s_3vl", projection=(1,), predicate=predicate)
    assert [p for _, p, _ in rows] == expected_positions


def test_decimal_predicate_exact_scale(table):
    pred = P("lt", field_id=3, literal={"kind": "decimal", "unscaled": 5000, "scale": 2})
    rows = scan_rows(table, "s_dec", projection=(1,), predicate=pred)
    assert [(f, p) for f, p, _ in rows] == [("P", 1), ("Q", 0), ("Q", 1)]
    # literal needing more precision than the column: column widens exactly
    big = P("lt", field_id=3,
            literal={"kind": "decimal", "unscaled": 99999999999999999, "scale": 2})
    rows = scan_rows(table, "s_dec", projection=(1,), predicate=big)
    assert [(f, p) for f, p, _ in rows] == [("P", 1), ("P", 2), ("Q", 0), ("Q", 1)]
    # literal with scale 3 that is not exactly representable at scale 2 -> reject
    bad = P("lt", field_id=3, literal={"kind": "decimal", "unscaled": 1234, "scale": 3})
    with pytest.raises(PredicateError):
        scan_rows(table, "s_dec", projection=(1,), predicate=bad)


def test_lt_on_boolean_rejected(table):
    pred = P("lt", field_id=4, literal={"kind": "bool", "value": True})
    with pytest.raises(PredicateError):
        scan_rows(table, "s_3vl", projection=(1,), predicate=pred)


# ---------------------------------------------------------------------------
# pruning: conservative, output identical to full scan
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("snapshot,schema,projection,predicate", [
    ("s_main", 2, (1, 2), None),
    ("s_overlap", 2, (1, 2), P("eq", field_id=2, literal=INT(10))),
    ("s_3vl", 2, (1, 2, 4), P("and", args=[P("lt", field_id=2, literal=INT(3)),
                                           P("not", arg=P("is_null", field_id=2))])),
    ("s_3vl", 2, (1,), P("is_null", field_id=2)),
    ("s_dec", 2, (1, 3), P("lt", field_id=3, literal={"kind": "decimal",
                                                      "unscaled": 5000, "scale": 2})),
    ("s_ren", 2, (1, 7, 9), P("eq", field_id=7, literal={"kind": "utf8",
                                                         "value": "keep"})),
])
def test_pruning_output_identical_to_full_scan(table, snapshot, schema,
                                               projection, predicate):
    pruned = scan_rows(table, snapshot, schema, projection, predicate, prune=True)
    full = scan_rows(table, snapshot, schema, projection, predicate, prune=False)
    assert [(f, p, tuple(sorted(fs.items()))) for f, p, fs in pruned] == [
        (f, p, tuple(sorted(fs.items()))) for f, p, fs in full
    ]


def test_pruning_actually_prunes_row_groups(table):
    sc = Scanner(table, prune=True)
    plan = sc.explain_plan("s_overlap", 2, (1, 2),
                           P("eq", field_id=2, literal=INT(10)))
    f_entry = next(f for f in plan["files"] if f["file_id"] == "F")
    # F row groups: [k=7,8] [k=9,9] [k=10] -> only rg 2 can contain k=10
    assert f_entry["row_groups_kept"] == [2]
    assert set(f_entry["row_groups_pruned"]) == {"0", "1"}


# ---------------------------------------------------------------------------
# pre-scan rejection battery
# ---------------------------------------------------------------------------


def test_illegal_narrowing_rejected(table):
    # H stores k as int64 (schema 2); target schema 1 says int32 -> narrowing
    with pytest.raises(TypeEvolutionError):
        scan_rows(table, "s_ren", schema=1, projection=(1, 2))


def test_unknown_projection_field_rejected(table):
    with pytest.raises(UnknownFieldError):
        scan_rows(table, "s_main", projection=(1, 42))


def test_unknown_snapshot_rejected(table):
    with pytest.raises(ContractError):
        scan_rows(table, "s_nope")


def test_unrepresentable_key_rejected_before_scan(table):
    with pytest.raises(UnrepresentableKeyError):
        scan_rows(table, "s_badkey")


def test_out_of_range_position_is_snapshot_error(table):
    with pytest.raises(PositionOutOfRangeError):
        scan_rows(table, "s_broken_pos")


def test_diagnostics_for_non_applied_pos_entries(table):
    sc = Scanner(table)
    plan = sc.explain_plan("s_diag", 2, (1, 2))
    reasons = {(d["delete_file_id"], d["reason"]) for d in plan["diagnostics"]}
    assert ("POS_BAD1", "inactive_target") in reasons  # GHOST is not active
    assert ("POS_BAD1", "data_seq_excluded") in reasons  # B.seq 6 > 5
    assert ("POS_BAD2", "partition_mismatch") in reasons  # t2 delete vs t1 file
    # ...and the data itself is untouched
    rows = scan_rows(table, "s_diag")
    assert len(rows) == 5


def test_duplicate_field_id_in_schema_rejected(table, tmp_path):
    root = tmp_path / "dup"
    shutil.copytree(table, root)
    tj = json.loads((root / "table.json").read_text())
    s1 = next(s for s in tj["schemas"] if s["schema_id"] == 1)
    s1["fields"].append(dict(s1["fields"][1]))  # duplicate field_id 2
    (root / "table.json").write_text(json.dumps(tj))
    with pytest.raises(ContractError):
        scan_rows(root, "s_base")


def test_duplicate_eq_field_ids_rejected(table, tmp_path):
    root = tmp_path / "dupeq"
    shutil.copytree(table, root)
    tj = json.loads((root / "table.json").read_text())
    snap = next(s for s in tj["snapshots"] if s["snapshot_id"] == "s_main")
    eq6 = next(f for f in snap["active_files"] if f["file_id"] == "EQ6")
    eq6["eq_field_ids"] = [2, 2]
    (root / "table.json").write_text(json.dumps(tj))
    with pytest.raises(ContractError):
        scan_rows(root, "s_main")


def test_incompatible_decimal_scale_rejected(table, tmp_path):
    root = tmp_path / "badscale"
    shutil.copytree(table, root)
    tj = json.loads((root / "table.json").read_text())
    # field 3 is decimal(*,2) everywhere; a decimal(*,3) appearance is illegal
    tj["schemas"].append({
        "schema_id": 95,
        "fields": [{"field_id": 3, "name": "amt",
                    "type": {"kind": "decimal", "precision": 10, "scale": 3}}],
    })
    (root / "table.json").write_text(json.dumps(tj))
    with pytest.raises(TypeEvolutionError):
        scan_rows(root, "s_base")


# ---------------------------------------------------------------------------
# source versioning
# ---------------------------------------------------------------------------


def test_source_byte_change_detected(table, tmp_path):
    root = tmp_path / "copy"
    shutil.copytree(table, root)
    # unmodified copy scans fine (files located by path, pinned by digest)
    assert len(scan_rows(root, "s_main")) == 3
    # flip bytes inside A.parquet -> source version mismatch, no stale results
    a = root / "files" / "A.parquet"
    data = bytearray(a.read_bytes())
    data[len(data) // 2] ^= 0xFF
    a.write_bytes(bytes(data))
    with pytest.raises(SourceVersionMismatch):
        scan_rows(root, "s_main")


def test_reopen_and_copied_tree_rescan_identical(table, tmp_path):
    first = triplet(scan_rows(table, "s_main"))
    second = triplet(scan_rows(table, "s_main"))  # reopen: fresh Scanner
    root = tmp_path / "relocated"
    shutil.copytree(table, root)
    third = triplet(scan_rows(root, "s_main"))
    assert first == second == third


# ---------------------------------------------------------------------------
# explain plan
# ---------------------------------------------------------------------------


def test_explain_plan_lineage_and_applicability(table):
    sc = Scanner(table)
    plan = sc.explain_plan("s_main", 2, (1, 2))
    assert plan["lineage"] == ["s_main", "s_base"]
    deletes = {d["file_id"]: d for d in plan["delete_files"]}
    assert deletes["EQ6"]["applies_to"] == ["A"]  # strict: only seq 5 < 6
    assert deletes["POS6"]["applies_to"] == ["A"]  # non-strict: seq 5 <= 6
    assert plan["delete_index"]["pos_entries"] == 2  # duplicate entry counted at load
    assert plan["delete_index"]["eq_keys"] == 2


# ---------------------------------------------------------------------------
# partition equality: empty value vs missing field (mini table)
# ---------------------------------------------------------------------------


def _mini_table(root):
    """Two data files: one with partition {"tenant": ""}, one with {}."""
    root = Path(root)
    (root / "files").mkdir(parents=True, exist_ok=True)
    arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int32())])

    def write(name, uids):
        rel = f"files/{name}.parquet"
        pq.write_table(pa.table({"row_uid": uids, "k": list(range(len(uids)))},
                                schema=arrow), root / rel)
        return rel, sha256_of(root / rel)

    r1, s1 = write("D_EMPTY", ["e0", "e1"])
    r2, s2 = write("D_MISSING", ["m0", "m1"])
    r3 = "files/EQ_EMPTY.parquet"
    pq.write_table(pa.table({"k": pa.array([0], pa.int32())}), root / r3)
    s3 = sha256_of(root / r3)
    schemas = [
        {"schema_id": 1, "fields": [
            {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 2, "name": "k", "type": {"kind": "int32"}}]},
        {"schema_id": 91, "fields": [
            {"field_id": 2, "name": "k", "type": {"kind": "int32"}}]},
    ]

    def ent(fid, kind, rel, sha, seq, part, schema_id, **kw):
        e = {"file_id": fid, "kind": kind, "path": rel, "sha256": sha,
             "partition": part, "data_seq": seq, "file_seq": seq,
             "schema_id": schema_id}
        e.update(kw)
        return e

    tj = {
        "format": "offline-lake/1",
        "table_id": "mini",
        "schemas": schemas,
        "snapshots": [{
            "snapshot_id": "s", "parent": None,
            "active_files": [
                ent("D_EMPTY", "DATA", r1, s1, 5, {"tenant": ""}, 1),
                ent("D_MISSING", "DATA", r2, s2, 5, {}, 1),
                ent("EQ_EMPTY", "EQ_DELETE", r3, s3, 6, {"tenant": ""}, 91,
                    eq_field_ids=[2]),
            ],
        }],
    }
    (root / "table.json").write_text(json.dumps(tj))
    return root


def test_partition_empty_value_distinct_from_missing(tmp_path):
    root = _mini_table(tmp_path / "mini")
    rows = scan_rows(root, "s", schema=1, projection=(1,))
    got = [(f, p, fields["row_uid"]) for f, p, fields in rows]
    # EQ_EMPTY (partition {"tenant": ""}, key k=0) deletes only D_EMPTY row 0;
    # the file with no tenant field at all is a different partition
    assert got == [("D_EMPTY", 1, "e1"), ("D_MISSING", 0, "m0"),
                   ("D_MISSING", 1, "m1")]


def test_eq_group_with_widened_key_types(tmp_path):
    """Two EQ files in one group declaring k as int32 / int64: the canonical
    encoding domain is identical, both must apply."""
    root = Path(tmp_path) / "wideq"
    (root / "files").mkdir(parents=True)
    pq.write_table(
        pa.table({"row_uid": ["a", "b"], "k": pa.array([1, 2], pa.int32())}),
        root / "files" / "D1.parquet")
    pq.write_table(pa.table({"k": pa.array([1], pa.int32())}),
                   root / "files" / "EQ_A.parquet")
    pq.write_table(pa.table({"k": pa.array([2], pa.int64())}),
                   root / "files" / "EQ_B.parquet")

    def ent(fid, kind, name, seq, schema_id, **kw):
        e = {"file_id": fid, "kind": kind, "path": f"files/{name}.parquet",
             "sha256": sha256_of(root / "files" / f"{name}.parquet"),
             "partition": {"tenant": "t1"}, "data_seq": seq, "file_seq": seq,
             "schema_id": schema_id}
        e.update(kw)
        return e

    tj = {
        "format": "offline-lake/1",
        "table_id": "wideq",
        "schemas": [
            {"schema_id": 1, "fields": [
                {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
                 "required": True},
                {"field_id": 2, "name": "k", "type": {"kind": "int32"}}]},
            {"schema_id": 91, "fields": [
                {"field_id": 2, "name": "k", "type": {"kind": "int32"}}]},
            {"schema_id": 92, "fields": [
                {"field_id": 2, "name": "k", "type": {"kind": "int64"}}]},
        ],
        "snapshots": [{
            "snapshot_id": "s", "parent": None,
            "active_files": [
                ent("D1", "DATA", "D1", 5, 1),
                ent("EQ_A", "EQ_DELETE", "EQ_A", 6, 91, eq_field_ids=[2]),
                ent("EQ_B", "EQ_DELETE", "EQ_B", 6, 92, eq_field_ids=[2]),
            ],
        }],
    }
    (root / "table.json").write_text(json.dumps(tj))
    rows = scan_rows(root, "s", schema=1, projection=(1,))
    assert rows == []  # row 0 deleted by int32 key, row 1 by int64 key


# ---------------------------------------------------------------------------
# large delete set: on-disk index, bounded memory
# ---------------------------------------------------------------------------


def test_large_delete_set_spills_to_disk_bounded_memory(table, tmp_path):
    root = tmp_path / "large"
    shutil.copytree(table, root)
    n_rows, n_keys = 100_000, 1_000_000

    arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int32()),
                       ("amt", pa.decimal128(10, 2)), ("code", pa.string())])

    pq.write_table(
        pa.table({
            "row_uid": [f"r{i}" for i in range(n_rows)],
            "k": pa.array(list(range(n_rows)), pa.int32()),
            "amt": pa.array([Decimal("1.00")] * n_rows, pa.decimal128(10, 2)),
            "code": ["x"] * n_rows,
        }, schema=arrow),
        root / "files" / "L.parquet",
    )
    # delete keys: every even number in [0, 2*n_keys) -> covers even k of L
    eq_keys = pa.table(
        {"k": pa.array(list(range(0, 2 * n_keys, 2)), pa.int32())},
        pa.schema([("k", pa.int32())]),
    )
    pq.write_table(eq_keys, root / "files" / "BIG_EQ.parquet")

    tj = json.loads((root / "table.json").read_text())
    tj["snapshots"].append({
        "snapshot_id": "s_large", "parent": None,
        "active_files": [
            {"file_id": "L", "kind": "DATA", "path": "files/L.parquet",
             "sha256": sha256_of(root / "files" / "L.parquet"),
             "partition": {"tenant": "t1"}, "data_seq": 5, "file_seq": 200,
             "schema_id": 1},
            {"file_id": "BIG_EQ", "kind": "EQ_DELETE",
             "path": "files/BIG_EQ.parquet",
             "sha256": sha256_of(root / "files" / "BIG_EQ.parquet"),
             "partition": {"tenant": "t1"}, "data_seq": 6, "file_seq": 201,
             "schema_id": 91, "eq_field_ids": [2]},
        ],
    })
    (root / "table.json").write_text(json.dumps(tj))

    work = tmp_path / "work"
    sc = Scanner(root, work_dir=str(work), batch_size=8192)
    tracemalloc.start()
    rows = collect(sc.scan("s_large", 2, (1, 2)))
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # exactly the odd-k rows survive
    assert len(rows) == n_rows // 2
    assert all(fields["k"] % 2 == 1 for _, _, fields in rows)
    assert rows[0][2]["row_uid"] == "r1" and rows[-1][2]["row_uid"] == f"r{n_rows - 1}"

    # keys lived on disk, not in a Python set (a set of 1M keys would be >60MB)
    db = Path(work) / "delete_index.sqlite"
    assert db.exists() and db.stat().st_size > 10 * 1024 * 1024
    assert peak < 40 * 1024 * 1024, f"python peak {peak / 1e6:.1f}MB too high"

    # rescanning the same snapshot after reopening gives identical output
    again = collect(Scanner(root).scan("s_large", 2, (1, 2)))
    assert [(f, p, fs["row_uid"]) for f, p, fs in rows] == [
        (f, p, fs["row_uid"]) for f, p, fs in again
    ]
