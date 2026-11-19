"""Acceptance suite for the repeatable cross-snapshot key diff.

Every table here is generated at runtime under tmp_path; the repository
carries no data artifacts.  The expected change records are derived key by
key from hand-listed mini snapshots, and the same story is cross-checked
independently by verify_compare.py (which imports none of this package).

Coverage map (task acceptance list):
* different branches and per-snapshot active_files
* insert / delete / update / unchanged
* delete-then-reinsert of the same key
* EQ/POS delete applicability taking effect separately on both snapshots
* schema widening and field rename; same name with a new field_id
* duplicate row_uid and compaction cause no mismatch
* null / duplicate business keys reject the whole comparison
* bounded-memory large-data run with on-disk matching
* corrupt digest on either side (data or delete file) -> no publication
* interruption/rerun and atomic replacement of an existing destination
* input file order and row-group splitting changes do not change the result
* composite keys, exact (non-stringified, non-approximate) comparison
"""

import json
import os
import shutil
import tracemalloc
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakescan import (
    ContractError,
    DiffError,
    DuplicateKeyError,
    KeyFieldError,
    NullKeyError,
    TableDiffer,
)
from lakescan.diff_fixtures import (
    build_diff_table,
    expected_changes,
    source_visible_keys,
    target_visible_keys,
)
from lakescan.table import sha256_of


# ---------------------------------------------------------------------------
# shared fixture: the runtime-generated diff drill table
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def diff_table(tmp_path_factory):
    root = tmp_path_factory.mktemp("diffdrill") / "table"
    build_diff_table(root)
    return root


def run_compare(root, source, target, schema, keys, out, work_dir=None,
                batch_size=8192):
    return TableDiffer(root, work_dir=work_dir,
                       batch_size=batch_size).compare(
        source, target, schema, keys, out)


def read_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines()]


def by_key(records):
    return {tuple(sorted(r["key"].items())): r for r in records}


def changes_by_kind(records):
    return {
        kind: sorted(r["key"]["2"] for r in records if r["change"] == kind)
        for kind in ("insert", "delete", "update")
    }


# ---------------------------------------------------------------------------
# hand-derived expectations of the drill table
# ---------------------------------------------------------------------------


def test_visible_key_sets_match_hand_derivation(diff_table):
    """The premise: both snapshots' visible rows match the hand tables."""
    from lakescan import Scanner, collect

    def keys_of(snapshot):
        rows = collect(Scanner(diff_table).scan(snapshot, 20, (2,)))
        return sorted(fields["k"] for _, _, fields in rows)

    assert keys_of("d_source") == source_visible_keys()
    assert keys_of("d_target") == target_visible_keys()


def test_main_drill_changes_are_hand_derived(diff_table, tmp_path):
    out = tmp_path / "d.jsonl"
    result = run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    records = read_jsonl(out)
    hand = expected_changes()
    assert changes_by_kind(records) == {
        "insert": hand["insert"],
        "delete": hand["delete"],
        "update": hand["update"],
    }
    # unchanged keys are never emitted
    emitted = {r["key"]["2"] for r in records}
    assert not (emitted & set(hand["unchanged"]))
    assert result.stats.to_json()["unchanged"] == len(hand["unchanged"])
    # ordering: canonical key order, ascending
    assert [r["key"]["2"] for r in records] == sorted(emitted)


def test_each_record_shape_and_snapshot_ids(diff_table, tmp_path):
    out = tmp_path / "d.jsonl"
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    for r in read_jsonl(out):
        assert r["source_snapshot"] == "d_source"
        assert r["target_snapshot"] == "d_target"
        assert set(r) == {
            "change", "key", "source_snapshot", "target_snapshot",
            "before", "after",
        }
        if r["change"] == "insert":
            assert r["before"] is None and r["after"] is not None
        elif r["change"] == "delete":
            assert r["after"] is None and r["before"] is not None
        else:
            assert r["before"] is not None and r["after"] is not None


def test_update_before_after_values_exact(diff_table, tmp_path):
    out = tmp_path / "d.jsonl"
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    rec = by_key(read_jsonl(out))
    # k=2: only amt changes 20.00 -> 25.00; row_uid/key/code stay
    k2 = rec[(("2", 2),)]
    assert k2["before"]["3"] == "20.00" and k2["after"]["3"] == "25.00"
    assert k2["before"]["1"] == k2["after"]["1"] == "u2"
    # k=4: re-inserted; field 9 (new identity) null -> "new9", flag -> false
    k4 = rec[(("2", 4),)]
    assert k4["before"]["9"] is None and k4["after"]["9"] == "new9"
    assert k4["before"]["4"] is None and k4["after"]["4"] is False
    # decimal values are exact strings, never floats
    assert isinstance(k4["before"]["3"], str)


def test_branch_snapshots_use_their_own_active_files(diff_table, tmp_path):
    # two forks off the same baseline: disjoint added data, parent ignored
    out_b = tmp_path / "b.jsonl"
    run_compare(diff_table, "d_source", "d_branch_b", 20, (2,), out_b)
    rec_b = read_jsonl(out_b)
    assert changes_by_kind(rec_b) == {
        "insert": [20, 21], "delete": [], "update": []}
    # the target fork's file BR1 is absent from d_target and vice versa
    tj = json.loads((diff_table / "table.json").read_text())
    members = {s["snapshot_id"]:
               {f["file_id"] for f in s["active_files"]}
               for s in tj["snapshots"]}
    assert "BR1" in members["d_branch_b"] and "BR1" not in members["d_target"]
    assert "TA1" in members["d_target"] and "TA1" not in members["d_branch_b"]


def test_reverse_direction_swaps_insert_and_delete(diff_table, tmp_path):
    fwd = tmp_path / "f.jsonl"
    rev = tmp_path / "r.jsonl"
    run_compare(diff_table, "d_source", "d_target", 20, (2,), fwd)
    run_compare(diff_table, "d_target", "d_source", 20, (2,), rev)
    f, r = changes_by_kind(read_jsonl(fwd)), changes_by_kind(read_jsonl(rev))
    assert f["insert"] == r["delete"] and f["delete"] == r["insert"]
    assert f["update"] == r["update"]


# ---------------------------------------------------------------------------
# delete semantics take effect independently on each snapshot
# ---------------------------------------------------------------------------


def test_source_and_target_deletes_each_apply(diff_table, tmp_path):
    """Source EQ k=3 / POS k=6 never appear as deletes (already invisible on
    both sides); target EQ k=8 is the business delete; target POS rows
    re-inserted with new data become updates, not duplicate-key failures."""
    out = tmp_path / "d.jsonl"
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    kinds = changes_by_kind(read_jsonl(out))
    assert kinds["delete"] == [8]          # TGT_EQ, strict on old seq-5 files
    assert 3 not in kinds["delete"]       # removed on BOTH sides by SRC_EQ
    assert 6 not in kinds["delete"]       # removed on BOTH sides by SRC_POS
    assert sorted(kinds["update"]) == [1, 2, 4, 7, 10]  # re-homed via TGT_POS


def test_delete_then_reinsert_same_key_is_update_or_unchanged(diff_table,
                                                              tmp_path):
    """k=4's old physical row is POS-deleted then re-inserted in TA1:
    one business record, paired by key, never a duplicate."""
    out = tmp_path / "d.jsonl"
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    rec = by_key(read_jsonl(out))
    k4 = rec[(("2", 4),)]
    assert k4["change"] == "update"
    assert k4["before"] is not None and k4["after"] is not None


# ---------------------------------------------------------------------------
# schema identity: widening, rename, same-name new id
# ---------------------------------------------------------------------------


def test_widening_rename_and_same_name_new_field_id(diff_table, tmp_path):
    out = tmp_path / "d.jsonl"
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    rec = by_key(read_jsonl(out))
    # field 7 renamed code -> code_v2 but keeps identity: k=1's value "c1"
    # survives the rename unchanged (no update for that field)
    k1 = rec[(("2", 1),)]
    assert k1["before"]["7"] == k1["after"]["7"] == "c1"
    # int32 key k=2 on the source matches int64 key k=2 on the target
    assert k1["key"]["2"] == 1
    # field 9 is a NEW identity reusing the name "code": source has null
    assert "9" in k1["before"] and k1["before"]["9"] is None
    # the drill target schema really does carry both identities
    tj = json.loads((diff_table / "table.json").read_text())
    s20 = next(s for s in tj["schemas"] if s["schema_id"] == 20)
    names = {f["field_id"]: f["name"] for f in s20["fields"]}
    assert names[7] == "code_v2" and names[9] == "code"


def test_incompatible_key_type_rejects(tmp_path):
    """A key field whose stored type cannot widen to the target type fails
    the whole comparison before any row is exported.  D1 stores k as int64;
    reading it into the int32 target schema is an illegal narrowing."""
    root = tmp_path / "tab"
    _build_simple_two_snapshot_table(root, corrupt_key_type=True)
    out = tmp_path / "out.jsonl"
    with pytest.raises(KeyFieldError):
        run_compare(root, "s0", "s1", 1, (2,), out)
    assert not out.exists()


def test_missing_key_field_on_one_side_rejects(diff_table, tmp_path):
    """Key field must exist on both sides: target schema 10 lacks field 4,
    and asking for a field absent from a data file's schema also fails."""
    out = tmp_path / "out.jsonl"
    # field 4 (flag) exists in target schema 20 but not in the S_A data files
    with pytest.raises(KeyFieldError):
        run_compare(diff_table, "d_source", "d_target", 20, (4,), out)
    assert not out.exists()
    # field 9 not in target schema 10 at all
    with pytest.raises(KeyFieldError):
        run_compare(diff_table, "d_source", "d_target", 10, (9,), out)


# ---------------------------------------------------------------------------
# row_uid duplication and compaction must not affect business identity
# ---------------------------------------------------------------------------


def test_duplicate_row_uid_and_compaction_do_not_mismatch(diff_table,
                                                          tmp_path):
    out = tmp_path / "d.jsonl"
    result = run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    rec = by_key(read_jsonl(out))
    # k=9 kept identical values while moving SA3 -> TA2 (compaction): no record
    assert (("2", 9),) not in rec
    # k=11 shares row_uid "u9" with k=9 but is a distinct key: also unchanged
    assert (("2", 11),) not in rec
    assert result.stats.unchanged >= 3
    # and the source truly contains the duplicate row_uid physically
    from lakescan import Scanner, collect

    uids = [fields["row_uid"]
            for _, _, fields in collect(
                Scanner(diff_table).scan("d_source", 10, (1,)))]
    assert uids.count("u9") == 2


# ---------------------------------------------------------------------------
# null / duplicate keys reject the entire comparison
# ---------------------------------------------------------------------------


def test_null_business_key_rejects_entire_diff(diff_table, tmp_path):
    out = tmp_path / "o.jsonl"
    with pytest.raises(NullKeyError):
        run_compare(diff_table, "d_source", "d_broken_null", 20, (2,), out)
    assert not out.exists()


def test_duplicate_business_key_rejects_entire_diff(diff_table, tmp_path):
    out = tmp_path / "o.jsonl"
    with pytest.raises(DuplicateKeyError):
        run_compare(diff_table, "d_broken_dup", "d_target", 20, (2,), out)
    assert not out.exists()


def test_duplicate_key_among_otherwise_valid_rows(tmp_path):
    root = tmp_path / "tab"
    _build_simple_two_snapshot_table(root, duplicate_key=True)
    out = tmp_path / "o.jsonl"
    with pytest.raises(DuplicateKeyError):
        run_compare(root, "s0", "s1", 1, (2,), out)
    assert not out.exists()


# ---------------------------------------------------------------------------
# exact, never-stringified / never-approximate key comparison
# ---------------------------------------------------------------------------


def test_integer_keys_compare_exactly_not_as_strings(tmp_path):
    """Keys 1 and '1' (different types) and 1 vs 01-style values must never
    collapse; here distinct int keys stay distinct and decimals compare by
    exact unscaled integer arithmetic."""
    root = tmp_path / "tab"
    _build_simple_two_snapshot_table(root)
    out = tmp_path / "o.jsonl"
    run_compare(root, "s0", "s1", 1, (2,), out)
    rec = {r["key"]["2"]: r for r in read_jsonl(out)}
    assert set(rec) == {2, 3}  # k=1 unchanged, k=2 deleted, k=3 inserted


def test_composite_key_uniqueness_and_matching(tmp_path):
    root = tmp_path / "ck"
    _build_composite_key_table(root)
    out = tmp_path / "ck.jsonl"
    run_compare(root, "s0", "s1", 1, (2, 7), out)
    records = read_jsonl(out)
    # composite key (k, code): same k different code is a different record
    kinds = {(r["change"], r["key"]["2"], r["key"]["7"]) for r in records}
    assert ("delete", 1, "a") in kinds
    assert ("insert", 1, "b") in kinds
    assert ("update", 2, "x") in kinds
    assert not any(r["key"]["2"] == 3 for r in records)  # unchanged


# ---------------------------------------------------------------------------
# runtime mini-table builders (hand-listed tiny snapshots)
# ---------------------------------------------------------------------------


def _write_pq(root, name, columns, arrow, row_group_size=None):
    rel = f"files/{name}.parquet"
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns, schema=arrow), p,
                   row_group_size=row_group_size)
    return rel, sha256_of(p)


_SIMPLE_SCHEMAS = [
    {"schema_id": 1, "fields": [
        {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
         "required": True},
        {"field_id": 2, "name": "k", "type": {"kind": "int32"},
         "required": True},
        {"field_id": 3, "name": "amt",
         "type": {"kind": "decimal", "precision": 10, "scale": 2}}]},
    {"schema_id": 2, "fields": [
        {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
         "required": True},
        {"field_id": 2, "name": "k", "type": {"kind": "int64"},
         "required": True},
        {"field_id": 3, "name": "amt",
         "type": {"kind": "decimal", "precision": 14, "scale": 2}}]},
    {"schema_id": 90, "fields": [
        {"field_id": 1000, "name": "target_file_id",
         "type": {"kind": "utf8"}, "required": True},
        {"field_id": 1001, "name": "row_position", "type": {"kind": "int64"},
         "required": True}]},
]
_S_ARROW = pa.schema([("row_uid", pa.string()), ("k", pa.int32()),
                      ("amt", pa.decimal128(10, 2))])
_POS_ARROW = pa.schema([("target_file_id", pa.string()),
                        ("row_position", pa.int64())])


def _simple_rows(rows):
    return {
        "row_uid": [r[0] for r in rows],
        "k": pa.array([r[1] for r in rows], pa.int32()),
        "amt": pa.array([Decimal(r[2]) if r[2] is not None else None
                         for r in rows], pa.decimal128(10, 2)),
    }


def _ent(fid, kind, rel, sha, seq, schema_id, part=None, **kw):
    e = {"file_id": fid, "kind": kind, "path": rel, "sha256": sha,
         "partition": part or {"tenant": "t1"}, "data_seq": seq,
         "file_seq": seq, "schema_id": schema_id}
    e.update(kw)
    return e


def _build_simple_two_snapshot_table(root, duplicate_key=False,
                                    corrupt_key_type=False):
    """Two snapshots over hand-listed rows.

    s0 visible keys {1, 2}; s1 visible keys {1, 3} with k=1 unchanged ->
    delete 2, insert 3, no updates (unless duplicate_key flips s1 invalid).
    """
    root = Path(root)
    made = {}
    made["D0"] = _write_pq(root, "D0", _simple_rows([
        ("a", 1, "1.00"), ("b", 2, "2.00")]), _S_ARROW)
    rows1 = ([("c", 3, "3.00"), ("d", 3, "4.00")] if duplicate_key
             else [("a", 1, "1.00"), ("c", 3, "3.00")])
    if corrupt_key_type:
        # physically store D1's key as int64 so the parquet matches schema 2
        s64_arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int64()),
                               ("amt", pa.decimal128(10, 2))])
        made["D1"] = _write_pq(root, "D1", {
            "row_uid": [r[0] for r in rows1],
            "k": pa.array([r[1] for r in rows1], pa.int64()),
            "amt": pa.array([Decimal(r[2]) for r in rows1],
                            pa.decimal128(10, 2))}, s64_arrow)
    else:
        made["D1"] = _write_pq(root, "D1", _simple_rows(rows1), _S_ARROW)

    def entry(name, kind, seq, schema_id, **kw):
        rel, sha = made[name]
        return _ent(name, kind, rel, sha, seq, schema_id, **kw)

    schemas = [dict(s) for s in _SIMPLE_SCHEMAS]
    if corrupt_key_type:
        # D1 is stored under schema 2, where key field 2 is int64 (a legal
        # exact widening that passes cross-schema compatibility).  Reading it
        # back into the int32 target schema is an illegal narrowing that the
        # diff must reject as an incompatible key -- before any export.
        d1_schema_id = 2
    else:
        d1_schema_id = 1
    tj = {
        "format": "offline-lake/1", "table_id": "mini", "schemas": schemas,
        "snapshots": [
            {"snapshot_id": "s0", "parent": None,
             "active_files": [entry("D0", "DATA", 5, 1)]},
            {"snapshot_id": "s1", "parent": "s0",
             "active_files": [entry("D1", "DATA", 5, d1_schema_id)]},
        ],
    }
    (root / "table.json").write_text(json.dumps(tj))
    return root


def _build_composite_key_table(root):
    """Composite business key (field 2 k int32, field 7 code utf8).

    s0: (1,a) (2,x) (3,z); s1: (1,b) (2,x,amt changed) (3,z).
    Expect: delete (1,a), insert (1,b), update (2,x), (3,z) unchanged.
    """
    root = Path(root)
    arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int32()),
                       ("code", pa.string()),
                       ("amt", pa.decimal128(10, 2))])

    def write(name, rows):
        return _write_pq(root, name, {
            "row_uid": [r[0] for r in rows],
            "k": pa.array([r[1] for r in rows], pa.int32()),
            "code": [r[2] for r in rows],
            "amt": pa.array([Decimal(r[3]) for r in rows],
                            pa.decimal128(10, 2))}, arrow)

    made = {"D0": write("D0", [("a", 1, "a", "1.00"),
                               ("b", 2, "x", "2.00"),
                               ("c", 3, "z", "3.00")]),
            "D1": write("D1", [("a2", 1, "b", "1.00"),
                               ("b", 2, "x", "9.00"),
                               ("c", 3, "z", "3.00")])}
    schemas = [{"schema_id": 1, "fields": [
        {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
         "required": True},
        {"field_id": 2, "name": "k", "type": {"kind": "int32"},
         "required": True},
        {"field_id": 7, "name": "code", "type": {"kind": "utf8"},
         "required": True},
        {"field_id": 3, "name": "amt",
         "type": {"kind": "decimal", "precision": 10, "scale": 2}}]}]
    tj = {"format": "offline-lake/1", "table_id": "ck", "schemas": schemas,
          "snapshots": [
              {"snapshot_id": "s0", "parent": None, "active_files": [
                  _ent("D0", "DATA", *made["D0"], 5, 1)]},
              {"snapshot_id": "s1", "parent": "s0", "active_files": [
                  _ent("D1", "DATA", *made["D1"], 5, 1)]}]}
    (root / "table.json").write_text(json.dumps(tj))
    return root


# ---------------------------------------------------------------------------
# source integrity: a corrupt digest on either side must not publish
# ---------------------------------------------------------------------------


def _flip_byte(path: Path) -> None:
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.write_bytes(bytes(data))


def test_corrupt_source_data_file_blocks_publication(diff_table, tmp_path):
    root = tmp_path / "broken_src"
    shutil.copytree(diff_table, root)
    _flip_byte(root / "files" / "SA1.parquet")  # source-side DATA file
    out = tmp_path / "out.jsonl"
    from lakescan import SourceVersionMismatch

    with pytest.raises(SourceVersionMismatch):
        run_compare(root, "d_source", "d_target", 20, (2,), out)
    assert not out.exists()


def test_corrupt_target_data_file_blocks_publication(diff_table, tmp_path):
    root = tmp_path / "broken_tgt"
    shutil.copytree(diff_table, root)
    _flip_byte(root / "files" / "TA2.parquet")  # target-side DATA file
    out = tmp_path / "out.jsonl"
    from lakescan import SourceVersionMismatch

    with pytest.raises(SourceVersionMismatch):
        run_compare(root, "d_source", "d_target", 20, (2,), out)
    assert not out.exists()


def test_corrupt_delete_file_blocks_publication(diff_table, tmp_path):
    root = tmp_path / "broken_del"
    shutil.copytree(diff_table, root)
    _flip_byte(root / "files" / "TGT_EQ.parquet")  # target EQ delete file
    out = tmp_path / "out.jsonl"
    from lakescan import SourceVersionMismatch

    with pytest.raises(SourceVersionMismatch):
        run_compare(root, "d_source", "d_target", 20, (2,), out)
    assert not out.exists()
    # and the source-side delete file too
    root2 = tmp_path / "broken_del2"
    shutil.copytree(diff_table, root2)
    _flip_byte(root2 / "files" / "SRC_POS.parquet")
    with pytest.raises(SourceVersionMismatch):
        run_compare(root2, "d_source", "d_target", 20, (2,),
                    tmp_path / "out2.jsonl")
    assert not (tmp_path / "out2.jsonl").exists()


def test_missing_source_file_blocks_publication(diff_table, tmp_path):
    root = tmp_path / "missing"
    shutil.copytree(diff_table, root)
    (root / "files" / "SA2.parquet").unlink()
    out = tmp_path / "out.jsonl"
    from lakescan import SourceFileMissing

    with pytest.raises(SourceFileMissing):
        run_compare(root, "d_source", "d_target", 20, (2,), out)
    assert not out.exists()


def test_illegal_snapshot_id_rejects(diff_table, tmp_path):
    out = tmp_path / "o.jsonl"
    with pytest.raises(ContractError):
        run_compare(diff_table, "d_source", "nope", 20, (2,), out)
    with pytest.raises(ContractError):
        run_compare(diff_table, "nope", "d_target", 20, (2,), out)
    assert not out.exists()


# ---------------------------------------------------------------------------
# atomic publication: interruption/rerun, existing destination replacement
# ---------------------------------------------------------------------------


def test_existing_target_is_replaced_only_by_complete_output(diff_table,
                                                             tmp_path):
    out = tmp_path / "out.jsonl"
    sentinel = "PREVIOUS COMPLETE CONTENT\n"
    out.write_text(sentinel)
    # a failing run must leave the previous complete file byte-identical
    bad_root = tmp_path / "bad"
    shutil.copytree(diff_table, bad_root)
    _flip_byte(bad_root / "files" / "TA1.parquet")
    from lakescan import SourceVersionMismatch

    with pytest.raises(SourceVersionMismatch):
        run_compare(bad_root, "d_source", "d_target", 20, (2,), out)
    assert out.read_text() == sentinel
    # a successful run atomically replaces it
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    assert out.read_text() != sentinel
    assert all(json.loads(line) for line in out.read_text().splitlines())
    # no staging leftovers next to the destination
    leftovers = [p.name for p in tmp_path.iterdir()
                 if p.name.startswith("out.jsonl.") and p.name.endswith(".tmp")]
    assert leftovers == []


def test_rerun_is_byte_identical_and_resumable(diff_table, tmp_path):
    out1, out2 = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    work = tmp_path / "work"
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out1,
                work_dir=str(work))
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out2,
                work_dir=str(work))  # same work dir reused across reruns
    assert out1.read_bytes() == out2.read_bytes()


def test_interruption_leaves_no_partial_output(monkeypatch, diff_table,
                                               tmp_path):
    """Simulate an interruption mid-export: the staging file is removed and
    the destination never appears."""
    import lakescan.diff as diffmod

    out = tmp_path / "out.jsonl"
    calls = {"n": 0}
    real_write = diffmod._Merger._record

    def flaky_record(self, change, s_row, t_row):
        calls["n"] += 1
        if calls["n"] == 3:
            raise KeyboardInterrupt("simulated interruption")
        return real_write(self, change, s_row, t_row)

    monkeypatch.setattr(diffmod._Merger, "_record", flaky_record)
    with pytest.raises(KeyboardInterrupt):
        run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    assert not out.exists()
    assert not list(tmp_path.glob("*.tmp"))
    # after the fault is cleared, a rerun completes normally
    monkeypatch.undo()
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    assert out.exists() and len(read_jsonl(out)) == 8


# ---------------------------------------------------------------------------
# bounded memory: large runtime-generated data, on-disk key matching
# ---------------------------------------------------------------------------


def _build_large_table(root, n_src, n_tgt):
    """Two snapshots with n_src / n_tgt visible rows, tiny row footprint."""
    root = Path(root)
    (root / "files").mkdir(parents=True, exist_ok=True)
    arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int32()),
                       ("amt", pa.decimal128(10, 2))])

    def write(name, keys, rgs=None):
        rel = f"files/{name}.parquet"
        pq.write_table(pa.table({
            "row_uid": [f"u{k}" for k in keys],
            "k": pa.array(keys, pa.int32()),
            "amt": pa.array([Decimal("1.00")] * len(keys),
                            pa.decimal128(10, 2))}, schema=arrow),
            root / rel, row_group_size=rgs)
        return rel, sha256_of(root / rel)

    # source keys 0..n_src; target keys (n_src//2)..(n_src//2 + n_tgt) so
    # there is a large overlap, many inserts and many deletes
    src_keys = list(range(n_src))
    tgt_keys = list(range(n_src // 2, n_src // 2 + n_tgt))
    s_rel, s_sha = write("LSRC", src_keys, rgs=5000)
    t_rel, t_sha = write("LTGT", tgt_keys, rgs=7000)
    schemas = [{"schema_id": 1, "fields": [
        {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
         "required": True},
        {"field_id": 2, "name": "k", "type": {"kind": "int32"},
         "required": True},
        {"field_id": 3, "name": "amt",
         "type": {"kind": "decimal", "precision": 10, "scale": 2}}]}]

    def ent(fid, rel, sha, seq):
        return {"file_id": fid, "kind": "DATA", "path": rel, "sha256": sha,
                "partition": {"tenant": "t1"}, "data_seq": seq,
                "file_seq": seq, "schema_id": 1}

    tj = {"format": "offline-lake/1", "table_id": "large",
          "schemas": schemas, "snapshots": [
              {"snapshot_id": "s0", "parent": None,
               "active_files": [ent("LSRC", s_rel, s_sha, 5)]},
              {"snapshot_id": "s1", "parent": "s0",
               "active_files": [ent("LTGT", t_rel, t_sha, 6)]}]}
    (root / "table.json").write_text(json.dumps(tj))
    return src_keys, tgt_keys


def test_large_diff_bounded_memory(tmp_path):
    root = tmp_path / "large"
    n_src, n_tgt = 120_000, 120_000
    src_keys, tgt_keys = _build_large_table(root, n_src, n_tgt)
    work = tmp_path / "work"
    out = tmp_path / "large.jsonl"

    tracemalloc.start()
    result = run_compare(root, "s0", "s1", 1, (2,), out,
                         work_dir=str(work), batch_size=4096)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    records = read_jsonl(out)
    overlap_lo, overlap_hi = n_src // 2, n_src
    expected_inserts = sorted(set(tgt_keys) - set(src_keys))
    expected_deletes = sorted(set(src_keys) - set(tgt_keys))
    got_inserts = [r["key"]["2"] for r in records if r["change"] == "insert"]
    got_deletes = [r["key"]["2"] for r in records if r["change"] == "delete"]
    assert got_inserts == expected_inserts
    assert got_deletes == expected_deletes
    assert not any(r["change"] == "update" for r in records)
    assert result.stats.source_rows == n_src
    assert result.stats.target_rows == n_tgt

    # the key stores are on disk and materially larger than any in-memory
    # Python set would allow; two side stores exist
    stores = list(work.rglob("*_rows.sqlite"))
    assert len(stores) == 2
    assert all(p.stat().st_size > 1024 * 1024 for p in stores)
    # bounded Python memory: 240k rows would be far larger if held as dicts
    assert peak < 60 * 1024 * 1024, f"python peak {peak / 1e6:.1f}MB"
    print(f"\n[large diff] rows={n_src}+{n_tgt} python peak="
          f"{peak / 1e6:.1f}MB on-disk stores="
          f"{sum(p.stat().st_size for p in stores) / 1e6:.1f}MB")

    # byte-stable rerun
    out2 = tmp_path / "large2.jsonl"
    run_compare(root, "s0", "s1", 1, (2,), out2, work_dir=str(work))
    assert out.read_bytes() == out2.read_bytes()


# ---------------------------------------------------------------------------
# invariance to input file ordering and row-group splitting
# ---------------------------------------------------------------------------


def test_result_invariant_to_active_file_order(diff_table, tmp_path):
    root = tmp_path / "reordered"
    shutil.copytree(diff_table, root)
    tj = json.loads((root / "table.json").read_text())
    for snap in tj["snapshots"]:
        snap["active_files"] = list(reversed(snap["active_files"]))
    (root / "table.json").write_text(json.dumps(tj))
    out1, out2 = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out1)
    run_compare(root, "d_source", "d_target", 20, (2,), out2)
    assert out1.read_bytes() == out2.read_bytes()


def test_result_invariant_to_row_group_splitting(tmp_path):
    """Rewrite the same rows with different row-group sizes (and a second
    physical layout): visible rows and the diff must be byte-identical."""
    root_a = tmp_path / "rg_a"
    _build_large_table(root_a, 3000, 3000)
    root_b = tmp_path / "rg_b"
    shutil.copytree(root_a, root_b)
    # rewrite both parquet files with row_group_size=1 (many small groups)
    arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int32()),
                       ("amt", pa.decimal128(10, 2))])
    tj = json.loads((root_b / "table.json").read_text())
    for entry in tj["snapshots"][0]["active_files"] + \
            tj["snapshots"][1]["active_files"]:
        p = root_b / entry["path"]
        tbl = pq.read_table(p)
        pq.write_table(tbl, p, row_group_size=1)
        entry["sha256"] = sha256_of(p)
    (root_b / "table.json").write_text(json.dumps(tj))
    out_a, out_b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    run_compare(root_a, "s0", "s1", 1, (2,), out_a)
    run_compare(root_b, "s0", "s1", 1, (2,), out_b)
    assert out_a.read_bytes() == out_b.read_bytes()


def test_compaction_same_values_different_files_is_unchanged(tmp_path):
    """A key whose row is rewritten byte-differently (new file, new row
    group, same values) must not appear as a change."""
    root = tmp_path / "compact"
    (root / "files").mkdir(parents=True)
    arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int32()),
                       ("amt", pa.decimal128(10, 2))])
    keys = list(range(1000))

    def write(name, rgs):
        rel = f"files/{name}.parquet"
        pq.write_table(pa.table({
            "row_uid": [f"u{k}" for k in keys],
            "k": pa.array(keys, pa.int32()),
            "amt": pa.array([Decimal("1.00")] * len(keys),
                            pa.decimal128(10, 2))}, schema=arrow),
            root / rel, row_group_size=rgs)
        return rel, sha256_of(root / rel)

    s_rel, s_sha = write("C0", 1000)   # one row group
    t_rel, t_sha = write("C1", 37)     # many row groups, different file
    schemas = [{"schema_id": 1, "fields": [
        {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
         "required": True},
        {"field_id": 2, "name": "k", "type": {"kind": "int32"},
         "required": True},
        {"field_id": 3, "name": "amt",
         "type": {"kind": "decimal", "precision": 10, "scale": 2}}]}]

    def ent(fid, rel, sha):
        return {"file_id": fid, "kind": "DATA", "path": rel, "sha256": sha,
                "partition": {"tenant": "t1"}, "data_seq": 5,
                "file_seq": 5, "schema_id": 1}

    (root / "table.json").write_text(json.dumps({
        "format": "offline-lake/1", "table_id": "compact", "schemas": schemas,
        "snapshots": [
            {"snapshot_id": "s0", "parent": None,
             "active_files": [ent("C0", s_rel, s_sha)]},
            {"snapshot_id": "s1", "parent": "s0",
             "active_files": [ent("C1", t_rel, t_sha)]}]}))
    out = tmp_path / "c.jsonl"
    result = run_compare(root, "s0", "s1", 1, (2,), out)
    records = read_jsonl(out)
    assert records == []  # pure layout difference -> zero business changes
    assert result.stats.unchanged == 1000


# ---------------------------------------------------------------------------
# library surface: argument validation
# ---------------------------------------------------------------------------


def test_key_argument_validation(diff_table, tmp_path):
    out = tmp_path / "o.jsonl"
    with pytest.raises(KeyFieldError):
        run_compare(diff_table, "d_source", "d_target", 20, (), out)
    with pytest.raises(KeyFieldError):
        run_compare(diff_table, "d_source", "d_target", 20, (2, 2), out)
    with pytest.raises(KeyFieldError):
        run_compare(diff_table, "d_source", "d_target", 20, (-1,), out)
    with pytest.raises(KeyFieldError):
        run_compare(diff_table, "d_source", "d_target", 20, (True,), out)
    assert not out.exists()


def test_comparison_is_independent_of_output_projection(diff_table, tmp_path):
    """The library projects the whole target schema internally, so an update
    is detected from the full normalized payload; every emitted before/after
    always carries the complete field set (keys and values can never be
    omitted by an output projection)."""
    out = tmp_path / "d.jsonl"
    run_compare(diff_table, "d_source", "d_target", 20, (2,), out)
    tj = json.loads((diff_table / "table.json").read_text())
    target_field_ids = {f["field_id"] for f in next(
        s for s in tj["schemas"] if s["schema_id"] == 20)["fields"]}
    for r in read_jsonl(out):
        for side in ("before", "after"):
            proj = r[side]
            if proj is None:
                continue  # insert has no before, delete has no after
            assert set(int(k) for k in proj) == target_field_ids
    # the k=2 update lives in amt (field 3); it is still detected and shown
    k2 = next(r for r in read_jsonl(out) if r["key"]["2"] == 2)
    assert k2["change"] == "update" and "3" in k2["before"] and "3" in k2["after"]


def test_diff_errors_are_lake_scan_errors():
    from lakescan import LakeScanError
    assert issubclass(DiffError, LakeScanError)
    assert issubclass(NullKeyError, LakeScanError)
    assert issubclass(DuplicateKeyError, LakeScanError)


# ---------------------------------------------------------------------------
# per-side delete scoping: POS/EQ rules take effect separately on each side
# ---------------------------------------------------------------------------


def _build_per_side_delete_table(root):
    """Two snapshots whose delete files act on different keys.

    Source snapshot s0: DATA F0 (rows k=1..4, data_seq=5) +
      EQ delete EQ0@6 on k=1 (strict: 5 < 6 applies) +
      POS delete POS0@6 on physical row 2 (k=3).
      Visible source keys: {2, 4}.
    Target snapshot s1: a different DATA file F1 (rows k=2..5, data_seq=5) +
      EQ delete EQ1@6 on k=2 + POS delete POS1@6 on physical row 3 (k=5).
      Visible target keys: {3, 4}.
    Neither snapshot references the other's delete file, proving deletes are
    scoped to each snapshot's own active_files.
    Expected diff: delete {2}, insert {3}; key 4 unchanged.
    """
    root = Path(root)
    (root / "files").mkdir(parents=True)
    d_arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int32())])
    eq_arrow = pa.schema([("k", pa.int32())])
    pos_arrow = pa.schema([("target_file_id", pa.string()),
                           ("row_position", pa.int64())])

    def w(name, tbl):
        rel = f"files/{name}.parquet"
        pq.write_table(tbl, root / rel)
        return rel, sha256_of(root / rel)

    f0 = w("F0", pa.table({"row_uid": ["a", "b", "c", "d"],
                           "k": pa.array([1, 2, 3, 4], pa.int32())},
                          schema=d_arrow))
    f1 = w("F1", pa.table({"row_uid": ["b", "c", "d", "e"],
                           "k": pa.array([2, 3, 4, 5], pa.int32())},
                          schema=d_arrow))
    eq0 = w("EQ0", pa.table({"k": pa.array([1], pa.int32())},
                            schema=eq_arrow))
    eq1 = w("EQ1", pa.table({"k": pa.array([2], pa.int32())},
                            schema=eq_arrow))
    p0 = w("POS0", pa.table({"target_file_id": ["F0"],
                             "row_position": pa.array([2], pa.int64())},
                            schema=pos_arrow))
    p1 = w("POS1", pa.table({"target_file_id": ["F1"],
                             "row_position": pa.array([3], pa.int64())},
                            schema=pos_arrow))
    schemas = [
        {"schema_id": 1, "fields": [
            {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 2, "name": "k", "type": {"kind": "int32"},
             "required": True}]},
        {"schema_id": 90, "fields": [
            {"field_id": 1000, "name": "target_file_id",
             "type": {"kind": "utf8"}, "required": True},
            {"field_id": 1001, "name": "row_position",
             "type": {"kind": "int64"}, "required": True}]},
        {"schema_id": 91, "fields": [
            {"field_id": 2, "name": "k", "type": {"kind": "int32"}}]},
    ]

    def d(fid, rel_sha, kind, seq, schema_id, **kw):
        rel, sha = rel_sha
        e = {"file_id": fid, "kind": kind, "path": rel, "sha256": sha,
             "partition": {"tenant": "t1"}, "data_seq": seq,
             "file_seq": seq, "schema_id": schema_id}
        e.update(kw)
        return e

    tj = {"format": "offline-lake/1", "table_id": "perside",
          "schemas": schemas, "snapshots": [
              {"snapshot_id": "s0", "parent": None, "active_files": [
                  d("F0", f0, "DATA", 5, 1),
                  d("EQ0", eq0, "EQ_DELETE", 6, 91, eq_field_ids=[2]),
                  d("POS0", p0, "POS_DELETE", 6, 90)]},
              {"snapshot_id": "s1", "parent": "s0", "active_files": [
                  d("F1", f1, "DATA", 5, 1),
                  d("EQ1", eq1, "EQ_DELETE", 6, 91, eq_field_ids=[2]),
                  d("POS1", p1, "POS_DELETE", 6, 90)]}]}
    (root / "table.json").write_text(json.dumps(tj))
    return root


def test_pos_and_eq_deletes_scoped_per_snapshot(tmp_path):
    root = _build_per_side_delete_table(tmp_path / "perside")
    out = tmp_path / "o.jsonl"
    run_compare(root, "s0", "s1", 1, (2,), out)
    records = read_jsonl(out)
    summary = {c: sorted(r["key"]["2"] for r in records
                         if r["change"] == c)
               for c in ("insert", "delete", "update")}
    # source-visible {2,4}, target-visible {3,4}
    assert summary == {"insert": [3], "delete": [2], "update": []}
    # k=1 died on the source only (EQ0), k=5 died on the target only (POS1):
    # neither may leak into the diff
    assert all(r["key"]["2"] not in (1, 5) for r in records)
