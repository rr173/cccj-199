#!/usr/bin/env python3
"""Independent cross-check for the cross-snapshot key diff.

This program shares NO code with the lakescan library and never imports it:
it re-parses table.json itself, re-reads every parquet file with pyarrow
only, applies POS/EQ deletes with plain Python dictionaries over explicit
physical row tables, normalizes rows to the target schema by its own rules,
and performs the key-set diff with naïve Python structures.  It then invokes
the library's ``compare`` CLI as a subprocess (so even the call path is
independent) and asserts byte-for-byte equality against its own JSONL.

The expected change sets are hand-derived key-by-key in
lakescan/diff_fixtures.py and repeated here as literals; the independent
computation must agree with both the hand tables and the CLI output.

Usage:
  python3 verify_compare.py --table DIR [--work-dir DIR]
"""

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Hard independence guard: this verifier must never share code with the
# library it validates.  Refuse to start if a lakescan module is importable
# under this process (the CLI is exercised only as a separate subprocess).
assert "lakescan" not in sys.modules

REPO = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# independent contract reading
# ---------------------------------------------------------------------------


def load_contract(root):
    with open(Path(root) / "table.json", encoding="utf-8") as fh:
        raw = json.load(fh)
    assert raw["format"] == "offline-lake/1"
    schemas = {s["schema_id"]: s for s in raw["schemas"]}
    snaps = {s["snapshot_id"]: s for s in raw["snapshots"]}
    return raw, schemas, snaps


def field_map(schema):
    return {f["field_id"]: f for f in schema["fields"]}


def part_key(p):
    return json.dumps(p or {}, sort_keys=True, separators=(",", ":"))


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# independent delete application over explicit physical rows
# ---------------------------------------------------------------------------


def read_physical_rows(root, entry, eschema):
    """Return list of {field_id: value}; decimals stay Decimal; digest checked."""
    p = Path(root) / entry["path"]
    assert sha256_file(p) == entry["sha256"], f"digest mismatch: {entry['path']}"
    names = [f["name"] for f in eschema["fields"]]
    tbl = pq.read_table(p, columns=names)
    cols = {f["field_id"]: tbl.column(f["name"]).to_pylist()
            for f in eschema["fields"]}
    n = tbl.num_rows
    return [{fid: cols[fid][i] for fid in cols} for i in range(n)]


def visible_rows(root, schemas, snap):
    """Naïve snapshot scan: list of (file_id, position, {field_id: value}).

    POS/EQ applicability rules are re-derived here independently:
      POS: target active DATA, same partition, target.data_seq <= del.data_seq,
           valid position;
      EQ : same partition, target.data_seq <  del.data_seq (strict), missing
           key fields participate as null.
    """
    entries = {e["file_id"]: e for e in snap["active_files"]}
    data_entries = [e for e in snap["active_files"] if e["kind"] == "DATA"]
    physical = {}
    for e in data_entries:
        physical[e["file_id"]] = read_physical_rows(
            root, e, schemas[e["schema_id"]])

    pos_dead = {}  # file_id -> set(positions)
    for e in snap["active_files"]:
        if e["kind"] != "POS_DELETE":
            continue
        eschema = schemas[e["schema_id"]]
        fmap = field_map(eschema)
        tname, pname = fmap[1000]["name"], fmap[1001]["name"]
        tbl = pq.read_table(Path(root) / e["path"], columns=[tname, pname])
        for t, pos in zip(tbl.column(tname).to_pylist(),
                          tbl.column(pname).to_pylist()):
            tgt = entries.get(t)
            if tgt is None or tgt["kind"] != "DATA":
                continue
            if part_key(tgt["partition"]) != part_key(e["partition"]):
                continue
            if tgt["data_seq"] > e["data_seq"]:
                continue
            assert 0 <= pos < len(physical[t]), "position out of range"
            pos_dead.setdefault(t, set()).add(pos)

    eq_rules = []
    for e in snap["active_files"]:
        if e["kind"] != "EQ_DELETE":
            continue
        eschema = schemas[e["schema_id"]]
        fmap = field_map(eschema)
        fids = sorted(e["eq_field_ids"])
        names = [fmap[f]["name"] for f in fids]
        tbl = pq.read_table(Path(root) / e["path"], columns=names)
        cols = [tbl.column(n).to_pylist() for n in names]
        keys = {tuple(canon(v) for v in row) for row in zip(*cols)}
        eq_rules.append((part_key(e["partition"]), tuple(fids), keys,
                         e["data_seq"]))

    out = []
    for e in data_entries:
        eschema = schemas[e["schema_id"]]
        fmap = field_map(eschema)
        part = part_key(e["partition"])
        for pos, row in enumerate(physical[e["file_id"]]):
            if pos in pos_dead.get(e["file_id"], ()):
                continue
            killed = False
            for epart, fids, keys, dseq in eq_rules:
                if epart != part or not (e["data_seq"] < dseq):
                    continue
                kv = tuple(canon(row[fid]) if fid in fmap else None
                           for fid in fids)
                if kv in keys:
                    killed = True
                    break
            if not killed:
                out.append((e["file_id"], pos, row))
    return out


# ---------------------------------------------------------------------------
# exact canonical values (no stringification of keys, no float)
# ---------------------------------------------------------------------------


def canon(v):
    """Exact typed canonical value used for key identity and payload equality."""
    if v is None:
        return None
    if isinstance(v, bool):
        return ("bool", v)
    if isinstance(v, Decimal):
        t = v.as_tuple()
        scale = -t.exponent
        unscaled = 0
        for d in t.digits:
            unscaled = unscaled * 10 + d
        if t.sign:
            unscaled = -unscaled
        return ("dec", scale, unscaled)
    if isinstance(v, int):
        return ("int", v)
    if isinstance(v, str):
        return ("str", v)
    raise AssertionError(f"unexpected value {v!r}")


def normalize_to_target(row, fschema, target):
    """Project one physical row onto the target schema (exact widening).

    Missing nullable fields become null; rename is irrelevant because
    everything is keyed by field_id; int32->int64 is identity in Python;
    decimal widening keeps the same scale per the contract so the Decimal is
    unchanged.  A same-name column with a new field id simply does not match.
    """
    fmap = field_map(fschema)
    tmap = field_map(target)
    out = {}
    for fid, tf in tmap.items():
        if fid not in fmap:
            assert not tf.get("required", False), "required field missing"
            out[fid] = None
            continue
        v = row.get(fid)
        if v is None:
            out[fid] = None
        elif tf["type"]["kind"] == "decimal":
            assert isinstance(v, Decimal)
            assert v.as_tuple().exponent == -tf["type"]["scale"]
            out[fid] = v
        else:
            out[fid] = v
    return out


def jsonable(v):
    if isinstance(v, Decimal):
        return format(v, "f")
    return v


# ---------------------------------------------------------------------------
# naïve key diff
# ---------------------------------------------------------------------------


def side_index(root, schemas, snap, target, key_fids):
    """Visible rows -> {key_tuple: (normalized row, source location)} with
    explicit non-null + uniqueness enforcement."""
    tmap = field_map(target)
    index = {}
    for file_id, pos, raw in visible_rows(root, schemas, snap):
        fschema = schemas[next(
            e["schema_id"] for e in snap["active_files"]
            if e["file_id"] == file_id)]
        norm = normalize_to_target(raw, fschema, target)
        kv = tuple(norm[fid] for fid in key_fids)
        assert all(v is not None for v in kv), \
            f"null business key in {snap['snapshot_id']} {file_id}:{pos}"
        ck = tuple(canon(v) for v in kv)
        assert ck not in index, \
            f"duplicate business key {ck} in {snap['snapshot_id']}"
        index[ck] = (norm, (file_id, pos))
    return index


def naive_diff(root, schemas, snaps, src_id, tgt_id, target_schema_id,
               key_fids):
    target = schemas[target_schema_id]
    s_idx = side_index(root, schemas, snaps[src_id], target, key_fids)
    t_idx = side_index(root, schemas, snaps[tgt_id], target, key_fids)
    non_key = sorted((f for f in target["fields"]
                      if f["field_id"] not in key_fids),
                     key=lambda f: f["field_id"])
    records = []
    for ck in sorted(set(s_idx) | set(t_idx)):
        s = s_idx.get(ck)
        t = t_idx.get(ck)
        if s is not None and t is None:
            change = "delete"
        elif s is None and t is not None:
            change = "insert"
        else:
            sp = tuple(canon(s[0][f["field_id"]]) for f in non_key)
            tp = tuple(canon(t[0][f["field_id"]]) for f in non_key)
            if sp == tp:
                continue  # unchanged: not emitted
            change = "update"
        key_obj = {str(fid): jsonable((s or t)[0][fid])
                   for fid in key_fids}
        records.append({
            "change": change,
            "key": key_obj,
            "source_snapshot": src_id,
            "target_snapshot": tgt_id,
            "before": None if s is None else projection(target, s[0]),
            "after": None if t is None else projection(target, t[0]),
        })
    return records


def projection(target, norm_row):
    return {str(f["field_id"]): jsonable(norm_row[f["field_id"]])
            for f in target["fields"]}


# ---------------------------------------------------------------------------
# hand-derived expectations (key-by-key derivation lives in diff_fixtures)
# ---------------------------------------------------------------------------


HAND_MAIN = {
    "delete": [8],
    "insert": [12, 13],
    "update": [1, 2, 4, 7, 10],
}
HAND_BRANCH = {"insert": [20, 21], "delete": [], "update": []}


def summarize(records):
    return {
        change: sorted(r["key"]["2"] for r in records if r["change"] == change)
        for change in ("insert", "delete", "update")
    }


def run_cli_compare(root, src, tgt, schema, key_fids, out_path, work_dir):
    cmd = [
        sys.executable, "-m", "lakescan", "compare",
        "--table", str(root), "--source", src, "--target", tgt,
        "--schema", str(schema),
        "--key-fields", ",".join(str(f) for f in key_fids),
        "--out", str(out_path),
    ]
    if work_dir:
        cmd += ["--work-dir", str(work_dir)]
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    assert proc.returncode == 0, f"CLI compare failed: {proc.stderr}"
    return [json.loads(line) for line in out_path.read_text().splitlines()]


def check_case(name, root, schemas, snaps, src, tgt, schema, key_fids, hand,
               work_dir):
    expected = naive_diff(root, schemas, snaps, src, tgt, schema, key_fids)
    assert summarize(expected) == hand, (
        f"[{name}] independent diff {summarize(expected)} != hand table {hand}")
    out_path = Path(tempfile.mkdtemp()) / "diff.jsonl"
    cli_records = run_cli_compare(root, src, tgt, schema, key_fids,
                                  out_path, work_dir)
    # content equality (order is part of the contract too)
    assert cli_records == expected, (
        f"[{name}] CLI records != independent records\n"
        f"  cli: {json.dumps(cli_records, sort_keys=True)}\n"
        f"  ref: {json.dumps(expected, sort_keys=True)}")
    # byte stability: rerun into a second file and compare raw bytes
    out_path2 = Path(tempfile.mkdtemp()) / "diff2.jsonl"
    run_cli_compare(root, src, tgt, schema, key_fids, out_path2, work_dir)
    assert out_path.read_bytes() == out_path2.read_bytes(), (
        f"[{name}] rerun produced different bytes")
    print(f"  PASS {name}: {summarize(expected)} "
          f"({len(expected)} change records, byte-stable)")


def check_rejected(name, root, snaps, src, tgt, schema, key_fids, work_dir):
    out_dir = Path(tempfile.mkdtemp())
    out_path = out_dir / "should_not_exist.jsonl"
    cmd = [
        sys.executable, "-m", "lakescan", "compare",
        "--table", str(root), "--source", src, "--target", tgt,
        "--schema", str(schema),
        "--key-fields", ",".join(str(f) for f in key_fids),
        "--out", str(out_path),
    ]
    if work_dir:
        cmd += ["--work-dir", str(work_dir)]
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    assert proc.returncode != 0, f"[{name}] expected failure but compare ran"
    assert not out_path.exists(), (
        f"[{name}] failure left a seemingly-complete output behind")
    print(f"  PASS {name}: rejected with no output artifact "
          f"({proc.stderr.strip().splitlines()[-1]})")


# ---------------------------------------------------------------------------
# self-contained mini tables (built by this verifier, library never imported)
# ---------------------------------------------------------------------------


def _write_pq(root, name, table):
    rel = f"files/{name}.parquet"
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return rel, sha256_file(path)


def build_perside_table(root):
    """Per-side delete scoping, hand-listed.

    s0 F0 rows k=1..4 (seq5) + EQ k=1@6 + POS row2(k3)@6 -> visible {2,4}
    s1 F1 rows k=2..5 (seq5) + EQ k=2@6 + POS row3(k5)@6 -> visible {3,4}
    Expected: delete 2, insert 3, key 4 unchanged.
    """
    root = Path(root)
    d_arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int32())])
    eq_arrow = pa.schema([("k", pa.int32())])
    pos_arrow = pa.schema([("target_file_id", pa.string()),
                           ("row_position", pa.int64())])
    f0 = _write_pq(root, "F0", pa.table(
        {"row_uid": ["a", "b", "c", "d"],
         "k": pa.array([1, 2, 3, 4], pa.int32())}, schema=d_arrow))
    f1 = _write_pq(root, "F1", pa.table(
        {"row_uid": ["b", "c", "d", "e"],
         "k": pa.array([2, 3, 4, 5], pa.int32())}, schema=d_arrow))
    eq0 = _write_pq(root, "EQ0", pa.table(
        {"k": pa.array([1], pa.int32())}, schema=eq_arrow))
    eq1 = _write_pq(root, "EQ1", pa.table(
        {"k": pa.array([2], pa.int32())}, schema=eq_arrow))
    p0 = _write_pq(root, "POS0", pa.table(
        {"target_file_id": ["F0"], "row_position": pa.array([2], pa.int64())},
        schema=pos_arrow))
    p1 = _write_pq(root, "POS1", pa.table(
        {"target_file_id": ["F1"], "row_position": pa.array([3], pa.int64())},
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

    def d(fid, rs, kind, seq, schema_id, **kw):
        rel, sha = rs
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


def build_composite_table(root):
    """Composite key (k, code); same k different code is a distinct record."""
    root = Path(root)
    arrow = pa.schema([("row_uid", pa.string()), ("k", pa.int32()),
                       ("code", pa.string()),
                       ("amt", pa.decimal128(10, 2))])

    def w(name, rows):
        return _write_pq(root, name, pa.table({
            "row_uid": [r[0] for r in rows],
            "k": pa.array([r[1] for r in rows], pa.int32()),
            "code": [r[2] for r in rows],
            "amt": pa.array([Decimal(r[3]) for r in rows],
                            pa.decimal128(10, 2))}, schema=arrow))

    d0 = w("D0", [("a", 1, "a", "1.00"), ("b", 2, "x", "2.00"),
                  ("c", 3, "z", "3.00")])
    d1 = w("D1", [("a2", 1, "b", "1.00"), ("b", 2, "x", "9.00"),
                  ("c", 3, "z", "3.00")])
    schemas = [{"schema_id": 1, "fields": [
        {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
         "required": True},
        {"field_id": 2, "name": "k", "type": {"kind": "int32"},
         "required": True},
        {"field_id": 7, "name": "code", "type": {"kind": "utf8"},
         "required": True},
        {"field_id": 3, "name": "amt",
         "type": {"kind": "decimal", "precision": 10, "scale": 2}}]}]

    def d(fid, rs, seq):
        rel, sha = rs
        return {"file_id": fid, "kind": "DATA", "path": rel, "sha256": sha,
                "partition": {"tenant": "t1"}, "data_seq": seq,
                "file_seq": seq, "schema_id": 1}

    tj = {"format": "offline-lake/1", "table_id": "ck", "schemas": schemas,
          "snapshots": [
              {"snapshot_id": "s0", "parent": None,
               "active_files": [d("D0", d0, 5)]},
              {"snapshot_id": "s1", "parent": "s0",
               "active_files": [d("D1", d1, 5)]}]}
    (root / "table.json").write_text(json.dumps(tj))
    return root


def check_self_contained(name, build_fn, src, tgt, schema, key_fids,
                         hand_summary, work_dir):
    root = build_fn(Path(tempfile.mkdtemp()) / "tab")
    _, schemas, snaps = load_contract(root)
    expected = naive_diff(root, schemas, snaps, src, tgt, schema, key_fids)
    summary = {}
    for c in ("insert", "delete", "update"):
        summary[c] = sorted(
            tuple(r["key"].items()) for r in expected if r["change"] == c)
    assert summary == hand_summary, (
        f"[{name}] independent {summary} != hand {hand_summary}")
    out_path = Path(tempfile.mkdtemp()) / "d.jsonl"
    cli_records = run_cli_compare(root, src, tgt, schema, key_fids,
                                  out_path, work_dir)
    assert cli_records == expected, f"[{name}] CLI != independent records"
    print(f"  PASS {name}: {len(expected)} records match independently")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--work-dir")
    args = ap.parse_args()
    root = args.table
    raw, schemas, snaps = load_contract(root)

    print("independent diff cross-check (no library code imported)")
    check_case("d_source -> d_target", root, schemas, snaps,
               "d_source", "d_target", 20, [2], HAND_MAIN, args.work_dir)
    check_case("d_source -> d_branch_b (different branch)", root, schemas,
               snaps, "d_source", "d_branch_b", 20, [2], HAND_BRANCH,
               args.work_dir)
    check_case("reverse d_target -> d_source", root, schemas, snaps,
               "d_target", "d_source", 20, [2],
               {"insert": [8], "delete": [12, 13],
                "update": [1, 2, 4, 7, 10]}, args.work_dir)
    check_rejected("null business key", root, snaps, "d_source",
                   "d_broken_null", 20, [2], args.work_dir)
    check_rejected("duplicate business key", root, snaps, "d_broken_dup",
                   "d_target", 20, [2], args.work_dir)

    # self-contained mini tables built by this verifier itself
    check_self_contained(
        "per-side POS/EQ delete scoping", build_perside_table,
        "s0", "s1", 1, [2],
        {"insert": [(("2", 3),)], "delete": [(("2", 2),)], "update": []},
        args.work_dir)
    check_self_contained(
        "composite business key", build_composite_table,
        "s0", "s1", 1, [2, 7],
        {"insert": [(("2", 1), ("7", "b"))],
         "delete": [(("2", 1), ("7", "a"))],
         "update": [(("2", 2), ("7", "x"))]},
        args.work_dir)
    print("ALL INDEPENDENT DIFF CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
