#!/usr/bin/env python3
"""Independent cross-check for the bitemporal fact attribution task.

This program shares NO code with the lakescan library and never imports it:
it re-parses both table.json contracts itself, re-reads every parquet file
with pyarrow only, applies POS/EQ deletes with plain Python dictionaries over
explicit physical row tables, performs the whole bitemporal attribution with
naive nested enumeration (the deliberately simple reference), and finally
invokes the library's ``attribute`` CLI as a subprocess (independent call
path) and compares the emitted Parquet row-by-row, value-by-value.

Usage:
  python3 verify_attribution.py --base DIR [--keep-dirs]

DIR is either a directory produced by `build-attribution-fixtures` or an
empty/non-existent location in which case the fixtures are generated first by
invoking the CLI as a subprocess.
"""

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq

assert "lakescan" not in sys.modules

REPO = Path(__file__).resolve().parent
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


# ---------------------------------------------------------------------------
# independent contract + visible-row reading
# ---------------------------------------------------------------------------


def load_contract(root):
    with open(Path(root) / "table.json", encoding="utf-8") as fh:
        raw = json.load(fh)
    assert raw["format"] == "offline-lake/1"
    return raw, {s["schema_id"]: s for s in raw["schemas"]}, \
        {s["snapshot_id"]: s for s in raw["snapshots"]}


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def part_key(p):
    return json.dumps(p or {}, sort_keys=True, separators=(",", ":"))


def read_rows(root, entry, eschema):
    p = Path(root) / entry["path"]
    assert sha256_file(p) == entry["sha256"], f"digest mismatch {entry['path']}"
    names = [f["name"] for f in eschema["fields"]]
    tbl = pq.read_table(p, columns=names)
    cols = {f["field_id"]: tbl.column(f["name"]).to_pylist()
            for f in eschema["fields"]}
    return [{fid: cols[fid][i] for fid in cols}
            for i in range(tbl.num_rows)]


def visible_rows(root, snap, schemas):
    """Naive snapshot scan -> list of (file_id, position, {field_id: value})."""
    entries = {e["file_id"]: e for e in snap["active_files"]}
    data_entries = [e for e in snap["active_files"] if e["kind"] == "DATA"]
    physical = {e["file_id"]: read_rows(root, e, schemas[e["schema_id"]])
                for e in data_entries}
    dead = {fid: set() for fid in physical}
    for e in snap["active_files"]:
        if e["kind"] == "POS_DELETE":
            es = schemas[e["schema_id"]]
            names = {f["field_id"]: f["name"] for f in es["fields"]}
            tbl = pq.read_table(Path(root) / e["path"],
                                columns=[names[1000], names[1001]])
            for t, pos in zip(tbl.column(names[1000]).to_pylist(),
                              tbl.column(names[1001]).to_pylist()):
                tgt = entries.get(t)
                if tgt is None or tgt["kind"] != "DATA":
                    continue
                if part_key(tgt["partition"]) != part_key(e["partition"]):
                    continue
                if tgt["data_seq"] > e["data_seq"]:
                    continue
                assert 0 <= pos < len(physical[t])
                dead[t].add(pos)
        elif e["kind"] == "EQ_DELETE":
            es = schemas[e["schema_id"]]
            key_ids = list(e["eq_field_ids"])
            names = {fid: next(f["name"] for f in es["fields"]
                               if f["field_id"] == fid) for fid in key_ids}
            tbl = pq.read_table(Path(root) / e["path"],
                                columns=list(names.values()))
            keys = set(zip(*(tbl.column(names[fid]).to_pylist()
                             for fid in key_ids)))
            for d in data_entries:
                if part_key(d["partition"]) != part_key(e["partition"]):
                    continue
                if not d["data_seq"] < e["data_seq"]:
                    continue
                for pos, row in enumerate(physical[d["file_id"]]):
                    tup = tuple(row.get(fid) for fid in key_ids)
                    if tup in keys:
                        dead[d["file_id"]].add(pos)
    out = []
    for e in data_entries:
        for pos, row in enumerate(physical[e["file_id"]]):
            if pos not in dead[e["file_id"]]:
                out.append((e["file_id"], pos, row))
    return out


# ---------------------------------------------------------------------------
# naive bitemporal reference
# ---------------------------------------------------------------------------


def naive_attribute(fact_rows, dim_rows):
    """Plain nested enumeration, exactly following the contract text."""
    # index revisions by (entity, rule id)
    by_rule = {}
    for file_id, pos, row in dim_rows:
        key = (row[11], row[12])
        by_rule.setdefault(key, []).append((file_id, pos, row))
    results = []
    for file_id, pos, fact in fact_rows:
        ek = fact[2]
        bt, kc = fact[3], fact[4]
        candidates = []
        for (rek, rid), recs in by_rule.items():
            if rek != ek:
                continue
            visible = [r for r in recs if r[2][13] <= kc]
            if not visible:
                continue
            # last record published at or before the cutoff (ties cannot
            # occur: the library rejects identity@pub duplicates)
            chosen = max(visible, key=lambda r: r[2][13])
            _, _, crow = chosen
            if crow[14] == "withdraw":
                continue
            vfrom, vto = crow[15], crow[16]
            if vfrom <= bt and (vto is None or bt < vto):
                candidates.append(chosen)
        if not candidates:
            status = "unmatched"
            results.append((fact[1], status, None, None, None, None, 0,
                            None, None))
            continue
        top_prio = max(r[2][17] for r in candidates)
        winners = [r for r in candidates if r[2][17] == top_prio]
        if len(winners) == 1:
            dfile, dpos, crow = winners[0]
            results.append((fact[1], "matched", crow[12], crow[13],
                            dfile, dpos, 1, None, None))
        else:
            winners.sort(key=lambda r: r[2][12].encode("utf-8"))
            ev = [(r[2][12], r[2][13], r[0], r[1]) for r in winners[:2]]
            results.append((fact[1], "ambiguous", None, None, None, None,
                            len(winners), ev, None))
    results.sort(key=lambda r: r[0].encode("utf-8"))
    return results


# ---------------------------------------------------------------------------
# comparison against the CLI output
# ---------------------------------------------------------------------------


def compare(output_dir, naive, fact_rows_by_id, mapping, schemas_fact,
            schemas_dim):
    table = pq.read_table(Path(output_dir) / "attribution.parquet")
    got = table.to_pylist()
    assert len(got) == len(naive), "one output row per fact"
    fids = [r["fact_id"] for r in got]
    assert fids == sorted(fids, key=lambda s: s.encode("utf-8")), "fid order"
    by_id = {r[0]: r for r in naive}
    for row in got:
        fid = row["fact_id"]
        nfid, status, rid, pub, dfile, dpos, count, ev, _ = by_id[fid]
        assert row["attribution_status"] == status
        if status == "matched":
            assert row["matched_rule_id"] == rid
            assert row["matched_published_at"] == pub
            assert row["matched_dim_file_id"] == dfile
            assert row["matched_dim_row_position"] == dpos
            assert row["candidate_count"] is None
        elif status == "unmatched":
            assert row["matched_rule_id"] is None
            assert row["candidate_count"] is None
            assert row["settlement_group"] is None
        else:
            assert row["candidate_count"] == count
            e1, e2 = ev[0], ev[1]
            assert row["evidence_1_rule_id"] == e1[0]
            assert row["evidence_1_published_at"] == e1[1]
            assert row["evidence_1_dim_file_id"] == e1[2]
            assert row["evidence_1_dim_row_position"] == e1[3]
            assert row["evidence_2_rule_id"] == e2[0]
            assert row["evidence_2_published_at"] == e2[1]
            assert row["settlement_group"] is None
        # fact provenance and timestamps retained exactly
        src = fact_rows_by_id[fid]
        assert row["fact_file_id"] == src[0]
        assert row["fact_row_position"] == src[1]
        assert row["business_time"] == src[2][3]
        assert row["knowledge_cutoff"] == src[2][4]
        assert row["entity_key"] == src[2][2]
    # exact arrow types follow the target schemas
    ftypes = {f["field_id"]: f
              for f in schemas_fact[mapping["fact"]["schema"]]["fields"]}
    dtypes = {f["field_id"]: f
              for f in schemas_dim[mapping["dimension"]["schema"]]["fields"]}
    for col in mapping["fact"]["fact_payload"]:
        field = table.schema.field(col["name"])
        ftype = ftypes[col["field_id"]]["type"]
        assert str(field.type) == _arrow_name(ftype["kind"], ftype)
    for col in mapping["dimension"]["dim_payload"]:
        field = table.schema.field(col["name"])
        assert field.nullable is True
    return got


def _arrow_name(kind, ftype):
    if kind == "decimal":
        return f"decimal128({ftype['precision']}, {ftype['scale']})"
    return {
        "int64": "int64", "int32": "int32", "utf8": "string",
        "boolean": "bool",
    }[kind]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--dim-snapshot", default="dims_full")
    ap.add_argument("--fact-snapshot", default="facts_main")
    ap.add_argument("--keep-dirs", action="store_true")
    args = ap.parse_args()
    base = Path(args.base)
    if not (base / "facts" / "table.json").exists():
        base.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([sys.executable, "-m", "lakescan",
                        "build-attribution-fixtures", "--out", str(base)],
                       check=True, cwd=REPO,
                       env={**__import__("os").environ,
                            "PYTHONPATH": str(REPO)})

    fact_root, dim_root = base / "facts", base / "dimensions"
    fraw, fschemas, fsnaps = load_contract(fact_root)
    draw, dschemas, dsnaps = load_contract(dim_root)
    fact_rows = visible_rows(
        fact_root, fsnaps[args.fact_snapshot], fschemas)
    dim_rows = visible_rows(
        dim_root, dsnaps[args.dim_snapshot], dschemas)
    naive = naive_attribute(fact_rows, dim_rows)

    mapping = {
        "fact": {
            "table": str(fact_root), "snapshot": args.fact_snapshot,
            "schema": fschemas and next(
                iter(fsnaps[args.fact_snapshot]["active_files"]))["schema_id"],
            "roles": {"fact_id": 1, "fact_entity_key": 2,
                      "fact_business_time": 3,
                      "fact_knowledge_cutoff": 4},
            "fact_payload": [
                {"field_id": 5, "name": "fact_amount"},
                {"field_id": 6, "name": "fact_note"}],
        },
        "dimension": {
            "table": str(dim_root), "snapshot": args.dim_snapshot,
            "schema": next(iter(
                dsnaps[args.dim_snapshot]["active_files"]))["schema_id"],
            "roles": {"dim_entity_key": 11, "dim_rule_id": 12,
                      "dim_published_at": 13, "dim_operation": 14,
                      "dim_valid_from": 15, "dim_valid_to": 16,
                      "dim_priority": 17},
            "dim_payload": [
                {"field_id": 18, "name": "settlement_group"},
                {"field_id": 19, "name": "rate_bps"}],
        },
        "output_dir": str(base / f"verify-out-{args.dim_snapshot}"),
    }
    out = Path(mapping["output_dir"])
    if out.exists():
        import shutil
        shutil.rmtree(out)
    cfg = base / "mapping.json"
    cfg.write_text(json.dumps(mapping))
    proc = subprocess.run(
        [sys.executable, "-m", "lakescan", "attribute",
         "--config", str(cfg)],
        capture_output=True, text=True, cwd=REPO,
        env={**__import__("os").environ, "PYTHONPATH": str(REPO)})
    assert proc.returncode == 0, proc.stderr
    fact_rows_by_id = {(r[2][1]): r for r in fact_rows}
    got = compare(out, naive, fact_rows_by_id, mapping, fschemas, dschemas)
    counts = {"matched": 0, "unmatched": 0, "ambiguous": 0}
    for r in got:
        counts[r["attribution_status"]] += 1
    print(f"OK {args.dim_snapshot}: {len(got)} rows {counts}")


if __name__ == "__main__":
    main()
