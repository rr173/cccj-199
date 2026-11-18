#!/usr/bin/env python3
"""Independent cross-check for the lakescan library.

This script shares NO code with the library: it re-reads table.json, applies
position/equality deletes with plain Python over physical row tables, does
decimal comparisons with integer arithmetic, evaluates predicates with its
own three-valued logic, and compares the result against the library's CLI
scan output.  The main drill's expectation is a hand-written physical row
table.

Usage: python3 verify.py --table DIR [--work-dir DIR]
"""

import argparse
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# minimal contract reading (independent re-implementation)
# ---------------------------------------------------------------------------


def load_contract(root):
    with open(Path(root) / "table.json", "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    schemas = {s["schema_id"]: s for s in raw["schemas"]}
    snaps = {s["snapshot_id"]: s for s in raw["snapshots"]}
    return raw, schemas, snaps


def field_map(schema):
    return {f["field_id"]: f for f in schema["fields"]}


def canon_part(p):
    return json.dumps(p or {}, sort_keys=True, separators=(",", ":"))


def dec_int(d: Decimal):
    """Decimal -> (unscaled int, scale) by integer arithmetic only."""
    t = d.as_tuple()
    scale = -t.exponent
    unscaled = 0
    for dig in t.digits:
        unscaled = unscaled * 10 + dig
    if t.sign:
        unscaled = -unscaled
    return unscaled, scale


def dec_norm(d: Decimal, scale: int) -> int:
    """Decimal -> unscaled int at the given scale (exact; scales match per
    field in this contract, so this is a no-op or a power-of-ten multiply)."""
    u, s = dec_int(d)
    if s == scale:
        return u
    if s < scale:
        return u * (10 ** (scale - s))
    assert u % (10 ** (s - scale)) == 0, "contract violation: inexact rescale"
    return u // (10 ** (s - scale))


# ---------------------------------------------------------------------------
# independent scan over physical row tables
# ---------------------------------------------------------------------------


def read_rows(root, entry, schema):
    """Yield physical rows as {field_id: python value}; decimals as Decimal."""
    import hashlib

    p = Path(root) / entry["path"]
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    assert digest == entry["sha256"], f"digest mismatch on {entry['path']}"
    fmap = field_map(schema)
    names = [f["name"] for f in schema["fields"]]
    tbl = pq.read_table(p, columns=names)
    cols = {}
    for f in schema["fields"]:
        cols[f["field_id"]] = tbl.column(f["name"]).to_pylist()
    n = tbl.num_rows
    for i in range(n):
        yield i, {fid: cols[fid][i] for fid in cols}


def independent_scan(root, schemas, snap, target_schema_id, projection,
                     predicate=None):
    target = schemas[target_schema_id]
    tmap = field_map(target)
    for fid in projection:
        assert fid in tmap, f"projection field {fid} not in target schema"

    files = {e["file_id"]: e for e in snap["active_files"]}
    data_files = [e for e in snap["active_files"] if e["kind"] == "DATA"]

    # ---- position deletes: {target_file_id: {positions}} ------------------
    pos_deleted = {}
    for e in snap["active_files"]:
        if e["kind"] != "POS_DELETE":
            continue
        eschema = schemas[e["schema_id"]]
        fmap = field_map(eschema)
        tname = fmap[1000]["name"]
        pname = fmap[1001]["name"]
        tbl = pq.read_table(Path(root) / e["path"], columns=[tname, pname])
        for t, pos in zip(tbl.column(tname).to_pylist(),
                          tbl.column(pname).to_pylist()):
            tgt = files.get(t)
            if tgt is None or tgt["kind"] != "DATA":
                continue  # inactive target: diagnostic only, never applied
            if canon_part(tgt["partition"]) != canon_part(e["partition"]):
                continue
            if tgt["data_seq"] > e["data_seq"]:
                continue  # non-strict: target.data_seq <= delete.data_seq
            nrows = pq.ParquetFile(Path(root) / tgt["path"]).metadata.num_rows
            assert 0 <= pos < nrows, f"snapshot error: position {pos} out of range"
            pos_deleted.setdefault(t, set()).add(pos)  # set: duplicates delete once

    # ---- equality deletes: list of (partition, field_ids, {keys}, seq) ----
    eq_deletes = []
    for e in snap["active_files"]:
        if e["kind"] != "EQ_DELETE":
            continue
        eschema = schemas[e["schema_id"]]
        fmap = field_map(eschema)
        fids = sorted(e["eq_field_ids"])
        names = [fmap[f]["name"] for f in fids]
        tbl = pq.read_table(Path(root) / e["path"], columns=names)
        cols = [tbl.column(n).to_pylist() for n in names]
        keys = set()
        for row in zip(*cols):
            keys.add(tuple(_canon(v) for v in row))
        eq_deletes.append((canon_part(e["partition"]), tuple(fids), keys,
                           e["data_seq"]))

    # ---- scan data files in listed order ----------------------------------
    out = []
    for e in data_files:
        eschema = schemas[e["schema_id"]]
        fmap = field_map(eschema)
        part = canon_part(e["partition"])
        for pos, row in read_rows(root, e, eschema):
            if pos in pos_deleted.get(e["file_id"], ()):
                continue
            deleted = False
            for epart, fids, keys, dseq in eq_deletes:
                if epart != part or not (e["data_seq"] < dseq):
                    continue  # strict <
                key = tuple(
                    _canon(row[fid]) if fid in fmap else None  # missing -> null
                    for fid in fids
                )
                if key in keys:
                    deleted = True
                    break
            if deleted:
                continue
            if predicate is not None and not _pred_true(predicate, row, fmap):
                continue
            out.append((e["file_id"], pos,
                        tuple(_widen(row.get(fid), fmap.get(fid), tmap[fid])
                              for fid in projection)))
    return out


def _canon(v):
    """Canonical exact value: Decimal -> ('dec', unscaled, scale); else itself."""
    if isinstance(v, Decimal):
        u, s = dec_int(v)
        return ("dec", u, s)
    if isinstance(v, bool):
        return ("bool", v)
    if isinstance(v, int):
        return ("int", v)
    if isinstance(v, str):
        return ("str", v)
    assert v is None, f"unexpected value {v!r}"
    return None


def _canon_at_scale(v, scale):
    """Canonical comparison value with decimals normalized by integers."""
    c = _canon(v)
    if c is None:
        return None
    if c[0] == "dec":
        return ("dec", dec_norm(_to_decimal(v), scale))
    return c


def _to_decimal(v):
    return v if isinstance(v, Decimal) else Decimal(v)


def _widen(v, sfield, tfield):
    """Widen a stored value to the target field type (exact)."""
    if v is None:
        return None
    kind = tfield["type"]["kind"]
    if kind == "decimal":
        return dec_norm(v, tfield["type"]["scale"])
    return v  # int32->int64, utf8, boolean are identity in Python


# ---------------------------------------------------------------------------
# independent three-valued predicate evaluation
# ---------------------------------------------------------------------------

UNKNOWN = None


def _lit(lit):
    k = lit["kind"]
    if k == "null":
        return None
    if k == "int":
        return ("int", lit["value"])
    if k == "utf8":
        return ("str", lit["value"])
    if k == "bool":
        return ("bool", lit["value"])
    if k == "decimal":
        return ("dec", lit["unscaled"], lit["scale"])
    raise AssertionError(lit)


def _cmp_values(a, b):
    """Exact comparison of canonical values; None anywhere -> UNKNOWN."""
    if a is None or b is None:
        return UNKNOWN
    if a[0] == "dec" and b[0] == "dec":
        s = min(a[2], b[2])
        x = a[1] * (10 ** (a[2] - s))
        y = b[1] * (10 ** (b[2] - s))
        return (x > y) - (x < y)
    if a[0] != b[0]:
        # int literal vs decimal column etc.: compare as integers at scale
        if {a[0], b[0]} == {"int", "dec"}:
            ai = a if a[0] == "int" else b
            dc = a if a[0] == "dec" else b
            x = ai[1] * (10 ** dc[2])
            return (x > dc[1]) - (x < dc[1])
        raise AssertionError(f"type mismatch {a} vs {b}")
    return (a[1] > b[1]) - (b[1] > a[1])


def _eval(pred, row, fmap):
    op = pred["op"]
    if op in ("eq", "lt"):
        fid = pred["field_id"]
        v = _canon(row.get(fid)) if fid in fmap else None
        lit = _lit(pred["literal"])
        if lit is not None and lit[0] == "dec" and v is not None and v[0] == "dec":
            # normalize both to common scale by integer arithmetic
            s = min(v[2], lit[2])
            v = ("dec", v[1] * (10 ** (v[2] - s)), s)
            lit = ("dec", lit[1] * (10 ** (lit[2] - s)), s)
        c = _cmp_values(v, lit)
        if c is UNKNOWN:
            return UNKNOWN
        return (c == 0) if op == "eq" else (c < 0)
    if op == "is_null":
        fid = pred["field_id"]
        return (fid not in fmap) or row.get(fid) is None
    if op == "and":
        # Kleene: False if any False; else UNKNOWN if any UNKNOWN; else True
        results = [_eval(a, row, fmap) for a in pred["args"]]
        if any(r is False for r in results):
            return False
        if any(r is UNKNOWN for r in results):
            return UNKNOWN
        return True
    if op == "or":
        results = [_eval(a, row, fmap) for a in pred["args"]]
        if any(r is True for r in results):
            return True
        if any(r is UNKNOWN for r in results):
            return UNKNOWN
        return False
    if op == "not":
        r = _eval(pred["arg"], row, fmap)
        return UNKNOWN if r is UNKNOWN else (not r)
    raise AssertionError(op)


def _pred_true(pred, row, fmap):
    return _eval(pred, row, fmap) is True


# ---------------------------------------------------------------------------
# hand-written expectations (manual physical row tables)
# ---------------------------------------------------------------------------

# Main drill.  File A (data_seq=5): 0:(u1,k=1) 1:(u1,k=null) 2:(v2,k=2)
# File B (data_seq=6): 0:(b1,k=1) 1:(b2,k=3)
# EQ@6 k=1  -> deletes A0 only (B.seq 6 is not strictly < 6)
# POS@6 A1  -> deletes A1 (A.seq 5 <= 6); the duplicate A1 entry deletes nothing more
# Expected survivors: A2, B0, B1.
MAIN_DRILL_EXPECTED = [
    ("A", 2, ("v2", 2)),
    ("B", 0, ("b1", 1)),
    ("B", 1, ("b2", 3)),
]

# Without the POS delete, A1 (k=null) comes back; the EQ key k=1 must not
# touch the null.
NOPOS_EXPECTED = [
    ("A", 1, ("u1", None)),
    ("A", 2, ("v2", 2)),
    ("B", 0, ("b1", 1)),
    ("B", 1, ("b2", 3)),
]


# ---------------------------------------------------------------------------
# comparison against the library CLI
# ---------------------------------------------------------------------------


def cli_scan(root, snapshot, schema_id, projection, predicate=None,
             extra_env=None):
    cmd = [
        sys.executable, "-m", "lakescan", "scan",
        "--table", str(root),
        "--snapshot", snapshot,
        "--schema", str(schema_id),
        "--project", ",".join(str(p) for p in projection),
    ]
    cleanup = None
    if predicate is not None:
        import tempfile

        fd, ppath = tempfile.mkstemp(suffix=".json")
        with open(fd, "w", encoding="utf-8") as fh:
            json.dump(predicate, fh)
        cmd += ["--predicate", ppath]
        cleanup = ppath
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    if cleanup:
        Path(cleanup).unlink()
    if proc.returncode != 0:
        raise RuntimeError(f"CLI scan failed: {proc.stderr}")
    rows = []
    for line in proc.stdout.splitlines():
        o = json.loads(line)
        rows.append((o["file_id"], o["row_position"], o["fields"]))
    return rows


def _cli_tuple(fields, projection, target_schema):
    """CLI JSON row -> comparison tuple (decimals normalized by integers)."""
    tmap = field_map(target_schema)
    out = []
    for fid in projection:
        v = fields[tmap[fid]["name"]]
        if v is not None and tmap[fid]["type"]["kind"] == "decimal":
            v = dec_norm(Decimal(v), tmap[fid]["type"]["scale"])
        out.append(v)
    return tuple(out)


def compare(name, root, schemas, snaps, snapshot, schema_id, projection,
            predicate=None, hand_expected=None):
    snap = snaps[snapshot]
    expected = independent_scan(root, schemas, snap, schema_id, projection,
                                predicate)
    if hand_expected is not None:
        assert list(expected) == list(hand_expected), (
            f"[{name}] independent computation disagrees with the "
            f"hand-written table:\n  independent: {expected}\n  hand: "
            f"{hand_expected}"
        )
    cli_rows = cli_scan(root, snapshot, schema_id, projection, predicate)
    tschema = schemas[schema_id]
    got = [(f, p, _cli_tuple(fields, projection, tschema))
           for f, p, fields in cli_rows]
    assert got == list(expected), (
        f"[{name}] library output != independent expectation:\n"
        f"  library:     {got}\n  independent: {list(expected)}"
    )
    print(f"  PASS {name} ({len(got)} rows)")
    return got


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--work-dir")
    args = ap.parse_args()
    root = args.table
    raw, schemas, snaps = load_contract(root)

    print("independent cross-check (no shared code with the library)")

    # 1. main drill, checked against the hand-written physical row table
    compare("main drill", root, schemas, snaps, "s_main", 2, [1, 2],
            hand_expected=MAIN_DRILL_EXPECTED)

    # 2. POS removed -> A1 restored, null not matched by key k=1
    compare("pos removed restores row", root, schemas, snaps, "s_nopos", 2,
            [1, 2], hand_expected=NOPOS_EXPECTED)

    # 3. branch snapshots: only the snapshot's own active_files
    got_c = compare("branch C", root, schemas, snaps, "s_fork_c", 2, [1, 2])
    assert any(f == "C" for f, _, _ in got_c) and not any(
        f == "D" for f, _, _ in got_c)
    got_d = compare("branch D", root, schemas, snaps, "s_fork_d", 2, [1, 2])
    assert any(f == "D" for f, _, _ in got_d) and not any(
        f == "C" for f, _, _ in got_d)

    # 4. strict (EQ) vs non-strict (POS) data_seq boundary; file_seq ignored
    compare("seq boundaries", root, schemas, snaps, "s_bound", 2, [1, 2],
            hand_expected=[("E", 0, ("e0", 5)), ("J", 1, ("j1", 1))])

    # 5. POS/EQ overlap, cross-row-group positions, identical-content files
    compare("overlap + cross-rowgroup + identical content", root, schemas,
            snaps, "s_overlap", 2, [1, 2],
            hand_expected=[("F", 0, ("f0", 7)), ("F", 1, ("f1", 8)),
                           ("K1", 1, ("kB", 2)), ("K2", 0, ("kA", 1)),
                           ("K2", 1, ("kB", 2))])

    # 6. rename keeps identity; same-name new id does not inherit; null keys
    #    (N's schema has no field 7 at all: the null key deletes every N row)
    compare("rename + null keys", root, schemas, snaps, "s_ren", 2, [1, 7, 9],
            hand_expected=[("G", 0, ("g0", "keep", None)),
                           ("G", 2, ("g2", "keep", None)),
                           ("H", 1, ("h1", None, None))])
    compare("missing fields read as null", root, schemas, snaps, "s_n_plain",
            2, [1, 7, 9],
            hand_expected=[("N", 0, ("n0", None, None)),
                           ("N", 1, ("n1", None, None))])

    # 7. decimal precision widening, exact integer comparison
    compare("decimal widening", root, schemas, snaps, "s_dec", 2, [1, 3],
            hand_expected=[("P", 1, ("p1", 2025)), ("P", 2, ("p2", 9999)),
                           ("Q", 0, ("q0", 1050)), ("Q", 1, ("q1", 2025))])

    # 8. delete key projected away: deletes must still apply
    compare("projection drops delete key", root, schemas, snaps, "s_main",
            2, [1],
            hand_expected=[("A", 2, ("v2",)), ("B", 0, ("b1",)),
                           ("B", 1, ("b2",))])

    # 9. three-valued logic battery on s_3vl (rows m0..m3 at positions 0..3)
    P = lambda op, **kw: dict(op=op, **kw)
    INT = lambda v: {"kind": "int", "value": v}
    compare("3vl eq null", root, schemas, snaps, "s_3vl", 2, [1],
            predicate=P("eq", field_id=2, literal={"kind": "null"}),
            hand_expected=[])
    compare("3vl is_null", root, schemas, snaps, "s_3vl", 2, [1],
            predicate=P("is_null", field_id=2),
            hand_expected=[("M", 1, ("m1",))])
    compare("3vl lt", root, schemas, snaps, "s_3vl", 2, [1],
            predicate=P("lt", field_id=2, literal=INT(3)),
            hand_expected=[("M", 0, ("m0",)), ("M", 2, ("m2",))])
    compare("3vl or", root, schemas, snaps, "s_3vl", 2, [1],
            predicate=P("or", args=[P("eq", field_id=2, literal=INT(1)),
                                    P("is_null", field_id=2)]),
            hand_expected=[("M", 0, ("m0",)), ("M", 1, ("m1",))])
    compare("3vl and+not", root, schemas, snaps, "s_3vl", 2, [1],
            predicate=P("and", args=[P("lt", field_id=2, literal=INT(3)),
                                     P("not", arg=P("is_null", field_id=2))]),
            hand_expected=[("M", 0, ("m0",)), ("M", 2, ("m2",))])
    compare("3vl not(or)", root, schemas, snaps, "s_3vl", 2, [1],
            predicate=P("not", arg=P("or", args=[
                P("eq", field_id=2, literal=INT(1)),
                P("eq", field_id=2, literal=INT(3))])),
            hand_expected=[("M", 2, ("m2",))])
    compare("3vl bool eq", root, schemas, snaps, "s_3vl", 2, [1],
            predicate=P("eq", field_id=4, literal={"kind": "bool", "value": True}),
            hand_expected=[("M", 0, ("m0",)), ("M", 3, ("m3",))])

    # 10. diagnostics-only snapshot scans clean
    compare("diagnostics do not alter data", root, schemas, snaps, "s_diag",
            2, [1, 2],
            hand_expected=[("A", 0, ("u1", 1)), ("A", 1, ("u1", None)),
                           ("A", 2, ("v2", 2)), ("B", 0, ("b1", 1)),
                           ("B", 1, ("b2", 3))])

    print("ALL INDEPENDENT CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
