"""Command line interface: build-fixtures / scan / explain-plan / explain-row / verify."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from .errors import LakeScanError
from .scan import Scanner


def _json_default(o):
    if isinstance(o, Decimal):
        return str(o)  # exact, never float
    if isinstance(o, (set, tuple)):
        return list(o)
    raise TypeError(f"not JSON serializable: {type(o).__name__}")


def _emit(obj):
    print(json.dumps(obj, default=_json_default, ensure_ascii=False))


def _parse_projection(text: str):
    text = text.strip()
    if not text:
        return ()
    return tuple(int(x) for x in text.split(","))


def _load_predicate(path: str):
    if path is None:
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def cmd_build_fixtures(args) -> int:
    from .fixtures import build

    out = build(args.out)
    _emit({"built": str(out)})
    return 0


def cmd_build_diff_fixtures(args) -> int:
    from .diff_fixtures import build_diff_table

    out = build_diff_table(args.out)
    _emit({"built": str(out)})
    return 0


def cmd_scan(args) -> int:
    sc = Scanner(args.table, work_dir=args.work_dir,
                 batch_size=args.batch_size, prune=not args.no_prune)
    pred = _load_predicate(args.predicate)
    projection = _parse_projection(args.project)
    for sb in sc.scan(args.snapshot, args.schema, projection, pred):
        names = sb.batch.schema.names
        cols = {n: sb.batch.column(n).to_pylist() for n in names}
        for i, pos in enumerate(sb.positions):
            _emit({
                "file_id": sb.file_id,
                "row_position": pos,
                "fields": {n: cols[n][i] for n in names},
            })
    return 0


def cmd_explain_plan(args) -> int:
    sc = Scanner(args.table, work_dir=args.work_dir, prune=not args.no_prune)
    pred = _load_predicate(args.predicate)
    projection = _parse_projection(args.project)
    _emit(sc.explain_plan(args.snapshot, args.schema, projection, pred))
    return 0


def cmd_explain_row(args) -> int:
    sc = Scanner(args.table, work_dir=args.work_dir)
    _emit(sc.explain_row(args.snapshot, args.file_id, args.row,
                         target_schema_id=args.schema))
    return 0


def cmd_verify(args) -> int:
    """Run the independent cross-check (verify.py) as a separate process."""
    verify_py = Path(__file__).resolve().parent.parent / "verify.py"
    cmd = [sys.executable, str(verify_py), "--table", args.table]
    if args.work_dir:
        cmd += ["--work-dir", args.work_dir]
    proc = subprocess.run(cmd)
    return proc.returncode


def cmd_compare(args) -> int:
    from .diff import TableDiffer

    key_field_ids = tuple(int(x) for x in args.key_fields.split(",") if x.strip())
    differ = TableDiffer(args.table, work_dir=args.work_dir,
                         batch_size=args.batch_size)
    result = differ.compare(
        source_snapshot=args.source,
        target_snapshot=args.target,
        target_schema_id=args.schema,
        key_field_ids=key_field_ids,
        output_path=args.out,
    )
    _emit({
        "output": str(result.output_path),
        "source_snapshot": result.source_snapshot,
        "target_snapshot": result.target_snapshot,
        "target_schema": result.target_schema_id,
        "key_field_ids": list(result.key_field_ids),
        "stats": result.stats.to_json(),
        "work_dir": str(result.work_dir) if result.work_dir is not None
        else None,
    })
    return 0


# ---------------------------------------------------------------------------
# lifecycle: retention and recoverable reclamation
# ---------------------------------------------------------------------------


def _lifecycle(args):
    from .lifecycle import LifecycleManager

    return LifecycleManager(args.table)


def cmd_onboard(args) -> int:
    mgr = _lifecycle(args)
    state = mgr.onboard()
    _emit({
        "onboarded": True,
        "table": str(mgr.root),
        "table_id": state.table_id,
        "manifest_sha256": state.manifest_sha256,
        "registered_paths": len(state.registered_paths),
        "state_dir": str(state.dir),
    })
    return 0


def cmd_lifecycle_status(args) -> int:
    _emit(_lifecycle(args).status())
    return 0


def cmd_set_refs(args) -> int:
    with open(args.config, "r", encoding="utf-8") as fh:
        config = json.load(fh)
    result = _lifecycle(args).set_references(config)
    _emit(result)
    return 0


def cmd_make_plan(args) -> int:
    plan = _lifecycle(args).plan()
    if args.out:
        plan.write(args.out)
    _emit(plan.to_json())
    return 0


def cmd_apply_plan(args) -> int:
    mgr = _lifecycle(args)
    result = mgr.apply_plan_file(args.plan)
    _emit(result.to_json())
    if result.cleanup_status == "blocked":
        # retirement happened but physical cleanup is incomplete: report
        # distinctly so callers never mistake this for "still readable".
        return 3
    return 0


def cmd_resume(args) -> int:
    result = _lifecycle(args).resume()
    _emit(result.to_json())
    if result.cleanup_status == "blocked":
        return 3
    return 0


def cmd_build_lifecycle_fixtures(args) -> int:
    from .lifecycle_fixtures import build_lifecycle_table

    out = build_lifecycle_table(args.out)
    _emit({"built": str(out)})
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lakescan",
        description="Snapshot-scanned reader for the offline lake table "
                    "contract (not a full Iceberg implementation).",
    )
    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser("build-fixtures", help="build the drill table")
    b.add_argument("--out", required=True)
    b.set_defaults(fn=cmd_build_fixtures)

    bd = sub.add_parser("build-diff-fixtures",
                        help="build the cross-snapshot diff drill table")
    bd.add_argument("--out", required=True)
    bd.set_defaults(fn=cmd_build_diff_fixtures)

    s = sub.add_parser("scan", help="scan one fixed snapshot")
    s.add_argument("--table", required=True)
    s.add_argument("--snapshot", required=True)
    s.add_argument("--schema", type=int, required=True, help="target schema id")
    s.add_argument("--project", default="", help="comma-separated field ids")
    s.add_argument("--predicate", help="JSON file with eq/lt/is_null + and/or/not")
    s.add_argument("--work-dir")
    s.add_argument("--batch-size", type=int, default=65536)
    s.add_argument("--no-prune", action="store_true")
    s.set_defaults(fn=cmd_scan)

    ep = sub.add_parser("explain-plan", help="explain the read plan of a scan")
    ep.add_argument("--table", required=True)
    ep.add_argument("--snapshot", required=True)
    ep.add_argument("--schema", type=int, required=True)
    ep.add_argument("--project", default="")
    ep.add_argument("--predicate")
    ep.add_argument("--work-dir")
    ep.add_argument("--no-prune", action="store_true")
    ep.set_defaults(fn=cmd_explain_plan)

    er = sub.add_parser("explain-row",
                        help="explain which delete file(s) exclude a row")
    er.add_argument("--table", required=True)
    er.add_argument("--snapshot", required=True)
    er.add_argument("--file-id", required=True)
    er.add_argument("--row", type=int, required=True)
    er.add_argument("--schema", type=int)
    er.add_argument("--work-dir")
    er.set_defaults(fn=cmd_explain_row)

    v = sub.add_parser("verify", help="independent cross-check (no shared code)")
    v.add_argument("--table", required=True)
    v.add_argument("--work-dir")
    v.set_defaults(fn=cmd_verify)

    c = sub.add_parser(
        "compare",
        help="repeatable key diff of two snapshots, atomically published",
    )
    c.add_argument("--table", required=True)
    c.add_argument("--source", required=True, help="source (baseline) snapshot id")
    c.add_argument("--target", required=True, help="target snapshot id")
    c.add_argument("--schema", type=int, required=True,
                   help="target schema id both sides are normalized to")
    c.add_argument("--key-fields", required=True,
                   help="comma-separated stable business-key field ids")
    c.add_argument("--out", required=True, help="output JSONL path")
    c.add_argument("--work-dir")
    c.add_argument("--batch-size", type=int, default=8192)
    c.set_defaults(fn=cmd_compare)

    # -- lifecycle ---------------------------------------------------------
    blf = sub.add_parser(
        "build-lifecycle-fixtures",
        help="build the retention/reclamation drill table (runtime, tiny files)",
    )
    blf.add_argument("--out", required=True)
    blf.set_defaults(fn=cmd_build_lifecycle_fixtures)

    ob = sub.add_parser(
        "onboard",
        help="bring an existing table directory under lifecycle management",
    )
    ob.add_argument("--table", required=True)
    ob.set_defaults(fn=cmd_onboard)

    st = sub.add_parser("lifecycle-status",
                        help="show management, retention, reader and cleanup state")
    st.add_argument("--table", required=True)
    st.set_defaults(fn=cmd_lifecycle_status)

    sr = sub.add_parser("set-refs",
                        help="wholesale-replace the named reference configuration")
    sr.add_argument("--table", required=True)
    sr.add_argument("--config", required=True,
                    help="JSON file: {branches:{name:{target,retain_generations}},"
                         " bookmarks:{name:{target}}}")
    sr.set_defaults(fn=cmd_set_refs)

    mp = sub.add_parser("make-plan",
                        help="derive a retention/reclamation plan (no mutation)")
    mp.add_argument("--table", required=True)
    mp.add_argument("--out", help="optional explicit plan JSON path")
    mp.set_defaults(fn=cmd_make_plan)

    ap = sub.add_parser("apply-plan",
                        help="retire snapshots and reclaim paths for a saved plan")
    ap.add_argument("--table", required=True)
    ap.add_argument("--plan", required=True)
    ap.set_defaults(fn=cmd_apply_plan)

    rs = sub.add_parser("resume", help="continue an unfinished reclamation batch")
    rs.add_argument("--table", required=True)
    rs.set_defaults(fn=cmd_resume)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args)
    except LakeScanError as e:
        print(f"error: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
