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
