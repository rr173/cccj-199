"""Subprocess worker for lifecycle crash/timing acceptance tests.

Runs one deterministic action per invocation against an externally built
table.  All communication is file-based (status JSON written to an output
path, ready-flags for timing), so the parent test never has to parse console
output or rely on sleeps.

Actions:
  apply       apply a saved plan, optionally dying at a named boundary
  resume      resume the open reclamation batch
  reader      stream a snapshot one batch at a time, waiting on flags
  compare     run the two-sided compare with a single guard
  refs        replace the reference configuration
  status      print the lifecycle status JSON

Crash boundaries (LAKESCAN_TEST_CRASH):
  before_commit   after pre-checks, before the retirement boundary persists
  after_commit    right after the durable commit, before any unlink
  after_unlink    after the first unlink, before progress is recorded
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

# make the in-tree package importable when run as a plain python script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lakescan import LifecycleManager, Scanner, TableDiffer, collect  # noqa: E402
from lakescan.lifecycle_fixtures import EXPECTED  # noqa: E402
import lakescan.lifecycle.manager as mgr_mod  # noqa: E402


def _emit(path, payload):
    Path(path).write_text(json.dumps(payload, ensure_ascii=False))


def _install_crash(name):
    if not name:
        return
    hook = {
        "before_commit": mgr_mod._HOOK_BEFORE_COMMIT,
        "after_commit": mgr_mod._HOOK_AFTER_COMMIT,
        "after_unlink": mgr_mod._HOOK_AFTER_UNLINK,
    }[name]
    os.environ[hook] = "exit:9"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action")
    ap.add_argument("--table", required=True)
    ap.add_argument("--plan")
    ap.add_argument("--snapshot")
    ap.add_argument("--schema", type=int, default=1)
    ap.add_argument("--source")
    ap.add_argument("--target")
    ap.add_argument("--key-fields", default="2")
    ap.add_argument("--out")
    ap.add_argument("--ready-flag")
    ap.add_argument("--continue-flag")
    ap.add_argument("--batch-size", type=int, default=1)
    ap.add_argument("--crash")
    ap.add_argument("--refs", default="")
    args = ap.parse_args()

    mgr = LifecycleManager(args.table)

    if args.action == "refs":
        config = json.loads(args.refs) if args.refs else EXPECTED["refs"]
        _emit(args.out, mgr.set_references(config))
        return 0

    if args.action == "status":
        _emit(args.out, mgr.status())
        return 0

    if args.action in ("apply", "resume"):
        from lakescan.errors import LakeScanError

        _install_crash(args.crash)
        try:
            if args.action == "apply":
                result = mgr.apply_plan_file(args.plan)
            else:
                result = mgr.resume()
        except LakeScanError as e:
            # surface the concrete error class so the parent can assert on it
            print(f"{type(e).__name__}: {e}", file=sys.stderr)
            return 2
        if args.out:
            _emit(args.out, result.to_json())
        return 0 if result.cleanup_status != "blocked" else 3

    if args.action == "reader":
        sc = Scanner(args.table, batch_size=args.batch_size)
        rows = []
        iterator = sc.scan(args.snapshot, args.schema, (1,))
        for i, batch in enumerate(iterator):
            for j, pos in enumerate(batch.positions):
                rows.append({
                    "file_id": batch.file_id,
                    "row_position": pos,
                    "row_uid": batch.batch.column("row_uid").to_pylist()[j],
                })
            if i == 0 and args.ready_flag:
                # parked between batch 0 and batch 1: guard must stay held
                Path(args.ready_flag).write_text("ready")
                deadline = time.time() + 60
                while not Path(args.continue_flag).exists():
                    if time.time() > deadline:
                        _emit(args.out, {"error": "timeout waiting continue"})
                        return 4
                    time.sleep(0.01)
        _emit(args.out, {"rows": rows})
        return 0

    if args.action == "compare":
        # Acquire the real two-sided guard first, then optionally park while
        # holding it so the parent process can race an apply deterministically
        # (the table is tiny and an unpaused compare would finish in ms).
        from lakescan.lifecycle.reader import read_preflight

        keys = tuple(int(x) for x in args.key_fields.split(",") if x.strip())
        guard = read_preflight(args.table, [args.source, args.target],
                               reason="compare")
        try:
            if args.ready_flag:
                Path(args.ready_flag).write_text("ready")
                deadline = time.time() + 60
                while not Path(args.continue_flag).exists():
                    if time.time() > deadline:
                        _emit(args.out, {"error": "timeout waiting continue"})
                        return 4
                    time.sleep(0.01)
            result = TableDiffer(args.table).compare(
                args.source, args.target, args.schema, keys,
                args.out + ".jsonl"
            )
            _emit(args.out, {"stats": result.stats.to_json(),
                             "output": args.out + ".jsonl"})
            return 0
        finally:
            if guard is not None:
                guard.close()

    raise SystemExit(f"unknown action {args.action}")


if __name__ == "__main__":
    raise SystemExit(main())
