"""Subprocess worker for attribution lifecycle/timing acceptance tests.

Actions:
  attribute   run the attribution task; when --ready-flag is given the worker
              parks (while holding both read guards) until --continue-flag
              appears, so the parent can race a reclamation commit.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lakescan import FactAttributor  # noqa: E402


def _emit(path, payload):
    Path(path).write_text(json.dumps(payload, ensure_ascii=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action")
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ready-flag")
    ap.add_argument("--continue-flag")
    args = ap.parse_args()

    if args.action != "attribute":
        raise SystemExit(f"unknown action {args.action}")

    def park():
        if not args.ready_flag:
            return
        Path(args.ready_flag).write_text("ready")
        deadline = time.time() + 60
        while not Path(args.continue_flag).exists():
            if time.time() > deadline:
                _emit(args.out, {"error": "timeout waiting continue"})
                sys.exit(4)
            time.sleep(0.01)

    result = FactAttributor(args.config).attribute(after_guards=park)
    _emit(args.out, {
        "output_dir": str(result.output_dir),
        "stats": result.stats.to_json(),
        "fact_rows": result.fact_rows,
        "dimension_rows": result.dimension_rows,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
