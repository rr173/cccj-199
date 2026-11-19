#!/usr/bin/env python3
"""Independent cross-check for the retention/reclamation lifecycle.

This script shares NO code with the lakescan library: it parses table.json
and .lakescan/state.json itself, hand-derives the expected retention and the
safe deletion set from the same hand-written expectations used by the
acceptance suite, drives the library exclusively through its CLI in
subprocesses, and checks every outcome against literals.

Usage: python3 verify_lifecycle.py [--work DIR]
The drill table is generated at runtime into a fresh directory; no large or
binary artifacts are committed.
"""

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent

# refuse to start if the library is importable in this process
sys.path.insert(0, str(REPO))
for mod in ("lakescan", "pyarrow"):
    if mod in sys.modules:  # pragma: no cover - defensive
        raise SystemExit(f"independence violated: {mod} already imported")


def run_cli(*args, check=True):
    proc = subprocess.run(
        [sys.executable, "-m", "lakescan", *args],
        capture_output=True, text=True, cwd=REPO,
        env=dict(os.environ, PYTHONPATH=str(REPO)),
    )
    if check and proc.returncode != 0:
        raise AssertionError(
            f"CLI failed: lakescan {' '.join(args)}\n{proc.stderr}"
        )
    return proc


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def independent_retirement(manifest: dict, refs: dict) -> tuple:
    """Hand-derived retention: branches walk parent chains, bookmarks point."""
    snaps = {s["snapshot_id"]: s for s in manifest["snapshots"]}
    retained = set()
    for name, spec in refs.get("branches", {}).items():
        cur = snaps[spec["target"]]
        depth = int(spec["retain_generations"])
        d = 0
        while cur is not None and d < depth:
            retained.add(cur["snapshot_id"])
            cur = snaps.get(cur["parent"]) if cur.get("parent") else None
            d += 1
    for name, spec in refs.get("bookmarks", {}).items():
        retained.add(spec["target"])
    retiring = sorted(set(snaps) - retained)
    return sorted(retained), retiring


def independent_safe_paths(manifest: dict, retained: list) -> list:
    """A path is deletable iff only retiring snapshots reference it."""
    snaps = {s["snapshot_id"]: s for s in manifest["snapshots"]}
    refs_by_path: dict[str, set] = {}
    for sid, snap in snaps.items():
        for f in snap["active_files"]:
            refs_by_path.setdefault(f["path"], set()).add(sid)
    safe = sorted(p for p, users in refs_by_path.items() if not (users & set(retained)))
    return safe


def expect(cond, msg):
    if not cond:
        raise AssertionError("INDEPENDENT CHECK FAILED: " + msg)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work")
    args = ap.parse_args()

    work = Path(args.work or tempfile.mkdtemp(prefix="lakescan-verify-lc-"))
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    table = work / "lc"

    # runtime-generated tiny drill table (library only builds the files)
    run_cli("build-lifecycle-fixtures", "--out", str(table))
    manifest = load_json(table / "table.json")

    # ---- independent expectations from literal graph knowledge -----------
    refs = {
        "branches": {"main": {"target": "M2", "retain_generations": 2}},
        "bookmarks": {"bm_l": {"target": "L"}},
    }
    retained, retiring = independent_retirement(manifest, refs)
    expect(sorted(retained) == ["L", "M1", "M2"], f"retained={retained}")
    expect(sorted(retiring) == ["R", "X"], f"retiring={retiring}")
    safe = independent_safe_paths(manifest, retained)
    # R still references r1/r2/m1/m2/pos_m2 transitively? recompute exactly:
    # only data_x_only is referenced exclusively by a retiring snapshot (X).
    expect(safe == ["files/data_x_only.parquet"], f"safe paths={safe}")

    # ---- onboarding -------------------------------------------------------
    ob = json.loads(run_cli("onboard", "--table", str(table)).stdout)
    expect(ob["registered_paths"] == 9, str(ob))
    expect(not os.access(table / "table.json", os.W_OK), "table.json not read-only")
    state_path = table / ".lakescan" / "state.json"
    state0 = load_json(state_path)
    expect(state0["format"] == "lakescan-lifecycle/1", "state format")
    # manifest digest independently recomputed
    canon = json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")).encode()
    expect(hashlib.sha256(canon).hexdigest() == state0["manifest_sha256"],
           "bound manifest digest")

    # ---- references -------------------------------------------------------
    cfg = work / "refs.json"
    cfg.write_text(json.dumps(refs))
    rr = json.loads(run_cli("set-refs", "--table", str(table),
                            "--config", str(cfg)).stdout)
    expect(rr["ref_generation"] == 2, str(rr))

    # ---- plan: compare against the independent derivation -----------------
    plan_file = work / "plan.json"
    run_cli("make-plan", "--table", str(table), "--out", str(plan_file))
    plan = load_json(plan_file)
    expect(sorted(plan["to_retire"]) == retiring, "plan retirement mismatch")
    expect(sorted(d["path"] for d in plan["deletions"]) == safe,
           "plan deletion mismatch")
    # no full ancestor chain / sibling auto-retention
    expect(set(plan["retained"]) == set(retained), "plan retained mismatch")
    # plan generation must not have mutated anything
    expect(load_json(state_path)["retired_snapshots"] == [], "plan mutated state")

    # ---- pre-apply: tamper a candidate, expect refusal, restore -----------
    target = table / "files" / "data_x_only.parquet"
    original_bytes = target.read_bytes()
    os.chmod(target, stat.S_IRUSR | stat.S_IWUSR)
    target.write_bytes(b"operator swap")
    blocked = run_cli("apply-plan", "--table", str(table),
                      "--plan", str(plan_file), check=False)
    expect(blocked.returncode != 0 and "PlanBlockedError" in blocked.stderr,
           "tampered candidate not blocked")
    expect(load_json(state_path)["retired_snapshots"] == [],
           "blocked apply retired snapshots anyway")
    target.write_bytes(original_bytes)
    os.chmod(target, 0o444)

    # ---- apply ------------------------------------------------------------
    applied = json.loads(run_cli("apply-plan", "--table", str(table),
                                 "--plan", str(plan_file)).stdout)
    expect(applied["retired_snapshots"] == ["R", "X"], str(applied))
    expect(applied["reclaimed"] == ["files/data_x_only.parquet"], str(applied))
    expect(not target.exists(), "candidate file still present")
    # protected physical paths all survive
    for rel in ("files/dl_shared.parquet", "files/data_l_only.parquet",
                "files/r1.parquet", "files/r2.parquet"):
        expect((table / rel).exists(), f"protected path removed: {rel}")
    state1 = load_json(state_path)
    expect(sorted(state1["retired_snapshots"]) == ["R", "X"], "state retired")
    expect(state1["cleanup"] is None, "cleanup not settled")

    # ---- retired snapshots unreadable; retained rows exact ----------------
    refused = run_cli("scan", "--table", str(table), "--snapshot", "R",
                      "--schema", "1", "--project", "1", check=False)
    expect(refused.returncode == 2 and "SnapshotRetiredError" in refused.stderr,
           "retired snapshot stayed readable")
    l_scan = run_cli("scan", "--table", str(table), "--snapshot", "L",
                     "--schema", "1", "--project", "1")
    l_uids = [json.loads(line)["fields"]["row_uid"]
              for line in l_scan.stdout.splitlines()]
    expect(l_uids == ["r1-a", "r1-b", "r2-a", "l-a", "l-b", "x-b"],
           f"L rows changed: {l_uids}")

    # ---- lineage still includes the retired ancestor ----------------------
    ep = json.loads(run_cli("explain-plan", "--table", str(table),
                            "--snapshot", "M2", "--schema", "1",
                            "--project", "1").stdout)
    expect(ep["lineage"] == ["M2", "M1", "R"], "lineage lost retired ancestor")

    # ---- manifest drift fails closed --------------------------------------
    tj = table / "table.json"
    os.chmod(tj, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
    raw = load_json(tj)
    original_manifest_text = tj.read_text()
    raw["table_id"] = "tampered"
    tj.write_text(json.dumps(raw))
    drift = run_cli("lifecycle-status", "--table", str(table), check=False)
    expect(drift.returncode != 0 and "ManifestDriftError" in drift.stderr,
           "manifest drift not detected")
    # restore the exact bound bytes before further operations
    tj.write_text(original_manifest_text)
    os.chmod(tj, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    # ---- resume is a clean no-op once settled -----------------------------
    idle = json.loads(run_cli("resume", "--table", str(table)).stdout)
    expect(idle["cleanup_status"] == "none", str(idle))

    print("independent lifecycle cross-check OK")
    print(f"  table: {table}")
    print(f"  retained={sorted(retained)} retired={sorted(retiring)}")
    print(f"  reclaimed={applied['reclaimed']}")


if __name__ == "__main__":
    main()
