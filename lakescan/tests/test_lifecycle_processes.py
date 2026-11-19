"""Cross-process acceptance: real OS-process timing, kills and recovery.

Every scenario uses independent subprocesses (``lifecycle_worker.py``) and
file-based flags for deterministic ordering -- no sleeps for synchronization.
The suite covers:

* a reader that starts *after* the plan and parks between batches;
* a two-sided compare whose single guard protects both snapshots;
* a reader process killed while parked (no permanent retention);
* reclamation processes killed at the three crash boundaries
  (before commit / after commit / after the first unlink) and cross-process
  resume of the same batch;
* a swapped to-be-deleted file and a concurrent duplicate apply.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from lakescan import LifecycleManager, Scanner, SnapshotRetiredError, collect
from lakescan.lifecycle_fixtures import EXPECTED, build_lifecycle_table

WORKER = Path(__file__).resolve().parent / "lifecycle_worker.py"


@pytest.fixture()
def table(tmp_path):
    root = tmp_path / "lc"
    build_lifecycle_table(root)
    mgr = LifecycleManager(root)
    mgr.onboard()
    mgr.set_references(EXPECTED["refs"])
    return root


def _run(args, check=True, env_extra=None, timeout=90):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        [sys.executable, str(WORKER), *args],
        capture_output=True, text=True, env=env, timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise AssertionError(
            f"worker failed ({proc.returncode}): {proc.stderr}\n{proc.stdout}"
        )
    return proc


def _wait_flag(path: Path, timeout=30):
    deadline = time.time() + timeout
    while not path.exists():
        if time.time() > deadline:
            raise AssertionError(f"timed out waiting for {path}")
        time.sleep(0.01)


def _read(path: Path):
    return json.loads(path.read_text())


def _make_plan(root: Path) -> Path:
    plan = LifecycleManager(root).plan()
    return plan.path


# ---------------------------------------------------------------------------
# reader started after the plan, parked between batches
# ---------------------------------------------------------------------------


def test_reader_after_plan_blocks_then_survives_resume(tmp_path, table):
    plan_path = _make_plan(table)
    ready = tmp_path / "reader.ready"
    cont = tmp_path / "reader.cont"
    out = tmp_path / "reader.out"

    reader = subprocess.Popen(
        [sys.executable, str(WORKER), "reader", "--table", str(table),
         "--snapshot", "R", "--batch-size", "1",
         "--ready-flag", str(ready), "--continue-flag", str(cont),
         "--out", str(out)],
    )
    try:
        _wait_flag(ready)
        # apply must be refused while the reader parks between batches
        blocked = _run(["apply", "--table", str(table), "--plan", str(plan_path)],
                       check=False)
        assert blocked.returncode != 0
        assert "PlanBlockedError" in blocked.stderr
        assert (table / "files" / "data_x_only.parquet").exists()

        cont.write_text("go")
        reader.wait(timeout=30)
        assert reader.returncode == 0
        rows = [(r["file_id"], r["row_position"], r["row_uid"])
                for r in _read(out)["rows"]]
        assert rows == EXPECTED["rows"]["R"]

        # reader exited: the original, still-current plan applies unchanged
        apply_out = tmp_path / "apply.json"
        _run(["apply", "--table", str(table), "--plan", str(plan_path),
              "--out", str(apply_out)])
        result = _read(apply_out)
        assert result["retired_snapshots"] == ["R", "X"]
        assert result["reclaimed"] == ["files/data_x_only.parquet"]
    finally:
        if reader.poll() is None:
            reader.kill()


def test_reader_after_plan_apply_succeeds_after_exit(tmp_path, table):
    plan_path = _make_plan(table)
    apply_out = tmp_path / "apply.json"
    proc = _run(["apply", "--table", str(table), "--plan", str(plan_path),
                 "--out", str(apply_out)])
    result = _read(apply_out)
    assert result["retired_snapshots"] == ["R", "X"]
    assert result["reclaimed"] == ["files/data_x_only.parquet"]
    assert not (table / "files" / "data_x_only.parquet").exists()
    # retained snapshot still reads in a fresh process
    rows_out = tmp_path / "rows.json"
    _run(["reader", "--table", str(table), "--snapshot", "L",
          "--out", str(rows_out)])
    rows = [(r["file_id"], r["row_position"], r["row_uid"])
            for r in _read(rows_out)["rows"]]
    assert rows == EXPECTED["rows"]["L"]
    # retired snapshot cannot be read from another process either
    refused = _run(["reader", "--table", str(table), "--snapshot", "R",
                    "--out", str(tmp_path / "r2.json")], check=False)
    assert refused.returncode != 0
    assert "SnapshotRetiredError" in refused.stderr


# ---------------------------------------------------------------------------
# two-sided compare guard
# ---------------------------------------------------------------------------


def test_compare_guard_protects_both_sides(tmp_path, table):
    # R is on the plan's retirement list while M2 is retained: one compare
    # guard over both sides must still block the whole retirement.
    plan_path = _make_plan(table)
    cmp_out = tmp_path / "cmp"
    ready = tmp_path / "cmp.ready"
    cont = tmp_path / "cmp.cont"
    proc = subprocess.Popen(
        [sys.executable, str(WORKER), "compare", "--table", str(table),
         "--source", "R", "--target", "M2", "--key-fields", "2",
         "--out", str(cmp_out),
         "--ready-flag", str(ready), "--continue-flag", str(cont)],
    )
    try:
        # one guard covering both sides is published before any file opens
        _wait_flag(ready)
        live = LifecycleManager(table).status()["live_readers"]
        assert live and set(live[0]["snapshot_ids"]) == {"R", "M2"}
        blocked = _run(["apply", "--table", str(table),
                        "--plan", str(plan_path)], check=False)
        assert blocked.returncode != 0
        assert "PlanBlockedError" in blocked.stderr
        # no partial retirement while the compare holds its guard
        assert LifecycleManager(table).state().retired == []
    finally:
        cont.write_text("go")
        proc.wait(timeout=30)
    assert proc.returncode == 0
    payload = _read(cmp_out)
    # R contributes three visible rows; compare completed despite the block
    assert payload["stats"]["source_visible_rows"] == 3
    assert Path(payload["output"]).exists()
    # after the guard releases, apply proceeds with its original scope
    _run(["apply", "--table", str(table), "--plan", str(plan_path),
          "--out", str(tmp_path / "apply.json")])
    assert not (table / "files" / "data_x_only.parquet").exists()
    # a compare touching an already-retired side now fails before reading
    refused = _run(["compare", "--table", str(table),
                    "--source", "R", "--target", "M2",
                    "--out", str(tmp_path / "cmp2")], check=False)
    assert refused.returncode != 0
    assert "SnapshotRetiredError" in refused.stderr


# ---------------------------------------------------------------------------
# killed reader: no permanent retention
# ---------------------------------------------------------------------------


def test_killed_reader_token_is_swept(tmp_path, table):
    ready = tmp_path / "ready"
    cont = tmp_path / "cont"
    reader = subprocess.Popen(
        [sys.executable, str(WORKER), "reader", "--table", str(table),
         "--snapshot", "R", "--batch-size", "1",
         "--ready-flag", str(ready), "--continue-flag", str(cont),
         "--out", str(tmp_path / "reader.out")],
    )
    _wait_flag(ready)
    assert LifecycleManager(table).status()["live_readers"]
    reader.kill()
    reader.wait(timeout=10)
    assert reader.returncode != 0

    plan_path = _make_plan(table)
    out = tmp_path / "apply.json"
    # the stale token is swept; retirement is not permanently blocked
    _run(["apply", "--table", str(table), "--plan", str(plan_path),
          "--out", str(out)])
    result = _read(out)
    assert set(result["retired_snapshots"]) == {"R", "X"}
    assert LifecycleManager(table).status()["live_readers"] == []


# ---------------------------------------------------------------------------
# kill at the three reclamation boundaries + cross-process recovery
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit",
                                      "after_unlink"])
def test_apply_killed_at_each_boundary_resumes(tmp_path, table, boundary):
    plan_path = _make_plan(table)
    target = table / "files" / "data_x_only.parquet"

    killed = _run(
        ["apply", "--table", str(table), "--plan", str(plan_path),
         "--crash", boundary, "--out", str(tmp_path / "killed.json")],
        check=False,
    )
    assert killed.returncode == 9

    state = LifecycleManager(table).state()
    if boundary == "before_commit":
        # nothing durable happened: no retirement, no ledger, file intact
        assert state.retired == []
        assert state.cleanup is None
        assert target.exists()
        # simply re-apply the same still-current plan
        out = tmp_path / "reapply.json"
        _run(["apply", "--table", str(table), "--plan", str(plan_path),
              "--out", str(out)])
        result = _read(out)
    else:
        # retirement boundary durable; ledger open; recovery continues the
        # SAME batch without re-deriving a wider one
        assert set(state.retired) == {"R", "X"}
        assert state.cleanup is not None
        assert state.cleanup["batch_id"]
        assert [it["path"] for it in state.cleanup["items"]] == [
            "files/data_x_only.parquet"
        ]
        # retained snapshots remain readable while recovery is pending
        rows = collect(Scanner(table).scan("L", 1, (1,)))
        assert len(rows) == 6
        out = tmp_path / "resume.json"
        resume_proc = _run(
            ["resume", "--table", str(table), "--out", str(out)],
            check=False,
        )
        result = _read(out)
        if boundary == "after_unlink":
            # predecessor already unlinked but died before recording: resume
            # detects the missing file and marks it reclaimed
            assert resume_proc.returncode == 0
        assert result["reclaimed"] == ["files/data_x_only.parquet"]

    assert result["retired_snapshots"] == ["R", "X"]
    assert not target.exists()
    final = LifecycleManager(table).status()
    assert final["cleanup"] is None
    assert set(final["snapshots"]["retired"]) == {"R", "X"}
    # shared/protected paths were never part of the batch
    assert (table / "files" / "r1.parquet").exists()
    assert (table / "files" / "dl_shared.parquet").exists()


# ---------------------------------------------------------------------------
# swapped candidate file across processes
# ---------------------------------------------------------------------------


def test_swapped_file_blocks_then_restored_resume_succeeds(tmp_path, table):
    plan_path = _make_plan(table)
    target = table / "files" / "data_x_only.parquet"
    os.chmod(target, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
    target.write_bytes(b"foreign bytes placed by an operator mistake")

    blocked = _run(["apply", "--table", str(table), "--plan", str(plan_path),
                    "--out", str(tmp_path / "blocked.json")], check=False)
    # pre-commit verification blocks before any retirement
    assert blocked.returncode != 0
    assert "PlanBlockedError" in blocked.stderr
    assert LifecycleManager(table).state().retired == []

    # restore the exact original bytes from a freshly built fixture
    fresh = tmp_path / "fresh"
    build_lifecycle_table(fresh)
    target.write_bytes((fresh / "files" / "data_x_only.parquet").read_bytes())
    os.chmod(target, 0o444)
    out = tmp_path / "apply.json"
    _run(["apply", "--table", str(table), "--plan", str(plan_path),
          "--out", str(out)])
    assert not target.exists()


# ---------------------------------------------------------------------------
# concurrent duplicate apply from two processes
# ---------------------------------------------------------------------------


def test_concurrent_duplicate_apply(tmp_path, table):
    plan_path = _make_plan(table)
    out_a = tmp_path / "a.json"
    out_b = tmp_path / "b.json"
    pa = subprocess.Popen(
        [sys.executable, str(WORKER), "apply", "--table", str(table),
         "--plan", str(plan_path), "--out", str(out_a)])
    pb = subprocess.Popen(
        [sys.executable, str(WORKER), "apply", "--table", str(table),
         "--plan", str(plan_path), "--out", str(out_b)])
    rc_a, rc_b = pa.wait(timeout=60), pb.wait(timeout=60)

    # exactly one apply may perform the commit; the other must fail safely
    codes = sorted([rc_a, rc_b])
    assert 0 in codes
    assert any(rc != 0 for rc in (rc_a, rc_b))
    ok_out = out_a if rc_a == 0 else out_b
    bad_out = out_b if rc_a == 0 else out_a
    result = _read(ok_out)
    assert result["retired_snapshots"] == ["R", "X"]
    assert result["reclaimed"] == ["files/data_x_only.parquet"]
    # the loser never deleted anything; its output file was never produced
    assert not bad_out.exists()
    target = table / "files" / "data_x_only.parquet"
    assert not target.exists()
    # and a follow-up plan/apply has nothing left to do
    idle = LifecycleManager(table).plan()
    assert idle.to_retire == [] and idle.deletions == []


# ---------------------------------------------------------------------------
# CLI smoke: end-to-end command compatibility
# ---------------------------------------------------------------------------


def test_cli_lifecycle_commands(tmp_path, table):
    repo = Path(__file__).resolve().parent.parent
    cli = [sys.executable, "-m", "lakescan"]
    env = dict(os.environ, PYTHONPATH=str(repo))
    status = subprocess.run(cli + ["lifecycle-status", "--table", str(table)],
                            capture_output=True, text=True, env=env, cwd=repo)
    assert status.returncode == 0
    payload = json.loads(status.stdout.strip())
    assert payload["managed"] and payload["snapshots"]["total"] == 5

    plan_out = tmp_path / "plan.json"
    mk = subprocess.run(cli + ["make-plan", "--table", str(table),
                               "--out", str(plan_out)],
                        capture_output=True, text=True, env=env)
    assert mk.returncode == 0
    ap = subprocess.run(cli + ["apply-plan", "--table", str(table),
                               "--plan", str(plan_out)],
                        capture_output=True, text=True, env=env)
    assert ap.returncode == 0
    applied = json.loads(ap.stdout.strip())
    assert applied["retired_snapshots"] == ["R", "X"]
    rs = subprocess.run(cli + ["resume", "--table", str(table)],
                        capture_output=True, text=True, env=env)
    assert rs.returncode == 0
    # retired snapshot scan fails via the CLI as well
    scan = subprocess.run(cli + ["scan", "--table", str(table),
                                 "--snapshot", "R", "--schema", "1",
                                 "--project", "1"],
                          capture_output=True, text=True, env=env)
    assert scan.returncode == 2
    assert "SnapshotRetiredError" in scan.stderr
