"""Cross-process acceptance for the attribution task's read lifecycle.

Scenarios:

* both input tables are managed; a worker acquires both read guards before
  opening any input file and parks holding them -- a concurrent reclamation
  commit is blocked while the guards live, then succeeds after release;
* a read failure injected halfway through extraction (after the fact side
  was fully streamed) publishes nothing, leaves no staging directory and
  releases every guard token;
* same-table two-snapshot case: one guard token covers both snapshots.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from lakescan import FactAttributor, LifecycleManager, Scanner, SnapshotRetiredError, collect
from lakescan.attribution_fixtures import (
    build_attribution_fixtures,
    main_mapping,
)

WORKER = Path(__file__).resolve().parent / "attribution_worker.py"


def _wait_flag(path: Path, timeout=30):
    deadline = time.time() + timeout
    while not path.exists():
        if time.time() > deadline:
            raise AssertionError(f"timed out waiting for {path}")
        time.sleep(0.01)


def _onboard(root: Path):
    mgr = LifecycleManager(root)
    mgr.onboard()
    return mgr


def _managed_fixtures(base: Path):
    info = build_attribution_fixtures(base)
    # onboard each table and retain exactly the snapshots the attribution
    # mapping uses; every other snapshot becomes reclaimable.
    _onboard(Path(info["fact_root"]))
    LifecycleManager(info["fact_root"]).set_references({
        "branches": {"main": {
            "target": info["fact_snapshot"], "retain_generations": 1}},
        "bookmarks": {},
    })
    _onboard(Path(info["dim_root"]))
    LifecycleManager(info["dim_root"]).set_references({
        "branches": {"main": {
            "target": info["dim_snapshot"], "retain_generations": 1}},
        "bookmarks": {},
    })
    return info


def test_guards_block_concurrent_reclaim_then_release(tmp_path):
    info = _managed_fixtures(tmp_path / "fx")
    fact_root = Path(info["fact_root"])
    dim_root = Path(info["dim_root"])

    # Build the retiring plan BEFORE the worker starts: the new branch target
    # leaves the currently-used fact snapshot unreferenced, so this plan names
    # it.  Its commit must later be gated by the worker's live read guard.
    mgr_fact = LifecycleManager(fact_root)
    mgr_fact.set_references({
        "branches": {"main": {
            "target": info["fact_snapshot_renamed"],
            "retain_generations": 1}},
        "bookmarks": {},
    })
    plan = mgr_fact.plan()
    assert info["fact_snapshot"] in plan.to_retire

    out = tmp_path / "attributed"
    cfg = tmp_path / "mapping.json"
    cfg.write_text(json.dumps(main_mapping(info, str(out))))
    ready, cont = tmp_path / "ready", tmp_path / "cont"
    result_file = tmp_path / "result.json"

    proc = subprocess.Popen([
        sys.executable, str(WORKER), "attribute",
        "--config", str(cfg), "--out", str(result_file),
        "--ready-flag", str(ready), "--continue-flag", str(cont),
    ])
    try:
        _wait_flag(ready)
        # both tables show a live reader while the worker parks
        for root in (fact_root, dim_root):
            live = LifecycleManager(root).status()["live_readers"]
            assert live, f"no guard visible on {root}"
        # the pre-built reclamation commit is refused while the guard is held
        blocked = subprocess.run(
            [sys.executable, "-m", "lakescan", "apply-plan",
             "--table", str(fact_root), "--plan", plan.path],
            capture_output=True, text=True,
            env=dict(os.environ, PYTHONPATH=str(Path(__file__).parent.parent)),
        )
        assert blocked.returncode != 0
        assert "PlanBlockedError" in blocked.stderr
        # nothing was published while parked
        assert not out.exists()
    finally:
        cont.write_text("go")
        proc.wait(timeout=60)
    assert proc.returncode == 0
    payload = json.loads(result_file.read_text())
    assert payload["stats"]["total_facts"] > 0
    assert (out / "attribution.parquet").is_file()
    # guards released after the worker exits
    for root in (fact_root, dim_root):
        assert LifecycleManager(root).status()["live_readers"] == []

    # once the reader is gone, the same plan applies without changing scope
    apply = subprocess.run(
        [sys.executable, "-m", "lakescan", "apply-plan",
         "--table", str(fact_root), "--plan", plan.path],
        capture_output=True, text=True,
        env=dict(os.environ, PYTHONPATH=str(Path(__file__).parent.parent)),
    )
    assert apply.returncode == 0, apply.stderr
    assert LifecycleManager(fact_root).status()["live_readers"] == []
    # the now-retired fact snapshot can no longer be read
    from lakescan import Scanner, SnapshotRetiredError
    with pytest.raises(SnapshotRetiredError):
        collect(Scanner(fact_root).scan(
            info["fact_snapshot"], 100, (1,)))


def test_halfway_read_failure_publishes_nothing_and_releases_guards(tmp_path):
    info = _managed_fixtures(tmp_path / "fx2")
    fact_root = Path(info["fact_root"])
    dim_root = Path(info["dim_root"])
    out = tmp_path / "attributed"
    cfg = tmp_path / "mapping.json"
    cfg.write_text(json.dumps(main_mapping(info, str(out))))

    env = dict(os.environ,
               LAKESCAN_ATTRIB_INJECT_FAIL="after_facts",
               PYTHONPATH=str(Path(__file__).parent.parent))
    proc = subprocess.run(
        [sys.executable, "-m", "lakescan", "attribute",
         "--config", str(cfg)],
        capture_output=True, text=True, env=env,
    )
    assert proc.returncode != 0
    assert "AttributionError" in proc.stderr
    # no partial result, no staging leftovers
    assert not out.exists()
    assert [p for p in tmp_path.rglob(".attr-stage-*")] == []
    # every guard token was released despite the mid-run failure
    for root in (fact_root, dim_root):
        assert LifecycleManager(root).status()["live_readers"] == []


def test_same_table_single_guard_covers_both_snapshots(tmp_path):
    """When both roles read one managed table, exactly one guard token names
    both snapshots and the attribution result is still correct end to end."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from lakescan.attribution_fixtures import (
        DIM_ARROW, DIM_RECORDS, DIM_SCHEMAS, FACT_ARROW, FACT_RECORDS,
        FACT_SCHEMAS, _dim_columns, _entry, _fact_columns, _write,
        _write_contract,
    )
    root = tmp_path / "one"
    (root / "files").mkdir(parents=True)
    fr, fs = _write(root, "F", _fact_columns(FACT_RECORDS), FACT_ARROW)
    dr, ds = _write(root, "R", _dim_columns(DIM_RECORDS), DIM_ARROW)
    _write_contract(root, "one", FACT_SCHEMAS + [
        s for s in DIM_SCHEMAS if s["schema_id"] == 200], [
        {"snapshot_id": "fs", "parent": None,
         "active_files": [_entry("F", fr, fs, 100, 5)]},
        {"snapshot_id": "ds", "parent": None,
         "active_files": [_entry("R", dr, ds, 200, 5)]},
    ])
    LifecycleManager(root).onboard()
    LifecycleManager(root).set_references({
        "branches": {"f": {"target": "fs", "retain_generations": 1},
                     "d": {"target": "ds", "retain_generations": 1}},
        "bookmarks": {},
    })
    mapping = {
        "fact": {"table": str(root), "snapshot": "fs", "schema": 100,
                 "roles": {"fact_id": 1, "fact_entity_key": 2,
                           "fact_business_time": 3,
                           "fact_knowledge_cutoff": 4},
                 "fact_payload": [{"field_id": 5, "name": "fact_amount"},
                                  {"field_id": 6, "name": "fact_note"}]},
        "dimension": {"table": str(root), "snapshot": "ds", "schema": 200,
                      "roles": {"dim_entity_key": 11, "dim_rule_id": 12,
                                "dim_published_at": 13, "dim_operation": 14,
                                "dim_valid_from": 15, "dim_valid_to": 16,
                                "dim_priority": 17},
                      "dim_payload": [
                          {"field_id": 18, "name": "settlement_group"},
                          {"field_id": 19, "name": "rate_bps"}]},
        "output_dir": str(tmp_path / "out"),
    }
    result = FactAttributor(mapping).attribute()
    assert result.stats.total == len(FACT_RECORDS)
    live = LifecycleManager(root).status()["live_readers"]
    assert live == []  # released after completion
    # and while a guard is explicitly held, one token covers both snapshots
    from lakescan.lifecycle.reader import read_preflight
    guard = read_preflight(root, ["fs", "ds"], reason="test")
    try:
        live = LifecycleManager(root).status()["live_readers"]
        assert len(live) == 1
        assert set(live[0]["snapshot_ids"]) == {"fs", "ds"}
    finally:
        guard.close()
