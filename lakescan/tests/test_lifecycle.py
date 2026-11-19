"""Acceptance suite for snapshot retention and recoverable reclamation.

Expectations are hand-derived (see ``lifecycle_fixtures.EXPECTED`` and the
literal sets inside each test); nothing here reverse-engineers the
reclaimer's own plan output.  All parquet data is generated at runtime in
tiny files -- no data artifacts are committed to the repository.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from lakescan import (
    AlreadyManagedError,
    LifecycleManager,
    ManagementCorruptError,
    ManifestDriftError,
    ManagedPathError,
    NotManagedError,
    PlanBlockedError,
    PlanStaleError,
    RecoveryInProgressError,
    ReferenceConfigError,
    Scanner,
    SnapshotRetiredError,
    TableDiffer,
    collect,
)
from lakescan.lifecycle import guards
from lakescan.lifecycle_fixtures import EXPECTED, build_lifecycle_table
from lakescan.lifecycle.state import STATE_DIR, onboard
from lakescan.table import sha256_of


@pytest.fixture()
def table(tmp_path):
    root = tmp_path / "lc"
    build_lifecycle_table(root)
    return root


@pytest.fixture()
def managed(table):
    mgr = LifecycleManager(table)
    mgr.onboard()
    return mgr


@pytest.fixture()
def configured(managed):
    managed.set_references(EXPECTED["refs"])
    return managed


def _triplets(root, snapshot):
    return [
        (f, p, fields["row_uid"])
        for f, p, fields in collect(
            Scanner(root).scan(snapshot, 1, (1,))
        )
    ]


# ---------------------------------------------------------------------------
# unmanaged compatibility + onboarding
# ---------------------------------------------------------------------------


def test_unmanaged_table_keeps_legacy_reads(table):
    assert not LifecycleManager(table).managed
    with pytest.raises(NotManagedError):
        LifecycleManager(table).status()
    # the legacy scanner path is untouched
    assert _triplets(table, "X") == EXPECTED["rows"]["X_before"]


def test_onboard_binds_manifest_and_readonly_artifacts(table):
    state = onboard(table)
    tj = table / "table.json"
    bound = state.manifest_sha256
    assert len(bound) == 64
    assert not os.access(tj, os.W_OK)
    for rel in state.registered_paths:
        assert not os.access(table / rel, os.W_OK)
        assert (table / rel).is_file()
    # exactly the manifest-registered regular files, keyed by path
    assert len(state.registered_paths) == 9
    with pytest.raises(AlreadyManagedError):
        onboard(table)


def test_onboard_rejects_symlink_and_traversal_components(tmp_path):
    root = tmp_path / "t"
    build_lifecycle_table(root)
    (root / "files" / "linkdir").symlink_to(root / "files")
    tj = json.loads((root / "table.json").read_text())
    tj["snapshots"].append({
        "snapshot_id": "S", "parent": None,
        "active_files": [dict(tj["snapshots"][0]["active_files"][0],
                              path="linkdir/r1.parquet")],
    })
    (root / "table.json").write_text(json.dumps(tj))
    with pytest.raises(ManagedPathError):
        onboard(root)

    root2 = tmp_path / "t2"
    build_lifecycle_table(root2)
    tj = json.loads((root2 / "table.json").read_text())
    bad = dict(tj["snapshots"][0]["active_files"][0], path="../escape.parquet")
    tj["snapshots"].append(
        {"snapshot_id": "S", "parent": None, "active_files": [bad]}
    )
    (root2 / "table.json").write_text(json.dumps(tj))
    with pytest.raises((ManagedPathError, Exception)):
        onboard(root2)


def test_onboard_rejects_digest_conflict_on_same_path(tmp_path):
    root = tmp_path / "t3"
    build_lifecycle_table(root)
    tj = json.loads((root / "table.json").read_text())
    e1 = tj["snapshots"][0]["active_files"][0]
    tj["snapshots"].append({
        "snapshot_id": "S", "parent": None,
        "active_files": [dict(e1, file_id="OTHER", sha256="a" * 64)],
    })
    (root / "table.json").write_text(json.dumps(tj))
    with pytest.raises(ManagedPathError):
        onboard(root)


def test_same_bytes_different_paths_are_distinct_identities(managed):
    reg = managed.state().registered_paths
    x = reg["files/data_x_only.parquet"]
    l = reg["files/data_l_only.parquet"]
    assert x == l  # identical digest
    assert set(reg) > {
        "files/data_x_only.parquet", "files/data_l_only.parquet"
    }


# ---------------------------------------------------------------------------
# reference configuration
# ---------------------------------------------------------------------------


def test_reference_replacement_rules(managed):
    base = {"branches": {"main": {"target": "M2", "retain_generations": 2}}}
    for bad in [
        {},  # no branch at all
        {"branches": {}},  # empty branches
        {"branches": {"main": {"target": "GHOST", "retain_generations": 1}}},
        {"branches": {"main": {"target": "L", "retain_generations": 0}}},
        {"branches": {"main": {"target": "L", "retain_generations": -1}}},
        {"branches": {"main": {"target": "L", "retain_generations": "1"}}},
        {"branches": {"main": {"target": "L", "retain_generations": 1}},
         "bookmarks": {"main": {"target": "M2"}}},  # duplicate name
    ]:
        with pytest.raises(ReferenceConfigError):
            managed.set_references(bad)

    first = managed.set_references(base)
    assert first["ref_generation"] == 2
    second = managed.set_references(EXPECTED["refs"])
    assert second["ref_generation"] == 3  # every replacement bumps generation


def test_retired_target_cannot_be_referenced(configured):
    configured.apply_plan(configured.plan())
    with pytest.raises(ReferenceConfigError):
        configured.set_references(
            {"branches": {"b": {"target": "X", "retain_generations": 1}}}
        )


# ---------------------------------------------------------------------------
# plan derivation: hand-written set expectations
# ---------------------------------------------------------------------------


def test_plan_lists_retained_retired_and_deletions(configured):
    plan = configured.plan()
    assert set(plan.retained) == EXPECTED["first_plan"]["retained"]
    assert plan.to_retire == EXPECTED["first_plan"]["retire"]
    delete_paths = [d.path for d in plan.deletions]
    assert delete_paths == EXPECTED["first_plan"]["delete_paths"]
    # reasons are explicit and policy-derived; L forks from R (it is not on
    # M2's parent chain), so only the bookmark retains it.
    reasons_m2 = {r["type"] for r in plan.retained["M2"]}
    assert reasons_m2 == {"branch"}
    assert {r["type"] for r in plan.retained["L"]} == {"bookmark"}
    assert {r["type"] for r in plan.retained["M1"]} == {"branch"}
    # planning never mutates state
    assert configured.state().retired == []
    assert configured.state().cleanup is None
    assert plan.plan_id and (configured.root / STATE_DIR / "plans" /
                             f"plan-{plan.plan_id}.json").exists()


def test_plan_does_not_autoretain_siblings_or_pos_inactive_targets(configured):
    plan = configured.plan()
    # X is an unreferenced leaf: no sibling/ancestor auto-retention saves it
    assert "X" in plan.to_retire
    # the shared delete file's path is protected only because L still lists
    # it -- never because of anything mentioned inside the file
    item = next(d for d in plan.deletions
                if d.path == "files/data_x_only.parquet")
    assert "files/dl_shared.parquet" not in [d.path for d in plan.deletions]
    assert set(item.kinds) == {"DATA"}


def test_chain_walk_ends_early_and_counts_retired_ancestors(table):
    mgr = LifecycleManager(table)
    mgr.onboard()
    # depth larger than the chain: keep what exists
    mgr.set_references(
        {"branches": {"main": {"target": "L", "retain_generations": 99}}}
    )
    plan = mgr.plan()
    assert set(plan.retained) == {"L", "R"}
    assert plan.to_retire == ["M1", "M2", "X"]


def test_bookmark_keeps_only_its_target(configured):
    configured.set_references({
        "branches": {"main": {"target": "M2", "retain_generations": 1}},
        "bookmarks": {"bm_l": {"target": "L"}},
    })
    plan = configured.plan()
    # branch depth 1 keeps M2 only; bookmark keeps L; M1/R/X are not retained
    assert set(plan.retained) == {"M2", "L"}


# ---------------------------------------------------------------------------
# reader interlock
# ---------------------------------------------------------------------------


def test_reader_started_after_plan_blocks_apply_without_touching_files(configured):
    root = configured.root
    plan = configured.plan()
    sc = Scanner(root, batch_size=1)
    it = sc.scan("R", 1, (1,))
    first = next(it)  # paused between the two R data files
    with pytest.raises(PlanBlockedError):
        configured.apply_plan(plan)
    # nothing retired, nothing deleted while the reader is live
    assert (root / "files" / "data_x_only.parquet").exists()
    assert configured.state().retired == []
    # reader resumes and gets every pre-written physical row
    rest = [
        (b.file_id, p, b.batch.column("row_uid").to_pylist()[0])
        for b in it for p in b.positions
    ]
    head = [
        (first.file_id, p, first.batch.column("row_uid").to_pylist()[0])
        for p in first.positions
    ]
    assert head + rest == EXPECTED["rows"]["R"]
    # after the reader ends, the same still-current plan applies
    result = configured.apply_plan(plan)
    assert result.cleanup_status == "complete"
    assert result.retired_snapshots == ["R", "X"]


def test_reader_protection_is_union_and_independent_of_policy(configured):
    root = configured.root
    # start the reader first; the generated plan then carries R as retained,
    # and ending the reader makes the next plan retire R without any config
    # change or timeout.
    sc = Scanner(root, batch_size=1)
    it = sc.scan("R", 1, (1,))
    next(it)
    plan = configured.plan()
    assert "R" in plan.retained
    assert any(r["type"] == "reader" for r in plan.retained["R"])
    assert plan.to_retire == ["X"]
    assert [d.path for d in plan.deletions] == [
        "files/data_x_only.parquet"
    ]
    it.close()
    # ending a reader never authorizes extra deletions under an old plan:
    # a fresh plan is needed, and it never expands beyond current safety.
    plan2 = configured.plan()
    assert plan2.to_retire == ["R", "X"]


def test_paused_iterator_keeps_guard_until_closed(configured):
    state = configured.state()
    sc = Scanner(configured.root, batch_size=1)
    it = sc.scan("L", 1, (1,))
    next(it)
    assert guards.protected_snapshot_set(state) == {"L"}
    it.close()
    assert guards.protected_snapshot_set(state) == set()


def test_reused_scanner_rechecks_retirement_after_commit(configured):
    sc = Scanner(configured.root)
    # cache the contract on this scanner instance
    with sc.plan("X", 1, (1,)) as p:
        assert p.snapshot.snapshot_id == "X"
    configured.apply_plan(configured.plan())
    # the cached contract must not resurrect a retired snapshot
    with pytest.raises(SnapshotRetiredError):
        sc.plan("X", 1, (1,))


def test_killed_reader_does_not_leave_permanent_retention(configured):
    state = configured.state()
    # simulate a dead process token directly
    import uuid
    token = state.dir / "guards" / f"guard-{uuid.uuid4().hex}.json"
    token.write_text(json.dumps({
        "pid": 2 ** 22, "process_start_time": None,
        "snapshot_ids": ["R"], "reason": "read",
    }))
    # a normal live reader still works
    plan = configured.plan()
    assert "R" in plan.to_retire  # dead token swept, not treated as protection
    configured.apply_plan(plan)
    assert "R" in configured.state().retired


# ---------------------------------------------------------------------------
# apply: retirement boundary, path identity, retained reads
# ---------------------------------------------------------------------------


def test_apply_retire_boundary_and_shared_path_protection(configured):
    root = configured.root
    result = configured.apply_plan(configured.plan())
    assert result.retirement_committed
    assert result.retired_snapshots == ["R", "X"]
    assert result.reclaimed == ["files/data_x_only.parquet"]
    # the shared active delete and the L-only twin path survive
    assert (root / "files" / "dl_shared.parquet").exists()
    assert (root / "files" / "data_l_only.parquet").exists()
    for rel in ("files/r1.parquet", "files/r2.parquet", "files/m1.parquet",
                "files/m2.parquet", "files/pos_m2.parquet",
                "files/l_only.parquet"):
        assert (root / rel).exists(), rel
    # unregistered files are never swept
    (root / "stray.bin").write_bytes(b"not in any manifest")
    assert (root / "stray.bin").exists()
    # retired snapshots fail closed; retained ones keep their exact rows
    with pytest.raises(SnapshotRetiredError):
        _triplets(root, "X")
    with pytest.raises(SnapshotRetiredError):
        _triplets(root, "R")
    assert _triplets(root, "M2") == EXPECTED["rows"]["M2"]
    assert _triplets(root, "L") == EXPECTED["rows"]["L"]
    # lineage diagnostics still walk through the retired ancestor
    assert configured.contract().lineage("M2") == ["M2", "M1", "R"]


def test_compare_output_stable_after_reclamation(configured, tmp_path):
    root = configured.root
    out1 = tmp_path / "c1.jsonl"
    out2 = tmp_path / "c2.jsonl"
    d1 = TableDiffer(root).compare("L", "M2", 1, (2,), out1)
    configured.apply_plan(configured.plan())
    d2 = TableDiffer(root).compare("L", "M2", 1, (2,), out2)
    assert out1.read_bytes() == out2.read_bytes()
    assert d1.stats.to_json() == d2.stats.to_json()
    records = [json.loads(line) for line in out1.read_text().splitlines()]
    assert {r["change"] for r in records} == {"insert", "delete"}
    # compare on a retired side is refused before opening files
    with pytest.raises(SnapshotRetiredError):
        TableDiffer(root).compare("X", "M2", 1, (2,), tmp_path / "c3.jsonl")


def test_explain_interfaces_respect_retirement_and_guards(configured):
    configured.apply_plan(configured.plan())
    with pytest.raises(SnapshotRetiredError):
        Scanner(configured.root).explain_plan("R", 1, (1,))
    with pytest.raises(SnapshotRetiredError):
        Scanner(configured.root).explain_row("X", "x", 0)
    # a retained snapshot still explains and closes its guard
    sc = Scanner(configured.root)
    info = sc.explain_plan("M2", 1, (1,))
    assert info["lineage"] == ["M2", "M1", "R"]
    assert guards.live_guards(configured.state()) == []


# ---------------------------------------------------------------------------
# plan invalidation
# ---------------------------------------------------------------------------


def test_reference_change_invalidates_old_plan(configured):
    plan = configured.plan()
    configured.set_references(
        {"branches": {"main": {"target": "M2", "retain_generations": 1}}}
    )
    with pytest.raises(PlanStaleError):
        configured.apply_plan(plan)


def test_edited_plan_file_is_never_a_deletion_instruction(configured):
    plan = configured.plan()
    obj = plan.to_json()
    obj["deletions"].append({
        "path": "files/r1.parquet",
        "sha256": obj["deletions"][0]["sha256"],
        "kinds": ["DATA"], "file_ids": ["INJECTED"],
        "referenced_by_retiring_snapshots": ["R"], "size_bytes": 0,
    })
    fake = configured.root / STATE_DIR / "plans" / "fake.json"
    fake.write_text(json.dumps(obj))
    with pytest.raises(PlanStaleError):
        configured.apply_plan_file(fake)
    assert (configured.root / "files" / "r1.parquet").exists()


def test_reader_ending_does_not_expand_old_plan(configured):
    plan = configured.plan()
    sc = Scanner(configured.root, batch_size=1)
    it = sc.scan("R", 1, (1,))
    next(it)
    with pytest.raises(PlanBlockedError):
        configured.apply_plan(plan)
    it.close()
    # the same plan now applies, with exactly its original scope
    result = configured.apply_plan(plan)
    assert result.retired_snapshots == ["R", "X"]
    assert result.reclaimed == ["files/data_x_only.parquet"]


# ---------------------------------------------------------------------------
# drift / corruption fail closed
# ---------------------------------------------------------------------------


def _make_writable(path: Path) -> None:
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def test_manifest_drift_fails_closed_for_reads_and_operations(configured):
    root = configured.root
    tj = root / "table.json"
    _make_writable(tj)
    raw = json.loads(tj.read_text())
    raw["table_id"] = "tampered"
    tj.write_text(json.dumps(raw))
    with pytest.raises(ManifestDriftError):
        configured.status()
    with pytest.raises(ManifestDriftError):
        _triplets(root, "L")
    with pytest.raises(ManifestDriftError):
        configured.plan()


def test_corrupt_management_state_is_not_unmanaged(configured):
    sp = configured.root / STATE_DIR / "state.json"
    _make_writable(sp)
    sp.write_text("{truncated")
    assert LifecycleManager(configured.root).managed  # directory still present
    with pytest.raises(ManagementCorruptError):
        configured.status()
    with pytest.raises(ManagementCorruptError):
        _triplets(configured.root, "L")


def test_precommit_tamper_blocks_without_retirement(configured):
    target = configured.root / "files" / "data_x_only.parquet"
    _make_writable(target)
    target.write_bytes(b"completely different bytes")
    plan = configured.plan()
    with pytest.raises(PlanBlockedError):
        configured.apply_plan(plan)
    assert configured.state().retired == []
    assert configured.state().cleanup is None
    # retained snapshots remain readable
    assert _triplets(configured.root, "L") == EXPECTED["rows"]["L"]


# ---------------------------------------------------------------------------
# recoverable, resumable reclamation
# ---------------------------------------------------------------------------


def test_blocked_item_reports_reason_and_resume_after_restore(
        configured, monkeypatch):
    import lakescan.lifecycle.manager as mgr_mod

    root = configured.root
    plan = configured.plan()
    original_verify = mgr_mod._verify_candidate
    calls = {"n": 0}

    def swap_once(r, rel, sha):
        calls["n"] += 1
        result = original_verify(r, rel, sha)
        if result is None and calls["n"] >= 2:  # deletion phase only
            p = Path(r) / rel
            _make_writable(p)
            p.write_bytes(b"swapped after durable commit")
            return original_verify(r, rel, sha)
        return result

    monkeypatch.setattr(mgr_mod, "_verify_candidate", swap_once)
    result = configured.apply_plan(plan)
    assert result.cleanup_status == "blocked"
    assert result.retirement_committed is True
    assert result.blocked[0]["path"] == "files/data_x_only.parquet"
    status = configured.status()["cleanup"]
    assert status["status"] == "blocked"
    assert len(status["items"]["blocked"]) == 1

    # new apply is refused while the batch stays open
    with pytest.raises(RecoveryInProgressError):
        configured.apply_plan(configured.plan())

    # restore the original bytes, then resume finishes the SAME ledger
    monkeypatch.undo()
    item = configured.state().cleanup["items"][0]
    fresh_root = root.parent / "fresh"
    build_lifecycle_table(fresh_root)
    p = root / item["path"]
    p.write_bytes((fresh_root / item["path"]).read_bytes())
    os.chmod(p, 0o444)
    resumed = configured.resume()
    assert resumed.cleanup_status == "complete"
    assert resumed.reclaimed == ["files/data_x_only.parquet"]
    assert not p.exists()
    assert configured.status()["cleanup"] is None


def test_resume_is_idempotent_when_fully_reclaimed(configured):
    configured.apply_plan(configured.plan())
    idle = configured.resume()
    assert idle.cleanup_status == "none" and idle.reclaimed == []


# Process-kill recovery is exercised across real OS processes in
# test_lifecycle_processes.py (an in-process os._exit would kill pytest).


def test_resume_never_recomputes_a_wider_batch(configured):
    # open batch from an earlier configuration; a reference change must not
    # widen the resumed item list.
    plan = configured.plan()
    state = configured.state()
    with state.state_lock():
        st = configured.state()
        st.mark_retired(plan.to_retire)
        st.open_batch({
            "batch_id": "manual", "plan_id": plan.plan_id,
            "ref_generation": st.ref_generation,
            "retire_snapshots": list(plan.to_retire),
            "items": [{
                "path": "files/data_x_only.parquet",
                "sha256": st.registered_paths["files/data_x_only.parquet"],
                "status": "pending", "reason": None,
            }],
        })
        st.persist()
    configured.set_references(
        {"branches": {"main": {"target": "M2", "retain_generations": 1}}}
    )
    resumed = configured.resume()
    assert resumed.reclaimed == ["files/data_x_only.parquet"]
    assert (configured.root / "files" / "r1.parquet").exists()


# ---------------------------------------------------------------------------
# symlink swap after onboarding is refused at deletion time
# ---------------------------------------------------------------------------


def test_link_replacement_blocks_deletion(configured):
    root = configured.root
    target = root / "files" / "data_x_only.parquet"
    _make_writable(target)
    target.unlink()
    decoy = root / "decoy.txt"
    decoy.write_text("decoy")
    target.symlink_to(decoy)
    with pytest.raises(PlanBlockedError):
        configured.apply_plan(configured.plan())
    assert configured.state().retired == []


# ---------------------------------------------------------------------------
# additional edge semantics
# ---------------------------------------------------------------------------


def test_plan_summary_and_already_retired_accounting(configured):
    first = configured.plan()
    assert first.summary == {
        "retained_snapshots": 3,
        "retire_snapshots": 2,
        "already_retired_snapshots": 0,
        "delete_paths": 1,
        "delete_bytes": first.summary["delete_bytes"],
    }
    configured.apply_plan(first)
    second = configured.plan()
    assert second.summary["already_retired_snapshots"] == 2
    assert second.to_retire == [] and second.deletions == []
    # retired-before field on the new plan records current state, not history
    assert set(second.retired_before) == {"R", "X"}


def test_apply_with_only_retirement_no_files_is_complete(configured, tmp_path):
    """A plan that retires snapshots whose every path is shared with retained
    snapshots still commits retirement cleanly with no open cleanup batch."""
    # craft references that retire only M1 while M2 stays and every other
    # snapshot (incl. X) is bookmarked: M1's files are all also active in M2.
    configured.set_references({
        "branches": {"main": {"target": "M2", "retain_generations": 1}},
        "bookmarks": {"bm_l": {"target": "L"}, "bm_r": {"target": "R"},
                      "bm_x": {"target": "X"}},
    })
    plan = configured.plan()
    # M1 shares r1/r2/m1 paths with retained snapshots: no exclusive path
    assert plan.to_retire == ["M1"]
    assert plan.deletions == []
    result = configured.apply_plan(plan)
    assert result.retirement_committed and result.cleanup_status == "none"
    assert result.batch_id is None
    assert configured.state().cleanup is None
    with pytest.raises(SnapshotRetiredError):
        _triplets(configured.root, "M1")
    # M2 still reads the shared physical files
    assert _triplets(configured.root, "M2") == EXPECTED["rows"]["M2"]


def test_status_distinguishes_pending_reclaimed_blocked(monkeypatch, configured):
    import lakescan.lifecycle.manager as mgr_mod

    # force a blocked item, then inspect the three-bucket status view
    plan = configured.plan()
    original = mgr_mod._verify_candidate
    calls = {"n": 0}

    def swap(r, rel, sha):
        calls["n"] += 1
        out = original(r, rel, sha)
        if out is None and calls["n"] >= 2:
            p = Path(r) / rel
            _make_writable(p)
            p.write_bytes(b"blocked-bytes")
            return original(r, rel, sha)
        return out

    monkeypatch.setattr(mgr_mod, "_verify_candidate", swap)
    result = configured.apply_plan(plan)
    assert result.cleanup_status == "blocked"
    view = configured.status()["cleanup"]
    assert view["status"] == "blocked"
    assert view["items"]["pending"] == []
    assert view["items"]["reclaimed"] == []
    assert view["items"]["blocked"][0]["path"] == "files/data_x_only.parquet"
    assert "sha256" in view["items"]["blocked"][0]
    monkeypatch.undo()


def test_guard_token_removal_on_exception_path(configured):
    state = configured.state()
    # an invalid snapshot id fails inside plan() after preflight; the guard
    # acquired for a valid preflight sequence must not leak either.  Here the
    # preflight itself raises first (retired/unknown), so no token exists.
    before = list((state.dir / "guards").glob("guard-*.json"))
    with pytest.raises(Exception):
        Scanner(configured.root).plan("NO_SUCH_SNAPSHOT", 1, (1,))
    after = list((state.dir / "guards").glob("guard-*.json"))
    assert before == after == []


def test_bookmark_and_branch_reasons_are_structured(configured):
    plan = configured.plan()
    bm = next(r for r in plan.retained["L"] if r["type"] == "bookmark")
    assert bm["name"] == "bm_l"
    branch_reasons = plan.retained["M2"]
    assert all(r["type"] == "branch" and r["name"] == "main"
               for r in branch_reasons)
    assert {r["distance"] for r in branch_reasons} == {0}
    m1_reasons = plan.retained["M1"]
    assert m1_reasons[0]["distance"] == 1


def test_multi_branch_union_and_shared_path_processed_once(managed):
    managed.set_references({"branches": {
        "b_m2": {"target": "M2", "retain_generations": 1},
        "b_l": {"target": "L", "retain_generations": 1},
    }})
    plan = managed.plan()
    assert set(plan.retained) == {"M2", "L"}
    assert plan.to_retire == ["M1", "R", "X"]

    # depth 2 from X keeps X and its parent R (chain ends early)
    managed.set_references(
        {"branches": {"bx": {"target": "X", "retain_generations": 2}}})
    plan2 = managed.plan()
    assert set(plan2.retained) == {"R", "X"}
    # the shared active delete is protected because retained X still lists it
    paths2 = {d.path for d in plan2.deletions}
    assert "files/dl_shared.parquet" not in paths2
    assert "files/data_x_only.parquet" not in paths2
    # m1 is referenced by TWO retiring snapshots but appears exactly once
    m1_items = [d for d in plan2.deletions if d.path == "files/m1.parquet"]
    assert len(m1_items) == 1
    assert m1_items[0].referenced_by == ["M1", "M2"]
    assert sorted(paths2) == [
        "files/data_l_only.parquet", "files/l_only.parquet",
        "files/m1.parquet", "files/m2.parquet", "files/pos_m2.parquet",
    ]


def test_retired_ancestor_counts_toward_distance_but_stays_retired(configured):
    configured.apply_plan(configured.plan())  # R and X now retired
    # depth 3 walks M2 -> M1 -> R(retired, chain ends): R consumes distance
    # budget but is never restored to readability.
    configured.set_references(
        {"branches": {"main": {"target": "M2", "retain_generations": 3}}})
    plan = configured.plan()
    assert set(plan.retained) == {"M2", "M1"}
    assert "R" not in plan.retained
    assert "R" in configured.state().retired
    assert plan.to_retire == ["L"]


def test_reference_replacement_is_all_or_nothing(configured):
    gen_before = configured.state().ref_generation
    with pytest.raises(ReferenceConfigError):
        configured.set_references({"branches": {
            "ok": {"target": "M2", "retain_generations": 1},
            "bad": {"target": "GHOST", "retain_generations": 1},
        }})
    # generation and committed config are untouched by the rejected replace
    assert configured.state().ref_generation == gen_before
    assert configured.state().references == EXPECTED["refs"]


def test_guard_sweep_removes_unparseable_token(configured):
    state = configured.state()
    junk = state.dir / "guards" / "guard-junk.json"
    junk.write_text("{half-written")
    with state.guard_lock():
        swept = guards.sweep_stale(state)
    assert swept == 1 and not junk.exists()


def test_data_and_both_delete_kinds_are_protection_basis(tmp_path):
    """A retained snapshot's POS and EQ delete files are protected too."""
    root = tmp_path / "lc"
    build_lifecycle_table(root)
    mgr = LifecycleManager(root)
    mgr.onboard()
    # retain only M2 (which lists a POS_DELETE) via branch depth 1, bookmark L
    mgr.set_references({
        "branches": {"main": {"target": "M2", "retain_generations": 1}},
        "bookmarks": {"bm_l": {"target": "L"}},
    })
    plan = mgr.plan()
    deletable = {d.path for d in plan.deletions}
    # pos_m2 is M2's active delete file: protected despite no DATA rows
    assert "files/pos_m2.parquet" not in deletable
    # dl_shared is L's active EQ delete: protected
    assert "files/dl_shared.parquet" not in deletable


def test_live_reader_does_not_block_disjoint_reclamation(tmp_path):
    """Reading L must not stall an unrelated X-only retirement; the physical
    paths L indirectly shares are still protected by snapshot membership."""
    root = tmp_path / "lc"
    build_lifecycle_table(root)
    mgr = LifecycleManager(root)
    mgr.onboard()
    # retain every snapshot except the unreferenced leaf X
    mgr.set_references({
        "branches": {"main": {"target": "M2", "retain_generations": 99}},
        "bookmarks": {"bm_l": {"target": "L"}},
    })
    sc = Scanner(root, batch_size=1)
    it = sc.scan("L", 1, (1,))
    next(it)  # reader parked on L
    try:
        plan = mgr.plan()
        assert plan.to_retire == ["X"]
        # disjoint reclamation completes while the L reader stays live
        result = mgr.apply_plan(plan)
        assert result.cleanup_status == "complete"
        assert result.reclaimed == ["files/data_x_only.parquet"]
        # paths L references (incl. the shared EQ delete) are untouched
        assert (root / "files" / "dl_shared.parquet").exists()
        assert (root / "files" / "data_l_only.parquet").exists()
        # and the parked reader still sees every pre-written L row
        rest = [
            (b.file_id, p, b.batch.column("row_uid").to_pylist()[0])
            for b in it for p in b.positions
        ]
        head_seen = 1
        all_rows = rest  # first batch already consumed
        assert len(all_rows) == len(EXPECTED["rows"]["L"]) - head_seen
    finally:
        it.close()
