"""Public lifecycle API: onboarding, references, plans, apply, resume.

Two-phase, recoverable reclamation protocol
-------------------------------------------

``apply_plan`` never deletes a file before the *retirement boundary* is
durably committed:

1. the plan is rebound against current state (manifest digest, reference
   generation, already-retired set) and re-derived from scratch; any
   divergence makes the plan stale and nothing is touched;
2. under the guard lock, stale reader tokens are swept and a live reader on a
   to-be-retired snapshot blocks the whole apply (no retirement, no deletion);
3. every candidate path is re-verified (regular file, not a link, digest
   still matches) before the commit;
4. the state file atomically records the retirement and the *complete*
   cleanup item list (all ``pending``) -- only after this fsync+rename may
   physical deletion start;
5. deletion proceeds item by item; each item's ``reclaimed``/``blocked``
   outcome is persisted before moving on, so a process killed mid-batch
   resumes the *same* item list and never re-derives a wider deletion;
6. blocked items (content swapped, path replaced by a link, ...) leave the
   batch open with explicit per-item reasons; ``resume`` retries them once the
   operator restores the original bytes.

While a batch is open, new applies are refused
(:class:`RecoveryInProgressError`) so two operations can never process the
same physical path from different states.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from ..errors import (
    PlanBlockedError,
    PlanStaleError,
    RecoveryInProgressError,
)
from ..table import TableContract
from . import guards as guard_mod
from .policy import (
    References,
    RetentionPlan,
    build_plan,
)
from .state import (
    LifecycleState,
    is_managed,
    load_state,
    onboard as _onboard,
)

# test/injection hooks: callables invoked at crash boundaries (tests only)
_HOOK_BEFORE_COMMIT = "LAKESCAN_HOOK_BEFORE_COMMIT"
_HOOK_AFTER_COMMIT = "LAKESCAN_HOOK_AFTER_COMMIT"
_HOOK_AFTER_UNLINK = "LAKESCAN_HOOK_AFTER_UNLINK"


def _run_hook(name: str) -> None:
    target = os.environ.get(name)
    if not target:
        return
    kind, _, arg = target.partition(":")
    try:
        if kind == "exit":
            os._exit(int(arg or "7"))
        if kind == "signal":
            import signal

            os.kill(os.getpid(), getattr(signal, arg or "SIGKILL"))
    except (ValueError, AttributeError):
        os._exit(7)


# ---------------------------------------------------------------------------
# results
# ---------------------------------------------------------------------------


@dataclass
class ApplyResult:
    plan_id: str
    retirement_committed: bool
    retired_snapshots: list
    batch_id: Optional[str]
    reclaimed: list          # paths deleted during this call
    blocked: list            # [{path, reason}]
    pending: list            # paths still awaiting deletion
    cleanup_status: str      # "complete" | "blocked" | "none"

    @property
    def fully_reclaimed(self) -> bool:
        return self.cleanup_status == "complete" or (
            self.cleanup_status == "none" and not self.retired_snapshots
        )

    def to_json(self) -> dict:
        return {
            "plan_id": self.plan_id,
            "retirement_committed": self.retirement_committed,
            "retired_snapshots": list(self.retired_snapshots),
            "batch_id": self.batch_id,
            "reclaimed": list(self.reclaimed),
            "blocked": list(self.blocked),
            "pending": list(self.pending),
            "cleanup_status": self.cleanup_status,
        }


# ---------------------------------------------------------------------------
# manager
# ---------------------------------------------------------------------------


class LifecycleManager:
    """All retention/reclamation operations for one managed table root."""

    def __init__(self, root):
        self.root = Path(root)

    # -- onboarding / discovery --------------------------------------------

    @property
    def managed(self) -> bool:
        return is_managed(self.root)

    def onboard(self) -> LifecycleState:
        return _onboard(self.root)

    def state(self) -> LifecycleState:
        return load_state(self.root)

    def contract(self, state: Optional[LifecycleState] = None) -> TableContract:
        state = state or load_state(self.root)
        return state.load_contract()

    # -- status -------------------------------------------------------------

    def status(self) -> dict:
        state = load_state(self.root)
        contract = state.load_contract()
        with state.guard_lock():
            guard_mod.sweep_stale(state)
            live = guard_mod.live_guards(state)
        cleanup = state.cleanup
        items_view = {"pending": [], "reclaimed": [], "blocked": []}
        if cleanup is not None:
            for it in cleanup["items"]:
                bucket = items_view.setdefault(it["status"], [])
                entry = {"path": it["path"], "sha256": it["sha256"]}
                if it["status"] == "blocked":
                    entry["reason"] = it.get("reason", "blocked")
                bucket.append(entry)
            if cleanup["items"] and all(
                it["status"] == "reclaimed" for it in cleanup["items"]
            ):
                batch_status = "complete"
            elif any(it["status"] == "blocked" for it in cleanup["items"]):
                batch_status = "blocked"
            else:
                batch_status = "in_progress"
        else:
            batch_status = "none"
        return {
            "managed": True,
            "table_id": state.table_id,
            "manifest_sha256": state.manifest_sha256,
            "ref_generation": state.ref_generation,
            "references": state.references,
            "snapshots": {
                "total": len(contract.snapshots),
                "readable": sorted(
                    s for s in contract.snapshots if s not in set(state.retired)
                ),
                "retired": list(state.retired),
            },
            "registered_paths": len(state.registered_paths),
            "cleanup": None if cleanup is None else {
                "batch_id": cleanup["batch_id"],
                "status": batch_status,
                "retire_snapshots": list(cleanup["retire_snapshots"]),
                "ref_generation": cleanup["ref_generation"],
                "items": items_view,
            },
            "live_readers": [
                {
                    "token": p.get("_token"),
                    "pid": p.get("pid"),
                    "reason": p.get("reason"),
                    "snapshot_ids": list(p.get("snapshot_ids", [])),
                }
                for p in live
            ],
            "batch_history": list(state.batches),
        }

    # -- reference configuration (wholesale replacement) -------------------

    def set_references(self, config: dict) -> dict:
        state = load_state(self.root)
        with state.state_lock():
            # re-read under the lock so the generation bump is serialized
            state = load_state(self.root)
            contract = state.load_contract()
            refs = References.parse(config, contract, set(state.retired))
            new_generation = state.bump_ref_generation()
            state.set_references(refs.to_json())
            state.persist()
            return {
                "ref_generation": new_generation,
                "references": refs.to_json(),
            }

    # -- planning -----------------------------------------------------------

    def plan(self) -> RetentionPlan:
        state = load_state(self.root)
        with state.state_lock():
            state = load_state(self.root)
            if state.cleanup is not None:
                raise RecoveryInProgressError(
                    "a reclamation batch is still open; call resume() before "
                    "planning a new retention change"
                )
            contract = state.load_contract()
            if not state.references:
                from ..errors import ReferenceConfigError

                raise ReferenceConfigError(
                    "no reference configuration committed; call set_references first"
                )
            refs = References.parse(
                state.references, contract, set(state.retired)
            )
            return build_plan(state, contract, refs)

    # -- apply --------------------------------------------------------------

    def apply_plan(self, plan: RetentionPlan) -> ApplyResult:
        state = load_state(self.root)
        with state.state_lock():
            state = load_state(self.root)
            if state.cleanup is not None:
                raise RecoveryInProgressError(
                    "a reclamation batch is still open; call resume() before "
                    "applying another plan"
                )
            return self._apply_locked(state, plan)

    def apply_plan_file(self, path) -> ApplyResult:
        return self.apply_plan(RetentionPlan.load(path))

    def _rebind(self, state: LifecycleState, plan: RetentionPlan,
                contract: TableContract) -> None:
        """Fail closed unless the plan still matches the current world.

        Reference/manifest/retirement-set movement makes the plan *stale*.
        A new reader that protects a snapshot the plan would retire does not
        change the configuration: it makes the plan *blocked*, and is
        reported separately after the guard lock is taken.
        """
        if plan.manifest_sha256 != state.manifest_sha256:
            raise PlanStaleError(
                "plan bound to a different table.json manifest; the manifest "
                "has changed since planning"
            )
        if plan.ref_generation != state.ref_generation:
            raise PlanStaleError(
                f"plan references generation {plan.ref_generation} but the "
                f"current reference generation is {state.ref_generation}; "
                "references changed, regenerate the plan"
            )
        if set(plan.retired_before) != set(state.retired):
            raise PlanStaleError(
                "the retired-snapshot set moved since the plan was generated"
            )
        # the user-edited plan file is never itself a deletion instruction.
        # Re-derive safety from current state; any difference between the
        # re-derived policy decision and the plan must be explained by live
        # read protection, otherwise the plan is treated as stale.
        refs = References.parse(
            state.references, contract, set(state.retired)
        )
        fresh = build_plan(state, contract, refs)
        if set(fresh.to_retire) != set(plan.to_retire):
            extra = set(plan.to_retire) - set(fresh.to_retire)
            with state.guard_lock():
                guard_mod.sweep_stale(state)
                protected = guard_mod.protected_snapshot_set(state)
            if extra <= protected:
                raise PlanBlockedError(
                    "live reader protects snapshot(s) "
                    f"{sorted(extra)} that this plan would retire; the plan "
                    "is refused and no file is touched"
                )
            raise PlanStaleError(
                "re-derived retirement set differs from the plan for reasons "
                "other than live read protection; refusing the stale plan"
            )
        fresh_paths = {d.path: d.sha256 for d in fresh.deletions}
        plan_paths = {d.path: d.sha256 for d in plan.deletions}
        if fresh_paths != plan_paths:
            raise PlanStaleError(
                "re-derived safe deletion set differs from the plan; refusing "
                "the stale plan"
            )

    def _apply_locked(self, state: LifecycleState,
                      plan: RetentionPlan) -> ApplyResult:
        contract = state.load_contract()  # raises on manifest drift/corruption
        self._rebind(state, plan, contract)

        # reader interlock (lock order: state -> guard)
        with state.guard_lock():
            guard_mod.sweep_stale(state)
            protected = guard_mod.protected_snapshot_set(state)
            blocked_by_reader = sorted(set(plan.to_retire) & protected)
            if blocked_by_reader:
                raise PlanBlockedError(
                    "live reader protects snapshot(s) "
                    f"{blocked_by_reader} that this plan would retire; the "
                    "plan is refused and no file is touched"
                )

            # pre-commit path verification: a swapped/linked/missing path
            # blocks the whole commit while nothing has been retired.
            precheck_failures = []
            for item in plan.deletions:
                reason = _verify_candidate(state.root, item.path, item.sha256)
                if reason is not None:
                    precheck_failures.append({"path": item.path, "reason": reason})
            if precheck_failures:
                raise PlanBlockedError(
                    "candidate paths failed pre-commit verification: "
                    + "; ".join(f"{f['path']} ({f['reason']})"
                                for f in precheck_failures)
                )

            _run_hook(_HOOK_BEFORE_COMMIT)

            # ---- durable retirement boundary + cleanup ledger -------------
            import uuid

            batch_id = uuid.uuid4().hex
            if plan.to_retire:
                state.mark_retired(plan.to_retire)
            items = [
                {
                    "path": d.path,
                    "sha256": d.sha256,
                    "status": "pending",
                    "reason": None,
                }
                for d in plan.deletions
            ]
            if items:
                state.open_batch({
                    "batch_id": batch_id,
                    "plan_id": plan.plan_id,
                    "ref_generation": state.ref_generation,
                    "retire_snapshots": list(plan.to_retire),
                    "items": items,
                })
            state.persist()
            _run_hook(_HOOK_AFTER_COMMIT)

        retirement_committed = bool(plan.to_retire)
        if not items:
            return ApplyResult(
                plan_id=plan.plan_id,
                retirement_committed=retirement_committed,
                retired_snapshots=list(plan.to_retire),
                batch_id=None,
                reclaimed=[], blocked=[], pending=[],
                cleanup_status="none",
            )

        # ---- physical reclamation (resumable; state lock still held) ------
        return self._run_deletions(state, batch_id, plan.plan_id,
                                   plan.to_retire, retirement_committed)

    def _run_deletions(self, state: LifecycleState, batch_id: str,
                       plan_id: str, retired_snapshots: list,
                       retirement_committed: bool) -> ApplyResult:
        batch = state.cleanup
        reclaimed, blocked, pending = [], [], []
        for item in batch["items"]:
            if item["status"] == "reclaimed":
                reclaimed.append(item["path"])
                continue
            if item["status"] == "blocked":
                # A previous attempt diagnosed this path.  Resume re-verifies
                # it: an operator may have restored the original bytes, and
                # the contract explicitly requires that retrying then works.
                reason = _verify_candidate(
                    state.root, item["path"], item["sha256"]
                )
                if reason is not None:
                    item["reason"] = reason
                    blocked.append({"path": item["path"], "reason": reason})
                    pending.append(item["path"])
                    state.persist()
                    continue
                item["status"] = "pending"
                item["reason"] = None
                state.persist()
                # fall through to the pending-handling logic below

            # pending (or just-unblocked): detect work already done by a
            # killed predecessor before re-verifying/unlinking.
            abs_path = state.root / item["path"]
            if not abs_path.exists() and not abs_path.is_symlink():
                item["status"] = "reclaimed"
                item["reason"] = None
                state.persist()
                reclaimed.append(item["path"])
                continue

            reason = _verify_candidate(state.root, item["path"], item["sha256"])
            if reason is not None:
                item["status"] = "blocked"
                item["reason"] = reason
                state.persist()
                blocked.append({"path": item["path"], "reason": reason})
                pending.append(item["path"])
                continue

            try:
                abs_path.unlink()
            except OSError as e:
                item["status"] = "blocked"
                item["reason"] = f"unlink failed: {e}"
                state.persist()
                blocked.append({"path": item["path"],
                                "reason": item["reason"]})
                pending.append(item["path"])
                continue
            _run_hook(_HOOK_AFTER_UNLINK)
            item["status"] = "reclaimed"
            state.persist()
            reclaimed.append(item["path"])

        any_blocked = any(it["status"] == "blocked" for it in batch["items"])
        if any_blocked:
            state.update_batch_record(batch_id, "blocked")
            state.persist()
            cleanup_status = "blocked"
        else:
            state.set_cleanup(None)
            state.update_batch_record(batch_id, "complete")
            state.persist()
            cleanup_status = "complete"
        return ApplyResult(
            plan_id=plan_id,
            retirement_committed=retirement_committed,
            retired_snapshots=list(retired_snapshots),
            batch_id=batch_id,
            reclaimed=reclaimed,
            blocked=blocked,
            pending=pending,
            cleanup_status=cleanup_status,
        )

    # -- resume -------------------------------------------------------------

    def resume(self) -> ApplyResult:
        """Continue the open reclamation batch with the same item list."""
        state = load_state(self.root)
        with state.state_lock():
            state = load_state(self.root)
            batch = state.cleanup
            if batch is None:
                return ApplyResult(
                    plan_id="",
                    retirement_committed=False,
                    retired_snapshots=[],
                    batch_id=None,
                    reclaimed=[], blocked=[], pending=[],
                    cleanup_status="none",
                )
            # readers can never un-retire snapshots; sweep only to keep the
            # registry tidy, then continue the *same* ledger (no re-derivation).
            with state.guard_lock():
                guard_mod.sweep_stale(state)
            return self._run_deletions(
                state,
                batch["batch_id"],
                batch.get("plan_id", ""),
                list(batch["retire_snapshots"]),
                retirement_committed=True,
            )


# ---------------------------------------------------------------------------
# candidate verification (used both pre-commit and during deletion)
# ---------------------------------------------------------------------------


def _verify_candidate(root: Path, rel: str, expected_sha: str) -> Optional[str]:
    """Return None if the path is still the bound regular file, else a reason.

    Symlinks (including a link replacing a regular file), non-regular objects
    and digest mismatches are all refused; the path is never deleted in those
    cases.  Missing paths are a failure here (pre-commit) but are interpreted
    as already-reclaimed during batch processing.
    """
    p = Path(root) / rel
    if p.is_symlink():
        return "path is now a symbolic link; refusing to delete"
    if not p.exists():
        return "path no longer exists"
    if not p.is_file():
        return "path is no longer a regular file"
    # reject a link introduced at any parent directory after onboarding
    from pathlib import PurePosixPath

    cur = Path(root)
    for part in PurePosixPath(rel).parts:
        cur = cur / part
        if cur.is_symlink():
            return f"component {part!r} is now a symbolic link; refusing to delete"
    from ..table import sha256_of

    try:
        actual = sha256_of(p)
    except OSError as e:
        return f"cannot hash path: {e}"
    if actual != expected_sha:
        return (
            "content replaced: sha256 mismatch "
            f"(expected {expected_sha[:16]}..., found {actual[:16]}...)"
        )
    return None
