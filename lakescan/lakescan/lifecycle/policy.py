"""Named-reference retention policy and reclamation-plan construction.

The policy is a single, *wholesale-replaceable* configuration:

* each **branch** reference names one snapshot and how many generations along
  its ``parent`` chain remain readable (the target counts as generation 0; at
  least one generation; a chain that ends early simply keeps what exists);
* each **bookmark** keeps only the snapshot it points at directly;
* at least one branch is required; unknown or already-retired targets reject
  the entire replacement;
* retired ancestors still count toward historical distance while walking a
  chain but are never restored to readability.

The effective retention is the union of the policy and every live read guard.
Plans never auto-retain full ancestor chains, sibling branches, or the
inactive ``target_file_id`` strings mentioned by POS_DELETE files, and they
never touch unregistered files.  Physical path identity -- never ``file_id``
and never byte digest -- decides what may be deleted.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ..table import TableContract
from . import guards as guard_mod
from .state import PLAN_DIR, LifecycleState

PLAN_FORMAT = "lakescan-retention-plan/1"


# ---------------------------------------------------------------------------
# reference configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BranchRef:
    name: str
    target: str
    retain_generations: int


@dataclass(frozen=True)
class BookmarkRef:
    name: str
    target: str


@dataclass(frozen=True)
class References:
    branches: tuple
    bookmarks: tuple

    @staticmethod
    def parse(obj: Any, contract: TableContract, retired: set) -> "References":
        if not isinstance(obj, dict):
            from ..errors import ReferenceConfigError

            raise ReferenceConfigError("reference configuration must be an object")
        branches_raw = obj.get("branches", {})
        bookmarks_raw = obj.get("bookmarks", {})
        if not isinstance(branches_raw, dict) or not isinstance(bookmarks_raw, dict):
            from ..errors import ReferenceConfigError

            raise ReferenceConfigError(
                "branches and bookmarks must be objects of named references"
            )
        if not branches_raw:
            from ..errors import ReferenceConfigError

            raise ReferenceConfigError(
                "at least one branch reference is required"
            )
        names: set[str] = set()

        def _check_name(name: Any, kind: str):
            from ..errors import ReferenceConfigError

            if not isinstance(name, str) or not name:
                raise ReferenceConfigError(
                    f"{kind} reference name must be a non-empty string: {name!r}"
                )
            if name in names:
                raise ReferenceConfigError(
                    f"duplicate reference name {name!r} across branches/bookmarks"
                )
            names.add(name)

        def _check_target(target: Any, where: str) -> str:
            from ..errors import ReferenceConfigError

            if not isinstance(target, str) or not target:
                raise ReferenceConfigError(
                    f"{where}: target must be a non-empty snapshot id"
                )
            if target not in contract.snapshots:
                raise ReferenceConfigError(
                    f"{where}: unknown snapshot target {target!r}"
                )
            if target in retired:
                raise ReferenceConfigError(
                    f"{where}: target {target!r} is retired and cannot be "
                    "referenced back into existence"
                )
            return target

        branches = []
        for name, spec in branches_raw.items():
            _check_name(name, "branch")
            if not isinstance(spec, dict):
                from ..errors import ReferenceConfigError

                raise ReferenceConfigError(f"branch {name!r}: spec must be an object")
            target = _check_target(spec.get("target"), f"branch {name!r}")
            depth = spec.get("retain_generations")
            if not isinstance(depth, int) or isinstance(depth, bool) or depth < 1:
                from ..errors import ReferenceConfigError

                raise ReferenceConfigError(
                    f"branch {name!r}: retain_generations must be an int >= 1"
                )
            branches.append(BranchRef(name, target, depth))

        bookmarks = []
        for name, spec in bookmarks_raw.items():
            _check_name(name, "bookmark")
            if not isinstance(spec, dict):
                from ..errors import ReferenceConfigError

                raise ReferenceConfigError(
                    f"bookmark {name!r}: spec must be an object"
                )
            target = _check_target(spec.get("target"), f"bookmark {name!r}")
            bookmarks.append(BookmarkRef(name, target))

        return References(tuple(branches), tuple(bookmarks))

    def to_json(self) -> dict:
        return {
            "branches": {
                b.name: {"target": b.target, "retain_generations": b.retain_generations}
                for b in self.branches
            },
            "bookmarks": {
                bm.name: {"target": bm.target} for bm in self.bookmarks
            },
        }


# ---------------------------------------------------------------------------
# retention computation
# ---------------------------------------------------------------------------


@dataclass
class RetentionView:
    """The derived retention/retirement decision for one plan generation."""

    retained: dict  # snapshot_id -> list[reason dict]
    to_retire: list  # snapshot ids that would become newly unreadable
    already_retired: list
    reader_protected: dict  # snapshot_id -> list[guard info dicts]


def _walk_chain(contract: TableContract, start: str, depth: int):
    """Yield (snapshot_id, distance) for up to ``depth`` parent-chain nodes.

    Retired ancestors still count toward the distance budget; the walk simply
    continues through them until the chain ends or the budget is spent.
    """
    cur = contract.snapshots.get(start)
    distance = 0
    while cur is not None and distance < depth:
        yield cur.snapshot_id, distance
        cur = contract.snapshots.get(cur.parent) if cur.parent else None
        distance += 1


def compute_retention(state: LifecycleState, contract: TableContract,
                      refs: References) -> RetentionView:
    retained: dict[str, list] = {}
    already_retired = sorted(state.retired)
    retired_set = set(already_retired)

    for br in refs.branches:
        for sid, distance in _walk_chain(
            contract, br.target, br.retain_generations
        ):
            if sid in retired_set:
                # counts toward distance but can never be restored
                continue
            retained.setdefault(sid, []).append({
                "type": "branch", "name": br.name, "distance": distance,
            })

    for bm in refs.bookmarks:
        if bm.target in retired_set:
            continue
        retained.setdefault(bm.target, []).append({
            "type": "bookmark", "name": bm.name,
        })

    # union with live read protection (caller-independent: sweep under lock)
    with state.guard_lock():
        guard_mod.sweep_stale(state)
        live = guard_mod.live_guards(state)
    reader_protected: dict[str, list] = {}
    for payload in live:
        info = {
            "type": "reader",
            "token": payload.get("_token"),
            "pid": payload.get("pid"),
            "reason": payload.get("reason"),
        }
        for sid in payload.get("snapshot_ids", []):
            if sid in retired_set or sid not in contract.snapshots:
                continue
            reader_protected.setdefault(sid, []).append(dict(info))
            retained.setdefault(sid, []).append(dict(info))

    to_retire = sorted(
        sid for sid in contract.snapshots
        if sid not in retained and sid not in retired_set
    )
    return RetentionView(
        retained=dict(sorted(retained.items())),
        to_retire=to_retire,
        already_retired=already_retired,
        reader_protected=reader_protected,
    )


# ---------------------------------------------------------------------------
# plan model
# ---------------------------------------------------------------------------


@dataclass
class DeletionItem:
    path: str
    sha256: str
    kinds: list  # manifest kinds referencing this path
    file_ids: list
    referenced_by: list  # snapshots (all of which are retiring in this plan)
    size_bytes: int

    def to_json(self) -> dict:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "kinds": list(self.kinds),
            "file_ids": list(self.file_ids),
            "referenced_by_retiring_snapshots": list(self.referenced_by),
            "size_bytes": self.size_bytes,
        }


@dataclass
class RetentionPlan:
    plan_id: str
    ref_generation: int
    manifest_sha256: str
    retired_before: list
    retained: dict
    to_retire: list
    deletions: list
    reader_protected: dict
    summary: dict
    path: Optional[Path] = None

    def to_json(self) -> dict:
        return {
            "format": PLAN_FORMAT,
            "plan_id": self.plan_id,
            "ref_generation": self.ref_generation,
            "manifest_sha256": self.manifest_sha256,
            "retired_before": list(self.retired_before),
            "retained": {sid: reasons for sid, reasons in self.retained.items()},
            "to_retire": list(self.to_retire),
            "deletions": [d.to_json() for d in self.deletions],
            "reader_protected": self.reader_protected,
            "summary": dict(self.summary),
        }

    def write(self, path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp",
                                   dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self.to_json(), fh, ensure_ascii=False, sort_keys=True,
                          indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        self.path = path
        return path

    @staticmethod
    def from_json(obj: Any, path: Optional[Path] = None) -> "RetentionPlan":
        from ..errors import PlanStaleError

        if not isinstance(obj, dict) or obj.get("format") != PLAN_FORMAT:
            raise PlanStaleError("not a lakescan retention plan (bad format)")
        try:
            deletions = [
                DeletionItem(
                    path=item["path"],
                    sha256=item["sha256"],
                    kinds=list(item["kinds"]),
                    file_ids=list(item["file_ids"]),
                    referenced_by=list(
                        item["referenced_by_retiring_snapshots"]
                    ),
                    size_bytes=int(item["size_bytes"]),
                )
                for item in obj["deletions"]
            ]
            plan = RetentionPlan(
                plan_id=obj["plan_id"],
                ref_generation=int(obj["ref_generation"]),
                manifest_sha256=obj["manifest_sha256"],
                retired_before=list(obj["retired_before"]),
                retained={sid: list(reasons)
                          for sid, reasons in obj["retained"].items()},
                to_retire=list(obj["to_retire"]),
                deletions=deletions,
                reader_protected=dict(obj.get("reader_protected", {})),
                summary=dict(obj["summary"]),
                path=path,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise PlanStaleError(f"malformed retention plan: {e}") from e
        return plan

    @staticmethod
    def load(path) -> "RetentionPlan":
        path = Path(path)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                obj = json.load(fh)
        except (OSError, json.JSONDecodeError) as e:
            from ..errors import PlanStaleError

            raise PlanStaleError(f"plan file unreadable: {e}") from e
        return RetentionPlan.from_json(obj, path=path)


def build_plan(state: LifecycleState, contract: TableContract,
               refs: References) -> RetentionPlan:
    """Derive a plan purely from current state + policy (no state mutation)."""
    view = compute_retention(state, contract, refs)

    # which still-readable snapshots reference which canonical paths
    readable = [
        sid for sid in contract.snapshots
        if sid not in set(state.retired) and sid not in set(view.to_retire)
    ]
    protected_paths: set[str] = set()
    for sid in readable:
        for entry in contract.snapshots[sid].files:
            protected_paths.add(entry.path)

    # candidate paths: anything the newly retiring snapshots reference
    candidates: dict[str, dict] = {}
    for sid in view.to_retire:
        for entry in contract.snapshots[sid].files:
            rec = candidates.setdefault(entry.path, {
                "kinds": set(), "file_ids": set(), "snapshots": [],
            })
            rec["kinds"].add(entry.kind)
            rec["file_ids"].add(entry.file_id)
            if sid not in rec["snapshots"]:
                rec["snapshots"].append(sid)

    deletions: list[DeletionItem] = []
    for rel in sorted(candidates):
        if rel in protected_paths:
            continue
        if rel not in state.registered_paths:
            # never act on paths the management state does not know about
            continue
        rec = candidates[rel]
        abs_path = state.root / rel
        try:
            size = abs_path.stat().st_size if abs_path.is_file() else 0
        except OSError:
            size = 0
        deletions.append(DeletionItem(
            path=rel,
            sha256=state.registered_paths[rel],
            kinds=sorted(rec["kinds"]),
            file_ids=sorted(rec["file_ids"]),
            referenced_by=sorted(rec["snapshots"]),
            size_bytes=size,
        ))

    total_bytes = sum(d.size_bytes for d in deletions)
    plan = RetentionPlan(
        plan_id=uuid.uuid4().hex,
        ref_generation=state.ref_generation,
        manifest_sha256=state.manifest_sha256,
        retired_before=list(view.already_retired),
        retained=view.retained,
        to_retire=view.to_retire,
        deletions=deletions,
        reader_protected=view.reader_protected,
        summary={
            "retained_snapshots": len(view.retained),
            "retire_snapshots": len(view.to_retire),
            "already_retired_snapshots": len(view.already_retired),
            "delete_paths": len(deletions),
            "delete_bytes": total_bytes,
        },
    )
    default_path = state.dir / PLAN_DIR / f"plan-{plan.plan_id}.json"
    plan.write(default_path)
    return plan
