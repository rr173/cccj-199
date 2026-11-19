"""Read-side lifecycle integration.

Every *real* read entry point (``Scanner.plan`` / ``scan``, the explain
interfaces and the two-sided ``compare``) goes through :func:`read_preflight`
before any input file is opened.  On a managed table it:

1. re-verifies that ``table.json`` still matches the bound manifest (drift or
   state corruption raises instead of silently acting unmanaged);
2. atomically acquires a :class:`~lakescan.lifecycle.guards.ReadGuard` for the
   snapshots about to be read, so a concurrent reclaim commit either sees the
   guard first and refuses retirement, or retires first -- in which case this
   call raises :class:`SnapshotRetiredError` before opening any input.

On an unmanaged table the function returns ``None`` and the legacy read path
is unchanged.  A guard passed in by an outer read (the two-sided compare
protects both snapshots in one claim) is reused without nesting tokens.
"""

from __future__ import annotations

from typing import Iterable

from ..errors import SnapshotRetiredError
from . import guards as guard_mod
from .state import LifecycleState, is_managed, load_state


def read_preflight(root, snapshot_ids: Iterable[str], reason: str = "read",
                   outer_guard=None):
    """Acquire read protection for ``snapshot_ids`` on a managed table.

    Returns a :class:`ReadGuard` (must be closed when the read ends) or
    ``None`` for unmanaged tables.  If ``outer_guard`` already covers the
    requested snapshots it is returned unchanged: nested library reads never
    create redundant tokens.
    """
    snapshot_ids = tuple(dict.fromkeys(snapshot_ids))
    if outer_guard is not None:
        covered = set(getattr(outer_guard, "snapshot_ids", ()))
        if all(s in covered for s in snapshot_ids):
            return outer_guard
    if not is_managed(root):
        return None
    state = load_state(root)  # fail closed on drift/corruption
    if outer_guard is not None:
        return outer_guard
    return guard_mod.protect(state, snapshot_ids, reason=reason)


def assert_readable(state: LifecycleState, snapshot_ids: Iterable[str]) -> None:
    """Raise if any snapshot was retired since the guard was taken."""
    retired = set(state.retired)
    gone = sorted(set(snapshot_ids) & retired)
    if gone:
        raise SnapshotRetiredError(
            f"snapshot(s) {gone} are retired and can no longer be read"
        )
