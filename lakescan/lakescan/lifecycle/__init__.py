"""Snapshot retention and recoverable reclamation for managed lake tables.

The package adds a storage-lifecycle layer on top of the read-only snapshot
contract.  Onboarding binds the exact ``table.json`` manifest and the set of
registered regular-file paths; afterwards named branch/bookmark references
drive retention plans whose application first durably retires snapshots and
only then reclaims physical paths, with full crash recovery and real
read-lifecycle protection (no timeouts).
"""

from .guards import ReadGuard
from .manager import ApplyResult, LifecycleManager
from .policy import (
    BookmarkRef,
    BranchRef,
    DeletionItem,
    References,
    RetentionPlan,
)
from .state import (
    LifecycleState,
    is_managed,
    load_state,
    onboard,
)

__all__ = [
    "LifecycleManager",
    "LifecycleState",
    "ApplyResult",
    "References",
    "BranchRef",
    "BookmarkRef",
    "RetentionPlan",
    "DeletionItem",
    "ReadGuard",
    "is_managed",
    "load_state",
    "onboard",
]
