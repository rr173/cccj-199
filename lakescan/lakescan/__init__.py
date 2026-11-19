"""Offline-lake snapshot scan library.

Reads one fixed snapshot of an offline lake table backup (the explicit
contract in table.json) with position/equality deletes, field identity by
field_id, exact type widening, SQL three-valued predicates, and provenance
output.  This is NOT a full Iceberg implementation: no writes, no commit
protocol, no generic SQL, no incremental subscription.
"""

from .errors import (
    AlreadyManagedError,
    ContractError,
    DiffError,
    DuplicateFieldError,
    DuplicateKeyError,
    KeyFieldError,
    LakeScanError,
    LifecycleError,
    ManagementCorruptError,
    ManifestDriftError,
    ManagedPathError,
    NotManagedError,
    NullKeyError,
    PlanBlockedError,
    PlanStaleError,
    PositionOutOfRangeError,
    PredicateError,
    RecoveryInProgressError,
    ReferenceConfigError,
    SnapshotError,
    SnapshotRetiredError,
    SourceFileMissing,
    SourceVersionMismatch,
    TypeEvolutionError,
    UnknownFieldError,
    UnrepresentableKeyError,
)
from .diff import DiffResult, TableDiffer, compare_snapshots
from .lifecycle import (
    ApplyResult,
    BookmarkRef,
    BranchRef,
    DeletionItem,
    LifecycleManager,
    LifecycleState,
    ReadGuard,
    References,
    RetentionPlan,
    is_managed,
    load_state,
    onboard,
)
from .scan import ScanBatch, Scanner, collect

__all__ = [
    "Scanner",
    "ScanBatch",
    "collect",
    "TableDiffer",
    "DiffResult",
    "compare_snapshots",
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
    "LakeScanError",
    "ContractError",
    "DiffError",
    "KeyFieldError",
    "NullKeyError",
    "DuplicateKeyError",
    "DuplicateFieldError",
    "UnknownFieldError",
    "TypeEvolutionError",
    "UnrepresentableKeyError",
    "SnapshotError",
    "PositionOutOfRangeError",
    "SourceVersionMismatch",
    "SourceFileMissing",
    "PredicateError",
    "LifecycleError",
    "NotManagedError",
    "AlreadyManagedError",
    "ManagementCorruptError",
    "ManifestDriftError",
    "ManagedPathError",
    "ReferenceConfigError",
    "SnapshotRetiredError",
    "PlanStaleError",
    "PlanBlockedError",
    "RecoveryInProgressError",
]

__version__ = "0.1.0"
