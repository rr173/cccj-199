"""Offline-lake snapshot scan library.

Reads one fixed snapshot of an offline lake table backup (the explicit
contract in table.json) with position/equality deletes, field identity by
field_id, exact type widening, SQL three-valued predicates, and provenance
output.  This is NOT a full Iceberg implementation: no writes, no commit
protocol, no generic SQL, no incremental subscription.
"""

from .errors import (
    ContractError,
    DiffError,
    DuplicateFieldError,
    DuplicateKeyError,
    KeyFieldError,
    LakeScanError,
    NullKeyError,
    PositionOutOfRangeError,
    PredicateError,
    SnapshotError,
    SourceFileMissing,
    SourceVersionMismatch,
    TypeEvolutionError,
    UnknownFieldError,
    UnrepresentableKeyError,
)
from .diff import DiffResult, TableDiffer, compare_snapshots
from .scan import ScanBatch, Scanner, collect

__all__ = [
    "Scanner",
    "ScanBatch",
    "collect",
    "TableDiffer",
    "DiffResult",
    "compare_snapshots",
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
]

__version__ = "0.1.0"
