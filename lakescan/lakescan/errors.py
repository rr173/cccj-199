"""Error taxonomy for the offline-lake scan library.

Every contract violation is raised *before* any data row is scanned
(the delete-loading / planning phase), never silently skipped.
"""


class LakeScanError(Exception):
    """Base class for all errors raised by this library."""


class ContractError(LakeScanError):
    """table.json, schema, or snapshot content violates the offline table contract."""


class UnknownFieldError(ContractError):
    """A field id was referenced that does not exist in the relevant schema."""


class DuplicateFieldError(ContractError):
    """A field id (or physical name) appears twice where it must be unique."""


class TypeEvolutionError(ContractError):
    """Illegal narrowing or otherwise incompatible type evolution."""


class UnrepresentableKeyError(ContractError):
    """A delete key value is not representable in its declared field type."""


class SnapshotError(LakeScanError):
    """A snapshot's own content is erroneous (e.g. out-of-range position)."""


class PositionOutOfRangeError(SnapshotError):
    """A position-delete targets a row outside the target file's row range."""


class SourceVersionMismatch(LakeScanError):
    """A source file's bytes no longer match the sha256 recorded in table.json.

    When this is raised, previously computed offsets/results for the file must
    be considered stale and are never reused: the scan aborts instead.
    """


class SourceFileMissing(LakeScanError):
    """A file listed in table.json does not exist under the scan root."""


class PredicateError(LakeScanError):
    """A scan predicate is malformed or not exactly representable."""


# ---------------------------------------------------------------------------
# cross-snapshot diff
# ---------------------------------------------------------------------------


class DiffError(LakeScanError):
    """Base class for errors raised by the cross-snapshot key diff."""


class KeyFieldError(DiffError):
    """A business-key field is missing on one side or not exactly compatible
    with the target schema."""


class NullKeyError(DiffError):
    """A visible row carries a null business-key value.  The whole diff fails:
    null can never be a business identity."""


class DuplicateKeyError(DiffError):
    """The same non-null business key occurs in more than one visible row of
    one snapshot.  The whole diff fails rather than pairing rows arbitrarily."""


# ---------------------------------------------------------------------------
# snapshot lifecycle: retention, retirement, recoverable reclamation
# ---------------------------------------------------------------------------


class LifecycleError(LakeScanError):
    """Base class for errors raised by the retention/reclamation lifecycle."""


class NotManagedError(LifecycleError):
    """The table directory has never been onboarded into lifecycle management."""


class AlreadyManagedError(LifecycleError):
    """Onboarding was requested for a directory that is already managed."""


class ManagementCorruptError(LifecycleError):
    """The on-disk lifecycle state is missing, unparseable or internally
    inconsistent.  A managed table in this state fails closed: every managed
    operation raises instead of behaving like an unmanaged directory."""


class ManifestDriftError(LifecycleError):
    """table.json no longer matches the manifest bound at onboarding time.

    Managed tables are read-only; any divergence fails closed instead of
    silently treating the new content as the source of truth."""


class ManagedPathError(LifecycleError):
    """A registered path escapes the table root, uses a link component, is not
    a regular file, or two entries claim the same canonical path with
    conflicting digests."""


class ReferenceConfigError(LifecycleError):
    """A named-reference configuration is invalid (no branch, unknown or
    retired target, bad retention depth, ...).  The whole replacement is
    rejected; the previously committed configuration stays in force."""


class SnapshotRetiredError(LifecycleError):
    """A read targeted a snapshot that has been retired by a committed
    lifecycle operation.  Retired snapshots stay visible in lineage
    diagnostics but can never be read or referenced back into existence."""


class PlanStaleError(LifecycleError):
    """A plan's bindings no longer match the current management generation.

    Raised when the manifest changed, the reference generation moved, the
    retirement set moved, or re-deriving safety from the *current* state would
    produce a different retirement/deletion set than the plan carries.  No
    file is touched."""


class PlanBlockedError(LifecycleError):
    """The plan is still current but cannot be applied: a live reader protects
    a snapshot the plan would retire, or a to-be-deleted path failed its
    pre-commit re-verification.  Nothing is retired and no file is deleted."""


class RecoveryInProgressError(LifecycleError):
    """A reclamation batch is still open (pending or blocked items).  New
    apply operations are refused until :meth:`resume` settles the batch, so
    two operations can never process the same physical path from stale
    state."""


# ---------------------------------------------------------------------------
# bitemporal fact attribution
# ---------------------------------------------------------------------------


class AttributionError(LakeScanError):
    """Base class for errors raised by the fact attribution task."""


class AttributionConfigError(AttributionError):
    """The attribution mapping config is malformed (unknown table/field id,
    bad role wiring, duplicate output column, invalid output path, ...)."""


class AttributionValidationError(AttributionError):
    """A visible input row violates the attribution contract (null/duplicate
    fact id, invalid revision, duplicate rule version, bad interval, ...).
    The whole task fails; no partial output is published."""


class AttributionOutputError(AttributionError):
    """The output directory already exists or another publishing error makes
    the one-shot directory delivery impossible."""
