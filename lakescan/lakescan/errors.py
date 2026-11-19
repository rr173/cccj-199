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
