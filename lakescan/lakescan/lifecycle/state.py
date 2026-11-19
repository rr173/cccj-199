"""Persistent lifecycle state for one managed offline-lake table directory.

Onboarding binds the *exact* ``table.json`` manifest content present at that
moment and the set of registered file paths/digests.  Afterwards the manifest
and the source files are treated as read-only:

* every state load re-hashes ``table.json`` and compares it with the bound
  digest -- divergence raises :class:`ManifestDriftError`;
* a missing, truncated or internally inconsistent state directory raises
  :class:`ManagementCorruptError` instead of silently falling back to the
  unmanaged behaviour;
* managed files are chmod-ed read-only as defence in depth (the state
  directory itself stays writable so reclamation can record progress).

The state file is rewritten atomically (temp file + fsync + ``os.replace``)
and every mutation happens while holding ``state.lock`` so two local
processes cannot apply/replace concurrently.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any, Optional

from ..errors import (
    AlreadyManagedError,
    ManagementCorruptError,
    ManifestDriftError,
    ManagedPathError,
    NotManagedError,
)
from ..table import FORMAT, TableContract, sha256_of

STATE_DIR = ".lakescan"
STATE_FILE = "state.json"
STATE_LOCK = "state.lock"
GUARD_DIR = "guards"
GUARD_LOCK = "guards.lock"
PLAN_DIR = "plans"
STAGING_DIR = "staging"

STATE_FORMAT = "lakescan-lifecycle/1"


# ---------------------------------------------------------------------------
# path admission: table-root-relative POSIX paths, regular files only
# ---------------------------------------------------------------------------


def validate_relpath(path: Any) -> str:
    """Validate a manifest-registered relative path.

    The path must be a non-empty POSIX-relative path with no absolute,
    ``..`` or empty components (``.`` is rejected as well -- only paths that
    name a concrete file are accepted), and no backslashes (which POSIX treats
    as a regular filename character but which would make cross-tool handling
    ambiguous).  The lexical check alone cannot catch symlinks; callers must
    additionally verify each component is not a link.
    """
    if not isinstance(path, str) or not path:
        raise ManagedPathError(f"registered path must be a non-empty string: {path!r}")
    if "\\" in path:
        raise ManagedPathError(f"backslash is not allowed in registered paths: {path!r}")
    p = PurePosixPath(path)
    if p.is_absolute():
        raise ManagedPathError(f"registered path must be relative: {path!r}")
    parts = p.parts
    if not parts:
        raise ManagedPathError(f"empty registered path: {path!r}")
    for part in parts:
        if part in ("", ".", ".."):
            raise ManagedPathError(
                f"registered path contains a traversal/empty component: {path!r}"
            )
    return path


def _check_regular_unlinked(root: Path, rel: str) -> Path:
    """Resolve a registered path and reject links or non-regular files.

    Every component up to the table root must exist and none of them (nor the
    final file) may be a symlink/hardlink-to-link; the final object must be a
    regular file.  Hard links themselves cannot be distinguished from regular
    files without inode heuristics and are not part of the threat model; a
    swapped-in *symlink* is detected both here and again at deletion time.
    """
    cur = root
    parts = PurePosixPath(rel).parts
    for part in parts:
        nxt = cur / part
        # check the link flag before anything that follows symlinks
        if nxt.is_symlink():
            raise ManagedPathError(
                f"registered path {rel!r}: symlink component {part!r} is not allowed"
            )
        if not nxt.exists():
            raise ManagedPathError(
                f"registered path {rel!r}: component {part!r} is missing "
                "(a dangling symlink is treated as missing)"
            )
        cur = nxt
    if not cur.is_file():
        raise ManagedPathError(f"registered path {rel!r}: not a regular file")
    return cur


def canonical_manifest_bytes(raw: dict) -> bytes:
    """Deterministic serialization of the manifest bound at onboarding."""
    return json.dumps(
        raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# file locks (whole-directory advisory locks; single-machine scope)
# ---------------------------------------------------------------------------


class _FileLock:
    """Exclusive ``flock``-based lock with a stable on-disk lock file."""

    def __init__(self, path: Path):
        self._path = Path(path)
        self._fh = None

    def acquire(self):
        import fcntl

        self._path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self._path, "a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        except OSError:
            fh.close()
            raise
        self._fh = fh
        return self

    def release(self):
        import fcntl

        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *exc):
        self.release()
        return False


# ---------------------------------------------------------------------------
# state object
# ---------------------------------------------------------------------------


class LifecycleState:
    """Typed view over ``.lakescan/state.json`` plus its binding data."""

    def __init__(self, root: Path, data: dict):
        self.root = Path(root)
        self.dir = self.root / STATE_DIR
        self._data = data

    # -- basic accessors ----------------------------------------------------

    @property
    def format(self) -> str:
        return self._data["format"]

    @property
    def table_id(self) -> str:
        return self._data["table_id"]

    @property
    def manifest_sha256(self) -> str:
        return self._data["manifest_sha256"]

    @property
    def ref_generation(self) -> int:
        return self._data["ref_generation"]

    @property
    def references(self) -> dict:
        return self._data["references"]

    @property
    def retired(self) -> list:
        return self._data["retired_snapshots"]

    @property
    def registered_paths(self) -> dict:
        """canonical relative path -> sha256 bound at onboarding."""
        return self._data["registered_paths"]

    @property
    def cleanup(self) -> Optional[dict]:
        return self._data.get("cleanup")

    @property
    def batches(self) -> list:
        return self._data.setdefault("batches", [])

    def to_json(self) -> dict:
        return json.loads(json.dumps(self._data, ensure_ascii=False, sort_keys=True))

    # -- locking ------------------------------------------------------------

    def state_lock(self) -> _FileLock:
        return _FileLock(self.dir / STATE_LOCK)

    def guard_lock(self) -> _FileLock:
        return _FileLock(self.dir / GUARD_LOCK)

    # -- persistence --------------------------------------------------------

    def _state_path(self) -> Path:
        return self.dir / STATE_FILE

    def persist(self) -> None:
        """Atomically rewrite the state file (fsync + rename)."""
        self.dir.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix="state.", suffix=".tmp", dir=str(self.dir))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self._data, fh, ensure_ascii=False, sort_keys=True,
                          indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._state_path())
            self._fsync_dir(self.dir)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(str(path), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # -- mutation helpers (callers hold state_lock) -------------------------

    def bump_ref_generation(self) -> int:
        self._data["ref_generation"] += 1
        return self._data["ref_generation"]

    def set_references(self, refs: dict) -> None:
        self._data["references"] = refs

    def mark_retired(self, snapshot_ids: list) -> None:
        cur = set(self._data["retired_snapshots"])
        cur.update(snapshot_ids)
        self._data["retired_snapshots"] = sorted(cur)

    def open_batch(self, batch: dict) -> None:
        assert self._data.get("cleanup") is None
        self._data["cleanup"] = batch
        self.batches.append({"batch_id": batch["batch_id"], "status": "open"})

    def set_cleanup(self, batch: Optional[dict]) -> None:
        self._data["cleanup"] = batch

    def update_batch_record(self, batch_id: str, status: str) -> None:
        for rec in self.batches:
            if rec["batch_id"] == batch_id:
                rec["status"] = status
                return
        self.batches.append({"batch_id": batch_id, "status": status})

    # -- manifest verification ---------------------------------------------

    def read_manifest_raw(self) -> dict:
        tj = self.root / "table.json"
        if not tj.is_file() or tj.is_symlink():
            raise ManifestDriftError(
                f"table.json under {self.root} is missing or replaced by a link"
            )
        try:
            with open(tj, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (OSError, json.JSONDecodeError) as e:
            raise ManifestDriftError(f"table.json cannot be read/parsed: {e}") from e
        digest = hashlib.sha256(canonical_manifest_bytes(raw)).hexdigest()
        if digest != self.manifest_sha256:
            raise ManifestDriftError(
                "table.json content differs from the manifest bound at "
                "onboarding; managed tables are read-only"
            )
        return raw

    def load_contract(self) -> TableContract:
        """Load the bound manifest as a validated contract (fails closed)."""
        raw = self.read_manifest_raw()
        return TableContract(self.root, raw)


# ---------------------------------------------------------------------------
# discovery / onboarding / loading
# ---------------------------------------------------------------------------


def is_managed(root) -> bool:
    """True iff a well-formed state directory exists.

    A half-written or truncated state directory counts as *managed* (and will
    fail closed on :func:`load_state`); only the complete absence of the
    ``.lakescan`` directory means unmanaged.
    """
    return (Path(root) / STATE_DIR / STATE_FILE).exists()


def _require_str(obj: dict, key: str, where: str) -> str:
    val = obj.get(key)
    if not isinstance(val, str) or not val:
        raise ManagementCorruptError(f"{where}: missing/non-string {key!r}")
    return val


def load_state(root) -> LifecycleState:
    """Load and structurally validate the lifecycle state (fail closed)."""
    root = Path(root)
    state_path = root / STATE_DIR / STATE_FILE
    if not state_path.exists():
        raise NotManagedError(f"{root} is not under lifecycle management")
    try:
        with open(state_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as e:
        raise ManagementCorruptError(f"lifecycle state unreadable: {e}") from e
    if not isinstance(data, dict):
        raise ManagementCorruptError("lifecycle state root must be an object")
    try:
        if _require_str(data, "format", "state") != STATE_FORMAT:
            raise ManagementCorruptError(
                f"unknown lifecycle state format {data.get('format')!r}"
            )
        _require_str(data, "table_id", "state")
        digest = _require_str(data, "manifest_sha256", "state")
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ManagementCorruptError("state: bad manifest_sha256")
        gen = data.get("ref_generation")
        if not isinstance(gen, int) or isinstance(gen, bool) or gen < 1:
            raise ManagementCorruptError("state: ref_generation must be a positive int")
        refs = data.get("references")
        # empty until the first explicit set_references; afterwards always
        # validated as a non-empty branches/bookmarks object on replacement.
        if not isinstance(refs, dict):
            raise ManagementCorruptError("state: references must be an object")
        retired = data.get("retired_snapshots")
        if not isinstance(retired, list) or any(not isinstance(s, str) for s in retired):
            raise ManagementCorruptError("state: retired_snapshots must be a list of strings")
        reg = data.get("registered_paths")
        if not isinstance(reg, dict) or not reg:
            raise ManagementCorruptError("state: registered_paths must be a non-empty object")
        for p, h in reg.items():
            validate_relpath(p)
            if not isinstance(h, str) or len(h) != 64:
                raise ManagementCorruptError(f"state: bad digest for registered path {p!r}")
        cleanup = data.get("cleanup")
        if cleanup is not None:
            _validate_batch_shape(cleanup, "state.cleanup")
        batches = data.get("batches", [])
        if not isinstance(batches, list):
            raise ManagementCorruptError("state: batches must be a list")
    except ManagementCorruptError:
        raise
    except (KeyError, TypeError) as e:
        raise ManagementCorruptError(f"lifecycle state malformed: {e}") from e
    state = LifecycleState(root, data)
    # always re-verify the bound manifest; a drift/corruption must not be
    # mistaken for an unmanaged directory.
    state.read_manifest_raw()
    return state


def _validate_batch_shape(batch: dict, where: str) -> None:
    if not isinstance(batch, dict):
        raise ManagementCorruptError(f"{where} must be an object")
    _require_str(batch, "batch_id", where)
    for key in ("ref_generation", "retire_snapshots"):
        if key not in batch:
            raise ManagementCorruptError(f"{where}: missing {key!r}")
    if not isinstance(batch["ref_generation"], int):
        raise ManagementCorruptError(f"{where}: bad ref_generation")
    if not isinstance(batch["retire_snapshots"], list):
        raise ManagementCorruptError(f"{where}: retire_snapshots must be a list")
    items = batch.get("items")
    if not isinstance(items, list):
        raise ManagementCorruptError(f"{where}: items must be a list")
    for it in items:
        if not isinstance(it, dict) or not isinstance(it.get("path"), str) \
                or not isinstance(it.get("sha256"), str):
            raise ManagementCorruptError(f"{where}: malformed cleanup item")
        if it.get("status") not in ("pending", "reclaimed", "blocked"):
            raise ManagementCorruptError(f"{where}: bad item status")


def onboard(root) -> LifecycleState:
    """Bring an existing table directory under lifecycle management.

    Binds the exact current ``table.json`` bytes and every registered path
    (regular files only, no link/escape components, no digest collisions on a
    shared canonical path).  Afterwards ``table.json`` and the registered
    files are made read-only.  Fails if the directory is already managed.
    """
    root = Path(root).resolve()
    state_dir = root / STATE_DIR
    if (state_dir / STATE_FILE).exists():
        raise AlreadyManagedError(f"{root} is already under lifecycle management")

    tj = root / "table.json"
    if not tj.is_file():
        from ..errors import SourceFileMissing

        raise SourceFileMissing(f"table.json not found under {root}")
    with open(tj, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    if raw.get("format") != FORMAT:
        from ..errors import ContractError

        raise ContractError(f"unsupported format: {raw.get('format')!r}")

    # full contract validation first (schemas, snapshot graph, file entries)
    contract = TableContract(root, raw)

    # path admission first: lexical validity, regular files, no link or
    # escape components, and no digest collisions on a shared canonical path.
    # Every manifest entry is admitted -- even when the same path was seen in
    # an earlier snapshot -- so a link/swap introduced on one occurrence can
    # never slip past via dedup.  Hashing only happens after full admission.
    registered: dict[str, str] = {}
    for snap in contract.snapshots.values():
        for entry in snap.files:
            rel = validate_relpath(entry.path)
            _check_regular_unlinked(root, rel)
            if rel in registered:
                if registered[rel] != entry.sha256:
                    raise ManagedPathError(
                        f"canonical path {rel!r} registered with conflicting "
                        f"sha256 digests"
                    )
            else:
                registered[rel] = entry.sha256

    # now verify the bound bytes for every admitted path
    for rel, expected in registered.items():
        actual = sha256_of(root / rel)
        if actual != expected:
            from ..errors import SourceVersionMismatch

            raise SourceVersionMismatch(
                f"onboarding refused: {rel} hashes to {actual[:16]}... but "
                f"table.json pins {expected[:16]}..."
            )

    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / GUARD_DIR).mkdir(exist_ok=True)
    (state_dir / PLAN_DIR).mkdir(exist_ok=True)
    (state_dir / STAGING_DIR).mkdir(exist_ok=True)

    data = {
        "format": STATE_FORMAT,
        "table_id": contract.table_id,
        "manifest_sha256": hashlib.sha256(
            canonical_manifest_bytes(raw)
        ).hexdigest(),
        "ref_generation": 1,
        # The first committed configuration is intentionally empty; callers
        # replace it explicitly via set_references (which requires >=1 branch).
        "references": {},
        "retired_snapshots": [],
        "registered_paths": dict(sorted(registered.items())),
        "cleanup": None,
        "batches": [],
    }
    state = LifecycleState(root, data)
    state.persist()

    # read-only enforcement on the bound artifacts (defence in depth).
    for rel in registered:
        try:
            os.chmod(root / rel, 0o444)
        except OSError:
            pass
    try:
        os.chmod(tj, 0o444)
    except OSError:
        pass
    return state


@contextmanager
def managed_state(root):
    """Convenience: load state and hold the state lock for a mutation block."""
    state = load_state(root)
    with state.state_lock():
        yield state
