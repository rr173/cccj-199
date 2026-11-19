"""Read-protection registry: real read lifecycles, not timeouts.

A *read guard* is a claim that one or more snapshots are about to be read and
their physical files must not be reclaimed until the read ends.  Guards are
first-class persistent objects (one token file per guard under
``.lakescan/guards/``) so the rule survives process boundaries:

* the registry is consulted by every reclamation commit, under a dedicated
  ``guards.lock`` that is distinct from the state lock (the lock ordering is
  always state-lock then guard-lock);
* a killed/crashed reader cannot create permanent retention: token files
  record pid + process start time and are treated as stale once that process
  is gone.  Stale tokens are swept (under guard-lock) before every commit
  decision;
* normal completion, explicit :meth:`ReadGuard.close` and abnormal exits all
  release the guard (context-manager + ``__del__`` best effort);
* streaming iterators that pause between batches keep their guard for the
  whole iterator lifetime.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Iterable, Optional

from ..errors import SnapshotRetiredError
from .state import GUARD_DIR, LifecycleState


def _proc_start_time(pid: int) -> Optional[int]:
    """Boot-time clock tick at which ``pid`` started (Linux /proc), else None."""
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="ascii") as fh:
            content = fh.read()
    except OSError:
        return None
    # /proc/<pid>/stat: comm is parenthesized and may contain spaces/parens;
    # field 22 (starttime) is the 20th field after the closing paren.
    rparen = content.rfind(")")
    if rparen < 0:
        return None
    fields = content[rparen + 2:].split()
    try:
        return int(fields[19])
    except (IndexError, ValueError):
        return None


def _pid_alive(pid: int, start_time: Optional[int]) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # process exists but is not ours; the start-time cross-check still
        # distinguishes a recycled pid when /proc is available.
        pass
    except OSError:
        return False
    if start_time is None:
        return True
    current = _proc_start_time(pid)
    return current is not None and current == start_time


class ReadGuard:
    """A held read-protection claim.  Always release it (``with`` or close())."""

    def __init__(self, state: LifecycleState, token_path: Path, pid: int,
                 start_time: Optional[int], snapshot_ids: tuple, reason: str):
        self._state = state
        self._token_path = token_path
        self._pid = pid
        self._start_time = start_time
        self.snapshot_ids = tuple(snapshot_ids)
        self.reason = reason
        self._closed = False

    @property
    def token(self) -> str:
        return self._token_path.name

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            os.unlink(self._token_path)
        except FileNotFoundError:
            # a concurrent sweep already removed a stale-looking token
            pass
        except OSError:
            pass

    def __enter__(self) -> "ReadGuard":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    def __del__(self):  # best-effort cleanup on abnormal interpreter paths
        try:
            self.close()
        except Exception:
            pass


def _guard_dir(state: LifecycleState) -> Path:
    d = state.dir / GUARD_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_token(d: Path, payload: dict) -> Path:
    name = "guard-" + uuid.uuid4().hex + ".json"
    path = d / name
    fd, tmp = tempfile.mkstemp(prefix=name + ".", suffix=".tmp", dir=str(d))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path


def _read_token(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def sweep_stale(state: LifecycleState) -> int:
    """Remove token files whose owning process no longer exists.

    Must be called while holding the guard lock.  Returns the number swept.
    Unparseable token files are removed as well -- a half-written token can
    never constitute valid protection.
    """
    swept = 0
    for path in _guard_dir(state).glob("guard-*.json"):
        data = _read_token(path)
        if data is None:
            try:
                path.unlink()
                swept += 1
            except OSError:
                pass
            continue
        pid = data.get("pid")
        start = data.get("process_start_time")
        if not isinstance(pid, int) or not _pid_alive(pid, start):
            try:
                path.unlink()
                swept += 1
            except OSError:
                pass
    return swept


def live_guards(state: LifecycleState) -> list:
    """Snapshot of currently live guards (token payloads), oldest first."""
    out = []
    for path in sorted(_guard_dir(state).glob("guard-*.json")):
        data = _read_token(path)
        if data is None:
            continue
        pid = data.get("pid")
        start = data.get("process_start_time")
        if isinstance(pid, int) and _pid_alive(pid, start):
            data["_token"] = path.name
            out.append(data)
    return out


def protected_snapshot_set(state: LifecycleState) -> set:
    """Union of snapshot ids held by live guards (caller holds guard lock)."""
    protected: set = set()
    for payload in live_guards(state):
        ids = payload.get("snapshot_ids")
        if isinstance(ids, list):
            protected.update(s for s in ids if isinstance(s, str))
    return protected


def protect(state: LifecycleState, snapshot_ids: Iterable[str],
            reason: str = "read") -> ReadGuard:
    """Register protection for ``snapshot_ids``; fail if any was retired.

    The retirement check and the token write happen atomically under the
    guard lock, which closes the read/reclaim race: either this token is
    visible before the reclaim commit acquires the guard lock (retirement is
    blocked), or the retirement commits first and this call raises before any
    input file is opened.
    """
    snapshot_ids = tuple(dict.fromkeys(snapshot_ids))
    retired = set(state.retired)
    already = [s for s in snapshot_ids if s in retired]
    if already:
        raise SnapshotRetiredError(
            f"snapshot(s) {sorted(already)} are retired and can no longer be read"
        )
    pid = os.getpid()
    start = _proc_start_time(pid)
    payload = {
        "pid": pid,
        "process_start_time": start,
        "snapshot_ids": list(snapshot_ids),
        "reason": reason,
    }
    with state.guard_lock():
        # re-check under the lock: a concurrent commit may have retired them
        retired_now = set(state.retired)
        raced = [s for s in snapshot_ids if s in retired_now]
        if raced:
            raise SnapshotRetiredError(
                f"snapshot(s) {sorted(raced)} were retired concurrently; "
                "read refused before opening input files"
            )
        sweep_stale(state)
        path = _write_token(_guard_dir(state), payload)
    return ReadGuard(state, path, pid, start, snapshot_ids, reason)
