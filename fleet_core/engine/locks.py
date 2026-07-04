"""fleet_core.engine.locks — single-writer flock guard for trades.db.

Spec: p3_rollout.md §4 "Mutual exclusion (dual-writer guard, F10)":
    (a) flock: both old bot (tiny pre-cutover patch) and engine take an
        exclusive flock on `data/trades.db.lock` at startup and REFUSE to
        run (exit 1, CRITICAL) if held — the authoritative guard, covers
        manual starts and systemd races.

ZERO EXEMPTIONS by construction (p3_rollout.md §3b): there is no bypass
flag, no env override, no "force" argument anywhere in this module.  The
write-canary runs IN-PROCESS via the admin socket precisely so that the
engine's flock stays held with no carve-out.

The lock is advisory flock(2) on a sibling ``<db>.lock`` file.  It is held
for the LIFETIME of the process (fd kept open; released by the OS on any
death, incl. kill -9 — no stale-lock cleanup problem).
"""
from __future__ import annotations

import errno
import fcntl
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Union

log = logging.getLogger("fleet_core.engine.locks")

__all__ = ["DBLockHeld", "DBLock", "lock_path_for", "acquire_db_lock", "acquire_or_die"]


class DBLockHeld(RuntimeError):
    """The exclusive db lock is already held by another process."""

    def __init__(self, lock_path: Path, holder: str = ""):
        self.lock_path = Path(lock_path)
        self.holder = holder
        msg = "db lock already held: %s" % lock_path
        if holder:
            msg += " (holder: %s)" % holder
        super().__init__(msg)


def lock_path_for(db_path: Union[str, Path]) -> Path:
    """Canonical lock-file path for a database file: ``<db_path>.lock``.

    For the live db this yields exactly the rollout §4 path
    ``data/trades.db.lock``.
    """
    p = Path(db_path)
    return p.with_name(p.name + ".lock")


class DBLock:
    """An acquired exclusive flock.  Keep a reference alive for the whole
    process lifetime — dropping/closing it releases the lock."""

    def __init__(self, lock_path: Path, fd: int):
        self.lock_path = Path(lock_path)
        self._fd = fd  # type: Optional[int]

    @property
    def held(self) -> bool:
        return self._fd is not None

    def release(self) -> None:
        """Explicit release (tests / clean shutdown).  Idempotent."""
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
            log.info("released db lock %s", self.lock_path)

    def __enter__(self) -> "DBLock":
        return self

    def __exit__(self, *exc) -> None:
        self.release()

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return "DBLock(path=%r, held=%s)" % (str(self.lock_path), self.held)


def _read_holder(lock_path: Path) -> str:
    try:
        return lock_path.read_text().strip()[:200]
    except OSError:
        return ""


def acquire_db_lock(db_path: Union[str, Path], role: str = "engine") -> DBLock:
    """Take the exclusive non-blocking flock for ``db_path``.

    Returns a :class:`DBLock` on success; raises :class:`DBLockHeld` if any
    other process holds it.  Never blocks, never retries, never bypasses.
    """
    lock_path = lock_path_for(db_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        holder = _read_holder(lock_path)
        os.close(fd)
        if e.errno in (errno.EACCES, errno.EAGAIN):
            raise DBLockHeld(lock_path, holder)
        raise
    # Diagnostic breadcrumb only — the flock itself is the guard.
    try:
        os.ftruncate(fd, 0)
        os.write(fd, ("pid=%d role=%s\n" % (os.getpid(), role)).encode())
        os.fsync(fd)
    except OSError:
        pass
    log.info("acquired exclusive db lock %s (role=%s pid=%d)", lock_path, role, os.getpid())
    return DBLock(lock_path, fd)


def acquire_or_die(db_path: Union[str, Path], role: str = "engine") -> DBLock:
    """Rollout §4 startup semantics: acquire or CRITICAL + exit(1)."""
    try:
        return acquire_db_lock(db_path, role=role)
    except DBLockHeld as e:
        log.critical(
            "CRITICAL: dual-writer guard tripped — %s is already flocked%s. "
            "Exactly one writer may run (p3_rollout.md §4 F10). Refusing to start.",
            e.lock_path,
            (" by [%s]" % e.holder) if e.holder else "",
        )
        sys.exit(1)
