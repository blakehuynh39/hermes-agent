"""Shared skill-tree locking helpers.

Hermes can run multiple processes against the same ``$HERMES_HOME``.  Any code
path that mutates ``$HERMES_HOME/skills`` must take this lock so skill edits,
bundled skill syncs, and hub installs do not interleave.
"""

from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import threading
from typing import Iterator, TextIO

from hermes_constants import get_hermes_home

try:  # pragma: no cover - exercised on Unix, fallback keeps Windows usable
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]


_thread_state = threading.local()
_process_fallback_lock = threading.RLock()


def skills_lock_path() -> Path:
    """Return the configured cross-process skills lock path."""
    override = os.getenv("HERMES_SKILLS_LOCK_PATH", "").strip()
    if override:
        return Path(override)
    return get_hermes_home() / ".locks" / "skills.lock"


@contextmanager
def skills_write_lock(lock_path: Path | None = None) -> Iterator[None]:
    """Take the shared skills write lock.

    The lock is re-entrant within a thread so high-level operations can call
    lower-level helpers that also take the lock without deadlocking.
    """
    depth = int(getattr(_thread_state, "skills_lock_depth", 0) or 0)
    if depth > 0:
        _thread_state.skills_lock_depth = depth + 1
        try:
            yield
        finally:
            _thread_state.skills_lock_depth = depth
        return

    path = lock_path or skills_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    lock_file: TextIO | None = None
    try:
        lock_file = path.open("a+", encoding="utf-8")
        if fcntl is None:
            _process_fallback_lock.acquire()
        else:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        _thread_state.skills_lock_depth = 1
        yield
    finally:
        _thread_state.skills_lock_depth = 0
        try:
            if lock_file is not None and fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            elif fcntl is None:
                _process_fallback_lock.release()
        finally:
            if lock_file is not None:
                lock_file.close()
