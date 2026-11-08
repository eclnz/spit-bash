"""The state file, a log of records by key, and the writes that change it."""

from __future__ import annotations

import fcntl
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from ..model import DagError
from .record import Record

# The one version of the state file this store reads and writes.
VERSION = 7


class StateHeader(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    # A state of any other version is refused, not read.
    version: Literal[VERSION]


class StateEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    key: str
    # None forgets the key.
    record: Record | None


@dataclass(frozen=True)
class StateWrite:
    """One change to the store: record `record` under `key`, or forget `key` when it is None."""

    key: str
    record: Record | None


class Store:
    """Records by key, kept as a log of JSON lines.

    The first line is a header. Each later line records or forgets one key, and
    the last line for a key wins, so recording one appends one line instead of
    rewriting the file. `compact` rewrites the file with one line per key.
    Failures to read or lock the file raise `DagError`.
    """

    def __init__(self, path: Path, write: bool = True):
        self.path = path
        self.jobs: dict[str, Record] = {}
        self._log: IO[str] | None = None
        self._lock: IO[str] | None = None
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except OSError as exc:
            raise DagError(f"cannot read state at {path}: {exc}") from exc
        if not text:
            return
        lines = text.split("\n")
        # A run that was killed mid-write may leave a partial last line, which
        # holds no complete record and is ignored.
        complete, partial = lines[:-1], lines[-1]
        try:
            StateHeader.model_validate_json(complete[0] if complete else partial)
            for line in complete[1:]:
                entry = StateEntry.model_validate_json(line)
                if entry.record is None:
                    self.jobs.pop(entry.key, None)
                else:
                    self.jobs[entry.key] = entry.record
        except ValidationError as exc:
            raise DagError(f"cannot read state at {path}: {exc}. If it was written by an older "
                           f"spit-bash, delete it to start from fresh records.") from exc
        # Reading a state to plan never rewrites it; a run tidies a truncated log.
        if write and partial and complete:
            self.compact()

    def lock(self) -> None:
        """Hold the state for this process until `close`, or fail if another holds it."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        stream = open(self.path.with_name(self.path.name + ".lock"), "w", encoding="utf-8")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            stream.close()
            raise DagError(f"another spit-bash run is using {self.path}") from None
        self._lock = stream

    def record(self, key: str, record: Record) -> None:
        self._append(StateEntry(key=key, record=record))
        self.jobs[key] = record

    def forget(self, key: str) -> None:
        """Drop the key's record; a key with none writes nothing."""
        if self.jobs.pop(key, None) is not None:
            self._append(StateEntry(key=key, record=None))

    def apply(self, writes: tuple[StateWrite, ...]) -> None:
        """Perform each write in order, as `record` and `forget` do."""
        for write in writes:
            if write.record is None:
                self.forget(write.key)
            else:
                self.record(write.key, write.record)

    def assume(self, writes: tuple[StateWrite, ...]) -> None:
        """Make each write to the records in memory only, as a plan does to see what a run would store."""
        for write in writes:
            if write.record is None:
                self.jobs.pop(write.key, None)
            else:
                self.jobs[write.key] = write.record

    def _append(self, entry: StateEntry) -> None:
        if self._log is None:
            if not self.path.exists():
                self.compact()
            self._log = open(self.path, "a", encoding="utf-8")
        self._log.write(entry.model_dump_json() + "\n")
        self._log.flush()

    def compact(self) -> None:
        """Replace the file, atomically, with the header and one line per key in key order."""
        if self._log is not None:
            self._log.close()
            self._log = None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lines = [StateHeader(version=VERSION).model_dump_json()]
        lines.extend(StateEntry(key=key, record=record).model_dump_json() for key, record in sorted(self.jobs.items()))
        fd, temporary = tempfile.mkstemp(prefix=".state-", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write("\n".join(lines) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def close(self) -> None:
        """Compact the log if this process appended to it, and release the lock."""
        if self._log is not None:
            self.compact()
        if self._lock is not None:
            self._lock.close()
            self._lock = None
