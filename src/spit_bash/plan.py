"""Decide which jobs are current from paths, fingerprints and file stamps."""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import IO, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .dag import Dag, DagError, Job


def job_key(job: Job) -> str:
    # Job IDs may change when SPIT resolves a different inventory. Paths are
    # relative to the root, so a dataset keeps its state when it moves.
    return json.dumps(sorted(job.outputs), separators=(",", ":"))


def stamp(path: Path, folder: bool = False) -> list[int] | None:
    """A file's size and modification time, or None when it is not a file.

    A folder's stamp is the total size of the files under it, the latest
    modification time of any of them or of any folder under it, and how many
    files it holds, or None when it is not a folder. A folder's own time
    changes only when an entry directly in it is added, removed or renamed, so
    a change deeper down shows only in what is under it.
    """
    if folder:
        return _folder_stamp(path)
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    return [info.st_size, info.st_mtime_ns] if path.is_file() else None


def _folder_stamp(path: Path) -> list[int] | None:
    if not path.is_dir():
        return None
    size, latest, count = 0, path.stat().st_mtime_ns, 0
    for directory, folders, files in os.walk(path):
        for name in folders:
            try:
                latest = max(latest, os.stat(os.path.join(directory, name)).st_mtime_ns)
            except FileNotFoundError:
                pass
        for name in files:
            try:
                info = os.stat(os.path.join(directory, name))
            except FileNotFoundError:
                # A link to nothing names no file.
                continue
            size += info.st_size
            latest = max(latest, info.st_mtime_ns)
            count += 1
    return [size, latest, count]


def stamps(dag: Dag, paths: tuple[str, ...]) -> dict[str, list[int] | None]:
    return {path: stamp(dag.path(path), path in dag.folders) for path in paths}


# A file's size and time, or a folder's, with how many files it holds. Both
# start with a size and a time, so either can be compared by time.
FileStamp = Annotated[list[int], Field(min_length=2, max_length=3)]


class RunRecord(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    fingerprint: str
    inputs: dict[str, FileStamp]
    outputs: dict[str, FileStamp]
    # The checks the job passed with on these files, as `Job.checks_key`
    # gives them; None for no checks, and for a job adopted without running any.
    checks: str | None = None


class StateHeader(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    # Version 3 records the checks each job passed; a version 2 record has none.
    version: Literal[2, 3]


class StateEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    key: str
    record: RunRecord | None


class State:
    """Successful runs, kept as a log of JSON lines.

    The first line is a header. Each later line records or forgets one job, and
    the last line for a key wins, so finishing a job appends one line instead of
    rewriting the file. `compact` rewrites the file with one line per job.
    """

    def __init__(self, path: Path):
        self.path = path
        self.jobs: dict[str, RunRecord] = {}
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
            raise DagError(f"cannot read state at {path}: {exc}") from exc
        if partial and complete:
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

    def record(self, key: str, record: RunRecord) -> None:
        self._append(StateEntry(key=key, record=record))
        self.jobs[key] = record

    def forget(self, key: str) -> None:
        if self.jobs.pop(key, None) is not None:
            self._append(StateEntry(key=key, record=None))

    def _append(self, entry: StateEntry) -> None:
        if self._log is None:
            if not self.path.exists():
                self.compact()
            self._log = open(self.path, "a", encoding="utf-8")
        self._log.write(entry.model_dump_json() + "\n")
        self._log.flush()

    def compact(self) -> None:
        if self._log is not None:
            self._log.close()
            self._log = None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lines = [StateHeader(version=3).model_dump_json()]
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
        if self._log is not None:
            self.compact()
        if self._lock is not None:
            self._lock.close()
            self._lock = None


class Status(Enum):
    RUN = "run"
    # Current, but its checks have not passed on these files: run them alone.
    CHECK = "check"
    SKIP = "skip"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class Decision:
    job: Job
    status: Status
    reason: str


def _missing_programs(dag: Dag) -> set[str]:
    """Programs that commands start with but that cannot be run."""
    missing = {name for name in dag.executables if shutil.which(name) is None}
    for job in dag.jobs:
        for command in (job.command, *job.verify, *(check.command for check in job.checks)):
            # SPIT leaves a program named by a path out of `executables`. It is
            # run from the root, so check it there.
            if command is not None and "/" in command[0] and command[0] not in missing:
                program = dag.root / command[0]
                if not (program.is_file() and os.access(program, os.X_OK)):
                    missing.add(command[0])
    return missing


def plan(dag: Dag, state: State, force: bool = False) -> tuple[Decision, ...]:
    decisions: list[Decision] = []
    by_id: dict[int, Decision] = {}
    missing_external = {path for path in dag.external_inputs if stamp(dag.path(path), path in dag.folders) is None}
    missing_programs = _missing_programs(dag)
    for job in dag.jobs:
        missing = [path for path in job.inputs if path in missing_external]
        blocked = [dep for dep in job.depends_on if by_id[dep].status is Status.BLOCKED]
        if missing:
            decision = Decision(job, Status.BLOCKED, "missing external input: " + ", ".join(missing))
        elif blocked:
            decision = Decision(job, Status.BLOCKED, "dependency blocked: " + ", ".join(map(str, blocked)))
        elif job.command is None:
            outputs = stamps(dag, job.outputs)
            if any(value is None for value in outputs.values()):
                decision = Decision(job, Status.BLOCKED, "no command and output is missing")
            elif any(by_id[dep].status is Status.RUN for dep in job.depends_on):
                decision = Decision(job, Status.BLOCKED, "no command and dependency will run")
            elif any(value is None for value in stamps(dag, job.inputs).values()):
                decision = Decision(job, Status.BLOCKED, "input is missing")
            elif job.inputs and max(value[1] for value in stamps(dag, job.inputs).values()) > min(value[1] for value in outputs.values()):
                decision = Decision(job, Status.BLOCKED, "no command and output is older than an input")
            elif job.checks:
                decision = Decision(job, Status.CHECK, "no command; outputs exist, checking them")
            else:
                decision = Decision(job, Status.SKIP, "no command; outputs exist")
        elif any(by_id[dep].status is Status.RUN for dep in job.depends_on):
            decision = Decision(job, Status.RUN, "dependency will run")
        elif any(value is None for value in stamps(dag, job.outputs).values()):
            decision = Decision(job, Status.RUN, "output missing")
        elif any(value is None for value in stamps(dag, job.inputs).values()):
            decision = Decision(job, Status.BLOCKED, "input is missing")
        elif force:
            decision = Decision(job, Status.RUN, "forced")
        else:
            record = state.jobs.get(job_key(job))
            if record is None:
                decision = Decision(job, Status.RUN, "no successful run recorded")
            elif record.fingerprint != job.fingerprint:
                decision = Decision(job, Status.RUN, "job fingerprint changed")
            elif record.inputs != stamps(dag, job.inputs):
                decision = Decision(job, Status.RUN, "input changed")
            elif record.outputs != stamps(dag, job.outputs):
                decision = Decision(job, Status.RUN, "output changed")
            elif record.checks != job.checks_key:
                reason = "checks changed" if record.checks is not None else "checks not yet run on these files"
                decision = Decision(job, Status.CHECK, reason)
            else:
                decision = Decision(job, Status.SKIP, "current")
        if decision.status in (Status.RUN, Status.CHECK):
            commands = [check.command for check in job.checks]
            if decision.status is Status.RUN:
                commands += [job.command, *job.verify]
            programs = sorted({command[0] for command in commands if command is not None} & missing_programs)
            if programs:
                decision = Decision(job, Status.BLOCKED, "program not found: " + ", ".join(programs))
        decisions.append(decision)
        by_id[job.id] = decision
    return tuple(decisions)


class AdoptStatus(Enum):
    ADOPTED = "adopted"
    LEFT = "left"


@dataclass(frozen=True)
class Adoption:
    job: Job
    status: AdoptStatus
    reason: str


def adopt(dag: Dag, state: State) -> tuple[Adoption, ...]:
    """Record every command job whose files exist as current, without running it.

    No checks are recorded, so the next run checks the adopted files.
    """
    adoptions = []
    for job in dag.jobs:
        inputs = stamps(dag, job.inputs)
        outputs = stamps(dag, job.outputs)
        if job.command is None:
            adoption = Adoption(job, AdoptStatus.LEFT, "no command")
        elif any(value is None for value in outputs.values()):
            adoption = Adoption(job, AdoptStatus.LEFT, "output missing")
        elif any(value is None for value in inputs.values()):
            adoption = Adoption(job, AdoptStatus.LEFT, "input missing")
        else:
            state.record(job_key(job), RunRecord(fingerprint=job.fingerprint, inputs=inputs, outputs=outputs))
            adoption = Adoption(job, AdoptStatus.ADOPTED, "outputs exist")
        adoptions.append(adoption)
    return tuple(adoptions)
