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

from .content import content_hash, text_hash
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
    # What each file or folder held, as `content.content_hash` gives it, so a
    # file that was only touched is not a change; None when the run did not hash.
    input_hashes: dict[str, str] | None = None
    output_hashes: dict[str, str] | None = None
    # The content of each program the job's commands start with, as `program_hashes`
    # gives it; None when the run did not look, which is unknown, not changed.
    programs: dict[str, str] | None = None


class StateHeader(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    # Version 3 records the checks each job passed; a version 2 record has none.
    # Version 4 records the hash of each file; an older record has none.
    version: Literal[2, 3, 4]


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
        lines = [StateHeader(version=4).model_dump_json()]
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
    # Current as it stands, but a job upstream will run: `run` decides once it
    # has, and skips this job if what it reads came out the same.
    MAYBE = "maybe"


@dataclass(frozen=True)
class Decision:
    job: Job
    status: Status
    reason: str
    # A current job whose files were touched but not changed: its record with the
    # files' new stamps, which `run` stores so the next plan need not hash them.
    refresh: RunRecord | None = None


def hashes(dag: Dag, paths: tuple[str, ...]) -> dict[str, str]:
    return {path: h for path in paths if (h := content_hash(str(dag.path(path)), path in dag.folders)) is not None}


def _unchanged(dag: Dag, paths: tuple[str, ...], now: dict[str, list[int] | None],
               recorded: dict[str, list[int]], recorded_hashes: dict[str, str] | None, hashing: bool) -> bool:
    """Whether these files are as the record has them: by stamp, else, where they differ, by content."""
    differing = [path for path in paths if now[path] != recorded.get(path)]
    if not differing:
        return True
    if not hashing or not recorded_hashes:
        return False
    return all(path in recorded_hashes and recorded_hashes[path] == content_hash(str(dag.path(path)), path in dag.folders)
               for path in differing)


def resolve_program(dag: Dag, name: str) -> Path | None:
    """The file a command that starts with `name` runs, or None when it cannot be run.

    SPIT leaves a program named by a path out of `executables`. It is run from
    the root, so it is looked for there.
    """
    if "/" in name:
        program = dag.root / name
        return program if program.is_file() and os.access(program, os.X_OK) else None
    found = shutil.which(name)
    return None if found is None else Path(found)


def _commands(job: Job) -> tuple[tuple[str, ...], ...]:
    return tuple(command for command in (job.command, *job.verify, *(check.command for check in job.checks))
                 if command is not None)


def _argument_files(dag: Dag, job: Job) -> tuple[str, ...]:
    """Files under the root that a job's commands name but the job does not declare.

    An argument, or the text after an `=` in one as in `--config=run.yaml`, that
    names such a file is taken to be read by the command, as a script is in
    `python3 tool.py`. Its input and output files are judged already.
    """
    declared = {*job.inputs, *job.outputs}
    found: dict[str, None] = {}
    for command in _commands(job):
        for argument in command[1:]:
            for text in (argument, argument.partition("=")[2]):
                if text and text not in declared and text not in found:
                    try:
                        if dag.path(text).is_file():
                            found[text] = None
                    except (DagError, OSError, ValueError):
                        pass  # outside the root, or not a path at all
    return tuple(found)


def job_programs(dag: Dag, job: Job) -> tuple[str, ...]:
    """What a job runs besides its declared files, once each: the programs its commands,
    `verify` commands and checks start with, and the files its commands name."""
    return tuple(dict.fromkeys((*(command[0] for command in _commands(job)), *_argument_files(dag, job))))


def _interpreter(program: Path) -> Path | None:
    """The interpreter a script's `#!` line names, as `#!/usr/bin/env python3` does."""
    try:
        with open(program, "rb") as stream:
            first = stream.readline(256)
    except OSError:
        return None
    if not first.startswith(b"#!"):
        return None
    words = first[2:].decode(errors="replace").split()
    if words and os.path.basename(words[0]) == "env":
        words = [word for word in words[1:] if not word.startswith("-")]
    if not words:
        return None
    found = shutil.which(words[0])
    return None if found is None else Path(found)


def program_hashes(dag: Dag, tracked: dict[str, tuple[str, ...]] | None = None) -> dict[str, str]:
    """A hash for each name in `job_programs`, over all the DAG's jobs, once each.

    A program's covers its file, the interpreter its `#!` line names, and each
    path given for it in `tracked` (a file or a folder, such as a virtualenv's
    packages). A named file's is its content. A program that cannot be found is
    left out: `plan` blocks the jobs that need it.
    """
    starts = {command[0] for job in dag.jobs for command in _commands(job)}
    result: dict[str, str] = {}
    for name in dict.fromkeys(name for job in dag.jobs for name in job_programs(dag, job)):
        parts: list[str | None] = []
        if name in starts:
            program = resolve_program(dag, name)
            if program is None:
                continue
            interpreter = _interpreter(program)
            parts = [content_hash(str(program)), None if interpreter is None else content_hash(str(interpreter))]
            for path in (tracked or {}).get(name, ()):
                full = os.path.join(dag.root, path)
                parts.append(content_hash(full, os.path.isdir(full)))
        else:
            parts = [content_hash(str(dag.path(name)))]
        present = [part for part in parts if part is not None]
        if present:
            result[name] = present[0] if len(present) == 1 else text_hash(*present)
    return result


def _missing_programs(dag: Dag) -> set[str]:
    """Programs that commands start with but that cannot be run."""
    names = dict.fromkeys((*dag.executables, *(command[0] for job in dag.jobs for command in _commands(job))))
    return {name for name in names if resolve_program(dag, name) is None}


# A job planned with one of these will have its inputs rewritten, or may.
UPSTREAM = (Status.RUN, Status.MAYBE)


def judge(dag: Dag, job: Job, state: State, force: bool, hashing: bool,
          programs: dict[str, str] | None = None) -> Decision:
    """Whether a command job is current on the files it has now, ignoring the jobs upstream.

    `plan` asks it for every job, and `run` asks it again for a job planned
    `maybe`, once the jobs it waits for have finished.
    """
    if any(value is None for value in stamps(dag, job.outputs).values()):
        return Decision(job, Status.RUN, "output missing")
    if any(value is None for value in stamps(dag, job.inputs).values()):
        return Decision(job, Status.BLOCKED, "input is missing")
    if force:
        return Decision(job, Status.RUN, "forced")
    record = state.jobs.get(job_key(job))
    now_in, now_out = stamps(dag, job.inputs), stamps(dag, job.outputs)
    if record is None:
        return Decision(job, Status.RUN, "no successful run recorded")
    if record.fingerprint != job.fingerprint:
        return Decision(job, Status.RUN, "job fingerprint changed")
    if programs is not None and record.programs is not None:
        changed = [name for name in job_programs(dag, job) if name in programs and name in record.programs
                   and programs[name] != record.programs[name]]
        # A program that was not recorded, or is not found now, is no change: `plan` blocks the latter.
        if changed:
            return Decision(job, Status.RUN, "program changed: " + ", ".join(changed))
    if not _unchanged(dag, job.inputs, now_in, record.inputs, record.input_hashes, hashing):
        return Decision(job, Status.RUN, "input changed")
    if not _unchanged(dag, job.outputs, now_out, record.outputs, record.output_hashes, hashing):
        return Decision(job, Status.RUN, "output changed")
    # Touched files hold what they held: keep the new stamps.
    refresh = None if (now_in, now_out) == (record.inputs, record.outputs) else record.model_copy(
        update={"inputs": now_in, "outputs": now_out})
    if record.checks != job.checks_key:
        reason = "checks changed" if record.checks is not None else "checks not yet run on these files"
        return Decision(job, Status.CHECK, reason, refresh)
    return Decision(job, Status.SKIP, "current", refresh)


def plan(dag: Dag, state: State, force: bool = False, hashing: bool = True,
         programs: dict[str, str] | None = None) -> tuple[Decision, ...]:
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
        elif any(by_id[dep].status in UPSTREAM for dep in job.depends_on):
            if not hashing or any(value is None for value in stamps(dag, job.inputs).values()):
                decision = Decision(job, Status.RUN, "dependency will run")
            else:
                decision = judge(dag, job, state, force, hashing, programs)
                if decision.status in (Status.SKIP, Status.CHECK):
                    decision = Decision(job, Status.MAYBE, "runs if an upstream output changes")
        else:
            decision = judge(dag, job, state, force, hashing, programs)
        if decision.status in (Status.RUN, Status.CHECK, Status.MAYBE):
            commands = [check.command for check in job.checks]
            if decision.status in (Status.RUN, Status.MAYBE):
                commands += [job.command, *job.verify]
            programs = sorted({command[0] for command in commands if command is not None} & missing_programs)
            if programs:
                decision = Decision(job, Status.BLOCKED, "program not found: " + ", ".join(programs))
        decisions.append(decision)
        by_id[job.id] = decision
    return tuple(decisions)


def _used(dag: Dag, job: Job, programs: dict[str, str] | None) -> dict[str, str] | None:
    """The part of `programs` this job's commands start with, or None when programs were not looked at."""
    return None if programs is None else {name: programs[name] for name in job_programs(dag, job) if name in programs}


def apply_refreshes(decisions: tuple[Decision, ...], state: State) -> None:
    """Store the new stamps of jobs whose files were touched but not changed."""
    for decision in decisions:
        if decision.refresh is not None and decision.status in (Status.SKIP, Status.CHECK):
            state.record(job_key(decision.job), decision.refresh)


class AdoptStatus(Enum):
    ADOPTED = "adopted"
    LEFT = "left"


@dataclass(frozen=True)
class Adoption:
    job: Job
    status: AdoptStatus
    reason: str


def adopt(dag: Dag, state: State, programs: dict[str, str] | None = None) -> tuple[Adoption, ...]:
    """Record every command job whose files exist as current, without running it.

    No checks are recorded, so the next run checks the adopted files. The
    programs found now are recorded as the ones that made them.
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
            state.record(job_key(job), RunRecord(fingerprint=job.fingerprint, inputs=inputs, outputs=outputs,
                                                 programs=_used(dag, job, programs)))
            adoption = Adoption(job, AdoptStatus.ADOPTED, "outputs exist")
        adoptions.append(adoption)
    return tuple(adoptions)
