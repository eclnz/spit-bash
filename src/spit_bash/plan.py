"""Decide which jobs are current from paths, fingerprints and file stamps."""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import tempfile
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import IO, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .dag import Dag, DagError, Job
from .files import Stamps, content_hash, stamps, stored_each


def job_key(job: Job) -> str:
    # Job IDs may change when SPIT resolves a different inventory, and paths
    # change when the pipeline's path rules do. What a job makes is its outputs'
    # products and entities, which neither changes.
    identities = sorted([output.product, output.entities] for output in job.produced)
    return json.dumps(identities, separators=(",", ":"))


# A file's size and time, or a folder's, with how many files it holds. Both
# start with a size and a time, so either can be compared by time.
FileStamp = Annotated[list[int], Field(min_length=2, max_length=3)]


class RunRecord(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    fingerprint: str
    # The fingerprint's parts as `Job.parts` gives them, to say which changed;
    # None for a record made before they were kept.
    parts: dict[str, str] | None = None
    inputs: dict[str, FileStamp]
    outputs: dict[str, FileStamp]
    # The checks the job passed with on these files, as `Job.checks_key`
    # gives them; None for no checks, and for a job adopted without running any.
    checks: str | None = None
    # What each file or folder held, as `content.content_hash` gives it, so a
    # file that was only touched is not a change; None when the run did not hash.
    input_hashes: dict[str, str] | None = None
    output_hashes: dict[str, str] | None = None
    # How long the job took, from its first check to its last, for estimating
    # how long a run will take; None for an adopted job, and before version 5.
    seconds: float | None = None
    # Where each output was made, by identity, so a run can find files
    # that a change of path rules left behind; None before the runner kept it.
    output_paths: dict[str, str] | None = None


class StateHeader(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    # Version 3 records the checks each job passed; a version 2 record has none.
    # Version 4 records the hash of each file; an older record has none.
    # Version 5 records how long each job took; an older record has no time.
    # Version 6 keys a record by the identity of what the job makes, not its
    # output paths. Version 7 names a record's files by their place in the job,
    # not by product and entities. A state from an earlier version is not read.
    version: Literal[7]


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

    def __init__(self, path: Path, write: bool = True):
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
        lines = [StateHeader(version=7).model_dump_json()]
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


def reconcile(dag: Dag, state: State, everything: Dag, persist: bool = False) -> tuple[tuple[str, str], ...]:
    """Give a record left by a renamed product, dimension, source or entity to the job that now does its work.

    A job with no record under its key takes an unclaimed record of the same
    work (the same fingerprint) when each input holds what the record's did, so
    the same work on the same bytes is reused whatever things are called.
    `everything` is the whole DAG, whose jobs claim their keys even when `dag`
    holds only some. Returns (old key, new key) for each record handed over;
    with `persist` the state file records the change, else only `state` does.
    """
    claimed = {job_key(job) for job in everything.jobs}
    orphans: dict[str, list[str]] = {}
    for key, record in sorted(state.jobs.items()):
        if key not in claimed and record.input_hashes is not None and record.output_hashes is not None:
            orphans.setdefault(record.fingerprint, []).append(key)
    if not orphans:
        return ()
    # What each file holds: as read, or, for a file a handed-over job has yet to make
    # at its new path, as that job's record says.
    held: dict[str, str | None] = {}

    def hash_of(job: Job, path: str) -> str | None:
        if path not in held:
            held[path] = content_hash(str(dag.path(path)), path in dag.folders)
        return held[path]

    handed = []
    for job in dag.jobs:
        key = job_key(job)
        if key in state.jobs or not job.commands or not orphans.get(job.fingerprint):
            continue
        for old in orphans[job.fingerprint]:
            record = state.jobs[old]
            assert record.input_hashes is not None and record.output_hashes is not None
            slots = {job.identities[path]: path for path in job.inputs}
            if set(slots) != set(record.input_hashes) or any(
                    hash_of(job, path) != record.input_hashes[slot] for slot, path in slots.items()):
                continue
            orphans[job.fingerprint].remove(old)
            if persist:
                state.forget(old)
                state.record(key, record)
            else:
                del state.jobs[old]
                state.jobs[key] = record
            for slot, path in job.output_paths.items():
                held[path] = record.output_hashes.get(slot)
            handed.append((old, key))
            break
    return tuple(handed)


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
    # The jobs upstream that will or may rewrite what this job reads, which it has to wait for.
    waits_for: tuple[int, ...] = ()
    # For a job blocked only because a job upstream is, the id of the job whose own cause it is.
    caused_by: int | None = None


# The start of the reason of a job blocked by a job upstream, which `Decision.caused_by` follows.
DEPENDENCY_BLOCKED = "dependency blocked: "


def _unchanged(dag: Dag, paths: dict[str, str], now: dict[str, list[int] | None],
               recorded: dict[str, list[int]], recorded_hashes: dict[str, str] | None, hashing: bool,
               verify: bool = False) -> bool:
    """Whether these files are as the record has them: by stamp, else, where they differ, by content.

    `paths` gives each artifact's current path by identity, as do `now` and the record. With
    `verify`, every file is compared by content, whatever its stamp, and a record without
    hashes counts as changed.
    """
    differing = list(paths) if verify else [key for key in paths if now[key] != recorded.get(key)]
    if not differing:
        return True
    if not hashing or not recorded_hashes:
        return False
    return all(key in recorded_hashes
               and recorded_hashes[key] == content_hash(str(dag.path(paths[key])), paths[key] in dag.folders)
               for key in differing)


def _missing_programs(dag: Dag) -> set[str]:
    """Programs that commands start with but that cannot be run."""
    missing = {name for name in dag.executables if shutil.which(name) is None}
    for job in dag.jobs:
        runs = (run.arguments for run in job.commands)
        for command in (*runs, *job.verify, *(check.command for check in job.checks)):
            # SPIT leaves a program named by a path out of `executables`. It is
            # run from the root, so check it there.
            if "/" in command[0] and command[0] not in missing:
                program = dag.root / command[0]
                if not (program.is_file() and os.access(program, os.X_OK)):
                    missing.add(command[0])
    return missing


# A job planned with one of these will have its inputs rewritten, or may.
UPSTREAM = (Status.RUN, Status.MAYBE)


PART_NAMES = {"commands": "command", "verify": "verify commands",
              "inputs": "the artifacts it reads", "outputs": "the artifacts it makes"}


def changed_parts(record: RunRecord, job: Job) -> str:
    """Why a job's fingerprint differs from its record's, naming the parts that changed when it can."""
    if record.parts is None:
        return "job fingerprint changed"
    names = [text for part, text in PART_NAMES.items() if record.parts.get(part) != job.parts.get(part)]
    return "; ".join(names) + " changed" if names else "job fingerprint changed"


def _missing(dag: Dag, job: Job, state: State) -> str:
    """'output missing', and where an earlier run left the file if a change of path rules stranded it."""
    record = state.jobs.get(job_key(job))
    here = {job.identities[path]: path for path in job.outputs}
    for key, old in sorted((record.output_paths or {}).items() if record else ()):
        if key in here and old != here[key] and old not in job.outputs:
            try:
                present = dag.checked(old).exists()
            except DagError:
                continue
            if present:
                return f"output missing; an earlier run left it at {old}"
    return "output missing"


def judge(dag: Dag, job: Job, state: State, force: bool, hashing: bool, seen: Stamps | None = None,
          verify: bool = False) -> Decision:
    """Whether a command job is current on the files it has now, ignoring the jobs upstream.

    `plan` asks it for every job, with the stamps it has read so far as
    `seen`, and `run` asks it again for a job planned `maybe`, once the jobs it
    waits for have finished, with none, as those jobs changed the files.
    """
    now_in, now_out = stamps(dag, job.inputs, seen), stamps(dag, job.outputs, seen)
    if any(value is None for value in now_out.values()):
        return Decision(job, Status.RUN, _missing(dag, job, state))
    if any(value is None for value in now_in.values()):
        return Decision(job, Status.BLOCKED, "input is missing")
    if force:
        return Decision(job, Status.RUN, "forced")
    record = state.jobs.get(job_key(job))
    if record is None:
        return Decision(job, Status.RUN, "no successful run recorded")
    if record.fingerprint != job.fingerprint:
        return Decision(job, Status.RUN, changed_parts(record, job))
    # A record holds its files by identity, so files moved to new paths are
    # the same files when their stamps or contents match.
    in_paths = {job.identities[path]: path for path in job.inputs}
    out_paths = {job.identities[path]: path for path in job.outputs}
    now_in, now_out = stored_each(job.keyed(now_in)), stored_each(job.keyed(now_out))
    if not _unchanged(dag, in_paths, now_in, record.inputs, record.input_hashes, hashing, verify):
        return Decision(job, Status.RUN, "input changed")
    if not _unchanged(dag, out_paths, now_out, record.outputs, record.output_hashes, hashing, verify):
        return Decision(job, Status.RUN, "output changed")
    # Touched files hold what they held: keep the new stamps.
    refresh = None if (now_in, now_out, out_paths) == (record.inputs, record.outputs, record.output_paths) else record.model_copy(
        update={"inputs": now_in, "outputs": now_out, "output_paths": out_paths})
    if record.checks != job.checks_key:
        reason = "checks changed" if record.checks is not None else "checks not yet run on these files"
        return Decision(job, Status.CHECK, reason, refresh)
    return Decision(job, Status.SKIP, "current", refresh)


def _without_command(dag: Dag, job: Job, by_id: dict[int, Decision], seen: Stamps) -> Decision:
    """A job with no command is current only when its outputs exist and are newer than its inputs."""
    outputs = stamps(dag, job.outputs, seen)
    if any(value is None for value in outputs.values()):
        return Decision(job, Status.BLOCKED, "no command and output is missing")
    # It cannot be rerun, so any change upstream would leave its outputs stale.
    if any(by_id[dep].status is Status.RUN for dep in job.depends_on):
        return Decision(job, Status.BLOCKED, "no command and dependency will run")
    if any(by_id[dep].status is Status.MAYBE for dep in job.depends_on):
        return Decision(job, Status.BLOCKED, "no command and dependency may run")
    inputs = stamps(dag, job.inputs, seen)
    if any(value is None for value in inputs.values()):
        return Decision(job, Status.BLOCKED, "input is missing")
    if job.inputs and max(value.mtime_ns for value in inputs.values()) > min(value.mtime_ns for value in outputs.values()):
        return Decision(job, Status.BLOCKED, "no command and output is older than an input")
    if job.checks:
        return Decision(job, Status.CHECK, "no command; outputs exist, checking them")
    return Decision(job, Status.SKIP, "no command; outputs exist")


def _below_rerun(dag: Dag, job: Job, state: State, force: bool, hashing: bool, seen: Stamps,
                 verify: bool = False) -> Decision:
    """A job that reads from one that will or may run: `maybe` when it is current as it stands."""
    if not hashing or any(value is None for value in stamps(dag, job.inputs, seen).values()):
        return Decision(job, Status.RUN, "dependency will run")
    decision = judge(dag, job, state, force, hashing, seen, verify)
    if decision.status in (Status.SKIP, Status.CHECK):
        return Decision(job, Status.MAYBE, "runs if an upstream output changes")
    return decision


def _with_programs(decision: Decision, missing_programs: set[str]) -> Decision:
    """Block a job that will start a program that cannot be found."""
    if decision.status not in (Status.RUN, Status.CHECK, Status.MAYBE):
        return decision
    job = decision.job
    commands = [check.command for check in job.checks]
    if decision.status is not Status.CHECK:
        commands += [*(run.arguments for run in job.commands), *job.verify]
    programs = sorted({command[0] for command in commands} & missing_programs)
    if not programs:
        return decision
    return Decision(job, Status.BLOCKED, "program not found: " + ", ".join(programs))


def _located(decision: Decision, by_id: dict[int, Decision]) -> Decision:
    """The decision with what it waits for, or the blocked job whose cause is its own."""
    job = decision.job
    waits = tuple(dep for dep in job.depends_on if by_id[dep].status in UPSTREAM)
    cause = None
    if decision.status is Status.BLOCKED and decision.reason.startswith(DEPENDENCY_BLOCKED):
        first = next(dep for dep in job.depends_on if by_id[dep].status is Status.BLOCKED)
        cause = by_id[first].caused_by or first
    return replace(decision, waits_for=waits if decision.status in UPSTREAM else (), caused_by=cause)


def plan(dag: Dag, state: State, force: bool | frozenset[int] = False, hashing: bool = True,
         seen: Stamps | None = None, verify: bool = False) -> tuple[Decision, ...]:
    """Decide every job. `seen` may hold stamps to take as the files' own; `plan --move-files`'s
    listing gives the stamps the files would have once moved. Without `hashing`, files are
    compared by stamp alone; with `verify`, by content alone."""
    decisions: list[Decision] = []
    by_id: dict[int, Decision] = {}
    seen = {} if seen is None else seen
    missing_external = {path for path, value in stamps(dag, dag.external_inputs, seen).items() if value is None}
    missing_programs = _missing_programs(dag)

    def decide(job: Job) -> Decision:
        forced = force is True or (not isinstance(force, bool) and job.id in force)
        missing = [path for path in job.inputs if path in missing_external]
        if missing:
            return Decision(job, Status.BLOCKED, "missing external input: " + ", ".join(missing))
        blocked = [dep for dep in job.depends_on if by_id[dep].status is Status.BLOCKED]
        if blocked:
            return Decision(job, Status.BLOCKED, DEPENDENCY_BLOCKED + ", ".join(map(str, blocked)))
        if not job.commands:
            return _without_command(dag, job, by_id, seen)
        if any(by_id[dep].status in UPSTREAM for dep in job.depends_on):
            return _below_rerun(dag, job, state, forced, hashing, seen, verify)
        return judge(dag, job, state, forced, hashing, seen, verify)

    for job in dag.jobs:
        decision = _located(_with_programs(decide(job), missing_programs), by_id)
        decisions.append(decision)
        by_id[job.id] = decision
    return tuple(decisions)


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


def adopt(dag: Dag, state: State) -> tuple[Adoption, ...]:
    """Record every command job whose files exist as current, without running it.

    No checks are recorded, so the next run checks the adopted files.
    """
    adoptions = []
    for job in dag.jobs:
        inputs = stamps(dag, job.inputs)
        outputs = stamps(dag, job.outputs)
        if not job.commands:
            adoption = Adoption(job, AdoptStatus.LEFT, "no command")
        elif any(value is None for value in outputs.values()):
            adoption = Adoption(job, AdoptStatus.LEFT, "output missing")
        elif any(value is None for value in inputs.values()):
            adoption = Adoption(job, AdoptStatus.LEFT, "input missing")
        else:
            state.record(job_key(job), RunRecord(fingerprint=job.fingerprint, parts=job.parts,
                                                    inputs=stored_each(job.keyed(inputs)),
                                                    outputs=stored_each(job.keyed(outputs)),
                                                    output_paths=job.output_paths))
            adoption = Adoption(job, AdoptStatus.ADOPTED, "outputs exist")
        adoptions.append(adoption)
    return tuple(adoptions)
