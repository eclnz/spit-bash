"""Decide which jobs are current from paths, fingerprints and file stamps."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path

from .model import Dag, DagError, Job
from .files import Stamps, content_hash, stamps, stored_each
from .state.record import Record
from .state.store import StateWrite, Store


def job_key(job: Job) -> str:
    # Job IDs may change when SPIT resolves a different inventory, and paths
    # change when the pipeline's path rules do. What a job makes is its outputs'
    # products and entities, which neither changes.
    made = sorted([output.product, output.entities] for output in job.output_artifacts)
    return json.dumps(made, separators=(",", ":"))


def reconcile(dag: Dag, state: Store, everything: Dag, persist: bool = False) -> tuple[tuple[str, str], ...]:
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
            slots = job.input_paths_by_slot
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
            for slot, path in job.output_paths_by_slot.items():
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
    refresh: Record | None = None
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


def changed_parts(record: Record, job: Job) -> str:
    """Why a job's fingerprint differs from its record's, naming the parts that changed when it can."""
    if record.parts is None:
        return "job fingerprint changed"
    names = [text for part, text in PART_NAMES.items() if record.parts.get(part) != job.parts.get(part)]
    return "; ".join(names) + " changed" if names else "job fingerprint changed"


def _missing(dag: Dag, job: Job, state: Store) -> str:
    """'output missing', and where an earlier run left the file if a change of path rules stranded it."""
    record = state.jobs.get(job_key(job))
    here = job.output_paths_by_slot
    for key, old in sorted((record.output_paths or {}).items() if record else ()):
        if key in here and old != here[key] and old not in job.output_paths:
            try:
                present = dag.checked(old).exists()
            except DagError:
                continue
            if present:
                return f"output missing; an earlier run left it at {old}"
    return "output missing"


def judge(dag: Dag, job: Job, state: Store, force: bool, hashing: bool, seen: Stamps | None = None,
          verify: bool = False) -> Decision:
    """Whether a command job is current on the files it has now, ignoring the jobs upstream.

    `plan` asks it for every job, with the stamps it has read so far as
    `seen`, and `run` asks it again for a job planned `maybe`, once the jobs it
    waits for have finished, with none, as those jobs changed the files.
    """
    now_in, now_out = stamps(dag, job.input_paths, seen), stamps(dag, job.output_paths, seen)
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
    in_paths = job.input_paths_by_slot
    out_paths = job.output_paths_by_slot
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
    outputs = stamps(dag, job.output_paths, seen)
    if any(value is None for value in outputs.values()):
        return Decision(job, Status.BLOCKED, "no command and output is missing")
    # It cannot be rerun, so any change upstream would leave its outputs stale.
    if any(by_id[dep].status is Status.RUN for dep in job.depends_on):
        return Decision(job, Status.BLOCKED, "no command and dependency will run")
    if any(by_id[dep].status is Status.MAYBE for dep in job.depends_on):
        return Decision(job, Status.BLOCKED, "no command and dependency may run")
    inputs = stamps(dag, job.input_paths, seen)
    if any(value is None for value in inputs.values()):
        return Decision(job, Status.BLOCKED, "input is missing")
    if job.input_paths and max(value.mtime_ns for value in inputs.values()) > min(value.mtime_ns for value in outputs.values()):
        return Decision(job, Status.BLOCKED, "no command and output is older than an input")
    if job.checks:
        return Decision(job, Status.CHECK, "no command; outputs exist, checking them")
    return Decision(job, Status.SKIP, "no command; outputs exist")


def _below_rerun(dag: Dag, job: Job, state: Store, force: bool, hashing: bool, seen: Stamps,
                 verify: bool = False) -> Decision:
    """A job that reads from one that will or may run: `maybe` when it is current as it stands."""
    if not hashing or any(value is None for value in stamps(dag, job.input_paths, seen).values()):
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


def plan(dag: Dag, state: Store, force: bool | frozenset[int] = False, hashing: bool = True,
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
        missing = [path for path in job.input_paths if path in missing_external]
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


def apply_refreshes(decisions: tuple[Decision, ...], state: Store) -> None:
    """Store the new stamps of jobs whose files were touched but not changed."""
    state.apply(tuple(StateWrite(job_key(decision.job), decision.refresh) for decision in decisions
                      if decision.refresh is not None and decision.status in (Status.SKIP, Status.CHECK)))


class AdoptStatus(Enum):
    ADOPTED = "adopted"
    LEFT = "left"


@dataclass(frozen=True)
class Adoption:
    job: Job
    status: AdoptStatus
    reason: str


def adopt(dag: Dag, state: Store) -> tuple[Adoption, ...]:
    """Record every command job whose files exist as current, without running it.

    No checks are recorded, so the next run checks the adopted files.
    """
    adoptions = []
    for job in dag.jobs:
        inputs = stamps(dag, job.input_paths)
        outputs = stamps(dag, job.output_paths)
        if not job.commands:
            adoption = Adoption(job, AdoptStatus.LEFT, "no command")
        elif any(value is None for value in outputs.values()):
            adoption = Adoption(job, AdoptStatus.LEFT, "output missing")
        elif any(value is None for value in inputs.values()):
            adoption = Adoption(job, AdoptStatus.LEFT, "input missing")
        else:
            state.record(job_key(job), Record(fingerprint=job.fingerprint, parts=job.parts,
                                                 inputs=stored_each(job.keyed(inputs)),
                                                 outputs=stored_each(job.keyed(outputs)),
                                                 output_paths=job.output_paths_by_slot))
            adoption = Adoption(job, AdoptStatus.ADOPTED, "outputs exist")
        adoptions.append(adoption)
    return tuple(adoptions)
