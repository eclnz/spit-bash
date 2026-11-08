"""Decide which jobs are current from paths, fingerprints and file stamps."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, replace
from enum import Enum

from .files import Stamps, stamps
from .model import Dag, DagError, Job
from .reuse import records
from .reuse.evidence import Compare, describe_parts, evidence
from .reuse.verdict import Stale, Standing, verdict
from .state.store import StateWrite, Store


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
    # What `run` stores for a current job whose files were touched but not changed:
    # its record with the files' new stamps, so the next plan need not hash them.
    writes: tuple[StateWrite, ...] = ()
    # The jobs upstream that will or may rewrite what this job reads, which it has to wait for.
    waits_for: tuple[int, ...] = ()
    # For a job blocked only because a job upstream is, the id of the job whose own cause it is.
    caused_by: int | None = None


# The start of the reason of a job blocked by a job upstream, which `Decision.caused_by` follows.
DEPENDENCY_BLOCKED = "dependency blocked: "


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


def _missing(dag: Dag, job: Job, state: Store) -> str:
    """'output missing', and where an earlier run left the file if a change of path rules stranded it."""
    record = state.jobs.get(job.work.key)
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


STALE_REASONS = {Stale.NO_RECORD: "no successful run recorded", Stale.INPUT_CHANGED: "input changed",
                 Stale.OUTPUT_CHANGED: "output changed"}


def judge(dag: Dag, job: Job, state: Store, force: bool, compare: Compare, seen: Stamps | None = None) -> Decision:
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
    found = evidence(dag, job, state.jobs.get(job.work.key), {**now_in, **now_out}, compare)
    judged = verdict(found)
    writes = () if judged.refreshed is None else (StateWrite(job.work.key, judged.refreshed),)
    match judged.standing, judged.cause:
        case Standing.STALE, Stale.WORK_CHANGED:
            return Decision(job, Status.RUN, describe_parts(found.parts))
        case Standing.STALE, cause:
            assert cause is not None, "a stale verdict has a cause"
            return Decision(job, Status.RUN, STALE_REASONS[cause])
        case Standing.CHECKS_ONLY, _:
            assert found.record is not None, "a verdict of checks only has a record"
            reason = "checks changed" if found.record.checks is not None else "checks not yet run on these files"
            return Decision(job, Status.CHECK, reason, writes)
        case _:
            return Decision(job, Status.SKIP, "current", writes)


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


def _below_rerun(dag: Dag, job: Job, state: Store, force: bool, compare: Compare, seen: Stamps) -> Decision:
    """A job that reads from one that will or may run: `maybe` when it is current as it stands."""
    if compare is Compare.STAT or any(value is None for value in stamps(dag, job.input_paths, seen).values()):
        return Decision(job, Status.RUN, "dependency will run")
    decision = judge(dag, job, state, force, compare, seen)
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


def plan(dag: Dag, state: Store, force: bool | frozenset[int] = False, compare: Compare = Compare.CONTENT,
         seen: Stamps | None = None) -> tuple[Decision, ...]:
    """Decide every job, comparing files with their records as `compare` says.

    `seen` may hold stamps to take as the files' own; `plan --move-files`'s
    listing gives the stamps the files would have once moved. Nothing is
    written: a decision carries the writes `run` makes for it.
    """
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
            return _below_rerun(dag, job, state, forced, compare, seen)
        return judge(dag, job, state, forced, compare, seen)

    for job in dag.jobs:
        decision = _located(_with_programs(decide(job), missing_programs), by_id)
        decisions.append(decision)
        by_id[job.id] = decision
    return tuple(decisions)


def writes(decisions: tuple[Decision, ...]) -> tuple[StateWrite, ...]:
    """Every write the decisions carry, in order, for a run to store."""
    return tuple(write for decision in decisions for write in decision.writes)


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
            state.record(job.work.key, records.adopted(job, inputs, outputs))
            adoption = Adoption(job, AdoptStatus.ADOPTED, "outputs exist")
        adoptions.append(adoption)
    return tuple(adoptions)
