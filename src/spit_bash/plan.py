"""Decide which jobs are current from paths, fingerprints and file stamps."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, replace
from enum import Enum

from .cause import Cause, Why
from .files import Stamps, stamps
from .model import Dag, DagError, Job
from .reuse import records
from .reuse.evidence import Compare, Evidence, evidence
from .reuse.verdict import Standing, verdict
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
    cause: Cause
    # What `run` stores for a current job whose files were touched but not changed:
    # its record with the files' new stamps, so the next plan need not hash them.
    writes: tuple[StateWrite, ...] = ()
    # The jobs upstream that will or may rewrite what this job reads, which it has to wait for.
    waits_for: tuple[int, ...] = ()
    # For a job blocked only because a job upstream is, the id of the job whose own cause it is.
    caused_by: int | None = None


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


def _missing(dag: Dag, job: Job, state: Store) -> Cause:
    """`Why.OUTPUT_MISSING`, with where an earlier run left the file if a change of path rules stranded it."""
    record = state.jobs.get(job.work.key)
    here = job.output_paths_by_slot
    for key, old in sorted((record.output_paths or {}).items() if record else ()):
        if key in here and old != here[key] and old not in job.output_paths:
            try:
                present = dag.checked(old).exists()
            except DagError:
                continue
            if present:
                return Cause(Why.OUTPUT_MISSING, (old,))
    return Cause(Why.OUTPUT_MISSING)


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
        return Decision(job, Status.BLOCKED, Cause(Why.INPUT_MISSING))
    if force:
        return Decision(job, Status.RUN, Cause(Why.FORCED))
    found = evidence(dag, job, state.jobs.get(job.work.key), {**now_in, **now_out}, compare)
    judged = verdict(found)
    writes = () if judged.refreshed is None else (StateWrite(job.work.key, judged.refreshed),)
    match judged.standing:
        case Standing.STALE:
            assert judged.cause is not None, "a stale verdict has a cause"
            return Decision(job, Status.RUN, judged.cause)
        case Standing.CHECKS_ONLY:
            assert found.record is not None, "a verdict of checks only has a record"
            why = Why.CHECKS_CHANGED if found.record.checks is not None else Why.CHECKS_NOT_RUN
            return Decision(job, Status.CHECK, Cause(why), writes)
        case Standing.CURRENT:
            return Decision(job, Status.SKIP, Cause(Why.CURRENT), writes)


def _without_command(dag: Dag, job: Job, by_id: dict[int, Decision], seen: Stamps) -> Decision:
    """A job with no command is current only when its outputs exist and are newer than its inputs."""
    outputs = stamps(dag, job.output_paths, seen)
    if any(value is None for value in outputs.values()):
        return Decision(job, Status.BLOCKED, Cause(Why.NO_COMMAND_OUTPUT_MISSING))
    # It cannot be rerun, so any change upstream would leave its outputs stale.
    if any(by_id[dep].status is Status.RUN for dep in job.depends_on):
        return Decision(job, Status.BLOCKED, Cause(Why.NO_COMMAND_DEPENDENCY_WILL_RUN))
    if any(by_id[dep].status is Status.MAYBE for dep in job.depends_on):
        return Decision(job, Status.BLOCKED, Cause(Why.NO_COMMAND_DEPENDENCY_MAY_RUN))
    inputs = stamps(dag, job.input_paths, seen)
    if any(value is None for value in inputs.values()):
        return Decision(job, Status.BLOCKED, Cause(Why.INPUT_MISSING))
    if job.input_paths and max(value.mtime_ns for value in inputs.values()) > min(value.mtime_ns for value in outputs.values()):
        return Decision(job, Status.BLOCKED, Cause(Why.NO_COMMAND_OUTPUT_OLDER))
    if job.checks:
        return Decision(job, Status.CHECK, Cause(Why.NO_COMMAND_CHECKING))
    return Decision(job, Status.SKIP, Cause(Why.NO_COMMAND_OUTPUTS_EXIST))


def _below_rerun(dag: Dag, job: Job, state: Store, force: bool, compare: Compare, seen: Stamps) -> Decision:
    """A job that reads from one that will or may run: `maybe` when it is current as it stands."""
    if compare is Compare.STAT or any(value is None for value in stamps(dag, job.input_paths, seen).values()):
        return Decision(job, Status.RUN, Cause(Why.DEPENDENCY_WILL_RUN))
    decision = judge(dag, job, state, force, compare, seen)
    if decision.status in (Status.SKIP, Status.CHECK):
        return Decision(job, Status.MAYBE, Cause(Why.UPSTREAM_MAY_RUN))
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
    return Decision(job, Status.BLOCKED, Cause(Why.PROGRAM_NOT_FOUND, tuple(programs)))


def _located(decision: Decision, by_id: dict[int, Decision]) -> Decision:
    """The decision with what it waits for, or the blocked job whose cause is its own."""
    job = decision.job
    waits = tuple(dep for dep in job.depends_on if by_id[dep].status in UPSTREAM)
    cause = None
    if decision.cause.why is Why.DEPENDENCY_BLOCKED:
        first = decision.cause.jobs[0]
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
            return Decision(job, Status.BLOCKED, Cause(Why.MISSING_EXTERNAL, tuple(missing)))
        blocked = [dep for dep in job.depends_on if by_id[dep].status is Status.BLOCKED]
        if blocked:
            return Decision(job, Status.BLOCKED, Cause(Why.DEPENDENCY_BLOCKED, jobs=tuple(blocked)))
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


def explained(dag: Dag, state: Store, decisions: tuple[Decision, ...], job_id: int,
              compare: Compare) -> tuple[Decision, Evidence]:
    """The decision on job `job_id` and the evidence behind it, every input and output compared, for `plan --job`."""
    decision = next((decision for decision in decisions if decision.job.id == job_id), None)
    if decision is None:
        raise DagError(f"no job {job_id} among the jobs planned")
    job = decision.job
    # The files shown are those whose stamps differ, which are compared by content
    # unless `compare` says not to, so `full` shows what `content` does.
    found = evidence(dag, job, state.jobs.get(job.work.key), stamps(dag, (*job.input_paths, *job.output_paths), {}),
                     Compare.STAT if compare is Compare.STAT else Compare.CONTENT, every=True)
    return decision, found


def writes(decisions: tuple[Decision, ...]) -> tuple[StateWrite, ...]:
    """Every write the decisions carry, in order, for a run to store."""
    return tuple(write for decision in decisions for write in decision.writes)


class AdoptStatus(Enum):
    ADOPTED = "adopted"
    LEFT = "left"


class AdoptCause(Enum):
    """Why `adopt` recorded a job or left it; only `OUTPUTS_EXIST` adopts."""

    NO_COMMAND = "no command"
    OUTPUT_MISSING = "output missing"
    INPUT_MISSING = "input missing"
    OUTPUTS_EXIST = "outputs exist"


@dataclass(frozen=True)
class Adoption:
    job: Job
    status: AdoptStatus
    cause: AdoptCause


def adopt(dag: Dag, state: Store) -> tuple[Adoption, ...]:
    """Record every command job whose files exist as current, without running it.

    No checks are recorded, so the next run checks the adopted files.
    """
    adoptions = []
    for job in dag.jobs:
        inputs = stamps(dag, job.input_paths)
        outputs = stamps(dag, job.output_paths)
        if not job.commands:
            adoption = Adoption(job, AdoptStatus.LEFT, AdoptCause.NO_COMMAND)
        elif any(value is None for value in outputs.values()):
            adoption = Adoption(job, AdoptStatus.LEFT, AdoptCause.OUTPUT_MISSING)
        elif any(value is None for value in inputs.values()):
            adoption = Adoption(job, AdoptStatus.LEFT, AdoptCause.INPUT_MISSING)
        else:
            state.record(job.work.key, records.adopted(job, inputs, outputs))
            adoption = Adoption(job, AdoptStatus.ADOPTED, AdoptCause.OUTPUTS_EXIST)
        adoptions.append(adoption)
    return tuple(adoptions)
