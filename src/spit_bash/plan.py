"""Decide which jobs are current from paths, fingerprints and file stamps."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, replace
from enum import Enum

from .cause import Cause, Why
from .files import Stamps, stamp, stamps
from .model import Dag, DagError, Job
from .reuse import records, relocate
from .reuse.evidence import Compare, Evidence, evidence
from .reuse.relocate import Migration, MoveCause
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
    # Current once its recorded outputs are moved to the paths the job now
    # gives them, under `--move-files`: `run` moves them, then runs its checks
    # when `needs_checks` says so.
    PLACE = "place"


@dataclass(frozen=True)
class Decision:
    job: Job
    status: Status
    cause: Cause
    # What `run` stores for a current job whose files were touched but not changed:
    # its record with the files' new stamps, so the next plan need not hash them.
    # For a job planned `place`, its record at its new paths, stored once the files are moved.
    writes: tuple[StateWrite, ...] = ()
    # The jobs upstream that will or may rewrite what this job reads, which it has to wait for.
    waits_for: tuple[int, ...] = ()
    # For a job blocked only because a job upstream is, the id of the job whose own cause it is.
    caused_by: int | None = None
    # Under `--move-files`: for a job planned `place`, its moves; for one whose outputs
    # the record has at other paths but that runs instead, why they are not moved.
    placement: Migration | None = None

    @property
    def needs_checks(self) -> bool:
        """Whether `run` runs the job's checks without its commands: it is planned `check`, or `place` with checks to run."""
        return self.status is Status.CHECK or (self.status is Status.PLACE and self.cause.why is not Why.CURRENT)


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


# A job planned with one of these will have its outputs rewritten, or may. A job
# planned `place` is not among them: its outputs arrive as recorded.
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


def _placed(dag: Dag, decision: Decision, state: Store, compare: Compare, seen: Stamps,
            claimed: set[str], moving: set[str]) -> Decision:
    """A job that would run, planned `place` when its recorded outputs can be moved to its paths instead.

    Once placed, its new paths take the stamps of the files moving there, in
    `seen`, so the jobs after it see its outputs as present and as recorded.
    """
    job = decision.job
    found = relocate.movable(dag, job, state.jobs.get(job.work.key), claimed, moving, seen)
    if found is None:
        return decision
    if not found.moves:
        return replace(decision, placement=found)
    # A rename keeps a file's size and time, so the moved files' stamps are those they have now.
    arrived = {new: stamp(dag.checked(old), new in dag.folders) for old, new in found.moves}
    moved = judge(dag, job, state, False, compare, {**seen, **arrived})
    if moved.status not in (Status.SKIP, Status.CHECK):
        # An input another job moves differs from this job's record.
        return replace(decision, placement=replace(found, moves=(), cause=MoveCause.INPUT_CHANGED, record=None))
    seen.update(arrived)
    moving.update(arrived)
    assert found.record is not None, "a job with moves has a record"
    writes = moved.writes or (StateWrite(job.work.key, records.relocated(found.record, job)),)
    return Decision(job, Status.PLACE, moved.cause, writes, placement=found)


def _with_programs(decision: Decision, missing_programs: set[str]) -> Decision:
    """Block a job that will start a program that cannot be found."""
    job = decision.job
    match decision.status:
        case Status.RUN | Status.MAYBE:
            commands = [*(check.command for check in job.checks), *(run.arguments for run in job.commands),
                        *job.verify]
        case Status.CHECK | Status.PLACE:
            commands = [check.command for check in job.checks] if decision.needs_checks else []
        case Status.SKIP | Status.BLOCKED:
            return decision
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
         move_files: bool = False) -> tuple[Decision, ...]:
    """Decide every job, comparing files with their records as `compare` says.

    With `move_files`, a job that would run but whose recorded outputs can be
    moved to its paths is planned `place`. Nothing is written or moved: a
    decision carries the writes and moves `run` makes for it.
    """
    decisions: list[Decision] = []
    by_id: dict[int, Decision] = {}
    seen: Stamps = {}
    claimed = relocate.claimed(dag) if move_files else set()
    moving: set[str] = set()
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
        decision = judge(dag, job, state, forced, compare, seen)
        if move_files and not forced and decision.status is Status.RUN:
            return _placed(dag, decision, state, compare, seen, claimed, moving)
        return decision

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
    # A file a job planned `place` moves is shown as it will be once moved.
    arrived = {new: stamp(dag.checked(old), new in dag.folders)
               for each in decisions if each.status is Status.PLACE and each.placement is not None
               for old, new in each.placement.moves}
    # The files shown are those whose stamps differ, which are compared by content
    # unless `compare` says not to, so `full` shows what `content` does.
    found = evidence(dag, job, state.jobs.get(job.work.key), stamps(dag, (*job.input_paths, *job.output_paths), arrived),
                     Compare.STAT if compare is Compare.STAT else Compare.CONTENT, every=True)
    return decision, found


def writes(decisions: tuple[Decision, ...]) -> tuple[StateWrite, ...]:
    """Every write the decisions carry, in order, for a run to store before it starts.

    A job planned `place` is left out: its record is stored once its files are moved.
    """
    return tuple(write for decision in decisions if decision.status is not Status.PLACE for write in decision.writes)


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
