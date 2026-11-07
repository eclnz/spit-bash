"""Move a job's recorded outputs to the paths its pipeline now gives them."""

from __future__ import annotations

import os
from dataclasses import dataclass

from .dag import Dag, DagError, Job
from .plan import State, _unchanged, job_key, stamp, stamps


@dataclass(frozen=True)
class Migration:
    job: Job
    # (recorded path, current path) of each output to move; empty when `reason` says why not.
    moves: tuple[tuple[str, str], ...]
    reason: str


def _plan_job(dag: Dag, job: Job, state: State, claimed: set[str], moving: set[str]) -> Migration | None:
    """The moves that bring a job's recorded outputs to its current paths, or None when it has nothing to move."""
    record = state.jobs.get(job_key(job))
    if record is None or record.output_paths is None or record.fingerprint != job.fingerprint:
        return None
    current = {job.identities[path]: path for path in job.outputs}
    if all(record.output_paths.get(key, path) == path for key, path in current.items()):
        return None
    # A moved output is reused only if the job would be current but for the move.
    # An input an earlier job is moving arrives with the stamps it has, so it counts as present.
    in_paths = {job.identities[path]: path for path in job.inputs if path not in moving}
    now = job.keyed(stamps(dag, tuple(in_paths.values())))
    if any(value is None for value in now.values()) or not _unchanged(
            dag, in_paths, now, record.inputs, record.input_hashes, True):
        return Migration(job, (), "an input is missing or changed since the run")
    moves = []
    for key, new in current.items():
        old = record.output_paths.get(key)
        if old is None:
            return Migration(job, (), "the record does not say where an output was made")
        if old == new:
            return Migration(job, (), "only some outputs moved; the rest are not moved")
        try:
            old_path, new_path = dag.checked(old), dag.checked(new)
        except DagError as exc:
            return Migration(job, (), str(exc))
        if os.path.lexists(new_path):
            return Migration(job, (), f"{new} already exists")
        if old in claimed:
            return Migration(job, (), f"{old} is a path the pipeline still uses")
        if stamp(old_path, new in dag.folders) != record.outputs.get(key):
            return Migration(job, (), f"{old} is missing or changed since the run")
        moves.append((old, new))
    moving.update(new for _, new in moves)
    return Migration(job, tuple(moves), "outputs recorded at other paths")


def find(dag: Dag, state: State, jobs: set[int] | None = None) -> tuple[Migration, ...]:
    """The jobs, among `jobs` when given, whose recorded outputs are at other paths than their current ones."""
    # A file at a path that any job reads or writes now is not the one the record left behind.
    claimed = set(dag.external_inputs)
    for job in dag.jobs:
        claimed.update(job.inputs)
        claimed.update(job.outputs)
    moving: set[str] = set()
    found = (_plan_job(dag, job, state, claimed, moving) for job in dag.jobs
             if job.commands and (jobs is None or job.id in jobs))
    return tuple(migration for migration in found if migration is not None)


def _undo(dag: Dag, done: list[tuple[str, str]]) -> None:
    for old, new in reversed(done):
        os.rename(dag.checked(new), dag.checked(old))


def apply(dag: Dag, state: State, migrations: tuple[Migration, ...]) -> tuple[Migration, ...]:
    """Move each migration's files, all of a job's or none, and return what each one came to.

    A file is renamed, so it keeps its size and time; the move is checked against
    the record's stamps and then recorded, and a job whose move fails or does not check is put back
    as it was and reported with the reason. Nothing is overwritten: a destination
    that appeared since `find` stops that job's move.
    """
    results = []
    for migration in migrations:
        if not migration.moves:
            results.append(migration)
            continue
        record = state.jobs[job_key(migration.job)]
        done: list[tuple[str, str]] = []
        try:
            for old, new in migration.moves:
                source, target = dag.checked(old), dag.checked(new)
                if os.path.lexists(target):
                    raise OSError(f"{new} already exists")
                target.parent.mkdir(parents=True, exist_ok=True)
                os.rename(source, target)
                done.append((old, new))
                key = migration.job.identities[new]
                if stamp(target, new in dag.folders) != record.outputs.get(key):
                    raise OSError(f"{new} does not match the record after the move")
        except OSError as exc:
            try:
                _undo(dag, done)
                reason = f"not moved: {exc}"
            except OSError as undo:
                reason = f"partly moved, and putting it back failed: {undo}"
            results.append(Migration(migration.job, (), reason))
        else:
            # Record the new paths at once, so a run stopped now still finds the files.
            state.record(job_key(migration.job), record.model_copy(update={"output_paths": migration.job.output_paths}))
            results.append(migration)
    return tuple(results)
