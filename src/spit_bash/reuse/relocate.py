"""Move a job's recorded outputs to the paths its pipeline now gives them."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Callable

from ..files import stamp, stamps, stored
from ..model import Dag, DagError, Job
from ..state.record import Record
from ..state.store import StateWrite
from . import records
from .evidence import Compare, changed, changed_parts, compare_files


class MoveCause(Enum):
    """Why a job's recorded outputs are moved, or why not."""

    # The outputs are recorded at other paths and can be moved: the one cause with moves.
    MOVABLE = "movable"
    # The job's work changed since the run; `Migration.parts` names what changed.
    WORK_CHANGED = "work changed"
    INPUT_CHANGED = "input changed"
    PATH_UNRECORDED = "path unrecorded"
    PARTLY_IN_PLACE = "partly in place"
    # `Migration.detail` is why the path is refused.
    BAD_PATH = "bad path"
    # `Migration.detail` is the destination.
    DESTINATION_EXISTS = "destination exists"
    # `Migration.detail` is the recorded path.
    STILL_USED = "still used"
    # `Migration.detail` is the recorded path.
    OUTPUT_CHANGED = "output changed"
    # `Migration.detail` is the error; the files were put back.
    MOVE_FAILED = "move failed"
    # `Migration.detail` is the error putting the files back.
    UNDO_FAILED = "undo failed"


@dataclass(frozen=True)
class Migration:
    job: Job
    # (recorded path, current path) of each output to move; empty unless `cause` is `MoveCause.MOVABLE`.
    moves: tuple[tuple[str, str], ...]
    cause: MoveCause
    # The path or error that `cause` names, for the causes that name one.
    detail: str = ""
    # For `MoveCause.WORK_CHANGED`, as `Evidence.parts`.
    parts: tuple[str, ...] | None = ()
    # The job's record as it stands, which the moved files are checked against; None without moves.
    record: Record | None = None

    @property
    def writes(self) -> tuple[StateWrite, ...]:
        """What the state records once the moves are made: the job's record at its new paths."""
        if self.record is None or not self.moves:
            return ()
        return (StateWrite(self.job.work.key, records.relocated(self.record, self.job)),)


def _plan_job(dag: Dag, job: Job, held: dict[str, Record], claimed: set[str], moving: set[str]) -> Migration | None:
    """The moves that bring a job's recorded outputs to its current paths, or None when it has nothing to move."""
    record = held.get(job.work.key)
    if record is None or record.output_paths is None:
        return None
    current = job.output_paths_by_slot
    if all(record.output_paths.get(key, path) == path for key, path in current.items()):
        return None
    if record.fingerprint != job.work.fingerprint:
        return Migration(job, (), MoveCause.WORK_CHANGED, parts=changed_parts(record, job))
    # A moved output is reused only if the job would be current but for the move.
    # An input an earlier job is moving arrives with the stamps it has, so it counts as present.
    in_paths = {slot: path for slot, path in job.input_paths_by_slot.items() if path not in moving}
    now = stamps(dag, tuple(in_paths.values()))
    if any(value is None for value in now.values()) or changed(compare_files(
            dag, in_paths, now, record.inputs, record.input_hashes, Compare.CONTENT, {})):
        return Migration(job, (), MoveCause.INPUT_CHANGED)
    moves = []
    for key, new in current.items():
        old = record.output_paths.get(key)
        if old is None:
            return Migration(job, (), MoveCause.PATH_UNRECORDED)
        if old == new:
            return Migration(job, (), MoveCause.PARTLY_IN_PLACE)
        try:
            old_path, new_path = dag.checked(old), dag.checked(new)
        except DagError as exc:
            return Migration(job, (), MoveCause.BAD_PATH, str(exc))
        if os.path.lexists(new_path):
            return Migration(job, (), MoveCause.DESTINATION_EXISTS, new)
        if old in claimed:
            return Migration(job, (), MoveCause.STILL_USED, old)
        if stored(stamp(old_path, new in dag.folders)) != record.outputs.get(key):
            return Migration(job, (), MoveCause.OUTPUT_CHANGED, old)
        moves.append((old, new))
    moving.update(new for _, new in moves)
    return Migration(job, tuple(moves), MoveCause.MOVABLE, record=record)


def find(dag: Dag, held: dict[str, Record], jobs: set[int] | None = None) -> tuple[Migration, ...]:
    """The jobs, among `jobs` when given, whose records in `held` have their outputs at other paths than their current ones."""
    # A file at a path that any job reads or writes now is not the one the record left behind.
    claimed = set(dag.external_inputs)
    for job in dag.jobs:
        claimed.update(job.input_paths)
        claimed.update(job.output_paths)
    moving: set[str] = set()
    found = (_plan_job(dag, job, held, claimed, moving) for job in dag.jobs
             if job.commands and (jobs is None or job.id in jobs))
    return tuple(migration for migration in found if migration is not None)


def _undo(dag: Dag, done: list[tuple[str, str]]) -> None:
    for old, new in reversed(done):
        os.rename(dag.checked(new), dag.checked(old))


def apply(dag: Dag, migrations: tuple[Migration, ...],
          record: Callable[[tuple[StateWrite, ...]], None]) -> tuple[Migration, ...]:
    """Move each migration's files, all of a job's or none, and return what each one came to.

    A file is renamed, so it keeps its size and time; the move is checked against
    the record's stamps, and then `record` is given the migration's writes at
    once, so a run stopped after it still finds the files. A job whose move fails
    or does not check is put back as it was and reported with the error.
    Nothing is overwritten: a destination that appeared since `find` stops that job's move.
    """
    results = []
    for migration in migrations:
        if not migration.moves or migration.record is None:
            results.append(migration)
            continue
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
                if stored(stamp(target, new in dag.folders)) != migration.record.outputs.get(key):
                    raise OSError(f"{new} does not match the record after the move")
        except OSError as exc:
            try:
                _undo(dag, done)
                failed = Migration(migration.job, (), MoveCause.MOVE_FAILED, str(exc))
            except OSError as undo:
                failed = Migration(migration.job, (), MoveCause.UNDO_FAILED, str(undo))
            results.append(failed)
        else:
            record(migration.writes)
            results.append(migration)
    return tuple(results)
