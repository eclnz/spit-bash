"""Whether a job's recorded outputs can be moved to the paths its pipeline now gives them, and which moves."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum

from ..files import Stamps, stamp, stamps, stored
from ..model import Dag, DagError, Job
from ..state.record import Record
from .evidence import Compare, changed, changed_parts, compare_files


class MoveCause(Enum):
    """Why a job's recorded outputs can be moved, or why not."""

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


@dataclass(frozen=True)
class Migration:
    job: Job
    # (recorded path, current path) of each output to move; empty unless `cause` is `MoveCause.MOVABLE`.
    moves: tuple[tuple[str, str], ...]
    cause: MoveCause
    # The path that `cause` names, or why a path is refused, for the causes that name one.
    detail: str = ""
    # For `MoveCause.WORK_CHANGED`, as `Evidence.parts`.
    parts: tuple[str, ...] | None = ()
    # The job's record as it stands, which the moved files are checked against; None without moves.
    record: Record | None = None


def claimed(dag: Dag) -> set[str]:
    """Every path a job reads or writes now: a file there is not the one a record left behind."""
    paths = set(dag.external_inputs)
    for job in dag.jobs:
        paths.update(job.input_paths)
        paths.update(job.output_paths)
    return paths


def movable(dag: Dag, job: Job, record: Record | None, claimed: set[str], moving: set[str],
            seen: Stamps | None = None) -> Migration | None:
    """The moves that bring a job's recorded outputs to its current paths, or None when the record has them there.

    `moving` holds the paths that jobs before this one move outputs to; an input
    there arrives with the stamps it has, so it counts as present and unchanged.
    `seen` caches stamps as `files.stamps` does.
    """
    if record is None or record.output_paths is None:
        return None
    current = job.output_paths_by_slot
    if all(record.output_paths.get(key, path) == path for key, path in current.items()):
        return None
    if record.fingerprint != job.work.fingerprint:
        return Migration(job, (), MoveCause.WORK_CHANGED, parts=changed_parts(record, job))
    # A moved output is reused only if the job would be current but for the move.
    in_paths = {slot: path for slot, path in job.input_paths_by_slot.items() if path not in moving}
    now = stamps(dag, tuple(in_paths.values()), seen)
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
    return Migration(job, tuple(moves), MoveCause.MOVABLE, record=record)
