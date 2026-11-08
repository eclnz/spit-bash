"""What a job's record says against the files the job has now: the work, the checks, and each file that differs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ..files import Stamp, Stamps, content_hash, stored
from ..model import Dag, Job
from ..state.record import Record
from .identity import PARTS


class Compare(Enum):
    """How a file counts as unchanged since its record, as `--compare` names it."""

    # By size and modification time alone.
    STAT = "stat"
    # By size and time, then by content where those differ.
    CONTENT = "content"
    # By content always, trusting no size or time.
    FULL = "full"


class Content(Enum):
    """What comparing a file's content with its record's hash found."""

    SAME = "same"
    DIFFERS = "differs"
    # The record holds no hash for the file, so it counts as changed.
    NO_HASH = "no hash"
    # Not compared: under `Compare.STAT`, or once an earlier file was found changed.
    NOT_COMPARED = "not compared"
    # The file cannot be read, so it counts as changed.
    UNREADABLE = "unreadable"


@dataclass(frozen=True)
class FileDiff:
    """One of a job's files that differs from its record by stamp, or that was compared by content regardless."""

    slot: str
    path: str
    recorded: Stamp | None
    now: Stamp | None
    content: Content

    @property
    def changed(self) -> bool:
        """Whether the file counts as changed: anything but the same content does."""
        return self.content is not Content.SAME


@dataclass(frozen=True)
class Evidence:
    """A job's record, or none, held against the job and its files now."""

    job: Job
    record: Record | None
    # Each of the job's input and output paths' stamp now.
    now: Stamps
    # Whether the record's work fingerprint differs from the job's.
    work_changed: bool = False
    # The names in `identity.PARTS` whose hashes differ; None when the work changed
    # and the record keeps no parts to say which.
    parts: tuple[str, ...] | None = ()
    # Whether the checks the record passed differ from the job's.
    checks_changed: bool = False
    # Each input, then each output, that `compare_files` found to differ.
    inputs: tuple[FileDiff, ...] = ()
    outputs: tuple[FileDiff, ...] = ()


def changed_parts(record: Record, job: Job) -> tuple[str, ...] | None:
    """The names in `identity.PARTS` whose hashes differ between the record and the job, or None when the record keeps none."""
    if record.parts is None:
        return None
    return tuple(part for part in PARTS if record.parts.get(part) != job.work.parts.get(part))


def compare_files(dag: Dag, paths: dict[str, str], now: Stamps, recorded: dict[str, list[int]],
                  recorded_hashes: dict[str, str] | None, compare: Compare, hashes: dict[str, str | None],
                  every: bool = False) -> tuple[FileDiff, ...]:
    """The files of `paths`, a path by slot, that differ from the record, each with what its content shows.

    A file differs when its stamp in `now` does not match the record's; under
    `Compare.FULL`, every file is compared. Each one compared by content is
    hashed once, through `hashes`, which keeps every hash it reads by path and
    may hold hashes to take as the files' own; a file missing from `now` with
    no hash there is `Content.UNREADABLE`, unread. Unless `every`, comparing stops at
    the first file found changed, and the rest are `Content.NOT_COMPARED`.
    """
    diffs = []
    found_changed = False
    for slot, path in paths.items():
        was, current = recorded.get(slot), now.get(path)
        if compare is not Compare.FULL and stored(current) == was:
            continue
        if compare is Compare.STAT or (found_changed and not every):
            content = Content.NOT_COMPARED
        elif not recorded_hashes or slot not in recorded_hashes:
            content = Content.NO_HASH
        elif path not in hashes and current is None:
            content = Content.UNREADABLE
        else:
            if path not in hashes:
                hashes[path] = content_hash(str(dag.path(path)), path in dag.folders)
            held = hashes[path]
            content = (Content.UNREADABLE if held is None
                       else Content.SAME if held == recorded_hashes[slot] else Content.DIFFERS)
        diffs.append(FileDiff(slot, path, None if was is None else Stamp.from_stored(was), current, content))
        found_changed = found_changed or content is not Content.SAME
    return tuple(diffs)


def changed(diffs: tuple[FileDiff, ...]) -> bool:
    """Whether any of the files counts as changed."""
    return any(diff.changed for diff in diffs)


def evidence(dag: Dag, job: Job, record: Record | None, now: Stamps, compare: Compare,
             every: bool = False) -> Evidence:
    """The record held against the job and its files' stamps `now`.

    Unless `every`, it compares only as far as a verdict needs: no files once
    the work has changed, and no outputs once an input has, so as to hash no
    more than it must. With `every`, it compares each input and output whatever
    else differs, as `plan --job` shows them.
    """
    if record is None:
        return Evidence(job, None, now)
    work_changed = record.fingerprint != job.work.fingerprint
    parts = changed_parts(record, job) if work_changed else ()
    found = Evidence(job, record, now, work_changed, parts, record.checks != job.work.checks_key)
    if work_changed and not every:
        return found
    # A record holds its files by slot, so files moved to new paths are
    # the same files when their stamps or contents match.
    hashes: dict[str, str | None] = {}
    inputs = compare_files(dag, job.input_paths_by_slot, now, record.inputs, record.input_hashes, compare,
                           hashes, every)
    if changed(inputs) and not every:
        return Evidence(job, record, now, work_changed, parts, found.checks_changed, inputs)
    outputs = compare_files(dag, job.output_paths_by_slot, now, record.outputs, record.output_hashes, compare,
                            hashes, every)
    return Evidence(job, record, now, work_changed, parts, found.checks_changed, inputs, outputs)

