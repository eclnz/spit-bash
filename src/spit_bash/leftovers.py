"""Outputs that earlier runs made for jobs the pipeline no longer has, and their removal."""

from __future__ import annotations

import shutil
from dataclasses import dataclass

from .files import stamp, stored
from .model import Dag, DagError
from .schedule import size
from .state.record import Record
from .state.store import StateWrite


@dataclass(frozen=True)
class Leftover:
    """An output on disk that the job of record `key` made, and no job of the DAG makes or reads now."""

    key: str
    path: str
    size: int
    folder: bool
    # Whether it is as the run that made it left it, so removing it loses nothing that run did not make.
    unchanged: bool


def leftovers(everything: Dag, records: dict[str, Record]) -> tuple[Leftover, ...]:
    """Each output of a record no job of `everything` claims, that still exists, in the order of its path.

    `records` are the state's after relabelling, so a renamed job's record is already its own. A record kept
    from before the runner stored output paths names no files, and is left out. A path a job makes or reads
    now is the pipeline's again, whatever an old record says of it.
    """
    claimed = {job.work.key for job in everything.jobs}
    used = {path for job in everything.jobs for path in (*job.input_paths, *job.output_paths)}
    used.update(everything.external_inputs)
    found = []
    for key, record in records.items():
        if key in claimed or record.output_paths is None:
            continue
        for slot, path in record.output_paths.items():
            recorded = record.outputs.get(slot)
            if path in used or recorded is None:
                continue
            folder = len(recorded) == 3
            try:
                now = stamp(everything.checked(path), folder)
            except DagError:
                # A path the state file names outside the root is no output of this dataset.
                continue
            if now is not None:
                found.append(Leftover(key, path, now.size, folder, stored(now) == recorded))
    return tuple(sorted(found, key=lambda leftover: leftover.path))


def remove(dag: Dag, found: tuple[Leftover, ...]) -> tuple[StateWrite, ...]:
    """Delete each unchanged leftover, and give the writes that forget each record left with no output on
    disk. A changed leftover is kept, and so is its record."""
    kept_keys = {leftover.key for leftover in found if not leftover.unchanged}
    for leftover in found:
        if not leftover.unchanged:
            continue
        # Checked again now: a link made since planning must not lead the deletion outside the root.
        path = dag.checked(leftover.path)
        if leftover.folder:
            shutil.rmtree(path)
        else:
            path.unlink()
    forgotten = sorted({leftover.key for leftover in found} - kept_keys)
    return tuple(StateWrite(key, None) for key in forgotten)


def amount(size_bytes: int) -> str:
    """A size as `512 bytes`, or as SPIT writes memory, as `1.5G`."""
    return f"{size_bytes} bytes" if size_bytes < 1024 else size(size_bytes)


def lines(found: tuple[Leftover, ...], verbose: bool, removing: bool) -> list[str]:
    """What to say of the leftovers: a count with their total size, and with `verbose` each one."""
    if not found:
        return []
    total = amount(sum(leftover.size for leftover in found))
    changed = sum(not leftover.unchanged for leftover in found)
    noun = "output" if len(found) == 1 else "outputs"
    head = f"{len(found)} {noun} ({total}) left over from jobs the pipeline no longer has"
    if removing:
        head += "; removing those unchanged since their run" + (f", keeping {changed} changed" if changed else "")
    else:
        head += "; run --remove-leftovers to delete those unchanged since their run"
        if not verbose:
            head += ", or --verbose to list them"
    listed = [f"  {leftover.path} ({amount(leftover.size)}" + ("" if leftover.unchanged else ", changed since its run")
              + ")" for leftover in found] if verbose else []
    return [head, *listed]
