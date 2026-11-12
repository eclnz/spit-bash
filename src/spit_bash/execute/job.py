"""Run one job on the files it has: its checks, `verify` commands and commands, and what it leaves at its outputs;
or move its recorded outputs into place."""

from __future__ import annotations

import asyncio
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Awaitable, Callable, Protocol, Union

from ..files import FolderEntries, Stamps, folder_entries, folder_inputs, stamp, stamps, stored
from ..logs import StampedLog, open_log
from ..model import Check, Dag, DagError, Job, Kind, Layout, Side
from ..reuse.relocate import Migration
from .failure import (CheckArtifactMissing, CheckFailed, CommandFailed, Failure, InputChanged, InputDisappeared,
                      NoCommand, OutputsMissing, Phase, PlaceFailed, StdoutNotMoved)
from .options import Options
from .process import CommandEnd, partial, run_command, succeeded

Log = Union[IO[bytes], StampedLog]


class Runner(Protocol):
    """What starts a command, as `process.run_command` does."""

    def __call__(self, dag: Dag, args: tuple[str, ...], log: Log, stdin: str | None = None,
                 stdout: str | None = None, limit: int | None = None) -> Awaitable[CommandEnd]: ...


@dataclass(frozen=True)
class Made:
    """The job succeeded: its inputs' and outputs' stamps once it ended, and how long it took from its first check."""

    inputs: Stamps
    outputs: Stamps
    seconds: float


@dataclass(frozen=True)
class Failed:
    """The job failed; `log` is its log, or None when it failed before one was opened."""

    failure: Failure
    log: Path | None


Outcome = Union[Made, Failed]

# What starts the first line of a job's log, before why the plan ran it.
REASON_PREFIX = "spit-bash: planned to run: "


def log_path(logs: Path, job: Job) -> Path:
    return logs / f"{job.id}-{job.operation}.log"


async def _checks(dag: Dag, checks: tuple[Check, ...], log: Log, runner: Runner) -> Failure | None:
    """Run checks in order, stopping at the first that fails."""
    for check in checks:
        if check.side is Side.BEFORE and stamps(dag, (check.path,))[check.path] is None:
            return CheckArtifactMissing(check)
        end = await runner(dag, check.command, log)
        if not succeeded(end):
            return CheckFailed(check, end)
    return None


async def check_job(dag: Dag, job: Job, log: Path, options: Options, runner: Runner = run_command) -> Failure | None:
    """Run a current job's checks alone, on the files it already has, writing their output to `log`."""
    with open_log(log, options.timestamp_logs) as stream:
        return await _checks(dag, job.checks, stream, runner)


def place_job(dag: Dag, placement: Migration) -> PlaceFailed | None:
    """Move a job's recorded outputs to its current paths, all of them or none.

    A file is renamed, so it keeps its size and time, and each one moved is
    checked against the record's stamps. Nothing is overwritten: a destination
    that appeared since the plan stops the move. A move that fails or does not
    check puts back every file it moved.
    """
    assert placement.record is not None, "a placement with moves has a record"
    done: list[tuple[str, str]] = []
    try:
        for old, new in placement.moves:
            source, target = dag.checked(old), dag.checked(new)
            if os.path.lexists(target):
                raise OSError(f"{new} already exists")
            target.parent.mkdir(parents=True, exist_ok=True)
            os.rename(source, target)
            done.append((old, new))
            key = placement.job.identities[new]
            if stored(stamp(target, new in dag.folders)) != placement.record.outputs.get(key):
                raise OSError(f"{new} does not match the record after the move")
    except (OSError, DagError) as exc:
        try:
            for old, new in reversed(done):
                os.rename(dag.checked(new), dag.checked(old))
        except (OSError, DagError) as undo:
            return PlaceFailed(str(undo), False)
        return PlaceFailed(str(exc), True)
    return None


def _clear(path: Path) -> None:
    """Remove whatever is at an output's path, file or folder.

    The job owns the path, as SPIT puts nothing else there or inside it. Before
    the command, it clears an output folder, so that files an earlier run left
    do not mix with this run's, and the folder is made again, empty, for the
    tool to write into. After a failure, it clears every output, so that a
    half-written file does not look finished.
    """
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def _remove_outputs(dag: Dag, job: Job, log: Log, keep: bool = False) -> None:
    """Remove what a failed or cancelled command left at the job's outputs, and say so in its log.

    Called only once the command's process group has stopped, so nothing is
    still writing to them. With `keep`, as for `--keep-failed`, the log names
    what is left instead.
    """
    for output in job.output_paths:
        path = dag.checked(output)
        if not (path.is_symlink() or path.exists()):
            continue
        if keep:
            log.write(f"spit-bash: kept failed output {output}\n".encode())
            continue
        try:
            _clear(path)
        except OSError as exc:
            log.write(f"spit-bash: could not remove failed output {output}: {exc}\n".encode())
        else:
            log.write(f"spit-bash: removed failed output {output}\n".encode())
    # The standard output the command wrote before it failed, not yet renamed onto its output.
    if job.stdout is not None:
        temporary = partial(dag, job.stdout)
        if keep:
            if temporary.exists():
                log.write(f"spit-bash: kept failed standard output {temporary.relative_to(dag.base(job.stdout))}\n".encode())
            log.flush()
            return
        try:
            temporary.unlink(missing_ok=True)
        except OSError as exc:
            log.write(f"spit-bash: could not remove partial output {temporary}: {exc}\n".encode())
    log.flush()


def _set_aside(dag: Dag, job: Job, log: Log) -> list[tuple[str, Path, Path]]:
    """Move what an earlier run left at each of the job's outputs out of the way, so the job starts
    from the state a first run does. Some tools refuse to write over a file that exists.

    An earlier version already waiting aside is a run that was killed, whose output may be half
    written, so it is the version to keep. Gives each output moved with its path and its place aside.
    """
    moved = []
    for output in job.output_paths:
        path = dag.checked(output)
        if not (path.is_symlink() or path.exists()):
            continue
        waiting = Layout.previous(path)
        if waiting.is_symlink() or waiting.exists():
            _clear(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.rename(waiting)
        moved.append((output, path, waiting))
    return moved


Listings = dict[Path, frozenset[str]]


def _listings(dag: Dag, job: Job) -> Listings:
    """The names in the folder of each of the job's outputs, to find what its commands wrote there."""
    listed = {}
    for output in job.output_paths:
        folder = dag.checked(output).parent
        if folder not in listed:
            try:
                listed[folder] = frozenset(os.listdir(folder))
            except OSError:
                listed[folder] = frozenset()
    return listed


def _written(dag: Dag, missing: tuple[str, ...], before: Listings) -> tuple[str, ...]:
    """The files that appeared, since `before`, in the folders of the `missing` outputs, other than the DAG's artifacts
    and the runner's own files, by path under the output's root."""
    found = set()
    for output in missing:
        folder = dag.checked(output).parent
        base = dag.base(output)
        try:
            now = os.listdir(folder)
        except OSError:
            continue
        for name in set(now) - before.get(folder, frozenset()):
            if name.endswith((".spit-bash-previous", ".spit-bash-partial")):
                continue
            relative = (folder / name).relative_to(base).as_posix()
            if relative not in dag.absolute:
                found.add(relative)
    return tuple(sorted(found))


def _settle_aside(moved: list[tuple[str, Path, Path]], log: Log, restore: bool) -> None:
    """Delete the earlier versions after a success, or put them back after a failure.

    Call after `_remove_outputs`, so that nothing is at the paths a version goes back to.
    Without `restore`, as for `--keep-failed`, the failed output stays and the earlier version goes.
    """
    for output, path, waiting in moved:
        try:
            if restore and not (path.is_symlink() or path.exists()):
                waiting.rename(path)
                log.write(f"spit-bash: restored the earlier {output}\n".encode())
            else:
                _clear(waiting)
        except OSError as exc:
            log.write(f"spit-bash: could not settle the earlier {output} at {waiting}: {exc}\n".encode())
    log.flush()


async def _make(dag: Dag, job: Job, before: Stamps, log: Log, limit: int | None,
                entries: dict[str, FolderEntries], listed: Listings,
                runner: Runner) -> Failure | tuple[Stamps, Stamps]:
    """Run a job's commands in order, then its output checks; give a failure, or its inputs' and outputs' stamps.

    The first command that fails stops the job, and the commands after it do not run.
    """
    for index, run in enumerate(job.commands, 1):
        end = await runner(dag, run.arguments, log, run.stdin, run.stdout, limit)
        if not succeeded(end):
            return CommandFailed(Phase.COMMAND, index, end)
        if run.stdout is not None:
            # Only a command that succeeded puts its standard output at the output's path, where
            # the commands after it may read it.
            try:
                partial(dag, run.stdout).replace(dag.checked(run.stdout))
            except OSError as exc:
                return StdoutNotMoved(run.stdout, str(exc))
    after = stamps(dag, job.input_paths)
    changed_folders = tuple(sorted(
        path for path, listed in entries.items()
        if listed != folder_entries(dag.path(path))
    ))
    if after != before or changed_folders:
        return InputChanged(before, after, changed_folders)
    outputs = stamps(dag, job.output_paths)
    missing = tuple((port.name, artifact.path) for port in job.outputs for artifact in port.artifacts
                    if outputs[artifact.path] is None)
    if missing:
        return OutputsMissing(missing, _written(dag, tuple(path for _, path in missing), listed))
    failure = await _checks(dag, job.after, log, runner)
    if failure is not None:
        return failure
    return after, outputs


async def run_job(dag: Dag, job: Job, log: Path, options: Options, runner: Runner = run_command,
                  on_start: Callable[[], None] = lambda: None,
                  on_discard: Callable[[], None] = lambda: None, reason: str | None = None) -> Outcome:
    """Run one job: its input checks and `verify` commands, its commands, then its output checks.

    `on_start` is called once the job's inputs are all there and it has a
    command, before its log at `log` is opened. A job that fails once its
    first command has started loses its outputs, unless `options.keep_failed`
    keeps them, and gets back what an earlier run left at them; one cancelled
    meanwhile does the same before the cancellation goes on.

    `reason`, why the plan ran the job, is the log's first line, so the log answers why
    the job ran after its record has been replaced.

    `on_discard` is called when a failed or cancelled job keeps its failed
    outputs in place of an earlier run's, so that the earlier run's record no
    longer describes what is at its paths. Otherwise, whatever the outcome, the
    earlier run's files are at the job's paths until the job succeeds.
    """
    before = stamps(dag, job.input_paths)
    if any(value is None for value in before.values()):
        return Failed(InputDisappeared(), None)
    if not job.commands:
        return Failed(NoCommand(), None)
    entries = folder_inputs(dag, job)
    on_start()
    started = time.monotonic()
    limit = job.memory if options.enforce_memory else None
    keep = options.keep_failed
    with open_log(log, options.timestamp_logs) as stream:
        if reason is not None:
            stream.write(f"{REASON_PREFIX}{reason}\n".encode())
        failure = await _checks(dag, job.before, stream, runner)
        if failure is not None:
            return Failed(failure, log)
        for index, command in enumerate(job.verify, 1):
            end = await runner(dag, command, stream, limit=limit)
            if not succeeded(end):
                return Failed(CommandFailed(Phase.VERIFY, index, end), log)
        # An earlier run's files would mix with this run's, and some tools refuse to write over
        # them: the job starts with nothing at its outputs, as a first run does.
        moved = _set_aside(dag, job, stream)
        for output in job.output_artifacts:
            written = dag.checked(output.path)
            if output.kind is Kind.FOLDER:
                # The tool writes into an empty folder.
                written.mkdir(parents=True)
            else:
                written.parent.mkdir(parents=True, exist_ok=True)
        listed = _listings(dag, job)
        # From here on, a failure removes what the command left at the job's
        # outputs, unless they are kept. A cancelled command's process group is stopped before
        # the cancellation reaches this point.
        try:
            made = await _make(dag, job, before, stream, limit, entries, listed, runner)
        except asyncio.CancelledError:
            _remove_outputs(dag, job, stream, keep)
            _settle_aside(moved, stream, not keep)
            if keep:
                on_discard()
            raise
        if not isinstance(made, tuple):
            _remove_outputs(dag, job, stream, keep)
            _settle_aside(moved, stream, not keep)
            if keep:
                on_discard()
            return Failed(made, log)
        _settle_aside(moved, stream, False)
    after, outputs = made
    return Made(after, outputs, time.monotonic() - started)
