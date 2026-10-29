"""Run independent ready jobs concurrently, without a shell."""

from __future__ import annotations

import asyncio
import os
import shlex
import shutil
import signal
import time
from collections import deque
from contextlib import ExitStack
from dataclasses import dataclass, replace
from pathlib import Path
from typing import IO

from .dag import Check, Dag, Job
from .memory import group_memory
from .plan import Decision, RunRecord, State, Status, hashes, job_key, judge, stamps
from .progress import Progress, about, duration
from .schedule import ReadyQueue, ranks, size, too_big

LOG_TAIL_LINES = 20
# Files this small, all of a job's together, are hashed on the event loop.
INLINE_HASH_BYTES = 1 << 20
# How long a cancelled command has to exit after SIGTERM before its process
# group is killed.
STOP_GRACE_SECONDS = 5.0
# How often a running command's memory is read, when it has a limit.
MEMORY_POLL_SECONDS = 0.5


@dataclass(frozen=True)
class Result:
    job: Job
    error: str | None = None
    log: Path | None = None
    # What a job planned `maybe` turned out to be, once its upstream jobs had finished.
    settled: Status | None = None


@dataclass(frozen=True)
class _Ran:
    """A job whose command and checks passed, before its files are hashed and recorded."""

    after: dict[str, list[int] | None]
    outputs: dict[str, list[int] | None]
    log: Path
    # How long it took, from its first check to its last.
    seconds: float


def log_path(logs: Path, job: Job) -> Path:
    return logs / f"{job.id}-{job.operation}.log"


def log_tail(path: Path, lines: int = LOG_TAIL_LINES) -> list[str]:
    try:
        with path.open(encoding="utf-8", errors="replace") as stream:
            return list(deque((line.rstrip("\n") for line in stream), lines))
    except OSError:
        return []


def partial(dag: Dag, stdout: str) -> Path:
    """The temporary file a command writes its standard output to, beside the output `stdout` it becomes."""
    path = dag.checked(stdout)
    return path.with_name(f".{path.name}.spit-bash-partial")


async def _command(dag: Dag, args: tuple[str, ...], kind: str, log: IO[bytes],
                   stdin: str | None = None, stdout: str | None = None, limit: int | None = None) -> str | None:
    """Run one of a job's commands; `kind` names it in an error, as `verify 1`, `command` or `command 2`.

    The command reads the input file `stdin` on its standard input, or nothing.
    Its standard output goes to the temporary file for the output `stdout`, or
    to the log; its standard error always goes to the log. A command whose
    process group holds more than `limit` bytes is killed.
    """
    shown = shlex.join(args)
    if stdin is not None:
        shown += f" < {shlex.quote(stdin)}"
    if stdout is not None:
        shown += f" > {shlex.quote(stdout)}"
    log.write(f"$ {shown}\n".encode())
    log.flush()
    with ExitStack() as files:
        try:
            source = asyncio.subprocess.DEVNULL if stdin is None else files.enter_context(dag.checked(stdin).open("rb"))
            target = log if stdout is None else files.enter_context(partial(dag, stdout).open("wb"))
            # A session of its own lets a cancelled job's whole process group be
            # stopped, and keeps a terminal's Ctrl-C from reaching it directly.
            process = await asyncio.create_subprocess_exec(
                *args, cwd=dag.root, stdin=source, stdout=target, stderr=log, start_new_session=True,
            )
        except OSError as exc:
            return f"{kind} could not start: {exc}"
    try:
        if limit is None:
            code = await process.wait()
        else:
            used = await _within(process, limit)
            if used is not None:
                await _kill(process)
                return f"{kind} used {size(used)} of memory, more than the {size(limit)} its job declares, and was killed"
            code = process.returncode
    except asyncio.CancelledError:
        await _stop(process)
        raise
    return None if code == 0 else f"{kind} exited with status {code}"


async def _within(process: asyncio.subprocess.Process, limit: int) -> int | None:
    """Wait for a command, reading its process group's memory as it runs; give what it held once it went over `limit`."""
    while True:
        try:
            # Shielded, so that a timeout leaves the wait to go on.
            await asyncio.wait_for(asyncio.shield(process.wait()), MEMORY_POLL_SECONDS)
            return None
        except asyncio.TimeoutError:
            # Off the event loop: it reads a file for every process, or runs `ps`.
            used = await asyncio.to_thread(group_memory, process.pid)
            if used > limit:
                return used


async def _kill(process: asyncio.subprocess.Process) -> None:
    """Kill a command's process group at once: one over its memory has no grace period to grow in."""
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    await process.wait()


async def _stop(process: asyncio.subprocess.Process) -> None:
    """Stop a cancelled command's process group: SIGTERM, then SIGKILL after a grace period.

    The group is killed even when the command exits in time, so that a child
    which ignores SIGTERM does not outlive it.
    """
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(process.wait(), STOP_GRACE_SECONDS)
    except asyncio.TimeoutError:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    await process.wait()


async def _checks(dag: Dag, checks: tuple[Check, ...], log: IO[bytes]) -> str | None:
    """Run checks in order, stopping at the first that fails."""
    for check in checks:
        if check.when == "before" and stamps(dag, (check.path,))[check.path] is None:
            return f"{check.describe()}: the artifact is missing"
        error = await _command(dag, check.command, check.describe(), log)
        if error:
            return error.replace(" exited ", " failed: exited ", 1)
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


def _remove_outputs(dag: Dag, job: Job, log: IO[bytes], keep: bool = False) -> None:
    """Remove what a failed or cancelled command left at the job's outputs, and say so in its log.

    Called only once the command's process group has stopped, so nothing is
    still writing to them. With `keep`, as for `--keep-failed`, the log names
    what is left instead.
    """
    for output in job.outputs:
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
                log.write(f"spit-bash: kept failed standard output {temporary.relative_to(dag.root)}\n".encode())
            log.flush()
            return
        try:
            temporary.unlink(missing_ok=True)
        except OSError as exc:
            log.write(f"spit-bash: could not remove partial output {temporary}: {exc}\n".encode())
    log.flush()


async def _make(dag: Dag, job: Job, before: dict[str, list[int] | None], log: IO[bytes],
                limit: int | None) -> str | tuple[dict[str, list[int] | None], dict[str, list[int] | None]]:
    """Run a job's commands in order, then its output checks; give an error, or its inputs' and outputs' stamps.

    The first command that fails stops the job, and the commands after it do not run.
    """
    for index, run in enumerate(job.commands, 1):
        kind = run.describe(index, len(job.commands))
        error = await _command(dag, run.arguments, kind, log, run.stdin, run.stdout, limit)
        if error:
            return error
        if run.stdout is not None:
            # Only a command that succeeded puts its standard output at the output's path, where
            # the commands after it may read it.
            try:
                partial(dag, run.stdout).replace(dag.checked(run.stdout))
            except OSError as exc:
                return f"could not move standard output onto {run.stdout}: {exc}"
    after = stamps(dag, job.inputs)
    if after != before:
        return "input changed during execution"
    outputs = stamps(dag, job.outputs)
    missing = [f"output {output.port} {output.path}" for output in job.produced
               if outputs[output.path] is None]
    if missing:
        return "command succeeded but did not create " + ", ".join(missing)
    error = await _checks(dag, job.after, log)
    if error:
        return error
    return after, outputs


def _hash_both(dag: Dag, job: Job) -> tuple[dict[str, str], dict[str, str]]:
    return hashes(dag, job.inputs), hashes(dag, job.outputs)


async def execute(dag: Dag, decisions: tuple[Decision, ...], state: State, cores: int, logs: Path,
                  hashing: bool = True, memory: int | None = None,
                  keep_failed: bool = False, enforce_memory: bool = True) -> tuple[Result, ...]:
    """Run every job planned to run, within `cores` and `memory` bytes; stop them all if the process gets SIGTERM.

    `memory` None sets no limit on memory. A job that declares memory has each
    of its `verify` commands and commands killed when it holds more, unless
    `enforce_memory` is off. A job that fails once its first command has started
    loses its outputs, unless `keep_failed` keeps them for debugging.
    """
    tasks: dict[int, asyncio.Task[Result]] = {}
    logs.mkdir(parents=True, exist_ok=True)

    async def failed_dependency(job: Job) -> int | None:
        """Wait for the jobs this one needs, and name the first that failed."""
        for dependency in job.depends_on:
            if dependency in tasks and (await tasks[dependency]).error:
                return dependency
        return None

    async def run_one(decision: Decision) -> Result:
        job = decision.job
        failed = await failed_dependency(job)
        if failed is not None:
            if decision.status in (Status.RUN, Status.MAYBE):
                progress.dropped(job)
            return Result(job, f"dependency {failed} failed")
        if decision.status is Status.MAYBE:
            return await settle(job)
        return await perform(decision)

    async def settle(job: Job) -> Result:
        """Judge a job planned `maybe` again, now that its upstream jobs have finished."""
        # Off the event loop: it may hash files the upstream jobs rewrote.
        settled = await asyncio.to_thread(judge, dag, job, state, False, hashing)
        if settled.status is not Status.RUN:
            progress.dropped(job)
        if settled.status is Status.BLOCKED:
            return Result(job, settled.reason)
        if settled.status is Status.SKIP:
            if settled.refresh is not None:
                state.record(job_key(job), settled.refresh)
            print(f"[{job.id}] skip {job.operation}: upstream output unchanged", flush=True)
            return Result(job, None, None, Status.SKIP)
        result = await perform(settled)
        return replace(result, settled=settled.status)

    async def perform(decision: Decision) -> Result:
        job = decision.job
        async with queue.slot(job):
            if decision.status is Status.CHECK:
                return await check_one(job)
            progress.started(job)
            ran = await attempt(job)
            if isinstance(ran, Result):
                progress.finished(job, None)
                return ran
        # With the job's cores and memory freed. Big files are hashed off the event loop, so they
        # hold up neither another job's start nor the loop; small ones where they are,
        # since a thread hop costs more than hashing a small file.
        if not hashing:
            input_hashes = output_hashes = None
        elif sum(value[0] for value in (*ran.after.values(), *ran.outputs.values())) < INLINE_HASH_BYTES:
            input_hashes, output_hashes = _hash_both(dag, job)
        else:
            input_hashes, output_hashes = await asyncio.to_thread(_hash_both, dag, job)
        record = RunRecord(fingerprint=job.fingerprint, inputs=ran.after, outputs=ran.outputs, checks=job.checks_key,
                           input_hashes=input_hashes, output_hashes=output_hashes, seconds=round(ran.seconds, 3))
        state.record(job_key(job), record)
        progress.finished(job, ran.seconds)
        print(f"[{job.id}] done in {duration(ran.seconds)}; {progress.report()}", flush=True)
        return Result(job, None, ran.log)

    async def attempt(job: Job) -> Result | _Ran:
        """Run one job: its input checks and `verify` commands, its commands, then its output checks."""
        before = stamps(dag, job.inputs)
        if any(value is None for value in before.values()):
            return Result(job, "input disappeared before execution")
        if not job.commands:
            return Result(job, "job has no command")
        path = log_path(logs, job)
        print(f"[{job.id}] start {job.operation}", flush=True)
        started = time.monotonic()
        state.forget(job_key(job))
        limit = job.memory if enforce_memory else None
        with path.open("wb") as log:
            error = await _checks(dag, job.before, log)
            if error:
                return Result(job, error, path)
            for index, command in enumerate(job.verify, 1):
                error = await _command(dag, command, f"verify {index}", log, limit=limit)
                if error:
                    return Result(job, error, path)
            for output in job.outputs:
                written = dag.checked(output)
                if output in dag.folders:
                    # An earlier run's files would mix with this run's; the tool writes into an empty folder.
                    _clear(written)
                    written.mkdir(parents=True)
                else:
                    written.parent.mkdir(parents=True, exist_ok=True)
            # From here on, a failure removes what the command left at the job's
            # outputs, unless they are kept. A cancelled command's process group is stopped before
            # the cancellation reaches this point.
            try:
                made = await _make(dag, job, before, log, limit)
            except asyncio.CancelledError:
                _remove_outputs(dag, job, log, keep_failed)
                raise
            if isinstance(made, str):
                _remove_outputs(dag, job, log, keep_failed)
                return Result(job, made, path)
        after, outputs = made
        return _Ran(after, outputs, path, time.monotonic() - started)

    async def check_one(job: Job) -> Result:
        """Run a current job's checks alone, on the files it already has."""
        path = log_path(logs, job)
        print(f"[{job.id}] check {job.operation}", flush=True)
        key = job_key(job)
        record = state.jobs.get(key)
        with path.open("wb") as log:
            error = await _checks(dag, job.checks, log)
        if error:
            # The files fail the checks, so the job is no longer current.
            state.forget(key)
            return Result(job, error, path)
        if record is not None:
            # A command could change a file under a check; record the stamps it passed on.
            outputs, inputs = stamps(dag, job.outputs), stamps(dag, job.inputs)
            if outputs == record.outputs and inputs == record.inputs:
                state.record(key, record.model_copy(update={"checks": job.checks_key}))
        print(f"[{job.id}] checked", flush=True)
        return Result(job, None, path)

    # A job planned `maybe` counts as one that runs until it is skipped, so the
    # estimate is the longest the run can take.
    to_run = [decision.job for decision in decisions if decision.status in (Status.RUN, Status.MAYBE)]
    previous = {}
    for job in dag.jobs:
        record = state.jobs.get(job_key(job))
        if record is not None and record.seconds is not None:
            previous[job.id] = record.seconds
    progress = Progress.of(to_run, dag.jobs, previous, cores)
    # Every job that will take cores and memory, ranked by the longest chain of work it heads.
    slotted = [decision.job for decision in decisions if decision.status in (Status.RUN, Status.CHECK, Status.MAYBE)]
    queue = ReadyQueue(cores, memory, ranks(slotted, progress.expected, previous))
    for job in slotted:
        over = too_big(job, cores, memory)
        if over is not None:
            print(f"[{job.id}] needs {over}; it will run alone, once no other job is running", flush=True)
    if to_run:
        remaining = progress.remaining()
        estimate = "" if remaining is None else f", {about(remaining)}"
        maybe = sum(1 for decision in decisions if decision.status is Status.MAYBE)
        more = f", and {maybe} more that may" if maybe else ""
        runs = len(to_run) - maybe
        print(f"{runs} job{'' if runs == 1 else 's'} to run{more}{estimate}", flush=True)

    loop = asyncio.get_running_loop()
    main = asyncio.current_task()
    assert main is not None
    loop.add_signal_handler(signal.SIGTERM, main.cancel)
    try:
        for decision in decisions:
            if decision.status in (Status.RUN, Status.CHECK, Status.MAYBE):
                tasks[decision.job.id] = asyncio.create_task(run_one(decision))
        return tuple(await asyncio.gather(*tasks.values()))
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
