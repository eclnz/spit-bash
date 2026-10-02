"""Run independent ready jobs concurrently, without a shell."""

from __future__ import annotations

import asyncio
import os
import shlex
import shutil
import signal
from collections import deque
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import IO

from .dag import Dag, Job
from .plan import Decision, RunRecord, State, Status, job_key, stamps

LOG_TAIL_LINES = 20


@dataclass(frozen=True)
class Result:
    job: Job
    error: str | None = None
    log: Path | None = None


def log_path(logs: Path, job: Job) -> Path:
    return logs / f"{job.id}-{job.operation}.log"


def log_tail(path: Path, lines: int = LOG_TAIL_LINES) -> list[str]:
    try:
        with path.open(encoding="utf-8", errors="replace") as stream:
            return list(deque((line.rstrip("\n") for line in stream), lines))
    except OSError:
        return []


class Step(Enum):
    VERIFY = "verify"
    COMMAND = "command"


async def _command(dag: Dag, args: tuple[str, ...], step: Step, index: int, log: IO[bytes]) -> str | None:
    """Run one of a job's commands; `index` numbers its verify commands from 1."""
    kind = f"verify {index}" if step is Step.VERIFY else "command"
    log.write(f"$ {shlex.join(args)}\n".encode())
    log.flush()
    try:
        # A session of its own lets a cancelled job's whole process group be
        # stopped, and keeps a terminal's Ctrl-C from reaching it directly.
        process = await asyncio.create_subprocess_exec(
            *args, cwd=dag.root, stdin=asyncio.subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
        )
    except OSError as exc:
        return f"{kind} could not start: {exc}"
    try:
        code = await process.wait()
    except asyncio.CancelledError:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        await process.wait()
        raise
    return None if code == 0 else f"{kind} exited with status {code}"


def _clear(path: Path) -> None:
    """Remove what an earlier run left at a folder output's path.

    The job owns the folder, as SPIT puts nothing else inside it, so files an
    earlier run left would only mix with this run's. The tool makes the folder
    itself: some refuse to write into one that exists.
    """
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


async def execute(dag: Dag, decisions: tuple[Decision, ...], state: State, workers: int, logs: Path) -> tuple[Result, ...]:
    """Run every job planned to run; stop them all if the process gets SIGTERM."""
    semaphore = asyncio.Semaphore(workers)
    tasks: dict[int, asyncio.Task[Result]] = {}
    logs.mkdir(parents=True, exist_ok=True)

    async def run_one(decision: Decision) -> Result:
        job = decision.job
        for dependency in job.depends_on:
            if dependency in tasks:
                result = await tasks[dependency]
                if result.error:
                    return Result(job, f"dependency {dependency} failed")
        async with semaphore:
            before = stamps(dag, job.inputs)
            if any(value is None for value in before.values()):
                return Result(job, "input disappeared before execution")
            if job.command is None:
                return Result(job, "job has no command")
            path = log_path(logs, job)
            print(f"[{job.id}] start {job.operation}", flush=True)
            state.forget(job_key(job))
            with path.open("wb") as log:
                for index, command in enumerate(job.verify, 1):
                    error = await _command(dag, command, Step.VERIFY, index, log)
                    if error:
                        return Result(job, error, path)
                for output in job.outputs:
                    written = dag.path(output)
                    if output in dag.folders:
                        _clear(written)
                    written.parent.mkdir(parents=True, exist_ok=True)
                error = await _command(dag, job.command, Step.COMMAND, 0, log)
            if error:
                return Result(job, error, path)
            after = stamps(dag, job.inputs)
            if after != before:
                return Result(job, "input changed during execution", path)
            outputs = stamps(dag, job.outputs)
            if any(value is None for value in outputs.values()):
                return Result(job, "command succeeded but did not create every output", path)
            state.record(job_key(job), RunRecord(fingerprint=job.fingerprint, inputs=after, outputs=outputs))
            print(f"[{job.id}] done", flush=True)
            return Result(job, None, path)

    loop = asyncio.get_running_loop()
    main = asyncio.current_task()
    assert main is not None
    loop.add_signal_handler(signal.SIGTERM, main.cancel)
    try:
        for decision in decisions:
            if decision.status is Status.RUN:
                tasks[decision.job.id] = asyncio.create_task(run_one(decision))
        return tuple(await asyncio.gather(*tasks.values()))
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
