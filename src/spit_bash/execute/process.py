"""Start one command without a shell, log what it writes, hold it to its memory, and stop it when cancelled."""

from __future__ import annotations

import asyncio
import os
import shlex
import signal
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Union

from ..logs import StampedLog, drain, pump
from ..memory import group_memory
from ..model import Dag, Layout

# How long a cancelled command has to exit after SIGTERM before its process
# group is killed.
STOP_GRACE_SECONDS = 5.0
# How often a running command's memory is read, when it has a limit.
MEMORY_POLL_SECONDS = 0.5


@dataclass(frozen=True)
class Exited:
    """The command exited by itself, or by a signal other than SIGKILL, which `code` gives negated."""

    code: int


@dataclass(frozen=True)
class KilledMemory:
    """The runner killed the command's process group once it held `used` bytes, more than `limit`."""

    used: int
    limit: int


@dataclass(frozen=True)
class KilledSignal:
    """Something other than the runner killed the command with SIGKILL, as an out-of-memory killer does.

    `limit` is the memory its job declares, when it is held to one.
    """

    limit: int | None


@dataclass(frozen=True)
class NotStarted:
    """The command could not be started, or its standard input or output could not be opened."""

    error: str


CommandEnd = Union[Exited, KilledMemory, KilledSignal, NotStarted]


def succeeded(end: CommandEnd) -> bool:
    return isinstance(end, Exited) and end.code == 0


def partial(dag: Dag, stdout: str) -> Path:
    """The temporary file a command writes its standard output to, beside the output `stdout` it becomes."""
    return Layout.partial(dag.checked(stdout))


async def run_command(dag: Dag, args: tuple[str, ...], log: IO[bytes] | StampedLog,
                      stdin: str | None = None, stdout: str | None = None, limit: int | None = None) -> CommandEnd:
    """Run one command from the output root and say how it ended.

    The command reads the input file `stdin` on its standard input, or nothing.
    Its standard output goes to the temporary file for the output `stdout`, or
    to the log; its standard error always goes to the log. A command whose
    process group holds more than `limit` bytes is killed. Cancelling the call
    stops the command's process group before the cancellation goes on.
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
            stamped = isinstance(log, StampedLog)
            # A stamped log is written by the runner, so the command's output passes through pipes to it.
            piped = asyncio.subprocess.PIPE
            target = (piped if stamped else log) if stdout is None else files.enter_context(partial(dag, stdout).open("wb"))
            # A session of its own lets a cancelled job's whole process group be
            # stopped, and keeps a terminal's Ctrl-C from reaching it directly.
            process = await asyncio.create_subprocess_exec(
                *args, cwd=dag.outputs, stdin=source, stdout=target, stderr=piped if stamped else log,
                start_new_session=True,
            )
        except OSError as exc:
            return NotStarted(str(exc))
    pumps = [asyncio.ensure_future(pump(stream, log)) for stream in (process.stdout, process.stderr)
             if stream is not None and isinstance(log, StampedLog)]
    try:
        return await _finish(process, pumps, limit)
    except asyncio.CancelledError:
        for task in pumps:
            task.cancel()
        raise


async def _finish(process: asyncio.subprocess.Process, pumps: list[asyncio.Task[None]],
                  limit: int | None) -> CommandEnd:
    """Wait for a started command, and for its output to reach the log."""
    try:
        if limit is None:
            code = await process.wait()
        else:
            used = await _within(process, limit)
            if used is not None:
                await _kill(process)
                await drain(pumps)
                return KilledMemory(used, limit)
            code = process.returncode
    except asyncio.CancelledError:
        await _stop(process)
        raise
    await drain(pumps)
    if code == -signal.SIGKILL:
        return KilledSignal(limit)
    assert code is not None, "a command that was waited for has a return code"
    return Exited(code)


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
