"""A job's log file, optionally with the time the runner received each line."""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import IO

# The longest stretch of a command's output held while waiting for a newline: a
# longer line is written in pieces, each with its own time.
LINE_LIMIT = 64 * 1024
# How long the runner waits for a command's output to end after the command has.
# A process it started that outlives it can hold the pipes open.
DRAIN_SECONDS = 1.0
# How many of a failed job's last log lines its failure report shows.
LOG_TAIL_LINES = 20


def now() -> str:
    """The local time with its UTC offset and millisecond precision, such as 2026-11-07T15:12:03.142+13:00."""
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


class StampedLog:
    """A log file whose every line starts with the time the runner received it.

    Not the time the tool produced it: a tool may buffer its output. Whole lines
    are written at a time, so lines from a command's two streams are not mixed.
    """

    def __init__(self, path: Path):
        self.file: IO[bytes] = path.open("wb")

    def __enter__(self) -> StampedLog:
        return self

    def __exit__(self, *exc: object) -> None:
        self.file.close()

    def write(self, data: bytes) -> int:
        stamp = f"{now()} ".encode()
        self.file.write(b"".join(stamp + line for line in data.splitlines(keepends=True)))
        return len(data)

    def flush(self) -> None:
        self.file.flush()


def log_tail(path: Path, lines: int = LOG_TAIL_LINES) -> list[str]:
    """A log's last lines, or none when it cannot be read."""
    try:
        with path.open(encoding="utf-8", errors="replace") as stream:
            return list(deque((line.rstrip("\n") for line in stream), lines))
    except OSError:
        return []


def open_log(path: Path, timestamps: bool) -> IO[bytes] | StampedLog:
    return StampedLog(path) if timestamps else path.open("wb")


async def pump(reader: asyncio.StreamReader, log: StampedLog) -> None:
    """Copy a command's output into the log a line at a time until it ends."""
    held = b""
    while chunk := await reader.read(LINE_LIMIT):
        held += chunk
        # The lines of one read arrived together, so they share a time.
        whole, newline, held = held.rpartition(b"\n")
        if newline:
            log.write(whole + newline)
        if len(held) >= LINE_LIMIT:
            log.write(held + b"\n")
            held = b""
        log.flush()
    if held:
        log.write(held + b"\n")
        log.flush()


async def drain(pumps: list[asyncio.Task[None]]) -> None:
    """Wait for the pumps to reach the end of the output, then stop any that a lingering process holds open."""
    if not pumps:
        return
    _, waiting = await asyncio.wait(pumps, timeout=DRAIN_SECONDS)
    for task in waiting:
        task.cancel()
    await asyncio.gather(*pumps, return_exceptions=True)
