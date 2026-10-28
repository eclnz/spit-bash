"""Choose which ready job starts next: the one with the longest expected path to the end of the run."""

from __future__ import annotations

import asyncio
import heapq
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from statistics import median

from .dag import Job


def longest_paths(jobs: Sequence[Job], weight: Callable[[Job], float]) -> tuple[dict[int, float], dict[int, float]]:
    """How long, at least, the run has left from each job's start, and from its end.

    `jobs` are in DAG order, so every job comes after the jobs it depends on,
    and a dependency outside `jobs` is ignored. A job's path from its end is the
    longest path from the start of any job that depends on it, or 0; from its
    start, its weight more. One pass, from the last job back.
    """
    after = {job.id: 0.0 for job in jobs}
    start: dict[int, float] = {}
    for job in reversed(jobs):
        start[job.id] = weight(job) + after[job.id]
        for dependency in job.depends_on:
            if dependency in after and after[dependency] < start[job.id]:
                after[dependency] = start[job.id]
    return start, after


def ranks(jobs: Sequence[Job], expected: Callable[[Job], float | None], previous: dict[int, float]) -> dict[int, float]:
    """Each job's rank: its longest path to the end of the run, by expected times.

    A job with no expected time counts as the median of every time in
    `previous`, or as 1 second when there is none, so that a job heading a
    long chain still ranks above one with nothing after it.
    """
    default = median(previous.values()) if previous else 1.0

    def weight(job: Job) -> float:
        seconds = expected(job)
        return default if seconds is None else seconds

    return longest_paths(jobs, weight)[0]


class ReadyQueue:
    """Worker slots, and the ready jobs waiting for one, highest rank first.

    Jobs that become ready at the same moment, such as every job at the start
    of a run or the dependents of a job that just finished, all join the queue
    before any of them is admitted, so the order depends on rank, then job id,
    and not on which task the event loop wakes first.
    """

    def __init__(self, workers: int, ranks: dict[int, float]) -> None:
        self.free = workers
        self._ranks = ranks
        self._waiting: list[tuple[float, int, asyncio.Future[None]]] = []
        self._admitting = False

    @asynccontextmanager
    async def slot(self, job: Job) -> AsyncIterator[None]:
        """Wait for a worker slot, and hold it until the block ends."""
        admitted = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiting, (-self._ranks.get(job.id, 0.0), job.id, admitted))
        self._admit_soon()
        try:
            await admitted
        except asyncio.CancelledError:
            # Admitted, then cancelled before it could start: give the slot back.
            if admitted.done() and not admitted.cancelled():
                self._release()
            raise
        try:
            yield
        finally:
            self._release()

    def _release(self) -> None:
        self.free += 1
        self._admit_soon()

    def _admit_soon(self) -> None:
        # Once the tasks woken with this one have queued too.
        if not self._admitting:
            self._admitting = True
            asyncio.get_running_loop().call_soon(self._admit)

    def _admit(self) -> None:
        """Start the highest-ranked waiting jobs while slots are free; the one place a job is let in."""
        self._admitting = False
        while self.free > 0 and self._waiting:
            _, _, admitted = heapq.heappop(self._waiting)
            if admitted.done():
                # Cancelled while it waited.
                continue
            self.free -= 1
            admitted.set_result(None)
