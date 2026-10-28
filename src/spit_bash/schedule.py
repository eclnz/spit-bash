"""Choose which ready job starts next, the one with the longest expected path to the end of the run, and when its cores and memory are free."""

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


def size(memory: int) -> str:
    """A number of bytes as SPIT writes memory, as `64M` or `1.5G`, each unit 1024 times the one before."""
    value, unit = float(memory), ""
    for next_unit in "KMGT":
        if value < 1024:
            break
        value, unit = value / 1024, next_unit
    return f"{value:.1f}".removesuffix(".0") + unit


def too_big(job: Job, cores: int, memory: int | None) -> str | None:
    """What a job needs beyond everything the run has, as `16 cores where the run has 8`, or None when it fits."""
    over = []
    if job.cores > cores:
        over.append(f"{job.cores} cores where the run has {cores}")
    if memory is not None and job.memory is not None and job.memory > memory:
        over.append(f"{size(job.memory)} of memory where the run has {size(memory)}")
    return ", and ".join(over) or None


def over_memory(jobs: list[Job], memory: int | None) -> list[str]:
    """Each job that declares more memory than the run has, as `[3] align needs 64G of memory where the run has 32G`."""
    if memory is None:
        return []
    return [f"[{job.id}] {job.operation} needs {size(job.memory)} of memory where the run has {size(memory)}"
            for job in jobs if job.memory is not None and job.memory > memory]


class ReadyQueue:
    """The run's cores and memory, and the ready jobs waiting for them, highest rank first.

    Jobs that become ready at the same moment, such as every job at the start
    of a run or the dependents of a job that just finished, all join the queue
    before any of them is admitted, so the order depends on rank, then job id,
    and not on which task the event loop wakes first.

    Jobs start in that order and no other: while the job at the head waits for
    cores or memory, the jobs behind it wait too, even those that would fit.
    The head is the job on the longest path, so letting smaller jobs past it
    could delay the whole run, and a stream of small jobs could keep a big one
    waiting for good. A job needing more cores or memory than the run has in
    all takes all of both, so it starts once nothing else is running and runs
    alone. `memory` None sets no limit on memory.
    """

    def __init__(self, cores: int, memory: int | None, ranks: dict[int, float]) -> None:
        self.cores = cores
        self.memory = memory
        self.free_cores = cores
        self.free_memory = memory or 0
        self._ranks = ranks
        self._waiting: list[tuple[float, int, int, int, asyncio.Future[None]]] = []
        self._admitting = False

    def needs(self, job: Job) -> tuple[int, int]:
        """The cores and memory a job takes while it runs: what it needs, or all the run has when that is too little."""
        if too_big(job, self.cores, self.memory) is not None:
            return self.cores, self.memory or 0
        return job.cores, 0 if self.memory is None or job.memory is None else job.memory

    @asynccontextmanager
    async def slot(self, job: Job) -> AsyncIterator[None]:
        """Wait until the job's cores and memory are free, and hold them until the block ends."""
        cores, memory = self.needs(job)
        admitted = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiting, (-self._ranks.get(job.id, 0.0), job.id, cores, memory, admitted))
        self._admit_soon()
        try:
            await admitted
        except asyncio.CancelledError:
            # Admitted, then cancelled before it could start: give its share back.
            if admitted.done() and not admitted.cancelled():
                self._release(cores, memory)
            raise
        try:
            yield
        finally:
            self._release(cores, memory)

    def _release(self, cores: int, memory: int) -> None:
        self.free_cores += cores
        self.free_memory += memory
        self._admit_soon()

    def _admit_soon(self) -> None:
        # Once the tasks woken with this one have queued too.
        if not self._admitting:
            self._admitting = True
            asyncio.get_running_loop().call_soon(self._admit)

    def _admit(self) -> None:
        """Start waiting jobs, highest rank first, while the next one's share is free; the one place a job is let in."""
        self._admitting = False
        while self._waiting:
            _, _, cores, memory, admitted = self._waiting[0]
            if admitted.done():
                # Cancelled while it waited.
                heapq.heappop(self._waiting)
                continue
            if cores > self.free_cores or memory > self.free_memory:
                # It starts when enough jobs finish; `needs` keeps it within the run's
                # whole share, so it always can.
                return
            heapq.heappop(self._waiting)
            self.free_cores -= cores
            self.free_memory -= memory
            admitted.set_result(None)
