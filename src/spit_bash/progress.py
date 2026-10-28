"""Estimate how long the rest of a run will take, from how long each job took before."""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass, field

from .dag import Job
from .schedule import longest_paths

# How often, at most, the longest path through the jobs left is worked out
# again, in seconds: it takes a pass over every job left.
REFRESH_SECONDS = 5.0


def duration(seconds: float) -> str:
    """A rough length of time, as `under 1s`, `45s`, `12m` or `3h05m`."""
    if seconds < 1:
        return "under 1s"
    seconds = round(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{round(seconds / 60)}m"
    hours, minutes = divmod(round(seconds / 60), 60)
    return f"{hours}h{minutes:02d}m"


def about(seconds: float) -> str:
    """An estimate, as `about 12m`, or `under 1s`."""
    return "under 1s" if seconds < 1 else f"about {duration(seconds)}"


@dataclass
class _Operation:
    """What is known of one operation's jobs: how long they took, and how much work is left."""

    total: float = 0.0
    count: int = 0
    # The core-seconds of the remaining jobs with a time of their own from an
    # earlier run, and the cores of the remaining jobs with none.
    known: float = 0.0
    unknown: int = 0

    def mean(self) -> float | None:
        return self.total / self.count if self.count else None


@dataclass
class Progress:
    """The jobs a run will run, and how long each is expected to take.

    A job is expected to take as long as its last successful run did. A job
    with no time of its own is expected to take the mean time of its
    operation's jobs, from earlier runs and from this one. Until every
    remaining job has an estimate, there is no estimate for the run.

    The run has at least two things left to do: its work, in core-seconds
    shared among its cores, and its longest chain of jobs that wait for one
    another, which extra cores cannot shorten. The estimate is the longer of
    the two. A job counts for the cores it takes, at most every core the run
    has; memory is not counted, so a run held back by memory takes longer.
    """

    cores: int
    left: int = 0
    # The cores the jobs left and running take, summed.
    _width: int = 0
    _operations: dict[str, _Operation] = field(default_factory=dict)
    _own: dict[int, float] = field(default_factory=dict)
    # The jobs running now, by id, and when each started.
    _started: dict[int, tuple[Job, float]] = field(default_factory=dict)
    # The jobs to run, in DAG order, and the ids of those not started yet.
    _jobs: list[Job] = field(default_factory=list)
    _waiting: set[int] = field(default_factory=set)
    # The longest path to the end of the run from each job's end, and the
    # waiting jobs by the longest from their start, longest first; ids no
    # longer waiting are dropped as they reach the top. Worked out at
    # `_paths_at`, by the estimates then.
    _after: dict[int, float] = field(default_factory=dict)
    _longest: list[tuple[float, int]] = field(default_factory=list)
    _paths_at: float | None = None

    @classmethod
    def of(cls, to_run: list[Job], dag_jobs: tuple[Job, ...], previous: dict[int, float], cores: int) -> Progress:
        """`previous` gives each job's time from its last run, by job id, for any job in the DAG.

        `to_run` is in DAG order, as the plan gives it.
        """
        progress = cls(cores, len(to_run), _jobs=list(to_run), _waiting={job.id for job in to_run})
        for job in dag_jobs:
            if job.id in previous:
                operation = progress._operation(job)
                operation.total += previous[job.id]
                operation.count += 1
        for job in to_run:
            operation = progress._operation(job)
            progress._width += progress._cores(job)
            if job.id in previous:
                progress._own[job.id] = previous[job.id]
                operation.known += previous[job.id] * progress._cores(job)
            else:
                operation.unknown += progress._cores(job)
        return progress

    def _operation(self, job: Job) -> _Operation:
        return self._operations.setdefault(job.operation, _Operation())

    def _cores(self, job: Job) -> int:
        """The cores a job takes: what it needs, or every core the run has when that is less."""
        return min(job.cores, self.cores)

    def expected(self, job: Job) -> float | None:
        """How long a job is expected to take, or None when neither it nor its operation has a time."""
        if job.id in self._own:
            return self._own[job.id]
        operation = self._operations.get(job.operation)
        return None if operation is None else operation.mean()

    def _remove(self, job: Job) -> None:
        operation = self._operation(job)
        if job.id in self._own:
            operation.known -= self._own[job.id] * self._cores(job)
        else:
            operation.unknown -= self._cores(job)
        self.left -= 1
        self._waiting.discard(job.id)

    def started(self, job: Job) -> None:
        self._remove(job)
        self._started[job.id] = (job, time.monotonic())

    def finished(self, job: Job, seconds: float | None) -> None:
        """Count the job as done: in `seconds` when it succeeded, else None."""
        del self._started[job.id]
        self._width -= self._cores(job)
        if seconds is not None:
            operation = self._operation(job)
            operation.total += seconds
            operation.count += 1

    def dropped(self, job: Job) -> None:
        """A job that will not start, as a job it needs failed."""
        self._remove(job)
        self._width -= self._cores(job)

    def remaining(self) -> float | None:
        """Seconds until the run ends, or None when some job has no estimate."""
        work = 0.0
        for operation in self._operations.values():
            mean = operation.mean()
            if operation.unknown and mean is None:
                return None
            work += operation.known + operation.unknown * (mean or 0.0)
        # A running job counts for what it has left, by its estimate.
        now = time.monotonic()
        for job, start in self._started.values():
            expected = self.expected(job)
            if expected is None:
                return None
            work += max(expected - (now - start), 0.0) * self._cores(job)
        if self.left + len(self._started) == 0:
            return 0.0
        return max(work / min(self.cores, self._width), self._critical(now))

    def _critical(self, now: float) -> float:
        """The longest chain of expected times through the jobs left; a running job counts for what it has left.

        Called only once every job left has an estimate.
        """
        if self._paths_at is None or now - self._paths_at >= REFRESH_SECONDS:
            self._paths(now)
        while self._longest and self._longest[0][1] not in self._waiting:
            heapq.heappop(self._longest)
        longest = -self._longest[0][0] if self._longest else 0.0
        for job, start in self._started.values():
            left = max((self.expected(job) or 0.0) - (now - start), 0.0)
            longest = max(longest, left + self._after.get(job.id, 0.0))
        return longest

    def _paths(self, now: float) -> None:
        """Work out each job's longest path again, by the estimates now, over the jobs left."""
        self._jobs = [job for job in self._jobs if job.id in self._waiting or job.id in self._started]
        start, self._after = longest_paths(self._jobs, lambda job: self.expected(job) or 0.0)
        self._longest = [(-start[job.id], job.id) for job in self._jobs if job.id in self._waiting]
        heapq.heapify(self._longest)
        self._paths_at = now

    def report(self) -> str:
        """`5 left, about 12m`, or `5 left` without an estimate."""
        jobs = self.left + len(self._started)
        remaining = self.remaining() if jobs else None
        return f"{jobs} left" if remaining is None else f"{jobs} left, {about(remaining)}"
