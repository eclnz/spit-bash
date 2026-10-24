"""Estimate how long the rest of a run will take, from how long each job took before."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .dag import Job


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


@dataclass
class _Operation:
    """What is known of one operation's jobs: how long they took, and how many are left."""

    total: float = 0.0
    count: int = 0
    # The remaining jobs with a time of their own from an earlier run, summed,
    # and how many remaining jobs have none.
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
    """

    workers: int
    left: int = 0
    _operations: dict[str, _Operation] = field(default_factory=dict)
    _own: dict[int, float] = field(default_factory=dict)
    # The jobs running now, by id, and when each started.
    _started: dict[int, tuple[Job, float]] = field(default_factory=dict)

    @classmethod
    def of(cls, to_run: list[Job], dag_jobs: tuple[Job, ...], previous: dict[int, float], workers: int) -> Progress:
        """`previous` gives each job's time from its last run, by job id, for any job in the DAG."""
        progress = cls(workers, len(to_run))
        for job in dag_jobs:
            if job.id in previous:
                operation = progress._operation(job)
                operation.total += previous[job.id]
                operation.count += 1
        for job in to_run:
            operation = progress._operation(job)
            if job.id in previous:
                progress._own[job.id] = previous[job.id]
                operation.known += previous[job.id]
            else:
                operation.unknown += 1
        return progress

    def _operation(self, job: Job) -> _Operation:
        return self._operations.setdefault(job.operation, _Operation())

    def _expected(self, job: Job) -> float | None:
        return self._own.get(job.id, self._operation(job).mean())

    def _remove(self, job: Job) -> None:
        operation = self._operation(job)
        if job.id in self._own:
            operation.known -= self._own[job.id]
        else:
            operation.unknown -= 1
        self.left -= 1

    def started(self, job: Job) -> None:
        self._remove(job)
        self._started[job.id] = (job, time.monotonic())

    def finished(self, job: Job, seconds: float | None) -> None:
        """Count the job as done: in `seconds` when it succeeded, else None."""
        del self._started[job.id]
        if seconds is not None:
            operation = self._operation(job)
            operation.total += seconds
            operation.count += 1

    def dropped(self, job: Job) -> None:
        """A job that will not start, as a job it needs failed."""
        self._remove(job)

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
            expected = self._expected(job)
            if expected is None:
                return None
            work += max(expected - (now - start), 0.0)
        jobs = self.left + len(self._started)
        if jobs == 0:
            return 0.0
        return work / min(self.workers, jobs)

    def report(self) -> str:
        """`5 left, about 12m`, or `5 left` without an estimate."""
        jobs = self.left + len(self._started)
        remaining = self.remaining() if jobs else None
        return f"{jobs} left" if remaining is None else f"{jobs} left, about {duration(remaining)}"
