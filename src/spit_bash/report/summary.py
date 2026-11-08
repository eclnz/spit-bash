"""What a run came to, as counts and lists, for its summary line and any other report of it."""

from __future__ import annotations

from dataclasses import dataclass

from ..execute.scheduler import Result
from ..model import Dag
from ..plan import Decision, Status


@dataclass(frozen=True)
class Summary:
    done: int
    # The jobs whose outputs were moved to their new paths, their checks run too where they needed them.
    placed: int
    checked: int
    # The jobs that failed of their own accord, in DAG order.
    failed: tuple[Result, ...]
    # The jobs that did not run because a job they need failed, in DAG order.
    not_run: tuple[Result, ...]
    current: int
    blocked: int
    # The current jobs planned `maybe` that were not rerun, as their upstream output was unchanged.
    cutoff: int

    @classmethod
    def of(cls, decisions: tuple[Decision, ...], results: tuple[Result, ...]) -> Summary:
        """Count each job by what it came to: a `maybe` job is whatever the run settled it as."""
        failures = [result for result in results if result.failure is not None]
        failed_ids = {result.job.id for result in failures}
        outcome = {result.job.id: result.settled for result in results}
        count = {status: 0 for status in Status}
        cutoff = 0
        for decision in decisions:
            status = outcome.get(decision.job.id) or decision.status
            if decision.job.id in failed_ids:
                continue
            count[status] += 1
            cutoff += decision.status is Status.MAYBE and status is not Status.RUN
        return cls(
            done=count[Status.RUN], placed=count[Status.PLACE], checked=count[Status.CHECK],
            failed=tuple(result for result in failures if result.caused_by is None),
            not_run=tuple(result for result in failures if result.caused_by is not None),
            current=count[Status.SKIP], blocked=count[Status.BLOCKED], cutoff=cutoff,
        )


def stale(everything: Dag, chosen: Dag, decisions: tuple[Decision, ...], results: tuple[Result, ...]) -> int:
    """The jobs left out of a partial run that read an output the run rewrote."""
    settled = {result.job.id: result.settled for result in results if result.failure is None}
    rewritten = set()
    for decision in decisions:
        if decision.job.id not in settled:
            continue
        status = settled[decision.job.id] or decision.status
        if status is Status.RUN:
            rewritten.update(decision.job.output_paths)
    kept = {job.id for job in chosen.jobs}
    return sum(1 for job in everything.jobs if job.id not in kept and rewritten.intersection(job.input_paths))
