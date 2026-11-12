"""What a run came to, as counts and lists, for its summary line and any other report of it."""

from __future__ import annotations

from dataclasses import dataclass

from ..execute.scheduler import Result
from ..files import stamps
from ..model import Dag
from ..plan import Decision, Status
from ..reuse.evidence import Compare, compare_files
from ..state.store import Store


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


@dataclass(frozen=True)
class Left:
    """What a partial run did to the jobs outside its selection that read what it rewrote."""

    # The jobs whose record no longer matches an output the run rewrote, or that have no record.
    stale: int
    # The rewritten outputs those jobs read that hold what every reader's record does.
    unchanged: int


def left_out(everything: Dag, chosen: Dag, decisions: tuple[Decision, ...], results: tuple[Result, ...],
             state: Store, compare: Compare) -> Left:
    """The jobs left out of a partial run that read an output the run rewrote, judged as `plan` would judge them.

    A rewritten output counts as changed for a reader only when it differs from
    the reader's record by stamp and then by content, so an output rewritten
    with the same bytes leaves its readers current.
    """
    settled = {result.job.id: result.settled for result in results if result.failure is None}
    # The hashes the run just recorded for each rewritten output, so no file is read again.
    hashes: dict[str, str | None] = {}
    for decision in decisions:
        job = decision.job
        if job.id not in settled or (settled[job.id] or decision.status) is not Status.RUN:
            continue
        record = state.jobs.get(job.work.key)
        held = record.output_hashes if record is not None and record.output_hashes else {}
        for slot, path in job.output_paths_by_slot.items():
            hashes[path] = held.get(slot)
    kept = {job.id for job in chosen.jobs}
    stale = 0
    read, changed_paths = set(), set()
    for job in everything.jobs:
        if job.id in kept:
            continue
        rewritten = {slot: path for slot, path in job.input_paths_by_slot.items() if path in hashes}
        if not rewritten:
            continue
        read.update(rewritten.values())
        record = state.jobs.get(job.work.key)
        if record is None:
            stale += 1
            continue
        # A path the run could not hash is read here, as `plan` would read it.
        known = {path: hashes[path] for path in rewritten.values() if hashes[path] is not None}
        now = stamps(everything, tuple(rewritten.values()))
        diffs = compare_files(everything, rewritten, now, record.inputs, record.input_hashes, compare, known, True)
        differing = {diff.path for diff in diffs if diff.changed}
        changed_paths.update(differing)
        stale += bool(differing)
    return Left(stale, len(read - changed_paths))
