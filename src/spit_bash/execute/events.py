"""What a run reports as it goes, as data: `report.run_text` gives the words."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

from ..model import Job

if TYPE_CHECKING:
    from .scheduler import Result


@dataclass(frozen=True)
class RunsAlone:
    """Before the run: a job needs more cores or memory than the run has, as `over` says, so it runs alone."""

    job: Job
    over: str


@dataclass(frozen=True)
class Running:
    """Before the run, when any job is to run or move its outputs.

    `runs` jobs will run, `maybe` more may, in about `remaining` seconds, and
    `places` jobs will move their outputs.
    """

    runs: int
    maybe: int
    # None when some job has no estimate.
    remaining: float | None
    places: int = 0


@dataclass(frozen=True)
class Started:
    job: Job


@dataclass(frozen=True)
class Done:
    """The job succeeded in `seconds`; `progress` is what is left of the run, as `Progress.report` gives it."""

    job: Job
    seconds: float
    progress: str


@dataclass(frozen=True)
class Placed:
    """A job planned `place` has moved its outputs, each as (recorded path, current path), and its record with them."""

    job: Job
    moves: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Checking:
    """A current job's checks start on the files it has."""

    job: Job


@dataclass(frozen=True)
class Checked:
    job: Job


@dataclass(frozen=True)
class SkippedUnchanged:
    """A job planned `maybe` is skipped: what it reads came out of its upstream jobs unchanged."""

    job: Job


@dataclass(frozen=True)
class JobFailed:
    """A job failed of its own accord."""

    result: Result


@dataclass(frozen=True)
class NotRun:
    """A job did not run because a job it needs failed."""

    result: Result


Event = Union[RunsAlone, Running, Started, Done, Placed, Checking, Checked, SkippedUnchanged, JobFailed, NotRun]
