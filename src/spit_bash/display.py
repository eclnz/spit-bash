"""How a job is named in the terminal: its operation and the work it does."""

from __future__ import annotations

from collections import Counter
from typing import Iterable

from .model import Job

# The longest entity value shown, and the longest label.
MAX_VALUE = 24
MAX_LABEL = 80


def _shorten(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _entities(job: Job) -> str:
    if not job.output_artifacts:
        return ""
    return " ".join(f"{name}={_shorten(value, MAX_VALUE)}" for name, value in job.output_artifacts[0].entities)


def labels(jobs: Iterable[Job]) -> dict[int, str]:
    """What each job works on, by job id: its first output's entities, such as `sub=02 run=01`.

    A group job's output keeps only the dimensions it groups by, so its label names the group.
    Where the entities leave two jobs of one operation looking alike, as for jobs with none,
    the product is shown too.
    """
    jobs = list(jobs)
    entities = {job.id: _entities(job) for job in jobs}
    alike = Counter((job.operation, entities[job.id]) for job in jobs)
    named = {}
    for job in jobs:
        label = entities[job.id]
        if alike[(job.operation, label)] > 1 and job.output_artifacts:
            label = f"{_shorten(job.output_artifacts[0].product, MAX_VALUE)} {label}".strip()
        named[job.id] = _shorten(label, MAX_LABEL)
    return named


def id_width(jobs: Iterable[Job]) -> int:
    """The width that lines up the ids of these jobs."""
    return max((len(str(job.id)) for job in jobs), default=1)


def name(job: Job, label: str) -> str:
    """The operation, then the label when there is one."""
    return f"{job.operation} {label}" if label else job.operation
