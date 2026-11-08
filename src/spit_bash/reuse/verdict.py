"""Whether a job's recorded work holds on its files now, from the evidence alone."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ..state.record import Record
from . import records
from .evidence import Evidence, changed


class Standing(Enum):
    """What a job's record comes to."""

    # The record holds: the job need not run.
    CURRENT = "current"
    # The record holds, but its checks have not passed on these files.
    CHECKS_ONLY = "checks only"
    # The record does not hold: the job runs.
    STALE = "stale"


class Stale(Enum):
    """Why a record does not hold, the first that applies in this order."""

    NO_RECORD = "no record"
    WORK_CHANGED = "work changed"
    INPUT_CHANGED = "input changed"
    OUTPUT_CHANGED = "output changed"


@dataclass(frozen=True)
class Verdict:
    standing: Standing
    # Set only when `standing` is `Standing.STALE`.
    cause: Stale | None = None
    # For a record that holds: the record with its files' new stamps, when they
    # were touched but not changed; else None.
    refreshed: Record | None = None


def verdict(found: Evidence) -> Verdict:
    """Whether the record in `found` holds, and why not when it does not."""
    record = found.record
    if record is None:
        return Verdict(Standing.STALE, Stale.NO_RECORD)
    if found.work_changed:
        return Verdict(Standing.STALE, Stale.WORK_CHANGED)
    if changed(found.inputs):
        return Verdict(Standing.STALE, Stale.INPUT_CHANGED)
    if changed(found.outputs):
        return Verdict(Standing.STALE, Stale.OUTPUT_CHANGED)
    refreshed = records.refreshed(record, found.job, found.now)
    return Verdict(Standing.CHECKS_ONLY if found.checks_changed else Standing.CURRENT, None, refreshed)
