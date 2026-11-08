"""Whether a job's recorded work holds on its files now, from the evidence alone."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ..cause import Cause, Why
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


@dataclass(frozen=True)
class Verdict:
    standing: Standing
    # Set only when `standing` is `Standing.STALE`: why the record does not hold, the first
    # that applies of `Why.NO_RECORD`, `Why.WORK_CHANGED`, `Why.INPUT_CHANGED` and `Why.OUTPUT_CHANGED`.
    cause: Cause | None = None
    # For a record that holds: the record with its files' new stamps, when they
    # were touched but not changed; else None.
    refreshed: Record | None = None


def verdict(found: Evidence) -> Verdict:
    """Whether the record in `found` holds, and why not when it does not."""
    record = found.record
    if record is None:
        return Verdict(Standing.STALE, Cause(Why.NO_RECORD))
    if found.work_changed:
        return Verdict(Standing.STALE, Cause(Why.WORK_CHANGED, parts=found.parts))
    if changed(found.inputs):
        return Verdict(Standing.STALE, Cause(Why.INPUT_CHANGED))
    if changed(found.outputs):
        return Verdict(Standing.STALE, Cause(Why.OUTPUT_CHANGED))
    refreshed = records.refreshed(record, found.job, found.now)
    return Verdict(Standing.CHECKS_ONLY if found.checks_changed else Standing.CURRENT, None, refreshed)
